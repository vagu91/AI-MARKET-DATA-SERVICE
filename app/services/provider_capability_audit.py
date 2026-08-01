from __future__ import annotations

import hashlib
import inspect
import json
import math
import re
import shutil
from collections import Counter, defaultdict
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, is_dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from app.core.redaction import redact_sensitive
from app.services.provider_audit_provenance import (
    provider_audit_source_provenance,
)
from app.services.source_policy_service import SourcePolicyService


class HealthStatus(StrEnum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNUSABLE = "UNUSABLE"
    DOWN = "DOWN"
    AUTH_FAILED = "AUTH_FAILED"
    RATE_LIMITED = "RATE_LIMITED"
    NOT_CONFIGURED = "NOT_CONFIGURED"
    UNKNOWN = "UNKNOWN"


TERMINAL_HEALTH_STATUSES = frozenset(item.value for item in HealthStatus)
AI_VERIFICATION_ORIGINS = frozenset(
    {
        "AUDIT_SOURCE_URL_GET",
        "AUDIT_TRANSPORT",
        "SERVER_ACQUIRED",
        "SOURCE_GATEWAY",
    }
)

class AuditStatus(StrEnum):
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class SystemHealth(StrEnum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    CRITICAL = "CRITICAL"
    UNKNOWN = "UNKNOWN"


QUALITY_COMPONENTS: tuple[tuple[str, str, int], ...] = (
    ("transport", "transport_valid", 10),
    ("schema", "schema_valid", 15),
    ("required_field_completeness", "completeness_valid", 10),
    ("freshness_lifecycle", "freshness_valid", 15),
    ("semantic_mapping", "semantic_mapping_valid", 20),
    ("occurrence_reference_release", "occurrence_match_valid", 15),
    ("field_lineage", "lineage_valid", 15),
)
QUALITY_SCORE_MAX = sum(component[2] for component in QUALITY_COMPONENTS)
if QUALITY_SCORE_MAX != 100:  # pragma: no cover - import-time invariant
    raise RuntimeError("provider capability quality weights must total 100")


CORRECTNESS_CHECKS = (
    "schema_valid",
    "completeness_valid",
    "freshness_valid",
    "semantic_mapping_valid",
    "occurrence_match_valid",
    "lineage_valid",
)
FATAL_QUALITY_CHECKS = (
    "schema_valid",
    "semantic_mapping_valid",
    "occurrence_match_valid",
    "lineage_valid",
)
DEGRADABLE_QUALITY_CHECKS = (
    "completeness_valid",
    "freshness_valid",
)

PRIMARY_ROLES = frozenset(
    {"PRIMARY", "CANONICAL", "CANONICAL_REPOSITORY"}
)
FALLBACK_ROLES = frozenset({"FALLBACK", "AI_FALLBACK", "PRIMARY"})
FIELD_OBSERVATION_SCHEMA_VERSION = "provider-capability-field-observation-v1"
_NORMALIZED_RESPONSE_NOT_SUPPLIED = object()
_REFERENCE_PARAMETER_BY_PROVIDER = {
    "CENSUS": "period",
    "INVESTING_EVENT_1062": "expected_period",
    "SPGLOBAL": "expected_period",
}


@dataclass(frozen=True)
class AuditFilters:
    providers: tuple[str, ...] = ()
    datasets: tuple[str, ...] = ()
    metrics: tuple[str, ...] = ()
    include_ai: bool = True

    @classmethod
    def from_values(
        cls,
        *,
        providers: Iterable[str] = (),
        datasets: Iterable[str] = (),
        metrics: Iterable[str] = (),
        include_ai: bool = True,
    ) -> AuditFilters:
        return cls(
            providers=_normalized_filter(providers),
            datasets=_normalized_filter(datasets),
            metrics=_normalized_filter(metrics),
            include_ai=bool(include_ai),
        )


@dataclass(frozen=True)
class CapabilityTarget:
    provider_id: str
    provider_type: str
    dataset_id: str
    metric_id: str
    fields: tuple[str, ...]
    frequency: str | None
    transformation: str | None
    probe_id: str | None
    field_validator_id: str | None
    probe_adapter_path: str | None
    request_group: str | None
    allowed_roles: tuple[str, ...]
    timeout_seconds: float
    max_attempts: int
    degradable_quality_checks: tuple[str, ...]
    registration: Any = field(repr=False, compare=False)
    capability: Any = field(repr=False, compare=False)

    @property
    def target_id(self) -> str:
        return f"{self.provider_id}|{self.dataset_id}|{self.metric_id}"

    def field_key(self, field_name: str) -> str:
        return f"{self.target_id}|{field_name}"


@dataclass(frozen=True)
class ProbeRequest:
    acquisition_id: str
    request_key: str
    run_id: str
    targets: tuple[CapabilityTarget, ...]
    sandbox_root: Path
    database_snapshot_path: Path | None
    settings: Any = field(repr=False, compare=False)
    request_correlations: Mapping[str, Mapping[str, Any]] = field(
        default_factory=dict,
        repr=False,
        compare=False,
    )
    acquisition_kind: str = "CAPABILITY"
    adapter_path_override: str | None = None

    @property
    def provider_id(self) -> str:
        return self.targets[0].provider_id

    @property
    def registration(self) -> Any:
        return self.targets[0].registration

    @property
    def timeout_seconds(self) -> float:
        return min(target.timeout_seconds for target in self.targets)

    @property
    def max_attempts(self) -> int:
        return min(target.max_attempts for target in self.targets)

    @property
    def leaf_request_count(self) -> int:
        """Bound the distinct provider leaves represented by this request.

        A grouped probe can legitimately fan out to one call per registered
        target or comma-separated series.  This is separate from
        ``max_attempts``, which remains the retry limit for any one exact HTTP
        request fingerprint.
        """

        declared = _value(
            self.registration,
            "audit_leaf_request_count",
            None,
        )
        if isinstance(declared, int) and not isinstance(declared, bool):
            return max(1, declared)
        return sum(
            max(
                1,
                len(
                    [
                        item
                        for item in re.split(r"\s*,\s*", target.metric_id)
                        if item
                    ]
                ),
            )
            for target in self.targets
        )

    @property
    def call_budget(self) -> int:
        declared = _value(self.registration, "audit_call_budget", None)
        if isinstance(declared, int) and not isinstance(declared, bool):
            base_budget = max(1, declared)
        else:
            base_budget = self.leaf_request_count * self.max_attempts
        source_url_budget = _value(
            self.registration,
            "audit_source_url_budget",
            0,
        )
        if not isinstance(source_url_budget, int) or isinstance(
            source_url_budget,
            bool,
        ):
            source_url_budget = 0
        return base_budget + max(0, source_url_budget)

    @property
    def adapter_path(self) -> str:
        if self.adapter_path_override:
            return self.adapter_path_override
        paths = {
            target.probe_adapter_path
            or str(_value(target.registration, "adapter_path", "")).strip()
            for target in self.targets
        }
        if len(paths) != 1:
            raise RuntimeError("PROBE_REQUEST_HAS_DIVERGENT_ADAPTER_PATHS")
        return next(iter(paths))

    def correlation_for(self, target: CapabilityTarget) -> Mapping[str, Any]:
        value = self.request_correlations.get(target.target_id)
        return value if isinstance(value, Mapping) else {}


@dataclass(frozen=True)
class CapturedHttpExchange:
    method: str
    url: str
    request_headers: Sequence[tuple[str, str]] | Mapping[str, Any]
    status_code: int
    response_headers: Sequence[tuple[str, str]] | Mapping[str, Any]
    response_body: bytes
    latency_ms: float
    attempt: int


@dataclass
class ProbeOutcome:
    configured: bool = True
    transport_status: str = "UNKNOWN"
    http_status: int | None = None
    headers: Sequence[tuple[str, str]] | Mapping[str, Any] = field(
        default_factory=dict
    )
    raw_response: bytes | None = None
    normalized_response: Any = None
    latency_ms: float | None = None
    attempts: int = 0
    error_kind: str | None = None
    reason_codes: tuple[str, ...] = ()
    checks: Mapping[str, bool | None] = field(default_factory=dict)
    field_checks: Mapping[str, Mapping[str, bool | None]] = field(default_factory=dict)
    evidence: Mapping[str, Any] = field(default_factory=dict)
    network_exchanges: tuple[CapturedHttpExchange, ...] = ()
    checked_at: str = field(default_factory=lambda: utc_now().isoformat())

    @classmethod
    def not_configured(cls, *reason_codes: str) -> ProbeOutcome:
        return cls(
            configured=False,
            transport_status=HealthStatus.NOT_CONFIGURED.value,
            reason_codes=tuple(reason_codes) or ("PROVIDER_NOT_CONFIGURED",),
            checks={name: None for name in _all_check_names()},
        )

    @classmethod
    def unknown(cls, *reason_codes: str) -> ProbeOutcome:
        return cls(
            transport_status=HealthStatus.UNKNOWN.value,
            reason_codes=tuple(reason_codes) or ("PROBE_RESULT_UNKNOWN",),
            checks={name: None for name in _all_check_names()},
        )


class ProbeExecutor(Protocol):
    def __call__(self, request: ProbeRequest) -> ProbeOutcome | Awaitable[ProbeOutcome]:
        """Execute one isolated, no-fallback acquisition."""


@dataclass(frozen=True)
class AuditExecution:
    run_id: str
    audit_status: str
    system_health: str
    report: Mapping[str, Any]
    capability_matrix: Mapping[str, Any]
    artifact_directory: Path | None = None
    candidate_pointer: Path | None = None
    latest_pointer: Path | None = None


class ProbeExecutionError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status: HealthStatus = HealthStatus.UNKNOWN,
        reason_code: str = "PROBE_EXECUTION_FAILED",
    ) -> None:
        super().__init__(message)
        self.status = status
        self.reason_code = reason_code


@dataclass(frozen=True)
class FallbackObservation:
    source_id: str
    source_kind: str
    attempted: bool
    valid: bool
    value: Any = None
    reason_code: str | None = None
    lineage: tuple[Mapping[str, Any], ...] = ()
    ai_certified: bool | None = None
    attempts: int = 0


@dataclass(frozen=True)
class FallbackDecision:
    selected_source: str | None
    delivered_value: Any
    reason_code: str
    attempted_order: tuple[str, ...]
    total_attempts: int
    lineage: tuple[Mapping[str, Any], ...]
    accounting: tuple[Mapping[str, Any], ...]


def evaluate_fallback_chain(
    observations: Sequence[FallbackObservation],
) -> FallbackDecision:
    """Evaluate an observed DB-first chain without executing or inventing attempts."""

    _validate_fallback_observation_order(observations)
    attempted_order: list[str] = []
    accounting: list[Mapping[str, Any]] = []
    total_attempts = 0
    for item in observations:
        if item.source_kind.upper() == "AI" and item.valid and item.ai_certified is not True:
            valid = False
            reason_code = "AI_CAPABILITY_NOT_CERTIFIED"
        else:
            valid = item.valid
            reason_code = item.reason_code
        if item.attempted:
            attempted_order.append(item.source_id)
            total_attempts += max(0, int(item.attempts))
        accounting.append(
            {
                "source_id": item.source_id,
                "source_kind": item.source_kind,
                "attempted": item.attempted,
                "attempts": max(0, int(item.attempts)),
                "valid": valid,
                "reason_code": reason_code,
                "selected": bool(item.attempted and valid),
            }
        )
        if item.attempted and valid:
            if item.value is None:
                accounting[-1]["selected"] = False
                accounting[-1]["reason_code"] = reason_code or "SOURCE_RETURNED_NULL"
                continue
            return FallbackDecision(
                selected_source=item.source_id,
                delivered_value=item.value,
                reason_code="SOURCE_SELECTED",
                attempted_order=tuple(attempted_order),
                total_attempts=total_attempts,
                lineage=item.lineage,
                accounting=tuple(accounting),
            )
    return FallbackDecision(
        selected_source=None,
        delivered_value=None,
        reason_code="ALL_SOURCES_FAILED_OR_UNAVAILABLE",
        attempted_order=tuple(attempted_order),
        total_attempts=total_attempts,
        lineage=(),
        accounting=tuple(accounting),
    )


def _validate_fallback_observation_order(
    observations: Sequence[FallbackObservation],
) -> None:
    if not observations:
        return
    if observations[0].source_kind.upper() not in {"DB", "REPOSITORY"}:
        raise ValueError("FALLBACK_CHAIN_MUST_START_WITH_DATABASE")
    ai_seen = False
    provider_seen = False
    for index, item in enumerate(observations):
        kind = item.source_kind.upper()
        if kind in {"DB", "REPOSITORY"}:
            if index != 0 or provider_seen or ai_seen:
                raise ValueError("DATABASE_MUST_BE_FIRST_AND_UNIQUE")
            continue
        if kind == "AI":
            ai_seen = True
            continue
        if ai_seen:
            raise ValueError("AI_MUST_FOLLOW_ALL_DETERMINISTIC_FALLBACKS")
        provider_seen = True


def _result_metric_id(result: Mapping[str, Any]) -> str:
    metric_id = str(result.get("metric_id") or "").strip()
    if metric_id:
        return metric_id
    capability_id = str(result.get("capability_id") or "")
    return (
        capability_id.split("|", 3)[2]
        if capability_id.count("|") >= 2
        else ""
    )


def _result_eligible_for_source_kind(
    result: Mapping[str, Any],
    source_kind: str,
) -> bool:
    if source_kind == "DB":
        return result.get("health_status") in {
            HealthStatus.HEALTHY.value,
            HealthStatus.DEGRADED.value,
        }
    if source_kind == "PRIMARY":
        return result.get("eligible_as_primary") is True
    return result.get("eligible_as_fallback") is True


@dataclass(frozen=True, slots=True)
class _PolicyPhase:
    phase_id: str
    phase_index: int
    source_id: str
    source_kind: str


def _policy_source_phases(policy: Any) -> tuple[_PolicyPhase, ...]:
    source_roles = (
        (
            str(_value(policy, "canonical_repository", "") or "").strip(),
            "DB",
        ),
        (
            str(_value(policy, "primary_provider", "") or "").strip(),
            "PRIMARY",
        ),
        *(
            (str(source_id or "").strip(), "FALLBACK")
            for source_id in (
                _value(policy, "fallback_providers", ()) or ()
            )
        ),
        *(
            (str(source_id or "").strip(), "AI")
            for source_id in (
                _value(policy, "ai_fallback_providers", ()) or ()
            )
        ),
    )
    return tuple(
        _PolicyPhase(
            phase_id=f"{index:02d}:{source_kind}:{source_id}",
            phase_index=index,
            source_id=source_id,
            source_kind=source_kind,
        )
        for index, (source_id, source_kind) in enumerate(source_roles)
    )


def _delivery_identifier(value: Any) -> tuple[str | None, bool]:
    if value is None:
        return None, True
    if not isinstance(value, str):
        return None, False
    normalized = value.strip()
    if not normalized or normalized.casefold() == "none":
        return None, False
    return normalized, True


def _delivery_field_pairs(
    capability: Any,
    required_fields: Sequence[str],
) -> tuple[tuple[tuple[str, str], ...], bool]:
    declared = _value(capability, "delivery_field_map", ())
    if declared is None:
        return (), False
    if not declared:
        return tuple((field_name, field_name) for field_name in required_fields), True
    pairs: list[tuple[str, str]] = []
    source_fields: set[str] = set()
    delivery_fields: set[str] = set()
    valid = True
    for item in declared:
        if not isinstance(item, (tuple, list)) or len(item) != 2:
            valid = False
            continue
        source_field, delivery_field = item
        if not isinstance(source_field, str) or not isinstance(
            delivery_field,
            str,
        ):
            valid = False
            continue
        source_field = source_field.strip()
        delivery_field = delivery_field.strip()
        if (
            not source_field
            or not delivery_field
            or source_field not in required_fields
            or source_field in source_fields
            or delivery_field in delivery_fields
        ):
            valid = False
            continue
        source_fields.add(source_field)
        delivery_fields.add(delivery_field)
        pairs.append((source_field, delivery_field))
    return tuple(pairs), bool(valid and pairs)


def _declared_atomic_capabilities_v2(
    source_id: str,
    dataset_id: str,
    *,
    registry: Sequence[Any] | None,
    result_rows: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    if registry is not None:
        registrations = [
            registration
            for registration in registry
            if str(_value(registration, "provider_id", "") or "").strip()
            == source_id
        ]
        if len(registrations) != 1:
            return ()
        capabilities = tuple(
            capability
            for capability in _capabilities(registrations[0])
            if str(_value(capability, "dataset_id", "") or "").strip()
            == dataset_id
        )
    else:
        inferred: dict[str, Mapping[str, Any]] = {}
        for row in result_rows:
            metric_id = _result_metric_id(row)
            if not metric_id:
                continue
            fields = tuple(
                str(field_name).strip()
                for field_name in (row.get("supported_fields") or ())
                if str(field_name).strip()
            )
            if not fields and isinstance(row.get("field_results"), Mapping):
                fields = tuple(str(field_name) for field_name in row["field_results"])
            inferred[metric_id] = {
                "metric_id": metric_id,
                "supported_fields": fields or ("__capability__",),
                "canonical_metric_ids": tuple(
                    row.get("canonical_metric_ids") or ()
                ),
                "delivery_capability_id": row.get(
                    "delivery_capability_id"
                ),
                "delivery_capability_ids": tuple(
                    row.get("delivery_capability_ids") or ()
                ),
                "delivery_field_map": tuple(
                    row.get("delivery_field_map") or ()
                ),
            }
        capabilities = tuple(inferred[key] for key in sorted(inferred))

    declared_capabilities: list[Mapping[str, Any]] = []
    for capability in capabilities:
        metric_id = str(_value(capability, "metric_id", "") or "").strip()
        required_fields = tuple(
            str(field_name).strip()
            for field_name in (
                _value(capability, "supported_fields", ())
                or _value(capability, "fields", ())
                or ()
            )
            if str(field_name).strip()
        )
        delivery_id, delivery_id_valid = _delivery_identifier(
            _value(capability, "delivery_capability_id", None)
        )
        raw_canonical_ids = (
            _value(capability, "canonical_metric_ids", ()) or ()
        )
        raw_delivery_ids = (
            _value(capability, "delivery_capability_ids", ()) or ()
        )
        delivery_capability_ids = tuple(
            str(item).strip()
            for item in raw_delivery_ids
            if isinstance(item, str) and item.strip()
        )
        delivery_ids_valid = bool(
            len(delivery_capability_ids) == len(tuple(raw_delivery_ids))
            and all(
                item.casefold() != "none"
                for item in delivery_capability_ids
            )
            and len(delivery_capability_ids)
            == len(set(delivery_capability_ids))
        )
        canonical_metric_ids = tuple(
            str(item).strip()
            for item in raw_canonical_ids
            if isinstance(item, str) and item.strip()
        )
        canonical_ids_valid = bool(
            len(canonical_metric_ids) == len(tuple(raw_canonical_ids))
            and all(item.casefold() != "none" for item in canonical_metric_ids)
        )
        delivery_fields, delivery_fields_valid = _delivery_field_pairs(
            capability,
            required_fields,
        )
        chain_metric_ids = (
            (delivery_id,)
            if delivery_id is not None
            else delivery_capability_ids
            or canonical_metric_ids
            or ((metric_id,) if metric_id else ())
        )
        declared_capabilities.append(
            {
                "metric_id": metric_id,
                "canonical_metric_ids": canonical_metric_ids,
                "chain_metric_ids": chain_metric_ids,
                "required_fields": required_fields,
                "delivery_fields": delivery_fields,
                "delivery_metadata_valid": bool(
                    metric_id
                    and required_fields
                    and delivery_id_valid
                    and delivery_ids_valid
                    and canonical_ids_valid
                    and delivery_fields_valid
                    and chain_metric_ids
                ),
            }
        )
    return tuple(declared_capabilities)


def _phase_atomic_results(
    phase: _PolicyPhase,
    dataset_id: str,
    *,
    registry: Sequence[Any] | None,
    result_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    source_results = [
        row
        for row in result_rows
        if str(row.get("provider_id") or "") == phase.source_id
        and str(row.get("dataset_id") or "") == dataset_id
    ]
    declared = _declared_atomic_capabilities_v2(
        phase.source_id,
        dataset_id,
        registry=registry,
        result_rows=source_results,
    )
    atomic: list[dict[str, Any]] = []
    for capability in declared:
        metric_id = str(capability["metric_id"])
        required_fields = tuple(capability["required_fields"])
        expected_capability_id = (
            f"{phase.source_id}|{dataset_id}|{metric_id}"
        )
        exact = [
            row
            for row in source_results
            if _result_metric_id(row) == metric_id
            and row.get("capability_id") == expected_capability_id
            and row.get("health_status") in TERMINAL_HEALTH_STATUSES
        ]
        result = exact[0] if len(exact) == 1 else None
        field_results = (
            result.get("field_results")
            if isinstance(result, Mapping)
            and isinstance(result.get("field_results"), Mapping)
            else {}
        )
        pseudo = required_fields == ("__capability__",)
        field_terminal = {
            field_name: bool(
                pseudo
                or (
                    isinstance(field_results.get(field_name), Mapping)
                    and field_results[field_name].get("health_status")
                    in TERMINAL_HEALTH_STATUSES
                )
            )
            for field_name in required_fields
        }
        exact_field_surface = bool(
            pseudo or set(field_results) == set(required_fields)
        )
        terminal = bool(
            result is not None
            and capability["delivery_metadata_valid"]
            and exact_field_surface
            and all(field_terminal.values())
        )
        eligible = bool(
            terminal
            and _result_eligible_for_source_kind(
                result,
                phase.source_kind,
            )
        )
        atomic.append(
            {
                "registered_metric_id": metric_id,
                "canonical_metric_ids": list(
                    capability["canonical_metric_ids"]
                ),
                "chain_metric_ids": list(capability["chain_metric_ids"]),
                "required_fields": list(required_fields),
                "delivery_fields": [
                    {
                        "source_field": source_field,
                        "delivery_field": delivery_field,
                    }
                    for source_field, delivery_field in capability[
                        "delivery_fields"
                    ]
                ],
                "delivery_metadata_valid": capability[
                    "delivery_metadata_valid"
                ],
                "capability_ids": sorted(
                    str(row.get("capability_id")) for row in exact
                ),
                "terminal_result_present": terminal,
                "terminal_fields_present": bool(
                    exact_field_surface and all(field_terminal.values())
                ),
                "eligible_for_all_declared_fields": eligible,
                "field_terminal": field_terminal,
            }
        )
    return tuple(atomic)


def _phase_delivery_keys(
    atomic: Sequence[Mapping[str, Any]],
) -> frozenset[tuple[str, str]]:
    return frozenset(
        (str(metric_id), str(delivery["delivery_field"]))
        for capability in atomic
        for metric_id in capability["chain_metric_ids"]
        for delivery in capability["delivery_fields"]
    )


def _strategy_capability_relationship_valid(
    strategy: str,
    phases: Sequence[_PolicyPhase],
    atomic_by_phase: Mapping[str, Sequence[Mapping[str, Any]]],
) -> bool:
    provider_phases = [phase for phase in phases if phase.source_kind != "DB"]
    if not provider_phases:
        return False
    if strategy in {"FAN_IN", "CASCADE"}:
        return all(
            _phase_delivery_keys(atomic_by_phase.get(phase.phase_id, ()))
            for phase in provider_phases
        )
    if strategy != "FALLBACK":
        return False
    primary = next(
        (phase for phase in provider_phases if phase.source_kind == "PRIMARY"),
        None,
    )
    if primary is None:
        return False
    primary_keys = _phase_delivery_keys(
        atomic_by_phase.get(primary.phase_id, ())
    )
    if not primary_keys:
        return False
    return all(
        bool(
            primary_keys.intersection(
                _phase_delivery_keys(
                    atomic_by_phase.get(phase.phase_id, ())
                )
            )
        )
        for phase in provider_phases
        if phase.phase_id != primary.phase_id
    )


def _strategy_capability_chains(
    strategy: str,
    phases: Sequence[_PolicyPhase],
    atomic_by_phase: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    database_selected: bool,
) -> list[Mapping[str, Any]]:
    provider_phases = [phase for phase in phases if phase.source_kind != "DB"]
    primary = next(
        (phase for phase in provider_phases if phase.source_kind == "PRIMARY"),
        None,
    )
    primary_keys = (
        _phase_delivery_keys(atomic_by_phase.get(primary.phase_id, ()))
        if primary is not None
        else frozenset()
    )
    grouped: dict[
        tuple[str, str],
        list[tuple[_PolicyPhase, Mapping[str, Any], str]],
    ] = defaultdict(list)
    for phase in provider_phases:
        for capability in atomic_by_phase.get(phase.phase_id, ()):
            for metric_id in capability["chain_metric_ids"]:
                for delivery in capability["delivery_fields"]:
                    key = (
                        str(metric_id),
                        str(delivery["delivery_field"]),
                    )
                    if strategy == "FALLBACK" and key not in primary_keys:
                        continue
                    grouped[key].append(
                        (
                            phase,
                            capability,
                            str(delivery["source_field"]),
                        )
                    )

    output: list[Mapping[str, Any]] = []
    for (metric_id, delivery_field), sources in sorted(grouped.items()):
        chain_accounting: list[Mapping[str, Any]] = []
        fallback_selected = False
        for phase, capability, source_field in sources:
            field_terminal = bool(
                capability["field_terminal"].get(source_field)
            )
            eligible = bool(
                field_terminal
                and capability["eligible_for_all_declared_fields"]
            )
            selected = False
            if not database_selected and eligible:
                if strategy == "FALLBACK":
                    selected = not fallback_selected
                    fallback_selected = True
                elif strategy in {"FAN_IN", "CASCADE"}:
                    selected = True
            chain_accounting.append(
                {
                    "phase_id": phase.phase_id,
                    "phase_index": phase.phase_index,
                    "source_id": phase.source_id,
                    "source_kind": phase.source_kind,
                    "registered_metric_id": capability[
                        "registered_metric_id"
                    ],
                    "source_field": source_field,
                    "delivery_field": delivery_field,
                    "field_terminal_result_present": field_terminal,
                    "eligible_for_exact_capability": eligible,
                    "selected": selected,
                }
            )
        selected_rows = [
            item for item in chain_accounting if item["selected"] is True
        ]
        output.append(
            {
                "metric_id": metric_id,
                "field": delivery_field,
                "selection_mode": (
                    "FIRST_ELIGIBLE_EQUIVALENT"
                    if strategy == "FALLBACK"
                    else "ALL_ELIGIBLE_CONTRIBUTORS"
                    if strategy == "FAN_IN"
                    else "ORDERED_ELIGIBLE_DEPENDENCIES"
                ),
                "phase_order": [item["phase_id"] for item in chain_accounting],
                "source_order": [
                    item["source_id"] for item in chain_accounting
                ],
                "accounting": chain_accounting,
                "selected_eligible_phases": [
                    item["phase_id"] for item in selected_rows
                ],
                "selected_eligible_sources": [
                    item["source_id"] for item in selected_rows
                ],
                "selected_eligible_source": (
                    selected_rows[0]["source_id"]
                    if len(selected_rows) == 1
                    else None
                ),
                "selected_eligible_role": (
                    selected_rows[0]["source_kind"]
                    if len(selected_rows) == 1
                    else None
                ),
                "complete": bool(
                    chain_accounting
                    and all(
                        item["field_terminal_result_present"]
                        for item in chain_accounting
                    )
                ),
            }
        )
    return output


def verify_policy_fallback_chains(
    policies: Sequence[Any],
    capability_results: Sequence[Mapping[str, Any]],
    *,
    registry: Sequence[Any] | None = None,
) -> Mapping[str, Any]:
    """Verify canonical DB phases and strategy-aware atomic delivery relations."""

    result_rows = [
        dict(item) for item in capability_results if isinstance(item, Mapping)
    ]
    result_contract_valid = len(result_rows) == len(capability_results)
    rows: list[Mapping[str, Any]] = []
    for policy in policies:
        dataset_id = str(_value(policy, "dataset_id", "") or "").strip()
        strategy = str(
            _value(policy, "provider_strategy", "FALLBACK") or ""
        ).upper()
        phases = _policy_source_phases(policy)
        atomic_by_phase = {
            phase.phase_id: _phase_atomic_results(
                phase,
                dataset_id,
                registry=registry,
                result_rows=result_rows,
            )
            for phase in phases
        }
        accounting: list[dict[str, Any]] = []
        for phase in phases:
            atomic = atomic_by_phase[phase.phase_id]
            source_complete = bool(
                atomic
                and all(
                    item["terminal_result_present"] for item in atomic
                )
            )
            source_eligible = bool(
                atomic
                and all(
                    item["eligible_for_all_declared_fields"]
                    for item in atomic
                )
            )
            accounting.append(
                {
                    "phase_id": phase.phase_id,
                    "phase_index": phase.phase_index,
                    "source_id": phase.source_id,
                    "source_kind": phase.source_kind,
                    "capability_ids": sorted(
                        capability_id
                        for item in atomic
                        for capability_id in item["capability_ids"]
                    ),
                    "atomic_capabilities": list(atomic),
                    "terminal_results_present": source_complete,
                    "eligible_for_capability": source_eligible,
                    "selected_by_eligibility_simulation": False,
                }
            )
        database_phase = accounting[0] if accounting else None
        database_complete = bool(
            database_phase
            and database_phase["source_kind"] == "DB"
            and database_phase["terminal_results_present"] is True
        )
        database_selected = bool(
            database_complete
            and database_phase["eligible_for_capability"] is True
        )
        capability_relationship_valid = (
            _strategy_capability_relationship_valid(
                strategy,
                phases,
                atomic_by_phase,
            )
        )
        capability_chains = _strategy_capability_chains(
            strategy,
            phases,
            atomic_by_phase,
            database_selected=database_selected,
        )
        selected_phase_ids = {
            item["phase_id"]
            for chain in capability_chains
            for item in chain["accounting"]
            if item["selected"] is True
        }
        if database_selected and database_phase is not None:
            selected_phase_ids.add(str(database_phase["phase_id"]))
        for item in accounting:
            item["selected_by_eligibility_simulation"] = (
                item["phase_id"] in selected_phase_ids
            )
        selected_phases = [
            item for item in accounting if item["phase_id"] in selected_phase_ids
        ]
        try:
            _validate_fallback_observation_order(
                tuple(
                    FallbackObservation(
                        source_id=phase.source_id,
                        source_kind=phase.source_kind,
                        attempted=False,
                        valid=False,
                    )
                    for phase in phases
                )
            )
            order_valid = True
        except ValueError:
            order_valid = False
        ai_phase_present = any(
            phase.source_kind == "AI" for phase in phases
        )
        complete = bool(
            result_contract_valid
            and strategy in {"FALLBACK", "FAN_IN", "CASCADE"}
            and order_valid
            and database_complete
            and len(accounting) == len(phases)
            and all(item["terminal_results_present"] for item in accounting)
            and capability_relationship_valid
            and capability_chains
            and all(item["complete"] for item in capability_chains)
        )
        rows.append(
            {
                "dataset_id": dataset_id,
                "provider_strategy": strategy,
                "verification_mode": (
                    "STRATEGY_AWARE_ATOMIC_METRIC_FIELD_ELIGIBILITY"
                ),
                "phase_order": [phase.phase_id for phase in phases],
                "expected_order": [phase.source_id for phase in phases],
                "database_first": bool(
                    phases and phases[0].source_kind == "DB"
                ),
                "database_phase_complete": database_complete,
                "database_phase_selected": database_selected,
                "ai_phase_present": ai_phase_present,
                "ai_after_deterministic_fallbacks": (
                    order_valid if ai_phase_present else False
                ),
                "capability_relationship_valid": (
                    capability_relationship_valid
                ),
                "accounting": accounting,
                "capability_chains": capability_chains,
                "selected_eligible_phases": [
                    item["phase_id"] for item in selected_phases
                ],
                "selected_eligible_sources": [
                    item["source_id"] for item in selected_phases
                ],
                "selected_eligible_source": (
                    selected_phases[0]["source_id"]
                    if len(selected_phases) == 1
                    else None
                ),
                "selected_eligible_role": (
                    selected_phases[0]["source_kind"]
                    if len(selected_phases) == 1
                    else None
                ),
                "delivery_value": None,
                "delivery_reason_code": (
                    "DATABASE_CANONICAL_PHASE_SELECTED"
                    if database_selected
                    else "ALL_ELIGIBLE_CONTRIBUTORS_IDENTIFIED"
                    if strategy == "FAN_IN" and selected_phases
                    else "ORDERED_DEPENDENCIES_IDENTIFIED"
                    if strategy == "CASCADE" and selected_phases
                    else "ELIGIBLE_SOURCE_IDENTIFIED"
                    if len(selected_phases) == 1
                    else "CAPABILITY_SPECIFIC_SOURCES_IDENTIFIED"
                    if selected_phases
                    else "ALL_SOURCES_FAILED_OR_UNAVAILABLE"
                ),
                "complete": complete,
            }
        )
    complete_rows = sum(item["complete"] is True for item in rows)
    return {
        "mode": (
            "POLICY_CHAIN_STRATEGY_AWARE_ATOMIC_METRIC_FIELD_"
            "ACCOUNTING_NO_LIVE_FAULT_INJECTION"
        ),
        "policies_expected": len(policies),
        "policies_accounted": len(rows),
        "complete_rows": complete_rows,
        "coverage_pct": (
            round(100 * complete_rows / len(policies), 3)
            if policies
            else 100.0
        ),
        "complete": complete_rows == len(policies),
        "rows": rows,
    }


class DatabaseBundleGuard:
    """Copies an operational SQLite bundle without opening or mutating the source."""

    def __init__(self, source: Path | None, sandbox_root: Path) -> None:
        self.source = source.resolve() if source is not None else None
        self.sandbox_root = sandbox_root.resolve()
        self.snapshot_directory = self.sandbox_root / "database-snapshot"
        self.snapshot_path: Path | None = None
        self._before: dict[str, Mapping[str, Any]] = {}

    def create_snapshot(self) -> Path | None:
        if self.source is None or not self.source.is_file():
            return None
        self.snapshot_directory.mkdir(parents=True, exist_ok=True)
        source_files = _sqlite_bundle_files(self.source)
        before = _hash_files(source_files)
        copied: list[Path] = []
        for source_file in source_files:
            destination = self.snapshot_directory / source_file.name
            shutil.copyfile(source_file, destination)
            copied.append(destination)
        after = _hash_files(source_files)
        if before != after:
            raise RuntimeError("OPERATIONAL_DATABASE_CHANGED_DURING_SNAPSHOT")
        copied_hashes = _hash_files(copied)
        for source_file in source_files:
            source_entry = before[str(source_file)]
            copied_entry = copied_hashes[str(self.snapshot_directory / source_file.name)]
            if source_entry != copied_entry:
                raise RuntimeError("DATABASE_SNAPSHOT_HASH_MISMATCH")
        self._before = before
        self.snapshot_path = self.snapshot_directory / self.source.name
        return self.snapshot_path

    def source_unchanged(self) -> bool:
        if not self._before:
            return True
        assert self.source is not None
        return self._before == _hash_files(_sqlite_bundle_files(self.source))

    @property
    def source_manifest(self) -> Mapping[str, Any]:
        return {
            Path(path).name: value
            for path, value in sorted(self._before.items())
        }


class ProviderCapabilityAuditEngine:
    def __init__(
        self,
        registry: Sequence[Any],
        probe_executor: ProbeExecutor,
        *,
        settings: Any = None,
        source_policies: Sequence[Any] = (),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.registry = tuple(registry)
        self.probe_executor = probe_executor
        self.settings = settings
        self.source_policies = tuple(source_policies)
        self.clock = clock or utc_now

    async def run(
        self,
        *,
        filters: AuditFilters | None = None,
        run_id: str | None = None,
        sandbox_root: Path | None = None,
        database_snapshot_path: Path | None = None,
        artifact_writer: Any = None,
        registry_validation: Mapping[str, Any] | Sequence[str] | None = None,
        database_source_unchanged: Callable[[], bool] | None = None,
    ) -> AuditExecution:
        selected_filters = filters or AuditFilters()
        actual_run_id = run_id or self.clock().astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
        sandbox = (sandbox_root or Path("data/provider-capability-audit-sandbox")).resolve()
        sandbox.mkdir(parents=True, exist_ok=True)
        started_at = self.clock().astimezone(UTC)
        source_provenance = provider_audit_source_provenance().as_dict()
        validation_errors = _validation_errors(registry_validation)
        targets = select_capability_targets(
            self.registry,
            selected_filters,
            settings=self.settings,
        )
        requests = build_probe_requests(
            targets,
            run_id=actual_run_id,
            sandbox_root=sandbox,
            database_snapshot_path=database_snapshot_path,
            settings=self.settings,
        )
        runtime_adapter_requests = build_runtime_adapter_probe_requests(
            targets,
            requests,
            run_id=actual_run_id,
            sandbox_root=sandbox,
            database_snapshot_path=database_snapshot_path,
            settings=self.settings,
        )
        results: list[Mapping[str, Any]] = []
        acquisitions: list[Mapping[str, Any]] = []
        runtime_adapter_acquisitions: list[Mapping[str, Any]] = []
        runtime_adapter_results: list[Mapping[str, Any]] = []
        internal_errors: list[str] = []
        if not targets:
            internal_errors.append("NO_CAPABILITY_TARGETS_SELECTED")

        for request in requests:
            outcome = await self._execute(request)
            acquisition_record = dict(_acquisition_record(request, outcome))
            acquisition_record["artifact_bindings"] = []
            acquisition_record["artifact_binding_complete"] = None
            if artifact_writer is not None:
                try:
                    recorded = artifact_writer.record_acquisition(request, outcome)
                    acquisition_record["artifact_bindings"] = (
                        _validated_artifact_bindings(
                            recorded,
                            request=request,
                            outcome=outcome,
                        )
                    )
                    acquisition_record["artifact_binding_complete"] = True
                except Exception as exc:  # keep a terminal row, but fail publication
                    acquisition_record["artifact_binding_complete"] = False
                    internal_errors.append(
                        "ARTIFACT_ACQUISITION_WRITE_FAILED:"
                        f"{type(exc).__name__}"
                    )
            acquisitions.append(acquisition_record)
            for target in request.targets:
                results.append(evaluate_capability(target, outcome, acquisition_record))

        for request in runtime_adapter_requests:
            outcome = await self._execute(request)
            acquisition_record = dict(_acquisition_record(request, outcome))
            acquisition_record["artifact_bindings"] = []
            acquisition_record["artifact_binding_complete"] = None
            if artifact_writer is not None:
                try:
                    recorded = artifact_writer.record_acquisition(request, outcome)
                    acquisition_record["artifact_bindings"] = (
                        _validated_artifact_bindings(
                            recorded,
                            request=request,
                            outcome=outcome,
                        )
                    )
                    acquisition_record["artifact_binding_complete"] = True
                except Exception as exc:
                    acquisition_record["artifact_binding_complete"] = False
                    internal_errors.append(
                        "ARTIFACT_RUNTIME_ADAPTER_ACQUISITION_WRITE_FAILED:"
                        f"{type(exc).__name__}"
                    )
            runtime_adapter_acquisitions.append(acquisition_record)
            runtime_adapter_results.append(
                _runtime_adapter_capability_result(
                    request,
                    outcome,
                    acquisition_record,
                )
            )

        runtime_fallback_findings = (
            _uncertified_runtime_leaf_findings(targets)
        )

        expected_ids = {target.target_id for target in targets}
        actual_ids = {str(result["capability_id"]) for result in results}
        missing_rows = sorted(expected_ids - actual_ids)
        duplicate_rows = sorted(
            capability_id
            for capability_id, count in Counter(
                str(result["capability_id"]) for result in results
            ).items()
            if count != 1
        )
        if missing_rows:
            internal_errors.append("MISSING_TERMINAL_CAPABILITY_ROWS")
        if duplicate_rows:
            internal_errors.append("DUPLICATE_TERMINAL_CAPABILITY_ROWS")
        if validation_errors:
            internal_errors.append("PROVIDER_REGISTRY_INVALID")
        if database_source_unchanged is not None and not database_source_unchanged():
            internal_errors.append("OPERATIONAL_DATABASE_BUNDLE_CHANGED")
        unsupported_probes = sum(
            item.get("probe_dispatch_status") == "UNSUPPORTED"
            and item.get("transport_status") != HealthStatus.NOT_CONFIGURED.value
            for item in (*acquisitions, *runtime_adapter_acquisitions)
        )
        if unsupported_probes:
            internal_errors.append("UNSUPPORTED_CONFIGURED_PROBES")
        missing_real_dispatch: list[str] = []
        if getattr(self.probe_executor, "requires_real_dispatch", False):
            missing_real_dispatch = [
                str(item["acquisition_id"])
                for item in acquisitions
                if item.get("transport_status")
                != HealthStatus.NOT_CONFIGURED.value
                and not _terminal_audit_acquisition(item, targets)
                and (
                    item.get("probe_dispatch_status") != "REAL_ADAPTER"
                    or item.get("real_adapter_invoked") is not True
                )
            ]
            if missing_real_dispatch:
                internal_errors.append(
                    "CONFIGURED_PROBE_DISPATCH_NOT_OBSERVED"
                )
        runtime_adapter_coverage = _runtime_adapter_coverage(
            targets,
            (*acquisitions, *runtime_adapter_acquisitions),
        )
        if runtime_adapter_coverage["complete"] is not True:
            internal_errors.append("RUNTIME_ADAPTER_PROBE_COVERAGE_INCOMPLETE")
        runtime_adapter_quality = _runtime_adapter_quality(
            runtime_adapter_coverage,
            (*results, *runtime_adapter_results),
        )
        if runtime_adapter_quality["complete"] is not True:
            internal_errors.append(
                "RUNTIME_ADAPTER_FIELD_EVALUATION_INCOMPLETE"
            )
        fallback_chain_scope_applicable = bool(
            self.source_policies
            and not selected_filters.providers
            and not selected_filters.datasets
            and not selected_filters.metrics
            and selected_filters.include_ai
        )
        fallback_chain_verification = (
            verify_policy_fallback_chains(
                self.source_policies,
                results,
                registry=self.registry,
            )
            if fallback_chain_scope_applicable
            else {
                "mode": "NOT_APPLICABLE_FILTERED_OR_AI_EXCLUDED_SCOPE",
                "policies_expected": len(self.source_policies),
                "policies_accounted": 0,
                "complete_rows": 0,
                "coverage_pct": None,
                "complete": None,
                "rows": [],
            }
        )
        fallback_chain_verification = {
            **fallback_chain_verification,
            "scope_applicable": fallback_chain_scope_applicable,
        }
        if (
            fallback_chain_scope_applicable
            and fallback_chain_verification["complete"] is not True
        ):
            internal_errors.append("FALLBACK_CHAIN_ACCOUNTING_INCOMPLETE")

        completed_at = self.clock().astimezone(UTC)
        audit_status = (
            AuditStatus.COMPLETED
            if not internal_errors and len(results) == len(targets)
            else AuditStatus.FAILED
        )
        all_evaluations = (
            *results,
            *runtime_adapter_results,
            *runtime_fallback_findings,
        )
        counts = Counter(
            str(item["health_status"]).lower()
            for item in all_evaluations
        )
        real_adapter_probes = sum(
            item.get("real_adapter_invoked") is True
            for item in (*acquisitions, *runtime_adapter_acquisitions)
        )
        acquisition_artifact_bindings_complete = (
            all(
                item.get("artifact_binding_complete") is True
                for item in (*acquisitions, *runtime_adapter_acquisitions)
            )
            if artifact_writer is not None
            else None
        )
        system_health = determine_system_health(
            all_evaluations,
            audit_status=audit_status,
        )
        registry_sha256 = stable_sha256(
            [_public_registration(item) for item in self.registry]
        )
        full_audit_scope = (
            not selected_filters.providers
            and not selected_filters.datasets
            and not selected_filters.metrics
            and selected_filters.include_ai
            and len({target.provider_id for target in targets}) == len(self.registry)
            and len(results)
            == sum(len(_capabilities(provider)) for provider in self.registry)
        )
        report: dict[str, Any] = {
            "schema_version": "provider-capability-audit-v1",
            "run_id": actual_run_id,
            "audit_status": audit_status.value,
            "system_health": system_health.value,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration_ms": max(
                0,
                round((completed_at - started_at).total_seconds() * 1000),
            ),
            "filters": {
                "providers": list(selected_filters.providers),
                "datasets": list(selected_filters.datasets),
                "metrics": list(selected_filters.metrics),
                "include_ai": selected_filters.include_ai,
            },
            "registry_sha256": registry_sha256,
            "source_provenance": source_provenance,
            "providers_registered": len(self.registry),
            "providers_tested": len({target.provider_id for target in targets}),
            "capabilities_registered": sum(
                len(_capabilities(provider)) for provider in self.registry
            ),
            "capabilities_tested": len(results),
            "runtime_adapter_capabilities_tested": len(
                runtime_adapter_results
            ),
            "uncertified_runtime_leaves": len(
                runtime_fallback_findings
            ),
            "evaluations_tested_total": len(all_evaluations),
            "full_audit_scope": full_audit_scope,
            "acquisitions_executed": (
                len(acquisitions) + len(runtime_adapter_acquisitions)
            ),
            "capability_acquisitions_executed": len(acquisitions),
            "runtime_adapter_acquisitions_executed": len(
                runtime_adapter_acquisitions
            ),
            "acquisition_artifact_bindings_complete": (
                acquisition_artifact_bindings_complete
            ),
            "real_adapter_probes": real_adapter_probes,
            "unsupported_isolated_probes": unsupported_probes,
            "configured_dispatches_missing": missing_real_dispatch,
            "runtime_adapter_coverage": runtime_adapter_coverage,
            "runtime_adapter_quality": runtime_adapter_quality,
            "fallback_chain_accounting": fallback_chain_verification,
            "deduplicated_capability_count": max(0, len(results) - len(acquisitions)),
            "healthy": counts["healthy"],
            "degraded": counts["degraded"],
            "unusable": counts["unusable"],
            "down": counts["down"],
            "auth_failed": counts["auth_failed"],
            "rate_limited": counts["rate_limited"],
            "not_configured": counts["not_configured"],
            "unknown": counts["unknown"],
            "source_gaps": sum(
                1
                for item in all_evaluations
                if "SOURCE_GAP" in item["recommendations"]
            ),
            "terminal_rows_complete": not missing_rows and not duplicate_rows,
            "runtime_adapter_terminal_rows_complete": (
                len(runtime_adapter_results)
                == len(runtime_adapter_acquisitions)
            ),
            "runtime_leaf_findings_complete": all(
                item.get("terminal") is True
                for item in runtime_fallback_findings
            ),
            "missing_capability_rows": missing_rows,
            "duplicate_capability_rows": duplicate_rows,
            "registry_validation_errors": validation_errors,
            "internal_errors": sorted(set(internal_errors)),
            "quality_score_formula": quality_score_formula(),
            "results": results,
            "acquisitions": acquisitions,
            "runtime_adapter_acquisitions": runtime_adapter_acquisitions,
            "runtime_adapter_results": runtime_adapter_results,
            "runtime_leaf_findings": runtime_fallback_findings,
        }
        matrix = {
            "schema_version": "provider-capability-matrix-v1",
            "run_id": actual_run_id,
            "audit_status": audit_status.value,
            "registry_sha256": registry_sha256,
            "source_provenance": source_provenance,
            "generated_at": completed_at.isoformat(),
            "capabilities": results,
            "runtime_adapter_capabilities": runtime_adapter_results,
            "runtime_leaf_findings": runtime_fallback_findings,
        }

        artifact_directory: Path | None = None
        candidate_pointer: Path | None = None
        latest_pointer: Path | None = None
        if artifact_writer is not None:
            try:
                finalized = artifact_writer.finalize(report, matrix)
                artifact_directory = Path(finalized["run_directory"])
                if finalized.get("candidate_pointer"):
                    candidate_pointer = Path(finalized["candidate_pointer"])
                if finalized.get("latest_pointer"):
                    latest_pointer = Path(finalized["latest_pointer"])
            except Exception as exc:
                report["audit_status"] = AuditStatus.FAILED.value
                report["system_health"] = SystemHealth.UNKNOWN.value
                report["internal_errors"] = sorted(
                    {*report["internal_errors"], f"ARTIFACT_FINALIZATION_FAILED:{type(exc).__name__}"}
                )
                audit_status = AuditStatus.FAILED
                system_health = SystemHealth.UNKNOWN

        return AuditExecution(
            run_id=actual_run_id,
            audit_status=audit_status.value,
            system_health=system_health.value,
            report=report,
            capability_matrix=matrix,
            artifact_directory=artifact_directory,
            candidate_pointer=candidate_pointer,
            latest_pointer=latest_pointer,
        )

    async def _execute(self, request: ProbeRequest) -> ProbeOutcome:
        configuration_reasons = _configuration_reasons(
            request.registration,
            self.settings,
            request.targets,
        )
        if configuration_reasons:
            return ProbeOutcome.not_configured(*configuration_reasons)
        try:
            result = self.probe_executor(request)
            if inspect.isawaitable(result):
                result = await result
            if not isinstance(result, ProbeOutcome):
                return ProbeOutcome.unknown("PROBE_EXECUTOR_RETURNED_INVALID_TYPE")
            return _normalize_outcome(result, request)
        except ProbeExecutionError as exc:
            return ProbeOutcome(
                transport_status=exc.status.value,
                error_kind=type(exc).__name__,
                reason_codes=(exc.reason_code,),
                checks={name: None for name in _all_check_names()},
            )
        except TimeoutError:
            return ProbeOutcome(
                transport_status=HealthStatus.DOWN.value,
                error_kind="TimeoutError",
                reason_codes=("PROBE_TIMEOUT",),
                checks={name: None for name in _all_check_names()},
            )
        except OSError as exc:
            return ProbeOutcome(
                transport_status=HealthStatus.DOWN.value,
                error_kind=type(exc).__name__,
                reason_codes=("PROVIDER_TRANSPORT_FAILED",),
                checks={name: None for name in _all_check_names()},
            )
        except Exception as exc:
            return ProbeOutcome(
                transport_status=HealthStatus.UNKNOWN.value,
                error_kind=type(exc).__name__,
                reason_codes=("PROBE_EXECUTION_FAILED",),
                checks={name: None for name in _all_check_names()},
            )


def select_capability_targets(
    registry: Sequence[Any],
    filters: AuditFilters,
    *,
    settings: Any = None,
) -> tuple[CapabilityTarget, ...]:
    selected: list[CapabilityTarget] = []
    for registration in registry:
        provider_id = str(_value(registration, "provider_id", "")).strip()
        provider_type = _enum_value(_value(registration, "provider_type", "UNKNOWN"))
        if filters.providers and provider_id.casefold() not in filters.providers:
            continue
        if not filters.include_ai and _is_ai_provider(provider_type):
            continue
        for capability in _capabilities(registration):
            dataset_id = str(_value(capability, "dataset_id", "")).strip()
            metric_id = str(_value(capability, "metric_id", "")).strip()
            if filters.datasets and dataset_id.casefold() not in filters.datasets:
                continue
            if filters.metrics and metric_id.casefold() not in filters.metrics:
                continue
            fields = tuple(
                str(item)
                for item in (
                    _value(capability, "supported_fields", ())
                    or _value(capability, "fields", ())
                    or ()
                )
                if str(item).strip()
            )
            fields = tuple(
                dict.fromkeys(
                    (
                        *fields,
                        *(
                            str(item)
                            for item in (
                                _value(
                                    capability,
                                    "audit_only_fields",
                                    (),
                                )
                                or ()
                            )
                            if str(item).strip()
                        ),
                    )
                )
            )
            if not fields:
                fields = ("value",)
            timeout_setting = (
                _value(capability, "timeout", None)
                or _value(registration, "timeout", None)
                or _value(registration, "timeout_seconds", None)
            )
            if isinstance(timeout_setting, str):
                configured_timeout = _setting_value(settings, timeout_setting)
            else:
                configured_timeout = timeout_setting
            timeout = _positive_float(configured_timeout, default=15.0)
            attempts = _positive_int(
                _value(capability, "max_attempts", None)
                or _value(registration, "max_attempts", None),
                default=1,
            )
            selected.append(
                CapabilityTarget(
                    provider_id=provider_id,
                    provider_type=provider_type,
                    dataset_id=dataset_id,
                    metric_id=metric_id,
                    fields=fields,
                    frequency=_optional_text(_value(capability, "frequency", None)),
                    transformation=_optional_text(
                        _value(capability, "transformation", None)
                    ),
                    probe_id=_optional_text(
                        _value(capability, "probe_id", None)
                        or _value(registration, "probe_id", None)
                    ),
                    field_validator_id=_optional_text(
                        _value(capability, "field_validator_id", None)
                    ),
                    probe_adapter_path=_optional_text(
                        _value(capability, "probe_adapter_path", None)
                    ),
                    request_group=_optional_text(
                        _value(capability, "request_group", None)
                        or _value(registration, "request_group", None)
                    ),
                    allowed_roles=tuple(
                        _enum_value(role).upper()
                        for role in (_value(registration, "allowed_roles", ()) or ())
                    ),
                    timeout_seconds=timeout,
                    max_attempts=attempts,
                    degradable_quality_checks=tuple(
                        str(item)
                        for item in (
                            _value(
                                capability,
                                "degradable_quality_checks",
                                (),
                            )
                            or ()
                        )
                        if str(item) in DEGRADABLE_QUALITY_CHECKS
                    ),
                    registration=registration,
                    capability=capability,
                )
            )
    return tuple(
        sorted(
            selected,
            key=lambda item: (item.provider_id, item.dataset_id, item.metric_id),
        )
    )


def build_probe_requests(
    targets: Sequence[CapabilityTarget],
    *,
    run_id: str,
    sandbox_root: Path,
    database_snapshot_path: Path | None,
    settings: Any,
) -> tuple[ProbeRequest, ...]:
    grouped: dict[tuple[str, str], list[CapabilityTarget]] = defaultdict(list)
    for target in targets:
        if target.request_group:
            grouping = f"explicit:{target.probe_id or 'no-probe'}:{target.request_group}"
        else:
            grouping = f"isolated:{target.dataset_id}:{target.metric_id}"
        grouped[(target.provider_id, grouping)].append(target)
    requests: list[ProbeRequest] = []
    for index, ((provider_id, grouping), grouped_targets) in enumerate(
        sorted(grouped.items()),
        start=1,
    ):
        request_material = {
            "run_id": run_id,
            "provider_id": provider_id,
            "grouping": grouping,
            "targets": [
                {
                    "dataset_id": item.dataset_id,
                    "metric_id": item.metric_id,
                    "fields": list(item.fields),
                    "frequency": item.frequency,
                    "transformation": item.transformation,
                    "probe_adapter_path": item.probe_adapter_path,
                    "field_validator_id": item.field_validator_id,
                    "degradable_quality_checks": list(
                        item.degradable_quality_checks
                    ),
                }
                for item in grouped_targets
            ],
        }
        request_key = stable_sha256(request_material)
        request_correlations = {
            target.target_id: _request_correlation(
                target,
                request_key,
                run_id=run_id,
            )
            for target in grouped_targets
        }
        requests.append(
            ProbeRequest(
                acquisition_id=f"acq-{index:04d}-{request_key[:12]}",
                request_key=request_key,
                run_id=run_id,
                targets=tuple(grouped_targets),
                sandbox_root=sandbox_root,
                database_snapshot_path=database_snapshot_path,
                settings=settings,
                request_correlations=request_correlations,
            )
        )
    return tuple(requests)


def build_runtime_adapter_probe_requests(
    targets: Sequence[CapabilityTarget],
    capability_requests: Sequence[ProbeRequest],
    *,
    run_id: str,
    sandbox_root: Path,
    database_snapshot_path: Path | None,
    settings: Any,
) -> tuple[ProbeRequest, ...]:
    """Create one adapter-path-bound probe for every uncovered runtime adapter.

    Capability requests remain the only source of capability results. These
    extra requests exist solely to prove that every adapter class constructed
    by the application runtime receives a terminal, adapter-specific audit
    acquisition. A provider-level row is deliberately insufficient evidence.
    """

    targets_by_provider: dict[str, list[CapabilityTarget]] = defaultdict(list)
    for target in targets:
        targets_by_provider[target.provider_id].append(target)
    covered_paths: dict[str, set[str]] = defaultdict(set)
    for request in capability_requests:
        covered_paths[request.provider_id].add(request.adapter_path)

    requests: list[ProbeRequest] = []
    for provider_id in sorted(targets_by_provider):
        provider_targets = sorted(
            targets_by_provider[provider_id],
            key=lambda item: (item.dataset_id, item.metric_id),
        )
        registration = provider_targets[0].registration
        if not bool(_value(registration, "runtime_adapter", False)):
            continue
        runtime_paths = tuple(
            dict.fromkeys(
                str(path).strip()
                for path in (
                    _value(registration, "adapter_path", ""),
                    *(
                        _value(
                            registration,
                            "additional_adapter_paths",
                            (),
                        )
                        or ()
                    ),
                )
                if str(path).strip()
            )
        )
        representative = provider_targets[0]
        for adapter_path in runtime_paths:
            if adapter_path in covered_paths[provider_id]:
                continue
            request_material = {
                "run_id": run_id,
                "provider_id": provider_id,
                "grouping": f"runtime-adapter:{adapter_path}",
                "adapter_path": adapter_path,
                "targets": [
                    {
                        "dataset_id": representative.dataset_id,
                        "metric_id": representative.metric_id,
                        "fields": list(representative.fields),
                        "frequency": representative.frequency,
                        "transformation": representative.transformation,
                        "probe_adapter_path": representative.probe_adapter_path,
                        "field_validator_id": representative.field_validator_id,
                        "degradable_quality_checks": list(
                            representative.degradable_quality_checks
                        ),
                    }
                ],
            }
            request_key = stable_sha256(request_material)
            requests.append(
                ProbeRequest(
                    acquisition_id=(
                        f"acq-runtime-{len(requests) + 1:04d}-"
                        f"{request_key[:12]}"
                    ),
                    request_key=request_key,
                    run_id=run_id,
                    targets=(representative,),
                    sandbox_root=sandbox_root,
                    database_snapshot_path=database_snapshot_path,
                    settings=settings,
                    request_correlations={
                        representative.target_id: _request_correlation(
                            representative,
                            request_key,
                            run_id=run_id,
                        )
                    },
                    acquisition_kind="RUNTIME_ADAPTER_COVERAGE",
                    adapter_path_override=adapter_path,
                )
            )
    return tuple(requests)


def _runtime_adapter_coverage(
    targets: Sequence[CapabilityTarget],
    acquisitions: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    expected: dict[tuple[str, str], Any] = {}
    for target in targets:
        registration = target.registration
        if not bool(_value(registration, "runtime_adapter", False)):
            continue
        for path in (
            _value(registration, "adapter_path", ""),
            *(
                _value(registration, "additional_adapter_paths", (),)
                or ()
            ),
        ):
            normalized = str(path).strip()
            if normalized:
                expected[(target.provider_id, normalized)] = registration

    rows: list[Mapping[str, Any]] = []
    for provider_id, adapter_path in sorted(expected):
        matches = [
            item
            for item in acquisitions
            if item.get("provider_id") == provider_id
            and adapter_path in (item.get("probe_adapter_paths") or ())
        ]
        configured_dispatches = [
            item
            for item in matches
            if item.get("configured") is True
            and item.get("real_adapter_invoked") is True
            and item.get("probe_dispatch_status") == "REAL_ADAPTER"
            and item.get("observed_adapter_path") == adapter_path
        ]
        not_configured = [
            item
            for item in matches
            if item.get("configured") is False
            and item.get("transport_status")
            == HealthStatus.NOT_CONFIGURED.value
            and item.get("real_adapter_invoked") is not True
            and item.get("probe_dispatch_status")
            in {None, "NOT_CONFIGURED"}
        ]
        terminal_reason = str(
            _value(registration, "terminal_audit_reason", None) or ""
        ).strip()
        terminal_unsupported = [
            item
            for item in matches
            if terminal_reason
            and item.get("configured") is True
            and item.get("transport_status") == HealthStatus.UNUSABLE.value
            and item.get("real_adapter_invoked") is False
            and item.get("probe_dispatch_status")
            == "TERMINAL_UNSUPPORTED"
            and item.get("attempts") == 0
            and item.get("network_exchange_count") == 0
            and item.get("reason_codes") == [terminal_reason]
        ]
        complete = bool(
            configured_dispatches or not_configured or terminal_unsupported
        )
        rows.append(
            {
                "provider_id": provider_id,
                "adapter_path": adapter_path,
                "acquisition_ids": sorted(
                    str(item.get("acquisition_id")) for item in matches
                ),
                "configured_dispatch_observed": bool(configured_dispatches),
                "not_configured_observed": bool(not_configured),
                "terminal_unsupported_observed": bool(
                    terminal_unsupported
                ),
                "complete": complete,
            }
        )
    complete_count = sum(item["complete"] is True for item in rows)
    return {
        "mode": "RUNTIME_ADAPTER_PATH_BOUND_ACQUISITION",
        "adapters_expected": len(rows),
        "adapter_bindings_expected": len(rows),
        "unique_adapter_paths_expected": len(
            {item["adapter_path"] for item in rows}
        ),
        "adapters_accounted": complete_count,
        "coverage_pct": (
            round(100 * complete_count / len(rows), 3)
            if rows
            else 100.0
        ),
        "complete": complete_count == len(rows),
        "missing_adapter_paths": [
            item["adapter_path"] for item in rows if item["complete"] is not True
        ],
        "rows": rows,
    }


def _terminal_audit_acquisition(
    acquisition: Mapping[str, Any],
    targets: Sequence[CapabilityTarget],
) -> bool:
    target_ids = {
        str(item) for item in acquisition.get("target_ids") or ()
    }
    matching = [
        target for target in targets if target.target_id in target_ids
    ]
    if not matching:
        return False
    reasons = {
        str(
            _value(target.registration, "terminal_audit_reason", None)
            or ""
        ).strip()
        for target in matching
    }
    if len(reasons) != 1:
        return False
    reason = next(iter(reasons))
    return bool(
        reason
        and acquisition.get("configured") is True
        and acquisition.get("transport_status")
        == HealthStatus.UNUSABLE.value
        and acquisition.get("attempts") == 0
        and acquisition.get("network_exchange_count") == 0
        and acquisition.get("real_adapter_invoked") is False
        and acquisition.get("probe_dispatch_status")
        == "TERMINAL_UNSUPPORTED"
        and acquisition.get("reason_codes") == [reason]
    )


def _runtime_adapter_capability_result(
    request: ProbeRequest,
    outcome: ProbeOutcome,
    acquisition_record: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Evaluate an uncovered runtime adapter as a real field-level target.

    The adapter override is part of the target used to bind field evidence;
    construction/dispatch coverage alone never supplies quality evidence.
    """

    target = replace(
        request.targets[0],
        probe_adapter_path=request.adapter_path,
    )
    result = dict(
        evaluate_capability(target, outcome, acquisition_record)
    )
    base_capability_id = target.target_id
    adapter_token = stable_sha256(request.adapter_path)[:16]
    result.update(
        {
            "capability_id": (
                f"{base_capability_id}|runtime_adapter|{adapter_token}"
            ),
            "base_capability_id": base_capability_id,
            "evaluation_kind": "RUNTIME_ADAPTER_CAPABILITY",
            "runtime_adapter_path": request.adapter_path,
        }
    )
    return result


def _runtime_adapter_quality(
    coverage: Mapping[str, Any],
    evaluations: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    rows: list[Mapping[str, Any]] = []
    for raw_row in coverage.get("rows") or ():
        if not isinstance(raw_row, Mapping):
            continue
        acquisition_ids = {
            str(item)
            for item in raw_row.get("acquisition_ids") or ()
            if str(item)
        }
        matched = [
            item
            for item in evaluations
            if str(item.get("acquisition_id") or "")
            in acquisition_ids
        ]
        terminal = bool(matched) and all(
            str(item.get("health_status") or "")
            in TERMINAL_HEALTH_STATUSES
            for item in matched
        )
        health = (
            max(
                (
                    HealthStatus(str(item["health_status"]))
                    for item in matched
                ),
                key=_health_severity,
            )
            if terminal
            else HealthStatus.UNKNOWN
        )
        quality_score = (
            min(int(item.get("quality_score") or 0) for item in matched)
            if matched
            else 0
        )
        certified = bool(
            terminal
            and health in {HealthStatus.HEALTHY, HealthStatus.DEGRADED}
            and all(
                item.get("checks", {}).get(check) is True
                for item in matched
                for check in FATAL_QUALITY_CHECKS
            )
        )
        rows.append(
            {
                "provider_id": raw_row.get("provider_id"),
                "adapter_path": raw_row.get("adapter_path"),
                "evaluation_ids": sorted(
                    str(item.get("capability_id") or "")
                    for item in matched
                ),
                "terminal_field_evaluation": terminal,
                "health_status": health.value,
                "quality_score": quality_score,
                "quality_certified": certified,
                "semantic_mapping_valid": _aggregate_evaluation_check(
                    matched,
                    "semantic_mapping_valid",
                ),
                "occurrence_match_valid": _aggregate_evaluation_check(
                    matched,
                    "occurrence_match_valid",
                ),
                "lineage_valid": _aggregate_evaluation_check(
                    matched,
                    "lineage_valid",
                ),
                "recommendations": sorted(
                    {
                        recommendation
                        for item in matched
                        for recommendation in (
                            item.get("recommendations") or ()
                        )
                    }
                ),
            }
        )
    complete_count = sum(
        item["terminal_field_evaluation"] is True for item in rows
    )
    certified_count = sum(
        item["quality_certified"] is True for item in rows
    )
    return {
        "mode": "RUNTIME_ADAPTER_FIELD_LEVEL_EVALUATION",
        "adapters_expected": len(rows),
        "adapters_evaluated": complete_count,
        "adapters_quality_certified": certified_count,
        "evaluation_coverage_pct": (
            round(100 * complete_count / len(rows), 3)
            if rows
            else 100.0
        ),
        "quality_certified_pct": (
            round(100 * certified_count / len(rows), 3)
            if rows
            else 100.0
        ),
        "complete": complete_count == len(rows),
        "all_quality_certified": certified_count == len(rows),
        "rows": rows,
    }


def _aggregate_evaluation_check(
    evaluations: Sequence[Mapping[str, Any]],
    check: str,
) -> bool | None:
    values = [
        item.get("checks", {}).get(check)
        for item in evaluations
    ]
    if not values or any(value is None for value in values):
        return None
    return all(value is True for value in values)


def _uncertified_runtime_leaf_findings(
    targets: Sequence[CapabilityTarget],
) -> list[Mapping[str, Any]]:
    registrations: dict[str, Any] = {}
    for target in targets:
        registrations[target.provider_id] = target.registration
    findings: list[Mapping[str, Any]] = []
    for provider_id, registration in sorted(registrations.items()):
        leaves = tuple(
            str(item).strip()
            for item in (
                _value(
                    registration,
                    "uncertified_runtime_leaves",
                    (),
                )
                or ()
            )
            if str(item).strip()
        )
        for leaf in leaves:
            findings.append(
                {
                    "finding_id": (
                        f"{provider_id}|runtime_leaf|"
                        f"{stable_sha256(leaf)[:16]}"
                    ),
                    "evaluation_kind": (
                        "UNCERTIFIED_RUNTIME_LEAF"
                    ),
                    "provider_id": provider_id,
                    "runtime_leaf": leaf,
                    "health_status": HealthStatus.UNUSABLE.value,
                    "quality_score": 0,
                    "checks": {
                        name: None for name in _all_check_names()
                    },
                    "eligible_as_primary": False,
                    "eligible_as_fallback": False,
                    "reason_codes": [
                        "RUNTIME_LEAF_NOT_CAPABILITY_CERTIFIED"
                    ],
                    "recommendations": recommendations_for_result(
                        HealthStatus.UNUSABLE,
                        roles={
                            _enum_value(item).upper()
                            for item in (
                                _value(
                                    registration,
                                    "allowed_roles",
                                    (),
                                )
                                or ()
                            )
                        },
                        eligible_as_primary=False,
                        eligible_as_fallback=False,
                    ),
                    "terminal": True,
                }
            )
    return findings


def _request_correlation(
    target: CapabilityTarget,
    request_key: str,
    *,
    run_id: str,
) -> Mapping[str, Any]:
    """Describe only identity/occurrence constraints actually sent by the probe.

    Every probe is bound to its registered provider, target and immutable request
    key. AI probes additionally receive a synthetic occurrence identifier derived
    from that request key; the dispatcher must send this exact value and the
    response must echo it before occurrence evidence can certify the field.
    """

    correlation: dict[str, Any] = {
        "request_key": request_key,
        "provider_id": target.provider_id,
        "target_id": target.target_id,
        "dataset_id": target.dataset_id,
        "metric_id": target.metric_id,
        "correlation_kind": "TARGET_REQUEST",
        "expected_occurrence_id": None,
        "expected_reference_period": None,
    }
    if _is_ai_provider(target.provider_type):
        correlation.update(
            {
                "correlation_kind": "SYNTHETIC_OCCURRENCE",
                "expected_occurrence_id": (
                    "provider-audit:"
                    f"{target.provider_id}:{target.dataset_id}:"
                    f"{target.metric_id}:{request_key[:16]}"
                ),
                "expected_reference_period": _audit_reference_period(
                    run_id
                ),
            }
        )
    return correlation


def _audit_reference_period(run_id: str) -> str:
    match = re.match(
        r"((?:19|20)\d{2})(\d{2})(\d{2})",
        str(run_id or ""),
    )
    if match:
        return "-".join(match.groups())
    return "provider-audit-request"


def _recomputed_acquisition_request_key(
    acquisition: Mapping[str, Any],
    registration: Any | None,
) -> str | None:
    if registration is None:
        return None
    target_ids = acquisition.get("target_ids")
    run_id = str(acquisition.get("run_id") or "").strip()
    provider_id = str(acquisition.get("provider_id") or "").strip()
    if (
        not isinstance(target_ids, list)
        or not target_ids
        or not run_id
        or not provider_id
    ):
        return None
    registered_targets = {
        target.target_id: target
        for target in select_capability_targets(
            (registration,),
            AuditFilters(),
        )
    }
    if any(target_id not in registered_targets for target_id in target_ids):
        return None
    targets = [registered_targets[target_id] for target_id in target_ids]
    if acquisition.get("acquisition_kind") == "RUNTIME_ADAPTER_COVERAGE":
        if len(targets) != 1:
            return None
        adapter_paths = acquisition.get("probe_adapter_paths")
        if not isinstance(adapter_paths, list) or len(adapter_paths) != 1:
            return None
        adapter_path = str(adapter_paths[0] or "").strip()
        registered_runtime_paths = {
            str(path).strip()
            for path in (
                _value(registration, "adapter_path", ""),
                *(
                    _value(
                        registration,
                        "additional_adapter_paths",
                        (),
                    )
                    or ()
                ),
            )
            if str(path).strip()
        }
        if (
            not bool(_value(registration, "runtime_adapter", False))
            or adapter_path not in registered_runtime_paths
        ):
            return None
        item = targets[0]
        return stable_sha256(
            {
                "run_id": run_id,
                "provider_id": provider_id,
                "grouping": f"runtime-adapter:{adapter_path}",
                "adapter_path": adapter_path,
                "targets": [
                    {
                        "dataset_id": item.dataset_id,
                        "metric_id": item.metric_id,
                        "fields": list(item.fields),
                        "frequency": item.frequency,
                        "transformation": item.transformation,
                        "probe_adapter_path": item.probe_adapter_path,
                        "field_validator_id": item.field_validator_id,
                        "degradable_quality_checks": list(
                            item.degradable_quality_checks
                        ),
                    }
                ],
            }
        )
    first = targets[0]
    grouping = (
        f"explicit:{first.probe_id or 'no-probe'}:{first.request_group}"
        if first.request_group
        else f"isolated:{first.dataset_id}:{first.metric_id}"
    )
    if any(
        target.provider_id != provider_id
        or (
            (
                f"explicit:{target.probe_id or 'no-probe'}:"
                f"{target.request_group}"
            )
            if target.request_group
            else f"isolated:{target.dataset_id}:{target.metric_id}"
        )
        != grouping
        for target in targets
    ):
        return None
    request_material = {
        "run_id": run_id,
        "provider_id": provider_id,
        "grouping": grouping,
        "targets": [
            {
                "dataset_id": item.dataset_id,
                "metric_id": item.metric_id,
                "fields": list(item.fields),
                "frequency": item.frequency,
                "transformation": item.transformation,
                "probe_adapter_path": item.probe_adapter_path,
                "field_validator_id": item.field_validator_id,
                "degradable_quality_checks": list(
                    item.degradable_quality_checks
                ),
            }
            for item in targets
        ],
    }
    return stable_sha256(request_material)


def evaluate_capability(
    target: CapabilityTarget,
    outcome: ProbeOutcome,
    acquisition_record: Mapping[str, Any],
) -> Mapping[str, Any]:
    field_results: dict[str, Mapping[str, Any]] = {}
    for field_name in target.fields:
        checks = _checks_for_field(target, field_name, outcome)
        score_components = _score_components(checks)
        quality_score = sum(int(item["earned"]) for item in score_components.values())
        health, reasons = _health_for_field(
            outcome,
            checks,
            quality_score,
            degradable_checks=target.degradable_quality_checks,
        )
        ai_reasons = _ai_evidence_reason_codes(target, field_name, outcome)
        field_results[field_name] = {
            "health_status": health.value,
            "quality_score": quality_score,
            "score_components": score_components,
            "checks": checks,
            "reason_codes": sorted(
                set((*outcome.reason_codes, *reasons, *ai_reasons))
            ),
            "evidence": _bound_field_evidence(
                target,
                field_name,
                outcome,
                acquisition_record,
                checks,
            ),
        }
    field_statuses = [
        HealthStatus(str(result["health_status"])) for result in field_results.values()
    ]
    health = max(field_statuses, key=_health_severity)
    score = min(int(result["quality_score"]) for result in field_results.values())
    all_checks = {
        check: all(
            result["checks"].get(check) is True for result in field_results.values()
        )
        if all(result["checks"].get(check) is not None for result in field_results.values())
        else None
        for check in _all_check_names()
    }
    reason_codes = sorted(
        {
            reason
            for result in field_results.values()
            for reason in result["reason_codes"]
        }
    )
    roles = {role.upper() for role in target.allowed_roles}
    primary_allowed = not roles or bool(roles & PRIMARY_ROLES)
    fallback_allowed = not roles or bool(roles & FALLBACK_ROLES)
    eligible_as_primary = (
        health is HealthStatus.HEALTHY
        and primary_allowed
        and all(all_checks.get(check) is True for check in CORRECTNESS_CHECKS)
    )
    eligible_as_fallback = (
        health in {HealthStatus.HEALTHY, HealthStatus.DEGRADED}
        and fallback_allowed
        and all(all_checks.get(check) is True for check in CORRECTNESS_CHECKS)
    )
    recommendations = recommendations_for_result(
        health,
        roles=roles,
        eligible_as_primary=eligible_as_primary,
        eligible_as_fallback=eligible_as_fallback,
    )
    return {
        "capability_id": target.target_id,
        "provider_id": target.provider_id,
        "provider_type": target.provider_type,
        "dataset_id": target.dataset_id,
        "metric_id": target.metric_id,
        "supported_fields": list(
            _value(
                target.capability,
                "supported_fields",
                target.fields,
            )
            or ()
        ),
        "audit_only_fields": list(
            _value(target.capability, "audit_only_fields", ()) or ()
        ),
        "audited_fields": list(target.fields),
        "frequency": target.frequency,
        "transformation": target.transformation,
        "probe_id": target.probe_id,
        "field_validator_id": target.field_validator_id,
        "probe_adapter_path": target.probe_adapter_path
        or str(_value(target.registration, "adapter_path", "")).strip(),
        "request_group": target.request_group,
        "acquisition_id": acquisition_record["acquisition_id"],
        "request_key": acquisition_record["request_key"],
        "field_results": field_results,
        "configured": outcome.configured,
        "transport_status": outcome.transport_status,
        "http_status": outcome.http_status,
        "attempts": outcome.attempts,
        "latency_ms": outcome.latency_ms,
        "checks": all_checks,
        "schema_valid": all_checks["schema_valid"],
        "freshness_valid": all_checks["freshness_valid"],
        "semantic_mapping_valid": all_checks["semantic_mapping_valid"],
        "occurrence_match_valid": all_checks["occurrence_match_valid"],
        "lineage_valid": all_checks["lineage_valid"],
        "quality_score": score,
        "health_status": health.value,
        "eligible_as_primary": eligible_as_primary,
        "eligible_as_fallback": eligible_as_fallback,
        "observed_reason_codes": sorted(set(outcome.reason_codes)),
        "reason_codes": reason_codes,
        "recommendations": recommendations,
        "checked_at": outcome.checked_at,
        "real_adapter_invoked": outcome.evidence.get("real_adapter_invoked")
        if isinstance(outcome.evidence, Mapping)
        else None,
        "probe_dispatch_status": outcome.evidence.get("probe_dispatch_status")
        if isinstance(outcome.evidence, Mapping)
        else None,
    }


def validate_capability_result_derivations(
    row: Mapping[str, Any],
    target: Any,
    registration: Any | None = None,
    *,
    require_bound_evidence: bool = False,
    normalized_response: Any = _NORMALIZED_RESPONSE_NOT_SUPPLIED,
    acquisition: Mapping[str, Any] | None = None,
    artifact_root: Path | None = None,
) -> tuple[str, ...]:
    """Recompute every derived result field from persisted observations."""

    errors: list[str] = []
    terminal_audit_reason = str(
        _value(registration, "terminal_audit_reason", None) or ""
    ).strip()
    expected_supported_fields = tuple(
        str(item)
        for item in (
            _value(target, "supported_fields", ()) or ()
        )
    )
    expected_audit_only_fields = tuple(
        str(item)
        for item in (
            _value(target, "audit_only_fields", ()) or ()
        )
    )
    expected_fields = tuple(
        str(item)
        for item in (
            _value(target, "fields", None)
            or (
                *expected_supported_fields,
                *expected_audit_only_fields,
            )
            or ()
        )
    )
    if list(row.get("supported_fields") or []) != list(
        expected_supported_fields or expected_fields
    ):
        errors.append("SUPPORTED_FIELDS_MISMATCH")
    if list(row.get("audit_only_fields") or []) != list(
        expected_audit_only_fields
    ):
        errors.append("AUDIT_ONLY_FIELDS_MISMATCH")
    if list(row.get("audited_fields") or []) != list(expected_fields):
        errors.append("AUDITED_FIELDS_MISMATCH")
    provider_type = str(
        _value(target, "provider_type", None)
        or _value(registration, "provider_type", None)
        or row.get("provider_type")
        or ""
    )
    roles = {
        str(item).upper()
        for item in (
            _value(target, "allowed_roles", None)
            or _value(registration, "allowed_roles", ())
            or ()
        )
    }
    configured = row.get("configured")
    if type(configured) is not bool:
        errors.append("CONFIGURED_OBSERVATION_INVALID")
        configured = False
    transport_status = str(row.get("transport_status") or "")
    observed_reasons_raw = row.get("observed_reason_codes")
    if (
        not isinstance(observed_reasons_raw, list)
        or any(not isinstance(item, str) for item in observed_reasons_raw)
    ):
        errors.append("OBSERVED_REASON_CODES_INVALID")
        observed_reasons: list[str] = []
    else:
        observed_reasons = sorted(set(observed_reasons_raw))
        if observed_reasons_raw != observed_reasons:
            errors.append("OBSERVED_REASON_CODES_NOT_CANONICAL")
    outcome = ProbeOutcome(
        configured=bool(configured),
        transport_status=transport_status,
        http_status=(
            row.get("http_status")
            if type(row.get("http_status")) is int
            else None
        ),
        reason_codes=tuple(observed_reasons),
    )
    terminal_outcome_expected = bool(
        terminal_audit_reason and configured is True
    )
    if terminal_outcome_expected and (
        transport_status != HealthStatus.UNUSABLE.value
        or row.get("attempts") != 0
        or row.get("http_status") is not None
        or row.get("real_adapter_invoked") is not False
        or row.get("probe_dispatch_status") != "TERMINAL_UNSUPPORTED"
        or observed_reasons != [terminal_audit_reason]
        or row.get("health_status") != HealthStatus.UNUSABLE.value
        or row.get("quality_score") != 0
        or row.get("eligible_as_primary") is not False
        or row.get("eligible_as_fallback") is not False
        or row.get("reason_codes") != [terminal_audit_reason]
        or not isinstance(row.get("checks"), Mapping)
        or any(
            value is not None
            for value in (row.get("checks") or {}).values()
        )
        or (
            isinstance(acquisition, Mapping)
            and (
                acquisition.get("network_exchange_count") != 0
                or acquisition.get("raw_response_sha256") is not None
            )
        )
    ):
        errors.append("TERMINAL_AUDIT_DECLARATION_MISMATCH")
    if terminal_audit_reason and configured is False and (
        transport_status != HealthStatus.NOT_CONFIGURED.value
        or row.get("probe_dispatch_status")
        == "TERMINAL_UNSUPPORTED"
        or row.get("real_adapter_invoked") is True
    ):
        errors.append("TERMINAL_AUDIT_DECLARATION_MISMATCH")
    field_results = row.get("field_results")
    if (
        not isinstance(field_results, Mapping)
        or set(field_results) != set(expected_fields)
    ):
        return tuple(sorted({*errors, "FIELD_RESULT_SET_MISMATCH"}))

    derived_fields: dict[str, Mapping[str, Any]] = {}
    check_names = set(_all_check_names())
    for field_name in expected_fields:
        field_result = field_results.get(field_name)
        prefix = f"{field_name}:"
        if not isinstance(field_result, Mapping):
            errors.append(f"{prefix}FIELD_RESULT_INVALID")
            continue
        checks = field_result.get("checks")
        if (
            not isinstance(checks, Mapping)
            or set(checks) != check_names
            or any(
                value is not None and type(value) is not bool
                for value in checks.values()
            )
        ):
            errors.append(f"{prefix}FIELD_CHECKS_INVALID")
            continue
        normalized_checks = {
            name: checks.get(name) for name in _all_check_names()
        }
        if terminal_outcome_expected and (
            any(value is not None for value in normalized_checks.values())
            or field_result.get("quality_score") != 0
            or field_result.get("health_status")
            != HealthStatus.UNUSABLE.value
            or field_result.get("reason_codes")
            != [terminal_audit_reason]
            or not isinstance(field_result.get("score_components"), Mapping)
            or any(
                not isinstance(component, Mapping)
                or component.get("earned") != 0
                or component.get("observed") is not None
                for component in (
                    field_result.get("score_components") or {}
                ).values()
            )
        ):
            errors.append("TERMINAL_AUDIT_DECLARATION_MISMATCH")
        if normalized_checks["transport_valid"] != _transport_valid(outcome):
            errors.append(f"{prefix}TRANSPORT_CHECK_NOT_DERIVED")
        evidence_raw = field_result.get("evidence")
        evidence = evidence_raw if isinstance(evidence_raw, Mapping) else {}
        if require_bound_evidence:
            errors.extend(
                _bound_field_evidence_errors(
                    row=row,
                    target=target,
                    registration=registration,
                    field_name=field_name,
                    evidence=evidence,
                    checks=normalized_checks,
                    normalized_response=normalized_response,
                    acquisition=acquisition,
                    artifact_root=artifact_root,
                )
            )
        if (
            _is_ai_provider(provider_type)
            and normalized_checks["transport_valid"] is True
        ):
            ai_evidence = _source_field_evidence(evidence)
            explicit_null_valid = bool(
                ai_evidence.get("field_observed") is True
                and ai_evidence.get("explicit_null") is True
                and ai_evidence.get("explicit_null_verified") is True
                and str(ai_evidence.get("null_reason") or "").strip()
            )
            expected_ai_checks = {
                "completeness_valid": (
                    ai_evidence.get("value_present") is True
                    and ai_evidence.get("value") is not None
                ),
                "freshness_valid": (
                    explicit_null_valid
                    or ai_evidence.get("freshness_verified") is True
                ),
                "semantic_mapping_valid": (
                    ai_evidence.get("semantic_mapping_verified") is True
                    and ai_evidence.get("invented") is not True
                    and ai_evidence.get("model_knowledge_only") is not True
                ),
                "occurrence_match_valid": (
                    ai_evidence.get("occurrence_verified") is True
                    and ai_evidence.get("reference_period_verified") is True
                    and ai_evidence.get("request_correlation_verified") is True
                ),
                "lineage_valid": _verified_ai_lineage(
                    ai_evidence,
                    field_name,
                ),
            }
            if any(
                normalized_checks[name] != expected
                for name, expected in expected_ai_checks.items()
            ):
                errors.append(f"{prefix}AI_CHECKS_NOT_DERIVED_FROM_EVIDENCE")

        score_components = _score_components(normalized_checks)
        quality_score = sum(
            int(item["earned"]) for item in score_components.values()
        )
        health, health_reasons = _health_for_field(
            outcome,
            normalized_checks,
            quality_score,
            degradable_checks=_target_degradable_quality_checks(
                target
            ),
        )
        ai_reasons = (
            _ai_evidence_reason_codes_from_evidence(
                field_name,
                evidence,
            )
            if _is_ai_provider(provider_type)
            and configured is True
            and normalized_checks["transport_valid"] is True
            and not terminal_outcome_expected
            else ()
        )
        reason_codes = sorted(
            set((*observed_reasons, *health_reasons, *ai_reasons))
        )
        if field_result.get("score_components") != score_components:
            errors.append(f"{prefix}SCORE_COMPONENTS_MISMATCH")
        if field_result.get("quality_score") != quality_score:
            errors.append(f"{prefix}QUALITY_SCORE_MISMATCH")
        if field_result.get("health_status") != health.value:
            errors.append(f"{prefix}HEALTH_STATUS_MISMATCH")
        if field_result.get("reason_codes") != reason_codes:
            errors.append(f"{prefix}REASON_CODES_MISMATCH")
        derived_fields[field_name] = {
            "checks": normalized_checks,
            "quality_score": quality_score,
            "health": health,
            "reason_codes": reason_codes,
        }

    if len(derived_fields) != len(expected_fields) or not derived_fields:
        return tuple(sorted({*errors, "FIELD_DERIVATION_INCOMPLETE"}))
    aggregate_checks = {
        check: (
            all(
                result["checks"].get(check) is True
                for result in derived_fields.values()
            )
            if all(
                result["checks"].get(check) is not None
                for result in derived_fields.values()
            )
            else None
        )
        for check in _all_check_names()
    }
    aggregate_health = max(
        (result["health"] for result in derived_fields.values()),
        key=_health_severity,
    )
    aggregate_score = min(
        int(result["quality_score"]) for result in derived_fields.values()
    )
    aggregate_reasons = sorted(
        {
            reason
            for result in derived_fields.values()
            for reason in result["reason_codes"]
        }
    )
    if row.get("checks") != aggregate_checks:
        errors.append("AGGREGATE_CHECKS_MISMATCH")
    for name in (
        "schema_valid",
        "freshness_valid",
        "semantic_mapping_valid",
        "occurrence_match_valid",
        "lineage_valid",
    ):
        if row.get(name) != aggregate_checks[name]:
            errors.append(f"AGGREGATE_{name.upper()}_MISMATCH")
    if row.get("quality_score") != aggregate_score:
        errors.append("AGGREGATE_QUALITY_SCORE_MISMATCH")
    if row.get("health_status") != aggregate_health.value:
        errors.append("AGGREGATE_HEALTH_STATUS_MISMATCH")
    if row.get("reason_codes") != aggregate_reasons:
        errors.append("AGGREGATE_REASON_CODES_MISMATCH")

    primary_allowed = not roles or bool(roles & PRIMARY_ROLES)
    fallback_allowed = not roles or bool(roles & FALLBACK_ROLES)
    eligible_as_primary = (
        aggregate_health is HealthStatus.HEALTHY
        and primary_allowed
        and all(
            aggregate_checks.get(check) is True
            for check in CORRECTNESS_CHECKS
        )
    )
    eligible_as_fallback = (
        aggregate_health in {HealthStatus.HEALTHY, HealthStatus.DEGRADED}
        and fallback_allowed
        and all(
            aggregate_checks.get(check) is True
            for check in CORRECTNESS_CHECKS
        )
    )
    if (
        type(row.get("eligible_as_primary")) is not bool
        or row.get("eligible_as_primary") != eligible_as_primary
    ):
        errors.append("PRIMARY_ELIGIBILITY_MISMATCH")
    if (
        type(row.get("eligible_as_fallback")) is not bool
        or row.get("eligible_as_fallback") != eligible_as_fallback
    ):
        errors.append("FALLBACK_ELIGIBILITY_MISMATCH")
    expected_recommendations = recommendations_for_result(
        aggregate_health,
        roles=roles,
        eligible_as_primary=eligible_as_primary,
        eligible_as_fallback=eligible_as_fallback,
    )
    if row.get("recommendations") != expected_recommendations:
        errors.append("RECOMMENDATIONS_MISMATCH")
    return tuple(sorted(set(errors)))


def quality_score_formula() -> Mapping[str, Any]:
    return {
        "maximum": QUALITY_SCORE_MAX,
        "components": [
            {
                "name": display_name,
                "check": check,
                "weight": weight,
                "earning_rule": "full weight only when the observed check is true",
            }
            for display_name, check, weight in QUALITY_COMPONENTS
        ],
        "health_rules": {
            "HEALTHY": (
                "transport is proven valid and every quality gate is true"
            ),
            "DEGRADED": (
                "transport, schema, semantic mapping, occurrence and lineage "
                "are proven valid; a capability explicitly declares "
                "completeness and/or freshness degradable, that check is "
                "false, and score >= 60"
            ),
            "UNUSABLE": (
                "transport, schema, semantic mapping, occurrence or lineage "
                "is explicitly false, or score < 60"
            ),
            "UNKNOWN": "one or more mandatory checks were not evaluated",
        },
    }


def recommendations_for_result(
    health: HealthStatus,
    *,
    roles: set[str],
    eligible_as_primary: bool,
    eligible_as_fallback: bool,
) -> list[str]:
    if eligible_as_primary:
        return ["KEEP_PRIMARY"]
    if eligible_as_fallback:
        return ["KEEP_FALLBACK"]
    if health is HealthStatus.DEGRADED and bool(roles & PRIMARY_ROLES):
        return ["DEMOTE_TO_FALLBACK"]
    if health is HealthStatus.UNUSABLE:
        return [
            "DISABLE_FOR_METRIC",
            "REMOVE_PROVIDER_CANDIDATE",
            "REQUIRES_FIX",
            "SOURCE_GAP",
        ]
    if health in {
        HealthStatus.AUTH_FAILED,
        HealthStatus.RATE_LIMITED,
        HealthStatus.DOWN,
    }:
        return ["DISABLE_FOR_METRIC", "REQUIRES_FIX", "SOURCE_GAP"]
    if health is HealthStatus.NOT_CONFIGURED:
        return ["REQUIRES_FIX", "SOURCE_GAP"]
    return ["REQUIRES_FIX"]


def determine_system_health(
    results: Sequence[Mapping[str, Any]],
    *,
    audit_status: AuditStatus,
) -> SystemHealth:
    if audit_status is AuditStatus.FAILED:
        return SystemHealth.UNKNOWN
    if not results:
        return SystemHealth.UNKNOWN
    statuses = {str(item["health_status"]) for item in results}
    if statuses == {HealthStatus.HEALTHY.value}:
        return SystemHealth.HEALTHY
    eligible = sum(
        bool(item["eligible_as_primary"] or item["eligible_as_fallback"])
        for item in results
    )
    if eligible == 0:
        return SystemHealth.CRITICAL
    return SystemHealth.DEGRADED


def stable_sha256(value: Any) -> str:
    payload = json.dumps(
        _json_safe(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def normalized_field_observation(
    normalized_response: Any,
    *,
    target_id: str,
    metric_id: str,
    field_name: str,
) -> tuple[bool, Any, Any]:
    """Resolve one exact target field from a persisted normalized response."""

    scopes, identities_observed = _normalized_target_scopes(
        normalized_response,
        target_id=target_id,
        metric_id=metric_id,
    )
    for scope in scopes:
        observed, value, owner = _normalized_field(scope, field_name)
        if observed:
            return True, value, owner
    if identities_observed:
        return False, None, None
    return _normalized_field(normalized_response, field_name)


def utc_now() -> datetime:
    return datetime.now(UTC)


def _configuration_reasons(
    registration: Any,
    settings: Any,
    targets: Sequence[Any] = (),
) -> tuple[str, ...]:
    if _value(registration, "probe_enabled", True) is False:
        return ("PROBE_DISABLED",)
    provider_id = str(
        _value(registration, "provider_id", "") or ""
    ).upper()
    if provider_id in {
        "CODEX_CLI_RESEARCH_BACKEND",
        "OPENAI_RESPONSES_RESEARCH",
    }:
        required_runtime_settings = (
            "ai_worker_enabled",
            "ai_research_web_access_enabled",
            "research_single_invocation_enabled",
        )
        missing_runtime = tuple(
            name
            for name in required_runtime_settings
            if not bool(_setting_value(settings, name))
        )
        if missing_runtime:
            return tuple(
                f"PROVIDER_DISABLED_BY_CONFIGURATION:{name}"
                for name in missing_runtime
            )
    enabled_setting = _optional_text(
        _value(registration, "enable_setting", None)
        or _value(registration, "enabled_setting", None)
    )
    if enabled_setting and not bool(_setting_value(settings, enabled_setting)):
        return ("PROVIDER_DISABLED_BY_CONFIGURATION",)
    configuration_setting = _optional_text(
        _value(registration, "configuration_setting", None)
    )
    configuration_value = _optional_text(
        _value(registration, "configuration_value", None)
    )
    if configuration_setting and configuration_value:
        observed_configuration = str(
            _setting_value(settings, configuration_setting) or ""
        ).strip().casefold()
        if observed_configuration != configuration_value.casefold():
            return (
                "PROVIDER_CONFIGURATION_VARIANT_NOT_SELECTED:"
                f"{configuration_setting}={configuration_value}",
            )
    for target in targets:
        capability = _value(target, "capability", None)
        profile_id = _optional_text(
            _value(capability, "runtime_profile_id", None)
        )
        if not profile_id:
            continue
        if not bool(_setting_value(settings, "research_agents_enabled")):
            return (
                f"RESEARCH_PROFILE_NOT_CONFIGURED:{profile_id}:"
                "research_agents_enabled",
            )
        profile_setting = _optional_text(
            _value(capability, "runtime_enable_setting", None)
        )
        if profile_setting and not bool(
            _setting_value(settings, profile_setting)
        ):
            return (
                f"RESEARCH_PROFILE_NOT_CONFIGURED:{profile_id}:"
                f"{profile_setting}",
            )
    missing: list[str] = []
    for requirement in _value(registration, "credential_requirements", ()) or ():
        raw_name = (
            str(_value(requirement, "setting_name", ""))
            or str(_value(requirement, "name", ""))
            or str(requirement)
        ).strip()
        required = _value(requirement, "required", True) is not False
        if "(optional" in raw_name.casefold():
            required = False
        alternatives = tuple(
            item.strip().split(" ", 1)[0]
            for item in raw_name.split("|")
            if item.strip()
        )
        if (
            required
            and alternatives
            and not any(_setting_value(settings, name) for name in alternatives)
        ):
            missing.append("|".join(alternatives))
    if missing:
        return tuple(f"CREDENTIAL_NOT_CONFIGURED:{name}" for name in sorted(missing))
    if not _optional_text(_value(registration, "probe_id", None)):
        return ("PROBE_NOT_REGISTERED",)
    return ()


def _setting_value(settings: Any, name: str) -> Any:
    if settings is None:
        return None
    candidates = [name]
    if name.upper().startswith("AI_MARKET_"):
        candidates.append(name[len("AI_MARKET_") :].lower())
    if isinstance(settings, Mapping):
        return next(
            (settings.get(candidate) for candidate in candidates if settings.get(candidate)),
            None,
        )
    return next(
        (
            getattr(settings, candidate, None)
            for candidate in candidates
            if getattr(settings, candidate, None)
        ),
        None,
    )


def _checks_for_field(
    target: CapabilityTarget,
    field_name: str,
    outcome: ProbeOutcome,
) -> dict[str, bool | None]:
    field_override = (
        outcome.field_checks.get(target.field_key(field_name))
        or outcome.field_checks.get(field_name)
        or {}
    )
    checks: dict[str, bool | None] = {}
    for name in _all_check_names():
        value = field_override.get(name, outcome.checks.get(name))
        checks[name] = value if isinstance(value, bool) or value is None else None
    checks["transport_valid"] = _transport_valid(outcome)
    if (
        _is_ai_provider(target.provider_type)
        and checks["transport_valid"] is True
    ):
        evidence = _field_evidence(target, field_name, outcome)
        evidence = evidence if isinstance(evidence, Mapping) else {}
        checks["completeness_valid"] = (
            evidence.get("value_present") is True
            and evidence.get("value") is not None
        )
        checks["freshness_valid"] = evidence.get("freshness_verified") is True
        checks["semantic_mapping_valid"] = (
            evidence.get("semantic_mapping_verified") is True
            and evidence.get("invented") is not True
            and evidence.get("model_knowledge_only") is not True
        )
        checks["occurrence_match_valid"] = (
            evidence.get("occurrence_verified") is True
            and evidence.get("reference_period_verified") is True
            and evidence.get("request_correlation_verified") is True
        )
        checks["lineage_valid"] = _verified_ai_lineage(evidence, field_name)
    return checks


def _transport_valid(outcome: ProbeOutcome) -> bool | None:
    status = outcome.transport_status.upper()
    if status in {"OK", "SUCCESS", "HEALTHY"}:
        if outcome.http_status is None:
            return True
        return 200 <= outcome.http_status < 400
    if status in {
        HealthStatus.DOWN.value,
        HealthStatus.AUTH_FAILED.value,
        HealthStatus.RATE_LIMITED.value,
    }:
        return False
    return None


def _score_components(
    checks: Mapping[str, bool | None],
) -> dict[str, Mapping[str, Any]]:
    return {
        display_name: {
            "check": check,
            "weight": weight,
            "observed": checks.get(check),
            "earned": weight if checks.get(check) is True else 0,
        }
        for display_name, check, weight in QUALITY_COMPONENTS
    }


def _health_for_field(
    outcome: ProbeOutcome,
    checks: Mapping[str, bool | None],
    quality_score: int,
    *,
    degradable_checks: Iterable[str] = (),
) -> tuple[HealthStatus, tuple[str, ...]]:
    transport_status = outcome.transport_status.upper()
    explicit = {
        HealthStatus.AUTH_FAILED.value: HealthStatus.AUTH_FAILED,
        HealthStatus.RATE_LIMITED.value: HealthStatus.RATE_LIMITED,
        HealthStatus.DOWN.value: HealthStatus.DOWN,
        HealthStatus.NOT_CONFIGURED.value: HealthStatus.NOT_CONFIGURED,
        HealthStatus.UNUSABLE.value: HealthStatus.UNUSABLE,
    }
    if not outcome.configured:
        return HealthStatus.NOT_CONFIGURED, ("PROVIDER_NOT_CONFIGURED",)
    if transport_status in explicit:
        return explicit[transport_status], ()
    if checks.get("transport_valid") is False:
        return (
            HealthStatus.DOWN
            if outcome.http_status is not None
            and outcome.http_status >= 500
            else HealthStatus.UNUSABLE,
            ("TRANSPORT_VALID_FAILED",),
        )
    degradable = {
        str(item)
        for item in degradable_checks
        if str(item) in DEGRADABLE_QUALITY_CHECKS
    }
    failed_fatal_checks = [
        name
        for name in CORRECTNESS_CHECKS
        if name not in degradable
        if checks.get(name) is False
    ]
    if failed_fatal_checks:
        return HealthStatus.UNUSABLE, tuple(
            f"{name.upper()}_FAILED" for name in failed_fatal_checks
        )
    unevaluated = [name for name in _all_check_names() if checks.get(name) is None]
    if unevaluated:
        return HealthStatus.UNKNOWN, tuple(
            f"{name.upper()}_NOT_EVALUATED" for name in unevaluated
        )
    if quality_score < 60:
        return HealthStatus.UNUSABLE, ("QUALITY_SCORE_BELOW_MINIMUM",)
    degraded_checks = [
        name
        for name in degradable
        if checks.get(name) is False
    ]
    if degraded_checks:
        return HealthStatus.DEGRADED, tuple(
            f"{name.upper()}_PARTIAL" for name in degraded_checks
        )
    if checks.get("transport_valid") is True and all(
        checks.get(name) is True for name in CORRECTNESS_CHECKS
    ):
        return HealthStatus.HEALTHY, ()
    return HealthStatus.UNKNOWN, ("QUALITY_GATES_NOT_PROVEN",)


def _target_degradable_quality_checks(
    target: Any,
) -> tuple[str, ...]:
    return tuple(
        str(item)
        for item in (
            _value(target, "degradable_quality_checks", ())
            or ()
        )
        if str(item) in DEGRADABLE_QUALITY_CHECKS
    )


def _normalize_outcome(outcome: ProbeOutcome, request: ProbeRequest) -> ProbeOutcome:
    attempts = max(0, int(outcome.attempts))
    if attempts > request.call_budget:
        return ProbeOutcome(
            transport_status=HealthStatus.UNKNOWN.value,
            error_kind="CallBudgetViolation",
            reason_codes=("PROBE_CALL_BUDGET_EXCEEDED",),
            attempts=attempts,
            checks={name: None for name in _all_check_names()},
        )
    if outcome.latency_ms is not None and (
        not math.isfinite(float(outcome.latency_ms)) or float(outcome.latency_ms) < 0
    ):
        outcome.latency_ms = None
        outcome.reason_codes = tuple(
            {*outcome.reason_codes, "INVALID_LATENCY_OBSERVATION"}
        )
    outcome.attempts = attempts
    return outcome


def _acquisition_record(
    request: ProbeRequest,
    outcome: ProbeOutcome,
) -> Mapping[str, Any]:
    raw_sha = (
        hashlib.sha256(outcome.raw_response).hexdigest()
        if outcome.raw_response is not None
        else None
    )
    execution_evidence = (
        outcome.evidence if isinstance(outcome.evidence, Mapping) else {}
    )
    capture_attestation = execution_evidence.get("capture_attestation")
    return {
        "run_id": request.run_id,
        "acquisition_id": request.acquisition_id,
        "acquisition_kind": request.acquisition_kind,
        "request_key": request.request_key,
        "provider_id": request.provider_id,
        "target_ids": [target.target_id for target in request.targets],
        "request_correlations": _json_safe(request.request_correlations),
        "probe_ids": sorted(
            {
                target.probe_id
                for target in request.targets
                if target.probe_id is not None
            }
        ),
        "probe_adapter_paths": sorted(
            {request.adapter_path}
            if request.acquisition_kind == "RUNTIME_ADAPTER_COVERAGE"
            else {
                target.probe_adapter_path
                or str(_value(target.registration, "adapter_path", "")).strip()
                for target in request.targets
            }
        ),
        "observed_adapter_path": execution_evidence.get("adapter_path"),
        "configured": outcome.configured,
        "attempts": outcome.attempts,
        "transport_status": outcome.transport_status,
        "http_status": outcome.http_status,
        "latency_ms": outcome.latency_ms,
        "network_exchange_count": len(outcome.network_exchanges),
        "error_kind": outcome.error_kind,
        "reason_codes": list(outcome.reason_codes),
        "raw_response_sha256": raw_sha,
        "normalized_response_sha256": (
            stable_sha256(outcome.normalized_response)
            if outcome.normalized_response is not None
            else None
        ),
        "real_adapter_invoked": execution_evidence.get(
            "real_adapter_invoked"
        ),
        "probe_dispatch_status": execution_evidence.get(
            "probe_dispatch_status"
        ),
        "capture_mode": execution_evidence.get("capture_mode"),
        "capture_mode_expected": execution_evidence.get(
            "capture_mode_expected"
        ),
        "capture_mode_configuration": _json_safe(
            execution_evidence.get("capture_mode_configuration")
            or _request_capture_mode_configuration(request)
        ),
        "capture_verified": execution_evidence.get("capture_verified"),
        "capture_attestation": _json_safe(capture_attestation),
        "capture_attestation_sha256": (
            stable_sha256(capture_attestation)
            if capture_attestation is not None
            else None
        ),
        "checked_at": outcome.checked_at,
    }


def _request_capture_mode_configuration(
    request: ProbeRequest,
) -> Mapping[str, str] | None:
    setting = str(
        _value(request.registration, "configuration_setting", None)
        or (
            "ai_researcher_mode"
            if request.provider_id == "AI_RESEARCHER"
            else ""
        )
    ).strip()
    if not setting:
        return None
    mode = str(
        _setting_value(
            request.settings,
            setting,
        )
        or _value(request.registration, "configuration_value", None)
        or ("codex_cli" if setting == "ai_researcher_mode" else "")
    ).strip().lower()
    return {
        "setting": setting,
        "value": mode,
    }


def _validated_artifact_bindings(
    recorded: Any,
    *,
    request: ProbeRequest,
    outcome: ProbeOutcome,
) -> list[Mapping[str, Any]]:
    if not isinstance(recorded, Mapping):
        raise ValueError("ACQUISITION_ARTIFACT_BINDING_MISSING")
    metadata = recorded.get("metadata")
    bindings = recorded.get("artifact_bindings")
    if not isinstance(metadata, Mapping) or not isinstance(bindings, Sequence):
        raise ValueError("ACQUISITION_ARTIFACT_BINDING_INVALID")
    expected_metadata = {
        "run_id": request.run_id,
        "acquisition_id": request.acquisition_id,
        "acquisition_kind": request.acquisition_kind,
        "request_key": request.request_key,
        "provider_id": request.provider_id,
        "target_ids": [target.target_id for target in request.targets],
        "probe_adapter_paths": [request.adapter_path],
        "observed_adapter_path": (
            outcome.evidence.get("adapter_path")
            if isinstance(outcome.evidence, Mapping)
            else None
        ),
    }
    if any(metadata.get(key) != value for key, value in expected_metadata.items()):
        raise ValueError("ACQUISITION_ARTIFACT_METADATA_MISMATCH")

    normalized: list[Mapping[str, Any]] = []
    paths: set[str] = set()
    kinds: list[str] = []
    for item in bindings:
        if not isinstance(item, Mapping):
            raise ValueError("ACQUISITION_ARTIFACT_IDENTITY_INVALID")
        path_text = str(item.get("path") or "").strip()
        path = Path(path_text)
        digest = str(item.get("sha256") or "")
        size = item.get("size_bytes")
        if (
            not path_text
            or path.is_absolute()
            or ".." in path.parts
            or path_text in paths
            or type(size) is not int
            or size < 0
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise ValueError("ACQUISITION_ARTIFACT_IDENTITY_INVALID")
        kind = str(item.get("kind") or "").strip()
        if not kind:
            raise ValueError("ACQUISITION_ARTIFACT_KIND_MISSING")
        paths.add(path_text)
        kinds.append(kind)
        normalized.append(dict(item))

    required = {
        "acquisition_metadata",
        "normalized_response",
        "response_headers",
    }
    if outcome.raw_response is not None:
        required.add("response_body")
    if "AI" in request.targets[0].provider_type.upper():
        required.add("ai_evidence")
    if not required.issubset(kinds):
        raise ValueError("ACQUISITION_ARTIFACT_REQUIRED_FILE_MISSING")
    if kinds.count("http_exchange_body") != len(outcome.network_exchanges):
        raise ValueError("HTTP_EXCHANGE_BODY_BINDING_COUNT_MISMATCH")
    if kinds.count("http_exchange_headers") != len(outcome.network_exchanges):
        raise ValueError("HTTP_EXCHANGE_HEADER_BINDING_COUNT_MISMATCH")
    return sorted(normalized, key=lambda item: str(item["path"]))


def _field_evidence(
    target: CapabilityTarget,
    field_name: str,
    outcome: ProbeOutcome,
) -> Any:
    field_map = outcome.evidence.get("fields", {}) if isinstance(outcome.evidence, Mapping) else {}
    if isinstance(field_map, Mapping):
        evidence = (
            field_map.get(target.field_key(field_name))
            or field_map.get(field_name)
        )
        if evidence is not None:
            return _json_safe(evidence)
    return None


def _bound_field_evidence(
    target: CapabilityTarget,
    field_name: str,
    outcome: ProbeOutcome,
    acquisition_record: Mapping[str, Any],
    checks: Mapping[str, bool | None],
) -> Mapping[str, Any]:
    evidence = _field_evidence(target, field_name, outcome)
    base = dict(evidence) if isinstance(evidence, Mapping) else {}
    source_evidence_present = bool(base)
    source_evidence = _json_safe(base) if source_evidence_present else None
    field_observed, value, owner = normalized_field_observation(
        outcome.normalized_response,
        target_id=target.target_id,
        metric_id=target.metric_id,
        field_name=field_name,
    )
    registered_adapter_path = (
        target.probe_adapter_path
        or str(_value(target.registration, "adapter_path", "")).strip()
    )
    execution_evidence = (
        outcome.evidence if isinstance(outcome.evidence, Mapping) else {}
    )
    verification_origin = base.get("verification_origin")
    if (
        not verification_origin
        and execution_evidence.get("real_adapter_invoked") is True
    ):
        verification_origin = "REGISTERED_ADAPTER_OUTPUT"
    request_correlations = acquisition_record.get("request_correlations")
    request_correlation = (
        request_correlations.get(target.target_id)
        if isinstance(request_correlations, Mapping)
        else None
    )
    base.update(
        {
            "schema_version": FIELD_OBSERVATION_SCHEMA_VERSION,
            "source_evidence_present": source_evidence_present,
            "source_evidence": source_evidence,
            "provider_id": target.provider_id,
            "target_id": target.target_id,
            "dataset_id": target.dataset_id,
            "metric_id": target.metric_id,
            "field": field_name,
            "probe_id": target.probe_id,
            "registered_adapter_path": registered_adapter_path,
            "observed_adapter_path": execution_evidence.get("adapter_path"),
            "acquisition_id": acquisition_record.get("acquisition_id"),
            "request_key": acquisition_record.get("request_key"),
            "request_correlation": _json_safe(request_correlation),
            "normalized_response_sha256": acquisition_record.get(
                "normalized_response_sha256"
            ),
            "field_observed": field_observed,
            "value": _json_safe(value) if field_observed else None,
            "value_present": field_observed and value is not None,
            "explicit_null": field_observed and value is None,
            "explicit_null_verified": bool(
                field_observed
                and value is None
                and str(
                    base.get("null_reason")
                    or (
                        owner.get("reason_code")
                        if isinstance(owner, Mapping)
                        else ""
                    )
                    or (
                        owner.get("null_reason")
                        if isinstance(owner, Mapping)
                        else ""
                    )
                    or ""
                ).strip()
            ),
            "value_sha256": (
                stable_sha256(value)
                if field_observed and value is not None
                else None
            ),
            "owner_sha256": (
                stable_sha256(owner) if field_observed else None
            ),
            "observed_checks": {
                name: checks.get(name) for name in _all_check_names()
            },
            "verification_origin": verification_origin,
            "real_adapter_invoked": execution_evidence.get(
                "real_adapter_invoked"
            ),
            "probe_dispatch_status": execution_evidence.get(
                "probe_dispatch_status"
            ),
            "capture_mode": acquisition_record.get("capture_mode"),
            "capture_mode_expected": acquisition_record.get(
                "capture_mode_expected"
            ),
            "capture_mode_configuration": acquisition_record.get(
                "capture_mode_configuration"
            ),
            "capture_verified": acquisition_record.get(
                "capture_verified"
            ),
            "capture_attestation_sha256": acquisition_record.get(
                "capture_attestation_sha256"
            ),
        }
    )
    for name in _all_check_names():
        base[name] = checks.get(name)
    return _json_safe(base)


def _bound_field_evidence_errors(
    *,
    row: Mapping[str, Any],
    target: Any,
    registration: Any | None,
    field_name: str,
    evidence: Mapping[str, Any],
    checks: Mapping[str, bool | None],
    normalized_response: Any,
    acquisition: Mapping[str, Any] | None,
    artifact_root: Path | None,
) -> list[str]:
    prefix = f"{field_name}:"
    if (
        normalized_response is _NORMALIZED_RESPONSE_NOT_SUPPLIED
        or not isinstance(acquisition, Mapping)
    ):
        return [f"{prefix}BOUND_EVIDENCE_CONTEXT_MISSING"]
    provider_id = str(
        _value(registration, "provider_id", None)
        or row.get("provider_id")
        or ""
    )
    dataset_id = str(
        _value(target, "dataset_id", None)
        or row.get("dataset_id")
        or ""
    )
    metric_id = str(
        _value(target, "metric_id", None)
        or row.get("metric_id")
        or ""
    )
    target_id = f"{provider_id}|{dataset_id}|{metric_id}"
    terminal_reason = str(
        _value(registration, "terminal_audit_reason", None) or ""
    ).strip()
    provider_type = str(
        _value(target, "provider_type", None)
        or _value(registration, "provider_type", None)
        or row.get("provider_type")
        or ""
    )
    ai_provider = _is_ai_provider(provider_type)
    if terminal_reason and row.get("configured") is True:
        if _bound_terminal_audit_context_valid(
            row=row,
            acquisition=acquisition,
            target_id=target_id,
            reason=terminal_reason,
            target=target,
            registration=registration,
            field_name=field_name,
            evidence=evidence,
            checks=checks,
            normalized_response=normalized_response,
        ):
            return []
        return [f"{prefix}TERMINAL_AUDIT_BOUND_EVIDENCE_MISMATCH"]
    registered_adapter_path = str(
        _value(target, "probe_adapter_path", None)
        or _value(registration, "adapter_path", None)
        or ""
    ).strip()
    request_correlations = acquisition.get("request_correlations")
    expected_request_correlation = (
        request_correlations.get(target_id)
        if isinstance(request_correlations, Mapping)
        else None
    )
    recomputed_request_key = _recomputed_acquisition_request_key(
        acquisition,
        registration,
    )
    canonical_correlation = {
        "request_key": acquisition.get("request_key"),
        "provider_id": provider_id,
        "target_id": target_id,
        "dataset_id": dataset_id,
        "metric_id": metric_id,
        "correlation_kind": (
            "SYNTHETIC_OCCURRENCE"
            if ai_provider
            else "TARGET_REQUEST"
        ),
        "expected_occurrence_id": None,
        "expected_reference_period": None,
    }
    accepted_correlations: list[Mapping[str, Any]]
    if canonical_correlation["correlation_kind"] == "SYNTHETIC_OCCURRENCE":
        canonical_correlation["expected_occurrence_id"] = (
            "provider-audit:"
            f"{provider_id}:{dataset_id}:{metric_id}:"
            f"{str(acquisition.get('request_key') or '')[:16]}"
        )
        canonical_correlation["expected_reference_period"] = (
            _audit_reference_period(
                str(acquisition.get("run_id") or "")
            )
        )
        accepted_correlations = [canonical_correlation]
    else:
        accepted_correlations = [dict(canonical_correlation)]
        reference_parameter = _REFERENCE_PARAMETER_BY_PROVIDER.get(
            provider_id
        )
        if reference_parameter:
            canonical_correlation["reference_parameter"] = (
                reference_parameter
            )
            canonical_correlation["expected_reference_period"] = (
                _audit_reference_period(
                    str(acquisition.get("run_id") or "")
                )[:7]
            )
            accepted_correlations.append(canonical_correlation)
    if recomputed_request_key != acquisition.get("request_key"):
        errors = [f"{prefix}REQUEST_KEY_NOT_BOUND_TO_RUN"]
    else:
        errors = []
    if (
        not isinstance(expected_request_correlation, Mapping)
        or not any(
            set(expected_request_correlation) == set(candidate)
            and all(
                expected_request_correlation.get(key) == value
                for key, value in candidate.items()
            )
            for candidate in accepted_correlations
        )
    ):
        errors.append(f"{prefix}REQUEST_CORRELATION_IDENTITY_MISMATCH")
    expected_identity = {
        "schema_version": FIELD_OBSERVATION_SCHEMA_VERSION,
        "provider_id": provider_id,
        "target_id": target_id,
        "dataset_id": dataset_id,
        "metric_id": metric_id,
        "field": field_name,
        "probe_id": (
            _value(target, "probe_id", None)
            or _value(registration, "probe_id", None)
        ),
        "registered_adapter_path": registered_adapter_path,
        "acquisition_id": acquisition.get("acquisition_id"),
        "request_key": acquisition.get("request_key"),
        "request_correlation": expected_request_correlation,
        "normalized_response_sha256": acquisition.get(
            "normalized_response_sha256"
        ),
        "real_adapter_invoked": acquisition.get("real_adapter_invoked"),
        "probe_dispatch_status": acquisition.get("probe_dispatch_status"),
        "capture_mode": acquisition.get("capture_mode"),
        "capture_mode_expected": acquisition.get("capture_mode_expected"),
        "capture_mode_configuration": acquisition.get(
            "capture_mode_configuration"
        ),
        "capture_verified": acquisition.get("capture_verified"),
        "capture_attestation_sha256": acquisition.get(
            "capture_attestation_sha256"
        ),
    }
    errors.extend(
        f"{prefix}BOUND_EVIDENCE_IDENTITY_MISMATCH"
        for key, value in expected_identity.items()
        if evidence.get(key) != value
    )
    observed_adapter_path = evidence.get("observed_adapter_path")
    if acquisition.get("configured") is True and (
        observed_adapter_path != registered_adapter_path
    ):
        errors.append(f"{prefix}OBSERVED_ADAPTER_NOT_REGISTERED")
    observed_checks = evidence.get("observed_checks")
    expected_checks = {
        name: checks.get(name) for name in _all_check_names()
    }
    if observed_checks != expected_checks or any(
        evidence.get(name) != value
        for name, value in expected_checks.items()
    ):
        errors.append(f"{prefix}CHECKS_NOT_DERIVED_FROM_FIELD_EVIDENCE")

    field_observed, value, owner = normalized_field_observation(
        normalized_response,
        target_id=target_id,
        metric_id=metric_id,
        field_name=field_name,
    )
    null_reason = (
        str(
            (
                owner.get("reason_code")
                or owner.get("null_reason")
                or ""
            )
            if isinstance(owner, Mapping)
            else ""
        ).strip()
        or None
    )
    expected_observation = {
        "field_observed": field_observed,
        "value": _json_safe(value) if field_observed else None,
        "value_present": field_observed and value is not None,
        "explicit_null": field_observed and value is None,
        "explicit_null_verified": bool(
            field_observed and value is None and null_reason
        ),
        "value_sha256": (
            stable_sha256(value)
            if field_observed and value is not None
            else None
        ),
        "owner_sha256": (
            stable_sha256(owner) if field_observed else None
        ),
    }
    if any(
        evidence.get(key) != expected
        for key, expected in expected_observation.items()
    ):
        errors.append(f"{prefix}NORMALIZED_FIELD_BINDING_MISMATCH")
    if checks.get("transport_valid") is True:
        source_evidence = _source_field_evidence(evidence)
        expected_occurrence = _bound_occurrence_valid(
            owner,
            target=target,
            normalized_response=normalized_response,
            expected_correlation=(
                expected_request_correlation
                if isinstance(expected_request_correlation, Mapping)
                else {}
            ),
            field_name=field_name,
        )
        if source_evidence:
            if source_evidence.get("occurrence_verified") != (
                expected_occurrence is True
            ):
                errors.append(
                    f"{prefix}OCCURRENCE_EVIDENCE_NOT_RECOMPUTED"
                )
            if source_evidence.get("request_correlation_verified") != (
                expected_occurrence is True
            ):
                errors.append(
                    f"{prefix}REQUEST_CORRELATION_EVIDENCE_NOT_RECOMPUTED"
                )
            if source_evidence.get("request_correlation") != (
                expected_request_correlation
                if isinstance(expected_request_correlation, Mapping)
                else {}
            ):
                errors.append(
                    f"{prefix}REQUEST_CORRELATION_EVIDENCE_MISMATCH"
                )
        if checks.get("occurrence_match_valid") != expected_occurrence:
            errors.append(
                f"{prefix}OCCURRENCE_NOT_DERIVED_FROM_REQUEST_CORRELATION"
            )
        if field_observed and value is None and null_reason:
            expected_lineage, lineage_errors = False, ()
        else:
            expected_lineage, lineage_errors = _bound_lineage_valid(
                source_evidence,
                owner=owner,
                field_name=field_name,
                value=value,
                expected_provider_id=provider_id,
                acquisition=acquisition,
                target=target,
                registration=registration,
                expected_correlation=(
                    expected_request_correlation
                    if isinstance(
                        expected_request_correlation,
                        Mapping,
                    )
                    else {}
                ),
                artifact_root=artifact_root,
            )
        errors.extend(f"{prefix}{item}" for item in lineage_errors)
        if checks.get("lineage_valid") != expected_lineage:
            errors.append(
                f"{prefix}LINEAGE_NOT_DERIVED_FROM_NORMALIZED_FIELD"
            )
        if (
            checks.get("completeness_valid")
            != bool(expected_observation["value_present"])
        ):
            errors.append(
                f"{prefix}COMPLETENESS_NOT_DERIVED_FROM_NORMALIZED_FIELD"
            )
        expected_schema = bool(
            field_observed
            and _bound_schema_valid(
                target=target,
                field_name=field_name,
                value=value,
            )
        )
        if checks.get("schema_valid") != expected_schema:
            errors.append(
                f"{prefix}SCHEMA_NOT_DERIVED_FROM_NORMALIZED_FIELD"
            )
        if not ai_provider:
            expected_freshness = (
                True
                if field_observed and value is None and null_reason
                else _bound_freshness_valid(
                    owner,
                    target=target,
                    field_name=field_name,
                    value=value,
                    checked_at=row.get("checked_at"),
                )
            )
            if checks.get("freshness_valid") != expected_freshness:
                errors.append(
                    f"{prefix}FRESHNESS_NOT_DERIVED_FROM_FIELD_LIFECYCLE"
                )
            expected_semantic = _bound_semantic_valid(
                target=target,
                owner=owner,
                field_name=field_name,
                value=value,
            )
            if checks.get("semantic_mapping_valid") != expected_semantic:
                errors.append(
                    f"{prefix}SEMANTIC_MAPPING_NOT_DERIVED_FROM_FIELD_CONTRACT"
                )
    if (
        acquisition.get("normalized_response_sha256") is not None
        and stable_sha256(normalized_response)
        != acquisition.get("normalized_response_sha256")
    ):
        errors.append(f"{prefix}NORMALIZED_RESPONSE_HASH_MISMATCH")
    return errors


def _bound_terminal_audit_context_valid(
    *,
    row: Mapping[str, Any],
    acquisition: Mapping[str, Any],
    target_id: str,
    reason: str,
    target: Any,
    registration: Any,
    field_name: str,
    evidence: Mapping[str, Any],
    checks: Mapping[str, bool | None],
    normalized_response: Any,
) -> bool:
    request_correlations = acquisition.get("request_correlations")
    request_correlation = (
        request_correlations.get(target_id)
        if isinstance(request_correlations, Mapping)
        else None
    )
    provider_id, dataset_id, metric_id = target_id.split("|", 2)
    registered_adapter_path = str(
        _value(target, "probe_adapter_path", None)
        or _value(registration, "adapter_path", None)
        or ""
    ).strip()
    expected_checks = {
        name: None for name in _all_check_names()
    }
    expected_evidence = {
        "schema_version": FIELD_OBSERVATION_SCHEMA_VERSION,
        "source_evidence_present": False,
        "source_evidence": None,
        "provider_id": provider_id,
        "target_id": target_id,
        "dataset_id": dataset_id,
        "metric_id": metric_id,
        "field": field_name,
        "probe_id": _value(target, "probe_id", None),
        "registered_adapter_path": registered_adapter_path,
        "observed_adapter_path": None,
        "acquisition_id": acquisition.get("acquisition_id"),
        "request_key": acquisition.get("request_key"),
        "request_correlation": request_correlation,
        "normalized_response_sha256": None,
        "field_observed": False,
        "value": None,
        "value_present": False,
        "explicit_null": False,
        "explicit_null_verified": False,
        "value_sha256": None,
        "owner_sha256": None,
        "observed_checks": expected_checks,
        "verification_origin": None,
        "real_adapter_invoked": False,
        "probe_dispatch_status": "TERMINAL_UNSUPPORTED",
        "capture_mode": None,
        "capture_mode_expected": None,
        "capture_mode_configuration": acquisition.get(
            "capture_mode_configuration"
        ),
        "capture_verified": None,
        "capture_attestation_sha256": None,
        **expected_checks,
    }
    return bool(
        reason
        and target_id in {
            str(item) for item in acquisition.get("target_ids") or ()
        }
        and acquisition.get("configured") is True
        and acquisition.get("transport_status") == "UNUSABLE"
        and acquisition.get("attempts") == 0
        and acquisition.get("network_exchange_count") == 0
        and acquisition.get("raw_response_sha256") is None
        and acquisition.get("normalized_response_sha256") is None
        and acquisition.get("real_adapter_invoked") is False
        and acquisition.get("probe_dispatch_status")
        == "TERMINAL_UNSUPPORTED"
        and acquisition.get("reason_codes") == [reason]
        and row.get("configured") is True
        and row.get("transport_status") == "UNUSABLE"
        and row.get("attempts") == 0
        and row.get("real_adapter_invoked") is False
        and row.get("probe_dispatch_status") == "TERMINAL_UNSUPPORTED"
        and row.get("observed_reason_codes") == [reason]
        and row.get("health_status") == "UNUSABLE"
        and row.get("eligible_as_primary") is False
        and row.get("eligible_as_fallback") is False
        and normalized_response is None
        and dict(checks) == expected_checks
        and dict(evidence) == expected_evidence
    )


def _bound_occurrence_valid(
    owner: Any,
    *,
    target: Any,
    normalized_response: Any,
    expected_correlation: Mapping[str, Any],
    field_name: str | None = None,
) -> bool | None:
    context = _bound_field_occurrence_scope(
        owner,
        normalized_response,
        target=target,
        expected_correlation=expected_correlation,
        field_name=field_name,
    )
    if not isinstance(context, Mapping):
        return False
    occurrences = _bound_direct_text_values(
        context,
        ("occurrence_id", "release_occurrence_id", "event_key"),
    )
    references = _bound_direct_text_values(
        context,
        ("reference_period", "release_reference_period", "period"),
    )
    metrics = _bound_direct_text_values(
        context,
        ("metric_id", "event_metric_id"),
    )
    metric_id = str(_value(target, "metric_id", ""))
    if metrics and metric_id not in metrics:
        return False
    expected_provider = str(
        expected_correlation.get("provider_id") or ""
    ).strip().casefold()
    provider_ids = _bound_provider_ids(context)
    if not provider_ids:
        for ancestor in reversed(
            _bound_mapping_path_to_identity(
                normalized_response,
                context,
            )
        ):
            provider_ids = _bound_provider_ids(ancestor)
            if provider_ids:
                break
    target_ids = _bound_direct_text_values(
        context,
        ("target_id", "capability_id"),
    )
    expected_target_id = str(
        expected_correlation.get("target_id") or ""
    ).strip()
    target_id_correlated = bool(
        expected_target_id and expected_target_id in target_ids
    )
    if expected_provider and (
        expected_provider not in provider_ids
        and not (
            target_id_correlated
            and expected_target_id.casefold().startswith(
                f"{expected_provider}|"
            )
        )
    ):
        return False
    expected_occurrence = str(
        expected_correlation.get("expected_occurrence_id") or ""
    ).strip()
    if expected_occurrence and expected_occurrence not in occurrences:
        return False
    expected_reference = str(
        expected_correlation.get("expected_reference_period") or ""
    ).strip()
    if expected_reference and not _bound_reference_matches(
        expected_reference,
        references,
    ):
        return False
    if occurrences or references:
        if not occurrences or not references:
            return False
        occurrence_periods = {
            token for item in occurrences for token in _bound_period_tokens(item)
        }
        reference_periods = {
            token for item in references for token in _bound_period_tokens(item)
        }
        if occurrence_periods and reference_periods:
            return any(
                occurrence == reference
                or occurrence.startswith(f"{reference}-")
                or reference.startswith(f"{occurrence}-")
                for occurrence in occurrence_periods
                for reference in reference_periods
            )
        return True if expected_occurrence and expected_reference else None
    if expected_occurrence or expected_reference:
        return False
    frequency = str(_value(target, "frequency", "") or "").upper()
    if frequency in {"INTRADAY", "DAILY", "REALTIME"}:
        return bool(not expected_provider or provider_ids)
    return False


def _bound_field_occurrence_scope(
    owner: Any,
    normalized_response: Any,
    *,
    target: Any,
    expected_correlation: Mapping[str, Any],
    field_name: str | None,
) -> Mapping[str, Any] | None:
    if not isinstance(owner, Mapping):
        return None
    path = _bound_mapping_path_to_identity(normalized_response, owner)
    candidates = list(reversed(path)) if path else [owner]
    expected_target_id = str(
        expected_correlation.get("target_id") or ""
    ).strip()
    expected_metric_id = str(_value(target, "metric_id", "") or "").strip()
    for candidate in candidates:
        if not _bound_mapping_owns_field(candidate, owner, field_name):
            continue
        if not (
            _bound_direct_text_values(
                candidate,
                (
                    "occurrence_id",
                    "release_occurrence_id",
                    "event_key",
                ),
            )
            or _bound_direct_text_values(
                candidate,
                (
                    "reference_period",
                    "release_reference_period",
                    "period",
                ),
            )
        ):
            continue
        candidate_targets = _bound_direct_text_values(
            candidate,
            ("target_id", "capability_id"),
        )
        candidate_metrics = _bound_direct_text_values(
            candidate,
            ("metric_id", "event_metric_id"),
        )
        if expected_target_id and candidate_targets:
            if expected_target_id not in candidate_targets:
                continue
        elif expected_metric_id and candidate_metrics:
            if expected_metric_id not in candidate_metrics:
                continue
        elif candidate is not owner:
            continue
        return candidate
    return owner


def _bound_mapping_path_to_identity(
    root: Any,
    sought: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    def visit(
        current: Any,
        path: list[Mapping[str, Any]],
        seen: set[int],
    ) -> list[Mapping[str, Any]]:
        if current is sought:
            return path
        if not isinstance(current, (Mapping, list, tuple)):
            return []
        identity = id(current)
        if identity in seen:
            return []
        seen.add(identity)
        if isinstance(current, Mapping):
            next_path = [*path, current]
            for item in current.values():
                result = visit(item, next_path, seen)
                if result:
                    return result
        else:
            for item in current:
                result = visit(item, path, seen)
                if result:
                    return result
        return []

    if root is sought:
        return [sought]
    parent_path = visit(root, [], set())
    return [*parent_path, sought] if parent_path else []


def _bound_mapping_owns_field(
    candidate: Mapping[str, Any],
    owner: Mapping[str, Any],
    field_name: str | None,
) -> bool:
    if candidate is owner:
        return True
    if field_name and field_name in candidate:
        return True
    return any(
        candidate.get(key) is owner
        for key in ("fields", "field_values", "values")
    )


def _bound_direct_text_values(
    value: Mapping[str, Any],
    keys: Sequence[str],
) -> set[str]:
    return {
        str(value.get(key)).strip()
        for key in keys
        if str(value.get(key) or "").strip()
    }


def _bound_lineage_valid(
    source_evidence: Mapping[str, Any],
    *,
    owner: Any,
    field_name: str,
    value: Any,
    expected_provider_id: str,
    acquisition: Mapping[str, Any],
    target: Any,
    registration: Any,
    expected_correlation: Mapping[str, Any],
    artifact_root: Path | None,
) -> tuple[bool, tuple[str, ...]]:
    if not isinstance(owner, Mapping):
        return False, ()
    value_sha256 = stable_sha256(value)
    capture_mode = str(acquisition.get("capture_mode_expected") or "").upper()
    bindings = acquisition.get("artifact_bindings")
    http_body_hashes = {
        str(digest)
        for item in bindings or ()
        if isinstance(item, Mapping)
        and item.get("kind") == "http_exchange_body"
        for digest in (item.get("sha256"), item.get("original_sha256"))
        if digest
    }
    http_exchanges = _bound_http_exchanges(
        acquisition,
        artifact_root=artifact_root,
    )
    allowed_domains = tuple(
        str(domain).strip().casefold().removeprefix("www.")
        for domain in (
            _value(target, "runtime_source_domains", ())
            or _value(registration, "source_domains", ())
            or ()
        )
        if str(domain).strip()
    )
    expected_occurrence_id = str(
        expected_correlation.get("expected_occurrence_id") or ""
    ).strip()
    expected_reference_period = str(
        expected_correlation.get("expected_reference_period") or ""
    ).strip()
    candidates = _bound_lineage_candidates(field_name, owner)
    valid_candidate: Mapping[str, Any] | None = None
    local_locator: Mapping[str, Any] | None = None
    valid_exchange: Mapping[str, Any] | None = None
    claim_evidence = False
    ai_lineage = _is_ai_provider(
        str(
            _value(target, "provider_type", None)
            or _value(registration, "provider_type", None)
            or ""
        )
    )
    for candidate, structurally_scoped in candidates:
        declared_field = str(
            candidate.get("field") or candidate.get("field_name") or ""
        ).strip()
        if (not structurally_scoped and declared_field != field_name) or (
            declared_field and declared_field != field_name
        ):
            continue
        declared_hash = str(candidate.get("value_sha256") or "")
        if declared_hash:
            value_bound = declared_hash == value_sha256
        else:
            value_bound = (
                "value" in candidate
                and stable_sha256(candidate.get("value")) == value_sha256
            )
        if not value_bound:
            continue
        candidate_is_claim = bool(
            ai_lineage
            or candidate.get("_audit_claim_evidence") is True
        )
        identities = (
            str(candidate.get("publisher") or "").strip(),
            str(
                candidate.get("distributor")
                or (expected_provider_id if candidate_is_claim else "")
            ).strip(),
            str(
                candidate.get("acquisition_provider")
                or (expected_provider_id if candidate_is_claim else "")
            ).strip(),
        )
        if not all(identities) or identities[2].casefold() != expected_provider_id.casefold():
            continue
        source_url = str(
            candidate.get("source_url")
            or candidate.get("evidence_url")
            or ""
        ).strip()
        observed_occurrences = _bound_direct_text_values(
            candidate,
            ("occurrence_id", "release_occurrence_id", "event_key"),
        )
        if expected_occurrence_id and (
            expected_occurrence_id not in observed_occurrences
        ):
            continue
        observed_references = _bound_direct_text_values(
            candidate,
            (
                "reference_period",
                "release_reference_period",
                "period",
            ),
        )
        if expected_reference_period and not _bound_reference_matches(
            expected_reference_period,
            observed_references,
        ):
            continue
        candidate_exchange = _bound_matching_http_exchange(
            source_url,
            http_exchanges,
        )
        if candidate_is_claim and (
            not _bound_source_allowed(
                source_url,
                publisher=identities[0],
                allowed_domains=allowed_domains,
            )
            or candidate_exchange is None
            or not _bound_exchange_supports_claim(
                candidate_exchange,
                field_name=field_name,
                value=value,
                candidate=candidate,
            )
        ):
            continue
        candidate_locator = _bound_local_locator(candidate, owner)
        if capture_mode == "HTTPX" and not source_url:
            continue
        if capture_mode == "LOCAL_SANDBOX" and not candidate_locator:
            continue
        if capture_mode == "SUBPROCESS" and (
            not source_url
            or candidate_is_claim
            and candidate_exchange is None
        ):
            continue
        valid_candidate = {
            **candidate,
            "distributor": identities[1],
            "acquisition_provider": identities[2],
        }
        local_locator = candidate_locator
        valid_exchange = candidate_exchange
        claim_evidence = candidate_is_claim
        break
    if valid_candidate is None:
        forged = (
            source_evidence.get("field_lineage_verified") is True
            or source_evidence.get("lineage_evidence") is not None
        )
        return False, (("LINEAGE_EVIDENCE_NOT_REPRODUCIBLE",) if forged else ())

    lineage_evidence = source_evidence.get("lineage_evidence")
    if not isinstance(lineage_evidence, Mapping):
        return False, ("LINEAGE_EVIDENCE_MISSING",)
    source_url = str(
        valid_candidate.get("source_url")
        or valid_candidate.get("evidence_url")
        or ""
    ).strip()
    source_hash = str(source_evidence.get("source_content_sha256") or "")
    expected_origin = (
        "AUDIT_SOURCE_URL_GET" if claim_evidence else "AUDIT_TRANSPORT"
    )
    exchange_hash = (
        str(valid_exchange.get("body_sha256") or "")
        if isinstance(valid_exchange, Mapping)
        else ""
    )
    if capture_mode == "HTTPX" and (
        source_evidence.get("verification_origin") != expected_origin
        or source_evidence.get("source_url_reachable") is not True
        or source_evidence.get("source_url") != source_url
        or not source_hash
        or source_hash not in http_body_hashes
        or claim_evidence
        and source_hash != exchange_hash
    ):
        return False, ("LINEAGE_HTTP_CAPTURE_NOT_BOUND",)
    if capture_mode == "LOCAL_SANDBOX" and (
        lineage_evidence.get("local_record_locator") != local_locator
    ):
        return False, ("LINEAGE_LOCAL_RECORD_NOT_BOUND",)
    if capture_mode == "SUBPROCESS" and (
        source_evidence.get("source_url") != source_url
        or source_evidence.get("source_url_reachable") is not True
        or not source_hash
        or source_evidence.get("verification_origin") != expected_origin
        or source_hash not in http_body_hashes
        or claim_evidence
        and source_hash != exchange_hash
    ):
        return False, ("LINEAGE_SUBPROCESS_SOURCE_NOT_BOUND",)
    expected_identity = {
        "field": field_name,
        "value_sha256": value_sha256,
        "publisher": valid_candidate.get("publisher"),
        "distributor": valid_candidate.get("distributor"),
        "acquisition_provider": valid_candidate.get("acquisition_provider"),
        "source_url": source_url or None,
    }
    if (
        source_evidence.get("field") != field_name
        or source_evidence.get("value_sha256") != value_sha256
        or source_evidence.get("field_lineage_verified") is not True
        or any(
            lineage_evidence.get(key) != expected
            for key, expected in expected_identity.items()
        )
        or any(
            source_evidence.get(key) != expected
            for key, expected in expected_identity.items()
            if key not in {"field", "value_sha256"}
        )
    ):
        return False, ("LINEAGE_IDENTITY_OR_VALUE_BINDING_MISMATCH",)
    if lineage_evidence.get("source_content_sha256") != (
        source_hash or None
    ):
        return False, ("LINEAGE_SOURCE_CONTENT_BINDING_MISMATCH",)
    return True, ()


def _bound_lineage_candidates(
    field_name: str,
    owner: Mapping[str, Any],
) -> list[tuple[Mapping[str, Any], bool]]:
    output: list[tuple[Mapping[str, Any], bool]] = []
    direct = owner.get(f"{field_name}_lineage")
    if isinstance(direct, Mapping):
        output.append((direct, True))
    elif isinstance(direct, list):
        output.extend((item, True) for item in direct if isinstance(item, Mapping))
    field_lineage = owner.get("field_lineage")
    if isinstance(field_lineage, Mapping):
        scoped = field_lineage.get(field_name)
        if isinstance(scoped, Mapping):
            output.append((scoped, True))
        elif isinstance(scoped, list):
            output.extend(
                (item, True) for item in scoped if isinstance(item, Mapping)
            )
    lineage = owner.get("lineage")
    if isinstance(lineage, Mapping):
        output.append((lineage, False))
    elif isinstance(lineage, list):
        output.extend((item, False) for item in lineage if isinstance(item, Mapping))
    claim_field = str(
        owner.get("field_semantics")
        or owner.get("field")
        or owner.get("field_name")
        or ""
    ).strip()
    claim_value_present = field_name in owner or (
        claim_field == field_name and "value" in owner
    )
    claim_value = (
        owner.get(field_name)
        if field_name in owner
        else owner.get("value")
    )
    if claim_value_present:
        for evidence in owner.get("evidence") or ():
            if not isinstance(evidence, Mapping):
                continue
            source_url = str(
                evidence.get("canonical_url")
                or evidence.get("source_url")
                or ""
            ).strip()
            if not source_url:
                continue
            output.append(
                (
                    {
                        "_audit_claim_evidence": True,
                        "field": field_name,
                        "value": claim_value,
                        "publisher": evidence.get("publisher"),
                        "source_url": source_url,
                        "evidence_text": evidence.get("evidence_text"),
                        "metric_id": owner.get("metric_id"),
                        "frequency": owner.get("frequency"),
                        "unit": owner.get("unit"),
                        "occurrence_id": (
                            owner.get("occurrence_id")
                            or owner.get("release_occurrence_id")
                            or owner.get("event_key")
                        ),
                        "reference_period": (
                            owner.get("reference_period")
                            or owner.get("release_reference_period")
                            or owner.get("period")
                        ),
                    },
                    True,
                )
            )
    return output


def _bound_field_scoped_items(
    owner: Mapping[str, Any],
    *,
    field_name: str,
    value: Any,
) -> tuple[tuple[str, Any], ...]:
    """Independently bind semantic/lifecycle metadata to one field."""

    items: list[tuple[str, Any]] = [
        (str(key).casefold(), item)
        for key, item in owner.items()
        if not isinstance(item, (Mapping, list, tuple))
    ]
    for container_name in (
        f"{field_name}_freshness",
        f"{field_name}_lifecycle",
        f"{field_name}_metadata",
    ):
        container = owner.get(container_name)
        if isinstance(container, Mapping):
            items.extend(
                (str(key).casefold(), item)
                for key, item in container.items()
                if not isinstance(item, (Mapping, list, tuple))
            )
    value_sha256 = stable_sha256(value)
    for candidate, structurally_scoped in _bound_lineage_candidates(
        field_name,
        owner,
    ):
        declared_field = str(
            candidate.get("field") or candidate.get("field_name") or ""
        ).strip()
        if declared_field and declared_field != field_name:
            continue
        if not structurally_scoped and declared_field != field_name:
            continue
        declared_hash = str(candidate.get("value_sha256") or "").strip()
        if declared_hash:
            if declared_hash != value_sha256:
                continue
        elif "value" in candidate:
            if stable_sha256(candidate.get("value")) != value_sha256:
                continue
        else:
            continue
        items.extend(
            (str(key).casefold(), item)
            for key, item in candidate.items()
            if not isinstance(item, (Mapping, list, tuple))
        )
    return tuple(items)


def _bound_schema_valid(
    *,
    target: Any,
    field_name: str,
    value: Any,
) -> bool:
    from app.services.provider_capability_registry import (
        field_value_type_contract,
    )

    validator_id = str(_value(target, "field_validator_id", "") or "")
    contract = field_value_type_contract(validator_id, field_name)
    if contract is None:
        return False
    if value is None:
        return True
    if contract == "boolean":
        return type(value) is bool
    if contract == "mapping":
        return isinstance(value, Mapping)
    if contract == "sequence":
        return isinstance(value, (list, tuple))
    if contract == "mapping_or_sequence":
        return isinstance(value, (Mapping, list, tuple))
    if contract == "temporal":
        return _bound_valid_temporal(value)
    if contract == "number":
        if isinstance(value, bool) or isinstance(
            value,
            (Mapping, list, tuple, set, frozenset),
        ):
            return False
        try:
            parsed = Decimal(str(value).strip())
        except (InvalidOperation, ValueError):
            return False
        return parsed.is_finite()
    if contract == "scalar":
        return not isinstance(
            value,
            (Mapping, list, tuple, set, frozenset, bool),
        )
    return False


def _bound_valid_temporal(value: Any) -> bool:
    if isinstance(value, (datetime, date)):
        return True
    text = str(value or "").strip()
    if not text:
        return False
    if re.fullmatch(
        r"(?:19|20)\d{2}(?:-Q[1-4]|-(?:0[1-9]|1[0-2]))?",
        text,
        re.I,
    ):
        return True
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            date.fromisoformat(text)
        except ValueError:
            return False
    return True


def _bound_semantic_valid(
    *,
    target: Any,
    owner: Any,
    field_name: str,
    value: Any,
) -> bool | None:
    if not isinstance(owner, Mapping):
        return None
    from app.services.provider_capability_registry import (
        KNOWN_NUMERIC_MEASUREMENT_UNITS,
        capability_field_measurement_contract,
    )

    items = _bound_field_scoped_items(
        owner,
        field_name=field_name,
        value=value,
    )
    expected_metric = str(_value(target, "metric_id", "") or "").casefold()
    capability = _value(target, "capability", None)
    canonical_metric_ids = tuple(
        str(item).strip()
        for item in (_value(capability, "canonical_metric_ids", ()) or ())
        if str(item).strip()
    )
    accepted_metrics = {
        expected_metric,
        *(item.casefold() for item in canonical_metric_ids),
    } - {""}
    structured_metrics = {
        str(item).strip().casefold()
        for key, item in items
        if key
        in {
            "canonical_series_id",
            "event_metric_id",
            "metric",
            "metric_id",
            "series_id",
        }
        and str(item or "").strip()
    }
    if structured_metrics and structured_metrics.isdisjoint(accepted_metrics):
        return False

    expected_frequency = (
        _bound_normalized_frequency(_value(target, "frequency", None))
        or str(_value(target, "frequency", "") or "").casefold()
    )
    raw_frequencies = [
        item
        for key, item in items
        if key
        in {
            "cadence",
            "frequency",
            "observation_frequency",
            "release_frequency",
        }
        and item not in (None, "")
    ]
    frequencies = {
        normalized
        for item in raw_frequencies
        if (normalized := _bound_normalized_frequency(item)) is not None
    }
    if frequencies and frequencies != {expected_frequency}:
        return False
    if raw_frequencies and not frequencies:
        return None

    expected_transformation = _bound_semantic_token(
        _value(target, "transformation", None)
    )
    transformations = {
        normalized
        for key, item in items
        if key in {"transformation", "transformation_id"}
        and item not in (None, "")
        if (normalized := _bound_semantic_token(item)) is not None
    }
    if expected_transformation and transformations and (
        transformations != {expected_transformation}
    ):
        return False
    if expected_transformation and not transformations:
        return None

    measurement_contract = capability_field_measurement_contract(
        capability or target,
        field_name,
    )
    expected_unit = _bound_unit(measurement_contract)
    units = {
        normalized
        for key, item in items
        if key in {"unit", "units", "measurement_unit"}
        and item not in (None, "")
        if (normalized := _bound_unit(item)) is not None
    }
    if measurement_contract == "mixed_numeric":
        if units and not units.issubset(KNOWN_NUMERIC_MEASUREMENT_UNITS):
            return False
        return None
    elif measurement_contract not in {
        "categorical",
        "mixed_structured",
        "temporal",
    }:
        if expected_unit and units and units != {expected_unit}:
            return False
        if expected_unit and not units:
            return None

    bases = _bound_metric_change_bases(
        " ".join(
            str(item).casefold()
            for _key, item in items
            if not isinstance(item, (Mapping, list, tuple))
        )
    )
    expected_basis = _bound_metric_change_basis(
        " ".join(
            item
            for item in (
                expected_metric,
                str(_value(target, "transformation", "") or "").casefold(),
            )
            if item
        )
    )
    if expected_basis is not None and bases and any(
        item != expected_basis for item in bases
    ):
        return False
    if (
        structured_metrics & accepted_metrics
        and frequencies == {expected_frequency}
    ):
        return True
    return None


def _bound_semantic_token(value: Any) -> str | None:
    token = re.sub(
        r"[^a-z0-9]+",
        "_",
        str(value or "").strip().casefold(),
    ).strip("_")
    return token or None


def _bound_unit(value: Any) -> str | None:
    raw = str(value or "").strip().casefold()
    token = _bound_semantic_token(value)
    aliases = {
        "%": "percent",
        "pct": "percent",
        "percentage": "percent",
        "percentage_point": "percentage_points",
        "percentage_points": "percentage_points",
        "points": "index_points",
    }
    return aliases.get(raw, aliases.get(token, token))


def _bound_normalized_frequency(value: Any) -> str | None:
    token = _bound_semantic_token(value)
    aliases = {
        "a": "yearly",
        "annual": "yearly",
        "annually": "yearly",
        "d": "daily",
        "daily": "daily",
        "day": "daily",
        "event": "event",
        "event_driven": "event",
        "intraday": "intraday",
        "m": "monthly",
        "m_m": "monthly",
        "mom": "monthly",
        "month": "monthly",
        "monthly": "monthly",
        "on_demand": "request",
        "per_request": "request",
        "q": "quarterly",
        "q_q": "quarterly",
        "qoq": "quarterly",
        "quarter": "quarterly",
        "quarterly": "quarterly",
        "real_time": "realtime",
        "realtime": "realtime",
        "release": "event",
        "request": "request",
        "request_scoped": "request",
        "w": "weekly",
        "week": "weekly",
        "weekly": "weekly",
        "year": "yearly",
        "yearly": "yearly",
    }
    return aliases.get(token)


def _bound_metric_change_basis(text: str) -> str | None:
    values = _bound_metric_change_bases(text)
    return next(iter(values)) if len(values) == 1 else None


def _bound_metric_change_bases(text: str) -> set[str]:
    normalized = re.sub(r"[^a-z0-9%]+", " ", text.casefold())
    matches: set[str] = set()
    patterns = {
        "MOM": (r"\bmom\b", r"\bm m\b", r"month over month"),
        "YOY": (r"\byoy\b", r"\ba a\b", r"year over year"),
        "QOQ": (r"\bqoq\b", r"\bq q\b", r"quarter over quarter"),
    }
    for basis, expressions in patterns.items():
        if any(re.search(expression, normalized) for expression in expressions):
            matches.add(basis)
    return matches


def _bound_freshness_valid(
    owner: Any,
    *,
    target: Any,
    field_name: str,
    value: Any,
    checked_at: Any,
) -> bool | None:
    if not isinstance(owner, Mapping):
        return None
    current = _bound_parse_datetime(checked_at) or datetime.now(UTC)
    items = _bound_field_scoped_items(
        owner,
        field_name=field_name,
        value=value,
    )
    valid_until = tuple(
        parsed
        for key, item in items
        if key in {"content_valid_until", "valid_until"}
        if (parsed := _bound_parse_datetime(item)) is not None
    )
    if valid_until and any(item <= current for item in valid_until):
        return False
    refresh_due = tuple(
        parsed
        for key, item in items
        if key in {"refresh_due_at", "next_refresh_at"}
        if (parsed := _bound_parse_datetime(item)) is not None
    )
    if refresh_due and any(item <= current for item in refresh_due):
        return False
    lifecycle = {
        str(item).strip().upper()
        for key, item in items
        if key in {"freshness", "lifecycle", "lifecycle_status"}
        and item is not None
    }
    if lifecycle & {"STALE", "EXPIRED"}:
        return False
    observations = tuple(
        parsed
        for key, item in items
        if key
        in {
            "data_as_of",
            "reference_period",
            "released_at",
            "release_at",
            "release_time",
            "provider_timestamp",
        }
        if (parsed := _bound_parse_reference_datetime(item, target)) is not None
    )
    latest = max(observations) if observations else None
    proof = _bound_release_lifecycle_proof(items, target=target)
    if proof is False:
        return False
    if latest is not None and latest > current + timedelta(days=2):
        return False
    if proof is True:
        return True
    maximum_age = _bound_freshness_maximum_age(target)
    if (
        latest is not None
        and maximum_age is not None
        and current - latest > maximum_age
    ):
        return False
    if _bound_requires_release_proof(items, target=target):
        return None
    current_labels = {
        "LIVE",
        "RECENT",
        "CURRENT",
        "CURRENT_RELEASE",
        "CURRENT_LATEST_OFFICIAL_RELEASE",
    }
    if latest is not None and lifecycle & current_labels:
        return True
    if latest is not None and valid_until and all(
        item > current for item in valid_until
    ):
        return True
    return None


def _bound_requires_release_proof(
    items: Sequence[tuple[str, Any]],
    *,
    target: Any,
) -> bool:
    frequency = str(_value(target, "frequency", "") or "").casefold()
    return frequency in {
        "weekly",
        "monthly",
        "quarterly",
        "annual",
        "yearly",
        "event",
    } or any(
        key
        in {
            "occurrence_id",
            "reference_period",
            "expected_occurrence_id",
            "latest_expected_occurrence_id",
            "expected_reference_period",
            "latest_expected_reference_period",
        }
        for key, _item in items
    )


def _bound_release_lifecycle_proof(
    items: Sequence[tuple[str, Any]],
    *,
    target: Any,
) -> bool | None:
    if not _bound_requires_release_proof(items, target=target):
        return None
    occurrences = {
        str(item).strip().casefold()
        for key, item in items
        if key in {"occurrence_id", "release_occurrence_id"}
        and str(item or "").strip()
    }
    expected_occurrences = {
        str(item).strip().casefold()
        for key, item in items
        if key
        in {"expected_occurrence_id", "latest_expected_occurrence_id"}
        and str(item or "").strip()
    }
    periods = {
        str(item).strip().casefold()
        for key, item in items
        if key in {"reference_period", "release_reference_period"}
        and str(item or "").strip()
    }
    expected_periods = {
        str(item).strip().casefold()
        for key, item in items
        if key
        in {"expected_reference_period", "latest_expected_reference_period"}
        and str(item or "").strip()
    }
    if expected_occurrences and (
        not occurrences or occurrences.isdisjoint(expected_occurrences)
    ):
        return False
    if expected_periods and (
        not periods or periods.isdisjoint(expected_periods)
    ):
        return False
    proof_values = [
        item
        for key, item in items
        if key
        in {
            "expected_occurrence_verified",
            "is_latest_expected_release",
            "latest_occurrence_verified",
            "latest_release_verified",
            "lifecycle_verified",
        }
    ]
    if any(item is False for item in proof_values):
        return False
    if any(item is True for item in proof_values) and (
        expected_occurrences or expected_periods
    ):
        return True
    return None


def _bound_freshness_maximum_age(target: Any) -> timedelta | None:
    dataset_id = str(_value(target, "dataset_id", "") or "")
    if dataset_id:
        try:
            from app.services.provider_capability_registry import (
                dataset_policy_by_id,
            )

            return timedelta(
                seconds=dataset_policy_by_id(dataset_id).sla_seconds
            )
        except KeyError:
            pass
    limits = {
        "realtime": timedelta(hours=2),
        "intraday": timedelta(hours=36),
        "daily": timedelta(days=4),
        "weekly": timedelta(days=15),
        "monthly": timedelta(days=70),
        "quarterly": timedelta(days=155),
        "annual": timedelta(days=400),
        "yearly": timedelta(days=400),
    }
    return limits.get(str(_value(target, "frequency", "") or "").casefold())


def _bound_parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time(), tzinfo=UTC)
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _bound_parse_reference_datetime(value: Any, target: Any) -> datetime | None:
    parsed = _bound_parse_datetime(value)
    if parsed is not None:
        return parsed
    text = str(value or "").strip().upper()
    month = re.fullmatch(r"((?:19|20)\d{2})-(0[1-9]|1[0-2])", text)
    if month:
        year, month_number = (int(item) for item in month.groups())
        if month_number == 12:
            return datetime(year + 1, 1, 1, tzinfo=UTC) - timedelta(
                microseconds=1
            )
        return datetime(year, month_number + 1, 1, tzinfo=UTC) - timedelta(
            microseconds=1
        )
    quarter = re.fullmatch(r"((?:19|20)\d{2})-?Q([1-4])", text)
    if quarter:
        year, quarter_number = (int(item) for item in quarter.groups())
        end_month = quarter_number * 3
        if end_month == 12:
            return datetime(year + 1, 1, 1, tzinfo=UTC) - timedelta(
                microseconds=1
            )
        return datetime(year, end_month + 1, 1, tzinfo=UTC) - timedelta(
            microseconds=1
        )
    if (
        re.fullmatch(r"(?:19|20)\d{2}", text)
        and str(_value(target, "frequency", "") or "").casefold()
        in {"annual", "yearly"}
    ):
        return datetime(int(text), 1, 1, tzinfo=UTC)
    return None


def _bound_http_exchanges(
    acquisition: Mapping[str, Any],
    *,
    artifact_root: Path | None,
) -> tuple[Mapping[str, Any], ...]:
    if artifact_root is None:
        return ()
    root = artifact_root.resolve()
    by_sequence: dict[int, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for binding in acquisition.get("artifact_bindings") or ():
        if not isinstance(binding, Mapping):
            continue
        kind = str(binding.get("kind") or "")
        sequence = binding.get("sequence")
        if kind not in {"http_exchange_body", "http_exchange_headers"} or (
            type(sequence) is not int
        ):
            continue
        by_sequence[sequence][kind] = binding
    exchanges: list[Mapping[str, Any]] = []
    for sequence in sorted(by_sequence):
        pair = by_sequence[sequence]
        body_binding = pair.get("http_exchange_body")
        headers_binding = pair.get("http_exchange_headers")
        if body_binding is None or headers_binding is None:
            continue
        body_path = _bound_artifact_path(root, body_binding)
        headers_path = _bound_artifact_path(root, headers_binding)
        if body_path is None or headers_path is None:
            continue
        try:
            body = body_path.read_bytes()
            headers_payload = json.loads(
                headers_path.read_text(encoding="utf-8")
            )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(headers_payload, Mapping):
            continue
        body_sha256 = hashlib.sha256(body).hexdigest()
        if (
            body_binding.get("exact_bytes_saved") is not True
            or body_binding.get("redaction_applied") is True
            or body_binding.get("sha256") != body_sha256
            or body_binding.get("original_sha256") != body_sha256
        ):
            continue
        request = headers_payload.get("request")
        response = headers_payload.get("response")
        if not isinstance(request, Mapping) or not isinstance(
            response,
            Mapping,
        ):
            continue
        status_code = response.get("status_code")
        if type(status_code) is not int:
            continue
        exchanges.append(
            {
                "url": str(request.get("url") or ""),
                "method": str(request.get("method") or "").upper(),
                "status_code": status_code,
                "body": body,
                "body_sha256": body_sha256,
            }
        )
    return tuple(exchanges)


def _bound_artifact_path(
    root: Path,
    binding: Mapping[str, Any],
) -> Path | None:
    relative = str(binding.get("path") or "")
    path = (root / relative).resolve()
    if not relative or not path.is_relative_to(root) or not path.is_file():
        return None
    try:
        payload = path.read_bytes()
    except OSError:
        return None
    if (
        hashlib.sha256(payload).hexdigest()
        != str(binding.get("sha256") or "")
        or len(payload) != binding.get("size_bytes")
    ):
        return None
    return path


def _bound_matching_http_exchange(
    source_url: str,
    exchanges: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    normalized = _bound_normalized_source_url(source_url)
    if normalized is None:
        return None
    return next(
        (
            exchange
            for exchange in exchanges
            if exchange.get("method") == "GET"
            and 200 <= int(exchange.get("status_code") or 0) < 300
            and _bound_normalized_source_url(
                str(exchange.get("url") or "")
            )
            == normalized
        ),
        None,
    )


def _bound_normalized_source_url(source_url: str) -> str | None:
    try:
        parsed = urlsplit(redact_sensitive(str(source_url).strip()))
        port = parsed.port
    except ValueError:
        return None
    if (
        parsed.scheme.casefold() != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or port not in (None, 443)
    ):
        return None
    host = parsed.hostname.casefold().rstrip(".")
    return urlunsplit(
        (
            "https",
            host if port is None else f"{host}:{port}",
            parsed.path or "/",
            urlencode(
                sorted(parse_qsl(parsed.query, keep_blank_values=True)),
                doseq=True,
            ),
            "",
        )
    )


def _bound_source_allowed(
    source_url: str,
    *,
    publisher: str,
    allowed_domains: Sequence[str],
) -> bool:
    normalized = _bound_normalized_source_url(source_url)
    if normalized is None or not allowed_domains:
        return False
    try:
        host = str(urlsplit(normalized).hostname or "").casefold()
    except ValueError:
        return False
    if not any(
        _bound_domain_matches(host, domain)
        for domain in allowed_domains
    ):
        return False
    policy = _bound_source_policy()
    if policy.publisher_matches_url(publisher, source_url):
        return True
    return bool(
        policy.rule_for(source_url, publisher) is None
        and _bound_publisher_host_fallback(publisher, host)
    )


@lru_cache(maxsize=1)
def _bound_source_policy() -> SourcePolicyService:
    return SourcePolicyService()


def _bound_domain_matches(host: str, allowed_domain: str) -> bool:
    normalized_host = str(host).casefold().rstrip(".").removeprefix("www.")
    normalized_allowed = (
        str(allowed_domain)
        .casefold()
        .strip()
        .rstrip(".")
        .removeprefix("www.")
    )
    return bool(
        normalized_allowed
        and (
            normalized_host == normalized_allowed
            or normalized_host.endswith(f".{normalized_allowed}")
        )
    )


def _bound_publisher_host_fallback(publisher: str, host: str) -> bool:
    aliases = {
        "aaii": "aaii",
        "ap": "apnews",
        "bea": "bea",
        "bls": "bls",
        "bloomberg": "bloomberg",
        "cboe": "cboe",
        "cftc": "cftc",
        "cme": "cmegroup",
        "federalreserve": "federalreserve",
        "fred": "stlouisfed",
        "nasdaq": "nasdaq",
        "reuters": "reuters",
        "sec": "sec",
    }
    tokens = set(re.findall(r"[a-z0-9]+", publisher.casefold()))
    labels = set(re.findall(r"[a-z0-9]+", host.casefold()))
    return any(token in labels or aliases.get(token) in labels for token in tokens)


def _bound_exchange_supports_claim(
    exchange: Mapping[str, Any],
    *,
    field_name: str,
    value: Any,
    candidate: Mapping[str, Any],
) -> bool:
    body = exchange.get("body")
    if not isinstance(body, bytes):
        return False
    normalized_body = re.sub(
        r"\s+",
        " ",
        body.decode("utf-8", errors="ignore").casefold(),
    ).strip()
    anchor = re.sub(
        r"\s+",
        " ",
        str(candidate.get("evidence_text") or "").casefold(),
    ).strip()
    if not anchor or anchor not in normalized_body:
        return False
    if not _bound_field_value_pair_matches(
        anchor,
        field_name=field_name,
        value=value,
    ):
        return False
    raw_metric_tokens = {
        token
        for token in re.findall(
            r"[a-z0-9]+",
            str(candidate.get("metric_id") or "").casefold(),
        )
        if len(token) >= 2
        and token not in {"headline", "core", "index", "rate"}
    }
    if str(candidate.get("metric_id") or "").casefold() == (
        "event_missing_fields"
    ):
        raw_metric_tokens.clear()
    groups = [
        raw_metric_tokens - {"yoy", "mom", "qoq"},
        set().union(
            *(
                _bound_frequency_aliases(token)
                for token in raw_metric_tokens & {"yoy", "mom", "qoq"}
            )
        ),
        _bound_period_tokens(
            str(candidate.get("reference_period") or "")
        )
        | _bound_period_tokens(
            str(candidate.get("occurrence_id") or "")
        ),
        _bound_unit_aliases(candidate.get("unit")),
        _bound_frequency_aliases(candidate.get("frequency")),
    ]
    applicable = [group for group in groups if group]
    return bool(
        applicable
        and all(
            any(
                _bound_context_token_in_text(token, anchor)
                for token in group
            )
            for group in applicable
        )
    )


_BOUND_FIELD_VALUE_ALIASES = {
    "actual": ("actual", "reported"),
    "forecast": ("forecast", "expected"),
    "consensus": ("consensus", "expected"),
    "previous": ("previous", "prior"),
    "previous_revised": (
        "previous revised",
        "revised previous",
        "revision",
    ),
}


def _bound_field_value_pair_matches(
    anchor: str,
    *,
    field_name: str,
    value: Any,
) -> bool:
    aliases = _BOUND_FIELD_VALUE_ALIASES.get(
        field_name.casefold().replace("_", " ")
    )
    if not aliases:
        return _bound_value_in_text(value, anchor)
    all_aliases = tuple(
        dict.fromkeys(
            alias
            for values in _BOUND_FIELD_VALUE_ALIASES.values()
            for alias in values
        )
    )
    pattern = re.compile(
        r"\b(?:"
        + "|".join(
            re.escape(alias)
            for alias in sorted(all_aliases, key=len, reverse=True)
        )
        + r")\b"
    )
    labels = list(pattern.finditer(anchor))
    return any(
        match.group(0) in aliases
        and _bound_value_in_text(
            value,
            anchor[
                match.end() : (
                    labels[index + 1].start()
                    if index + 1 < len(labels)
                    else len(anchor)
                )
            ],
        )
        for index, match in enumerate(labels)
    )


def _bound_value_in_text(value: Any, text: str) -> bool:
    expected_number = _bound_decimal_value(value)
    if expected_number is not None:
        return any(
            observed == expected_number
            for observed in _bound_numbers_in_text(text)
        )
    if isinstance(value, str):
        token = value.strip().casefold()
        return _bound_string_value_in_text(token, text)
    token = json.dumps(
        _json_safe(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).casefold()
    return bool(token and token in text)


def _bound_string_value_in_text(value: str, text: str) -> bool:
    normalized = re.sub(r"\s+", " ", value.casefold()).strip()
    if not normalized:
        return False
    normalized_url = _bound_normalized_source_url(normalized)
    if normalized_url is not None:
        return any(
            _bound_normalized_source_url(candidate.rstrip(".,);]"))
            == normalized_url
            for candidate in re.findall(
                r"https://[^\s\"'<>]+",
                text,
                flags=re.IGNORECASE,
            )
        )
    words = re.findall(r"[a-z0-9]+", normalized)
    if words:
        pattern = r"[\s_./-]+".join(
            re.escape(word) for word in words
        )
        return re.search(
            rf"(?<![a-z0-9]){pattern}(?![a-z0-9])",
            text,
            flags=re.IGNORECASE,
        ) is not None
    return _bound_context_token_in_text(normalized, text)


_BOUND_NUMBER_PATTERN = re.compile(
    r"(?<![a-z0-9_.,])"
    r"[+\-\u2212\ufe63\uff0d]?"
    r"(?:"
    r"\d{1,3}(?:,\d{3})+(?:\.\d+)?"
    r"|\d+,\d{1,2}"
    r"|(?:\d+(?:\.\d+)?|\.\d+)"
    r")"
    r"(?:[eE][+\-]?\d+)?"
    r"\s*%?"
    r"(?![a-z0-9_.,])",
    re.IGNORECASE,
)


def _bound_decimal_value(value: Any) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    token = str(value).strip()
    if not token:
        return None
    match = _BOUND_NUMBER_PATTERN.fullmatch(token)
    if match is None:
        return None
    return _bound_decimal_token(match.group(0))


def _bound_numbers_in_text(text: str) -> tuple[Decimal, ...]:
    return tuple(
        number
        for match in _BOUND_NUMBER_PATTERN.finditer(text)
        if (number := _bound_decimal_token(match.group(0))) is not None
    )


def _bound_decimal_token(token: str) -> Decimal | None:
    normalized = (
        token.strip()
        .rstrip("%")
        .strip()
        .replace("\u2212", "-")
        .replace("\ufe63", "-")
        .replace("\uff0d", "-")
    )
    if "," in normalized:
        if re.fullmatch(
            r"[+\-]?\d{1,3}(?:,\d{3})+(?:\.\d+)?(?:[eE][+\-]?\d+)?",
            normalized,
        ):
            normalized = normalized.replace(",", "")
        else:
            integer, _, fractional = normalized.partition(",")
            normalized = f"{integer}.{fractional}"
    try:
        parsed = Decimal(normalized)
    except InvalidOperation:
        return None
    return parsed if parsed.is_finite() else None


def _bound_unit_aliases(value: Any) -> set[str]:
    normalized = str(value or "").strip().casefold()
    if normalized in {"%", "percent", "percentage", "percentage point"}:
        return {"%", "percent", "percentage", "percentage point"}
    return {normalized} if normalized else set()


def _bound_context_token_in_text(token: str, text: str) -> bool:
    normalized = re.sub(r"\s+", " ", str(token).casefold()).strip()
    if not normalized:
        return False
    escaped = re.escape(normalized)
    left = r"(?<![a-z0-9])" if normalized[0].isalnum() else ""
    right = r"(?![a-z0-9])" if normalized[-1].isalnum() else ""
    return re.search(f"{left}{escaped}{right}", text) is not None


def _bound_frequency_aliases(value: Any) -> set[str]:
    normalized = str(value or "").strip().casefold()
    aliases = {
        "yoy": {"yoy", "y/y", "year over year", "year-on-year"},
        "mom": {"mom", "m/m", "month over month", "month-on-month"},
        "qoq": {"qoq", "q/q", "quarter over quarter", "quarter-on-quarter"},
        "monthly": {"monthly"},
        "quarterly": {"quarterly"},
        "weekly": {"weekly"},
        "daily": {"daily"},
    }
    return aliases.get(normalized, {normalized} if normalized else set())


def _bound_local_locator(
    candidate: Mapping[str, Any],
    owner: Mapping[str, Any],
) -> dict[str, Any]:
    keys = (
        "repository_record_id",
        "record_id",
        "cache_key",
        "target_id",
        "metric_id",
        "series_id",
        "occurrence_id",
        "data_as_of",
    )
    return {
        key: candidate.get(key, owner.get(key))
        for key in keys
        if candidate.get(key, owner.get(key)) not in (None, "")
    }


def _bound_walk_items(value: Any) -> list[tuple[str, Any]]:
    output: list[tuple[str, Any]] = []
    stack = [value]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if isinstance(current, (Mapping, list, tuple)):
            identity = id(current)
            if identity in seen:
                continue
            seen.add(identity)
        if isinstance(current, Mapping):
            for key, item in current.items():
                output.append((str(key).casefold(), item))
                if isinstance(item, (Mapping, list, tuple)):
                    stack.append(item)
        elif isinstance(current, (list, tuple)):
            stack.extend(current)
    return output


def _bound_provider_ids(value: Any) -> set[str]:
    if not isinstance(value, Mapping):
        return set()
    strong = {
        str(value.get(key)).strip().casefold()
        for key in ("provider_id", "acquisition_provider")
        if str(value.get(key) or "").strip()
    }
    if strong:
        return strong
    return {
        str(value.get("source")).strip().casefold()
        for _ in (0,)
        if isinstance(value.get("source"), str)
        and str(value.get("source")).strip()
    }


def _bound_reference_matches(expected: str, observed: set[str]) -> bool:
    if expected in observed:
        return True
    expected_tokens = _bound_period_tokens(expected)
    return bool(
        expected_tokens
        and any(expected_tokens & _bound_period_tokens(item) for item in observed)
    )


def _bound_period_tokens(value: str) -> set[str]:
    tokens: set[str] = set()
    for match in re.finditer(
        r"(?<!\d)(20\d{2})(?:[-_/](0[1-9]|1[0-2]))?"
        r"(?:[-_/](0[1-9]|[12]\d|3[01]))?(?!\d)",
        value,
    ):
        year, month, day = match.groups()
        tokens.add(
            f"{year}-{month}-{day}"
            if month and day
            else f"{year}-{month}"
            if month
            else year
        )
    return tokens


def _normalized_field(value: Any, field_name: str) -> tuple[bool, Any, Any]:
    aliases = {
        field_name.casefold(),
        field_name.casefold().replace("_", ""),
    }
    stack = [value]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if isinstance(current, (dict, list, tuple)):
            identity = id(current)
            if identity in seen:
                continue
            seen.add(identity)
        if isinstance(current, Mapping):
            items = sorted(
                current.items(),
                key=lambda pair: (
                    str(pair[0]).casefold(),
                    str(pair[0]),
                ),
            )
            for key, item in items:
                normalized_key = str(key).casefold()
                if (
                    normalized_key in aliases
                    or normalized_key.replace("_", "") in aliases
                ):
                    return True, item, current
            stack.extend(
                reversed(
                    [
                        item
                        for _key, item in items
                        if isinstance(item, (Mapping, list, tuple))
                    ]
                )
            )
        elif isinstance(current, (list, tuple)):
            stack.extend(reversed(current))
    return False, None, None


def _normalized_target_scopes(
    value: Any,
    *,
    target_id: str,
    metric_id: str,
) -> tuple[list[Any], bool]:
    desired_target = target_id.strip().casefold()
    desired_metric = metric_id.strip().casefold()
    target_keys = {"target_id", "capability_id"}
    metric_keys = {
        "metric_id",
        "event_metric_id",
        "series_id",
        "canonical_series_id",
        "metric",
    }
    exact_target_matches: list[Any] = []
    metric_matches: list[Any] = []
    identities_observed = False
    stack = [value]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if not isinstance(current, (Mapping, list, tuple)):
            continue
        identity = id(current)
        if identity in seen:
            continue
        seen.add(identity)
        if isinstance(current, Mapping):
            items = sorted(
                current.items(),
                key=lambda pair: (
                    str(pair[0]).casefold(),
                    str(pair[0]),
                ),
            )
            for key, item in items:
                key_text = str(key).strip().casefold()
                item_text = str(item).strip().casefold()
                if key_text in target_keys and item not in (None, ""):
                    identities_observed = True
                    if item_text == desired_target:
                        exact_target_matches.append(current)
                elif key_text in metric_keys and item not in (None, ""):
                    identities_observed = True
                    if item_text == desired_metric:
                        metric_matches.append(current)
                elif key_text == desired_target and isinstance(
                    item,
                    (Mapping, list, tuple),
                ):
                    identities_observed = True
                    exact_target_matches.append(item)
                elif key_text == desired_metric and isinstance(
                    item,
                    (Mapping, list, tuple),
                ):
                    identities_observed = True
                    metric_matches.append(item)
            stack.extend(
                reversed(
                    [
                        item
                        for _key, item in items
                        if isinstance(item, (Mapping, list, tuple))
                    ]
                )
            )
        else:
            stack.extend(reversed(current))
    return (
        exact_target_matches or metric_matches,
        identities_observed,
    )


def _verified_ai_lineage(evidence: Mapping[str, Any], field_name: str) -> bool:
    if evidence.get("verification_origin") not in AI_VERIFICATION_ORIGINS:
        return False
    lineage = evidence.get("lineage_evidence")
    if not isinstance(lineage, Mapping):
        return False
    if (
        not evidence.get("source_url")
        or evidence.get("source_url_reachable") is not True
        or not evidence.get("source_content_sha256")
        or not evidence.get("publisher")
        or not evidence.get("distributor")
        or not evidence.get("acquisition_provider")
        or evidence.get("field_lineage_verified") is not True
    ):
        return False
    evidence_field = evidence.get("field") or evidence.get("field_name")
    value_sha256 = str(evidence.get("value_sha256") or "")
    return (
        str(evidence_field) == field_name
        and str(lineage.get("field") or lineage.get("field_name") or "")
        == field_name
        and bool(value_sha256)
        and lineage.get("value_sha256") == value_sha256
        and lineage.get("publisher") == evidence.get("publisher")
        and lineage.get("distributor") == evidence.get("distributor")
        and lineage.get("acquisition_provider")
        == evidence.get("acquisition_provider")
        and lineage.get("source_url") == evidence.get("source_url")
        and lineage.get("source_content_sha256")
        == evidence.get("source_content_sha256")
    )


def _source_field_evidence(
    evidence: Mapping[str, Any],
) -> Mapping[str, Any]:
    if (
        evidence.get("schema_version")
        == FIELD_OBSERVATION_SCHEMA_VERSION
    ):
        source = evidence.get("source_evidence")
        return source if isinstance(source, Mapping) else {}
    return evidence


def _ai_evidence_reason_codes(
    target: CapabilityTarget,
    field_name: str,
    outcome: ProbeOutcome,
) -> tuple[str, ...]:
    if not _is_ai_provider(target.provider_type):
        return ()
    if outcome.configured is not True or _transport_valid(outcome) is not True:
        return ()
    if _optional_text(
        _value(target.registration, "terminal_audit_reason", None)
    ):
        return ()
    raw = _field_evidence(target, field_name, outcome)
    evidence = raw if isinstance(raw, Mapping) else {}
    return _ai_evidence_reason_codes_from_evidence(field_name, evidence)


def _ai_evidence_reason_codes_from_evidence(
    field_name: str,
    evidence: Mapping[str, Any],
) -> tuple[str, ...]:
    reasons: list[str] = []
    source_evidence = _source_field_evidence(evidence)
    if not source_evidence:
        return ("AI_FIELD_EVIDENCE_MISSING",)
    evidence = source_evidence
    if evidence.get("value") is None:
        if (
            evidence.get("field_observed") is True
            and evidence.get("explicit_null") is True
            and evidence.get("explicit_null_verified") is True
            and str(evidence.get("null_reason") or "").strip()
        ):
            return (str(evidence["null_reason"]).strip(),)
        return ("AI_EXPLICIT_NULL_REASON_MISSING",)
    if not evidence.get("source_url"):
        reasons.append("AI_SOURCE_URL_MISSING")
    if evidence.get("source_url_reachable") is not True:
        reasons.append("AI_SOURCE_NOT_VERIFIED")
    if evidence.get("verification_origin") not in AI_VERIFICATION_ORIGINS:
        reasons.append("AI_EVIDENCE_NOT_SERVER_ACQUIRED")
    if evidence.get("occurrence_verified") is not True:
        reasons.append("AI_OCCURRENCE_AMBIGUOUS")
    if evidence.get("request_correlation_verified") is not True:
        reasons.append("AI_REQUEST_CORRELATION_UNVERIFIED")
    if evidence.get("reference_period_verified") is not True:
        reasons.append("AI_REFERENCE_PERIOD_UNVERIFIED")
    if evidence.get("invented") is True or evidence.get("model_knowledge_only") is True:
        reasons.append("AI_VALUE_INVENTED_OR_MODEL_KNOWLEDGE")
    if evidence.get("value_present") is not True or evidence.get("value") is None:
        reasons.append(
            str(evidence.get("null_reason") or "AI_SUBSTANTIVE_VALUE_MISSING")
        )
    if not _verified_ai_lineage(evidence, field_name):
        reasons.append("AI_FIELD_LINEAGE_UNVERIFIED")
    return tuple(reasons)


def _health_severity(status: HealthStatus) -> int:
    order = {
        HealthStatus.HEALTHY: 0,
        HealthStatus.DEGRADED: 1,
        HealthStatus.NOT_CONFIGURED: 2,
        HealthStatus.RATE_LIMITED: 3,
        HealthStatus.DOWN: 4,
        HealthStatus.AUTH_FAILED: 5,
        HealthStatus.UNUSABLE: 6,
        HealthStatus.UNKNOWN: 7,
    }
    return order[status]


def _all_check_names() -> tuple[str, ...]:
    return tuple(component[1] for component in QUALITY_COMPONENTS)


def _capabilities(registration: Any) -> tuple[Any, ...]:
    return tuple(_value(registration, "capabilities", ()) or ())


def _value(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _enum_value(value: Any) -> str:
    if hasattr(value, "value"):
        return str(value.value)
    return str(value)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _enum_value(value).strip()
    return text or None


def _positive_float(value: Any, *, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) and parsed > 0 else default


def _positive_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _normalized_filter(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                item.strip().casefold()
                for raw in values
                for item in str(raw).split(",")
                if item.strip()
            }
        )
    )


def _is_ai_provider(provider_type: str) -> bool:
    return "AI" in provider_type.upper() or "RESEARCH" in provider_type.upper()


def _validation_errors(
    validation: Mapping[str, Any] | Sequence[str] | None,
) -> list[str]:
    if validation is None:
        return []
    if isinstance(validation, Mapping):
        errors = validation.get("errors", ())
        valid = validation.get("valid", not errors)
        if valid:
            return []
        return [str(item) for item in errors] or ["REGISTRY_VALIDATION_FAILED"]
    return [str(item) for item in validation]


def _public_registration(registration: Any) -> Any:
    result = _json_safe(registration)
    if isinstance(result, Mapping):
        return {
            key: value
            for key, value in result.items()
            if not any(
                marker in str(key).casefold()
                for marker in ("secret", "token", "password", "credential_value")
            )
        }
    return result


def _json_safe(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _json_safe(asdict(value))
    if hasattr(value, "model_dump"):
        return _json_safe(value.model_dump(mode="json"))
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, bytes):
        return {"sha256": hashlib.sha256(value).hexdigest(), "size_bytes": len(value)}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if callable(value):
        return f"{value.__module__}.{value.__qualname__}"
    return repr(value)


def _sqlite_bundle_files(source: Path) -> list[Path]:
    candidates = [source, Path(f"{source}-wal"), Path(f"{source}-shm")]
    return [item for item in candidates if item.is_file()]


def _hash_files(paths: Sequence[Path]) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for path in paths:
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
                size += len(chunk)
        result[str(path)] = {"size_bytes": size, "sha256": digest.hexdigest()}
    return result


__all__ = [
    "AuditExecution",
    "AuditFilters",
    "AuditStatus",
    "CapabilityTarget",
    "CapturedHttpExchange",
    "DatabaseBundleGuard",
    "FallbackDecision",
    "FALLBACK_ROLES",
    "FallbackObservation",
    "FIELD_OBSERVATION_SCHEMA_VERSION",
    "HealthStatus",
    "ProbeExecutionError",
    "ProbeExecutor",
    "ProbeOutcome",
    "ProbeRequest",
    "PRIMARY_ROLES",
    "ProviderCapabilityAuditEngine",
    "SystemHealth",
    "TERMINAL_HEALTH_STATUSES",
    "build_probe_requests",
    "determine_system_health",
    "evaluate_capability",
    "evaluate_fallback_chain",
    "normalized_field_observation",
    "quality_score_formula",
    "select_capability_targets",
    "stable_sha256",
    "validate_capability_result_derivations",
    "verify_policy_fallback_chains",
]
