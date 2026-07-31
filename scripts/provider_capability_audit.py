from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import inspect
import ipaddress
import json
import re
import shutil
import socket
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import asdict, is_dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.core.config import Settings  # noqa: E402
from app.core.redaction import redact_payload, redact_sensitive  # noqa: E402
from app.infrastructure.persistence.provider_cache_repository import (  # noqa: E402
    ProviderCacheRepository,
)
from app.services.provider_audit_artifacts import (  # noqa: E402
    ProviderAuditArtifactWriter,
    ProviderAuditPublicationRejected,
    publish_verified_audit_candidate,
)
from app.services.provider_capability_audit import (  # noqa: E402
    AuditFilters,
    CapturedHttpExchange,
    DatabaseBundleGuard,
    HealthStatus,
    ProbeExecutionError,
    ProbeOutcome,
    ProbeRequest,
    ProviderCapabilityAuditEngine,
    _configuration_reasons,
)
from app.services.provider_capability_registry import (  # noqa: E402
    DATASET_SOURCE_POLICIES,
    FIELD_VALIDATOR_SCHEMAS,
    KNOWN_NUMERIC_MEASUREMENT_UNITS,
    PROVIDER_REGISTRY,
    capability_field_measurement_contract,
    dataset_policy_by_id,
    effective_capture_mode,
    field_value_type_contract,
    registry_summary,
    validate_registry,
)
from app.services.source_policy_service import SourcePolicyService  # noqa: E402
from scripts.validate_senior_analyst_payload import (  # noqa: E402
    _verified_capability_audit,
)


UNSAFE_COMPOSITE_ADAPTERS = frozenset(
    {
        "EarningsProvider",
    }
)
AI_RUNTIME_COMPONENT_ADAPTERS = frozenset(
    {
        "app.services.research_source_gateway:ResearchSourceGateway",
        (
            "app.services.evidence_verification_service:"
            "DeterministicEvidenceVerifier"
        ),
        "app.services.agentic_research_runtime:AgenticResearchRuntime",
        "app.services.ai_research_worker:AIResearchWorker",
    }
)
LOCAL_CAPTURE_PROVIDER_TYPES = frozenset(
    {
        "MANUAL_FILE",
        "RECONCILIATION",
        "REPOSITORY",
        "TRANSFORMATION",
    }
)
_SOURCE_SPECIFIC_NOT_HANDLED = object()
_LINEAGE_VALUE_UNSET = object()


class _AuditBackendProxy:
    """Observe one backend result while preserving the production worker path."""

    def __init__(self, backend: Any) -> None:
        self._backend = backend
        self.backend_name = str(
            getattr(backend, "backend_name", "audit_backend")
        )
        self.last_result: Any = None
        self.last_error: ProbeExecutionError | None = None
        self.last_exception: Exception | None = None
        self.observed_tool_events: list[Mapping[str, Any]] = []

    def execute_research(self, **kwargs: Any) -> Any:
        downstream_observer = kwargs.get("event_observer")

        def observe(event: Mapping[str, Any]) -> None:
            if isinstance(event, Mapping):
                self.observed_tool_events.append(dict(event))
                counts = _research_tool_attempt_counts(
                    self.observed_tool_events
                )
                if counts["search"] > 1 or counts["source_open"] > 1:
                    error = ProbeExecutionError(
                        "research backend exceeded the observed audit tool budget",
                        status=HealthStatus.UNUSABLE,
                        reason_code="RESEARCH_TOOL_BUDGET_EXCEEDED",
                    )
                    error.dispatch_observation = {
                        "schema_version": (
                            "provider-audit-dispatch-observation-v1"
                        ),
                        "origin": "BACKEND_EVENT_OBSERVER",
                        "backend_class": type(self._backend).__qualname__,
                        "budget_stop_observed": True,
                        "events_observed": len(self.observed_tool_events),
                        "search_attempts": counts["search"],
                        "source_open_attempts": counts["source_open"],
                        "event_sha256": [
                            _stable_sha256(
                                redact_payload(_json_safe(observed_event))
                            )
                            for observed_event in self.observed_tool_events
                        ],
                    }
                    self.last_error = error
                    raise error
            if callable(downstream_observer):
                downstream_observer(event)

        kwargs["event_observer"] = observe
        try:
            self.last_result = self._backend.execute_research(**kwargs)
        except ProbeExecutionError as exc:
            self.last_error = exc
            raise
        except Exception as exc:
            self.last_exception = exc
            raise
        return self.last_result


def _backend_process_failure_observation(
    exc: Exception | None,
) -> dict[str, Any] | None:
    diagnostic = getattr(exc, "diagnostic", None)
    if not isinstance(diagnostic, Mapping):
        return None
    category = str(diagnostic.get("category") or "UNKNOWN").upper()
    candidate = diagnostic.get("process_attestation")
    if not isinstance(candidate, Mapping):
        return None
    normalized = redact_payload(_json_safe(candidate))
    if not isinstance(normalized, dict):
        return None
    if _subprocess_result_attestation(normalized) is None:
        return None
    transport_status = {
        "AUTH_UNAVAILABLE": HealthStatus.AUTH_FAILED.value,
        "RATE_LIMIT": HealthStatus.RATE_LIMITED.value,
        "TIMEOUT": HealthStatus.DOWN.value,
        "NETWORK_TRANSIENT": HealthStatus.DOWN.value,
        "BACKEND_5XX": HealthStatus.DOWN.value,
        "TRANSIENT_INTERRUPTION": HealthStatus.DOWN.value,
    }.get(category, HealthStatus.UNUSABLE.value)
    return {
        "category": category,
        "transport_status": transport_status,
        "reason_code": (
            str(getattr(exc, "reason_code", "") or "")
            or (
                "PROBE_TIMEOUT"
                if category == "TIMEOUT"
                else f"CODEX_CLI_{category}"
            )
        ),
        "process_attestation": normalized,
    }


class _AuditLiveCapability:
    @staticmethod
    def probe(*, persist: bool = True) -> dict[str, str]:
        del persist
        return {"status": "LIVE_VERIFIED"}


class _AuditResearchSourceStore:
    """Minimal request-local store for a path-bound gateway control probe."""

    policy = SourcePolicyService()

    def research_source_for_url(self, run_id: str, url: str) -> None:
        del run_id, url
        return None

    def research_source_for_hash(
        self,
        run_id: str,
        content_hash: str,
    ) -> None:
        del run_id, content_hash
        return None

    def persist_research_source(
        self,
        run_id: str,
        source: dict[str, Any],
    ) -> dict[str, Any]:
        del run_id
        return source

    def research_sources(self, run_id: str) -> list[dict[str, Any]]:
        del run_id
        return []

    def record_evidence_verification(
        self,
        run_id: str,
        verification: dict[str, Any],
    ) -> dict[str, Any]:
        del run_id
        return verification

    def mark_research_source_verified(
        self,
        source_id: str,
        verification: dict[str, Any],
    ) -> None:
        del source_id, verification


class _SourceUrlRejected(Exception):
    pass


def _remaining_probe_timeout(deadline: float) -> float:
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        raise asyncio.TimeoutError
    return remaining


class HttpxAuditCapture(AbstractContextManager["HttpxAuditCapture"]):
    """Captures exact HTTPX response bytes inside this dedicated audit process."""

    def __init__(self, request: ProbeRequest) -> None:
        self.request = request
        self.exchanges: list[CapturedHttpExchange] = []
        self._fingerprint_attempts: Counter[str] = Counter()
        self._attempted_send_count = 0
        self._original_async_send: Any = None
        self._original_sync_send: Any = None

    def __enter__(self) -> HttpxAuditCapture:
        self._original_async_send = httpx.AsyncClient.send
        self._original_sync_send = httpx.Client.send
        capture = self

        async def async_send(
            client: httpx.AsyncClient,
            request: httpx.Request,
            *args: Any,
            **kwargs: Any,
        ) -> httpx.Response:
            fingerprint, attempt = capture._before(request)
            started = time.perf_counter()
            response = await capture._original_async_send(client, request, *args, **kwargs)
            body = await response.aread()
            capture._record(request, response, body, started, attempt)
            capture._fingerprint_attempts[fingerprint] = attempt
            return response

        def sync_send(
            client: httpx.Client,
            request: httpx.Request,
            *args: Any,
            **kwargs: Any,
        ) -> httpx.Response:
            fingerprint, attempt = capture._before(request)
            started = time.perf_counter()
            response = capture._original_sync_send(client, request, *args, **kwargs)
            body = response.read()
            capture._record(request, response, body, started, attempt)
            capture._fingerprint_attempts[fingerprint] = attempt
            return response

        httpx.AsyncClient.send = async_send
        httpx.Client.send = sync_send
        return self

    def __exit__(self, *exc_info: Any) -> None:
        httpx.AsyncClient.send = self._original_async_send
        httpx.Client.send = self._original_sync_send

    def _before(self, request: httpx.Request) -> tuple[str, int]:
        fingerprint = _request_fingerprint(request)
        attempt = self._fingerprint_attempts[fingerprint] + 1
        if attempt > self.request.max_attempts:
            raise ProbeExecutionError(
                "registered request fingerprint retry limit exceeded",
                status=HealthStatus.UNUSABLE,
                reason_code="PROBE_MAX_ATTEMPTS_EXCEEDED",
            )
        if self._attempted_send_count >= self.call_budget:
            raise ProbeExecutionError(
                "registered acquisition call budget exceeded",
                status=HealthStatus.UNUSABLE,
                reason_code="PROBE_CALL_BUDGET_EXCEEDED",
            )
        # Count an attempted send before handing control to the transport.  A
        # timeout or connection failure has no response to record, but it is
        # still a real provider attempt and must remain visible to the audit.
        self._attempted_send_count += 1
        self._fingerprint_attempts[fingerprint] = attempt
        return fingerprint, attempt

    @property
    def attempts(self) -> int:
        return self._attempted_send_count

    @property
    def call_budget(self) -> int:
        value = getattr(
            self.request,
            "call_budget",
            self.request.max_attempts,
        )
        return max(1, int(value))

    def _record(
        self,
        request: httpx.Request,
        response: httpx.Response,
        body: bytes,
        started: float,
        attempt: int,
    ) -> None:
        self.exchanges.append(
            CapturedHttpExchange(
                method=request.method,
                url=redact_sensitive(str(request.url)),
                request_headers=tuple(request.headers.multi_items()),
                status_code=response.status_code,
                response_headers=tuple(response.headers.multi_items()),
                response_body=body,
                latency_ms=round((time.perf_counter() - started) * 1000, 3),
                attempt=attempt,
            )
        )


class IsolatedRegistryProbeExecutor:
    """Calls a registered real adapter once, with all persistence redirected to a sandbox."""

    requires_real_dispatch = True

    def __init__(
        self,
        settings: Settings,
        *,
        source_url_client_factory: Any | None = None,
        source_url_resolver: Any | None = None,
        allow_test_source_urls: bool = False,
    ) -> None:
        self.settings = settings
        self._source_url_client_factory = source_url_client_factory
        self._source_url_resolver = (
            source_url_resolver or _resolve_source_host
        )
        self._allow_test_source_urls = allow_test_source_urls
        self._source_policy = SourcePolicyService(
            settings.source_policy_path
        )
        self._source_url_verification_cache: dict[
            tuple[str, str],
            dict[str, Any],
        ] = {}

    async def __call__(self, request: ProbeRequest) -> ProbeOutcome:
        registration = request.registration
        terminal_audit_reason = str(
            getattr(registration, "terminal_audit_reason", None)
            or (
                registration.get("terminal_audit_reason")
                if isinstance(registration, Mapping)
                else ""
            )
            or ""
        ).strip()
        configuration_reasons = _configuration_reasons(
            registration,
            self.settings,
            request.targets,
        )
        if configuration_reasons:
            outcome = ProbeOutcome.not_configured(*configuration_reasons)
            outcome.evidence = {
                "real_adapter_invoked": False,
                "probe_dispatch_status": "NOT_CONFIGURED",
                "terminal_audit_reason": terminal_audit_reason or None,
            }
            return outcome
        if terminal_audit_reason:
            return _terminal_unsupported_outcome(terminal_audit_reason)
        adapter_path = request.adapter_path
        if not adapter_path:
            return _unsupported_outcome("REGISTERED_ADAPTER_PATH_MISSING")
        adapter_type = _load_symbol(adapter_path)
        adapter_name = getattr(adapter_type, "__name__", str(adapter_type))
        if (
            adapter_name in UNSAFE_COMPOSITE_ADAPTERS
            and request.acquisition_kind != "RUNTIME_ADAPTER_COVERAGE"
        ):
            return _unsupported_outcome("ISOLATED_REAL_PROBE_NOT_IMPLEMENTED")
        request_settings = _request_settings(self.settings, request)
        try:
            adapter = self._construct(adapter_type, request, request_settings)
        except Exception as exc:
            return _unsupported_outcome(
                "ISOLATED_REAL_PROBE_NOT_IMPLEMENTED",
                error_kind=type(exc).__name__,
            )
        if (
            request.acquisition_kind == "RUNTIME_ADAPTER_COVERAGE"
            and adapter_path in AI_RUNTIME_COMPONENT_ADAPTERS
        ):
            return await _runtime_source_component_probe(
                adapter,
                request,
                adapter_path=adapter_path,
            )
        if adapter_name in UNSAFE_COMPOSITE_ADAPTERS:
            return _safe_composite_runtime_adapter_probe(
                adapter,
                request,
                adapter_path=adapter_path,
            )

        started = time.perf_counter()
        deadline = asyncio.get_running_loop().time() + float(
            request.timeout_seconds
        )
        with HttpxAuditCapture(request) as capture:
            try:
                invocation = self._invoke(adapter, request)
                if _expected_capture_mode(request) == "SUBPROCESS":
                    # The registered subprocess adapters own their Popen
                    # watchdog and process-group cleanup. Cancelling their
                    # to_thread path here could return while the child was
                    # still being reaped and would destroy its attestation.
                    value = await invocation
                else:
                    value = await asyncio.wait_for(
                        invocation,
                        timeout=_remaining_probe_timeout(deadline),
                    )
            except (asyncio.TimeoutError, httpx.TimeoutException):
                return _failed_transport_outcome(
                    request,
                    capture,
                    started,
                    HealthStatus.DOWN,
                    "PROBE_TIMEOUT",
                    "TimeoutError",
                )
            except httpx.TransportError as exc:
                return _failed_transport_outcome(
                    request,
                    capture,
                    started,
                    HealthStatus.DOWN,
                    "PROVIDER_TRANSPORT_FAILED",
                    type(exc).__name__,
                )
            except ProbeExecutionError as exc:
                if exc.status is HealthStatus.NOT_CONFIGURED:
                    outcome = ProbeOutcome.not_configured(exc.reason_code)
                    outcome.evidence = {
                        "real_adapter_invoked": False,
                        "probe_dispatch_status": "NOT_CONFIGURED",
                        "adapter_path": adapter_path,
                        "adapter_constructed": True,
                    }
                    return outcome
                return _failed_transport_outcome(
                    request,
                    capture,
                    started,
                    exc.status,
                    exc.reason_code,
                    type(exc).__name__,
                    dispatch_status=(
                        "FAILED"
                        if exc.reason_code
                        in {
                            "ISOLATED_REAL_PROBE_NOT_IMPLEMENTED",
                            "LOCAL_CAPABILITY_PROBE_NOT_IMPLEMENTED",
                        }
                        else "REAL_ADAPTER"
                    ),
                )
            except Exception as exc:
                return _failed_transport_outcome(
                    request,
                    capture,
                    started,
                    (
                        HealthStatus.UNUSABLE
                        if capture.exchanges
                        and 200 <= capture.exchanges[-1].status_code < 400
                        else HealthStatus.UNKNOWN
                    ),
                    (
                        "ADAPTER_RESPONSE_PARSE_FAILED"
                        if capture.exchanges
                        and 200 <= capture.exchanges[-1].status_code < 400
                        else "ADAPTER_RUNTIME_FAILED"
                    ),
                    type(exc).__name__,
                    dispatch_status="REAL_ADAPTER",
                )
            normalized = redact_payload(_json_safe(value))
            subprocess_attestation = (
                _subprocess_result_attestation(normalized)
                if _expected_capture_mode(request) == "SUBPROCESS"
                else None
            )
            backend_failure = (
                normalized.get("backend_failure_observation")
                if isinstance(normalized, Mapping)
                else None
            )
            subprocess_terminal_failure = bool(
                subprocess_attestation is not None
                and (
                    subprocess_attestation.get("bounded_timeout_observed")
                    is True
                    or bool(subprocess_attestation.get("failure_reason"))
                    or (
                        type(subprocess_attestation.get("exit_code")) is int
                        and subprocess_attestation["exit_code"] != 0
                    )
                    or isinstance(backend_failure, Mapping)
                )
            )
            if subprocess_terminal_failure:
                source_url_verifications = []
                lineage_exchanges = tuple(capture.exchanges)
            else:
                source_url_deadline = deadline
                if _expected_capture_mode(request) == "SUBPROCESS":
                    # A subprocess adapter owns a separate bounded execution
                    # watchdog. Give successful output its own bounded source
                    # verification window after the child has been reaped;
                    # reusing the process deadline would reject a legitimate
                    # result solely because process execution consumed it.
                    source_url_deadline = (
                        asyncio.get_running_loop().time()
                        + float(request.timeout_seconds)
                    )
                try:
                    (
                        source_url_verifications,
                        lineage_exchanges,
                    ) = await asyncio.wait_for(
                        self._verify_ai_source_urls(
                            request,
                            normalized,
                            capture,
                        ),
                        timeout=_remaining_probe_timeout(
                            source_url_deadline
                        ),
                    )
                except (asyncio.TimeoutError, httpx.TimeoutException):
                    return _failed_transport_outcome(
                        request,
                        capture,
                        started,
                        HealthStatus.DOWN,
                        "PROBE_TIMEOUT",
                        "TimeoutError",
                    )
        # The normalized observation is itself part of the signed audit
        # evidence. Redact it before hashing/deriving field evidence so the
        # in-memory digest and persisted artifact always describe the same
        # canonical value, even when a provider echoes a credentialed URL.
        subprocess_output = (
            _subprocess_output_bytes(normalized, request.sandbox_root)
            if _expected_capture_mode(request) == "SUBPROCESS"
            else None
        )
        latency_ms = round((time.perf_counter() - started) * 1000, 3)
        status, http_status, transport_reasons = _transport_classification(
            capture.exchanges,
            registration,
        )
        checks, field_checks = _evidence_checks(
            request,
            normalized,
            lineage_exchanges,
        )
        if status not in {"OK", "SUCCESS"}:
            checks = {name: None for name in checks}
            field_checks = {}
        capture_evidence = {
            **_capture_attestation(
                request,
                normalized,
                lineage_exchanges,
                subprocess_output=subprocess_output,
            ),
            "capture_mode_configuration": (
                _capture_mode_configuration(request)
            ),
        }
        capture_attestation = capture_evidence.get("capture_attestation")
        if (
            isinstance(capture_attestation, Mapping)
            and capture_attestation.get("bounded_timeout_observed") is True
        ):
            status = HealthStatus.DOWN.value
            checks = {name: None for name in checks}
            field_checks = {}
            transport_reasons = [*transport_reasons, "PROBE_TIMEOUT"]
        if isinstance(backend_failure, Mapping):
            observed_status = str(
                backend_failure.get("transport_status") or ""
            ).upper()
            observed_reason = str(
                backend_failure.get("reason_code") or ""
            ).upper()
            if observed_status in {
                HealthStatus.AUTH_FAILED.value,
                HealthStatus.RATE_LIMITED.value,
                HealthStatus.DOWN.value,
                HealthStatus.UNUSABLE.value,
            } and observed_reason:
                status = observed_status
                checks = {name: None for name in checks}
                field_checks = {}
                transport_reasons = [
                    *transport_reasons,
                    observed_reason,
                ]
        transport_reasons = list(dict.fromkeys(transport_reasons))
        if capture_evidence["capture_verified"] is not True:
            status = HealthStatus.UNUSABLE.value
            checks = {name: None for name in checks}
            field_checks = {}
            transport_reasons = [
                *transport_reasons,
                "PROBE_ACQUISITION_NOT_CAPTURED",
            ]
        raw_response = (
            subprocess_output
            if _expected_capture_mode(request) == "SUBPROCESS"
            else capture.exchanges[-1].response_body
            if capture.exchanges
            else None
        )
        headers = (
            capture.exchanges[-1].response_headers
            if capture.exchanges
            else ()
        )
        return ProbeOutcome(
            configured=True,
            transport_status=status,
            http_status=http_status,
            headers=headers,
            raw_response=raw_response,
            normalized_response=normalized,
            latency_ms=latency_ms,
            attempts=max(
                capture.attempts
                + _backend_tool_attempt_count(normalized)
                + _backend_invocation_attempt_count(request, normalized),
                1,
            ),
            reason_codes=tuple(transport_reasons),
            checks=checks,
            field_checks=field_checks,
            evidence={
                "real_adapter_invoked": True,
                "probe_dispatch_status": "REAL_ADAPTER",
                "adapter_path": adapter_path,
                "adapter_constructed": True,
                "probe_id": request.targets[0].probe_id,
                "network_call_count": capture.attempts,
                "backend_tool_attempt_count": (
                    _backend_tool_attempt_count(normalized)
                ),
                "backend_invocation_attempt_count": (
                    _backend_invocation_attempt_count(request, normalized)
                ),
                "request_correlations": _json_safe(request.request_correlations),
                **capture_evidence,
                "fields": _extract_field_evidence(
                    request,
                    normalized,
                    lineage_exchanges,
                ),
                "source_url_verifications": source_url_verifications,
            },
            network_exchanges=lineage_exchanges,
        )

    async def _verify_ai_source_urls(
        self,
        request: ProbeRequest,
        normalized: Any,
        capture: HttpxAuditCapture,
    ) -> tuple[list[dict[str, Any]], tuple[CapturedHttpExchange, ...]]:
        if not any(
            "AI" in str(target.provider_type).upper()
            for target in request.targets
        ):
            return [], tuple(capture.exchanges)

        bindings = _ai_source_url_bindings(normalized)
        grouped: dict[str, dict[str, Any]] = {}
        for binding in bindings:
            normalized_url = _normalized_source_url(binding["source_url"])
            if normalized_url is None:
                continue
            row = grouped.setdefault(
                normalized_url,
                {
                    "source_url": binding["source_url"],
                    "normalized_url": normalized_url,
                    "bindings": [],
                },
            )
            row["bindings"].append(
                {
                    key: binding.get(key)
                    for key in (
                        "field_semantics",
                        "value_sha256",
                        "publisher",
                        "occurrence_id",
                        "reference_period",
                        "evidence_text",
                        "metric_id",
                        "frequency",
                        "unit",
                        "_value",
                    )
                }
            )

        public_rows: list[dict[str, Any]] = []
        binding_exchanges = list(capture.exchanges)
        for normalized_url, row in grouped.items():
            cache_key = (request.run_id, normalized_url)
            cached = self._source_url_verification_cache.get(cache_key)
            reused = cached is not None
            if cached is None:
                cached = await self._fetch_source_url_once(
                    request,
                    str(row["source_url"]),
                    normalized_url,
                    capture,
                )
                self._source_url_verification_cache[cache_key] = cached
            exchange = cached.get("_exchange")
            if isinstance(exchange, CapturedHttpExchange) and exchange not in binding_exchanges:
                binding_exchanges.append(exchange)
            public_bindings = []
            for binding in row["bindings"]:
                binding_safety_reason = self._source_url_safety_reason(
                    str(row["source_url"]),
                    allowed_domains=_allowed_source_domains_for_binding(
                        request,
                        binding,
                    ),
                )
                publisher = str(binding.get("publisher") or "").strip()
                if binding_safety_reason is None and (
                    not publisher
                    or not _publisher_matches_source(
                        self._source_policy,
                        publisher,
                        str(row["source_url"]),
                    )
                ):
                    binding_safety_reason = (
                        "SOURCE_URL_PUBLISHER_HOST_MISMATCH"
                    )
                content_match = bool(
                    binding_safety_reason is None
                    and
                    isinstance(exchange, CapturedHttpExchange)
                    and _exchange_supports_claim(
                        exchange,
                        field_name=str(
                            binding.get("field_semantics") or ""
                        ),
                        value=binding.get("_value"),
                        candidate=binding,
                    )
                )
                public_bindings.append(
                    {
                        key: value
                        for key, value in {
                            **binding,
                            "content_claim_match": content_match,
                            "binding_verification_reason": (
                                binding_safety_reason
                                or (
                                    None
                                    if content_match
                                    else "SOURCE_URL_CLAIM_CONTEXT_MISMATCH"
                                )
                            ),
                        }.items()
                        if not key.startswith("_")
                    }
                )
            public_row = {
                    key: value
                    for key, value in {
                        **cached,
                        "bindings": public_bindings,
                        "reused_from_run_cache": reused,
                        "shared_verification_binding": (
                            cached.get("origin_acquisition_id")
                            if reused
                            else None
                        ),
                    }.items()
                    if not key.startswith("_")
                }
            content_certified = bool(
                public_bindings
                and all(
                    binding["content_claim_match"] is True
                    for binding in public_bindings
                )
            )
            public_row["transport_verified"] = cached.get("verified") is True
            public_row["verified"] = bool(
                cached.get("verified") is True and content_certified
            )
            if cached.get("verified") is True and not content_certified:
                public_row["reason_code"] = (
                    "SOURCE_URL_CLAIM_CONTEXT_MISMATCH"
                )
            public_rows.append(public_row)
        return public_rows, tuple(binding_exchanges)

    async def _fetch_source_url_once(
        self,
        request: ProbeRequest,
        source_url: str,
        normalized_url: str,
        capture: HttpxAuditCapture,
    ) -> dict[str, Any]:
        before = len(capture.exchanges)
        attempts_before = capture.attempts
        reason_code: str | None = None
        response: httpx.Response | None = None
        allowed_domains = _allowed_source_domains(request)
        safety_reason = self._source_url_safety_reason(
            source_url,
            allowed_domains=allowed_domains,
        )
        if safety_reason is not None:
            reason_code = safety_reason
        try:
            if reason_code is not None:
                raise _SourceUrlRejected
            client = (
                self._source_url_client_factory()
                if self._source_url_client_factory is not None
                else httpx.AsyncClient(
                    follow_redirects=False,
                    timeout=min(float(request.timeout_seconds), 10.0),
                )
            )
            async with client:
                response = await client.get(source_url)
        except _SourceUrlRejected:
            pass
        except ProbeExecutionError:
            reason_code = "SOURCE_URL_VERIFICATION_BUDGET_EXHAUSTED"
        except (httpx.HTTPError, OSError, ValueError):
            reason_code = "SOURCE_URL_UNREACHABLE"

        exchange = (
            capture.exchanges[-1]
            if len(capture.exchanges) > before
            else None
        )
        if response is not None and 300 <= response.status_code < 400:
            redirect_host = _redirect_host(response, source_url)
            source_host = httpx.URL(source_url).host.casefold()
            reason_code = (
                "SOURCE_URL_REDIRECT_HOST_MISMATCH"
                if redirect_host and redirect_host.casefold() != source_host
                else "SOURCE_URL_REDIRECT_NOT_FOLLOWED"
            )
        elif response is not None and not 200 <= response.status_code < 300:
            reason_code = f"SOURCE_URL_HTTP_{response.status_code}"

        verified = bool(
            response is not None
            and 200 <= response.status_code < 300
            and isinstance(exchange, CapturedHttpExchange)
        )
        if not verified and reason_code is None:
            reason_code = "SOURCE_URL_EXCHANGE_NOT_CAPTURED"
        return {
            "verification_id": hashlib.sha256(
                normalized_url.encode("utf-8")
            ).hexdigest(),
            "source_url": normalized_url,
            "method": "GET",
            "attempts": capture.attempts - attempts_before,
            "status_code": (
                response.status_code if response is not None else None
            ),
            "verified": verified,
            "reason_code": reason_code,
            "response_body_sha256": (
                hashlib.sha256(exchange.response_body).hexdigest()
                if verified and isinstance(exchange, CapturedHttpExchange)
                else None
            ),
            "response_size_bytes": (
                len(exchange.response_body)
                if verified and isinstance(exchange, CapturedHttpExchange)
                else None
            ),
            "_exchange": exchange if verified else None,
            "origin_acquisition_id": request.acquisition_id,
        }

    def _source_url_safety_reason(
        self,
        source_url: str,
        *,
        allowed_domains: tuple[str, ...],
    ) -> str | None:
        validation = self._source_policy.validate_url(
            source_url,
            require_https=True,
            allow_test_reserved=self._allow_test_source_urls,
        )
        if not validation.accepted:
            return str(
                validation.reason_code or "SOURCE_URL_POLICY_REJECTED"
            )
        try:
            parsed = urlsplit(source_url)
            if parsed.port not in (None, 443):
                return "SOURCE_URL_NONSTANDARD_PORT"
        except ValueError:
            return "SOURCE_URL_INVALID_PORT"
        host = str(parsed.hostname or "").casefold().rstrip(".")
        if not allowed_domains or not any(
            _domain_matches(host, allowed)
            for allowed in allowed_domains
        ):
            return "SOURCE_URL_NOT_IN_CAPABILITY_ALLOWLIST"
        try:
            addresses = tuple(self._source_url_resolver(host))
        except OSError:
            return "SOURCE_URL_DNS_RESOLUTION_FAILED"
        if not addresses:
            return "SOURCE_URL_DNS_RESOLUTION_EMPTY"
        for raw_address in addresses:
            try:
                address = ipaddress.ip_address(raw_address)
            except ValueError:
                return "SOURCE_URL_DNS_INVALID_ADDRESS"
            if not address.is_global:
                return "SOURCE_URL_SSRF_NON_GLOBAL_ADDRESS"
        return None

    def _construct(
        self,
        adapter_type: Any,
        request: ProbeRequest,
        request_settings: Settings,
    ) -> Any:
        if not inspect.isclass(adapter_type):
            return adapter_type
        signature = inspect.signature(adapter_type)
        kwargs: dict[str, Any] = {}
        for name, parameter in signature.parameters.items():
            if name == "settings":
                kwargs[name] = request_settings
            elif name == "cache":
                kwargs[name] = ProviderCacheRepository(request_settings.database_path)
            elif name == "database":
                kwargs[name] = request_settings.database_path
            elif name == "market_news_repository":
                from app.services.market_news_repository import MarketNewsRepository

                kwargs[name] = MarketNewsRepository(request_settings)
            elif name == "repository" and adapter_type.__name__ == (
                "ResearchSourceGateway"
            ):
                kwargs[name] = _AuditResearchSourceStore()
            elif name == "network_observer":
                kwargs[name] = None
            elif parameter.default is inspect.Parameter.empty:
                raise TypeError(f"unsupported required constructor parameter: {name}")
        return adapter_type(**kwargs)

    async def _invoke(self, adapter: Any, request: ProbeRequest) -> Any:
        if hasattr(adapter, "enabled") and callable(adapter.enabled) and not adapter.enabled():
            raise ProbeExecutionError(
                "adapter is disabled",
                status=HealthStatus.NOT_CONFIGURED,
                reason_code="PROVIDER_DISABLED_BY_CONFIGURATION",
            )
        if hasattr(adapter, "audit_probe"):
            result = adapter.audit_probe(request)
            if inspect.isawaitable(result):
                result = await result
            return result
        source_specific = await _invoke_registered_source(adapter, request)
        if source_specific is not _SOURCE_SPECIFIC_NOT_HANDLED:
            return source_specific
        target = request.targets[0]
        method, kwargs = _select_probe_method(
            adapter,
            target,
            targets=request.targets,
            request_correlation=request.correlation_for(target),
            request_correlations=request.request_correlations,
        )
        if method is None:
            raise ProbeExecutionError(
                "adapter has no isolated probe surface",
                status=HealthStatus.UNUSABLE,
                reason_code="ISOLATED_REAL_PROBE_NOT_IMPLEMENTED",
            )
        _bind_invocation_correlation(request, kwargs)
        result = method(**kwargs)
        if inspect.isawaitable(result):
            result = await result
        return result


def _safe_composite_runtime_adapter_probe(
    adapter: Any,
    request: ProbeRequest,
    *,
    adapter_path: str,
) -> ProbeOutcome:
    """Attest construction of a composite without executing its fallback tree.

    The atomic capability is already exercised through its registered local
    reconciliation hook. Calling ``fetch`` here would violate source isolation
    by silently invoking multiple external providers, so this adapter-path
    coverage acquisition is deliberately local and side-effect free.
    """

    normalized = {
        "adapter_path": adapter_path,
        "adapter_type": type(adapter).__qualname__,
        "runtime_adapter_constructed": True,
        "source_isolation_preserved": True,
        "delegated_capability_probe_paths": sorted(
            {
                target.probe_adapter_path
                for target in request.targets
                if target.probe_adapter_path
            }
        ),
    }
    raw = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    attestation = {
        "database_snapshot_isolated": request.database_snapshot_path is not None,
        "sandbox_root": str(request.sandbox_root.resolve()),
        "composite_fallback_invoked": False,
    }
    return ProbeOutcome(
        configured=True,
        transport_status="SUCCESS",
        raw_response=raw,
        normalized_response=normalized,
        latency_ms=0.0,
        attempts=1,
        checks={
            "transport_valid": True,
            "schema_valid": None,
            "completeness_valid": None,
            "freshness_valid": None,
            "semantic_mapping_valid": None,
            "occurrence_match_valid": None,
            "lineage_valid": None,
        },
        evidence={
            "real_adapter_invoked": True,
            "probe_dispatch_status": "REAL_ADAPTER",
            "adapter_path": adapter_path,
            "adapter_constructed": True,
            "probe_id": request.targets[0].probe_id,
            "network_call_count": 0,
            "capture_mode": "LOCAL_SANDBOX",
            "capture_mode_expected": "LOCAL_SANDBOX",
            "capture_verified": True,
            "capture_attestation": attestation,
        },
    )


async def _runtime_source_component_probe(
    adapter: Any,
    request: ProbeRequest,
    *,
    adapter_path: str,
) -> ProbeOutcome:
    """Invoke a bounded negative-control surface on one runtime source leaf."""

    adapter_name = type(adapter).__name__
    workspace = (
        request.sandbox_root
        / "runtime-source-components"
        / _safe_path_part(request.acquisition_id)
    )
    workspace.mkdir(parents=True, exist_ok=True)
    if adapter_name == "ResearchSourceGateway":
        method_name = "acquire_many"
        result = await asyncio.wait_for(
            asyncio.to_thread(
                adapter.acquire_many,
                request.run_id,
                [],
            ),
            timeout=request.timeout_seconds,
        )
    elif adapter_name == "DeterministicEvidenceVerifier":
        method_name = "verify"
        result = await asyncio.wait_for(
            asyncio.to_thread(adapter.verify, {}),
            timeout=request.timeout_seconds,
        )
    elif adapter_name == "AgenticResearchRuntime":
        method_name = "run"
        result = await asyncio.wait_for(
            asyncio.to_thread(
                adapter.run,
                {
                    "job_id": f"provider-audit-{request.acquisition_id}",
                    "job_type": "MNQ_MARKET_RESEARCH",
                    "symbol": "MNQ",
                    "request_payload": {"execution_context": None},
                },
                workspace,
                object(),
                1,
            ),
            timeout=request.timeout_seconds,
        )
    elif adapter_name == "AIResearchWorker":
        method_name = "process_once"
        result = await asyncio.wait_for(
            asyncio.to_thread(adapter.process_once),
            timeout=request.timeout_seconds,
        )
    else:  # pragma: no cover - caller allow-list makes this unreachable.
        raise ProbeExecutionError(
            "runtime source component probe is not registered",
            status=HealthStatus.UNUSABLE,
            reason_code="RUNTIME_SOURCE_COMPONENT_PROBE_NOT_REGISTERED",
        )
    normalized = {
        "adapter_path": adapter_path,
        "adapter_type": adapter_name,
        "invoked_method": method_name,
        "runtime_component_invoked": True,
        "negative_control": True,
        "negative_control_result": _json_safe(result),
        "external_source_attempted": False,
        "reason_code": "RUNTIME_COMPONENT_NEGATIVE_CONTROL",
    }
    raw = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return ProbeOutcome(
        configured=True,
        transport_status="SUCCESS",
        raw_response=raw,
        normalized_response=normalized,
        latency_ms=0.0,
        attempts=1,
        checks={name: None for name in (
            "transport_valid",
            "schema_valid",
            "completeness_valid",
            "freshness_valid",
            "semantic_mapping_valid",
            "occurrence_match_valid",
            "lineage_valid",
        )},
        evidence={
            "real_adapter_invoked": True,
            "probe_dispatch_status": "REAL_ADAPTER",
            "adapter_path": adapter_path,
            "adapter_constructed": True,
            "runtime_component_method": method_name,
            "runtime_component_negative_control": True,
            "network_call_count": 0,
            "capture_mode": "LOCAL_SANDBOX",
            "capture_mode_expected": "LOCAL_SANDBOX",
            "capture_verified": True,
            "capture_attestation": {
                "sandbox_root": str(request.sandbox_root.resolve()),
                "runtime_component_method_observed": True,
                "external_source_attempted": False,
            },
        },
    )


def _bind_invocation_correlation(
    request: ProbeRequest,
    kwargs: Mapping[str, Any],
) -> None:
    """Persist only request constraints that were actually passed to an adapter."""

    correlations = [
        request.request_correlations.get(item.target_id)
        for item in request.targets
    ]
    for parameter in ("expected_period", "release_date", "period"):
        value = kwargs.get(parameter)
        if value not in (None, ""):
            for correlation in correlations:
                if isinstance(correlation, dict):
                    correlation["expected_reference_period"] = _json_safe(value)
                    correlation["reference_parameter"] = parameter
            break


async def _invoke_registered_source(adapter: Any, request: ProbeRequest) -> Any:
    """Invoke a leaf of a runtime cascade without permitting its next fallback."""

    provider_id = request.provider_id
    settings = adapter.settings if hasattr(adapter, "settings") else request.settings
    target = request.targets[0]
    if provider_id == "INVESCO" or (
        provider_id == "ALPHA_VANTAGE"
        and target.dataset_id == "nasdaq_100"
    ):
        from app.providers import qqq_holdings_provider as qqq

        async with httpx.AsyncClient(timeout=request.timeout_seconds) as client:
            if provider_id == "INVESCO":
                response = await client.get(
                    settings.invesco_qqq_holdings_url,
                    headers=qqq.REQUEST_HEADERS,
                    timeout=request.timeout_seconds,
                )
                response.raise_for_status()
                holdings, data_as_of, errors = qqq.parse_invesco_holdings_csv(
                    response.text
                )
                return {
                    "provider_id": provider_id,
                    "metric_id": target.metric_id,
                    "frequency": target.frequency,
                    "data_as_of": data_as_of,
                    "holdings": [
                        item.model_dump(mode="json") for item in holdings
                    ],
                    "parse_errors": errors,
                }
            response = await client.get(
                settings.alpha_vantage_base_url,
                params={
                    "function": "ETF_PROFILE",
                    "symbol": "QQQ",
                    "apikey": settings.alpha_vantage_api_key,
                },
                headers=qqq.REQUEST_HEADERS,
                timeout=request.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            qqq.ensure_alpha_payload_ok(payload)
            if qqq.is_alpha_vantage_daily_rate_limited(payload):
                raise ProbeExecutionError(
                    "Alpha Vantage reported its daily rate limit",
                    status=HealthStatus.RATE_LIMITED,
                    reason_code="PROVIDER_RATE_LIMIT_PAYLOAD",
                )
            holdings, errors = qqq.parse_alpha_vantage_etf_profile(payload)
            return {
                "provider_id": provider_id,
                "metric_id": target.metric_id,
                "frequency": target.frequency,
                "holdings": [item.model_dump(mode="json") for item in holdings],
                "parse_errors": errors,
            }

    if provider_id in {
        "ALPHA_VANTAGE",
        "YAHOO_FINANCE_CHART",
        "STOOQ",
        "YAHOO_FINANCE_QUOTE",
    }:
        from app.providers import mega_cap_snapshot_provider as mega

        async with httpx.AsyncClient(timeout=request.timeout_seconds) as client:
            if provider_id == "ALPHA_VANTAGE":
                stocks, errors = await adapter._fetch_alpha_vantage_fallback(  # noqa: SLF001
                    client
                )
                return {
                    "provider_id": provider_id,
                    "metric_id": target.metric_id,
                    "frequency": target.frequency,
                    "stocks": stocks,
                    "parse_errors": errors,
                }
            if provider_id == "YAHOO_FINANCE_CHART":
                symbol = "NVDA"
                response = await client.get(
                    f"{settings.yahoo_chart_url}/{symbol}",
                    params={"range": "5d", "interval": "1d"},
                    headers=mega.REQUEST_HEADERS,
                    timeout=request.timeout_seconds,
                )
                response.raise_for_status()
                rows = [mega.parse_yahoo_chart(symbol, response.json())]
                stocks = [item for item in rows if item]
            elif provider_id == "STOOQ":
                response = await client.get(
                    mega._stooq_url(),  # noqa: SLF001 - registered runtime leaf
                    headers=mega.REQUEST_HEADERS,
                    timeout=request.timeout_seconds,
                )
                response.raise_for_status()
                stocks, errors = mega.parse_stooq_quotes(response.text)
                return {
                    "provider_id": provider_id,
                    "metric_id": target.metric_id,
                    "frequency": target.frequency,
                    "stocks": stocks,
                    "parse_errors": errors,
                }
            else:
                response = await client.get(
                    settings.yahoo_quote_url,
                    params={"symbols": "NVDA"},
                    headers=mega.REQUEST_HEADERS,
                    timeout=request.timeout_seconds,
                )
                response.raise_for_status()
                stocks, errors = mega.parse_yahoo_quotes(response.json())
                return {
                    "provider_id": provider_id,
                    "metric_id": target.metric_id,
                    "frequency": target.frequency,
                    "stocks": stocks,
                    "parse_errors": errors,
                }
        return {
            "provider_id": provider_id,
            "metric_id": target.metric_id,
            "frequency": target.frequency,
            "stocks": stocks,
            "parse_errors": [],
        }

    news_ids = {
        "ALPHA_VANTAGE_NEWS_SENTIMENT",
        "GDELT_DOC_API",
        "FEDERAL_RESERVE_RSS",
        "BLS_RSS",
        "BEA_RSS",
        "YAHOO_FINANCE_RSS",
        "MARKETWATCH_RSS",
        "GOOGLE_NEWS_RSS",
    }
    if provider_id in news_ids:
        from app.providers import news_provider as news

        symbols = ["NVDA", "AAPL", "MSFT", "QQQ"]
        limit = 10
        recency_days = 14
        async with httpx.AsyncClient(timeout=request.timeout_seconds) as client:
            if provider_id == "ALPHA_VANTAGE_NEWS_SENTIMENT":
                return await adapter._fetch_alpha_vantage(  # noqa: SLF001
                    client=client,
                    symbols=symbols,
                    limit=limit,
                    recency_days=recency_days,
                )
            if provider_id == "GDELT_DOC_API":
                return await adapter._fetch_gdelt(  # noqa: SLF001
                    client=client,
                    symbols=symbols,
                    query=" OR ".join([*symbols, "Federal Reserve", "Nasdaq"]),
                    limit=limit,
                    recency_days=recency_days,
                )
            feeds = {
                "FEDERAL_RESERVE_RSS": (
                    "Federal Reserve RSS",
                    settings.federal_reserve_rss_url,
                    {},
                    0.76,
                ),
                "BLS_RSS": ("BLS RSS", settings.bls_rss_url, {}, 0.86),
                "BEA_RSS": ("BEA RSS", settings.bea_rss_url, {}, 0.86),
                "YAHOO_FINANCE_RSS": (
                    "Yahoo Finance RSS",
                    settings.yahoo_finance_rss_url,
                    {},
                    0.58,
                ),
                "MARKETWATCH_RSS": (
                    "MarketWatch RSS",
                    settings.marketwatch_rss_url,
                    {},
                    0.56,
                ),
                "GOOGLE_NEWS_RSS": (
                    "Google News RSS",
                    settings.google_news_rss_url,
                    {
                        "q": f"{' OR '.join(symbols)} Nasdaq",
                        "hl": "en-US",
                        "gl": "US",
                        "ceid": "US:en",
                    },
                    0.64,
                ),
            }
            source, url, params, reliability = feeds[provider_id]
            if not url:
                raise ProbeExecutionError(
                    "registered RSS URL is not configured",
                    status=HealthStatus.NOT_CONFIGURED,
                    reason_code="PROVIDER_URL_NOT_CONFIGURED",
                )
            return await news._fetch_one_rss_feed(  # noqa: SLF001
                client=client,
                source=source,
                url=url,
                params=params,
                reliability=reliability,
                symbols=symbols,
                limit=limit,
                recency_days=recency_days,
                timeout=request.timeout_seconds,
            )

    if provider_id == "AAII":
        method = getattr(adapter, "fetch", None)
        if method is None:
            return _SOURCE_SPECIFIC_NOT_HANDLED
        return await method()

    if provider_id == "BLS":
        # A BLS capability probe must never silently become a FRED probe when
        # the BLS daily quota is exhausted.
        if hasattr(adapter, "settings"):
            adapter.settings = adapter.settings.model_copy(update={"fred_api_key": None})
        method = getattr(adapter, "fetch", None)
        if method is None:
            return _SOURCE_SPECIFIC_NOT_HANDLED
        return await method()

    if provider_id in {
        "CODEX_CLI_RESEARCH_BACKEND",
        "OPENAI_RESPONSES_RESEARCH",
    }:
        from app.services.ai_research_job_repository import (
            AIResearchJobRepository,
        )
        from app.services.ai_research_job_service import AIResearchJobService
        from app.services.ai_research_worker import AIResearchWorker
        from app.services.execution_context import ExecutionContext
        from app.services.research_backend import (
            project_backend_claim_fields,
        )
        from app.services.research_profiles import PROFILES

        runtime_profile_id = str(
            getattr(target.capability, "runtime_profile_id", None)
            or target.metric_id
        ).upper()
        runtime_profile = PROFILES.get(runtime_profile_id)
        if runtime_profile is None:
            raise ProbeExecutionError(
                "registered research profile is not available",
                status=HealthStatus.UNUSABLE,
                reason_code="RESEARCH_RUNTIME_PROFILE_NOT_REGISTERED",
            )
        runtime_job_type = str(
            getattr(target.capability, "runtime_job_type", None) or ""
        ).upper()
        if not runtime_job_type:
            raise ProbeExecutionError(
                "registered research job type is not available",
                status=HealthStatus.UNUSABLE,
                reason_code="RESEARCH_RUNTIME_JOB_TYPE_NOT_REGISTERED",
            )
        request_correlation = request.correlation_for(target)
        expected_occurrence_id = request_correlation.get(
            "expected_occurrence_id"
        )
        workspace = (
            request.sandbox_root
            / provider_id.casefold().replace("_", "-")
            / request.acquisition_id
        )
        workspace.mkdir(parents=True, exist_ok=True)
        execution_context = ExecutionContext.explicit_ai(
            correlation_id=(
                f"provider-audit-{request.acquisition_id}"
            ),
        )
        job, created = AIResearchJobService(settings).enqueue_explicit(
            job_type=runtime_job_type,
            symbol="MNQ",
            correlation_id=execution_context.correlation_id,
            request_payload={
                "gap": {
                    "dataset_id": target.dataset_id,
                    "metric_id": target.metric_id,
                    "provider_id": provider_id,
                    "request_key": request.request_key,
                    "expected_occurrence_id": expected_occurrence_id,
                    "expected_reference_period": request_correlation.get(
                        "expected_reference_period"
                    ),
                    "frequency": target.frequency,
                    "transformation": target.transformation,
                },
                "pending_fields": list(target.fields),
            },
            pending_fields=list(target.fields),
            force=True,
            execution_context=execution_context,
        )
        if not created or not str(job.get("job_id") or ""):
            raise ProbeExecutionError(
                "audit research job was not enqueued",
                status=HealthStatus.NOT_CONFIGURED,
                reason_code=str(
                    job.get("last_error")
                    or "RESEARCH_RUNTIME_JOB_NOT_ENQUEUED"
                ),
            )
        backend = _AuditBackendProxy(adapter)
        worker = AIResearchWorker(
            settings,
            repository=AIResearchJobRepository(settings),
            executor=backend,
            capabilities=_AuditLiveCapability(),
            worker_id=f"provider-audit-{request.acquisition_id}",
        )
        processed = await asyncio.to_thread(worker.process_once)
        stored_job = worker.repository.get(str(job["job_id"]))
        if not processed or not isinstance(stored_job, Mapping):
            raise ProbeExecutionError(
                "production research worker did not process the audit job",
                status=HealthStatus.UNUSABLE,
                reason_code="RESEARCH_RUNTIME_WORKER_PATH_NOT_OBSERVED",
            )
        backend_exception = (
            backend.last_error
            if backend.last_error is not None
            else backend.last_exception
        )
        backend_failure = _backend_process_failure_observation(
            backend_exception
        )
        if backend.last_error is not None and backend_failure is None:
            raise backend.last_error
        if backend_failure is not None:
            tool_observation = _validated_dispatch_observation(
                getattr(backend_exception, "dispatch_observation", None)
            )
            return {
                "runtime_profile_id": runtime_profile_id,
                "runtime_job_type": runtime_job_type,
                "metric_id": target.metric_id,
                "provider_id": provider_id,
                "status": "provider_failed",
                "failure_reason": backend_failure["reason_code"],
                "backend_failure_observation": backend_failure,
                "runtime_result": (
                    stored_job.get("result_payload")
                    if isinstance(stored_job.get("result_payload"), Mapping)
                    else {}
                ),
                "audit_tool_observation": tool_observation,
                "runtime_chain_attestation": {
                    "worker_class": type(worker).__qualname__,
                    "worker_process_once_observed": processed is True,
                    "runtime_class": type(
                        worker.agentic_runtime
                    ).__qualname__,
                    "backend_class": type(adapter).__qualname__,
                    "job_id": stored_job.get("job_id"),
                    "job_status": stored_job.get("status"),
                    "backend_process_failure_observed": True,
                },
            }
        backend_result = backend.last_result
        if backend_result is None:
            raise ProbeExecutionError(
                "production research runtime did not invoke its backend",
                status=HealthStatus.UNUSABLE,
                reason_code="RESEARCH_RUNTIME_BACKEND_PATH_NOT_OBSERVED",
            )
        runtime_result = stored_job.get("result_payload")
        runtime_result = (
            runtime_result if isinstance(runtime_result, Mapping) else {}
        )
        run_id = str(runtime_result.get("run_id") or "")
        runtime_repository = worker.agentic_runtime.repository
        tool_events = [
            *backend.observed_tool_events,
            *(
                getattr(backend_result, "tool_events", ())
                or ()
            ),
            *(
                runtime_repository.observed_sources(run_id)
                if run_id
                else ()
            ),
        ]
        observed_tool_events = [
            redact_payload(_json_safe(event))
            for event in tool_events
            if isinstance(event, Mapping)
        ]
        tool_counts = _research_tool_attempt_counts(observed_tool_events)
        if tool_counts["search"] > 1 or tool_counts["source_open"] > 1:
            raise ProbeExecutionError(
                "research backend exceeded the observed audit tool budget",
                status=HealthStatus.UNUSABLE,
                reason_code="RESEARCH_TOOL_BUDGET_EXCEEDED",
            )
        result: dict[str, Any] = {
            "runtime_profile_id": runtime_profile_id,
            "runtime_job_type": runtime_job_type,
            "metric_id": target.metric_id,
            "provider_id": provider_id,
            "backend_result": backend_result,
            "runtime_result": runtime_result,
            "runtime_chain_attestation": {
                "worker_class": type(worker).__qualname__,
                "worker_process_once_observed": processed is True,
                "runtime_class": type(worker.agentic_runtime).__qualname__,
                "runtime_run_id": run_id or None,
                "source_gateway_class": type(
                    worker.agentic_runtime.source_gateway
                ).__qualname__,
                "evidence_verifier_class": type(
                    worker.agentic_runtime.verifier
                ).__qualname__,
                "backend_class": type(adapter).__qualname__,
                "job_id": stored_job.get("job_id"),
                "job_status": stored_job.get("status"),
            },
            "capability_projection": {
                "target_id": getattr(target, "target_id", None),
                "metric_id": target.metric_id,
                "observations": project_backend_claim_fields(
                    backend_result.payload,
                    required_fields=tuple(
                        getattr(
                            target.capability,
                            "runtime_required_fields",
                            (),
                        )
                        or getattr(target, "fields", ())
                    ),
                    acquisition_provider=provider_id,
                ),
            },
            "audit_tool_observation": {
                "event_observer_installed": True,
                "events_observed": len(observed_tool_events),
                "search_attempts": tool_counts["search"],
                "source_open_attempts": tool_counts["source_open"],
                "event_sha256": [
                    _stable_sha256(event)
                    for event in observed_tool_events
                ],
            },
        }
        return result

    return _SOURCE_SPECIFIC_NOT_HANDLED


def _select_probe_method(
    adapter: Any,
    target: Any,
    *,
    targets: Sequence[Any] = (),
    request_correlation: Mapping[str, Any] | None = None,
    request_correlations: Mapping[
        str, Mapping[str, Any]
    ] | None = None,
) -> tuple[Any | None, dict[str, Any]]:
    current = datetime.now(UTC)
    provider_id = str(getattr(target, "provider_id", "") or "").upper()
    if (
        provider_id == "TRADIER"
        and target.dataset_id == "market_internals"
        and hasattr(adapter, "quotes")
    ):
        from app.core.senior_analyst_policy import MNQ_PRIMARY_SYMBOLS

        return adapter.quotes, {
            "symbols": MNQ_PRIMARY_SYMBOLS,
            "force": True,
        }
    if (
        provider_id == "FINNHUB"
        and target.dataset_id == "current_news"
        and hasattr(adapter, "company_news")
    ):
        from app.core.senior_analyst_policy import MNQ_PRIMARY_SYMBOLS

        return adapter.company_news, {
            "symbol": MNQ_PRIMARY_SYMBOLS[0],
            "start": (current - timedelta(days=14)).date(),
            "end": current.date(),
        }
    if (
        provider_id == "TARGETED_SEARCH_EVENT"
        and hasattr(adapter, "fetch_for_events")
    ):
        from app.models.common import Impact
        from app.models.events import EconomicEvent

        event = EconomicEvent(
            event_id=f"provider-audit-{target.metric_id}",
            occurrence_id=f"provider-audit:{target.metric_id}:{current.date().isoformat()}",
            name="Personal Consumption Expenditures",
            country="US",
            category="PCE",
            metric_id=target.metric_id,
            reference_period=current.strftime("%Y-%m"),
            frequency=target.frequency,
            date=current.date().isoformat(),
            time_utc=current,
            release_at=current,
            impact=Impact.HIGH,
            source="PROVIDER_CAPABILITY_AUDIT",
            source_url="https://www.bea.gov/data/personal-consumption-expenditures-price-index",
            reliability=1.0,
            event_risk_level=Impact.HIGH,
        )
        return adapter.fetch_for_events, {
            "events": (event,),
            "country": "US",
            "start": current - timedelta(days=1),
            "end": current + timedelta(days=1),
        }
    if "AI" in target.provider_type.upper() and hasattr(adapter, "research"):
        selected_targets = tuple(targets or (target,))
        events: list[dict[str, Any]] = []
        for selected_target in selected_targets:
            correlation = (
                request_correlations.get(selected_target.target_id)
                if request_correlations is not None
                else None
            )
            if correlation is None:
                correlation = (
                    request_correlation
                    if selected_target is target
                    else None
                )
            correlation = correlation or {}
            expected_occurrence_id = correlation.get(
                "expected_occurrence_id"
            )
            if not expected_occurrence_id:
                return None, {}
            request_key = str(correlation.get("request_key") or "")
            fact_key = (
                "provider-capability-audit:"
                f"{selected_target.dataset_id}:"
                f"{selected_target.metric_id}:"
                f"{request_key[:16]}"
            )
            events.append(
                {
                    "fact_key": fact_key,
                    "event_id": expected_occurrence_id,
                    "occurrence_id": expected_occurrence_id,
                    "expected_occurrence_id": expected_occurrence_id,
                    "reference_period": correlation.get(
                        "expected_reference_period"
                    ),
                    "expected_reference_period": correlation.get(
                        "expected_reference_period"
                    ),
                    "request_key": request_key,
                    "provider_id": correlation.get("provider_id"),
                    "country": "US",
                    "date": current.date().isoformat(),
                    "time_utc": current.isoformat(),
                    "category": selected_target.dataset_id.upper(),
                    "event_name": (
                        "Provider capability audit "
                        f"{selected_target.metric_id}"
                    ),
                    "valid_until": (
                        current + timedelta(hours=1)
                    ).isoformat(),
                    "dataset_id": selected_target.dataset_id,
                    "metric_id": selected_target.metric_id,
                    "supported_fields": list(selected_target.fields),
                    "frequency": selected_target.frequency,
                    "transformation": selected_target.transformation,
                    "audit_only": True,
                }
            )
        return adapter.research, {
            "events": events
        }
    if hasattr(adapter, "fetch_nasdaq"):
        return adapter.fetch_nasdaq, {}
    if hasattr(adapter, "relevant_option_chains"):
        return adapter.relevant_option_chains, {
            "symbol": "QQQ",
            "max_expirations": 1,
            "force": True,
        }
    if hasattr(adapter, "earnings_calendar") and "earning" in target.dataset_id.casefold():
        return adapter.earnings_calendar, {
            "start": current.date(),
            "end": (current + timedelta(days=14)).date(),
            "symbols": ("AAPL", "AMD", "AMZN", "META", "NVDA", "TSLA"),
        }
    method = getattr(adapter, "fetch", None)
    if method is None:
        return None, {}
    kwargs: dict[str, Any] = {}
    for name, parameter in inspect.signature(method).parameters.items():
        if name == "country":
            kwargs[name] = "US"
        elif name == "start":
            kwargs[name] = current - timedelta(days=1)
        elif name == "end":
            kwargs[name] = current + timedelta(days=21)
        elif name == "expected_period":
            kwargs[name] = current.strftime("%Y-%m")
        elif name == "release_date":
            kwargs[name] = current.date().isoformat()
        elif name == "expected_release_at":
            kwargs[name] = current.isoformat()
        elif name == "series_ids":
            series_ids = [
                item.metric_id
                for item in (targets or (target,))
                if re.fullmatch(r"[A-Z0-9]+", str(item.metric_id))
            ]
            if series_ids:
                kwargs[name] = series_ids
        elif name == "days":
            kwargs[name] = 14
        elif name == "period":
            kwargs[name] = current.strftime("%Y-%m")
        elif name == "datasets":
            selected_targets = tuple(targets or (target,))
            if provider_id == "CENSUS":
                query_ids = tuple(
                    str(
                        getattr(
                            getattr(item, "capability", None),
                            "probe_query_id",
                            "",
                        )
                        or ""
                    ).strip()
                    for item in selected_targets
                )
                if not all(query_ids):
                    return None, {}
                kwargs[name] = tuple(dict.fromkeys(query_ids))
            else:
                kwargs[name] = tuple(
                    item.metric_id for item in selected_targets
                )
        elif name == "cik":
            kwargs[name] = "0000914208"
        elif name == "listed_class_symbols":
            kwargs[name] = {"QQQ": "QQQ"}
        elif parameter.default is inspect.Parameter.empty:
            return None, {}
    return method, kwargs


def _evidence_checks(
    request: ProbeRequest,
    normalized: Any,
    exchanges: Sequence[CapturedHttpExchange] = (),
) -> tuple[dict[str, bool | None], dict[str, dict[str, bool | None]]]:
    global_checks: dict[str, bool | None] = {
        "transport_valid": True,
        "schema_valid": None,
        "completeness_valid": None,
        "freshness_valid": None,
        "semantic_mapping_valid": None,
        "occurrence_match_valid": None,
        "lineage_valid": None,
    }
    field_checks: dict[str, dict[str, bool | None]] = {}
    for target in request.targets:
        for field_name in target.fields:
            observed, value, owner = _find_target_field_observation(
                normalized,
                target,
                field_name,
            )
            explicit_null = _explicit_null_reason(owner) if observed and value is None else None
            completeness = bool(observed and value is not None)
            semantic = _semantic_check(
                target,
                owner,
                normalized,
                field_name=field_name,
                value=value,
            )
            occurrence = _occurrence_check(
                target,
                owner,
                normalized,
                expected_correlation=request.correlation_for(target),
                field_name=field_name,
            )
            lineage = (
                False
                if explicit_null
                else _lineage_check(
                    field_name,
                    owner,
                    normalized,
                    value=value,
                    expected_provider_id=target.provider_id,
                    capture_mode=_expected_capture_mode(request),
                    exchanges=exchanges,
                    expected_correlation=request.correlation_for(target),
                    allowed_source_domains=tuple(
                        getattr(
                            target.capability,
                            "runtime_source_domains",
                            (),
                        )
                        or getattr(
                            target.registration,
                            "source_domains",
                            (),
                        )
                        or ()
                    ),
                    require_claim_http_binding=(
                        "AI" in str(target.provider_type).upper()
                    ),
                )
            )
            field_checks[target.field_key(field_name)] = {
                **global_checks,
                "schema_valid": _field_schema_check(
                    target,
                    field_name,
                    normalized,
                    owner,
                    value,
                ),
                "completeness_valid": completeness,
                "freshness_valid": (
                    True
                    if explicit_null
                    else _freshness_check(
                        owner if owner is not None else normalized,
                        target=target,
                        field_name=field_name,
                        field_value=value,
                    )
                ),
                "semantic_mapping_valid": semantic,
                "occurrence_match_valid": occurrence,
                "lineage_valid": lineage,
            }
    return global_checks, field_checks


def _freshness_check(
    value: Any,
    *,
    target: Any | None = None,
    field_name: str | None = None,
    field_value: Any = _LINEAGE_VALUE_UNSET,
    now: datetime | None = None,
) -> bool | None:
    current = now or datetime.now(UTC)
    walked = (
        _field_scoped_items(
            value,
            field_name=field_name,
            field_value=field_value,
        )
        if field_name and isinstance(value, Mapping)
        else _walk_items(value)
    )
    valid_until_values = [
        item
        for key, item in walked
        if key in {"content_valid_until", "valid_until"}
    ]
    parsed_valid_until = [
        item for item in map(_parse_datetime, valid_until_values) if item is not None
    ]
    # Lifecycle labels never override an observed expiry.  In particular, a
    # record retrieved today with an expired content_valid_until is not current.
    if parsed_valid_until and any(item <= current for item in parsed_valid_until):
        return False
    refresh_due_values = [
        item
        for key, item in walked
        if key in {"refresh_due_at", "next_refresh_at"}
    ]
    parsed_refresh_due = [
        item for item in map(_parse_datetime, refresh_due_values) if item is not None
    ]
    if parsed_refresh_due and any(item <= current for item in parsed_refresh_due):
        return False
    freshness_values = [
        str(item).upper()
        for key, item in walked
        if key in {"freshness", "lifecycle", "lifecycle_status"} and item is not None
    ]
    if any(item in {"STALE", "EXPIRED"} for item in freshness_values):
        return False
    observed_at_values = [
        item
        for key, item in walked
        if key
        in {
            "data_as_of",
            "reference_period",
            "released_at",
            "release_at",
            "release_time",
            "provider_timestamp",
        }
    ]
    observed_at = [
        item
        for item in (
            _parse_reference_datetime(value, target=target)
            for value in observed_at_values
        )
        if item is not None
    ]
    latest_observation = max(observed_at) if observed_at else None
    lifecycle_proof = _release_lifecycle_proof(
        walked,
        target=target,
    )
    if lifecycle_proof is False:
        return False
    if latest_observation is not None and (
        latest_observation > current + timedelta(days=2)
    ):
        return False
    # A verified match to the explicitly expected occurrence/reference period
    # is stronger than generic age. This permits the latest monthly or
    # quarterly release to remain valid until its lifecycle says otherwise.
    if lifecycle_proof is True:
        return True
    if latest_observation is not None:
        maximum_age = _freshness_maximum_age(target)
        if maximum_age is not None and current - latest_observation > maximum_age:
            return False
    if _requires_release_lifecycle_proof(walked, target=target):
        # A recent retrieval, a CURRENT label, or an observation merely inside
        # an SLA cannot prove that the expected release has been observed.
        return None
    current_lifecycle = any(
        item
        in {
            "LIVE",
            "RECENT",
            "CURRENT",
            "CURRENT_RELEASE",
            "CURRENT_LATEST_OFFICIAL_RELEASE",
        }
        for item in freshness_values
    )
    if current_lifecycle and latest_observation is not None:
        return True
    if (
        latest_observation is not None
        and parsed_valid_until
        and all(item > current for item in parsed_valid_until)
    ):
        return True
    return None


def _requires_release_lifecycle_proof(
    walked: Sequence[tuple[str, Any]],
    *,
    target: Any | None,
) -> bool:
    frequency = str(getattr(target, "frequency", "") or "").casefold()
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
        for key, _ in walked
    )


def _release_lifecycle_proof(
    walked: Sequence[tuple[str, Any]],
    *,
    target: Any | None,
) -> bool | None:
    if not _requires_release_lifecycle_proof(walked, target=target):
        return None
    observed_occurrences = {
        str(item).strip().casefold()
        for key, item in walked
        if key in {"occurrence_id", "release_occurrence_id"}
        and str(item or "").strip()
    }
    expected_occurrences = {
        str(item).strip().casefold()
        for key, item in walked
        if key
        in {
            "expected_occurrence_id",
            "latest_expected_occurrence_id",
        }
        and str(item or "").strip()
    }
    observed_periods = {
        str(item).strip().casefold()
        for key, item in walked
        if key in {"reference_period", "release_reference_period"}
        and str(item or "").strip()
    }
    expected_periods = {
        str(item).strip().casefold()
        for key, item in walked
        if key
        in {
            "expected_reference_period",
            "latest_expected_reference_period",
        }
        and str(item or "").strip()
    }
    if expected_occurrences and (
        not observed_occurrences
        or observed_occurrences.isdisjoint(expected_occurrences)
    ):
        return False
    if expected_periods and (
        not observed_periods
        or observed_periods.isdisjoint(expected_periods)
    ):
        return False
    proof_values = [
        item
        for key, item in walked
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
    if (
        any(item is True for item in proof_values)
        and (expected_occurrences or expected_periods)
    ):
        return True
    return None


def _freshness_maximum_age(target: Any | None) -> timedelta | None:
    dataset_id = str(getattr(target, "dataset_id", "") or "")
    if dataset_id:
        try:
            return timedelta(
                seconds=dataset_policy_by_id(dataset_id).sla_seconds
            )
        except KeyError:
            pass
    frequency = str(getattr(target, "frequency", "") or "").casefold()
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
    return limits.get(frequency)


def _parse_reference_datetime(
    value: Any,
    *,
    target: Any | None,
) -> datetime | None:
    parsed = _parse_datetime(value)
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
    year = re.fullmatch(r"((?:19|20)\d{2})", text)
    if year and str(getattr(target, "frequency", "")).casefold() in {
        "annual",
        "yearly",
    }:
        return datetime(int(year.group(1)), 1, 1, tzinfo=UTC)
    return None


def _semantic_check(
    target: Any,
    owner: Any,
    normalized: Any,
    *,
    field_name: str | None = None,
    value: Any = _LINEAGE_VALUE_UNSET,
) -> bool | None:
    expected_frequency = str(target.frequency or "").casefold()
    expected_metric = str(target.metric_id).casefold()
    semantic_scope = owner if isinstance(owner, Mapping) else normalized
    walked = (
        _field_scoped_items(
            semantic_scope,
            field_name=field_name,
            field_value=value,
        )
        if field_name and isinstance(semantic_scope, Mapping)
        else _walk_items(semantic_scope)
    )
    text = " ".join(
        str(item).casefold()
        for _key, item in walked
        if not isinstance(item, (Mapping, list, tuple))
    )
    expected_basis = _metric_change_basis(
        " ".join(
            value
            for value in (
                expected_metric,
                str(target.transformation or "").casefold(),
            )
            if value
        )
    )
    observed_bases = _metric_change_bases(text)
    if (
        expected_basis is not None
        and observed_bases
        and any(item != expected_basis for item in observed_bases)
    ):
        return False
    structured_metrics = {
        str(value).strip().casefold()
        for key, value in walked
        if key
        in {
            "canonical_series_id",
            "event_metric_id",
            "metric",
            "metric_id",
            "series_id",
        }
        and str(value or "").strip()
    }
    canonical_metric_ids = {
        str(item).strip().casefold()
        for item in getattr(
            getattr(target, "capability", None),
            "canonical_metric_ids",
            (),
        )
        if str(item).strip()
    }
    accepted_metrics = {expected_metric, *canonical_metric_ids} - {""}
    if structured_metrics and structured_metrics.isdisjoint(accepted_metrics):
        return False
    metric_present = bool(structured_metrics & accepted_metrics)

    raw_structured_frequencies = [
        value
        for key, value in walked
        if key
        in {
            "cadence",
            "frequency",
            "observation_frequency",
            "release_frequency",
        }
        and value not in (None, "")
    ]
    structured_frequencies = {
        normalized_frequency
        for value in raw_structured_frequencies
        if (normalized_frequency := _normalized_frequency(value)) is not None
    }
    normalized_expected_frequency = (
        _normalized_frequency(expected_frequency) or expected_frequency
    )
    if structured_frequencies and (
        structured_frequencies != {normalized_expected_frequency}
    ):
        # A structured declaration belongs to the field-owning record and
        # cannot be overridden by an unrelated token elsewhere in the body.
        return False
    if raw_structured_frequencies and not structured_frequencies:
        return None

    raw_transformations = [
        item
        for key, item in walked
        if key in {"transformation", "transformation_id"}
        and item not in (None, "")
    ]
    observed_transformations = {
        normalized
        for item in raw_transformations
        if (normalized := _normalized_semantic_token(item))
    }
    expected_transformation = _normalized_semantic_token(
        target.transformation
    )
    if expected_transformation and observed_transformations and (
        observed_transformations != {expected_transformation}
    ):
        return False
    if expected_transformation and not observed_transformations:
        return None

    measurement_contract = capability_field_measurement_contract(
        getattr(target, "capability", target),
        str(field_name or ""),
    )
    expected_unit = _normalized_unit(measurement_contract)
    raw_units = [
        item
        for key, item in walked
        if key in {"unit", "units", "measurement_unit"}
        and item not in (None, "")
    ]
    observed_units = {
        normalized
        for item in raw_units
        if (normalized := _normalized_unit(item))
    }
    if measurement_contract == "mixed_numeric":
        if observed_units and not observed_units.issubset(
            KNOWN_NUMERIC_MEASUREMENT_UNITS
        ):
            return False
        # A wildcard numeric unit cannot certify one atomic field, even when
        # the response happens to use another known unit label.
        return None
    elif measurement_contract not in {
        "categorical",
        "mixed_structured",
        "temporal",
    }:
        if expected_unit and observed_units and observed_units != {expected_unit}:
            return False
        if expected_unit and not observed_units:
            return None

    frequency_present = bool(
        structured_frequencies == {normalized_expected_frequency}
    )
    if metric_present and frequency_present:
        return True
    return None


def _normalized_semantic_token(value: Any) -> str | None:
    token = re.sub(
        r"[^a-z0-9]+",
        "_",
        str(value or "").strip().casefold(),
    ).strip("_")
    return token or None


def _normalized_unit(value: Any) -> str | None:
    token = _normalized_semantic_token(value)
    aliases = {
        "%": "percent",
        "pct": "percent",
        "percentage": "percent",
        "percentage_point": "percentage_points",
        "percentage_points": "percentage_points",
        "points": "index_points",
    }
    return aliases.get(str(value or "").strip().casefold(), aliases.get(token, token))


def _normalized_frequency(value: Any) -> str | None:
    normalized = re.sub(
        r"[^a-z0-9]+",
        "_",
        str(value or "").strip().casefold(),
    ).strip("_")
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
        "on_demand": "request",
        "per_request": "request",
        "request": "request",
        "request_scoped": "request",
        "m": "monthly",
        "m_m": "monthly",
        "mom": "monthly",
        "month": "monthly",
        "monthly": "monthly",
        "q": "quarterly",
        "q_q": "quarterly",
        "qoq": "quarterly",
        "quarter": "quarterly",
        "quarterly": "quarterly",
        "real_time": "realtime",
        "realtime": "realtime",
        "release": "event",
        "w": "weekly",
        "week": "weekly",
        "weekly": "weekly",
        "year": "yearly",
        "yearly": "yearly",
    }
    return aliases.get(normalized)


def _occurrence_check(
    target: Any,
    owner: Any,
    normalized: Any,
    *,
    expected_correlation: Mapping[str, Any] | None = None,
    field_name: str | None = None,
) -> bool | None:
    """Verify occurrence identity against the request, not only its date token."""

    correlation = expected_correlation or {}
    context = _field_occurrence_scope(
        owner,
        normalized,
        target=target,
        expected_correlation=correlation,
        field_name=field_name,
    )
    if not isinstance(context, Mapping):
        return False
    occurrence_values = _direct_text_values(
        context,
        ("occurrence_id", "release_occurrence_id", "event_key"),
    )
    reference_values = _direct_text_values(
        context,
        ("reference_period", "release_reference_period", "period"),
    )
    metric_values = _direct_text_values(
        context,
        ("metric_id", "event_metric_id"),
    )
    if metric_values and str(target.metric_id) not in metric_values:
        return False

    expected_provider = str(
        correlation.get("provider_id")
        or getattr(target, "provider_id", "")
        or ""
    ).strip()
    provider_values = _observed_provider_ids(context)
    if not provider_values:
        for ancestor in reversed(
            _mapping_path_to_identity(normalized, context)
        ):
            provider_values = _observed_provider_ids(ancestor)
            if provider_values:
                break
    target_ids = _direct_text_values(
        context,
        ("target_id", "capability_id"),
    )
    expected_target_id = str(correlation.get("target_id") or "").strip()
    target_id_correlated = bool(
        expected_target_id and expected_target_id in target_ids
    )
    if expected_provider and (
        expected_provider.casefold() not in provider_values
        and not (
            target_id_correlated
            and expected_target_id.casefold().startswith(
                f"{expected_provider.casefold()}|"
            )
        )
    ):
        return False

    expected_occurrence = str(
        correlation.get("expected_occurrence_id") or ""
    ).strip()
    if expected_occurrence:
        # A same-period result from another occurrence is not correlated. The
        # synthetic occurrence contains the immutable request-key prefix.
        if expected_occurrence not in occurrence_values:
            return False
    expected_reference = str(
        correlation.get("expected_reference_period") or ""
    ).strip()
    if expected_reference and not _reference_period_matches(
        expected_reference,
        reference_values,
    ):
        return False

    if occurrence_values or reference_values:
        if not occurrence_values or not reference_values:
            return False
        occurrence_periods = {
            period
            for value in occurrence_values
            for period in _period_tokens(value)
        }
        reference_periods = {
            period
            for value in reference_values
            for period in _period_tokens(value)
        }
        if occurrence_periods and reference_periods:
            return any(
                occurrence == reference
                or occurrence.startswith(f"{reference}-")
                or reference.startswith(f"{occurrence}-")
                for occurrence in occurrence_periods
                for reference in reference_periods
            )
        # Exact request correlation can certify a non-date occurrence, but a
        # response-local pair of opaque identifiers cannot certify itself.
        return True if expected_occurrence and expected_reference else None
    if expected_occurrence or expected_reference:
        return False
    if str(target.frequency or "").upper() in {"INTRADAY", "DAILY", "REALTIME"}:
        return bool(not expected_provider or provider_values)
    return False


def _field_occurrence_scope(
    owner: Any,
    normalized: Any,
    *,
    target: Any,
    expected_correlation: Mapping[str, Any],
    field_name: str | None,
) -> Mapping[str, Any] | None:
    """Return the nearest field-owning identity node; never scan siblings."""

    if not isinstance(owner, Mapping):
        return None
    path = _mapping_path_to_identity(normalized, owner)
    candidates = list(reversed(path)) if path else [owner]
    expected_target_id = str(
        expected_correlation.get("target_id") or ""
    ).strip()
    expected_metric_id = str(getattr(target, "metric_id", "") or "").strip()
    for candidate in candidates:
        if not _mapping_owns_field(candidate, owner, field_name):
            continue
        occurrence_present = bool(
            _direct_text_values(
                candidate,
                (
                    "occurrence_id",
                    "release_occurrence_id",
                    "event_key",
                ),
            )
        )
        reference_present = bool(
            _direct_text_values(
                candidate,
                (
                    "reference_period",
                    "release_reference_period",
                    "period",
                ),
            )
        )
        if not occurrence_present and not reference_present:
            continue
        candidate_targets = _direct_text_values(
            candidate,
            ("target_id", "capability_id"),
        )
        candidate_metrics = _direct_text_values(
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
            # An ancestor without explicit target/metric identity could cover
            # several sibling events and must not supply their occurrence.
            continue
        return candidate
    return owner


def _mapping_path_to_identity(
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


def _mapping_owns_field(
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


def _direct_text_values(
    value: Mapping[str, Any],
    keys: Sequence[str],
) -> set[str]:
    return {
        str(value.get(key)).strip()
        for key in keys
        if str(value.get(key) or "").strip()
    }


def _observed_provider_ids(value: Any) -> set[str]:
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


def _reference_period_matches(
    expected: str,
    observed: set[str],
) -> bool:
    if expected in observed:
        return True
    expected_tokens = _period_tokens(expected)
    return bool(
        expected_tokens
        and any(expected_tokens & _period_tokens(item) for item in observed)
    )


def _lineage_check(
    field_name: str,
    owner: Any,
    normalized: Any,
    *,
    value: Any = _LINEAGE_VALUE_UNSET,
    expected_provider_id: str | None = None,
    capture_mode: str | None = None,
    exchanges: Sequence[CapturedHttpExchange] = (),
    expected_correlation: Mapping[str, Any] | None = None,
    allowed_source_domains: Sequence[str] = (),
    require_claim_http_binding: bool = False,
) -> bool | None:
    context = owner if isinstance(owner, Mapping) else normalized
    if not isinstance(context, Mapping):
        return False
    if value is _LINEAGE_VALUE_UNSET:
        value, resolved_owner = _find_field(context, field_name)
        if resolved_owner is None:
            return False
    return (
        _field_lineage_binding(
            field_name,
            value,
            context,
            expected_provider_id=expected_provider_id,
            capture_mode=capture_mode,
            exchanges=exchanges,
            expected_occurrence_id=str(
                (expected_correlation or {}).get(
                    "expected_occurrence_id"
                )
                or ""
            )
            or None,
            expected_reference_period=str(
                (expected_correlation or {}).get(
                    "expected_reference_period"
                )
                or ""
            )
            or None,
            allowed_source_domains=allowed_source_domains,
            require_claim_http_binding=require_claim_http_binding,
        )
        is not None
    )


def _field_lineage_binding(
    field_name: str,
    value: Any,
    owner: Mapping[str, Any],
    *,
    expected_provider_id: str | None,
    capture_mode: str | None,
    exchanges: Sequence[CapturedHttpExchange],
    expected_occurrence_id: str | None = None,
    expected_reference_period: str | None = None,
    allowed_source_domains: Sequence[str] = (),
    require_claim_http_binding: bool = False,
) -> dict[str, Any] | None:
    expected_value_sha256 = _stable_sha256(value)
    normalized_mode = str(capture_mode or "").strip().upper()
    for candidate, structurally_scoped in _field_lineage_candidates(
        field_name,
        owner,
    ):
        declared_field = str(
            candidate.get("field") or candidate.get("field_name") or ""
        ).strip()
        if not structurally_scoped and declared_field != field_name:
            continue
        if declared_field and declared_field != field_name:
            continue
        declared_hash = str(candidate.get("value_sha256") or "").strip()
        if declared_hash:
            if declared_hash != expected_value_sha256:
                continue
            binding_kind = "VALUE_SHA256"
        elif "value" in candidate and _stable_sha256(candidate.get("value")) == expected_value_sha256:
            binding_kind = "VALUE_EXACT"
        else:
            # `actual_source=...` or `lineage=[{"field":"actual"}]`
            # identifies a label only; it does not bind the source to this value.
            continue
        claim_evidence = bool(
            require_claim_http_binding
            or candidate.get("_audit_claim_evidence") is True
        )
        publisher = str(candidate.get("publisher") or "").strip()
        distributor = str(
            candidate.get("distributor")
            or (expected_provider_id if claim_evidence else "")
        ).strip()
        acquisition_provider = str(
            candidate.get("acquisition_provider")
            or (expected_provider_id if claim_evidence else "")
        ).strip()
        if not publisher or not distributor or not acquisition_provider:
            continue
        if (
            expected_provider_id
            and acquisition_provider.casefold()
            != expected_provider_id.strip().casefold()
        ):
            continue
        observed_occurrences = _direct_text_values(
            candidate,
            (
                "occurrence_id",
                "release_occurrence_id",
                "event_key",
            ),
        )
        if expected_occurrence_id and (
            expected_occurrence_id not in observed_occurrences
        ):
            continue
        observed_references = _direct_text_values(
            candidate,
            (
                "reference_period",
                "release_reference_period",
                "period",
            ),
        )
        if expected_reference_period and not _reference_period_matches(
            expected_reference_period,
            observed_references,
        ):
            continue

        source_url = str(
            candidate.get("source_url")
            or candidate.get("evidence_url")
            or ""
        ).strip()
        source_host = _source_url_host(source_url)
        if claim_evidence and (
            not source_host
            or not allowed_source_domains
            or not any(
                _domain_matches(source_host, domain)
                for domain in allowed_source_domains
            )
            or not _publisher_matches_source(
                _default_source_policy(),
                publisher,
                source_url,
            )
        ):
            continue
        normalized_source = _normalized_source_url(source_url)
        matching_exchange = next(
            (
                exchange
                for exchange in exchanges
                if normalized_source is not None
                and normalized_source
                == _normalized_source_url(exchange.url)
                and 200 <= exchange.status_code < 300
            ),
            None,
        )
        if claim_evidence and (
            matching_exchange is None
            or not _exchange_supports_claim(
                matching_exchange,
                field_name=field_name,
                value=value,
                candidate=candidate,
            )
        ):
            continue
        source_content_sha256 = (
            hashlib.sha256(matching_exchange.response_body).hexdigest()
            if matching_exchange
            else str(candidate.get("source_content_sha256") or "").strip()
            or None
        )
        source_url_reachable = matching_exchange is not None or (
            candidate.get("source_url_reachable") is True
        )
        verification_origin = (
            "AUDIT_SOURCE_URL_GET"
            if matching_exchange and claim_evidence
            else "AUDIT_TRANSPORT"
            if matching_exchange
            else candidate.get("verification_origin")
        )
        local_record_locator = _local_record_locator(candidate, owner)
        if normalized_mode == "HTTPX" and matching_exchange is None:
            continue
        if normalized_mode == "LOCAL_SANDBOX" and not local_record_locator:
            continue
        if normalized_mode == "SUBPROCESS" and matching_exchange is None:
            continue
        return {
            "field": field_name,
            "value_sha256": expected_value_sha256,
            "value_binding": binding_kind,
            "publisher": publisher,
            "distributor": distributor,
            "acquisition_provider": acquisition_provider,
            "source_url": source_url or None,
            "source_url_reachable": source_url_reachable,
            "source_content_sha256": source_content_sha256,
            "verification_origin": verification_origin,
            "local_record_locator": local_record_locator or None,
        }
    return None


def _field_lineage_candidates(
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


def _field_scoped_items(
    owner: Mapping[str, Any],
    *,
    field_name: str,
    field_value: Any = _LINEAGE_VALUE_UNSET,
) -> list[tuple[str, Any]]:
    """Collect semantics/lifecycle facts bound to one field only.

    Direct scalar metadata on the field-owning record may apply to the field.
    Nested siblings never do. Nested metadata is admitted only from an
    explicitly field-scoped container or a lineage record whose field/value
    binding matches the observation.
    """

    output = [
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
            output.extend(
                (str(key).casefold(), item)
                for key, item in container.items()
                if not isinstance(item, (Mapping, list, tuple))
            )

    expected_hash = (
        _stable_sha256(field_value)
        if field_value is not _LINEAGE_VALUE_UNSET
        else None
    )
    for candidate, structurally_scoped in _field_lineage_candidates(
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
        if expected_hash is not None:
            declared_hash = str(candidate.get("value_sha256") or "").strip()
            if declared_hash and declared_hash != expected_hash:
                continue
            if (
                not declared_hash
                and "value" in candidate
                and _stable_sha256(candidate.get("value")) != expected_hash
            ):
                continue
            if not declared_hash and "value" not in candidate:
                continue
        output.extend(
            (str(key).casefold(), item)
            for key, item in candidate.items()
            if not isinstance(item, (Mapping, list, tuple))
        )
    return output


def _local_record_locator(
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


def _metric_change_basis(value: str) -> str | None:
    observed = _metric_change_bases(value)
    return next(iter(observed)) if len(observed) == 1 else None


def _metric_change_bases(value: str) -> set[str]:
    normalized = re.sub(r"[^a-z0-9/]+", " ", value.casefold())
    tokens = set(normalized.split())
    compact = normalized.replace(" ", "")
    yoy = bool(
        tokens & {"yoy", "annual", "yearly"}
        or "a/a" in value.casefold()
        or "yearoveryear" in compact
    )
    mom = bool(
        tokens & {"mom"}
        or "m/m" in value.casefold()
        or "monthovermonth" in compact
    )
    qoq = bool(
        tokens & {"qoq"}
        or "q/q" in value.casefold()
        or "quarteroverquarter" in compact
    )
    return {
        name
        for name, present in (("YOY", yoy), ("MOM", mom), ("QOQ", qoq))
        if present
    }


def _period_tokens(value: str) -> set[str]:
    """Extract comparable YYYY, YYYY-MM and YYYY-MM-DD reference tokens."""

    tokens: set[str] = set()
    for match in re.finditer(
        r"(?<!\d)(20\d{2})(?:[-_/](0[1-9]|1[0-2]))?(?:[-_/](0[1-9]|[12]\d|3[01]))?(?!\d)",
        value,
    ):
        year, month, day = match.groups()
        if month and day:
            tokens.add(f"{year}-{month}-{day}")
        elif month:
            tokens.add(f"{year}-{month}")
        else:
            tokens.add(year)
    return tokens


def _extract_field_evidence(
    request: ProbeRequest,
    normalized: Any,
    exchanges: Sequence[CapturedHttpExchange],
) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    for target in request.targets:
        for field_name in target.fields:
            observed, value, owner = _find_target_field_observation(
                normalized,
                target,
                field_name,
            )
            if not observed:
                continue
            owner_map = owner if isinstance(owner, Mapping) else {}
            null_reason = (
                _explicit_null_reason(owner_map)
                if value is None
                else None
            )
            request_correlation = request.correlation_for(target)
            lineage_binding = (
                None
                if value is None
                else _field_lineage_binding(
                    field_name,
                    value,
                    owner_map,
                    expected_provider_id=target.provider_id,
                    capture_mode=_expected_capture_mode(request),
                    exchanges=exchanges,
                    expected_occurrence_id=str(
                        request_correlation.get(
                            "expected_occurrence_id"
                        )
                        or ""
                    )
                    or None,
                    expected_reference_period=str(
                        request_correlation.get(
                            "expected_reference_period"
                        )
                        or ""
                    )
                    or None,
                    allowed_source_domains=tuple(
                        getattr(
                            target.capability,
                            "runtime_source_domains",
                            (),
                        )
                        or getattr(
                            target.registration,
                            "source_domains",
                            (),
                        )
                        or ()
                    ),
                    require_claim_http_binding=(
                        "AI" in str(target.provider_type).upper()
                    ),
                )
            )
            occurrence_verified = _occurrence_check(
                target,
                owner_map,
                normalized,
                expected_correlation=request_correlation,
                field_name=field_name,
            )
            scoped_items = _field_scoped_items(
                owner_map,
                field_name=field_name,
                field_value=value,
            )
            field_evidence = {
                "field": field_name,
                "value": _json_safe(value),
                "value_present": value is not None,
                "field_observed": True,
                "explicit_null": value is None,
                "explicit_null_verified": bool(
                    value is None and null_reason
                ),
                "value_sha256": (
                    _stable_sha256(value) if value is not None else None
                ),
                "owner_sha256": _stable_sha256(owner),
                "lineage_evidence": lineage_binding,
                "source_url": (
                    lineage_binding.get("source_url") if lineage_binding else None
                ),
                "source_url_reachable": (
                    lineage_binding.get("source_url_reachable")
                    if lineage_binding
                    else False
                ),
                "source_content_sha256": (
                    lineage_binding.get("source_content_sha256")
                    if lineage_binding
                    else None
                ),
                "publisher": (
                    lineage_binding.get("publisher") if lineage_binding else None
                ),
                "distributor": (
                    lineage_binding.get("distributor") if lineage_binding else None
                ),
                "acquisition_provider": (
                    lineage_binding.get("acquisition_provider")
                    if lineage_binding
                    else None
                ),
                "verification_origin": (
                    lineage_binding.get("verification_origin")
                    if lineage_binding
                    else None
                ),
                "semantic_mapping_verified": (
                    _semantic_check(
                        target,
                        owner_map,
                        normalized,
                        field_name=field_name,
                        value=value,
                    )
                    is True
                ),
                "occurrence_verified": occurrence_verified is True,
                "request_correlation_verified": occurrence_verified is True,
                "request_correlation": _json_safe(request_correlation),
                "expected_request_key": request_correlation.get("request_key"),
                "expected_provider_id": request_correlation.get("provider_id"),
                "expected_occurrence_id": request_correlation.get(
                    "expected_occurrence_id"
                ),
                "expected_reference_period": request_correlation.get(
                    "expected_reference_period"
                ),
                "reference_period_verified": bool(
                    _first_scoped_value(
                        scoped_items,
                        ("reference_period", "data_as_of"),
                    )
                ) and occurrence_verified is True,
                "occurrence_id": _first_scoped_value(
                    scoped_items,
                    ("occurrence_id",),
                ),
                "reference_period": _first_scoped_value(
                    scoped_items,
                    ("reference_period",),
                ),
                "released_at": _first_scoped_value(
                    scoped_items,
                    ("released_at", "release_at", "release_time"),
                ),
                "data_as_of": _first_scoped_value(
                    scoped_items,
                    ("data_as_of",),
                ),
                "content_valid_until": _first_scoped_value(
                    scoped_items,
                    ("content_valid_until", "valid_until"),
                ),
                "refresh_due_at": _first_scoped_value(
                    scoped_items,
                    ("refresh_due_at",),
                ),
                "lifecycle": _first_scoped_value(
                    scoped_items,
                    ("lifecycle", "lifecycle_status", "freshness"),
                ),
                "freshness_verified": (
                    bool(null_reason)
                    if value is None
                    else _freshness_check(
                        owner_map,
                        target=target,
                        field_name=field_name,
                        field_value=value,
                    )
                    is True
                ),
                "field_lineage_verified": (
                    lineage_binding is not None
                    if value is not None
                    else False
                ),
                "invented": owner_map.get("invented"),
                "model_knowledge_only": owner_map.get("model_knowledge_only"),
                "null_reason": null_reason,
            }
            evidence[target.field_key(field_name)] = field_evidence
    return evidence


def _first_scoped_value(
    items: Sequence[tuple[str, Any]],
    keys: Sequence[str],
) -> Any:
    for key, item in items:
        if key in keys and item not in (None, "", []):
            return item
    return None


def _transport_classification(
    exchanges: Sequence[CapturedHttpExchange],
    registration: Any,
) -> tuple[str, int | None, list[str]]:
    if not exchanges:
        return "OK", None, []
    status = exchanges[-1].status_code
    credentialed = bool(_value(registration, "credential_requirements", ()))
    if status == 429:
        return HealthStatus.RATE_LIMITED.value, status, ["HTTP_RATE_LIMITED"]
    if status == 401 or (status == 403 and credentialed):
        return HealthStatus.AUTH_FAILED.value, status, ["HTTP_AUTHENTICATION_FAILED"]
    if status == 403:
        return HealthStatus.UNUSABLE.value, status, ["HTTP_FORBIDDEN"]
    if status >= 500:
        return HealthStatus.DOWN.value, status, ["HTTP_UPSTREAM_FAILURE"]
    if status >= 400:
        return HealthStatus.UNUSABLE.value, status, [f"HTTP_{status}"]
    return "OK", status, []


def _capture_attestation(
    request: ProbeRequest,
    normalized: Any,
    exchanges: Sequence[CapturedHttpExchange],
    *,
    subprocess_output: bytes | None = None,
) -> dict[str, Any]:
    """Bind a successful probe to its observed acquisition mechanism.

    HTTP providers are accepted only when this audit process captured at least
    one real HTTPX exchange.  Repository and pure-transform probes are
    explicitly local and operate on the request-local sandbox.  The Codex CLI
    AI adapter is the only registered subprocess source and must expose a
    process result (exit code or a bounded timeout) in its normalized result.
    """

    expected_mode = _expected_capture_mode(request)

    if expected_mode == "SUBPROCESS":
        attestation = _subprocess_result_attestation(normalized)
        successful_exit = bool(
            attestation is not None and attestation.get("exit_code") == 0
        )
        output_sha256 = (
            hashlib.sha256(subprocess_output).hexdigest()
            if subprocess_output is not None
            else None
        )
        if attestation is not None:
            attestation = {
                **attestation,
                "subprocess_output_sha256": output_sha256,
                "subprocess_output_size_bytes": (
                    len(subprocess_output)
                    if subprocess_output is not None
                    else None
                ),
                "source_exchange_count": len(exchanges),
            }
        output_binding_verified = bool(
            attestation is not None
            and output_sha256 is not None
            and attestation.get("declared_output_sha256")
            == output_sha256
        )
        return {
            "capture_mode": "SUBPROCESS",
            "capture_mode_expected": expected_mode,
            "capture_verified": bool(
                attestation is not None
                and (
                    not successful_exit
                    or output_binding_verified
                )
            ),
            "capture_attestation": attestation,
        }
    if exchanges:
        return {
            "capture_mode": "HTTPX",
            "capture_mode_expected": expected_mode,
            "capture_verified": True,
            "capture_attestation": {
                "exchange_count": len(exchanges),
                "first_attempt": min(exchange.attempt for exchange in exchanges),
                "last_attempt": max(exchange.attempt for exchange in exchanges),
            },
        }
    if expected_mode == "LOCAL_SANDBOX":
        return {
            "capture_mode": "LOCAL_SANDBOX",
            "capture_mode_expected": expected_mode,
            "capture_verified": True,
            "capture_attestation": {
                "database_snapshot_isolated": (
                    request.database_snapshot_path is not None
                ),
                "sandbox_root": str(request.sandbox_root),
            },
        }
    return {
        "capture_mode": "NONE",
        "capture_mode_expected": expected_mode,
        "capture_verified": False,
        "capture_attestation": None,
    }


def _expected_capture_mode(request: ProbeRequest) -> str:
    return effective_capture_mode(
        request.registration,
        request.settings,
    )


def _capture_mode_configuration(
    request: ProbeRequest,
) -> dict[str, str] | None:
    setting = str(
        getattr(request.registration, "configuration_setting", None)
        or (
            "ai_researcher_mode"
            if request.provider_id == "AI_RESEARCHER"
            else ""
        )
    ).strip()
    if not setting:
        return None
    default = (
        getattr(request.registration, "configuration_value", None)
        or ("codex_cli" if setting == "ai_researcher_mode" else "")
    )
    mode = str(
        getattr(request.settings, setting, None)
        or (
            request.settings.get(setting)
            if isinstance(request.settings, Mapping)
            else ""
        )
        or default
    ).strip().lower()
    return {
        "setting": setting,
        "value": mode,
    }


def _subprocess_result_attestation(value: Any) -> dict[str, Any] | None:
    for candidate in _mapping_values(value):
        exit_code = candidate.get("exit_code")
        failure_reason = str(candidate.get("failure_reason") or "")
        bounded_timeout = (
            failure_reason in {"ai_research_timeout", "codex_cli_timeout"}
            and isinstance(candidate.get("timeout_seconds"), (int, float))
            and not isinstance(candidate.get("timeout_seconds"), bool)
            and float(candidate["timeout_seconds"]) > 0
            and candidate.get("process_terminated") is True
        )
        process_observed = candidate.get("process_observed") is True
        process_id = candidate.get("process_id")
        output_sha256 = str(candidate.get("output_sha256") or "")
        stdout_sha256 = str(candidate.get("stdout_sha256") or "")
        stderr_sha256 = str(candidate.get("stderr_sha256") or "")
        command_sha256 = str(candidate.get("command_sha256") or "")
        process_attested = bool(
            process_observed
            and type(process_id) is int
            and process_id > 0
            and candidate.get("process_terminated") is True
            and all(
                re.fullmatch(r"[0-9a-f]{64}", digest)
                for digest in (
                    output_sha256,
                    stdout_sha256,
                    stderr_sha256,
                    command_sha256,
                )
            )
        )
        if process_attested and (type(exit_code) is int or bounded_timeout):
            return {
                "exit_code": exit_code,
                "process_observed": process_observed,
                "process_id": process_id,
                "failure_reason": failure_reason or None,
                "status": candidate.get("status"),
                "duration_ms": candidate.get("duration_ms"),
                "bounded_timeout_observed": bounded_timeout,
                "stdout_sha256": stdout_sha256 or None,
                "stderr_sha256": stderr_sha256 or None,
                "command_sha256": command_sha256 or None,
                "declared_output_sha256": output_sha256 or None,
                "process_terminated": candidate.get("process_terminated"),
            }
    return None


def _subprocess_output_bytes(
    value: Any,
    sandbox_root: Path,
    *,
    maximum_bytes: int = 10 * 1024 * 1024,
) -> bytes | None:
    """Read only the exact structured subprocess output inside the audit sandbox."""

    root = sandbox_root.resolve()
    for candidate in _mapping_values(value):
        output_path = str(candidate.get("output_path") or "").strip()
        if not output_path:
            continue
        path = Path(output_path).resolve()
        try:
            contained = path.is_relative_to(root)
        except ValueError:
            contained = False
        if not contained or not path.is_file():
            continue
        try:
            size = path.stat().st_size
            if size < 0 or size > maximum_bytes:
                return None
            return path.read_bytes()
        except OSError:
            return None
    return None


def _mapping_values(value: Any) -> list[Mapping[str, Any]]:
    output: list[Mapping[str, Any]] = []
    pending = [value]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if isinstance(current, (Mapping, list, tuple)):
            identity = id(current)
            if identity in seen:
                continue
            seen.add(identity)
        if isinstance(current, Mapping):
            output.append(current)
            pending.extend(current.values())
        elif isinstance(current, (list, tuple)):
            pending.extend(current)
    return output


def _research_tool_attempt_counts(
    events: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    counts = {"search": 0, "source_open": 0}
    observed: set[tuple[str, str]] = set()
    for event in events:
        action = _research_tool_action(event)
        if action is None:
            continue
        fingerprint = str(
            event.get("tool_action_fingerprint")
            or event.get("item_id")
            or _stable_sha256(
                {
                    "action": action,
                    "query": event.get("query"),
                    "source_url": event.get("source_url"),
                    "canonical_url": event.get("canonical_url"),
                }
            )
        )
        identity = (action, fingerprint)
        if identity in observed:
            continue
        observed.add(identity)
        counts[action] += 1
    return counts


def _research_tool_action(
    event: Mapping[str, Any],
) -> str | None:
    action = str(
        event.get("semantic_action")
        or event.get("event_type")
        or ""
    ).strip().casefold()
    if action in {"search", "web_search"}:
        return "search"
    if action in {
        "open_source",
        "fetch",
        "verify_source",
        "server_source_verified",
    }:
        return "source_open"
    return None


def _backend_tool_attempt_count(value: Any) -> int:
    for candidate in _mapping_values(value):
        observation = candidate.get("audit_tool_observation")
        if not isinstance(observation, Mapping):
            continue
        return max(
            int(observation.get("search_attempts") or 0),
            0,
        ) + max(
            int(observation.get("source_open_attempts") or 0),
            0,
        )
    return 0


def _backend_invocation_attempt_count(
    request: ProbeRequest,
    value: Any,
) -> int:
    """Count a real subprocess launch not observable as an HTTPX send.

    OpenAI Responses is already counted by ``HttpxAuditCapture``.  The Codex
    CLI backend crosses a subprocess boundary instead, so its invocation must
    consume one separate call-budget unit when a result was actually emitted.
    """

    if _expected_capture_mode(request) != "SUBPROCESS":
        return 0
    return int(
        any(
            isinstance(candidate.get("backend_result"), Mapping)
            or candidate.get("runtime_profile_id") is not None
            for candidate in _mapping_values(value)
        )
    )


def _ai_source_url_bindings(value: Any) -> list[dict[str, Any]]:
    """Return only URLs that are structurally bound to an emitted claim."""

    bindings: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for candidate in _mapping_values(value):
        field_semantics = str(
            candidate.get("field_semantics")
            or candidate.get("field")
            or candidate.get("field_name")
            or ""
        ).strip()
        occurrence_id = str(
            candidate.get("occurrence_id")
            or candidate.get("release_occurrence_id")
            or candidate.get("event_key")
            or ""
        ).strip()
        reference_period = str(
            candidate.get("reference_period")
            or candidate.get("release_reference_period")
            or candidate.get("period")
            or ""
        ).strip()
        value_present = (
            "value" in candidate and candidate.get("value") is not None
        )
        declared_value_hash = str(
            candidate.get("value_sha256") or ""
        ).strip()
        claim_value_hash = (
            _stable_sha256(candidate.get("value"))
            if value_present
            else declared_value_hash
        )
        evidence_rows = [
            item
            for item in (candidate.get("evidence") or ())
            if isinstance(item, Mapping)
        ]
        if (
            (value_present or declared_value_hash)
            and candidate.get("source_url")
        ):
            evidence_rows.append(candidate)
        if not field_semantics or not claim_value_hash:
            continue
        for evidence in evidence_rows:
            source_url = str(
                evidence.get("canonical_url")
                or evidence.get("source_url")
                or ""
            ).strip()
            if not source_url:
                continue
            identity = (
                source_url,
                field_semantics,
                claim_value_hash,
                occurrence_id,
                reference_period,
            )
            if identity in seen:
                continue
            seen.add(identity)
            bindings.append(
                {
                    "source_url": source_url,
                    "field_semantics": field_semantics,
                    "value_sha256": claim_value_hash,
                    "occurrence_id": occurrence_id or None,
                    "reference_period": reference_period or None,
                    "publisher": str(evidence.get("publisher") or "")
                    or None,
                    "evidence_text": str(
                        evidence.get("evidence_text") or ""
                    ).strip()
                    or None,
                    "metric_id": str(
                        candidate.get("metric_id") or ""
                    ).strip()
                    or None,
                    "frequency": str(
                        candidate.get("frequency") or ""
                    ).strip()
                    or None,
                    "unit": str(candidate.get("unit") or "").strip()
                    or None,
                    "_value": candidate.get("value"),
                }
            )
    return bindings


def _normalized_source_url(source_url: str) -> str | None:
    try:
        parsed = urlsplit(str(source_url).strip())
    except ValueError:
        return None
    if (
        parsed.scheme.casefold() != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
    ):
        return None
    try:
        redacted = urlsplit(redact_sensitive(source_url))
        hostname = (redacted.hostname or parsed.hostname).casefold()
        port = redacted.port
    except ValueError:
        return None
    netloc = hostname if port is None else f"{hostname}:{port}"
    query = urlencode(
        sorted(parse_qsl(redacted.query, keep_blank_values=True)),
        doseq=True,
    )
    return urlunsplit(
        (
            parsed.scheme.casefold(),
            netloc,
            redacted.path or "/",
            query,
            "",
        )
    )


def _allowed_source_domains(request: ProbeRequest) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            str(domain).strip().casefold().removeprefix("www.")
            for target in request.targets
            for domain in (
                getattr(
                    target.capability,
                    "runtime_source_domains",
                    (),
                )
                or getattr(
                    target.registration,
                    "source_domains",
                    (),
                )
                or ()
            )
            if str(domain).strip()
        )
    )


def _allowed_source_domains_for_binding(
    request: ProbeRequest,
    binding: Mapping[str, Any],
) -> tuple[str, ...]:
    metric_id = str(binding.get("metric_id") or "").strip()
    occurrence_id = str(binding.get("occurrence_id") or "").strip()
    reference_period = str(
        binding.get("reference_period") or ""
    ).strip()
    matched_targets = []
    for target in request.targets:
        correlation = request.correlation_for(target)
        if metric_id and target.metric_id != metric_id:
            continue
        if occurrence_id and str(
            correlation.get("expected_occurrence_id") or ""
        ) != occurrence_id:
            continue
        if reference_period and not _reference_period_matches(
            reference_period,
            (
                str(
                    correlation.get("expected_reference_period")
                    or ""
                ),
            ),
        ):
            continue
        matched_targets.append(target)
    return tuple(
        dict.fromkeys(
            str(domain).strip().casefold().removeprefix("www.")
            for target in matched_targets
            for domain in (
                getattr(
                    target.capability,
                    "runtime_source_domains",
                    (),
                )
                or getattr(
                    target.registration,
                    "source_domains",
                    (),
                )
                or ()
            )
            if str(domain).strip()
        )
    )


def _domain_matches(host: str, allowed_domain: str) -> bool:
    normalized_host = str(host or "").casefold().rstrip(".")
    normalized_allowed = (
        str(allowed_domain or "")
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


def _source_url_host(source_url: str) -> str | None:
    try:
        parsed = urlsplit(source_url)
    except ValueError:
        return None
    return (
        parsed.hostname.casefold().rstrip(".")
        if parsed.hostname
        else None
    )


_PUBLISHER_DOMAIN_ALIASES = {
    "aaii": "aaii",
    "ap": "apnews",
    "bea": "bea",
    "bls": "bls",
    "bloomberg": "bloomberg",
    "cboe": "cboe",
    "cftc": "cftc",
    "cme": "cmegroup",
    "cnbc": "cnbc",
    "federalreserve": "federalreserve",
    "fred": "stlouisfed",
    "ft": "ft",
    "invesco": "invesco",
    "nasdaq": "nasdaq",
    "reuters": "reuters",
    "sec": "sec",
    "treasury": "treasury",
    "wsj": "wsj",
}


def _publisher_matches_source_host(
    publisher: str,
    host: str,
) -> bool:
    normalized_host = host.casefold().removeprefix("www.")
    publisher_tokens = {
        token
        for token in re.findall(r"[a-z0-9]+", publisher.casefold())
        if len(token) >= 2
        and token
        not in {
            "and",
            "bureau",
            "company",
            "corporation",
            "department",
            "global",
            "group",
            "inc",
            "news",
            "office",
            "official",
            "the",
            "united",
            "states",
        }
    }
    host_labels = set(re.findall(r"[a-z0-9]+", normalized_host))
    return any(
        token in host_labels
        or _PUBLISHER_DOMAIN_ALIASES.get(token) in host_labels
        for token in publisher_tokens
    )


def _publisher_matches_source(
    source_policy: SourcePolicyService,
    publisher: str,
    source_url: str,
) -> bool:
    """Use policy aliases first; retain a bounded fallback for test/unknown rules."""

    if source_policy.publisher_matches_url(publisher, source_url):
        return True
    host = _source_url_host(source_url)
    return bool(
        host
        and source_policy.rule_for(source_url, publisher) is None
        and _publisher_matches_source_host(publisher, host)
    )


@lru_cache(maxsize=1)
def _default_source_policy() -> SourcePolicyService:
    return SourcePolicyService()


def _exchange_supports_claim(
    exchange: CapturedHttpExchange,
    *,
    field_name: str,
    value: Any,
    candidate: Mapping[str, Any],
) -> bool:
    body_text = exchange.response_body.decode(
        "utf-8",
        errors="ignore",
    ).casefold()
    normalized_body = re.sub(r"\s+", " ", body_text).strip()
    evidence_text = re.sub(
        r"\s+",
        " ",
        str(candidate.get("evidence_text") or "").casefold(),
    ).strip()
    if not evidence_text or evidence_text not in normalized_body:
        return False
    if not _evidence_field_value_pair_matches(
        evidence_text,
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
        # This is an orchestration/profile identifier, not a publisher metric.
        # The request correlation proves the selected profile; the source body
        # still has to prove the atomic field/value and occurrence context.
        raw_metric_tokens.clear()
    metric_tokens = raw_metric_tokens - {"yoy", "mom", "qoq"}
    metric_basis_tokens = set().union(
        *(
            _claim_frequency_aliases(token)
            for token in raw_metric_tokens & {"yoy", "mom", "qoq"}
        )
    )
    reference_tokens = _period_tokens(
        str(candidate.get("reference_period") or "")
    )
    occurrence_tokens = _period_tokens(
        str(candidate.get("occurrence_id") or "")
    )
    unit_tokens = _claim_unit_aliases(candidate.get("unit"))
    frequency_tokens = _claim_frequency_aliases(
        candidate.get("frequency")
    )
    declared_context_groups = [
        metric_tokens,
        metric_basis_tokens,
        reference_tokens | occurrence_tokens,
        unit_tokens,
        frequency_tokens,
    ]
    context_groups = [group for group in declared_context_groups if group]
    return bool(
        context_groups
        and all(
            any(
                _claim_context_token_in_text(token, evidence_text)
                for token in group
            )
            for group in context_groups
        )
    )


_FIELD_VALUE_ALIASES = {
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


def _evidence_field_value_pair_matches(
    evidence_text: str,
    *,
    field_name: str,
    value: Any,
) -> bool:
    normalized_field = field_name.casefold().replace("_", " ")
    aliases = _FIELD_VALUE_ALIASES.get(normalized_field)
    if not aliases:
        return _claim_value_in_text(value, evidence_text)
    all_aliases = tuple(
        dict.fromkeys(
            alias
            for values in _FIELD_VALUE_ALIASES.values()
            for alias in values
        )
    )
    label_pattern = re.compile(
        r"\b(?:"
        + "|".join(
            re.escape(alias)
            for alias in sorted(all_aliases, key=len, reverse=True)
        )
        + r")\b"
    )
    labels = list(label_pattern.finditer(evidence_text))
    for index, match in enumerate(labels):
        if match.group(0) not in aliases:
            continue
        segment_end = (
            labels[index + 1].start()
            if index + 1 < len(labels)
            else len(evidence_text)
        )
        segment = evidence_text[match.end() : segment_end]
        if _claim_value_in_text(value, segment):
            return True
    return False


def _claim_value_in_text(value: Any, text: str) -> bool:
    expected_number = _claim_decimal_value(value)
    if expected_number is not None:
        return any(
            observed == expected_number
            for observed in _claim_numbers_in_text(text)
        )
    if isinstance(value, str):
        token = value.strip().casefold()
        return _claim_string_value_in_text(token, text)
    token = json.dumps(
        _json_safe(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).casefold()
    return bool(token and token in text)


def _claim_string_value_in_text(value: str, text: str) -> bool:
    normalized = re.sub(r"\s+", " ", value.casefold()).strip()
    if not normalized:
        return False
    normalized_url = _normalized_source_url(normalized)
    if normalized_url is not None:
        return any(
            _normalized_source_url(candidate.rstrip(".,);]"))
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
    return _claim_context_token_in_text(normalized, text)


_CLAIM_NUMBER_PATTERN = re.compile(
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


def _claim_decimal_value(value: Any) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    token = str(value).strip()
    if not token:
        return None
    match = _CLAIM_NUMBER_PATTERN.fullmatch(token)
    if match is None:
        return None
    return _claim_decimal_token(match.group(0))


def _claim_numbers_in_text(text: str) -> tuple[Decimal, ...]:
    return tuple(
        number
        for match in _CLAIM_NUMBER_PATTERN.finditer(text)
        if (number := _claim_decimal_token(match.group(0))) is not None
    )


def _claim_decimal_token(token: str) -> Decimal | None:
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


def _claim_unit_aliases(value: Any) -> set[str]:
    normalized = str(value or "").strip().casefold()
    if not normalized:
        return set()
    if normalized in {"%", "percent", "percentage", "percentage point"}:
        return {"%", "percent", "percentage", "percentage point"}
    return {normalized}


def _claim_context_token_in_text(token: str, text: str) -> bool:
    normalized = re.sub(r"\s+", " ", str(token).casefold()).strip()
    if not normalized:
        return False
    escaped = re.escape(normalized)
    left = r"(?<![a-z0-9])" if normalized[0].isalnum() else ""
    right = r"(?![a-z0-9])" if normalized[-1].isalnum() else ""
    return re.search(f"{left}{escaped}{right}", text) is not None


def _claim_frequency_aliases(value: Any) -> set[str]:
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


def _resolve_source_host(host: str) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            address[4][0]
            for address in socket.getaddrinfo(
                host,
                443,
                type=socket.SOCK_STREAM,
            )
        )
    )


def _redirect_host(
    response: httpx.Response,
    source_url: str,
) -> str | None:
    location = str(response.headers.get("location") or "").strip()
    if not location:
        return None
    try:
        return httpx.URL(source_url).join(location).host
    except (TypeError, ValueError):
        return None


def _unsupported_outcome(
    reason_code: str,
    *,
    error_kind: str | None = None,
) -> ProbeOutcome:
    return ProbeOutcome(
        transport_status=HealthStatus.UNUSABLE.value,
        error_kind=error_kind,
        reason_codes=(reason_code,),
        checks={
            "transport_valid": None,
            "schema_valid": None,
            "completeness_valid": None,
            "freshness_valid": None,
            "semantic_mapping_valid": None,
            "occurrence_match_valid": None,
            "lineage_valid": None,
        },
        evidence={
            "real_adapter_invoked": False,
            "probe_dispatch_status": "UNSUPPORTED",
        },
    )


def _terminal_unsupported_outcome(reason_code: str) -> ProbeOutcome:
    return ProbeOutcome(
        configured=True,
        transport_status=HealthStatus.UNUSABLE.value,
        attempts=0,
        reason_codes=(reason_code,),
        checks={
            "transport_valid": None,
            "schema_valid": None,
            "completeness_valid": None,
            "freshness_valid": None,
            "semantic_mapping_valid": None,
            "occurrence_match_valid": None,
            "lineage_valid": None,
        },
        evidence={
            "real_adapter_invoked": False,
            "probe_dispatch_status": "TERMINAL_UNSUPPORTED",
        },
    )


def _failed_transport_outcome(
    request: ProbeRequest,
    capture: HttpxAuditCapture,
    started: float,
    health: HealthStatus,
    reason_code: str,
    error_kind: str,
    *,
    dispatch_status: str = "REAL_ADAPTER",
) -> ProbeOutcome:
    last_exchange = capture.exchanges[-1] if capture.exchanges else None
    expected_mode = _expected_capture_mode(request)
    captured_attempt = capture.attempts > 0
    local_invocation = (
        expected_mode == "LOCAL_SANDBOX"
        and dispatch_status == "REAL_ADAPTER"
    )
    dispatch_observed = captured_attempt or local_invocation
    effective_dispatch_status = (
        dispatch_status if dispatch_observed else "FAILED"
    )
    return ProbeOutcome(
        configured=True,
        transport_status=health.value,
        http_status=last_exchange.status_code if last_exchange else None,
        headers=last_exchange.response_headers if last_exchange else {},
        raw_response=last_exchange.response_body if last_exchange else None,
        normalized_response=None,
        latency_ms=round((time.perf_counter() - started) * 1000, 3),
        attempts=capture.attempts,
        error_kind=error_kind,
        reason_codes=(reason_code,),
        checks={
            "transport_valid": False,
            "schema_valid": None,
            "completeness_valid": None,
            "freshness_valid": None,
            "semantic_mapping_valid": None,
            "occurrence_match_valid": None,
            "lineage_valid": None,
        },
        evidence={
            "real_adapter_invoked": (
                effective_dispatch_status == "REAL_ADAPTER"
            ),
            "probe_dispatch_status": effective_dispatch_status,
            "adapter_path": request.adapter_path,
            "probe_id": request.targets[0].probe_id,
            "network_call_count": capture.attempts,
            "capture_mode": (
                "HTTPX"
                if captured_attempt
                else expected_mode
                if local_invocation
                else "NONE"
            ),
            "capture_mode_expected": expected_mode,
            "capture_verified": (
                captured_attempt
                or local_invocation
            ),
            "capture_attestation": {
                "attempted_send_count": capture.attempts,
                "response_exchange_count": len(capture.exchanges),
                "failure_reason_code": reason_code,
                "bounded_timeout_observed": False,
                "local_invocation_observed": local_invocation,
                "source_exchange_count": len(capture.exchanges),
            }
            if (
                captured_attempt
                or local_invocation
            )
            else None,
            "capture_mode_configuration": (
                _capture_mode_configuration(request)
            ),
        },
        network_exchanges=tuple(capture.exchanges),
    )


def _validated_dispatch_observation(
    value: Any,
) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    events_observed = value.get("events_observed")
    search_attempts = value.get("search_attempts")
    source_open_attempts = value.get("source_open_attempts")
    event_hashes = value.get("event_sha256")
    if (
        value.get("schema_version")
        != "provider-audit-dispatch-observation-v1"
        or value.get("origin") != "BACKEND_EVENT_OBSERVER"
        or value.get("budget_stop_observed") is not True
        or type(events_observed) is not int
        or events_observed < 1
        or type(search_attempts) is not int
        or type(source_open_attempts) is not int
        or max(search_attempts, source_open_attempts) <= 1
        or not isinstance(event_hashes, list)
        or len(event_hashes) != events_observed
        or any(
            not isinstance(item, str)
            or re.fullmatch(r"[0-9a-f]{64}", item) is None
            for item in event_hashes
        )
    ):
        return None
    return {
        "schema_version": str(value["schema_version"]),
        "origin": str(value["origin"]),
        "backend_class": str(value.get("backend_class") or ""),
        "budget_stop_observed": True,
        "events_observed": events_observed,
        "search_attempts": search_attempts,
        "source_open_attempts": source_open_attempts,
        "event_sha256": list(event_hashes),
    }


def _request_settings(
    settings: Settings,
    request: ProbeRequest,
) -> Settings:
    """Return request-local writable settings with no access to provider caches.

    External probes always start from an empty SQLite database so an operational
    cache hit cannot masquerade as a source response.  Repository probes get a
    private copy of the already isolated database snapshot; any migrations or
    writes performed by their constructors therefore remain acquisition-local.
    """

    request_root = (
        request.sandbox_root / "acquisitions" / _safe_path_part(request.acquisition_id)
    ).resolve()
    request_root.mkdir(parents=True, exist_ok=False)
    database_path = request_root / "audit.sqlite"
    provider_type = request.targets[0].provider_type.upper()
    if (
        provider_type == "REPOSITORY"
        and request.database_snapshot_path is not None
        and request.database_snapshot_path.is_file()
    ):
        source = request.database_snapshot_path
        for suffix in ("", "-wal", "-shm"):
            source_file = Path(f"{source}{suffix}")
            if source_file.is_file():
                destination = Path(f"{database_path}{suffix}")
                shutil.copyfile(source_file, destination)
    sandboxed = _sandbox_settings(settings, database_path, request_root)
    if request.provider_id == "AI_RESEARCHER":
        target_count = max(len(request.targets), 1)
        sandboxed = sandboxed.model_copy(
            update={
                "ai_researcher_max_events": target_count,
                "ai_researcher_max_macro_events": target_count,
            }
        )
    return sandboxed


def _safe_path_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._") or "probe"


def _load_symbol(path: str) -> Any:
    module_name, separator, symbol_name = path.replace(":", ".").rpartition(".")
    if not separator:
        raise ImportError(f"adapter path is not dotted: {path}")
    return getattr(importlib.import_module(module_name), symbol_name)


def _find_field(value: Any, field_name: str) -> tuple[Any, Any]:
    aliases = {
        field_name.casefold(),
        field_name.casefold().replace("_", ""),
    }
    stack = [value]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if isinstance(current, (dict, list)):
            identity = id(current)
            if identity in seen:
                continue
            seen.add(identity)
        if isinstance(current, Mapping):
            for key, item in current.items():
                normalized_key = str(key).casefold()
                if normalized_key in aliases or normalized_key.replace("_", "") in aliases:
                    return item, current
                stack.append(item)
        elif isinstance(current, list):
            stack.extend(reversed(current))
    return None, None


def _find_target_field(
    value: Any,
    target: Any,
    field_name: str,
) -> tuple[Any, Any]:
    _observed, field_value, owner = _find_target_field_observation(
        value,
        target,
        field_name,
    )
    return field_value, owner


def _find_target_field_observation(
    value: Any,
    target: Any,
    field_name: str,
) -> tuple[bool, Any, Any]:
    scopes, identities_observed = _target_scopes(value, str(target.metric_id))
    for scope in scopes:
        field_value, owner = _find_field(scope, field_name)
        if owner is not None:
            return True, field_value, owner
    if identities_observed:
        return False, None, None
    field_value, owner = _find_field(value, field_name)
    return owner is not None, field_value, owner


def _explicit_null_reason(owner: Any) -> str | None:
    if not isinstance(owner, Mapping):
        return None
    value = owner.get("reason_code") or owner.get("null_reason")
    reason = str(value or "").strip()
    return reason or None


def _target_scopes(value: Any, metric_id: str) -> tuple[list[Any], bool]:
    desired = metric_id.strip().casefold()
    identity_keys = {
        "metric_id",
        "event_metric_id",
        "series_id",
        "canonical_series_id",
        "metric",
    }
    matches: list[Any] = []
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
            for key, item in current.items():
                key_text = str(key).strip().casefold()
                if key_text in identity_keys and item not in (None, ""):
                    identities_observed = True
                    if str(item).strip().casefold() == desired:
                        matches.append(current)
                if key_text == desired and isinstance(
                    item,
                    (Mapping, list, tuple),
                ):
                    identities_observed = True
                    matches.append(item)
                if isinstance(item, (Mapping, list, tuple)):
                    stack.append(item)
        else:
            stack.extend(current)
    return matches, identities_observed


def _field_schema_check(
    target: Any,
    field_name: str,
    normalized: Any,
    owner: Any,
    value: Any,
) -> bool:
    if not isinstance(normalized, (Mapping, list, tuple)):
        return False
    validator_id = str(getattr(target, "field_validator_id", "") or "")
    if validator_id:
        declared = FIELD_VALIDATOR_SCHEMAS.get(validator_id)
        if declared is None or field_name not in declared:
            return False
    if not isinstance(owner, Mapping):
        return False
    contract = field_value_type_contract(validator_id, field_name)
    return bool(contract and _value_matches_schema_contract(value, contract))


def _value_matches_schema_contract(value: Any, contract: str) -> bool:
    # An observed explicit null is structurally valid; completeness and its
    # reason code are evaluated independently.
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
        return _valid_temporal_value(value)
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


def _valid_temporal_value(value: Any) -> bool:
    if isinstance(value, (datetime, date)):
        return True
    text = str(value or "").strip()
    if not text:
        return False
    if re.fullmatch(r"(?:19|20)\d{2}(?:-Q[1-4]|-(?:0[1-9]|1[0-2]))?", text, re.I):
        return True
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            date.fromisoformat(text)
        except ValueError:
            return False
    return True


def _walk_items(value: Any) -> list[tuple[str, Any]]:
    output: list[tuple[str, Any]] = []
    stack = [value]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if isinstance(current, (dict, list)):
            identity = id(current)
            if identity in seen:
                continue
            seen.add(identity)
        if isinstance(current, Mapping):
            for key, item in current.items():
                output.append((str(key).casefold(), item))
                stack.append(item)
        elif isinstance(current, list):
            stack.extend(current)
    return output


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _json_safe(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return _json_safe(value.model_dump(mode="json"))
    if is_dataclass(value) and not isinstance(value, type):
        return _json_safe(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "value"):
        return _json_safe(value.value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return repr(value)


def _stable_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            _json_safe(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _request_fingerprint(request: httpx.Request) -> str:
    return hashlib.sha256(
        (
            request.method.upper()
            + "\n"
            + redact_sensitive(str(request.url))
            + "\n"
            + hashlib.sha256(request.content).hexdigest()
        ).encode("utf-8")
    ).hexdigest()


def _value(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _registry_validation_result() -> Mapping[str, Any]:
    try:
        result = validate_registry(raise_on_error=False)
    except TypeError:
        try:
            result = validate_registry()
        except Exception as exc:
            return {"valid": False, "errors": [f"{type(exc).__name__}:{exc}"]}
    except Exception as exc:
        return {"valid": False, "errors": [f"{type(exc).__name__}:{exc}"]}
    if result is None:
        return {"valid": True, "errors": []}
    if isinstance(result, Mapping):
        return result
    if is_dataclass(result):
        return asdict(result)
    if isinstance(result, (list, tuple)):
        return {"valid": not result, "errors": [str(item) for item in result]}
    return {"valid": bool(result), "errors": [] if result else ["REGISTRY_INVALID"]}


def _sandbox_settings(settings: Settings, database_path: Path, root: Path) -> Settings:
    updates: dict[str, Any] = {"database_path": database_path}
    candidates = {
        "diagnostics_dir": root / "diagnostics",
        "ai_diagnostics_dir": root / "ai-diagnostics",
        "backups_dir": root / "backups",
        "logs_dir": root / "logs",
        "temp_dir": root / "temp",
        "codex_workspace_dir": root / "codex-workspace",
        "ai_job_workspace_root": root / "ai-jobs",
    }
    model_fields = type(settings).model_fields
    updates.update({name: path for name, path in candidates.items() if name in model_fields})
    for path in candidates.values():
        path.mkdir(parents=True, exist_ok=True)
    if "manual_event_enrichment_path" in model_fields:
        manual_source = Path(settings.manual_event_enrichment_path)
        manual_destination = root / "manual-event-enrichment.json"
        if manual_source.is_file():
            shutil.copyfile(manual_source, manual_destination)
        updates["manual_event_enrichment_path"] = manual_destination
    return settings.model_copy(update=updates)


def _configured_secret_values(settings: Settings) -> tuple[str, ...]:
    values: set[str] = set()
    for registration in PROVIDER_REGISTRY:
        for requirement in _value(registration, "credential_requirements", ()) or ():
            for name in str(requirement).split("|"):
                setting_name = name.strip().split(" ", 1)[0]
                value = getattr(settings, setting_name, None)
                if value and len(str(value)) >= 4:
                    values.add(str(value))
    return tuple(values)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the isolated Provider Capability Audit.",
    )
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--provider", action="append", default=[])
    parser.add_argument("--dataset", action="append", default=[])
    parser.add_argument("--metric", action="append", default=[])
    parser.add_argument(
        "--verify-pointer",
        type=Path,
        help="strongly verify an already-published latest pointer without probing",
    )
    parser.add_argument(
        "--publish-candidate",
        type=Path,
        help=(
            "strongly verify and atomically publish a completed candidate "
            "after the runner has verified process cleanup"
        ),
    )
    ai = parser.add_mutually_exclusive_group()
    ai.add_argument("--include-ai", dest="include_ai", action="store_true")
    ai.add_argument("--exclude-ai", dest="include_ai", action="store_false")
    parser.set_defaults(include_ai=True)
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> int:
    settings = Settings()
    output_root = (
        args.output_root.resolve()
        if args.output_root
        else (REPO_ROOT / "data" / "provider-capability-audit").resolve()
    )
    run_id = args.run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    writer = ProviderAuditArtifactWriter(
        output_root,
        run_id,
        secret_values=_configured_secret_values(settings),
    )
    sandbox = Path(
        tempfile.mkdtemp(
            prefix=f"provider-capability-audit-{run_id}-",
            dir=REPO_ROOT / "data",
        )
    ).resolve()
    guard = DatabaseBundleGuard(settings.database_path, sandbox)
    try:
        snapshot = guard.create_snapshot()
        # Provider probes receive an empty audit-only cache.  The operational
        # snapshot is passed separately and is copied again only for an
        # explicit repository capability, so a cache hit can never replace a
        # real external-source probe.
        writable_database = sandbox / "audit.sqlite"
        isolated_settings = _sandbox_settings(settings, writable_database, sandbox)
        engine = ProviderCapabilityAuditEngine(
            PROVIDER_REGISTRY,
            IsolatedRegistryProbeExecutor(isolated_settings),
            settings=isolated_settings,
            source_policies=DATASET_SOURCE_POLICIES,
        )
        execution = await engine.run(
            filters=AuditFilters.from_values(
                providers=args.provider,
                datasets=args.dataset,
                metrics=args.metric,
                include_ai=args.include_ai,
            ),
            run_id=run_id,
            sandbox_root=sandbox,
            database_snapshot_path=snapshot,
            artifact_writer=writer,
            registry_validation=_registry_validation_result(),
            database_source_unchanged=guard.source_unchanged,
        )
        if (
            execution.audit_status == "COMPLETED"
            and execution.report.get("full_audit_scope") is True
            and execution.candidate_pointer is None
        ):
            raise ProviderAuditPublicationRejected(
                ("CAPABILITY_AUDIT_CANDIDATE_POINTER_MISSING",)
            )
    except Exception:
        writer.abandon()
        raise
    finally:
        shutil.rmtree(sandbox, ignore_errors=True)
    summary = {
        "run_id": execution.run_id,
        "audit_status": execution.audit_status,
        "system_health": execution.system_health,
        "artifact_directory": str(execution.artifact_directory),
        "candidate_pointer": (
            str(execution.candidate_pointer)
            if execution.candidate_pointer is not None
            else None
        ),
        "latest_pointer": (
            str(execution.latest_pointer)
            if execution.latest_pointer is not None
            else None
        ),
        "registry": redact_payload(registry_summary()),
    }
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0 if execution.audit_status == "COMPLETED" else 1


def _strong_publication_verifier(
    candidate_pointer: Path,
    canonical_pointer: Path,
) -> tuple[
    Mapping[str, Any] | None,
    Mapping[str, Any] | None,
    Sequence[str],
]:
    return _verified_capability_audit(
        candidate_pointer,
        canonical_pointer_path=canonical_pointer,
    )


def _verify_published_pointer(pointer_path: Path) -> int:
    pointer, report, errors = _verified_capability_audit(pointer_path)
    print(
        json.dumps(
            {
                "run_id": pointer.get("run_id") if pointer else None,
                "audit_status": report.get("audit_status") if report else None,
                "verification_errors": errors,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 1 if errors else 0


def _publish_completed_candidate(candidate_pointer: Path) -> int:
    candidate = candidate_pointer.resolve()
    expected_parent = (REPO_ROOT / "data").resolve()
    if (
        candidate.parent != expected_parent
        or not candidate.name.startswith(
            ".provider-capability-audit-latest."
        )
        or not candidate.name.endswith(".candidate.json")
    ):
        print(
            json.dumps(
                {
                    "published": False,
                    "verification_errors": [
                        "CAPABILITY_AUDIT_CANDIDATE_LOCATION_INVALID"
                    ],
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
    latest = expected_parent / "provider-capability-audit-latest.json"
    try:
        published = publish_verified_audit_candidate(
            candidate,
            latest,
            _strong_publication_verifier,
        )
    except ProviderAuditPublicationRejected as exc:
        print(
            json.dumps(
                {
                    "published": False,
                    "verification_errors": list(exc.errors),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
    print(
        json.dumps(
            {
                "published": True,
                "latest_pointer": str(published),
                "verification_errors": [],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


def main() -> int:
    args = _parse_args()
    exclusive_modes = sum(
        value is not None
        for value in (args.verify_pointer, args.publish_candidate)
    )
    if exclusive_modes > 1:
        raise SystemExit(
            "--verify-pointer and --publish-candidate are mutually exclusive"
        )
    if args.verify_pointer is not None:
        return _verify_published_pointer(args.verify_pointer.resolve())
    if args.publish_candidate is not None:
        return _publish_completed_candidate(args.publish_candidate.resolve())
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
