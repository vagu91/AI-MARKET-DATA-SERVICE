from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Mapping, Protocol

from app.core.config import Settings
from app.services.data_freshness_service import parse_datetime
from app.services.event_driven_lifecycle_service import compute_datum_lifecycle
from app.services.research_agent_enablement import research_agent_enablement
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

    def resolve(self, item: dict[str, Any]) -> dict[str, Any]:
        del item
        return {
            "status": self.status,
            "datum": dict(self.datum or {}),
            "reason": self.reason or "static_provider_result",
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

    def resolve(self, item: dict[str, Any]) -> dict[str, Any]:
        output = self.acquire(item)
        if inspect.isawaitable(output):
            output = asyncio.run(output)
        datum = self.select(output, item)
        if not datum:
            if _has_temporary_provider_failure(output):
                raise TemporaryLifecycleProviderError(
                    f"{self.name}_temporary_failure"
                )
            return {
                "status": "NO_DATA",
                "reason": f"{self.name}_exhausted",
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
        lookback_start = now - timedelta(
            hours=int(self.settings.lifecycle_startup_catchup_hours)
        )
        if release < lookback_start:
            return {
                "status": "NO_DATA",
                "reason": "macro_actual_occurrence_outside_lookback",
            }

        expected_key = str(
            payload.get("canonical_event_key")
            or canonical_event_key(payload)
        )
        entity_key = str(item.get("entity_key") or "")
        if entity_key.startswith("event:") and entity_key != expected_key:
            return {
                "status": "NO_DATA",
                "reason": "macro_actual_item_identity_mismatch",
            }
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
            return {
                "status": "NO_DATA",
                "reason": "macro_actual_exact_occurrence_not_found",
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
        )
        status = str(resolution.get("status") or "NO_DATA").upper()
        if status == "OFFICIAL_FEED_DELAYED" or resolution.get(
            "retryable"
        ) is True:
            return {
                "status": "DEFERRED",
                "reason": str(
                    resolution.get("error")
                    or "official_macro_actual_feed_delayed"
                ),
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
            "datum": datum,
            "missing_fields": missing_fields,
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

    def resolve(self, item: dict[str, Any]) -> dict[str, Any]:
        now = self.clock()
        entity_type = str(item.get("entity_type") or "unknown").lower()
        entity_key = str(item.get("entity_key") or "")
        datum = item.get("payload")
        if isinstance(datum, dict) and datum:
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
            if committed.freshness_state == "FRESH":
                return {
                    "status": "RESOLVED",
                    "reason": "committed_provider_payload_is_fresh",
                    "datum": datum,
                    "lifecycle": committed,
                    "next_refresh_at": committed.next_refresh_at,
                    "ai_eligible": False,
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
            }

        adapter = self.adapters.get(entity_type)
        if adapter is None:
            return self._exhausted(
                item,
                status="EXHAUSTED",
                reason="deterministic_provider_not_configured",
            )
        try:
            result = (
                adapter.resolve(item)
                if hasattr(adapter, "resolve")
                else adapter(item)
            )
            if inspect.isawaitable(result):
                result = asyncio.run(result)
        except TemporaryLifecycleProviderError as exc:
            return self._temporary_failure(item, reason=str(exc) or "provider_temporary_error")
        except (TimeoutError, ConnectionError) as exc:
            return self._temporary_failure(
                item,
                reason=f"provider_temporary_error:{type(exc).__name__}",
            )

        if not isinstance(result, dict):
            return self._temporary_failure(
                item,
                reason="provider_contract_invalid",
            )
        status = str(result.get("status") or "NO_DATA").upper()
        if status in {
            "DEFERRED",
            "TEMPORARY_ERROR",
            "TIMEOUT",
            "RATE_LIMITED",
            "UNAVAILABLE",
        }:
            return self._temporary_failure(
                item,
                reason=str(result.get("reason") or status.lower()),
            )
        if status in {"EXHAUSTED", "NOT_FOUND", "NO_DATA", "NOT_CONFIGURED"}:
            return self._exhausted(
                item,
                status="NO_DATA" if status == "NO_DATA" else "EXHAUSTED",
                reason=str(result.get("reason") or "deterministic_provider_exhausted"),
            )
        if status not in {"RESOLVED", "FRESH", "SUCCEEDED", "PARTIAL"}:
            return self._temporary_failure(
                item,
                reason=f"provider_status_invalid:{status}",
            )
        provider_datum = result.get("datum")
        if not isinstance(provider_datum, dict) or not provider_datum:
            return self._exhausted(
                item,
                status="NO_DATA",
                reason="deterministic_provider_returned_no_data",
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
        if lifecycle.freshness_state != "FRESH":
            return self._exhausted(
                item,
                status="NO_DATA",
                reason="deterministic_provider_result_not_fresh",
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
            }
        return {
            **result,
            "status": "RESOLVED",
            "datum": provider_datum,
            "lifecycle": lifecycle,
            "next_refresh_at": lifecycle.next_refresh_at,
            "ai_eligible": False,
        }

    def _exhausted(
        self,
        item: dict[str, Any],
        *,
        status: str,
        reason: str,
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
        return {
            "status": status,
            "reason": reason,
            "ai_eligible": bool(
                decision["agent_enabled"] and not retry_deadline_exhausted
            ),
            "agent_status": (
                "ENABLED" if decision["agent_enabled"] else "DISABLED"
            ),
            "execution_status": (
                "ELIGIBLE"
                if decision["agent_enabled"] and not retry_deadline_exhausted
                else "NOT_REQUESTED"
            ),
            "data_outcome": (
                "NO_DATA" if retry_deadline_exhausted else "PENDING"
            ),
            "retry_deadline_exhausted": retry_deadline_exhausted,
            "enablement": decision,
        }

    def _temporary_failure(
        self,
        item: dict[str, Any],
        *,
        reason: str,
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
    actual_lineage = {
        "source": source,
        "publisher": candidate.get("publisher"),
        "source_url": source_url,
        "canonical_url": candidate.get("canonical_url"),
        "source_tier": candidate.get("source_tier") or 1,
        "source_classification": (
            candidate.get("source_classification") or "official_source"
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
        "validation_status": (
            candidate.get("validation_status") or "accepted"
        ),
    }
    enrichment = (
        dict(event.get("enrichment") or {})
        if isinstance(event.get("enrichment"), dict)
        else {}
    )
    field_lineage = dict(enrichment.get("field_lineage") or {})
    field_lineage["actual"] = actual_lineage
    enrichment.update(
        {
            "actual": value,
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
        "source": source,
        "source_url": source_url,
        "source_lineage": [actual_lineage],
        "acquisition_method": "api_provider",
        "actual_is_official": True,
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
