from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable, Mapping, Protocol

from app.core.config import Settings
from app.services.data_freshness_service import parse_datetime
from app.services.event_driven_lifecycle_service import compute_datum_lifecycle
from app.services.research_agent_enablement import research_agent_enablement


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
        return {
            "status": "RESOLVED",
            "reason": f"{self.name}_resolved",
            "datum": datum,
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
        if status not in {"RESOLVED", "FRESH", "SUCCEEDED"}:
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
        return {
            "status": status,
            "reason": reason,
            "ai_eligible": bool(decision["agent_enabled"]),
            "agent_status": (
                "ENABLED" if decision["agent_enabled"] else "DISABLED"
            ),
            "execution_status": (
                "ELIGIBLE" if decision["agent_enabled"] else "NOT_REQUESTED"
            ),
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
) -> dict[str, LifecycleProviderAdapter]:
    """Wire only provider services that already exist in application bootstrap."""

    def macro(_: dict[str, Any]) -> Any:
        return macro_service.latest()

    async def events(_: dict[str, Any]) -> Any:
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
            events,
            select=_select_event,
            name="event_service",
        ),
        "macro_actual": CallableLifecycleProviderAdapter(
            events,
            select=_select_event,
            name="event_service",
        ),
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
    return adapters


def _model_dump(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    dump = getattr(value, "model_dump", None)
    return dump(mode="json") if callable(dump) else {}


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
