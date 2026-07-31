from __future__ import annotations

import asyncio
import copy
import logging
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.core.config import Settings
from app.services.data_freshness_service import (
    CanonicalFreshnessPolicy,
    CanonicalFreshnessResult,
    DataFreshnessService,
    evaluate_canonical_freshness,
    parse_datetime,
)
from app.services.provider_adapter_factory import create_registered_adapter
from app.services.risk_context_normalization_service import (
    RiskContextNormalizationService,
    build_legacy_risk_sentiment,
)
from app.services.risk_context_repository import RiskContextHistoryRepository


logger = logging.getLogger(__name__)
RISK_CONTEXT_MAX_AGE = timedelta(hours=2)
_SUPPLEMENTAL_QQQ_DISABLED_REASON = "SUPPLEMENTAL_QQQ_OPTIONS_DISABLED_FOR_REQUEST_ACCOUNTING"


class RiskContextRuntimeService:
    def __init__(
        self,
        settings: Settings,
        repository: RiskContextHistoryRepository | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))
        self.repository = repository or RiskContextHistoryRepository(settings)
        self.freshness = DataFreshnessService(
            settings,
            clock=self.clock,
        )
        self.normalizer = RiskContextNormalizationService(settings)
        self.risk_indices_provider = create_registered_adapter("CBOE", settings)
        self.vix_futures_provider = create_registered_adapter(
            "CBOE",
            settings,
            adapter_name="CboeVixFuturesProvider",
        )
        self.put_call_provider = create_registered_adapter(
            "CBOE",
            settings,
            adapter_name="CboePutCallProvider",
        )
        self.qqq_options_provider = create_registered_adapter(
            "NASDAQ_QQQ_OPTIONS",
            settings,
        )
        self.last_database_lookup: dict[str, Any] | None = None

    def lookup_canonical(
        self,
    ) -> tuple[dict[str, Any] | None, CanonicalFreshnessResult]:
        latest = self.repository.latest()
        freshness = self.freshness.evaluate_canonical(
            latest,
            max_age=RISK_CONTEXT_MAX_AGE,
            data_reference_mode="point_in_time",
            data_as_of_fields=(
                "database_data_as_of",
                "data_as_of",
            ),
        )
        self.last_database_lookup = _database_lookup_evidence(freshness)
        return latest, freshness

    async def snapshot(
        self,
        *,
        refresh: str,
        macro_snapshot: dict[str, Any],
        preloaded_risk_indices: dict[str, Any] | None = None,
        preloaded_qqq_options: dict[str, Any] | None = None,
        include_supplemental_options: bool = True,
        existing_legacy: dict[str, Any] | None = None,
        canonical_preflight: (
            tuple[
                dict[str, Any] | None,
                CanonicalFreshnessResult,
            ]
            | None
        ) = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        logger.info("risk_context_lookup_started", extra={"refresh": refresh})
        latest, freshness = (
            canonical_preflight if canonical_preflight is not None else self.lookup_canonical()
        )
        self.last_database_lookup = _database_lookup_evidence(freshness)
        latest_uses_qqq = _canonical_uses_supplemental_qqq(latest)
        latest_allowed = bool(latest and (include_supplemental_options or not latest_uses_qqq))
        if latest_uses_qqq and not include_supplemental_options:
            self.last_database_lookup.update(
                {
                    "freshness": "INVALID",
                    "reason_code": _SUPPLEMENTAL_QQQ_DISABLED_REASON,
                }
            )
        if freshness.usable and latest_allowed:
            canonical = _runtime_view(
                latest,
                refresh=refresh,
                now=self.clock(),
            )
            if not include_supplemental_options:
                _record_supplemental_qqq_skip(canonical)
            return canonical, build_legacy_risk_sentiment(
                canonical,
                existing_legacy,
            )
        if refresh == "false":
            canonical = empty_risk_context(refresh=refresh)
            if latest:
                canonical["reason_code"] = (
                    freshness.reason_code
                    or "RISK_CONTEXT_CANONICAL_RECORD_NOT_USABLE"
                )
                canonical["warnings"] = [
                    "risk_context_canonical_record_rejected_refresh_false"
                ]
                canonical["diagnostics"]["cache_used"] = False
                canonical["diagnostics"][
                    "database_record_rejected"
                ] = True
            if not include_supplemental_options:
                _record_supplemental_qqq_skip(canonical)
            return canonical, build_legacy_risk_sentiment(canonical, existing_legacy)

        risk_indices = preloaded_risk_indices or {}
        qqq_options = preloaded_qqq_options or {} if include_supplemental_options else {}
        for source in ("Cboe Futures Exchange", "Cboe Daily Market Statistics"):
            logger.info("risk_source_attempted", extra={"source": source, "metric": "risk_context"})
        tasks: list[Any] = [self.vix_futures_provider.fetch(), self.put_call_provider.fetch()]
        fetch_indices = not risk_indices or _preloaded_requires_provider_call(
            risk_indices,
            now=self.clock(),
        )
        fetch_options = bool(
            include_supplemental_options
            and (
                not qqq_options
                or _preloaded_requires_provider_call(
                    qqq_options,
                    now=self.clock(),
                )
            )
        )
        if fetch_indices:
            tasks.append(self.risk_indices_provider.fetch())
        if fetch_options:
            tasks.append(self.qqq_options_provider.fetch())
        results = await asyncio.gather(*tasks, return_exceptions=True)
        futures = _provider_result(results[0], "Cboe Futures Exchange")
        put_call = _provider_result(results[1], "Cboe Daily Market Statistics")
        offset = 2
        if fetch_indices:
            risk_indices = _provider_result(results[offset], "CBOE Delayed Quotes")
            offset += 1
        if fetch_options:
            qqq_options = _provider_result(results[offset], "Nasdaq QQQ Option Chain")

        observed_payloads = (risk_indices, futures, put_call)
        if include_supplemental_options:
            observed_payloads = (*observed_payloads, qqq_options)
        for payload in observed_payloads:
            event = (
                "risk_source_succeeded"
                if payload.get("status") in {"found", "partial", "valid"}
                else "risk_source_failed"
            )
            logger.info(
                event,
                extra={
                    "source": payload.get("source") or payload.get("provider"),
                    "metric": "risk_context",
                    "fallback_reason": None
                    if event.endswith("succeeded")
                    else payload.get("status"),
                },
            )

        history = self.repository.history()
        candidate = self.normalizer.build(
            risk_indices=risk_indices,
            vix_futures=futures,
            cboe_put_call=put_call,
            qqq_options=qqq_options,
            macro_snapshot=macro_snapshot,
            snapshot_history=history,
            now=self.clock(),
        )
        candidate["diagnostics"]["provider_calls"] += int(fetch_indices) + int(fetch_options)
        if not include_supplemental_options:
            candidate["diagnostics"].update(
                {
                    "source_attempt_count": 3,
                    "source_failure_count": max(
                        3 - int(candidate["diagnostics"].get("source_success_count") or 0),
                        0,
                    ),
                }
            )
            _record_supplemental_qqq_skip(candidate)
        expected_depth = self.repository.count() + (
            1 if candidate.get("status") != "not_found" else 0
        )
        candidate["history"]["snapshot_count"] = expected_depth
        candidate["diagnostics"]["history_snapshot_count"] = expected_depth
        current_score = float((candidate.get("quality") or {}).get("quality_score") or 0)
        previous_score = float(((latest or {}).get("quality") or {}).get("quality_score") or 0)
        if latest_allowed and freshness.usable and (
            candidate.get("status") == "not_found" or current_score + 0.1 < previous_score
        ):
            canonical = _runtime_view(
                latest,
                refresh=refresh,
                now=self.clock(),
            )
            canonical["status"] = (
                "stale_acceptable" if not freshness.usable else latest.get("status")
            )
            canonical["source_summary"]["last_known_good_used"] = True
            canonical["quality"]["last_known_good_penalty"] = 0.05
            canonical["quality"]["quality_score"] = round(max(previous_score - 0.05, 0), 3)
            canonical["diagnostics"]["last_known_good_used"] = True
            canonical["warnings"] = list(
                dict.fromkeys(
                    [
                        *(canonical.get("warnings") or []),
                        "new_risk_snapshot_did_not_replace_higher_quality_last_known_good",
                    ]
                )
            )
            logger.warning(
                "risk_fallback_selected", extra={"fallback_reason": "candidate_lower_quality"}
            )
            return canonical, build_legacy_risk_sentiment(canonical, existing_legacy)
        if candidate.get("status") == "not_found":
            return candidate, build_legacy_risk_sentiment(candidate, existing_legacy)

        self.repository.append(candidate)
        canonical = self.repository.latest() or candidate
        canonical["diagnostics"].update(
            {
                "persisted_count": 1,
                "read_back_count": 1,
                "materialized_count": 1,
            }
        )
        logger.info("risk_snapshot_persisted", extra={"status": canonical.get("status")})
        logger.info(
            "risk_snapshot_read_back",
            extra={"quality_score": canonical.get("quality", {}).get("quality_score")},
        )
        logger.info("risk_snapshot_materialized", extra={"status": canonical.get("status")})
        logger.info(
            "risk_history_updated",
            extra={
                "metric": "risk_context",
                "value": self.repository.count(),
                "data_as_of": canonical.get("data_as_of"),
            },
        )
        canonical = _runtime_view(
            canonical,
            refresh=refresh,
            force_read_back=True,
            now=self.clock(),
        )
        return canonical, build_legacy_risk_sentiment(canonical, existing_legacy)


def _canonical_uses_supplemental_qqq(
    payload: dict[str, Any] | None,
) -> bool:
    if not isinstance(payload, dict):
        return False
    put_call = payload.get("put_call") or {}
    for ratio in put_call.get("ratios") or []:
        if not isinstance(ratio, dict):
            continue
        ratio_id = str(ratio.get("ratio_id") or "").lower()
        source = str(ratio.get("source") or "").lower()
        if ratio_id.startswith("qqq_") or "nasdaq qqq" in source:
            return True
    return False


def _record_supplemental_qqq_skip(payload: dict[str, Any]) -> None:
    diagnostics = payload.setdefault("diagnostics", {})
    diagnostics["supplemental_qqq_options"] = {
        "status": "not_called",
        "attempted": False,
        "provider_calls": 0,
        "actual_network_calls": 0,
        "selected": False,
        "reason_code": _SUPPLEMENTAL_QQQ_DISABLED_REASON,
    }


def empty_risk_context(*, refresh: str) -> dict[str, Any]:
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    metric = {
        "status": "not_found",
        "value": None,
        "warnings": ["not_in_db_refresh_false"] if refresh == "false" else [],
        "errors": [],
    }
    return {
        "status": "not_found",
        "reason_code": "RISK_CONTEXT_NOT_AVAILABLE",
        "data_as_of": None,
        "retrieved_at": now,
        "valid_until": None,
        "next_refresh_at": None,
        "vix": dict(metric),
        "vvix": dict(metric),
        "skew": dict(metric),
        "vix_term_structure": {"status": "not_found", "contracts": [], "structure": "UNKNOWN"},
        "put_call": {"status": "not_found", "ratios": [], "by_id": {}},
        "derived_context": {"composite_status": "NOT_AVAILABLE"},
        "history": {
            "snapshot_count": 0,
            "compact_series": [],
            "history_status": "history_insufficient",
        },
        "source_summary": {"selected_sources": {}, "last_known_good_used": False},
        "quality": {
            "quality_score": 0.0,
            "vix_available": False,
            "vvix_available": False,
            "skew_available": False,
            "vix_curve_coverage_pct": 0.0,
            "put_call_scope_coverage_pct": 0.0,
            "official_source_coverage_pct": 0.0,
        },
        "diagnostics": {
            "source_attempt_count": 0,
            "source_success_count": 0,
            "source_failure_count": 0,
            "provider_calls": 0,
            "actual_network_calls": 0,
            "browser_calls": 0,
            "AI_called": False,
            "cache_used": refresh == "false",
            "history_snapshot_count": 0,
        },
        "warnings": ["risk_context_not_in_db_refresh_false"] if refresh == "false" else [],
        "errors": [],
        "service_role": "data provider only",
    }


def _runtime_view(
    payload: dict[str, Any] | None,
    *,
    refresh: str,
    force_read_back: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    if not payload:
        return empty_risk_context(refresh=refresh)
    current = now or datetime.now(UTC)
    output = copy.deepcopy(payload)
    selected_sources = (output.get("source_summary") or {}).get("selected_sources") or {}
    if not output.get("source") and isinstance(
        selected_sources,
        dict,
    ):
        output["source"] = selected_sources.get("risk")
    diagnostics = output.setdefault("diagnostics", {})
    if refresh == "false":
        diagnostics["provider_calls"] = 0
        diagnostics["actual_network_calls"] = 0
    diagnostics["browser_calls"] = 0
    diagnostics["AI_called"] = False
    # Every payload handled here came from the canonical history repository,
    # including a just-persisted read-back after provider acquisition.
    diagnostics["cache_used"] = True
    cache_status = "DB_READ_BACK" if force_read_back else "DB"
    for key in ("vix", "vvix", "skew"):
        output.setdefault(key, {})["cache_status"] = cache_status
    for contract in (output.get("vix_term_structure") or {}).get("contracts") or []:
        contract["cache_status"] = cache_status
    curve = output.get("vix_term_structure") or {}
    curve["cache_status"] = cache_status
    for key in ("front_month", "second_month", "third_month"):
        if isinstance(curve.get(key), dict):
            curve[key]["cache_status"] = cache_status
    for ratio in (output.get("put_call") or {}).get("ratios") or []:
        ratio["cache_status"] = cache_status
    for ratio in ((output.get("put_call") or {}).get("by_id") or {}).values():
        if isinstance(ratio, dict):
            ratio["cache_status"] = cache_status
    output["stale"] = _is_stale(output, now=current)
    retrieved = parse_datetime(output.get("retrieved_at"))
    output["age_minutes"] = (
        round(max((current - retrieved).total_seconds() / 60, 0), 2) if retrieved else None
    )
    return output


def _provider_result(value: Any, source: str) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, Exception):
        return {
            "status": "provider_failed",
            "source": source,
            "warnings": [],
            "errors": [str(value) or type(value).__name__],
            "diagnostics": {"actual_network_calls": 0},
        }
    return {
        "status": "not_found",
        "source": source,
        "warnings": ["empty_payload"],
        "errors": [],
        "diagnostics": {"actual_network_calls": 0},
    }


def _is_stale(
    payload: dict[str, Any],
    *,
    now: datetime | None = None,
) -> bool:
    current = now or datetime.now(UTC)
    decision = evaluate_canonical_freshness(
        payload,
        policy=CanonicalFreshnessPolicy(
            max_age=RISK_CONTEXT_MAX_AGE,
            data_reference_mode="point_in_time",
            data_as_of_fields=(
                "database_data_as_of",
                "data_as_of",
            ),
        ),
        observed_at=current,
    )
    return not decision.usable


def _preloaded_requires_provider_call(
    payload: dict[str, Any],
    *,
    now: datetime,
) -> bool:
    # A provider execution already observed in this request must not be
    # repeated here. Cached preloads, however, must pass the canonical
    # freshness policy before they can suppress a provider call.
    if (
        payload.get("attempted") is True
        and int(payload.get("provider_calls") or 0) > 0
        and payload.get("cache_used") is not True
    ):
        return False
    if payload.get("status") in {None, "not_found"}:
        return True
    return _is_stale(payload, now=now)


def _database_lookup_evidence(
    result: CanonicalFreshnessResult,
) -> dict[str, Any]:
    return {
        "performed": True,
        "found": result.found,
        "data_as_of": result.data_as_of,
        "content_valid_until": result.content_valid_until,
        "refresh_due_at": result.refresh_due_at,
        "lifecycle_status": result.lifecycle,
        "expired": result.expired,
        "freshness": result.evaluation,
        "reason_code": result.reason_code,
    }
