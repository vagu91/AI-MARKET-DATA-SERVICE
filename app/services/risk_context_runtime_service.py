from __future__ import annotations

import asyncio
import copy
import logging
from datetime import UTC, datetime
from typing import Any

from app.core.config import Settings
from app.providers.cboe_put_call_provider import CboePutCallProvider
from app.providers.cboe_risk_indices_provider import CboeRiskIndicesProvider
from app.providers.cboe_vix_futures_provider import CboeVixFuturesProvider
from app.providers.nasdaq_qqq_option_chain_provider import NasdaqQQQOptionChainProvider
from app.services.data_freshness_service import parse_datetime
from app.services.risk_context_normalization_service import (
    RiskContextNormalizationService,
    build_legacy_risk_sentiment,
)
from app.services.risk_context_repository import RiskContextHistoryRepository


logger = logging.getLogger(__name__)


class RiskContextRuntimeService:
    def __init__(self, settings: Settings, repository: RiskContextHistoryRepository | None = None) -> None:
        self.settings = settings
        self.repository = repository or RiskContextHistoryRepository(settings)
        self.normalizer = RiskContextNormalizationService(settings)
        self.risk_indices_provider = CboeRiskIndicesProvider(settings)
        self.vix_futures_provider = CboeVixFuturesProvider(settings)
        self.put_call_provider = CboePutCallProvider(settings)
        self.qqq_options_provider = NasdaqQQQOptionChainProvider(settings)

    async def snapshot(
        self,
        *,
        refresh: str,
        macro_snapshot: dict[str, Any],
        preloaded_risk_indices: dict[str, Any] | None = None,
        preloaded_qqq_options: dict[str, Any] | None = None,
        existing_legacy: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        logger.info("risk_context_lookup_started", extra={"refresh": refresh})
        latest = self.repository.latest()
        if refresh == "false" or (refresh == "auto" and latest and not _is_stale(latest)):
            canonical = _runtime_view(latest, refresh=refresh) if latest else empty_risk_context(refresh=refresh)
            return canonical, build_legacy_risk_sentiment(canonical, existing_legacy)

        risk_indices = preloaded_risk_indices or {}
        qqq_options = preloaded_qqq_options or {}
        for source in ("Cboe Futures Exchange", "Cboe Daily Market Statistics"):
            logger.info("risk_source_attempted", extra={"source": source, "metric": "risk_context"})
        tasks: list[Any] = [self.vix_futures_provider.fetch(), self.put_call_provider.fetch()]
        fetch_indices = not risk_indices or risk_indices.get("status") in {None, "not_found"} or _is_stale(risk_indices)
        fetch_options = not qqq_options or qqq_options.get("status") in {None, "not_found"} or _is_stale(qqq_options)
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

        for payload in (risk_indices, futures, put_call, qqq_options):
            event = "risk_source_succeeded" if payload.get("status") in {"found", "partial", "valid"} else "risk_source_failed"
            logger.info(event, extra={"source": payload.get("source") or payload.get("provider"), "metric": "risk_context", "fallback_reason": None if event.endswith("succeeded") else payload.get("status")})

        history = self.repository.history()
        candidate = self.normalizer.build(
            risk_indices=risk_indices,
            vix_futures=futures,
            cboe_put_call=put_call,
            qqq_options=qqq_options,
            macro_snapshot=macro_snapshot,
            snapshot_history=history,
        )
        candidate["diagnostics"]["provider_calls"] += int(fetch_indices) + int(fetch_options)
        expected_depth = self.repository.count() + (1 if candidate.get("status") != "not_found" else 0)
        candidate["history"]["snapshot_count"] = expected_depth
        candidate["diagnostics"]["history_snapshot_count"] = expected_depth
        current_score = float((candidate.get("quality") or {}).get("quality_score") or 0)
        previous_score = float(((latest or {}).get("quality") or {}).get("quality_score") or 0)
        if latest and (candidate.get("status") == "not_found" or current_score + 0.1 < previous_score):
            canonical = _runtime_view(latest, refresh=refresh)
            canonical["status"] = "stale_acceptable" if _is_stale(latest) else latest.get("status")
            canonical["source_summary"]["last_known_good_used"] = True
            canonical["quality"]["last_known_good_penalty"] = 0.05
            canonical["quality"]["quality_score"] = round(max(previous_score - 0.05, 0), 3)
            canonical["diagnostics"]["last_known_good_used"] = True
            canonical["warnings"] = list(dict.fromkeys([*(canonical.get("warnings") or []), "new_risk_snapshot_did_not_replace_higher_quality_last_known_good"]))
            logger.warning("risk_fallback_selected", extra={"fallback_reason": "candidate_lower_quality"})
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
        logger.info("risk_snapshot_read_back", extra={"quality_score": canonical.get("quality", {}).get("quality_score")})
        logger.info("risk_snapshot_materialized", extra={"status": canonical.get("status")})
        logger.info("risk_history_updated", extra={"metric": "risk_context", "value": self.repository.count(), "data_as_of": canonical.get("data_as_of")})
        canonical = _runtime_view(canonical, refresh=refresh, force_read_back=True)
        return canonical, build_legacy_risk_sentiment(canonical, existing_legacy)


def empty_risk_context(*, refresh: str) -> dict[str, Any]:
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    metric = {"status": "not_found", "value": None, "warnings": ["not_in_db_refresh_false"] if refresh == "false" else [], "errors": []}
    return {
        "status": "not_found",
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
        "history": {"snapshot_count": 0, "compact_series": [], "history_status": "history_insufficient"},
        "source_summary": {"selected_sources": {}, "last_known_good_used": False},
        "quality": {"quality_score": 0.0, "vix_available": False, "vvix_available": False, "skew_available": False, "vix_curve_coverage_pct": 0.0, "put_call_scope_coverage_pct": 0.0, "official_source_coverage_pct": 0.0},
        "diagnostics": {"source_attempt_count": 0, "source_success_count": 0, "source_failure_count": 0, "provider_calls": 0, "actual_network_calls": 0, "browser_calls": 0, "AI_called": False, "cache_used": refresh == "false", "history_snapshot_count": 0},
        "warnings": ["risk_context_not_in_db_refresh_false"] if refresh == "false" else [],
        "errors": [],
        "service_role": "data provider only",
    }


def _runtime_view(payload: dict[str, Any] | None, *, refresh: str, force_read_back: bool = False) -> dict[str, Any]:
    if not payload:
        return empty_risk_context(refresh=refresh)
    output = copy.deepcopy(payload)
    diagnostics = output.setdefault("diagnostics", {})
    if refresh == "false":
        diagnostics["provider_calls"] = 0
        diagnostics["actual_network_calls"] = 0
    diagnostics["browser_calls"] = 0
    diagnostics["AI_called"] = False
    diagnostics["cache_used"] = refresh in {"false", "auto"} or force_read_back
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
    output["stale"] = _is_stale(output)
    retrieved = parse_datetime(output.get("retrieved_at"))
    output["age_minutes"] = round(max((datetime.now(UTC) - retrieved).total_seconds() / 60, 0), 2) if retrieved else None
    return output


def _provider_result(value: Any, source: str) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, Exception):
        return {"status": "provider_failed", "source": source, "warnings": [], "errors": [str(value) or type(value).__name__], "diagnostics": {"actual_network_calls": 0}}
    return {"status": "not_found", "source": source, "warnings": ["empty_payload"], "errors": [], "diagnostics": {"actual_network_calls": 0}}


def _is_stale(payload: dict[str, Any]) -> bool:
    valid_until = parse_datetime(payload.get("valid_until"))
    return bool(valid_until and datetime.now(UTC) >= valid_until)
