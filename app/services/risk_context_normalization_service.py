from __future__ import annotations

import math
import re
import logging
from datetime import UTC, date, datetime, time, timedelta
from statistics import fmean, pstdev
from typing import Any

from app.core.config import Settings
from app.services.data_freshness_service import parse_datetime


SOURCE_RANKS = {
    "risk_indices": {
        "official_cboe_current": 6,
        "official_cboe_historical": 5,
        "verified_institutional_provider": 4,
        "last_known_good_official": 3,
        "secondary_provider": 2,
        "not_found": 0,
    },
    "vix_futures": {
        "official_cfe_cboe": 5,
        "verified_futures_market_provider": 4,
        "last_known_good_official": 3,
        "secondary_provider": 2,
        "not_found": 0,
    },
    "put_call": {
        "official_cboe_statistics": 5,
        "verified_exchange_derived_provider": 4,
        "last_known_good_official": 3,
        "secondary_provider": 2,
        "not_found": 0,
    },
}

logger = logging.getLogger(__name__)


class RiskContextNormalizationService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build(
        self,
        *,
        risk_indices: dict[str, Any],
        vix_futures: dict[str, Any],
        cboe_put_call: dict[str, Any],
        qqq_options: dict[str, Any],
        macro_snapshot: dict[str, Any],
        snapshot_history: list[dict[str, Any]],
        now: datetime | None = None,
    ) -> dict[str, Any]:
        now = now or datetime.now(UTC)
        histories = risk_indices.get("history") or {}
        vix = normalize_vix(
            macro_snapshot,
            history=histories.get("vix") or [],
            history_min=self.settings.risk_context_history_min_points,
            now=now,
        )
        vvix = normalize_risk_index(
            "VVIX",
            (risk_indices.get("indices") or {}).get("vvix") or {},
            history=histories.get("vvix") or [],
            history_min=self.settings.risk_context_history_min_points,
            now=now,
        )
        skew = normalize_risk_index(
            "SKEW",
            (risk_indices.get("indices") or {}).get("skew") or {},
            history=histories.get("skew") or [],
            history_min=self.settings.risk_context_history_min_points,
            now=now,
        )
        curve = normalize_vix_curve(
            vix_futures.get("contracts") or [],
            vix_spot=vix.get("value"),
            flat_tolerance_pct=self.settings.risk_curve_flat_tolerance_pct,
            source_payload=vix_futures,
            now=now,
        )
        put_call = normalize_put_call(
            cboe_put_call.get("ratios") or [],
            qqq_options=qqq_options,
            snapshot_history=snapshot_history,
            history_min=self.settings.risk_context_history_min_points,
            now=now,
        )
        alignment = calculate_temporal_alignment(
            [vix, vvix, skew, curve, *(put_call.get("ratios") or [])],
            now=now,
            max_gap_minutes=self.settings.risk_alignment_max_gap_minutes,
        )
        quality = calculate_risk_quality(vix, vvix, skew, curve, put_call, alignment)
        composite = composite_status(vix, vvix, skew, curve, put_call, alignment)
        if vvix.get("status") == "found":
            logger.info("vvix_snapshot_loaded", extra={"metric": "VVIX", "value": vvix.get("value"), "data_as_of": vvix.get("data_as_of"), "freshness": vvix.get("freshness")})
        if skew.get("status") == "found":
            logger.info("skew_snapshot_loaded", extra={"metric": "SKEW", "value": skew.get("value"), "data_as_of": skew.get("data_as_of"), "freshness": skew.get("freshness")})
        for contract in curve.get("contracts") or []:
            logger.info("vix_future_contract_loaded", extra={"contract_symbol": contract.get("contract_symbol"), "expiration_date": contract.get("expiration_date"), "value": contract.get("last_price"), "source": contract.get("source")})
        logger.info("vix_curve_validated" if curve.get("status") == "found" else "vix_curve_rejected", extra={"contract_symbol": None, "value": curve.get("m1_m2_spread_pct"), "source": curve.get("source")})
        for ratio in put_call.get("ratios") or []:
            logger.info("put_call_snapshot_loaded", extra={"scope": ratio.get("scope"), "basis": ratio.get("basis"), "value": ratio.get("ratio"), "data_as_of": ratio.get("data_as_of"), "source": ratio.get("source")})
        if not alignment.get("aligned"):
            logger.warning("risk_temporal_alignment_degraded", extra={"stale": False, "fallback_reason": "temporal_misalignment", "duration_ms": None})
        logger.info("risk_derived_metrics_calculated", extra={"metric": "risk_context", "value": quality.get("quality_score"), "freshness": alignment.get("market_session_status")})
        timestamps = [
            value
            for value in (vix.get("data_as_of"), vvix.get("data_as_of"), skew.get("data_as_of"), curve.get("data_as_of"), put_call.get("data_as_of"))
            if value
        ]
        retrieved_at = _iso(now)
        valid_until = _iso(now + timedelta(minutes=self.settings.risk_context_ttl_minutes))
        diagnostics = build_diagnostics(
            risk_indices=risk_indices,
            vix_futures=vix_futures,
            cboe_put_call=cboe_put_call,
            qqq_options=qqq_options,
            vvix=vvix,
            skew=skew,
            curve=curve,
            put_call=put_call,
            alignment=alignment,
            history_count=len(snapshot_history),
        )
        return {
            "status": "available" if composite == "COMPLETE" else "partial" if composite == "PARTIAL" else "degraded" if composite == "DEGRADED" else "not_found",
            "data_as_of": max(timestamps, default=None),
            "retrieved_at": retrieved_at,
            "valid_until": valid_until,
            "next_refresh_at": valid_until,
            "age_minutes": 0.0,
            "stale": False,
            "market_session_status": market_session_status(now),
            "last_successful_refresh_at": retrieved_at,
            "vix": vix,
            "vvix": vvix,
            "skew": skew,
            "vix_term_structure": curve,
            "put_call": put_call,
            "data_alignment": alignment,
            "derived_context": {
                "volatility_level": _relative_context("VIX", vix),
                "volatility_of_volatility": _relative_context("VVIX", vvix),
                "tail_risk": _relative_context("SKEW", skew),
                "curve_regime": {
                    "structure": curve.get("structure"),
                    "m1_m2_spread_pct": curve.get("m1_m2_spread_pct"),
                },
                "options_positioning": {
                    "equity_put_call_regime": _ratio_regime(put_call, "equity_volume_put_call"),
                    "index_put_call_regime": _ratio_regime(put_call, "index_volume_put_call"),
                },
                "data_alignment": alignment,
                "composite_status": composite,
            },
            "history": {
                "snapshot_count": len(snapshot_history),
                "compact_series": compact_history(snapshot_history),
                "history_status": "available" if snapshot_history else "history_insufficient",
            },
            "source_summary": build_source_summary(vix, vvix, skew, curve, put_call),
            "quality": quality,
            "diagnostics": diagnostics,
            "warnings": list(dict.fromkeys([
                *(risk_indices.get("warnings") or []),
                *(vix_futures.get("warnings") or []),
                *(cboe_put_call.get("warnings") or []),
                *(alignment.get("warnings") or []),
            ])),
            "errors": list(dict.fromkeys([
                *(risk_indices.get("errors") or []),
                *(vix_futures.get("errors") or []),
                *(cboe_put_call.get("errors") or []),
            ])),
            "service_role": "data provider only",
        }


def normalize_risk_index(
    symbol: str,
    raw: dict[str, Any],
    *,
    history: list[dict[str, Any]],
    history_min: int,
    now: datetime,
) -> dict[str, Any]:
    value = _positive_float(raw.get("current_price"))
    previous = _positive_float(raw.get("previous_close"))
    observed = _metric_datetime(raw.get("last_trade_time") or raw.get("provider_timestamp") or raw.get("retrieved_at"))
    warnings = list(raw.get("warnings") or [])
    errors: list[str] = []
    if value is None:
        errors.append("invalid_value")
    if observed and observed > now + timedelta(minutes=5):
        errors.append("invalid_timestamp")
        value = None
    stats = historical_statistics(value, history, min_points=history_min)
    if stats["percentile_1y"] is None:
        warnings.append("history_insufficient")
    stale = bool(raw.get("stale"))
    regime = relative_regime(stats.get("percentile_1y"), high_label="ELEVATED_RELATIVE")
    change = raw.get("change")
    if change is None and value is not None and previous is not None:
        change = round(value - previous, 6)
    if value is not None and change is not None and (
        previous is None or not math.isclose(value - previous, float(change), abs_tol=0.01)
    ):
        previous = round(value - float(change), 6)
        warnings.append("provider_previous_close_inconsistent_derived_from_change")
    change_pct = raw.get("percentage_change")
    if change_pct is None and change is not None and previous:
        change_pct = round(float(change) / previous * 100, 6)
    return {
        "status": "found" if value is not None else "not_found",
        "symbol": symbol,
        "value": value,
        "previous_close": previous,
        "change": change,
        "change_pct": change_pct,
        "data_as_of": _iso(observed) if observed else raw.get("retrieved_at"),
        "source": raw.get("source") or "CBOE",
        "source_url": raw.get("source_url"),
        "provider_type": "OFFICIAL_EXCHANGE_DELAYED_QUOTE",
        "retrieved_at": raw.get("retrieved_at"),
        "valid_until": None,
        "freshness": "STALE" if stale else "RECENT",
        "reliability": raw.get("reliability") or 0.0,
        "confidence": 0.9 if value is not None and not stale else 0.55 if value is not None else 0.0,
        "is_official_source": bool(raw.get("is_official_source", True)),
        "cache_status": raw.get("cache_status") or "provider",
        "stale": stale,
        **stats,
        "tail_risk_regime": regime if symbol == "SKEW" else None,
        "relative_regime": regime,
        "warnings": list(dict.fromkeys(warnings)),
        "errors": errors,
    }


def normalize_vix(
    macro_snapshot: dict[str, Any], *, history: list[dict[str, Any]], history_min: int, now: datetime
) -> dict[str, Any]:
    raw = (macro_snapshot.get("financial_conditions") or {}).get("VIXCLS") or {}
    value = _positive_float(raw.get("value"))
    stats = historical_statistics(value, history, min_points=history_min)
    previous = history[-2]["value"] if len(history) >= 2 else None
    change = round(value - previous, 6) if value is not None and previous is not None else None
    return {
        "status": "found" if value is not None else "not_found",
        "symbol": "VIX",
        "value": value,
        "previous_close": previous,
        "change": change,
        "change_pct": round(change / previous * 100, 6) if change is not None and previous else None,
        "data_as_of": raw.get("data_as_of"),
        "source": raw.get("source") or "FRED",
        "source_url": raw.get("source_url") or "https://fred.stlouisfed.org/series/VIXCLS",
        "provider_type": raw.get("provider_type") or "OFFICIAL_MACRO_API",
        "retrieved_at": raw.get("retrieved_at"),
        "valid_until": raw.get("valid_until"),
        "freshness": "LAST_SESSION" if value is not None and str(raw.get("freshness") or "UNKNOWN").upper() == "UNKNOWN" else raw.get("freshness"),
        "reliability": raw.get("reliability") or 0.95 if value is not None else 0.0,
        "confidence": raw.get("reliability") or 0.95 if value is not None else 0.0,
        "is_official_source": bool(raw.get("actual_is_official")) or str(raw.get("source") or "FRED").upper() == "FRED",
        "cache_status": raw.get("cache_status") or "DB",
        "stale": str(raw.get("freshness") or "").upper() in {"STALE", "EXPIRED"},
        **stats,
        "relative_regime": relative_regime(stats.get("percentile_1y"), high_label="ELEVATED_RELATIVE"),
        "warnings": ["history_insufficient"] if stats["percentile_1y"] is None else [],
        "errors": [] if value is not None else ["vix_not_available"],
    }


def historical_statistics(value: float | None, history: list[dict[str, Any]], *, min_points: int) -> dict[str, Any]:
    values = [float(item["value"]) for item in history if _positive_float(item.get("value")) is not None]
    latest_1y = values[-252:]
    percentile = z_score = None
    if value is not None and len(latest_1y) >= max(min_points, 2):
        percentile = round(sum(item <= value for item in latest_1y) / len(latest_1y) * 100, 4)
        deviation = pstdev(latest_1y)
        z_score = round((value - fmean(latest_1y)) / deviation, 6) if deviation else 0.0
    return {
        "change_1d": round(value - values[-2], 6) if value is not None and len(values) >= 2 else None,
        "change_5d": round(value - values[-6], 6) if value is not None and len(values) >= 6 else None,
        "percentile_1y": percentile,
        "z_score_1y": z_score,
        "history_depth": len(values),
    }


def relative_regime(percentile: float | None, *, high_label: str = "HIGH_RELATIVE") -> str:
    if percentile is None:
        return "UNKNOWN"
    if percentile < 10:
        return "LOW_RELATIVE"
    if percentile < 75:
        return "NORMAL_RELATIVE"
    if percentile < 95:
        return high_label
    return "EXTREME_RELATIVE"


def select_ranked_source(
    candidates: list[dict[str, Any]], *, family: str, last_known_good: dict[str, Any] | None = None
) -> dict[str, Any] | None:
    ranks = SOURCE_RANKS[family]
    eligible = [item for item in candidates if item.get("status") in {"found", "available", "partial"}]
    if last_known_good:
        eligible.append(last_known_good)
    return max(
        eligible,
        key=lambda item: (
            ranks.get(str(item.get("ranking_class") or "not_found"), 0),
            int(item.get("status") in {"found", "available"}),
            float(item.get("quality_score") or 0),
        ),
        default=None,
    )


def normalize_vix_curve(
    contracts: list[dict[str, Any]],
    *,
    vix_spot: float | None,
    flat_tolerance_pct: float,
    source_payload: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    valid: list[dict[str, Any]] = []
    seen: set[str] = set()
    expired = duplicate = invalid = 0
    for item in sorted(contracts, key=lambda row: str(row.get("expiration_date") or "")):
        symbol = str(item.get("contract_symbol") or "")
        price = _positive_float(item.get("last_price"))
        try:
            expiration = date.fromisoformat(str(item.get("expiration_date") or ""))
        except ValueError:
            invalid += 1
            continue
        if expiration <= now.date():
            expired += 1
            continue
        if not symbol or price is None:
            invalid += 1
            continue
        if symbol in seen:
            duplicate += 1
            continue
        seen.add(symbol)
        row = dict(item)
        row["last_price"] = price
        row["tenor"] = f"M{len(valid) + 1}"
        valid.append(row)
    m1 = valid[0] if valid else None
    m2 = valid[1] if len(valid) > 1 else None
    m3 = valid[2] if len(valid) > 2 else None
    m6 = valid[5] if len(valid) > 5 else None
    spread_points = round(m2["last_price"] - m1["last_price"], 6) if m1 and m2 else None
    spread_pct = round((m2["last_price"] / m1["last_price"] - 1) * 100, 6) if m1 and m2 else None
    adjacent = [round((b["last_price"] / a["last_price"] - 1) * 100, 6) for a, b in zip(valid, valid[1:])]
    structure = classify_curve(adjacent, tolerance_pct=flat_tolerance_pct)
    data_as_of = source_payload.get("data_as_of") or (m1.get("data_as_of") if m1 else None)
    return {
        "status": "found" if len(valid) >= 2 else "partial" if valid else "not_found",
        "spot": vix_spot,
        "front_month": m1,
        "second_month": m2,
        "third_month": m3,
        "contracts": valid[:6],
        "contract_count": len(valid[:6]),
        "coverage_pct": round(min(len(valid), 6) / 6 * 100, 4),
        "m1_m2_spread_points": spread_points,
        "m1_m2_spread_pct": spread_pct,
        "spot_m1_spread_points": round(m1["last_price"] - vix_spot, 6) if m1 and vix_spot else None,
        "spot_m1_spread_pct": round((m1["last_price"] / vix_spot - 1) * 100, 6) if m1 and vix_spot else None,
        "curve_slope_m1_m3": round(m3["last_price"] - m1["last_price"], 6) if m1 and m3 else None,
        "curve_slope_m1_m6": round(m6["last_price"] - m1["last_price"], 6) if m1 and m6 else None,
        "weighted_front_30d": None,
        "implied_roll_slope": spread_pct,
        "structure": structure,
        "data_as_of": data_as_of,
        "retrieved_at": source_payload.get("retrieved_at"),
        "valid_until": source_payload.get("valid_until"),
        "source": source_payload.get("source"),
        "source_url": source_payload.get("source_url"),
        "provider_type": "OFFICIAL_EXCHANGE_SETTLEMENT",
        "is_official_source": bool(source_payload.get("is_official_source")),
        "freshness": "LAST_SESSION",
        "reliability": 0.96 if valid else 0.0,
        "confidence": 0.94 if len(valid) >= 2 else 0.55 if valid else 0.0,
        "cache_status": "provider",
        "diagnostics": {
            "valid_contract_count": len(valid),
            "expired_contract_count": expired,
            "duplicate_contract_count": duplicate,
            "invalid_contract_count": invalid,
            "adjacent_spreads_pct": adjacent,
        },
        "warnings": ["partial_curve_missing_m2"] if len(valid) == 1 else [],
        "errors": [],
    }


def classify_curve(adjacent_spreads_pct: list[float], *, tolerance_pct: float) -> str:
    if not adjacent_spreads_pct:
        return "UNKNOWN"
    signs = [0 if abs(value) <= tolerance_pct else 1 if value > 0 else -1 for value in adjacent_spreads_pct]
    nonzero = {value for value in signs if value}
    if not nonzero:
        return "FLAT"
    if len(nonzero) > 1:
        return "MIXED"
    return "CONTANGO" if 1 in nonzero else "BACKWARDATION"


def normalize_put_call(
    cboe_ratios: list[dict[str, Any]],
    *,
    qqq_options: dict[str, Any],
    snapshot_history: list[dict[str, Any]],
    history_min: int,
    now: datetime,
) -> dict[str, Any]:
    ratios = [dict(item) for item in cboe_ratios]
    qqq_rows = qqq_put_call_ratios(qqq_options, now=now)
    ratios.extend(qqq_rows)
    for item in ratios:
        prior = ratio_history(
            snapshot_history,
            item["ratio_id"],
            exclude_data_as_of=item.get("data_as_of"),
        )
        values = [row["ratio"] for row in prior]
        current_value = float(item["ratio"])
        observations = [current_value, *values]
        five_day_prior = _prior_at_least_days(prior, item.get("data_as_of"), days=5)
        history_depth = len(observations)
        item.update(
            {
                "moving_average_5d": round(fmean(observations[:5]), 6) if history_depth >= 5 else None,
                "moving_average_20d": round(fmean(observations[:20]), 6) if history_depth >= 20 else None,
                "change_1d": round(current_value - values[0], 6) if values else None,
                "change_5d": round(current_value - five_day_prior["ratio"], 6) if five_day_prior else None,
                "percentile_1y": _percentile(current_value, values[:252]) if len(values) >= history_min else None,
                "z_score_1y": _zscore(current_value, values[:252]) if len(values) >= history_min else None,
                "history_depth": history_depth,
                "history_status": "SUFFICIENT" if len(values) >= history_min else "INSUFFICIENT",
            }
        )
        item["relative_regime"] = relative_regime(item["percentile_1y"])
        if item["percentile_1y"] is None:
            item["warnings"] = list(dict.fromkeys([*(item.get("warnings") or []), "history_insufficient"]))
    ratios.sort(key=lambda item: item["ratio_id"])
    by_id = {item["ratio_id"]: item for item in ratios}
    timestamps = [item.get("data_as_of") for item in ratios if item.get("data_as_of")]
    return {
        "status": "found" if ratios else "not_found",
        "ratios": ratios,
        "by_id": by_id,
        "ratio_count": len(ratios),
        "data_as_of": max(timestamps, default=None),
        "scope_coverage_pct": round(min(len({item["ratio_id"] for item in ratios}) / 7 * 100, 100.0), 4),
        "warnings": [],
        "errors": [],
    }


def qqq_put_call_ratios(payload: dict[str, Any], *, now: datetime) -> list[dict[str, Any]]:
    global_values = payload.get("global_aggregates") if isinstance(payload.get("global_aggregates"), dict) else None
    observed = payload.get("observed_aggregates") or payload.get("aggregates") or {}
    values = global_values or observed
    if not values:
        return []
    complete = global_values is not None and bool((global_values.get("scope") or {}).get("full_chain_complete", True))
    rows = []
    for basis, put_key, call_key in (
        ("volume", "put_volume", "call_volume"),
        ("open_interest", "put_open_interest", "call_open_interest"),
    ):
        put_value = values.get(put_key)
        call_value = values.get(call_key)
        if put_value is None:
            put_value = values.get(f"observed_{put_key}")
        if call_value is None:
            call_value = values.get(f"observed_{call_key}")
        try:
            put_number, call_number = int(put_value), int(call_value)
        except (TypeError, ValueError):
            continue
        if put_number < 0 or call_number <= 0:
            continue
        retrieved = payload.get("retrieved_at") or _iso(now)
        rows.append(
            {
                "ratio_id": f"qqq_{basis}_put_call",
                "scope": "qqq",
                "basis": basis,
                "put_value": put_number,
                "call_value": call_number,
                "ratio": round(put_number / call_number, 6),
                "data_as_of": _qqq_data_as_of((payload.get("snapshot") or {}).get("source_timestamp"), retrieved),
                "source": "Nasdaq QQQ Option Chain",
                "source_url": payload.get("source_url"),
                "provider_type": "EXCHANGE_DISTRIBUTED_PARTIAL_CHAIN" if not complete else "EXCHANGE_DISTRIBUTED_CHAIN",
                "retrieved_at": retrieved,
                "valid_until": payload.get("valid_until"),
                "freshness": "RECENT",
                "reliability": 0.82 if complete else 0.58,
                "confidence": 0.8 if complete else 0.5,
                "is_official_source": False,
                "cache_status": "provider",
                "warnings": [] if complete else ["qqq_ratio_from_partial_observed_chain"],
                "errors": [],
            }
        )
    return rows


def ratio_history(
    snapshots: list[dict[str, Any]],
    ratio_id: str,
    *,
    exclude_data_as_of: Any = None,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    excluded = _observation_date(exclude_data_as_of)
    for snapshot in snapshots:
        item = (((snapshot.get("put_call") or {}).get("by_id") or {}).get(ratio_id))
        if not item or _positive_float(item.get("ratio")) is None:
            continue
        observed = _observation_date(item.get("data_as_of") or snapshot.get("data_as_of"))
        if observed is None or observed == excluded or observed in seen_dates:
            continue
        seen_dates.add(observed)
        output.append({"ratio": float(item["ratio"]), "data_as_of": observed})
    return output


def _prior_at_least_days(
    observations: list[dict[str, Any]],
    current_data_as_of: Any,
    *,
    days: int,
) -> dict[str, Any] | None:
    current = _observation_date_value(current_data_as_of)
    if current is None:
        return None
    cutoff = current - timedelta(days=days)
    return next(
        (
            item
            for item in observations
            if (observed := _observation_date_value(item.get("data_as_of"))) is not None
            and observed <= cutoff
        ),
        None,
    )


def _observation_date(value: Any) -> str | None:
    if value in (None, ""):
        return None
    parsed = parse_datetime(value)
    if parsed:
        return parsed.date().isoformat()
    raw = str(value).strip()
    try:
        return date.fromisoformat(raw[:10]).isoformat()
    except ValueError:
        return None


def _observation_date_value(value: Any) -> date | None:
    observed = _observation_date(value)
    if observed is None:
        return None
    try:
        return date.fromisoformat(observed)
    except ValueError:
        return None


def calculate_temporal_alignment(
    metrics: list[dict[str, Any]], *, now: datetime, max_gap_minutes: int
) -> dict[str, Any]:
    timestamps = []
    for item in metrics:
        if item.get("status") == "not_found":
            continue
        parsed = _metric_datetime(item.get("data_as_of") or item.get("retrieved_at"))
        if parsed:
            timestamps.append(parsed)
    if not timestamps:
        return {"aligned": False, "max_timestamp_gap_minutes": None, "oldest_metric_at": None, "newest_metric_at": None, "session_consistent": False, "warnings": ["temporal_alignment_not_available"]}
    oldest, newest = min(timestamps), max(timestamps)
    gap = round((newest - oldest).total_seconds() / 60, 2)
    session = market_session_status(now)
    allowed = max(max_gap_minutes * 3, 1440) if session in {"weekend", "market_closed"} else max_gap_minutes
    aligned = gap <= allowed
    return {
        "aligned": aligned,
        "max_timestamp_gap_minutes": gap,
        "oldest_metric_at": _iso(oldest),
        "newest_metric_at": _iso(newest),
        "session_consistent": aligned or session in {"weekend", "market_closed"},
        "market_session_status": session,
        "warnings": [] if aligned else ["temporal_misalignment"],
    }


def calculate_risk_quality(
    vix: dict[str, Any], vvix: dict[str, Any], skew: dict[str, Any], curve: dict[str, Any], put_call: dict[str, Any], alignment: dict[str, Any]
) -> dict[str, Any]:
    vix_ok = vix.get("status") == "found"
    vvix_ok = vvix.get("status") == "found"
    skew_ok = skew.get("status") == "found"
    curve_coverage = float(curve.get("coverage_pct") or 0)
    pc_coverage = min(float(put_call.get("scope_coverage_pct") or 0), 100.0)
    official_flags = [
        vix.get("is_official_source"),
        vvix.get("is_official_source"),
        skew.get("is_official_source"),
        curve.get("is_official_source"),
        any(item.get("is_official_source") for item in put_call.get("ratios") or []),
    ]
    official_coverage = sum(bool(value) for value in official_flags) / len(official_flags) * 100
    stale_count = sum(bool(item.get("stale")) for item in (vix, vvix, skew))
    freshness = max(0.0, 1 - stale_count / 3)
    history_score = min((int(vvix.get("history_depth") or 0) + int(skew.get("history_depth") or 0)) / 504, 1.0)
    alignment_score = 1.0 if alignment.get("aligned") else 0.35 if alignment.get("session_consistent") else 0.0
    score = (
        0.08 * vix_ok + 0.14 * vvix_ok + 0.14 * skew_ok + 0.22 * (curve_coverage / 100)
        + 0.18 * (pc_coverage / 100) + 0.1 * (official_coverage / 100) + 0.06 * freshness
        + 0.04 * alignment_score + 0.04 * history_score
    )
    secondary_penalty = 0.04 if any(not bool(item.get("is_official_source")) for item in put_call.get("ratios") or []) else 0.0
    partial_curve_penalty = 0.08 if 0 < curve_coverage < 50 else 0.0
    missing_scope_penalty = round(max(0.0, 100 - pc_coverage) / 100 * 0.08, 4)
    stale_penalty = round(stale_count / 3 * 0.08, 4)
    score = max(0.0, min(1.0, score - secondary_penalty - partial_curve_penalty - missing_scope_penalty - stale_penalty))
    return {
        "quality_score": round(score, 3),
        "vix_available": vix_ok,
        "vvix_available": vvix_ok,
        "skew_available": skew_ok,
        "vix_curve_coverage_pct": curve_coverage,
        "put_call_scope_coverage_pct": pc_coverage,
        "official_source_coverage_pct": round(official_coverage, 2),
        "freshness_score": round(freshness, 3),
        "temporal_alignment_score": alignment_score,
        "historical_depth_score": round(history_score, 3),
        "last_known_good_penalty": 0.0,
        "secondary_source_penalty": secondary_penalty,
        "partial_curve_penalty": partial_curve_penalty,
        "missing_scope_penalty": missing_scope_penalty,
        "stale_penalty": stale_penalty,
    }


def composite_status(
    vix: dict[str, Any], vvix: dict[str, Any], skew: dict[str, Any], curve: dict[str, Any], put_call: dict[str, Any], alignment: dict[str, Any]
) -> str:
    available = sum(item.get("status") == "found" for item in (vix, vvix, skew, curve, put_call))
    if available == 5 and alignment.get("aligned"):
        return "COMPLETE"
    if available >= 3:
        return "PARTIAL"
    if available:
        return "DEGRADED"
    return "NOT_AVAILABLE"


def build_source_summary(vix: dict[str, Any], vvix: dict[str, Any], skew: dict[str, Any], curve: dict[str, Any], put_call: dict[str, Any]) -> dict[str, Any]:
    return {
        "selected_sources": {
            "vix": _source(vix),
            "vvix": _source(vvix),
            "skew": _source(skew),
            "vix_futures": _source(curve),
            "put_call": sorted({_source(item)["source"] for item in put_call.get("ratios") or [] if _source(item)["source"]}),
        },
        "ranking": {
            "risk_indices": ["official_cboe_current", "official_cboe_historical", "verified_institutional_provider", "last_known_good_official", "secondary_provider", "not_found"],
            "vix_futures": ["official_cfe_cboe", "verified_futures_market_provider", "last_known_good_official", "secondary_provider", "not_found"],
            "put_call": ["official_cboe_statistics", "verified_exchange_derived_provider", "last_known_good_official", "secondary_provider", "not_found"],
        },
        "alternative_sources": [],
        "last_known_good_used": False,
    }


def build_diagnostics(**values: Any) -> dict[str, Any]:
    risk_indices = values["risk_indices"]
    vix_futures = values["vix_futures"]
    cboe_put_call = values["cboe_put_call"]
    qqq_options = values["qqq_options"]
    curve = values["curve"]
    put_call = values["put_call"]
    successful = sum(payload.get("status") in {"found", "partial", "valid"} for payload in (risk_indices, vix_futures, cboe_put_call, qqq_options))
    provider_calls = sum(int(payload.get("provider_calls") or 0) for payload in (risk_indices, qqq_options)) + 2
    return {
        "source_attempt_count": 4,
        "source_success_count": successful,
        "source_failure_count": 4 - successful,
        "official_source_success_count": sum(payload.get("status") in {"found", "partial"} for payload in (risk_indices, vix_futures, cboe_put_call)),
        "secondary_source_success_count": int(bool(qqq_options.get("contracts"))),
        "last_known_good_used": False,
        "vvix_found": values["vvix"].get("status") == "found",
        "skew_found": values["skew"].get("status") == "found",
        "futures_contract_count": len(vix_futures.get("contracts") or []),
        "valid_futures_contract_count": len(curve.get("contracts") or []),
        "expired_contract_count": (curve.get("diagnostics") or {}).get("expired_contract_count", 0),
        "put_call_ratio_count": len(cboe_put_call.get("ratios") or []) + len(qqq_put_call_ratios(qqq_options, now=datetime.now(UTC))),
        "valid_put_call_ratio_count": len(put_call.get("ratios") or []),
        "invalid_put_call_ratio_count": int((cboe_put_call.get("diagnostics") or {}).get("rejected_ratio_count") or 0),
        "history_snapshot_count": values["history_count"],
        "temporal_alignment_gap_minutes": values["alignment"].get("max_timestamp_gap_minutes"),
        "provider_calls": provider_calls,
        "actual_network_calls": sum(int((payload.get("diagnostics") or {}).get("actual_network_calls") or 0) for payload in (risk_indices, vix_futures, cboe_put_call, qqq_options)),
        "AI_called": False,
        "browser_calls": 0,
        "cache_used": False,
        "error_breakdown": {
            "access_restricted": int((vix_futures.get("diagnostics") or {}).get("delayed_feed_status") == "access_restricted"),
            "rate_limited": 0,
            "schema_changed": 0,
            "empty_payload": int(not put_call.get("ratios")),
            "invalid_value": 0,
            "invalid_timestamp": 0,
            "stale_source": sum(bool(item.get("stale")) for item in (values["vvix"], values["skew"])),
            "missing_contract": int(not curve.get("contracts")),
            "expired_contract": (curve.get("diagnostics") or {}).get("expired_contract_count", 0),
            "duplicate_contract": (curve.get("diagnostics") or {}).get("duplicate_contract_count", 0),
            "invalid_expiration_order": 0,
            "partial_curve": int(curve.get("status") == "partial"),
            "zero_call_denominator": int((cboe_put_call.get("diagnostics") or {}).get("rejected_ratio_count") or 0),
            "scope_mismatch": 0,
            "basis_mismatch": 0,
            "history_insufficient": int(values["history_count"] == 0),
            "temporal_misalignment": int(not values["alignment"].get("aligned")),
            "provider_unavailable": 4 - successful,
        },
    }


def build_legacy_risk_sentiment(canonical: dict[str, Any], existing: dict[str, Any] | None = None) -> dict[str, Any]:
    curve = canonical.get("vix_term_structure") or {}
    ratios = (canonical.get("put_call") or {}).get("by_id") or {}
    total = ratios.get("total_volume_put_call") or {}
    output = dict(existing or {})
    output.update(
        {
            "status": canonical.get("status"),
            "vix": canonical.get("vix") or {},
            "vix_term_structure": {
                "front_month": (curve.get("front_month") or {}).get("last_price"),
                "second_month": (curve.get("second_month") or {}).get("last_price"),
                "spread": curve.get("m1_m2_spread_points"),
                "spread_pct": curve.get("m1_m2_spread_pct"),
                "structure": curve.get("structure") or "UNKNOWN",
                "source": curve.get("source"),
                "source_url": curve.get("source_url"),
                "warnings": curve.get("warnings") or [],
                "deprecated": "Use risk_context.vix_term_structure.",
            },
            "put_call_ratio": {
                "value": total.get("ratio"),
                "data_as_of": total.get("data_as_of"),
                "source": total.get("source"),
                "source_url": total.get("source_url"),
                "warnings": total.get("warnings") or [],
                "deprecated": "Use risk_context.put_call.by_id; scopes and bases must remain separate.",
            },
        }
    )
    return output


def compact_history(snapshots: list[dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    output = []
    for item in snapshots[:limit]:
        output.append(
            {
                "retrieved_at": item.get("retrieved_at"),
                "vix": (item.get("vix") or {}).get("value"),
                "vvix": (item.get("vvix") or {}).get("value"),
                "skew": (item.get("skew") or {}).get("value"),
                "curve_regime": (item.get("vix_term_structure") or {}).get("structure"),
                "quality_score": (item.get("quality") or {}).get("quality_score"),
            }
        )
    return output


def market_session_status(now: datetime) -> str:
    if now.weekday() >= 5:
        return "weekend"
    eastern_hour = (now.hour - 4) % 24
    return "market_open" if 9 <= eastern_hour < 16 else "market_closed"


def _relative_context(metric: str, item: dict[str, Any]) -> dict[str, Any]:
    return {"source_metric": metric, "relative_regime": item.get("relative_regime"), "percentile": item.get("percentile_1y"), "z_score": item.get("z_score_1y")}


def _ratio_regime(put_call: dict[str, Any], ratio_id: str) -> str:
    return (((put_call.get("by_id") or {}).get(ratio_id)) or {}).get("relative_regime") or "UNKNOWN"


def _source(item: dict[str, Any]) -> dict[str, Any]:
    return {"source": item.get("source"), "source_url": item.get("source_url"), "is_official_source": bool(item.get("is_official_source")), "provider_type": item.get("provider_type")}


def _percentile(value: float, history: list[float]) -> float | None:
    return round(sum(item <= value for item in history) / len(history) * 100, 4) if history else None


def _zscore(value: float, history: list[float]) -> float | None:
    if not history:
        return None
    deviation = pstdev(history)
    return round((value - fmean(history)) / deviation, 6) if deviation else 0.0


def _positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0 else None


def _metric_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if len(text) == 10:
        try:
            return datetime.combine(date.fromisoformat(text), time(21, 0), tzinfo=UTC)
        except ValueError:
            pass
    parsed = parse_datetime(value)
    if parsed:
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    try:
        parsed_date = date.fromisoformat(str(value or "")[:10])
    except ValueError:
        return None
    return datetime.combine(parsed_date, time(21, 0), tzinfo=UTC)


def _qqq_data_as_of(value: Any, fallback: str) -> str:
    text = str(value or "").strip()
    match = re.search(r"AS OF\s+([A-Z]{3}\s+\d{1,2},\s+\d{4})", text, flags=re.I)
    if match:
        try:
            return datetime.strptime(match.group(1).title(), "%b %d, %Y").date().isoformat()
        except ValueError:
            pass
    parsed = _metric_datetime(value)
    return parsed.date().isoformat() if parsed else fallback


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")
