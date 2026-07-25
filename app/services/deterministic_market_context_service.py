from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, Iterable, Mapping


MEGA_CAP_SYMBOLS = {
    "AAPL",
    "AMZN",
    "AVGO",
    "GOOG",
    "GOOGL",
    "META",
    "MSFT",
    "NVDA",
    "TSLA",
}
NUMERIC_FIELD_TOKENS = {
    "actual",
    "ask",
    "bid",
    "consensus",
    "date",
    "delta",
    "eps",
    "estimate",
    "expiration",
    "gamma",
    "greek",
    "iv",
    "open_interest",
    "price",
    "revenue",
    "rho",
    "strike",
    "theta",
    "timestamp",
    "vega",
    "volume",
    "yield",
}
AUTHORIZED_QUALITATIVE_AI_FIELDS = {
    "guidance_interpretation",
    "materiality_context",
    "news_qualitative_summary",
    "official_guidance_summary",
    "residual_risk_interpretation",
}


def compute_options_positioning(
    *,
    quote: Mapping[str, Any],
    chains: Mapping[str, Iterable[Mapping[str, Any]]],
    retrieved_at: datetime | None = None,
) -> dict[str, Any]:
    retrieved = _aware(retrieved_at or datetime.now(UTC))
    spot = _finite(quote.get("last"))
    expirations: dict[str, dict[str, Any]] = {}
    all_contracts: list[dict[str, Any]] = []
    warnings: list[str] = []
    for expiration, raw_contracts in chains.items():
        contracts = [dict(item) for item in raw_contracts]
        all_contracts.extend(contracts)
        expirations[str(expiration)] = _expiration_metrics(
            contracts,
            spot=spot,
        )
    call_volume = _sum(all_contracts, "volume", option_type="call")
    put_volume = _sum(all_contracts, "volume", option_type="put")
    call_oi = _sum(all_contracts, "open_interest", option_type="call")
    put_oi = _sum(all_contracts, "open_interest", option_type="put")
    volume_ratio, volume_reason = _safe_ratio(put_volume, call_volume)
    oi_ratio, oi_reason = _safe_ratio(put_oi, call_oi)
    gamma = _gamma_proxies(all_contracts, spot=spot)
    if gamma["excluded_missing_contract_size"]:
        warnings.append("gamma_proxy_excludes_missing_contract_size")
    if gamma["coverage"] == 0 and all_contracts:
        warnings.append("zero_gamma_proxy_coverage")
    greeks_coverage = _coverage(
        all_contracts,
        lambda item: all(
            _finite(item.get(field)) is not None
            for field in ("delta", "gamma", "theta", "vega", "rho")
        ),
    )
    oi_coverage = _coverage(
        all_contracts,
        lambda item: _finite(item.get("open_interest")) is not None,
    )
    if not all_contracts:
        warnings.append("empty_option_chain")
    return {
        "status": "AVAILABLE" if all_contracts else "NO_DATA",
        "underlying": "QQQ",
        "target_context": "MNQ",
        "relationship": "Nasdaq-100 liquid ETF proxy",
        "proxy_used": True,
        "provider": "TRADIER",
        "environment": quote.get("environment"),
        "observed_at": quote.get("observed_at"),
        "retrieved_at": retrieved.isoformat(),
        "freshness": quote.get("freshness_state") or "UNKNOWN",
        "trigger_class": "REFRESH_ON_TRIGGER",
        "methodology": "deterministic_option_chain_aggregation_v1",
        "contract_count": len(all_contracts),
        "selected_expirations": list(expirations),
        "volume": {
            "calls": call_volume,
            "puts": put_volume,
            "put_call_ratio": volume_ratio,
            "ratio_reason_code": volume_reason,
        },
        "open_interest": {
            "calls": call_oi,
            "puts": put_oi,
            "put_call_ratio": oi_ratio,
            "ratio_reason_code": oi_reason,
        },
        "top_strikes_by_open_interest": _top_strikes(
            all_contracts,
            "open_interest",
        ),
        "top_strikes_by_volume": _top_strikes(all_contracts, "volume"),
        "expirations": expirations,
        "iv_atm": _atm_iv(all_contracts, spot=spot),
        "skew": _deterministic_skew(all_contracts, spot=spot),
        "spread_liquidity": _spread_liquidity(all_contracts),
        "greeks_coverage": greeks_coverage,
        "open_interest_coverage": oi_coverage,
        "gamma_proxy": gamma,
        "warnings": warnings,
        "lineage": {
            "quote_request_fingerprint": quote.get("request_fingerprint"),
            "chain_request_fingerprints": sorted(
                {
                    str(item.get("request_fingerprint"))
                    for item in all_contracts
                    if item.get("request_fingerprint")
                }
            ),
        },
    }


def compute_market_internals(
    *,
    constituents: Iterable[str | Mapping[str, Any]],
    holdings: Iterable[Mapping[str, Any]],
    quotes: Iterable[Mapping[str, Any]],
    minimum_coverage: float = 0.8,
) -> dict[str, Any]:
    universe = _constituent_symbols(constituents)
    quote_by_symbol = _deduplicate_by_symbol(quotes)
    holding_by_symbol = _deduplicate_by_symbol(holdings)
    accepted: list[dict[str, Any]] = []
    missing: list[str] = []
    stale: list[str] = []
    rejected: list[str] = []
    for symbol in universe:
        quote = quote_by_symbol.get(symbol)
        if quote is None:
            missing.append(symbol)
            continue
        change = _quote_return(quote)
        if change is None:
            rejected.append(symbol)
            continue
        if str(quote.get("freshness_state") or "").upper() in {
            "STALE",
            "REJECTED_FUTURE",
        }:
            stale.append(symbol)
        weight = _finite(
            (holding_by_symbol.get(symbol) or {}).get("weight_pct")
            or (holding_by_symbol.get(symbol) or {}).get("weight")
        )
        volume = _finite(quote.get("volume"))
        accepted.append(
            {
                "symbol": symbol,
                "return_pct": change,
                "weight_pct": weight,
                "volume": volume,
                "contribution": (
                    change * weight / 100
                    if weight is not None
                    else None
                ),
            }
        )
    advancers = sum(1 for item in accepted if item["return_pct"] > 0)
    decliners = sum(1 for item in accepted if item["return_pct"] < 0)
    unchanged = len(accepted) - advancers - decliners
    ad_ratio, ad_reason = _safe_ratio(advancers, decliners)
    covered_weights = [
        item for item in accepted if item["weight_pct"] is not None
    ]
    weight_total = sum(float(item["weight_pct"]) for item in covered_weights)
    weighted_breadth = (
        sum(
            math.copysign(float(item["weight_pct"]), item["return_pct"])
            if item["return_pct"]
            else 0
            for item in covered_weights
        )
        / weight_total
        if weight_total
        else None
    )
    up_volume = sum(
        float(item["volume"] or 0)
        for item in accepted
        if item["return_pct"] > 0
    )
    down_volume = sum(
        float(item["volume"] or 0)
        for item in accepted
        if item["return_pct"] < 0
    )
    coverage = len(accepted) / len(universe) if universe else 0.0
    returns = [float(item["return_pct"]) for item in accepted]
    contributions = [
        item for item in accepted if item["contribution"] is not None
    ]
    mega_caps = [item for item in accepted if item["symbol"] in MEGA_CAP_SYMBOLS]
    mega_weight = sum(float(item["weight_pct"] or 0) for item in mega_caps)
    warnings = []
    if coverage < minimum_coverage:
        warnings.append("constituent_coverage_insufficient")
    if set(holding_by_symbol).difference(universe):
        warnings.append("holdings_universe_mismatch")
    return {
        "status": "AVAILABLE" if accepted else "NO_DATA",
        "label": "Nasdaq-100 constituent breadth proxy",
        "source_universe": "Nasdaq-100 constituents with QQQ weights",
        "provider": "TRADIER",
        "trigger_class": "REFRESH_ON_TRIGGER",
        "methodology": "constituent_quote_breadth_proxy_v1",
        "coverage": round(coverage, 6),
        "covered_constituents": len(accepted),
        "universe_size": len(universe),
        "advancers": advancers,
        "decliners": decliners,
        "unchanged": unchanged,
        "advance_decline_ratio": ad_ratio,
        "advance_decline_reason_code": ad_reason,
        "percent_advancers": (
            round(advancers / len(accepted) * 100, 6) if accepted else None
        ),
        "weighted_breadth": weighted_breadth,
        "up_volume_proxy": up_volume,
        "down_volume_proxy": down_volume,
        "dispersion": (
            statistics.pstdev(returns) if len(returns) > 1 else 0.0 if returns else None
        ),
        "top_positive_contributions": sorted(
            contributions,
            key=lambda item: float(item["contribution"]),
            reverse=True,
        )[:10],
        "top_negative_contributions": sorted(
            contributions,
            key=lambda item: float(item["contribution"]),
        )[:10],
        "mega_cap_concentration": {
            "symbols_covered": [item["symbol"] for item in mega_caps],
            "weight_pct": mega_weight,
            "weighted_contribution": sum(
                float(item["contribution"] or 0) for item in mega_caps
            ),
        },
        "stale_quote_count": len(stale),
        "stale_symbols": stale,
        "rejected_quote_count": len(rejected),
        "rejected_symbols": rejected,
        "missing_constituents": missing,
        "warnings": warnings,
    }


def compute_cross_asset_context(
    *,
    quotes: Iterable[Mapping[str, Any]],
    fred_series: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    quote_by_symbol = _deduplicate_by_symbol(quotes)
    changes = {
        symbol: _quote_return(quote)
        for symbol, quote in quote_by_symbol.items()
    }
    changes = {key: value for key, value in changes.items() if value is not None}
    qqq = changes.get("QQQ")
    relative = {
        symbol: round(value - qqq, 6) if qqq is not None else None
        for symbol, value in changes.items()
    }
    equity = [changes[symbol] for symbol in ("QQQ", "SPY", "IWM", "DIA") if symbol in changes]
    hyg = changes.get("HYG")
    lqd = changes.get("LQD")
    credit = round(hyg - lqd, 6) if hyg is not None and lqd is not None else None
    risk_score_inputs = [
        _sign(sum(equity) / len(equity)) if equity else None,
        _sign(credit),
        -_sign(changes.get("UUP")),
        -_sign(changes.get("TLT")),
    ]
    usable = [item for item in risk_score_inputs if item is not None]
    risk_score = sum(usable) / len(usable) if usable else None
    if risk_score is None:
        regime = "NO_DATA"
    elif risk_score >= 0.25:
        regime = "RISK_ON_PROXY"
    elif risk_score <= -0.25:
        regime = "RISK_OFF_PROXY"
    else:
        regime = "MIXED"
    rates = {
        series_id: {
            "value": row.get("value"),
            "data_as_of": row.get("data_as_of"),
            "frequency": row.get("frequency"),
            "freshness": row.get("freshness") or row.get("freshness_state"),
        }
        for series_id, row in fred_series.items()
        if series_id in {"DGS2", "DGS10", "DGS30", "SOFR", "T10Y2Y", "T10Y3M", "NFCI"}
    }
    warnings: list[str] = []
    if qqq is not None and credit is not None and _sign(qqq) != _sign(credit):
        warnings.append("equity_credit_divergence")
    stale_rates = [
        key
        for key, value in rates.items()
        if str(value.get("freshness") or "").upper() == "STALE"
    ]
    if stale_rates:
        warnings.append("stale_rate_series")
    expected = {"QQQ", "SPY", "IWM", "DIA", "TLT", "HYG", "LQD", "GLD", "USO", "UUP"}
    return {
        "status": "AVAILABLE" if changes else "NO_DATA",
        "provider": "TRADIER+FRED",
        "trigger_class": "REFRESH_ON_TRIGGER",
        "methodology": "ETF and official-rate proxies; no ETF is represented as futures or spot",
        "quote_changes_pct": changes,
        "relative_strength_vs_qqq": relative,
        "risk_regime": regime,
        "risk_score": risk_score,
        "equity_breadth": {
            "advancing": sum(1 for value in equity if value > 0),
            "declining": sum(1 for value in equity if value < 0),
            "universe": len(equity),
        },
        "credit_risk_proxy_hyg_minus_lqd": credit,
        "duration_proxy_tlt": changes.get("TLT"),
        "usd_proxy_uup": changes.get("UUP"),
        "commodity_proxies": {
            "gold_etf_gld": changes.get("GLD"),
            "oil_etf_uso": changes.get("USO"),
        },
        "rates_context": rates,
        "coverage": round(len(changes) / len(expected), 6),
        "missing_proxies": sorted(expected.difference(changes)),
        "stale_rate_series": stale_rates,
        "warnings": warnings,
    }


class ProviderFirstResolutionPlanner:
    def plan(
        self,
        *,
        requested_fields: Iterable[str],
        committed_values: Mapping[str, Any] | None = None,
        provider_values: Mapping[str, Any] | None = None,
        negative_cache_fields: Iterable[str] = (),
        agent_enabled: bool,
    ) -> dict[str, Any]:
        requested = list(dict.fromkeys(str(item) for item in requested_fields))
        committed = dict(committed_values or {})
        provider = dict(provider_values or {})
        negative = set(str(item) for item in negative_cache_fields)
        resolved_cache = [
            field for field in requested if _present(committed.get(field))
        ]
        resolved_provider = [
            field
            for field in requested
            if field not in resolved_cache and _present(provider.get(field))
        ]
        remaining = [
            field
            for field in requested
            if field not in resolved_cache
            and field not in resolved_provider
            and field not in negative
        ]
        numeric_gaps = [field for field in remaining if _numeric_field(field)]
        qualitative_gaps = [
            field
            for field in remaining
            if field in AUTHORIZED_QUALITATIVE_AI_FIELDS
        ]
        ai_fields = qualitative_gaps if agent_enabled else []
        avoided = len(requested) > 0 and not ai_fields
        if not agent_enabled:
            avoided_reason = "agent_disabled"
        elif not remaining:
            avoided_reason = "provider_or_cache_coverage_complete"
        elif numeric_gaps and not qualitative_gaps:
            avoided_reason = "numeric_gaps_cannot_use_ai"
        elif all(field in negative for field in requested if field not in resolved_cache):
            avoided_reason = "negative_cache_active"
        else:
            avoided_reason = None
        return {
            "fields_requested": requested,
            "fields_resolved_by_cache": resolved_cache,
            "fields_resolved_by_provider": resolved_provider,
            "fields_negative_cached": sorted(negative.intersection(requested)),
            "fields_remaining": remaining,
            "numeric_no_data_fields": numeric_gaps,
            "qualitative_residual_fields": qualitative_gaps,
            "ai_fields": ai_fields,
            "ai_avoided": avoided,
            "ai_avoided_reason": avoided_reason,
            "estimated_avoided_invocations": 1 if avoided else 0,
            "actual_ai_invocations": 0,
            "provider_coverage": (
                round((len(resolved_cache) + len(resolved_provider)) / len(requested), 6)
                if requested
                else 1.0
            ),
            "data_outcome": (
                "COMPLETE"
                if not remaining
                else "PARTIAL"
                if resolved_cache or resolved_provider
                else "NO_DATA"
            ),
        }


def deterministic_anomalies(value: Any) -> list[str]:
    findings: list[str] = []

    def walk(item: Any, path: str) -> None:
        if isinstance(item, dict):
            bid = _finite(item.get("bid"))
            ask = _finite(item.get("ask"))
            if bid is not None and ask is not None and bid > ask:
                findings.append(f"bid_exceeds_ask:{path}")
            strike = _finite(item.get("strike"))
            if strike is not None and strike < 0:
                findings.append(f"negative_strike:{path}")
            for key, child in item.items():
                walk(child, f"{path}.{key}" if path else str(key))
        elif isinstance(item, list):
            for index, child in enumerate(item):
                walk(child, f"{path}[{index}]")
        elif isinstance(item, float) and not math.isfinite(item):
            findings.append(f"non_finite_numeric:{path}")

    walk(value, "")
    return sorted(set(findings))


def _expiration_metrics(
    contracts: list[dict[str, Any]],
    *,
    spot: float | None,
) -> dict[str, Any]:
    call_volume = _sum(contracts, "volume", option_type="call")
    put_volume = _sum(contracts, "volume", option_type="put")
    call_oi = _sum(contracts, "open_interest", option_type="call")
    put_oi = _sum(contracts, "open_interest", option_type="put")
    volume_ratio, volume_reason = _safe_ratio(put_volume, call_volume)
    oi_ratio, oi_reason = _safe_ratio(put_oi, call_oi)
    return {
        "contract_count": len(contracts),
        "call_volume": call_volume,
        "put_volume": put_volume,
        "put_call_volume_ratio": volume_ratio,
        "volume_ratio_reason_code": volume_reason,
        "call_open_interest": call_oi,
        "put_open_interest": put_oi,
        "put_call_open_interest_ratio": oi_ratio,
        "oi_ratio_reason_code": oi_reason,
        "iv_atm": _atm_iv(contracts, spot=spot),
        "gamma_proxy": _gamma_proxies(contracts, spot=spot),
    }


def _gamma_proxies(
    contracts: Iterable[Mapping[str, Any]],
    *,
    spot: float | None,
) -> dict[str, Any]:
    calls = 0.0
    puts = 0.0
    eligible = 0
    total = 0
    excluded_contract_size = 0
    for item in contracts:
        total += 1
        gamma = _finite(item.get("gamma"))
        oi = _finite(item.get("open_interest"))
        contract_size = _finite(item.get("contract_size"))
        if contract_size is None:
            excluded_contract_size += 1
            continue
        if gamma is None or oi is None or spot is None:
            continue
        value = gamma * oi * contract_size * spot**2 * 0.01
        eligible += 1
        if str(item.get("option_type")).lower() == "call":
            calls += value
        elif str(item.get("option_type")).lower() == "put":
            puts += value
    return {
        "call_gamma_exposure_proxy": calls if eligible else None,
        "put_gamma_exposure_proxy_unsigned": puts if eligible else None,
        "convention_signed_net_gamma_proxy": calls - puts if eligible else None,
        "formula": "gamma × open_interest × contract_size × spot² × 0.01",
        "signed_convention": "calls_positive_puts_negative",
        "sign_assumption": "conventional",
        "is_model_derived": True,
        "dealer_positioning_confirmed": False,
        "coverage": eligible / total if total else 0.0,
        "eligible_contracts": eligible,
        "total_contracts": total,
        "excluded_missing_contract_size": excluded_contract_size,
    }


def _atm_iv(
    contracts: Iterable[Mapping[str, Any]],
    *,
    spot: float | None,
) -> float | None:
    if spot is None:
        return None
    eligible = [
        item
        for item in contracts
        if _finite(item.get("strike")) is not None
        and _finite(item.get("iv")) is not None
    ]
    if not eligible:
        return None
    distance = min(abs(float(item["strike"]) - spot) for item in eligible)
    values = [
        float(item["iv"])
        for item in eligible
        if abs(float(item["strike"]) - spot) == distance
    ]
    return sum(values) / len(values)


def _deterministic_skew(
    contracts: Iterable[Mapping[str, Any]],
    *,
    spot: float | None,
) -> dict[str, Any]:
    if spot is None:
        return {"value": None, "reason_code": "spot_missing"}
    calls = [
        _finite(item.get("iv"))
        for item in contracts
        if str(item.get("option_type")).lower() == "call"
        and _finite(item.get("strike")) is not None
        and 0.9 * spot <= float(item["strike"]) <= 1.1 * spot
    ]
    puts = [
        _finite(item.get("iv"))
        for item in contracts
        if str(item.get("option_type")).lower() == "put"
        and _finite(item.get("strike")) is not None
        and 0.9 * spot <= float(item["strike"]) <= 1.1 * spot
    ]
    calls = [value for value in calls if value is not None]
    puts = [value for value in puts if value is not None]
    if not calls or not puts:
        return {"value": None, "reason_code": "insufficient_iv_coverage"}
    return {
        "value": sum(puts) / len(puts) - sum(calls) / len(calls),
        "convention": "mean_put_iv_minus_mean_call_iv_within_10pct_spot",
        "reason_code": None,
    }


def _spread_liquidity(contracts: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    spreads: list[float] = []
    crossed = 0
    for item in contracts:
        bid = _finite(item.get("bid"))
        ask = _finite(item.get("ask"))
        if bid is None or ask is None:
            continue
        if bid > ask:
            crossed += 1
            continue
        spreads.append(ask - bid)
    return {
        "quoted_contract_count": len(spreads),
        "mean_absolute_spread": sum(spreads) / len(spreads) if spreads else None,
        "median_absolute_spread": statistics.median(spreads) if spreads else None,
        "crossed_market_count": crossed,
    }


def _top_strikes(
    contracts: Iterable[Mapping[str, Any]],
    field: str,
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    totals: dict[tuple[str, float], float] = defaultdict(float)
    for item in contracts:
        strike = _finite(item.get("strike"))
        value = _finite(item.get(field))
        if strike is None or value is None:
            continue
        totals[(str(item.get("option_type") or "").lower(), strike)] += value
    return [
        {"option_type": key[0], "strike": key[1], field: value}
        for key, value in sorted(
            totals.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:limit]
    ]


def _sum(
    contracts: Iterable[Mapping[str, Any]],
    field: str,
    *,
    option_type: str,
) -> float:
    return sum(
        float(value)
        for item in contracts
        if str(item.get("option_type") or "").lower() == option_type
        and (value := _finite(item.get(field))) is not None
    )


def _safe_ratio(
    numerator: int | float,
    denominator: int | float,
) -> tuple[float | None, str | None]:
    if denominator == 0:
        return None, "DENOMINATOR_ZERO"
    return float(numerator) / float(denominator), None


def _coverage(
    values: Iterable[Mapping[str, Any]],
    predicate,
) -> float:
    rows = list(values)
    return sum(1 for item in rows if predicate(item)) / len(rows) if rows else 0.0


def _constituent_symbols(
    values: Iterable[str | Mapping[str, Any]],
) -> list[str]:
    output: list[str] = []
    for item in values:
        raw = item if isinstance(item, str) else item.get("symbol")
        symbol = str(raw or "").strip().upper()
        if symbol and symbol not in output:
            output.append(symbol)
    return output


def _deduplicate_by_symbol(
    values: Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for item in values:
        symbol = str(item.get("symbol") or "").strip().upper()
        if symbol and symbol not in output:
            output[symbol] = dict(item)
    return output


def _quote_return(quote: Mapping[str, Any]) -> float | None:
    direct = _finite(quote.get("change_percentage"))
    if direct is not None:
        return direct
    last = _finite(quote.get("last"))
    close = _finite(quote.get("close") or quote.get("prevclose"))
    if last is None or close in (None, 0):
        return None
    return (last - close) / abs(close) * 100


def _finite(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _sign(value: float | None) -> int | None:
    if value is None:
        return None
    return 1 if value > 0 else -1 if value < 0 else 0


def _numeric_field(field: str) -> bool:
    normalized = str(field).lower()
    return any(token in normalized for token in NUMERIC_FIELD_TOKENS)


def _present(value: Any) -> bool:
    return value is not None and value != "" and value != []


def _aware(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
