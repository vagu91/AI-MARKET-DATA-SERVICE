from __future__ import annotations

import logging
import re
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

from app.services.qqq_weight_intelligence_service import parse_market_value, parse_weight_value


logger = logging.getLogger(__name__)

MULTI_CLASS_MAP_VERSION = "nasdaq_multiclass_issuer_map_v1"
KNOWN_MULTI_CLASS_ISSUERS = {
    "alphabet": {
        "issuer_id": "CIK0001652044",
        "issuer_name": "Alphabet Inc.",
        "cik": "0001652044",
        "symbols": {"GOOGL": "A", "GOOG": "C"},
        "unlisted_classes": ["B"],
    }
}

SEMANTICS_SECURITY_VERIFIED = "security_level_verified"
SEMANTICS_ISSUER_DUPLICATED = "issuer_level_duplicated"
SEMANTICS_ISSUER_PROBABLE = "issuer_level_probable"
SEMANTICS_ISSUER_VERIFIED = "issuer_level_verified"
SEMANTICS_UNKNOWN = "unknown"


def detect_multi_class_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_symbol = {str(row.get("symbol") or "").upper(): row for row in rows}
    groups: list[dict[str, Any]] = []
    assigned: set[str] = set()
    for group_id, config in KNOWN_MULTI_CLASS_ISSUERS.items():
        symbols = [symbol for symbol in config["symbols"] if symbol in by_symbol]
        if len(symbols) < 2:
            continue
        expected_name = normalize_issuer_name(config["issuer_name"]).lower()
        if not all(
            normalize_issuer_name(
                by_symbol[symbol].get("companyName")
                or by_symbol[symbol].get("company_name")
                or by_symbol[symbol].get("name")
            ).lower()
            == expected_name
            for symbol in symbols
        ):
            continue
        assigned.update(symbols)
        groups.append(
            {
                **config,
                "issuer_group": group_id,
                "multi_class_group": group_id,
                "symbols": symbols,
                "class_by_symbol": {symbol: config["symbols"][symbol] for symbol in symbols},
                "detection_method": MULTI_CLASS_MAP_VERSION,
            }
        )
        log_multi_class_event(
            "multi_class_group_detected",
            issuer=config["issuer_name"],
            symbols=symbols,
            source=MULTI_CLASS_MAP_VERSION,
        )

    cautious: dict[str, list[str]] = {}
    for symbol, row in by_symbol.items():
        if symbol in assigned:
            continue
        name = normalize_issuer_name(row.get("companyName") or row.get("company_name") or row.get("name"))
        if name:
            cautious.setdefault(name, []).append(symbol)
    for name, symbols in cautious.items():
        if len(symbols) < 2:
            continue
        rows_for_group = [by_symbol[symbol] for symbol in symbols]
        if not all(extract_share_class(row) for row in rows_for_group):
            continue
        groups.append(
            {
                "issuer_id": None,
                "issuer_name": name,
                "cik": None,
                "issuer_group": f"name:{name.lower().replace(' ', '-')}",
                "multi_class_group": f"name:{name.lower().replace(' ', '-')}",
                "symbols": sorted(symbols),
                "class_by_symbol": {symbol: extract_share_class(by_symbol[symbol]) for symbol in symbols},
                "unlisted_classes": [],
                "detection_method": "normalized_company_name_with_explicit_share_classes",
            }
        )
    return groups


def classify_group_semantics(
    group: dict[str, Any],
    rows_by_symbol: dict[str, dict[str, Any]],
    shares_snapshot: dict[str, Any] | None,
    *,
    near_equal_tolerance_pct: float = 2.0,
) -> dict[str, Any]:
    symbols = list(group.get("symbols") or [])
    raw_caps = [parse_market_value(rows_by_symbol[symbol].get("marketCap") or rows_by_symbol[symbol].get("market_cap_raw") or rows_by_symbol[symbol].get("market_cap")) for symbol in symbols]
    prices = [parse_market_value(rows_by_symbol[symbol].get("lastSalePrice") or rows_by_symbol[symbol].get("last_price") or rows_by_symbol[symbol].get("price")) for symbol in symbols]
    implied = [cap / price if cap and price else None for cap, price in zip(raw_caps, prices, strict=True)]
    valid_caps = [cap for cap in raw_caps if cap and cap > 0]
    near_equal = False
    if len(valid_caps) == len(symbols) and valid_caps:
        near_equal = (max(valid_caps) - min(valid_caps)) / max(valid_caps) * 100.0 <= near_equal_tolerance_pct
    class_shares = (shares_snapshot or {}).get("class_shares") or {}
    listed_shares = (shares_snapshot or {}).get("listed_shares") or {}
    total_shares = sum(float(value) for value in class_shares.values()) if class_shares else None
    security_level_matches = bool(
        listed_shares
        and all(
            cap
            and price
            and parse_market_value(listed_shares.get(symbol))
            and abs(cap - price * float(listed_shares[symbol])) / cap <= 0.03
            for symbol, cap, price in zip(symbols, raw_caps, prices, strict=True)
        )
    )
    implied_matches_total = bool(
        total_shares
        and implied
        and all(value and abs(value - total_shares) / total_shares <= 0.03 for value in implied)
    )
    if security_level_matches:
        classification = SEMANTICS_SECURITY_VERIFIED
        confidence = 0.99
        reason = "provider_caps_match_price_times_verified_class_shares"
    elif near_equal and implied_matches_total:
        classification = SEMANTICS_ISSUER_DUPLICATED
        confidence = 0.99
        reason = "near_equal_caps_and_each_implied_share_count_matches_total_issuer_shares"
    elif near_equal:
        classification = SEMANTICS_ISSUER_PROBABLE
        confidence = 0.82
        reason = "near_equal_market_caps_across_listed_classes"
    elif all(value and value > 0 for value in valid_caps) and len(valid_caps) == len(symbols):
        classification = SEMANTICS_UNKNOWN
        confidence = 0.45
        reason = "distinct_caps_without_security_level_share_verification"
    else:
        classification = SEMANTICS_UNKNOWN
        confidence = 0.2
        reason = "insufficient_market_cap_or_price_data"
    result = {
        "classification": classification,
        "confidence": confidence,
        "reason": reason,
        "raw_market_caps": raw_caps,
        "prices": prices,
        "implied_shares": implied,
        "aggregate_reported_shares": total_shares,
        "market_cap_equal_or_near_equal": near_equal,
        "possible_company_cap_duplication": classification in {SEMANTICS_ISSUER_DUPLICATED, SEMANTICS_ISSUER_PROBABLE},
    }
    log_multi_class_event(
        "market_cap_semantics_classified",
        issuer=group.get("issuer_name"),
        symbols=symbols,
        raw_market_caps=raw_caps,
        prices=prices,
        classification=classification,
        confidence=confidence,
        reason=reason,
    )
    if classification == SEMANTICS_ISSUER_DUPLICATED:
        log_multi_class_event(
            "issuer_level_market_cap_duplicate_detected",
            issuer=group.get("issuer_name"),
            symbols=symbols,
            raw_market_caps=raw_caps,
            prices=prices,
            confidence=confidence,
            reason=reason,
        )
    return result


def apply_multi_class_adjustments(
    rows: list[dict[str, Any]],
    shares_by_issuer: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    adjusted = deepcopy(rows)
    by_symbol = {str(row.get("symbol") or "").upper(): row for row in adjusted}
    groups = detect_multi_class_groups(adjusted)
    diagnostics: list[dict[str, Any]] = []
    adjustment_count = 0
    duplicate_count = 0
    unresolved_count = 0
    adjusted_symbols: set[str] = set()

    for group in groups:
        issuer_key = str(group.get("issuer_group") or "")
        shares_snapshot = shares_by_issuer.get(issuer_key) or {}
        semantics = classify_group_semantics(group, by_symbol, shares_snapshot)
        if semantics["classification"] == SEMANTICS_ISSUER_DUPLICATED:
            duplicate_count += 1
        group_adjusted = True
        listed_shares = shares_snapshot.get("listed_shares") or {}
        for symbol in group["symbols"]:
            row = by_symbol[symbol]
            raw_cap_value = row.get("marketCap") or row.get("market_cap_raw") or row.get("market_cap")
            raw_cap = parse_market_value(raw_cap_value)
            price = parse_market_value(row.get("lastSalePrice") or row.get("last_price") or row.get("price"))
            class_code = group["class_by_symbol"].get(symbol)
            class_shares = parse_market_value(listed_shares.get(symbol))
            security_cap = price * class_shares if price and class_shares else None
            verified = bool(shares_snapshot.get("verified") and security_cap and class_code)
            row.update(
                {
                    "issuer_id": group.get("issuer_id"),
                    "issuer_identifier": group.get("issuer_id"),
                    "issuer_name": group.get("issuer_name"),
                    "issuer_group": issuer_key,
                    "multi_class_group": issuer_key,
                    "cik": group.get("cik"),
                    "share_class": class_code,
                    "market_cap_raw": raw_cap_value,
                    "market_cap_parsed": raw_cap,
                    "implied_shares": raw_cap / price if raw_cap and price else None,
                    "market_cap_raw_semantics": semantics["classification"],
                    "market_cap_semantics": SEMANTICS_SECURITY_VERIFIED if verified else semantics["classification"],
                    "market_cap_source": shares_snapshot.get("source") if verified else "Nasdaq constituent snapshot",
                    "market_cap_source_url": shares_snapshot.get("source_url") if verified else None,
                    "market_cap_verified": verified,
                    "market_cap_is_issuer_level": not verified and semantics["classification"] in {SEMANTICS_ISSUER_DUPLICATED, SEMANTICS_ISSUER_PROBABLE, SEMANTICS_ISSUER_VERIFIED},
                    "market_cap_is_security_level": verified,
                    "security_market_cap": security_cap,
                    "market_cap": security_cap if verified else None,
                    "multi_class_adjustment_applied": verified,
                    "multi_class_adjustment_method": "price_times_verified_class_shares" if verified else None,
                    "multi_class_confidence": 0.99 if verified else semantics["confidence"],
                    "class_shares": class_shares,
                    "class_shares_source": shares_snapshot.get("source"),
                    "class_shares_source_url": shares_snapshot.get("source_url"),
                    "class_shares_as_of": shares_snapshot.get("shares_as_of"),
                    "class_shares_retrieved_at": shares_snapshot.get("retrieved_at"),
                    "class_shares_valid_until": shares_snapshot.get("valid_until"),
                    "class_shares_verified": bool(verified),
                }
            )
            if verified:
                adjusted_symbols.add(symbol)
                adjustment_count += 1
                log_multi_class_event(
                    "security_level_market_cap_verified",
                    issuer=group.get("issuer_name"),
                    symbols=[symbol],
                    prices=[price],
                    class_shares=[class_shares],
                    classification=SEMANTICS_SECURITY_VERIFIED,
                    confidence=0.99,
                    adjustment_method="price_times_verified_class_shares",
                    source=shares_snapshot.get("source"),
                )
            else:
                group_adjusted = False
                log_multi_class_event(
                    "multi_class_adjustment_skipped",
                    issuer=group.get("issuer_name"),
                    symbols=[symbol],
                    classification=semantics["classification"],
                    confidence=semantics["confidence"],
                    reason="verified_class_shares_unavailable",
                )
        if not group_adjusted:
            unresolved_count += 1
        else:
            log_multi_class_event(
                "multi_class_adjustment_applied",
                issuer=group.get("issuer_name"),
                symbols=group["symbols"],
                class_shares=[listed_shares.get(symbol) for symbol in group["symbols"]],
                classification=SEMANTICS_SECURITY_VERIFIED,
                confidence=0.99,
                adjustment_method="price_times_verified_class_shares",
                source=shares_snapshot.get("source"),
            )
        diagnostics.append(
            {
                "issuer_id": group.get("issuer_id"),
                "issuer_name": group.get("issuer_name"),
                "issuer_group": issuer_key,
                "symbols": group["symbols"],
                "share_classes": group["class_by_symbol"],
                "unlisted_classes": shares_snapshot.get("unlisted_classes") or group.get("unlisted_classes") or [],
                "raw_market_caps": semantics["raw_market_caps"],
                "prices": semantics["prices"],
                "implied_shares": semantics["implied_shares"],
                "aggregate_reported_shares": semantics["aggregate_reported_shares"],
                "market_cap_equal_or_near_equal": semantics["market_cap_equal_or_near_equal"],
                "possible_company_cap_duplication": semantics["possible_company_cap_duplication"],
                "raw_market_cap_semantics": semantics["classification"],
                "final_market_cap_semantics": SEMANTICS_SECURITY_VERIFIED if group_adjusted else semantics["classification"],
                "class_shares_verified": bool(shares_snapshot.get("verified")),
                "adjustment_applied": group_adjusted,
                "adjustment_method": "price_times_verified_class_shares" if group_adjusted else None,
                "confidence": 0.99 if group_adjusted else semantics["confidence"],
                "source": shares_snapshot.get("source"),
                "source_url": shares_snapshot.get("source_url"),
                "reason": semantics["reason"],
            }
        )

    for symbol, row in by_symbol.items():
        if symbol in adjusted_symbols or row.get("multi_class_group"):
            continue
        raw_cap_value = row.get("marketCap") or row.get("market_cap_raw") or row.get("market_cap")
        parsed = parse_market_value(raw_cap_value)
        row.update(
            {
                "market_cap_raw": raw_cap_value,
                "market_cap_parsed": parsed,
                "security_market_cap": parsed,
                "market_cap": parsed,
                "market_cap_semantics": SEMANTICS_ISSUER_PROBABLE,
                "market_cap_source": "Nasdaq constituent snapshot",
                "market_cap_verified": False,
                "market_cap_is_issuer_level": True,
                "market_cap_is_security_level": False,
                "multi_class_confidence": 0.72,
            }
        )

    total_security_count = sum(len(group.get("symbols") or []) for group in groups)
    quality = {
        "multi_class_issuer_count": len(groups),
        "multi_class_security_count": total_security_count,
        "verified_security_cap_count": len(adjusted_symbols),
        "issuer_level_duplicate_count": duplicate_count,
        "issuer_level_probable_count": sum(1 for row in adjusted if row.get("market_cap_semantics") == SEMANTICS_ISSUER_PROBABLE),
        "unknown_market_cap_semantics_count": sum(1 for row in adjusted if row.get("market_cap_semantics") == SEMANTICS_UNKNOWN),
        "multi_class_adjustment_count": adjustment_count,
        "multi_class_weight_coverage_pct": round(len(adjusted_symbols) / max(total_security_count, 1) * 100.0, 4),
        "issuer_semantics_quality_score": round(
            (len(adjusted_symbols) / max(total_security_count, 1)) if groups else 1.0,
            3,
        ),
        "multi_class_unresolved_count": unresolved_count,
        "multi_class_diagnostics": diagnostics,
    }
    log_multi_class_event(
        "multi_class_weight_set_recalculated",
        symbols=sorted(adjusted_symbols),
        confidence=quality["issuer_semantics_quality_score"],
        adjustment_method="price_times_verified_class_shares" if adjustment_count else None,
    )
    return adjusted, quality


def attach_issuer_aggregate_weights(holdings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    totals: dict[str, float] = {}
    for item in holdings:
        group = str(item.get("issuer_group") or "")
        weight = parse_weight_value(item.get("weight_pct") if item.get("weight_pct") is not None else item.get("weight"))
        if group and weight is not None:
            totals[group] = totals.get(group, 0.0) + weight
    for item in holdings:
        group = str(item.get("issuer_group") or "")
        item["issuer_aggregate_weight_pct"] = round(totals[group], 8) if group in totals else None
    return holdings


def normalize_issuer_name(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"\bClass\s+[A-Z]\b", "", text, flags=re.I)
    text = re.sub(r"\b(Common|Capital)\s+Stock\b", "", text, flags=re.I)
    text = re.sub(r"[^A-Za-z0-9]+", " ", text).strip()
    return re.sub(r"\s+", " ", text)


def extract_share_class(row: dict[str, Any]) -> str | None:
    existing = str(row.get("share_class") or "").upper()
    if existing:
        return existing
    name = str(row.get("companyName") or row.get("company_name") or row.get("name") or "")
    match = re.search(r"\bClass\s+([A-Z])\b", name, flags=re.I)
    return match.group(1).upper() if match else None


def log_multi_class_event(event: str, **fields: Any) -> None:
    logger.info(event, extra={key: value for key, value in fields.items() if value is not None})
