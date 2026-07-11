from __future__ import annotations

import csv
import io
import json
import logging
import math
import re
import zipfile
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable
from xml.etree import ElementTree


logger = logging.getLogger(__name__)

OFFICIAL_QQQ_WEIGHT = "official_qqq_weight"
OFFICIAL_NASDAQ100_WEIGHT = "official_nasdaq100_weight"
VENDOR_QQQ_WEIGHT = "vendor_qqq_weight"
RECONSTRUCTED_MARKET_CAP_WEIGHT = "reconstructed_unadjusted_market_cap_weight"
EQUAL_WEIGHT_PROXY = "equal_weight_proxy"

WEIGHT_METHOD_RANK = {
    OFFICIAL_QQQ_WEIGHT: 700,
    OFFICIAL_NASDAQ100_WEIGHT: 650,
    VENDOR_QQQ_WEIGHT: 500,
    RECONSTRUCTED_MARKET_CAP_WEIGHT: 400,
    EQUAL_WEIGHT_PROXY: 100,
}


def parse_weight_value(value: Any) -> float | None:
    if value in (None, ""):
        return None
    cleaned = str(value).replace("%", "").replace(",", "").strip()
    try:
        parsed = float(cleaned)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def parse_market_value(value: Any) -> float | None:
    if value in (None, ""):
        return None
    cleaned = re.sub(r"[^0-9.()\-]", "", str(value).replace(",", ""))
    if not cleaned:
        return None
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    cleaned = cleaned.strip("()")
    try:
        parsed = float(cleaned)
    except ValueError:
        return None
    if negative:
        parsed = -parsed
    return parsed if math.isfinite(parsed) else None


def normalize_symbol(value: Any) -> str:
    return re.sub(r"[^A-Z0-9\-]", "", str(value or "").strip().upper().replace(".", "-"))


def share_class(symbol: str, company_name: str | None = None) -> str | None:
    value = str(company_name or "")
    match = re.search(r"\bClass\s+([A-Z])\b", value, flags=re.IGNORECASE)
    if match:
        return match.group(1).upper()
    if symbol in {"GOOG", "GOOGL"}:
        return "C" if symbol == "GOOG" else "A"
    return None


def parse_csv_holdings(text: str) -> tuple[list[dict[str, Any]], str | None, list[str]]:
    lines = [line for line in text.splitlines() if line.strip()]
    as_of = _find_as_of(lines[:12])
    header_idx = next(
        (
            idx
            for idx, line in enumerate(lines)
            if any(term in line.lower() for term in ("ticker", "symbol"))
            and "weight" in line.lower()
        ),
        -1,
    )
    if header_idx < 0:
        return [], as_of, ["schema_changed:weight_header_not_found"]
    return _rows_to_holdings(list(csv.DictReader(io.StringIO("\n".join(lines[header_idx:]))))), as_of, []


def parse_json_holdings(payload: dict[str, Any] | list[Any]) -> tuple[list[dict[str, Any]], list[str]]:
    if isinstance(payload, list):
        rows = payload
    else:
        rows = (
            payload.get("holdings")
            or payload.get("Holdings")
            or (payload.get("data") or {}).get("holdings")
            or (payload.get("data") or {}).get("rows")
            or []
        )
    if not isinstance(rows, list):
        return [], ["schema_changed:holdings_array_not_found"]
    return _rows_to_holdings([row for row in rows if isinstance(row, dict)]), []


def parse_xlsx_holdings(content: bytes) -> tuple[list[dict[str, Any]], list[str]]:
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            shared = _xlsx_shared_strings(archive)
            sheet_name = next(
                name for name in archive.namelist() if name.startswith("xl/worksheets/sheet") and name.endswith(".xml")
            )
            root = ElementTree.fromstring(archive.read(sheet_name))
    except (ValueError, KeyError, zipfile.BadZipFile, StopIteration, ElementTree.ParseError) as exc:
        return [], [f"invalid_xlsx:{type(exc).__name__}"]
    namespace = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rows: list[list[str]] = []
    for row in root.findall(".//x:row", namespace):
        values: list[str] = []
        for cell in row.findall("x:c", namespace):
            cell_type = cell.attrib.get("t")
            raw = cell.findtext("x:v", default="", namespaces=namespace)
            if cell_type == "s" and raw.isdigit() and int(raw) < len(shared):
                raw = shared[int(raw)]
            elif cell_type == "inlineStr":
                raw = "".join(cell.itertext())
            values.append(raw)
        if any(value.strip() for value in values):
            rows.append(values)
    header_idx = next(
        (idx for idx, row in enumerate(rows) if any("weight" in value.lower() for value in row)),
        -1,
    )
    if header_idx < 0:
        return [], ["schema_changed:weight_header_not_found"]
    headers = rows[header_idx]
    records = [dict(zip(headers, row, strict=False)) for row in rows[header_idx + 1 :]]
    return _rows_to_holdings(records), []


def reconstruct_market_cap_weights(
    rows: Iterable[dict[str, Any]],
    *,
    source: str,
    source_url: str,
    as_of: str | None,
    retrieved_at: datetime | None = None,
    ttl_hours: float = 12.0,
    total_tolerance_pct: float = 1.0,
    minimum_coverage_pct: float = 95.0,
    maximum_constituent_pct: float = 25.0,
) -> dict[str, Any]:
    now = retrieved_at or datetime.now(UTC)
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    duplicate_symbols: list[str] = []
    for raw in rows:
        symbol = normalize_symbol(raw.get("symbol") or raw.get("ticker"))
        if not symbol:
            continue
        if symbol in seen:
            duplicate_symbols.append(symbol)
            continue
        seen.add(symbol)
        market_cap = parse_market_value(
            raw.get("security_market_cap")
            or raw.get("market_cap")
            or raw.get("marketCap")
        )
        company_name = raw.get("company_name") or raw.get("companyName") or raw.get("name")
        item = {
                "symbol": symbol,
                "name": company_name,
                "company_name": company_name,
                "share_class": share_class(symbol, company_name),
                "sector": raw.get("sector") or None,
                "market_cap": market_cap,
                "price": parse_market_value(raw.get("price") or raw.get("last_sale_price") or raw.get("lastSalePrice")),
                "change_pct": parse_weight_value(raw.get("change_pct") or raw.get("percentage_change") or raw.get("percentageChange")),
                "price_source": source,
                "warnings": [] if market_cap and market_cap > 0 else ["market_cap_missing_or_invalid"],
        }
        for field in (
            "market_cap_raw",
            "market_cap_parsed",
            "security_market_cap",
            "implied_shares",
            "issuer_id",
            "issuer_name",
            "issuer_group",
            "issuer_identifier",
            "cik",
            "isin",
            "cusip",
            "security_type",
            "market_cap_semantics",
            "market_cap_raw_semantics",
            "market_cap_source",
            "market_cap_source_url",
            "market_cap_verified",
            "market_cap_is_issuer_level",
            "market_cap_is_security_level",
            "multi_class_group",
            "multi_class_adjustment_applied",
            "multi_class_adjustment_method",
            "multi_class_confidence",
            "class_shares",
            "class_shares_source",
            "class_shares_source_url",
            "class_shares_as_of",
            "class_shares_retrieved_at",
            "class_shares_valid_until",
            "class_shares_verified",
        ):
            if field in raw:
                item[field] = raw.get(field)
        normalized.append(item)
    usable_total = sum(float(item["market_cap"]) for item in normalized if item.get("market_cap") and item["market_cap"] > 0)
    for item in normalized:
        market_cap = item.get("market_cap")
        weight = round(float(market_cap) / usable_total * 100.0, 8) if market_cap and market_cap > 0 and usable_total else None
        item.update(
            _weight_fields(
                weight=weight,
                method=RECONSTRUCTED_MARKET_CAP_WEIGHT,
                source=source,
                source_url=source_url,
                as_of=as_of,
                retrieved_at=now,
                valid_until=now + timedelta(hours=ttl_hours),
                verified=bool(weight is not None),
                official=False,
                reconstructed=True,
                confidence=0.76 if weight is not None else 0.0,
            )
        )
    issuer_totals: dict[str, float] = {}
    for item in normalized:
        issuer_group = str(item.get("issuer_group") or "")
        if issuer_group and item.get("weight") is not None:
            issuer_totals[issuer_group] = issuer_totals.get(issuer_group, 0.0) + float(item["weight"])
    for item in normalized:
        issuer_group = str(item.get("issuer_group") or "")
        item["issuer_aggregate_weight_pct"] = (
            round(issuer_totals[issuer_group], 8) if issuer_group in issuer_totals else None
        )
    validation = validate_weight_set(
        normalized,
        expected_symbols=seen,
        total_tolerance_pct=total_tolerance_pct,
        minimum_coverage_pct=minimum_coverage_pct,
        maximum_constituent_pct=maximum_constituent_pct,
    )
    validation["duplicate_symbol_count"] += len(set(duplicate_symbols))
    return {
        "status": "found" if validation["valid"] else "partial" if normalized else "not_found",
        "as_of": as_of,
        "source": source,
        "source_url": source_url,
        "weight_method": RECONSTRUCTED_MARKET_CAP_WEIGHT,
        "weight_is_official": False,
        "weight_is_reconstructed": True,
        "weight_verified": validation["valid"],
        "weight_confidence": 0.76 if validation["valid"] else 0.5,
        "weight_valid_until": _iso(now + timedelta(hours=ttl_hours)),
        "holdings": sorted(normalized, key=lambda item: float(item.get("weight") or -1), reverse=True),
        "validation": validation,
    }


def validate_weight_set(
    holdings: list[dict[str, Any]],
    *,
    expected_symbols: set[str] | None = None,
    total_tolerance_pct: float = 1.0,
    minimum_coverage_pct: float = 95.0,
    maximum_constituent_pct: float = 25.0,
    normalize_within_tolerance: bool = True,
) -> dict[str, Any]:
    symbols = [normalize_symbol(item.get("symbol")) for item in holdings if normalize_symbol(item.get("symbol"))]
    duplicate_symbols = {symbol for symbol in symbols if symbols.count(symbol) > 1}
    weights = [parse_weight_value(item.get("weight_pct") if item.get("weight_pct") is not None else item.get("weight")) for item in holdings]
    non_null = [weight for weight in weights if weight is not None]
    total = sum(non_null)
    expected = expected_symbols or set(symbols)
    weighted_symbols = {symbols[idx] for idx, weight in enumerate(weights) if idx < len(symbols) and weight is not None}
    coverage = len(weighted_symbols & expected) / max(len(expected), 1) * 100.0
    negative = sum(1 for weight in non_null if weight < 0)
    zero = sum(1 for weight in non_null if weight == 0)
    largest = max(non_null, default=None)
    close_to_total = abs(total - 100.0) <= total_tolerance_pct
    normalization_applied = False
    if normalize_within_tolerance and close_to_total and total and abs(total - 100.0) > 1e-7:
        factor = 100.0 / total
        for item in holdings:
            value = parse_weight_value(item.get("weight_pct") if item.get("weight_pct") is not None else item.get("weight"))
            if value is not None:
                normalized = round(value * factor, 8)
                item["weight"] = normalized
                item["weight_pct"] = normalized
        total = 100.0
        normalization_applied = True
    valid = bool(
        holdings
        and not duplicate_symbols
        and negative == 0
        and coverage >= minimum_coverage_pct
        and close_to_total
        and (largest is None or largest <= maximum_constituent_pct)
    )
    missing = sorted(expected - weighted_symbols)
    foreign = sorted(set(symbols) - expected) if expected_symbols is not None else []
    top = sorted((weight for weight in non_null if weight >= 0), reverse=True)
    reasons: list[str] = []
    if duplicate_symbols:
        reasons.append("duplicate_symbols")
    if negative:
        reasons.append("negative_weight")
    if not close_to_total:
        reasons.append("invalid_total_weight")
    if coverage < minimum_coverage_pct:
        reasons.append("missing_constituents")
    if largest is not None and largest > maximum_constituent_pct:
        reasons.append("weight_outlier")
    return {
        "valid": valid,
        "partial": bool(non_null) and not valid,
        "constituent_count": len(holdings),
        "non_null_weight_count": len(non_null),
        "weighted_constituent_count": len(non_null),
        "missing_weight_count": len(holdings) - len(non_null),
        "duplicate_symbol_count": len(duplicate_symbols),
        "negative_weight_count": negative,
        "zero_weight_count": zero,
        "total_weight_pct": round(total, 8) if non_null else None,
        "top_10_weight_pct": round(sum(top[:10]), 8) if top else None,
        "largest_weight_pct": round(largest, 8) if largest is not None else None,
        "coverage_pct": round(coverage, 4),
        "missing_weight_pct": round(100.0 - coverage, 4),
        "missing_symbols": missing,
        "foreign_symbols": foreign,
        "normalization_applied": normalization_applied,
        "rejection_reasons": reasons,
    }


def select_weight_candidate(candidates: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    usable = [candidate for candidate in candidates if candidate and (candidate.get("validation") or {}).get("valid")]
    if not usable:
        return None
    return max(
        usable,
        key=lambda candidate: (
            WEIGHT_METHOD_RANK.get(str(candidate.get("weight_method")), 0),
            _timestamp(candidate.get("as_of")),
            float(candidate.get("weight_confidence") or 0.0),
        ),
    )


def concentration_metrics(holdings: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = sorted(
        (
            (str(item.get("symbol") or ""), float(weight))
            for item in holdings
            if (weight := parse_weight_value(item.get("weight_pct") if item.get("weight_pct") is not None else item.get("weight"))) is not None
            and weight >= 0
        ),
        key=lambda item: item[1],
        reverse=True,
    )
    if not rows:
        return {
            "top_1_weight_pct": None,
            "top_3_weight_pct": None,
            "top_5_weight_pct": None,
            "top_10_weight_pct": None,
            "top_20_weight_pct": None,
            "largest_constituent_symbol": None,
            "largest_constituent_weight_pct": None,
            "effective_number_of_constituents": None,
            "herfindahl_hirschman_index": None,
            "classification": "UNKNOWN",
            "weight_data_available": False,
        }
    weights = [weight for _, weight in rows]
    hhi = sum((weight / 100.0) ** 2 for weight in weights)
    top10 = sum(weights[:10])
    classification = (
        "VERY_HIGH"
        if top10 >= 60 or hhi >= 0.10
        else "HIGH"
        if top10 >= 45 or hhi >= 0.07
        else "MODERATE"
        if top10 >= 30 or hhi >= 0.04
        else "LOW"
    )
    return {
        "top_1_weight_pct": round(sum(weights[:1]), 4),
        "top_3_weight_pct": round(sum(weights[:3]), 4),
        "top_5_weight_pct": round(sum(weights[:5]), 4),
        "top_10_weight_pct": round(top10, 4),
        "top_20_weight_pct": round(sum(weights[:20]), 4),
        "largest_constituent_symbol": rows[0][0],
        "largest_constituent_weight_pct": round(rows[0][1], 4),
        "effective_number_of_constituents": round(1.0 / hhi, 4) if hhi else None,
        "herfindahl_hirschman_index": round(hhi, 6),
        "classification": classification,
        "classification_thresholds": {
            "very_high": "top10>=60 or HHI>=0.10",
            "high": "top10>=45 or HHI>=0.07",
            "moderate": "top10>=30 or HHI>=0.04",
            "low": "below moderate thresholds",
        },
        "weight_data_available": True,
    }


def weighted_contributions(
    holdings: Iterable[dict[str, Any]],
    prices: Iterable[dict[str, Any]] = (),
) -> dict[str, Any]:
    price_map = {normalize_symbol(item.get("symbol")): item for item in prices}
    rows: list[dict[str, Any]] = []
    missing_price: list[str] = []
    missing_weight: list[str] = []
    for holding in holdings:
        symbol = normalize_symbol(holding.get("symbol"))
        if not symbol:
            continue
        weight = parse_weight_value(holding.get("weight_pct") if holding.get("weight_pct") is not None else holding.get("weight"))
        price_row = price_map.get(symbol, {})
        change = parse_weight_value(price_row.get("change_pct"))
        if change is None:
            change = parse_weight_value(holding.get("change_pct"))
        if weight is None:
            missing_weight.append(symbol)
            continue
        if change is None:
            missing_price.append(symbol)
            continue
        contribution = change * weight / 100.0
        rows.append(
            {
                "symbol": symbol,
                "change_pct": round(change, 6),
                "weight": round(weight, 8),
                "weight_pct": round(weight, 8),
                "weighted_contribution": round(contribution, 8),
                "weighted_contribution_pct_points": round(contribution, 8),
                "direction": "positive" if contribution > 0 else "negative" if contribution < 0 else "neutral",
                "price_source": price_row.get("source") or holding.get("price_source"),
                "weight_source": holding.get("weight_source"),
                "weight_method": holding.get("weight_method"),
            }
        )
    rows.sort(key=lambda item: abs(float(item["weighted_contribution"])), reverse=True)
    for index, row in enumerate(rows, start=1):
        row["contribution_rank"] = index
    covered = sum(float(item["weight_pct"]) for item in rows)
    positive = sum(float(item["weighted_contribution"]) for item in rows if item["weighted_contribution"] > 0)
    negative = sum(float(item["weighted_contribution"]) for item in rows if item["weighted_contribution"] < 0)
    net = positive + negative
    return {
        "contributors": rows,
        "weighted_positive_contribution": round(positive, 8),
        "weighted_negative_contribution": round(negative, 8),
        "weighted_net_contribution": round(net, 8),
        "weighted_average_change_pct": round(net, 8),
        "coverage_adjusted_weighted_change_pct": round(net / covered * 100.0, 8) if covered else None,
        "covered_weight_pct": round(covered, 6),
        "uncovered_weight_pct": round(max(0.0, 100.0 - covered), 6),
        "missing_price_symbols": sorted(set(missing_price)),
        "missing_weight_symbols": sorted(set(missing_weight)),
    }


def sector_weight_exposure(holdings: Iterable[dict[str, Any]]) -> dict[str, Any]:
    sectors: dict[str, dict[str, Any]] = defaultdict(lambda: {"weight_pct": 0.0, "holding_count": 0, "holdings": []})
    total = 0.0
    known = 0.0
    for item in holdings:
        weight = parse_weight_value(item.get("weight_pct") if item.get("weight_pct") is not None else item.get("weight"))
        if weight is None:
            continue
        sector = str(item.get("sector") or "Unknown")
        total += weight
        if sector != "Unknown":
            known += weight
        bucket = sectors[sector]
        bucket["weight_pct"] += weight
        bucket["holding_count"] += 1
        bucket["holdings"].append((str(item.get("symbol") or ""), weight))
    rows = []
    for sector, bucket in sectors.items():
        top = sorted(bucket.pop("holdings"), key=lambda row: row[1], reverse=True)[:5]
        rows.append(
            {
                "sector": sector,
                "weight_pct": round(bucket["weight_pct"], 4),
                "holding_count": bucket["holding_count"],
                "top_constituents": [{"symbol": symbol, "weight_pct": round(weight, 4)} for symbol, weight in top],
            }
        )
    rows.sort(key=lambda item: item["weight_pct"], reverse=True)
    return {
        "sectors": rows,
        "sector_weight_coverage_pct": round(known, 4),
        "unknown_weight_pct": round(max(0.0, total - known), 4),
        "total_weight_pct": round(total, 4),
        "complete_portfolio_coverage": bool(total >= 99.0 and known >= 99.0),
    }


def weight_quality_score(
    *,
    method: str | None,
    weight_coverage_pct: float,
    price_coverage_pct: float,
    sector_coverage_pct: float,
    stale: bool,
    issuer_semantics_quality_score: float = 1.0,
) -> dict[str, Any]:
    method_score = {
        OFFICIAL_QQQ_WEIGHT: 1.0,
        OFFICIAL_NASDAQ100_WEIGHT: 0.97,
        VENDOR_QQQ_WEIGHT: 0.88,
        RECONSTRUCTED_MARKET_CAP_WEIGHT: 0.76,
        EQUAL_WEIGHT_PROXY: 0.35,
    }.get(str(method), 0.0)
    proxy_penalty = 0.25 if method == EQUAL_WEIGHT_PROXY else 0.08 if method == RECONSTRUCTED_MARKET_CAP_WEIGHT else 0.0
    stale_penalty = 0.15 if stale else 0.0
    semantics_penalty = (1.0 - min(max(issuer_semantics_quality_score, 0.0), 1.0)) * 0.25
    score = (
        method_score * 0.45
        + min(max(weight_coverage_pct, 0.0), 100.0) / 100.0 * 0.30
        + min(max(price_coverage_pct, 0.0), 100.0) / 100.0 * 0.15
        + min(max(sector_coverage_pct, 0.0), 100.0) / 100.0 * 0.10
        - proxy_penalty
        - stale_penalty
        - semantics_penalty
    )
    return {
        "weight_quality_score": round(min(max(score, 0.0), 1.0), 3),
        "proxy_penalty": proxy_penalty,
        "stale_penalty": stale_penalty,
        "issuer_semantics_penalty": round(semantics_penalty, 3),
    }


def log_weight_event(event: str, **fields: Any) -> None:
    logger.info(event, extra={key: value for key, value in fields.items() if value is not None})


def _rows_to_holdings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    holdings: list[dict[str, Any]] = []
    for row in rows:
        lowered = {str(key).strip().lower(): value for key, value in row.items() if key is not None}
        symbol = normalize_symbol(_pick(lowered, "ticker", "holding ticker", "symbol", "identifier"))
        if not symbol:
            continue
        name = _pick(lowered, "name", "holding name", "security name", "company", "description")
        weight = parse_weight_value(_pick(lowered, "weight", "weight (%)", "% weight", "weighting", "portfolio_percentage"))
        holdings.append(
            {
                "symbol": symbol,
                "name": name,
                "company_name": name,
                "share_class": share_class(symbol, str(name or "")),
                "weight": weight,
                "weight_pct": weight,
                "sector": _pick(lowered, "sector", "gics sector"),
                "market_cap": parse_market_value(_pick(lowered, "market cap", "marketcap")),
                "warnings": [] if weight is not None else ["weight_missing_or_invalid"],
            }
        )
    return holdings


def _weight_fields(
    *,
    weight: float | None,
    method: str,
    source: str,
    source_url: str,
    as_of: str | None,
    retrieved_at: datetime,
    valid_until: datetime,
    verified: bool,
    official: bool,
    reconstructed: bool,
    confidence: float,
) -> dict[str, Any]:
    return {
        "weight": weight,
        "weight_pct": weight,
        "weight_source": source,
        "weight_source_url": source_url,
        "weight_method": method,
        "weight_as_of": as_of,
        "weight_retrieved_at": _iso(retrieved_at),
        "weight_valid_until": _iso(valid_until),
        "weight_verified": verified,
        "weight_is_official": official,
        "weight_is_reconstructed": reconstructed,
        "weight_confidence": confidence,
        "source": source,
        "source_url": source_url,
        "as_of": as_of,
        "retrieved_at": _iso(retrieved_at),
        "valid_until": _iso(valid_until),
        "is_official": official,
        "is_reconstructed": reconstructed,
        "confidence": confidence,
    }


def apply_weight_provenance(
    holdings: list[dict[str, Any]],
    *,
    method: str,
    source: str,
    source_url: str,
    as_of: str | None,
    retrieved_at: datetime,
    valid_until: datetime,
    official: bool,
    reconstructed: bool,
    confidence: float,
) -> list[dict[str, Any]]:
    for item in holdings:
        item.update(
            _weight_fields(
                weight=parse_weight_value(item.get("weight_pct") if item.get("weight_pct") is not None else item.get("weight")),
                method=method,
                source=source,
                source_url=source_url,
                as_of=as_of,
                retrieved_at=retrieved_at,
                valid_until=valid_until,
                verified=True,
                official=official,
                reconstructed=reconstructed,
                confidence=confidence,
            )
        )
    return holdings


def _find_as_of(lines: Iterable[str]) -> str | None:
    for line in lines:
        match = re.search(r"as\s+of[^0-9]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2})", line, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _pick(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _xlsx_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    try:
        root = ElementTree.fromstring(archive.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    return ["".join(item.itertext()) for item in root]


def _timestamp(value: Any) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)
