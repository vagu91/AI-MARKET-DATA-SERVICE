from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.text_normalization import normalize_text
from app.services.data_freshness_service import parse_datetime
from app.services.source_policy_service import SourcePolicyService


OFFICIAL_SOURCE_KEYWORDS = {
    "BLS": ("BLS", "BUREAU OF LABOR STATISTICS", "bls.gov"),
    "BEA": ("BEA", "BUREAU OF ECONOMIC ANALYSIS", "bea.gov"),
    "FEDERAL_RESERVE": ("FEDERAL RESERVE", "FOMC", "federalreserve.gov"),
    "SEC": ("SEC", "sec.gov"),
    "NASDAQ_OFFICIAL": ("NASDAQ OFFICIAL",),
}

MARKET_SOURCE_KEYWORDS = (
    "REUTERS",
    "CNBC",
    "BLOOMBERG",
    "WSJ",
    "BARRON",
    "YAHOO FINANCE",
    "SEEKING ALPHA",
    "MARKETWATCH",
    "MARKETBEAT",
)

NON_OFFICIAL_PUBLISHERS = (
    "MARKETBEAT",
    "SEEKING ALPHA",
    "YAHOO FINANCE",
    "AOL",
    "BARRON",
    "MARKETWATCH",
)

SECTOR_MAP_VERSION = "qqq_top_holdings_static_v1"
SECTOR_MAP = {
    "NVDA": "Information Technology",
    "AAPL": "Information Technology",
    "MSFT": "Information Technology",
    "AMZN": "Consumer Discretionary",
    "META": "Communication Services",
    "GOOGL": "Communication Services",
    "GOOG": "Communication Services",
    "AVGO": "Information Technology",
    "TSLA": "Consumer Discretionary",
    "AMD": "Information Technology",
    "NFLX": "Communication Services",
    "COST": "Consumer Staples",
    "MU": "Information Technology",
    "INTC": "Information Technology",
    "AMAT": "Information Technology",
    "CSCO": "Information Technology",
    "WMT": "Consumer Staples",
    "PLTR": "Information Technology",
    "ADBE": "Information Technology",
    "QCOM": "Information Technology",
    "TXN": "Information Technology",
    "ISRG": "Health Care",
    "PEP": "Consumer Staples",
    "BKNG": "Consumer Discretionary",
    "LIN": "Materials",
    "INTU": "Information Technology",
    "AMGN": "Health Care",
    "HON": "Industrials",
    "PDD": "Consumer Discretionary",
}


def utc_now() -> datetime:
    return datetime.now(UTC)


def temporal_status(
    *,
    release_at: Any,
    actual: Any = None,
    valid_until: Any = None,
    invalid: bool = False,
    duplicate: bool = False,
    now: datetime | None = None,
) -> str:
    now = now or utc_now()
    release = parse_datetime(release_at)
    valid = parse_datetime(valid_until)
    if invalid:
        return "invalid"
    if duplicate:
        return "duplicate"
    if valid and now > valid and release is None:
        return "expired"
    if release is None:
        return "scheduled"
    if now < release:
        return "pre_release"
    if actual in (None, ""):
        return "awaiting_actual"
    return "released"


def fact_temporal_kind(fact: dict[str, Any]) -> str:
    fact_type = str(fact.get("fact_type") or "").lower()
    if fact_type == "official_macro_latest":
        return "published_macro_series"
    if fact_type in {"qqq_holdings", "mega_cap_snapshot", "mega_cap_breadth", "earnings_event", "nasdaq_context"}:
        return "market_snapshot"
    if fact_type in {"macro_event_enrichment", "ai_research_result"}:
        return "scheduled_release_event"
    if fact_type == "news_item":
        return "news_item"
    if fact.get("event_name") and fact.get("release_at"):
        return "scheduled_release_event"
    return "market_snapshot"


def fact_temporal_status(fact: dict[str, Any], *, now: datetime | None = None) -> str:
    kind = fact_temporal_kind(fact)
    if kind == "published_macro_series":
        valid_until = parse_datetime(fact.get("valid_until"))
        if valid_until and (now or utc_now()) > valid_until:
            return "refresh_due"
        if fact.get("value") not in (None, "") and fact.get("release_at"):
            return "published"
        return "no_data_available"
    if kind == "scheduled_release_event":
        return temporal_status(
            release_at=fact.get("release_at"),
            actual=fact.get("actual"),
            valid_until=fact.get("valid_until"),
            now=now,
        )
    valid_until = parse_datetime(fact.get("valid_until"))
    if valid_until and (now or utc_now()) > valid_until:
        return "refresh_due"
    return "published"


def news_content_status(item: dict[str, Any]) -> str:
    title = clean_text(item.get("title"))
    text = str(title or "").strip()
    upper = text.upper()
    if not text:
        return "invalid_content"
    if upper in {"META_TITLE_QUOTE", "TITLE_QUOTE", "N/A", "NULL", "NONE"}:
        return "invalid_content"
    if "META_TITLE_QUOTE" in upper:
        return "invalid_content"
    if upper.endswith("_TITLE_QUOTE") or upper.endswith("_QUOTE"):
        return "invalid_content"
    if "_" in text and upper == text and not any(ch.isalpha() and ch.islower() for ch in text):
        return "invalid_content"
    if any(token in text for token in ("Ã", "â", "\ufffd")):
        return "invalid_content"
    return "valid"


def reject_future_actual(item: dict[str, Any], *, now: datetime | None = None) -> tuple[dict[str, Any], bool]:
    now = now or utc_now()
    output = dict(item)
    release = parse_datetime(output.get("time_utc") or output.get("release_at") or output.get("date"))
    if release is None or now >= release:
        return output, False
    rejected = False
    if output.get("actual") not in (None, ""):
        output["actual"] = None
        rejected = True
    metrics = []
    for metric in output.get("metrics") or []:
        if not isinstance(metric, dict):
            metrics.append(metric)
            continue
        metric = dict(metric)
        if metric.get("actual") not in (None, ""):
            metric["actual"] = None
            rejected = True
        semantics = dict(metric.get("field_semantics") or {})
        semantics["actual_is_official"] = False
        semantics["actual_release_verified"] = False
        metric["field_semantics"] = semantics
        metrics.append(metric)
    if metrics:
        output["metrics"] = metrics
    if rejected:
        warnings = list(output.get("warnings") or [])
        if "actual_before_release_rejected" not in warnings:
            warnings.append("actual_before_release_rejected")
        output["warnings"] = warnings
        output["status"] = "pre_release"
    return output, rejected


def freshness_label(
    *,
    valid_until: Any = None,
    release_at: Any = None,
    actual: Any = None,
    now: datetime | None = None,
) -> str:
    now = now or utc_now()
    valid = parse_datetime(valid_until)
    release = parse_datetime(release_at)
    if release and now >= release and actual in (None, ""):
        return "AWAITING_REFRESH"
    if valid is None:
        return "UNKNOWN"
    if now <= valid:
        return "FRESH"
    if release and now > release:
        return "EXPIRED"
    return "STALE"


def next_release_refresh_at(
    *,
    release_at: Any,
    attempt_count: int,
    retry_seconds: list[int],
    now: datetime | None = None,
) -> str | None:
    release = parse_datetime(release_at)
    if release is None:
        return None
    now = now or utc_now()
    index = max(min(attempt_count, len(retry_seconds) - 1), 0)
    candidate = release + timedelta(seconds=retry_seconds[index])
    if candidate < now and index + 1 < len(retry_seconds):
        candidate = now + timedelta(seconds=retry_seconds[index + 1])
    return candidate.replace(microsecond=0).isoformat()


def parse_retry_seconds(value: str | None) -> list[int]:
    output = []
    for item in str(value or "").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            output.append(max(1, int(item)))
        except ValueError:
            continue
    return output or [30, 120, 300, 900, 1800, 3600]


def classify_source(source: Any, source_url: Any = None) -> dict[str, Any]:
    return SourcePolicyService().provenance(
        source=str(source or "") or None,
        source_url=str(source_url or "") or None,
    )


def clean_text(value: Any) -> Any:
    return normalize_text(value)


def classify_holding_sector(holding: dict[str, Any]) -> dict[str, Any]:
    output = dict(holding)
    symbol = str(output.get("symbol") or "").upper()
    if not output.get("sector"):
        output["sector"] = SECTOR_MAP.get(symbol) or _infer_sector_from_name(str(output.get("name") or ""))
        if output["sector"] != "Unknown":
            output["sector_source"] = SECTOR_MAP_VERSION if symbol in SECTOR_MAP else "name_keyword_fallback_v1"
    return output


def sector_exposure(holdings: list[dict[str, Any]], *, total_holdings_count: int | None = None, coverage_scope: str | None = None) -> dict[str, Any]:
    exposure: dict[str, float] = {}
    total = 0.0
    classified = 0.0
    holdings_with_weight = 0
    for raw in holdings:
        holding = classify_holding_sector(raw)
        weight_value = holding.get("weight")
        if weight_value is None:
            continue
        weight = float(weight_value or 0.0)
        holdings_with_weight += 1
        sector = str(holding.get("sector") or "Unknown")
        total += weight
        if sector != "Unknown":
            classified += weight
        exposure[sector] = exposure.get(sector, 0.0) + weight
    covered_count = len(holdings)
    total_count = total_holdings_count or covered_count
    if covered_count and holdings_with_weight == 0:
        return {
            "by_sector_weight_pct": {},
            "sector_weight_pct": {},
            "classified_weight_pct": None,
            "unknown_weight_pct": None,
            "total_weight_pct": None,
            "covered_holdings_count": covered_count,
            "total_holdings_count": total_count,
            "covered_holdings_weight_pct": None,
            "uncovered_holdings_weight_pct": None,
            "portfolio_weight_pct": None,
            "coverage_scope": coverage_scope or f"top_{covered_count}_holdings_weight_unavailable",
            "sector_classification_source": SECTOR_MAP_VERSION,
            "complete_portfolio_coverage": False,
            "weight_data_available": False,
            "sector_map_version": SECTOR_MAP_VERSION,
            "data_quality": {
                "unknown_below_threshold": None,
                "warnings": ["sector_weight_data_unavailable"],
            },
        }
    complete_by_weight = 99.0 <= total <= 101.0
    complete_by_count = total_count == covered_count and holdings_with_weight == covered_count and complete_by_weight
    inferred_scope = coverage_scope or ("complete_portfolio" if complete_by_count else f"top_{covered_count}_holdings" if covered_count else "empty")
    uncovered = 0.0 if complete_by_weight or total <= 0 else max(0.0, 100.0 - total)
    unknown = exposure.get("Unknown", 0.0) + uncovered
    sector_weights = {sector: round(weight, 4) for sector, weight in exposure.items() if sector != "Unknown"}
    if exposure.get("Unknown"):
        sector_weights["Unknown"] = round(exposure["Unknown"], 4)
    return {
        "by_sector_weight_pct": sector_weights,
        "sector_weight_pct": sector_weights,
        "classified_weight_pct": round(classified, 4),
        "unknown_weight_pct": round(unknown, 4),
        "total_weight_pct": round(total, 4),
        "covered_holdings_count": covered_count,
        "total_holdings_count": total_count,
        "covered_holdings_weight_pct": round(total, 4),
        "uncovered_holdings_weight_pct": round(uncovered, 4),
        "portfolio_weight_pct": 100.0 if total > 0 else 0.0,
        "coverage_scope": inferred_scope,
        "sector_classification_source": SECTOR_MAP_VERSION,
        "complete_portfolio_coverage": complete_by_count,
        "weight_data_available": holdings_with_weight > 0,
        "sector_map_version": SECTOR_MAP_VERSION,
        "data_quality": {
            "unknown_below_threshold": unknown < 10.0,
            "warnings": [] if unknown < 10.0 else ["sector_unknown_or_uncovered_weight_above_threshold"],
        },
    }


def calculate_surprise(metric: dict[str, Any]) -> dict[str, Any] | None:
    actual = _num(metric.get("actual"))
    forecast = _num(metric.get("forecast"))
    consensus = _num(metric.get("consensus"))
    if actual is None:
        return None
    vs_forecast = None if forecast is None else round(actual - forecast, 6)
    vs_consensus = None if consensus is None else round(actual - consensus, 6)
    direction = None
    if vs_consensus is not None:
        direction = "above_consensus" if vs_consensus > 0 else "below_consensus" if vs_consensus < 0 else "in_line_consensus"
    elif vs_forecast is not None:
        direction = "above_forecast" if vs_forecast > 0 else "below_forecast" if vs_forecast < 0 else "in_line_forecast"
    return {
        "vs_forecast": vs_forecast,
        "vs_consensus": vs_consensus,
        "direction": direction,
        "unit": metric.get("unit"),
    }


def _num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace("%", "").replace(",", ""))
    except ValueError:
        return None


def _infer_sector_from_name(name: str) -> str:
    text = name.upper()
    if not text or text in {"N/A", "CASH"} or "FUTURE" in text:
        return "Unknown"
    if any(token in text for token in ("SEMICONDUCTOR", "TECHNOLOGY", "TECH", "SOFTWARE", "SYSTEMS", "DATA", "NETWORKS", "MICRO", "ANALOG", "ELECTRONIC", "CYBER", "DIGITAL", "CROWDSTRIKE", "PALO ALTO", "FORTINET", "AUTODESK", "SYNOPSYS", "CADENCE", "PAYPAL", "WORKDAY", "COREWEAVE", "APPLOVIN", "ASTERA", "ASML", "ARM", "SANDISK", "WESTERN DIGITAL", "SEAGATE", "MARVELL", "LUMENTUM", "ROPER")):
        return "Information Technology"
    if any(token in text for token in ("PHARM", "BIOTECH", "HEALTH", "LABORATOR", "GILEAD", "VERTEX", "REGENERON", "DEXCOM", "IDEXX", "ALNYLAM")):
        return "Health Care"
    if any(token in text for token in ("HOTEL", "RESTAURANT", "STARBUCKS", "MARRIOTT", "AIRBNB", "AUTOMOTIVE", "MERCADOLIBRE", "DOORDASH", "O'REILLY", "ROSS STORES", "BOOKING", "ELECTRONIC ARTS", "TAKE-TWO")):
        return "Consumer Discretionary"
    if any(token in text for token in ("BEVERAGE", "FOODS", "MONDELEZ", "KRAFT", "COCA-COLA", "KEURIG", "MONSTER")):
        return "Consumer Staples"
    if any(token in text for token in ("TELECOM", "T-MOBILE", "COMCAST", "WARNER", "THOMSON REUTERS")):
        return "Communication Services"
    if any(token in text for token in ("ENERGY", "BAKER HUGHES", "DIAMONDBACK", "CONSTELLATION", "EXELON", "XCEL", "ELECTRIC POWER")):
        return "Energy" if any(token in text for token in ("BAKER", "DIAMONDBACK")) else "Utilities"
    if any(token in text for token in ("RAIL", "PACCAR", "TRUCK", "FREIGHT", "AEROSPACE", "HONEYWELL", "CINTAS", "COPART", "ROCKET LAB", "TERADYNE", "FASTENAL", "FERROVIAL", "AXON")):
        return "Industrials"
    if any(token in text for token in ("LINDE", "MATERIAL", "CHEMICAL")):
        return "Materials"
    return "Unknown"
