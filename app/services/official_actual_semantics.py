from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any


@dataclass(frozen=True)
class OfficialMetricSpec:
    event_metric_id: str
    provider: str
    source_series_id: str
    transformation: str
    seasonal_adjustment: str
    frequency: str
    unit: str
    comparison_lag: int
    precision: str
    canonical_url: str


OFFICIAL_METRICS: dict[str, OfficialMetricSpec] = {
    "headline_cpi_mom": OfficialMetricSpec(
        "headline_cpi_mom", "BLS", "CUSR0000SA0", "pct_change_mom", "SA",
        "monthly", "percent", 1, "0.1", "https://www.bls.gov/cpi/",
    ),
    "headline_cpi_yoy": OfficialMetricSpec(
        "headline_cpi_yoy", "BLS", "CUUR0000SA0", "pct_change_yoy", "NSA",
        "monthly", "percent", 12, "0.1", "https://www.bls.gov/cpi/",
    ),
    "core_cpi_mom": OfficialMetricSpec(
        "core_cpi_mom", "BLS", "CUSR0000SA0L1E", "pct_change_mom", "SA",
        "monthly", "percent", 1, "0.1", "https://www.bls.gov/cpi/",
    ),
    "core_cpi_yoy": OfficialMetricSpec(
        "core_cpi_yoy", "BLS", "CUUR0000SA0L1E", "pct_change_yoy", "NSA",
        "monthly", "percent", 12, "0.1", "https://www.bls.gov/cpi/",
    ),
    "headline_ppi_mom": OfficialMetricSpec(
        "headline_ppi_mom", "BLS", "WPSFD4", "pct_change_mom", "SA",
        "monthly", "percent", 1, "0.1", "https://www.bls.gov/ppi/",
    ),
    "headline_ppi_yoy": OfficialMetricSpec(
        "headline_ppi_yoy", "BLS", "WPUFD4", "pct_change_yoy", "NSA",
        "monthly", "percent", 12, "0.1", "https://www.bls.gov/ppi/",
    ),
    "nonfarm_payrolls_change": OfficialMetricSpec(
        "nonfarm_payrolls_change", "BLS", "CES0000000001", "delta", "SA",
        "monthly", "thousands of jobs", 1, "1", "https://www.bls.gov/ces/",
    ),
    "unemployment_rate": OfficialMetricSpec(
        "unemployment_rate", "BLS", "LNS14000000", "level", "SA",
        "monthly", "percent", 0, "0.1", "https://www.bls.gov/cps/",
    ),
    "average_hourly_earnings_mom": OfficialMetricSpec(
        "average_hourly_earnings_mom", "BLS", "CES0500000003", "pct_change_mom", "SA",
        "monthly", "percent", 1, "0.1", "https://www.bls.gov/ces/",
    ),
    "average_hourly_earnings_yoy": OfficialMetricSpec(
        "average_hourly_earnings_yoy", "BLS", "CES0500000003", "pct_change_yoy", "SA",
        "monthly", "percent", 12, "0.1", "https://www.bls.gov/ces/",
    ),
    "real_gdp_annualized_qoq": OfficialMetricSpec(
        "real_gdp_annualized_qoq", "BEA", "BEA:GDP", "official_annualized_qoq_rate", "SAAR",
        "quarterly", "percent", 0, "0.1", "https://www.bea.gov/data/gdp/gross-domestic-product",
    ),
    "real_gdp_yoy": OfficialMetricSpec(
        "real_gdp_yoy", "BEA", "BEA:REAL_GDP", "pct_change_yoy", "SAAR",
        "quarterly", "percent", 4, "0.1", "https://www.bea.gov/data/gdp/gross-domestic-product",
    ),
    "headline_pce_mom": OfficialMetricSpec(
        "headline_pce_mom", "BEA", "BEA:PCE_PRICE_INDEX", "pct_change_mom", "SA",
        "monthly", "percent", 1, "0.1", "https://www.bea.gov/data/consumer-spending/main",
    ),
    "headline_pce_yoy": OfficialMetricSpec(
        "headline_pce_yoy", "BEA", "BEA:PCE_PRICE_INDEX", "pct_change_yoy", "SA",
        "monthly", "percent", 12, "0.1", "https://www.bea.gov/data/consumer-spending/main",
    ),
    "core_pce_mom": OfficialMetricSpec(
        "core_pce_mom", "BEA", "BEA:CORE_PCE", "pct_change_mom", "SA",
        "monthly", "percent", 1, "0.1", "https://www.bea.gov/data/personal-consumption-expenditures-price-index-excluding-food-and-energy",
    ),
    "core_pce_yoy": OfficialMetricSpec(
        "core_pce_yoy", "BEA", "BEA:CORE_PCE", "pct_change_yoy", "SA",
        "monthly", "percent", 12, "0.1", "https://www.bea.gov/data/personal-consumption-expenditures-price-index-excluding-food-and-energy",
    ),
    "personal_income_mom": OfficialMetricSpec(
        "personal_income_mom", "BEA", "BEA:PERSONAL_INCOME", "pct_change_mom", "SAAR",
        "monthly", "percent", 1, "0.1", "https://www.bea.gov/data/income-saving/personal-income",
    ),
    "personal_spending_mom": OfficialMetricSpec(
        "personal_spending_mom", "BEA", "BEA:PERSONAL_SPENDING", "pct_change_mom", "SAAR",
        "monthly", "percent", 1, "0.1", "https://www.bea.gov/data/consumer-spending/main",
    ),
}


UNSUPPORTED_OFFICIAL_METRICS = {
    "core_ppi_mom": "official_core_ppi_series_not_demonstrated_in_existing_provider",
    "core_ppi_yoy": "official_core_ppi_series_not_demonstrated_in_existing_provider",
    "initial_jobless_claims": "official_weekly_claims_adapter_not_present",
}


def derive_official_actual(
    spec: OfficialMetricSpec,
    series: dict[str, Any],
    *,
    expected_period: Any,
    retrieved_at: str,
    release_timestamp: str | None,
) -> dict[str, Any]:
    all_observations = _normalized_observations(series)
    latest_by_period: dict[str, dict[str, Any]] = {}
    for observation in all_observations:
        latest_by_period[observation["period"]] = observation
    observations = sorted(latest_by_period.values(), key=lambda item: _period_key(item["period"]))
    expected = normalize_reference_period(expected_period, frequency=spec.frequency)
    if not observations:
        raise ValueError("official_observations_missing")
    current_index = len(observations) - 1
    if expected:
        matches = [index for index, item in enumerate(observations) if item["period"] == expected]
        if not matches:
            raise ValueError("period_mismatch")
        current_index = matches[-1]
    current = observations[current_index]
    if current_index < spec.comparison_lag:
        raise ValueError("insufficient_official_observations")
    previous = observations[current_index - spec.comparison_lag] if spec.comparison_lag else None
    value = _transform(spec, current["value"], previous["value"] if previous else None)
    revisions = [item for item in all_observations if item["period"] == current["period"]]
    warnings = ["official_observation_revised"] if len({item["value"] for item in revisions}) > 1 else []
    release_vintage = str(current.get("release_vintage") or series.get("release_vintage") or retrieved_at)
    lineage = {
        "current_observation": _lineage_observation(current),
        "comparison_observation": _lineage_observation(previous) if previous else None,
        "observation_count": len(observations),
        "formula": _formula(spec.transformation),
    }
    return {
        "field": "actual",
        "field_semantics": "actual",
        "value": _decimal_text(value),
        "metric_id": spec.event_metric_id,
        "event_metric_id": spec.event_metric_id,
        "source_series_id": spec.source_series_id,
        "transformation": spec.transformation,
        "seasonal_adjustment": spec.seasonal_adjustment,
        "frequency": spec.frequency,
        "unit": spec.unit,
        "period": current["period"],
        "reference_period": current["period"],
        "release_timestamp": release_timestamp,
        "retrieved_at": retrieved_at,
        "release_vintage": release_vintage,
        "current_level": _decimal_text(current["value"]),
        "comparison_level": _decimal_text(previous["value"]) if previous else None,
        "calculation_lineage": lineage,
        "warnings": warnings,
    }


def normalize_reference_period(value: Any, *, frequency: str) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    month = re.search(r"(?:month:)?(20\d{2})[-/m: ]0?(1[0-2]|[1-9])", text)
    if frequency == "monthly" and month:
        return f"{month.group(1)}-{int(month.group(2)):02d}"
    quarter = re.search(r"(20\d{2})[-/ ]?q(?:uarter:)?([1-4])", text)
    if frequency == "quarterly" and quarter:
        return f"{quarter.group(1)}-Q{quarter.group(2)}"
    if frequency == "monthly" and re.fullmatch(r"20\d{2}-\d{2}(?:-\d{2})?", text):
        return text[:7]
    if frequency == "quarterly" and re.fullmatch(r"20\d{2}q[1-4]", text):
        return f"{text[:4]}-Q{text[-1]}"
    return text.upper()


def _normalized_observations(series: dict[str, Any]) -> list[dict[str, Any]]:
    raw_items = series.get("observations") or [{
        "period": series.get("period") or series.get("data_as_of"),
        "value": series.get("value"),
        "release_vintage": series.get("release_vintage"),
    }]
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for position, raw in enumerate(raw_items):
        if not isinstance(raw, dict) or raw.get("value") in (None, "", "."):
            continue
        period = normalize_reference_period(
            raw.get("period") or raw.get("data_as_of"),
            frequency="quarterly" if "Q" in str(raw.get("period") or raw.get("data_as_of") or "").upper() else "monthly",
        )
        if not period:
            continue
        try:
            value = Decimal(str(raw["value"]).replace(",", ""))
        except InvalidOperation:
            continue
        vintage = str(raw.get("release_vintage") or raw.get("vintage") or position)
        selected[(period, vintage)] = {**raw, "period": period, "value": value, "release_vintage": vintage}
    return sorted(selected.values(), key=lambda item: (_period_key(item["period"]), str(item["release_vintage"])))


def _transform(spec: OfficialMetricSpec, current: Decimal, previous: Decimal | None) -> Decimal:
    if spec.transformation in {"level", "official_annualized_qoq_rate"}:
        value = current
    elif previous is None or previous == 0:
        raise ValueError("insufficient_official_observations")
    elif spec.transformation == "delta":
        value = current - previous
    elif spec.transformation in {"pct_change_mom", "pct_change_yoy"}:
        value = ((current / previous) - Decimal("1")) * Decimal("100")
    else:
        raise ValueError("unsupported_official_transformation")
    return value.quantize(Decimal(spec.precision), rounding=ROUND_HALF_UP)


def _period_key(period: str) -> tuple[int, int]:
    quarter = re.fullmatch(r"(20\d{2})-Q([1-4])", period)
    if quarter:
        return int(quarter.group(1)), int(quarter.group(2)) * 3
    month = re.fullmatch(r"(20\d{2})-(\d{2})", period)
    if month:
        return int(month.group(1)), int(month.group(2))
    return 0, 0


def _lineage_observation(item: dict[str, Any] | None) -> dict[str, Any] | None:
    if item is None:
        return None
    return {
        "period": item["period"], "value": _decimal_text(item["value"]),
        "release_vintage": item.get("release_vintage"),
    }


def _formula(transformation: str) -> str:
    return {
        "level": "current",
        "delta": "current - comparison",
        "pct_change_mom": "((current / previous_month) - 1) * 100",
        "pct_change_yoy": "((current / prior_year_period) - 1) * 100",
        "official_annualized_qoq_rate": "official_published_annualized_qoq_rate",
    }.get(transformation, transformation)


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")
