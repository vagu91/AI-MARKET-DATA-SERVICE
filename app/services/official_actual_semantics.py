from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import date, datetime
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


def _metric_change_basis_markers(value: Any) -> set[str]:
    text = " ".join(
        re.findall(
            r"[a-z0-9]+",
            str(value or "").casefold(),
        )
    )
    tokens = set(text.split())
    detected: set[str] = set()
    if (
        tokens & {"yoy", "annuale"}
        or re.search(r"\b(?:a|y)\s+(?:a|y)\b", text)
        or "anno su anno" in text
        or "year over year" in text
    ):
        detected.add("yoy")
    if (
        tokens & {"mom", "mensile"}
        or re.search(r"\bm\s+m\b", text)
        or "mese su mese" in text
        or "month over month" in text
    ):
        detected.add("mom")
    if (
        tokens & {"qoq"}
        or re.search(r"\b(?:q\s+q|t\s+t)\b", text)
        or "trimestre su trimestre" in text
        or "quarter over quarter" in text
    ):
        detected.add("qoq")
    return detected


def metric_change_basis_from_text(value: Any) -> str | None:
    detected = _metric_change_basis_markers(value)
    return next(iter(detected)) if len(detected) == 1 else None


def metric_semantics_mismatch_reason(
    metric_id: Any,
    *,
    name: Any,
    frequency_hint: Any = None,
) -> str | None:
    metric = str(metric_id or "").strip().lower()
    if not metric:
        return None
    observed_markers = _metric_change_basis_markers(
        " ".join(
            str(item or "")
            for item in (name, frequency_hint)
        )
    )
    if len(observed_markers) > 1:
        return "EVENT_METRIC_FREQUENCY_AMBIGUOUS"
    expected_basis = next(
        (
            basis
            for basis in ("mom", "yoy", "qoq")
            if metric.endswith(f"_{basis}")
        ),
        None,
    )
    observed_basis = (
        next(iter(observed_markers))
        if observed_markers
        else None
    )
    if (
        expected_basis is not None
        and observed_basis is not None
        and expected_basis != observed_basis
    ):
        return "EVENT_METRIC_FREQUENCY_MISMATCH"

    expected_family = next(
        (
            family
            for family in ("cpi", "ppi", "pce")
            if re.search(rf"(?:^|_){family}(?:_|$)", metric)
        ),
        None,
    )
    normalized_name = " ".join(
        re.findall(r"[a-z0-9]+", str(name or "").casefold())
    )
    family_markers = {
        "cpi": (
            "cpi",
            "consumer price index",
            "prezzi al consumo",
        ),
        "ppi": (
            "ppi",
            "producer price index",
            "prezzi alla produzione",
        ),
        "pce": (
            "pce",
            "personal consumption expenditure",
            "personal consumption expenditures",
        ),
    }
    observed_family = next(
        (
            family
            for family, markers in family_markers.items()
            if any(
                re.search(rf"\b{re.escape(marker)}\b", normalized_name)
                for marker in markers
            )
        ),
        None,
    )
    if (
        expected_family is not None
        and observed_family is not None
        and expected_family != observed_family
    ):
        return "EVENT_METRIC_FAMILY_MISMATCH"
    expected_variant = next(
        (
            variant
            for variant in ("core", "headline")
            if metric.startswith(f"{variant}_")
        ),
        None,
    )
    core_markers = (
        "core",
        "base",
        "di base",
        "di fondo",
        "excluding food and energy",
        "ex food and energy",
    )
    headline_markers = ("headline",)
    observed_variant = (
        "core"
        if any(
            re.search(
                rf"\b{re.escape(marker)}\b",
                normalized_name,
            )
            for marker in core_markers
        )
        else "headline"
        if any(
            re.search(
                rf"\b{re.escape(marker)}\b",
                normalized_name,
            )
            for marker in headline_markers
        )
        else None
    )
    if (
        expected_variant is not None
        and observed_variant is not None
        and expected_variant != observed_variant
    ):
        return "EVENT_METRIC_VARIANT_MISMATCH"
    return None


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
    "employment_cost_index_qoq": OfficialMetricSpec(
        "employment_cost_index_qoq", "BLS", "CIU1010000000000A", "pct_change_qoq", "NSA",
        "quarterly", "percent", 1, "0.1", "https://www.bls.gov/eci/",
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
    "advance_retail_sales": OfficialMetricSpec(
        "advance_retail_sales", "CENSUS", "CENSUS:MARTS:RETAIL_SALES", "level", "SA",
        "monthly", "millions_usd", 0, "0.1", "https://www.census.gov/retail/",
    ),
    "advance_durable_goods_orders": OfficialMetricSpec(
        "advance_durable_goods_orders", "CENSUS", "CENSUS:ADVM3:DURABLE_GOODS", "level", "SA",
        "monthly", "millions_usd", 0, "0.1", "https://www.census.gov/manufacturing/m3/",
    ),
    "housing_starts": OfficialMetricSpec(
        "housing_starts", "CENSUS", "CENSUS:RESCONST:HOUSING_STARTS", "level", "SAAR",
        "monthly", "thousands_annual_rate", 0, "1", "https://www.census.gov/construction/nrc/",
    ),
    "building_permits": OfficialMetricSpec(
        "building_permits", "CENSUS", "CENSUS:RESCONST:BUILDING_PERMITS", "level", "SAAR",
        "monthly", "thousands_annual_rate", 0, "1", "https://www.census.gov/construction/nrc/",
    ),
    "international_trade_balance": OfficialMetricSpec(
        "international_trade_balance", "CENSUS", "CENSUS:FTD:TRADE_BALANCE", "level", "SA",
        "monthly", "millions_usd", 0, "0.1", "https://www.census.gov/foreign-trade/",
    ),
    "new_home_sales": OfficialMetricSpec(
        "new_home_sales", "FRED", "HSN1F", "level", "SAAR",
        "monthly", "thousands_annual_rate", 1, "1", "https://fred.stlouisfed.org/series/HSN1F",
    ),
    "flash_services_pmi": OfficialMetricSpec(
        "flash_services_pmi", "SPGLOBAL", "SPGLOBAL:US:FLASH_SERVICES_PMI", "level", "SA",
        "monthly", "index_points", 1, "0.1", "https://www.pmi.spglobal.com/Public/Home/PressRelease",
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
        "previous": (
            _decimal_text(previous["value"])
            if previous is not None and spec.transformation == "level"
            else None
        ),
        "previous_reference_period": (
            previous["period"]
            if previous is not None and spec.transformation == "level"
            else None
        ),
        "calculation_lineage": lineage,
        "warnings": warnings,
    }


def normalize_reference_period(
    value: Any,
    *,
    frequency: str,
    release_date: date | datetime | None = None,
) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    localized_months = {
        "gennaio": 1,
        "january": 1,
        "jan": 1,
        "febbraio": 2,
        "february": 2,
        "feb": 2,
        "marzo": 3,
        "march": 3,
        "mar": 3,
        "aprile": 4,
        "april": 4,
        "apr": 4,
        "maggio": 5,
        "may": 5,
        "giugno": 6,
        "june": 6,
        "jun": 6,
        "luglio": 7,
        "july": 7,
        "jul": 7,
        "agosto": 8,
        "august": 8,
        "aug": 8,
        "settembre": 9,
        "september": 9,
        "sep": 9,
        "sept": 9,
        "ottobre": 10,
        "october": 10,
        "oct": 10,
        "novembre": 11,
        "november": 11,
        "nov": 11,
        "dicembre": 12,
        "december": 12,
        "dec": 12,
    }
    if frequency == "monthly" and text in localized_months and release_date:
        anchor = (
            release_date.date()
            if isinstance(release_date, datetime)
            else release_date
        )
        month_number = localized_months[text]
        year = (
            anchor.year
            if month_number <= anchor.month
            else anchor.year - 1
        )
        return f"{year:04d}-{month_number:02d}"
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
    elif spec.transformation in {"pct_change_mom", "pct_change_yoy", "pct_change_qoq"}:
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
        "pct_change_qoq": "((current / previous_quarter) - 1) * 100",
        "official_annualized_qoq_rate": "official_published_annualized_qoq_rate",
    }.get(transformation, transformation)


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")
