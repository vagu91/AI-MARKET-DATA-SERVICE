from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse

from app.services.provider_capability_registry import (
    OFFICIAL_METRIC_REGISTRATIONS,
    OfficialMetricRegistration,
)


OfficialMetricSpec = OfficialMetricRegistration

_EVENT_FAMILY_MARKERS: dict[str, tuple[str, ...]] = {
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
        "personal income and outlays",
        "personal consumption expenditure",
        "personal consumption expenditures",
    ),
    "new_home_sales": (
        "new home sales",
        "new home sale",
        "new homes sales",
        "vendita case nuove",
        "vendite case nuove",
        "vendita di case nuove",
        "vendite di nuove abitazioni",
        "vendite di nuove case",
    ),
    "flash_services_pmi": (
        "flash services pmi",
        "flash service pmi",
        "flash us services pmi",
        "services pmi",
        "us services pmi",
        "indice pmi dei servizi",
        "indice pmi servizi",
        "pmi dei servizi",
        "pmi servizi flash",
    ),
}
_STRICT_EVENT_NAME_FAMILIES = frozenset(
    {"pce", "new_home_sales", "flash_services_pmi"}
)


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

    expected_family = (
        metric
        if metric in {"new_home_sales", "flash_services_pmi"}
        else next(
            (
                family
                for family in ("cpi", "ppi", "pce")
                if re.search(rf"(?:^|_){family}(?:_|$)", metric)
            ),
            None,
        )
    )
    normalized_name = " ".join(
        re.findall(r"[a-z0-9]+", str(name or "").casefold())
    )
    observed_families = {
        family
        for family, markers in _EVENT_FAMILY_MARKERS.items()
        if any(
            re.search(rf"\b{re.escape(marker)}\b", normalized_name)
            for marker in markers
        )
    }
    if len(observed_families) > 1:
        return "EVENT_METRIC_FAMILY_AMBIGUOUS"
    observed_family = next(iter(observed_families), None)
    if (
        expected_family is not None
        and observed_family is not None
        and expected_family != observed_family
    ):
        return "EVENT_METRIC_FAMILY_MISMATCH"
    if (
        expected_family in _STRICT_EVENT_NAME_FAMILIES
        and observed_family is None
    ):
        return "EVENT_METRIC_FAMILY_NOT_PROVEN"
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
    specification.canonical_metric_id: specification
    for specification in OFFICIAL_METRIC_REGISTRATIONS
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
    source_series_id = str(series.get("series_id") or "").strip()
    if source_series_id != spec.source_series_id:
        raise ValueError("source_series_mismatch")
    source_candidates = (
        series.get("source"),
        series.get("source_originator"),
    )
    if not any(
        _source_identity(candidate)
        == _source_identity(spec.provider_id)
        for candidate in source_candidates
        if candidate
    ):
        raise ValueError("source_provider_mismatch")
    frequency = str(series.get("frequency") or "").strip().lower()
    if frequency != spec.frequency:
        raise ValueError("source_frequency_mismatch")
    seasonal_adjustment = str(
        series.get("seasonal_adjustment") or ""
    ).strip().upper()
    if seasonal_adjustment != spec.seasonal_adjustment:
        raise ValueError("source_seasonal_adjustment_mismatch")

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
    release_vintage = (
        current.get("release_vintage")
        or series.get("release_vintage")
        or None
    )
    calculation_lineage = {
        "current_observation": _lineage_observation(current),
        "comparison_observation": _lineage_observation(previous) if previous else None,
        "observation_count": len(observations),
        "formula": _formula(spec.transformation),
    }
    actual = _decimal_text(value)
    source_url = str(
        series.get("source_url")
        or series.get("canonical_url")
        or spec.canonical_url
    )
    source_domain = str(
        series.get("source_domain")
        or urlparse(source_url).hostname
        or ""
    ).strip().casefold()
    field_lineage = {
        "field": "actual",
        "value": actual,
        "metric_id": spec.canonical_metric_id,
        "source": spec.provider_id,
        "acquisition_provider": spec.provider_id,
        "source_series_id": spec.source_series_id,
        "source_url": source_url,
        "source_domain": source_domain,
        "reference_period": current["period"],
        "frequency": spec.frequency,
        "transformation": spec.transformation,
        "seasonal_adjustment": spec.seasonal_adjustment,
        "unit": spec.unit,
        "retrieved_at": retrieved_at,
        "release_timestamp": release_timestamp,
        "calculation": calculation_lineage,
        "verification_status": "VERIFIED",
    }
    return {
        "field": "actual",
        "field_semantics": "actual",
        "value": actual,
        "actual": actual,
        "metric_id": spec.event_metric_id,
        "event_metric_id": spec.event_metric_id,
        "source_series_id": spec.source_series_id,
        "transformation": spec.transformation,
        "seasonal_adjustment": spec.seasonal_adjustment,
        "frequency": spec.frequency,
        "unit": spec.unit,
        "source": spec.provider_id,
        "acquisition_provider": spec.provider_id,
        "source_url": source_url,
        "source_domain": source_domain,
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
        "calculation_lineage": calculation_lineage,
        "lineage": {"actual": field_lineage},
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
        release_vintage = raw.get("release_vintage") or raw.get("vintage")
        vintage_key = str(release_vintage or position)
        selected[(period, vintage_key)] = {
            **raw,
            "period": period,
            "value": value,
            "release_vintage": release_vintage,
        }
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


def _source_identity(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())
