from __future__ import annotations

from typing import Any, Iterable


BLS_REQUIRED_SERIES: dict[str, dict[str, str]] = {
    "CPI": {"canonical_name": "CPI", "series_id": "CUSR0000SA0"},
    "PPI": {"canonical_name": "PPI", "series_id": "WPUFD4"},
    "Nonfarm Payrolls": {"canonical_name": "Nonfarm Payrolls", "series_id": "CES0000000001"},
    "Unemployment Rate": {"canonical_name": "Unemployment Rate", "series_id": "LNS14000000"},
}
BLS_REQUIRED_SERIES_IDS: tuple[str, ...] = tuple(item["series_id"] for item in BLS_REQUIRED_SERIES.values())
BLS_CANONICAL_NAME_BY_ID: dict[str, str] = {item["series_id"]: item["canonical_name"] for item in BLS_REQUIRED_SERIES.values()}

_BLS_ALIASES: dict[str, str] = {
    "CPI": "CUSR0000SA0",
    "HEADLINE CPI": "CUSR0000SA0",
    "HEADLINE_CPI": "CUSR0000SA0",
    "CONSUMER PRICE INDEX": "CUSR0000SA0",
    "PPI": "WPUFD4",
    "HEADLINE PPI": "WPUFD4",
    "HEADLINE_PPI": "WPUFD4",
    "PRODUCER PRICE INDEX": "WPUFD4",
    "PPI FINAL DEMAND": "WPUFD4",
    "NONFARM PAYROLLS": "CES0000000001",
    "NONFARM_PAYROLLS": "CES0000000001",
    "NFP": "CES0000000001",
    "PAYROLLS": "CES0000000001",
    "UNEMPLOYMENT RATE": "LNS14000000",
    "UNEMPLOYMENT_RATE": "LNS14000000",
}
_BLS_ALIASES.update({series_id: series_id for series_id in BLS_REQUIRED_SERIES_IDS})


def normalize_bls_series_id(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    if not text:
        return None
    compact = text.replace("_", " ").replace("-", " ").replace("/", " ")
    if text in _BLS_ALIASES:
        return _BLS_ALIASES[text]
    for series_id in BLS_REQUIRED_SERIES_IDS:
        if series_id in text:
            return series_id
    if "CONSUMER PRICE INDEX" in compact or compact == "CPI" or "HEADLINE CPI" in compact or " CPI " in f" {compact} ":
        return "CUSR0000SA0"
    if "PRODUCER PRICE INDEX" in compact or compact == "PPI" or " PPI " in f" {compact} " or "FINAL DEMAND" in compact:
        return "WPUFD4"
    if "NONFARM" in compact or "PAYROLL" in compact or compact == "NFP":
        return "CES0000000001"
    if "UNEMPLOYMENT" in compact:
        return "LNS14000000"
    return None


def normalize_bls_series_ids(values: Iterable[Any]) -> list[str]:
    found = {series_id for value in values if (series_id := normalize_bls_series_id(value))}
    return _ordered(found)


def bls_required_series_status(
    present_ids: Iterable[Any],
    *,
    materialized_ids: Iterable[Any] | None = None,
    invalid_ids: Iterable[Any] | None = None,
) -> dict[str, list[str]]:
    present = set(normalize_bls_series_ids(present_ids))
    materialized = set(normalize_bls_series_ids(materialized_ids if materialized_ids is not None else present_ids))
    invalid = set(normalize_bls_series_ids(invalid_ids or []))
    required = set(BLS_REQUIRED_SERIES_IDS)
    return {
        "required": list(BLS_REQUIRED_SERIES_IDS),
        "present": _ordered(present),
        "missing": _ordered(required - present),
        "invalid": _ordered(invalid),
        "materialized": _ordered(materialized),
    }


def bls_required_series_status_from_macro_series(series: Iterable[Any]) -> dict[str, list[str]]:
    present: list[str] = []
    invalid: list[str] = []
    for item in series:
        series_id = _series_id_from_item(item)
        if not series_id:
            continue
        present.append(series_id)
        if _value_missing(_get(item, "value")):
            invalid.append(series_id)
    return bls_required_series_status(present, invalid_ids=invalid)


def bls_required_series_status_from_macro_snapshot(snapshot: dict[str, Any]) -> dict[str, list[str]]:
    present: list[str] = []
    invalid: list[str] = []
    for key, item in _flatten_snapshot(snapshot):
        series_id = _series_id_from_item(item, fallback_key=key)
        if not series_id:
            continue
        present.append(series_id)
        if _value_missing(item.get("value")) and _value_missing(item.get("latest_released_value")):
            invalid.append(series_id)
    return bls_required_series_status(present, materialized_ids=present, invalid_ids=invalid)


def bls_required_series_status_from_facts(facts: Iterable[dict[str, Any]], *, include_metrics: bool = False) -> dict[str, list[str]]:
    present: list[str] = []
    invalid: list[str] = []
    for fact in facts:
        raw = fact.get("raw_payload") if isinstance(fact.get("raw_payload"), dict) else {}
        if fact.get("fact_type") == "official_macro_latest" or raw.get("series_id"):
            candidates = (
                raw.get("series_id"),
                fact.get("category"),
                fact.get("event_name"),
                raw.get("name"),
                fact.get("fact_key"),
            )
            series_id = next((normalized for value in candidates if (normalized := normalize_bls_series_id(value))), None)
            if series_id:
                present.append(series_id)
                if _value_missing(fact.get("value")) and _value_missing(raw.get("value")):
                    invalid.append(series_id)
        if include_metrics:
            for metric in raw.get("metrics") or []:
                if not isinstance(metric, dict):
                    continue
                metric_id = _series_id_from_item(metric)
                if not metric_id:
                    continue
                present.append(metric_id)
                if _value_missing(metric.get("actual")) and _value_missing(metric.get("previous")) and _value_missing(metric.get("value")):
                    invalid.append(metric_id)
    return bls_required_series_status(present, materialized_ids=present, invalid_ids=invalid)


def required_macro_saved_but_missing_from_snapshot(db_present_ids: Iterable[Any], snapshot_present_ids: Iterable[Any]) -> list[str]:
    db_present = set(normalize_bls_series_ids(db_present_ids))
    snapshot_present = set(normalize_bls_series_ids(snapshot_present_ids))
    return _ordered(set(BLS_REQUIRED_SERIES_IDS).intersection(db_present) - snapshot_present)


def _series_id_from_item(item: Any, *, fallback_key: Any = None) -> str | None:
    candidates = (
        fallback_key,
        _get(item, "series_id"),
        _get(item, "category"),
        _get(item, "metric_id"),
        _get(item, "metric"),
        _get(item, "label"),
        _get(item, "name"),
        _get(item, "event_name"),
    )
    return next((normalized for value in candidates if (normalized := normalize_bls_series_id(value))), None)


def _flatten_snapshot(snapshot: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    output: list[tuple[str, dict[str, Any]]] = []
    for value in snapshot.values():
        if not isinstance(value, dict):
            continue
        for key, item in value.items():
            if isinstance(item, dict):
                output.append((str(key), item))
    return output


def _ordered(ids: Iterable[str]) -> list[str]:
    values = set(ids)
    return [series_id for series_id in BLS_REQUIRED_SERIES_IDS if series_id in values]


def _get(item: Any, key: str) -> Any:
    if isinstance(item, dict):
        return item.get(key)
    return getattr(item, key, None)


def _value_missing(value: Any) -> bool:
    return value is None or value == ""
