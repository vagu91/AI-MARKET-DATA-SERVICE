from __future__ import annotations

import re
from typing import Any


MISSING_VALUES = {"", "--", "-", "—", "N/A", "NA", "NULL", "NONE"}
MULTIPLIERS = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000, "T": 1_000_000_000_000}
MISSING_VALUES.update({"—", "–"})


def parse_economic_value(value: Any, *, default_unit: str | None = None) -> dict[str, Any]:
    raw = "" if value is None else str(value).strip()
    if raw.upper() in MISSING_VALUES:
        return {"value": None, "raw": raw, "unit": default_unit, "multiplier": 1, "parse_status": "missing"}

    cleaned = raw.strip().replace("*", "")
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    if negative:
        cleaned = cleaned[1:-1].strip()
    unit = default_unit
    percent = "%" in cleaned
    if percent:
        unit = "percent"
        cleaned = cleaned.replace("%", "")

    suffix_match = re.search(r"([KMBT])\s*$", cleaned, flags=re.IGNORECASE)
    multiplier = 1
    if suffix_match:
        suffix = suffix_match.group(1).upper()
        multiplier = MULTIPLIERS[suffix]
        cleaned = cleaned[: suffix_match.start()].strip()
        unit = unit or suffix

    cleaned = re.sub(r"[$€£]", "", cleaned).strip()
    cleaned = _normalize_decimal(cleaned)
    if not cleaned:
        return {"value": None, "raw": raw, "unit": unit, "multiplier": multiplier, "parse_status": "missing"}
    try:
        parsed = float(cleaned) * multiplier
    except ValueError:
        return {"value": None, "raw": raw, "unit": unit, "multiplier": multiplier, "parse_status": "invalid"}
    if negative:
        parsed = -parsed
    return {"value": parsed, "raw": raw, "unit": unit, "multiplier": multiplier, "parse_status": "parsed"}


def parse_int_value(value: Any) -> int | None:
    parsed = parse_economic_value(value)
    if parsed["parse_status"] != "parsed" or parsed["value"] is None:
        return None
    return int(parsed["value"])


def _normalize_decimal(value: str) -> str:
    value = value.replace(" ", "")
    if "," in value and "." in value:
        if value.rfind(",") > value.rfind("."):
            return value.replace(".", "").replace(",", ".")
        return value.replace(",", "")
    if "," in value:
        pieces = value.split(",")
        if len(pieces[-1]) in {1, 2}:
            return value.replace(".", "").replace(",", ".")
        return value.replace(",", "")
    return value.replace(",", "")
