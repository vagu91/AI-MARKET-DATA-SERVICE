import csv
from io import StringIO
from typing import Any

from app.core.redaction import redact_sensitive
from app.providers.base import ProviderError


RATE_OR_ERROR_KEYS = ("Note", "Information", "Error Message")


def alpha_vantage_message(payload: dict[str, Any]) -> str | None:
    for key in RATE_OR_ERROR_KEYS:
        value = payload.get(key)
        if value:
            return redact_sensitive(f"Alpha Vantage {key}: {value}")
    return None


def parse_percent(value: str | float | int | None) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, int | float):
        return float(value)
    cleaned = str(value).replace("%", "").replace(",", "").strip()
    try:
        parsed = float(cleaned)
    except ValueError:
        return None
    normalized = parsed * 100.0 if 0 < parsed <= 1 and "%" not in str(value) else parsed
    return round(normalized, 6)


def parse_float(value: str | float | int | None) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, int | float):
        return float(value)
    cleaned = str(value).replace("%", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_int(value: str | int | None) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value).replace(",", "").strip())
    except ValueError:
        return None


def ensure_alpha_payload_ok(payload: dict[str, Any]) -> None:
    message = alpha_vantage_message(payload)
    if message:
        raise ProviderError(message)


def csv_rows(text: str) -> list[dict[str, str]]:
    reader = csv.DictReader(StringIO(text))
    return [row for row in reader if any(row.values())]
