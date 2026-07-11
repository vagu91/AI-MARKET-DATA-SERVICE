from __future__ import annotations

import html
import re
import unicodedata
from typing import Any

MOJIBAKE_MARKERS = ("Ã", "Â", "â€", "â€™", "â€œ", "â€˜", "â€“", "â€”")
TEXT_KEYS = {
    "name",
    "holiday_name",
    "title",
    "summary",
    "content_snippet",
    "source",
    "company",
    "company_name",
    "event_name",
    "description",
    "question",
}


def normalize_text(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = html.unescape(value)
    for _ in range(3):
        fixed = _fix_mojibake(text)
        if fixed == text:
            break
        text = fixed
    text = unicodedata.normalize("NFC", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_payload_text(value: Any) -> Any:
    if isinstance(value, list):
        return [normalize_payload_text(item) for item in value]
    if isinstance(value, dict):
        output: dict[str, Any] = {}
        for key, item in value.items():
            if key in {"url", "source_url", "canonical_url", "aggregator_url"}:
                output[key] = item
            elif key in TEXT_KEYS:
                output[key] = normalize_text(item)
            else:
                output[key] = normalize_payload_text(item)
        return output
    return normalize_text(value) if isinstance(value, str) else value


def _fix_mojibake(text: str) -> str:
    if not any(marker in text for marker in MOJIBAKE_MARKERS):
        return text
    candidates = [text]
    for source_encoding in ("latin1", "cp1252"):
        try:
            candidates.append(text.encode(source_encoding, errors="strict").decode("utf-8", errors="strict"))
        except UnicodeError:
            continue
    candidates.extend(
        [
            text.replace("Ã¢â‚¬â„¢", "'").replace("Ã¢â‚¬Ëœ", "'").replace("Ã¢â‚¬Å“", '"').replace("Ã¢â‚¬ï¿½", '"'),
            text.replace("â€™", "'").replace("â€˜", "'").replace("â€œ", '"').replace("â€�", '"').replace("â€“", "-").replace("â€”", "-"),
        ]
    )
    return min(candidates, key=_mojibake_score)


def _mojibake_score(text: str) -> int:
    return sum(text.count(marker) for marker in MOJIBAKE_MARKERS) + text.count("\ufffd") * 3
