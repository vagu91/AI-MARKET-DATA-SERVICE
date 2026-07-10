from __future__ import annotations

import re
from typing import Any

SENSITIVE_QUERY_RE = re.compile(
    r"(?i)(api_key|apikey|userid|registrationkey|alpha_vantage_api_key|ai_market_alpha_vantage_api_key|openai_api_key|ai_market_openai_api_key|authorization|bearer|codex_token|auth\.json)=([^&\s]+)",
)
SENSITIVE_PHRASE_RE = re.compile(
    r"(?i)((?:api\s*key|apikey|token|secret)(?:\s+(?:as|is|was|detected\s+as))?\s*[:=]?\s+)([A-Za-z0-9_\-]{8,})"
)


def redact_sensitive(value: str) -> str:
    redacted = SENSITIVE_QUERY_RE.sub(lambda match: f"{match.group(1)}=<redacted>", value)
    return SENSITIVE_PHRASE_RE.sub(lambda match: f"{match.group(1)}<redacted>", redacted)


def redact_payload(value: Any) -> Any:
    if isinstance(value, str):
        return redact_sensitive(value)
    if isinstance(value, list):
        return [redact_payload(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_payload(item) for item in value)
    if isinstance(value, dict):
        return {key: redact_payload(item) for key, item in value.items()}
    return value
