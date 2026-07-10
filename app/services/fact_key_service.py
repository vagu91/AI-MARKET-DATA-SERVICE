from __future__ import annotations

import hashlib
import re
from datetime import date


def slug(value: str | None) -> str:
    text = (value or "unknown").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_") or "unknown"


class FactKeyService:
    def macro_event_key(
        self,
        *,
        country: str | None,
        category: str | None,
        event_date: str | date | None,
        event_name: str | None,
        period: str | None = None,
        fact_type: str = "macro_event_enrichment",
    ) -> str:
        date_part = event_date.isoformat() if isinstance(event_date, date) else (event_date or "unknown-date")
        name_part = slug(f"{event_name or category} {period or ''}")
        return f"{(country or 'US').upper()}:{(category or 'UNKNOWN').upper()}:{date_part}:{name_part}:{fact_type}"

    def event_key(self, *, event_id: str | None, country: str, date_value: str, name: str) -> str:
        if event_id:
            return event_id
        return f"{country.upper()}:{date_value}:{slug(name)}"

    def news_key(self, *, title: str, source_url: str) -> str:
        normalized = f"{slug(title)}|{source_url.strip().lower()}"
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
