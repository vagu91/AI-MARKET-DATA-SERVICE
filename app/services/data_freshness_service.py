from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.config import Settings


@dataclass(frozen=True)
class FreshnessResult:
    usable: bool
    cache_status: str
    warnings: list[str]
    stale: bool = False


def parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


class DataFreshnessService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def evaluate(self, row: dict[str, Any], *, allow_stale: bool | None = None) -> FreshnessResult:
        allow_stale = self.settings.allow_stale_facts if allow_stale is None else allow_stale
        warnings: list[str] = []
        valid_until = parse_datetime(row.get("valid_until"))
        if valid_until is None:
            retrieved_at = parse_datetime(row.get("retrieved_at"))
            if retrieved_at is None:
                return FreshnessResult(False, "miss", ["missing_valid_until_and_retrieved_at"], stale=True)
            valid_until = retrieved_at + timedelta(hours=self.settings.default_fact_ttl_hours)
            warnings.append("valid_until_missing_default_ttl_used")
        if datetime.now(UTC) < valid_until:
            return FreshnessResult(True, "hit", warnings)
        warnings.append("stale_fact")
        if allow_stale:
            return FreshnessResult(True, "expired", warnings, stale=True)
        return FreshnessResult(False, "expired", warnings, stale=True)

    def macro_valid_until(self, event: Any) -> str | None:
        time_utc = getattr(event, "time_utc", None) if not isinstance(event, dict) else event.get("time_utc")
        if time_utc:
            parsed = parse_datetime(time_utc)
            return parsed.isoformat() if parsed else str(time_utc)
        date_value = getattr(event, "date", None) if not isinstance(event, dict) else event.get("date")
        if date_value:
            return f"{date_value}T23:59:59+00:00"
        return None

    def news_valid_until(
        self,
        *,
        published_at: str | None,
        retrieved_at: str,
        topics: list[str] | None = None,
    ) -> str:
        fast_topics = {"fed", "fomc", "cpi", "ppi", "nfp", "pce", "gdp", "risk_event", "inflation"}
        ttl_hours = 6 if fast_topics.intersection({topic.lower() for topic in topics or []}) else self.settings.default_news_ttl_hours
        base = parse_datetime(published_at) or parse_datetime(retrieved_at) or datetime.now(UTC)
        return (base + timedelta(hours=ttl_hours)).replace(microsecond=0).isoformat()

    def next_refresh_at(self, valid_until: str | None) -> str | None:
        parsed = parse_datetime(valid_until)
        return parsed.replace(microsecond=0).isoformat() if parsed else None
