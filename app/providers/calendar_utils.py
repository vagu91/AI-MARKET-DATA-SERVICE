import hashlib
import re
from datetime import UTC, date, datetime, time
from html.parser import HTMLParser
from zoneinfo import ZoneInfo

from app.models.common import Impact

EASTERN_TZ = ZoneInfo("America/New_York")

HIGH_IMPACT_TERMS = {
    "consumer price index": "CPI",
    "core cpi": "Core CPI",
    "producer price index": "PPI",
    "employment situation": "NFP / Nonfarm Payrolls",
    "nonfarm payroll": "NFP / Nonfarm Payrolls",
    "unemployment rate": "Unemployment Rate",
    "jobless claims": "Jobless Claims",
    "personal income and outlays": "PCE",
    "pce": "PCE",
    "gross domestic product": "GDP",
    "gdp": "GDP",
    "ism manufacturing": "ISM Manufacturing",
    "ism services": "ISM Services",
    "retail sales": "Retail Sales",
    "fomc": "FOMC",
    "fomc minutes": "FOMC Minutes",
    "powell": "Powell speech",
    "chair": "Fed Chair speech",
}

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; AI-MARKET-DATA-SERVICE/0.1; "
        "+https://localhost)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        value = " ".join(data.split())
        if value:
            self.parts.append(value)


def html_text_lines(html: str) -> list[str]:
    parser = TextExtractor()
    parser.feed(html)
    return [part.strip() for part in parser.parts if part.strip()]


def event_id(prefix: str, *parts: object) -> str:
    digest = hashlib.sha256(":".join(str(part) for part in parts).encode("utf-8")).hexdigest()
    return f"{prefix}-{digest[:16]}"


def classify_event(name: str) -> tuple[Impact, str, bool]:
    lower = name.lower()
    for term, category in HIGH_IMPACT_TERMS.items():
        if term in lower:
            return Impact.HIGH, category, True
    return Impact.MEDIUM, "Economic Release", False


def parse_time(value: str) -> time | None:
    match = re.search(r"(\d{1,2}):(\d{2})\s*([AP]\.?M\.?)", value, re.IGNORECASE)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    marker = match.group(3).lower()
    if marker.startswith("p") and hour != 12:
        hour += 12
    if marker.startswith("a") and hour == 12:
        hour = 0
    return time(hour=hour, minute=minute)


def eastern_to_utc(local_date: date, local_time: time | None) -> tuple[datetime | None, datetime | None]:
    if local_time is None:
        return None, None
    local_dt = datetime.combine(local_date, local_time, tzinfo=EASTERN_TZ)
    return local_dt.astimezone(UTC), local_dt
