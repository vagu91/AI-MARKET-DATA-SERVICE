import json
import logging
import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from pydantic import BaseModel, Field

from app.core.config import Settings
from app.models.common import ProviderType
from app.providers.base import redact_sensitive
from app.providers.calendar_utils import REQUEST_HEADERS

logger = logging.getLogger(__name__)


class EnrichmentItem(BaseModel):
    name: str
    country: str = "US"
    category: str
    date: str
    time_utc: datetime | None = None
    actual: Any | None = None
    forecast: Any | None = None
    previous: Any | None = None
    consensus: Any | None = None
    source: str
    source_url: str
    provider_type: ProviderType = ProviderType.SCRAPER
    retrieved_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    reliability: float = Field(default=0.0, ge=0.0, le=1.0)
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class CalendarEnrichmentProvider:
    source: str
    source_url: str
    reliability: float
    provider_type = ProviderType.SCRAPER

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @property
    def enabled(self) -> bool:
        return True

    async def fetch(
        self,
        country: str,
        start: datetime,
        end: datetime,
    ) -> tuple[list[EnrichmentItem], list[str]]:
        if not self.settings.enable_event_enrichment_scrapers:
            return [], [f"{self.source} enrichment scraper disabled by config"]
        timeout = min(float(self.settings.http_timeout_seconds), 6.0)
        headers = {
            **REQUEST_HEADERS,
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0 Safari/537.36"
            ),
        }
        try:
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                response = await client.get(self.source_url, headers=headers)
                response.raise_for_status()
                if _looks_blocked(response.text):
                    return [], [f"{self.source} provider_unavailable: blocked page or challenge detected"]
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            category = "provider_unavailable" if status in {403, 429} else "provider_failed"
            return [], [f"{self.source} {category}: HTTP {status}"]
        except Exception as exc:
            message = redact_sensitive(str(exc) or f"{self.source} request failed")
            logger.warning(
                "event_enrichment_provider_failed",
                extra={"_provider": self.source, "_error": message},
            )
            return [], [f"{self.source} provider_failed: {message}"]

        items, warnings = self.parse(response.text)
        filtered = [
            item
            for item in items
            if item.country.upper() == country.upper()
            and _date_in_range(item.date, start, end)
        ]
        if not filtered:
            warnings.append(f"{self.source} no enrichment rows for requested window")
        return filtered, _dedupe(warnings)

    def parse(self, text: str) -> tuple[list[EnrichmentItem], list[str]]:
        return parse_calendar_payload(
            text=text,
            source=self.source,
            source_url=self.source_url,
            reliability=self.reliability,
        )


class DailyFxEnrichmentProvider(CalendarEnrichmentProvider):
    source = "DailyFX Economic Calendar"
    reliability = 0.56

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.source_url = settings.dailyfx_calendar_url


class ForexFactoryEnrichmentProvider(CalendarEnrichmentProvider):
    source = "ForexFactory Calendar"
    reliability = 0.5

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.source_url = settings.forex_factory_calendar_url


class InvestingEnrichmentProvider(CalendarEnrichmentProvider):
    source = "Investing Economic Calendar"
    reliability = 0.48

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.source_url = settings.investing_calendar_url


class FXStreetEconomicCalendarProvider(CalendarEnrichmentProvider):
    source = "FXStreet Economic Calendar"
    reliability = 0.54

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.source_url = settings.fxstreet_calendar_url


class MarketWatchEconomicCalendarProvider(CalendarEnrichmentProvider):
    source = "MarketWatch Economic Calendar"
    reliability = 0.52

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.source_url = settings.marketwatch_calendar_url


class YahooEconomicCalendarProvider(CalendarEnrichmentProvider):
    source = "Yahoo Economic Calendar"
    reliability = 0.5

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.source_url = settings.yahoo_economic_calendar_url


class GenericSearchSnippetCalendarProvider(CalendarEnrichmentProvider):
    source = "Generic Search Snippet Calendar"
    reliability = 0.35

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.source_url = settings.generic_search_calendar_url or ""

    @property
    def enabled(self) -> bool:
        return bool(self.source_url and self.settings.enable_aggressive_scraping)


class TargetedSearchEventEnrichmentProvider:
    source = "Targeted Search Event Enrichment"
    provider_type = ProviderType.SEARCH_SNIPPET
    reliability = 0.46

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.source_url = settings.google_news_rss_url
        self.last_queries: list[str] = []
        self.last_match_count = 0
        self.last_no_match_count = 0

    @property
    def enabled(self) -> bool:
        return bool(self.settings.enable_targeted_search_enrichment)

    async def fetch(
        self,
        country: str,
        start: datetime,
        end: datetime,
    ) -> tuple[list[EnrichmentItem], list[str]]:
        return [], []

    async def fetch_for_events(
        self,
        events,
        country: str,
        start: datetime,
        end: datetime,
    ) -> tuple[list[EnrichmentItem], list[str]]:
        self.last_queries = []
        self.last_match_count = 0
        self.last_no_match_count = 0
        if not self.enabled:
            return [], []
        selected = [
            event
            for event in events
            if event.country.upper() == country.upper()
            and str(event.impact).split(".")[-1] == "HIGH"
            and infer_event_category(event.category or event.name).upper()
            in {
                "CPI",
                "CORE CPI",
                "PPI",
                "NFP",
                "GDP",
                "PCE",
                "CORE PCE",
                "FOMC",
                "RETAIL SALES",
                "ISM MANUFACTURING",
                "ISM SERVICES",
                "JOBLESS CLAIMS",
            }
        ][: self.settings.targeted_search_max_events]
        if not selected:
            return [], []

        errors: list[str] = []
        items: list[EnrichmentItem] = []
        timeout = min(float(self.settings.targeted_search_timeout_seconds), 15.0)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            for event in selected:
                matched = False
                for query in targeted_search_queries(event):
                    self.last_queries.append(query)
                    try:
                        response = await client.get(
                            self.source_url,
                            params={
                                "q": query,
                                "hl": "en-US",
                                "gl": "US",
                                "ceid": "US:en",
                            },
                            headers=REQUEST_HEADERS,
                        )
                        response.raise_for_status()
                    except httpx.HTTPStatusError as exc:
                        status = exc.response.status_code
                        category = "provider_unavailable" if status in {403, 429} else "provider_failed"
                        errors.append(f"{self.source} {category}: HTTP {status}")
                        continue
                    except Exception as exc:
                        errors.append(f"{self.source} provider_failed: {exc or 'empty error detail'}")
                        continue
                    candidates = parse_targeted_search_rss(
                        response.text,
                        event=event,
                        query=query,
                        require_source_url=self.settings.targeted_search_require_source_url,
                        recency_days=self.settings.targeted_search_recency_days,
                    )
                    if candidates:
                        items.extend(candidates)
                        matched = True
                        break
                if matched:
                    self.last_match_count += 1
                else:
                    self.last_no_match_count += 1
        return _dedupe_items(items), _dedupe(errors)


class BrowserCalendarEnrichmentProvider(CalendarEnrichmentProvider):
    provider_type = ProviderType.SCRAPER

    @property
    def enabled(self) -> bool:
        return bool(self.settings.enable_browser_scraping)

    async def fetch(
        self,
        country: str,
        start: datetime,
        end: datetime,
    ) -> tuple[list[EnrichmentItem], list[str]]:
        if not self.enabled:
            return [], []
        try:
            from playwright.async_api import async_playwright
        except Exception as exc:
            return [], [f"{self.source} provider_unavailable: Playwright unavailable: {exc or 'import failed'}"]

        timeout_ms = int(min(float(self.settings.browser_scraping_timeout_seconds), 30.0) * 1000)
        try:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=self.settings.browser_scraping_headless)
                page = await browser.new_page(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/126.0 Safari/537.36"
                    )
                )
                try:
                    await page.goto(self.source_url, wait_until="networkidle", timeout=timeout_ms)
                except Exception:
                    await page.goto(self.source_url, wait_until="domcontentloaded", timeout=timeout_ms)
                html = await page.content()
                await browser.close()
        except Exception as exc:
            return [], [f"{self.source} provider_unavailable: browser scraping failed: {exc or 'empty error detail'}"]

        if _looks_blocked(html):
            return [], [f"{self.source} provider_unavailable: captcha or access challenge detected"]
        items, warnings = self.parse(html)
        filtered = [
            item
            for item in items
            if item.country.upper() == country.upper()
            and _date_in_range(item.date, start, end)
        ]
        if not filtered:
            warnings.append(f"{self.source} no enrichment rows for requested window")
        return filtered, _dedupe(warnings)


class PlaywrightDailyFXProvider(BrowserCalendarEnrichmentProvider):
    source = "Playwright DailyFX"
    reliability = 0.58

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.source_url = settings.dailyfx_calendar_url


class PlaywrightForexFactoryProvider(BrowserCalendarEnrichmentProvider):
    source = "Playwright ForexFactory"
    reliability = 0.52

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.source_url = settings.forex_factory_calendar_url


class PlaywrightInvestingProvider(BrowserCalendarEnrichmentProvider):
    source = "Playwright Investing"
    reliability = 0.5

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.source_url = settings.investing_calendar_url


class ManualEventEnrichmentProvider:
    source = "Manual Event Enrichment"
    reliability = 0.6

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.source_url = str(settings.manual_event_enrichment_path)

    @property
    def enabled(self) -> bool:
        return Path(self.settings.manual_event_enrichment_path).exists()

    async def fetch(
        self,
        country: str,
        start: datetime,
        end: datetime,
    ) -> tuple[list[EnrichmentItem], list[str]]:
        path = Path(self.settings.manual_event_enrichment_path)
        if not path.exists():
            return [], []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            return [], [f"{self.source} provider_failed: {exc or 'invalid manual enrichment file'}"]
        rows = payload.get("events", []) if isinstance(payload, dict) else []
        items: list[EnrichmentItem] = []
        warnings: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                warnings.append(f"{self.source} skipped non-object row")
                continue
            item = _manual_item_from_mapping(row, source_url=str(path))
            if not item:
                warnings.append(f"{self.source} skipped incomplete row")
                continue
            if item.country.upper() == country.upper() and _date_in_range(item.date, start, end):
                items.append(item)
        if rows and not items:
            warnings.append(f"{self.source} no enrichment rows for requested window")
        return items, _dedupe(warnings)


class OpenAIEventEnrichmentProvider:
    source = "OpenAI Event Enrichment"
    provider_type = ProviderType.AI_WEB_FALLBACK
    reliability = 0.0

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.source_url = "OpenAI web fallback"

    @property
    def enabled(self) -> bool:
        return bool(self.settings.enable_openai_event_enrichment)

    async def fetch(
        self,
        country: str,
        start: datetime,
        end: datetime,
    ) -> tuple[list[EnrichmentItem], list[str]]:
        if not self.settings.enable_openai_event_enrichment:
            return [], []
        if not self.settings.openai_api_key:
            return [], [f"{self.source} provider_unavailable: API key is not configured"]
        return [], [
            (
                f"{self.source} no_data_available: scaffold only; "
                "no external enrichment call is implemented"
            )
        ]


def parse_calendar_payload(
    text: str,
    source: str,
    source_url: str,
    reliability: float,
) -> tuple[list[EnrichmentItem], list[str]]:
    items: list[EnrichmentItem] = []
    warnings: list[str] = []
    for payload in _json_payloads(text):
        items.extend(_items_from_json(payload, source, source_url, reliability))
    table_items, table_warnings = _items_from_tables(text, source, source_url, reliability)
    items.extend(table_items)
    warnings.extend(table_warnings)
    items = _dedupe_items(items)
    if not items:
        warnings.append(f"{source} parser found no enrichment rows")
    return items, _dedupe(warnings)


def targeted_search_queries(event) -> list[str]:
    category = infer_event_category(event.category or event.name)
    period = _event_period(event)
    category_upper = category.upper()
    if category_upper in {"CPI", "CORE CPI"}:
        return [
            f"US CPI {period} forecast previous consensus",
            f"United States CPI {period} forecast previous",
            f"US Consumer Price Index {period} consensus forecast",
        ]
    if category_upper == "PPI":
        return [
            f"US PPI {period} forecast previous consensus",
            f"United States Producer Price Index {period} forecast previous",
        ]
    if category_upper == "NFP":
        return [
            f"US nonfarm payrolls {period} forecast previous consensus",
            f"US employment situation {period} forecast unemployment rate",
        ]
    if category_upper == "GDP":
        return [
            f"US GDP {period} advance estimate forecast previous consensus",
            f"United States GDP {period} forecast",
        ]
    if category_upper in {"PCE", "CORE PCE"}:
        return [
            f"US core PCE {period} forecast previous consensus",
            f"US personal income outlays {period} forecast previous",
        ]
    if category_upper == "FOMC":
        return [
            f"FOMC {period} rate decision forecast consensus",
            f"Fed rate decision {period} consensus previous",
        ]
    label = (event.category or event.name).replace("/", " ")
    return [f"US {label} {period} forecast previous consensus"]


def parse_targeted_search_rss(
    text: str,
    event,
    query: str,
    require_source_url: bool,
    recency_days: int,
) -> list[EnrichmentItem]:
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return []
    items: list[EnrichmentItem] = []
    period = _event_period(event)
    for rss_item in root.findall(".//item"):
        title = unescape((rss_item.findtext("title") or "").strip())
        description = _strip_tags(unescape((rss_item.findtext("description") or "").strip()))
        link = (rss_item.findtext("link") or "").strip()
        if require_source_url and not _valid_source_url(link):
            continue
        published = _parse_rss_datetime(rss_item.findtext("pubDate"))
        if published and (datetime.now(UTC) - published).days > recency_days:
            continue
        combined = " ".join(part for part in [title, description] if part)
        if not _period_matches(combined, period):
            continue
        extracted = extract_enrichment_values(combined)
        if not _has_extracted_value(extracted):
            continue
        source = rss_item.findtext("source") or _domain_label(link) or "Targeted Search"
        warnings = []
        available_count = sum(value is not None for key, value in extracted.items() if key != "extracted_text")
        if available_count < 3:
            warnings.append("partial enrichment from targeted search")
        items.append(
            EnrichmentItem(
                name=event.name,
                country=event.country,
                category=infer_event_category(event.category or event.name),
                date=event.date,
                time_utc=event.time_utc,
                actual=extracted.get("actual"),
                forecast=extracted.get("forecast"),
                previous=extracted.get("previous"),
                consensus=extracted.get("consensus"),
                source=source,
                source_url=link,
                provider_type=ProviderType.SEARCH_SNIPPET,
                reliability=0.46 if available_count >= 2 else 0.34,
                warnings=warnings,
                errors=[],
            )
        )
        items[-1].warnings.append(f"extracted_text: {str(extracted.get('extracted_text'))[:220]}")
    return items


def extract_enrichment_values(text: str) -> dict[str, str | None]:
    normalized = " ".join(text.split())
    return {
        "forecast": _extract_first(
            normalized,
            [
                r"\bforecast(?:\s+(?:of|at|for|is|was))?\s+(?P<value>[+-]?\d+(?:\.\d+)?\s?(?:%|K|M|B|bps)?)",
                r"\bexpected\s+to\s+(?:rise|increase|fall|drop|add|show|be)\s+(?P<value>[+-]?\d+(?:\.\d+)?\s?(?:%|K|M|B|bps)?)",
                r"\bexpected\s+(?:at|around|near)\s+(?P<value>[+-]?\d+(?:\.\d+)?\s?(?:%|K|M|B|bps)?)",
            ],
        ),
        "consensus": _extract_first(
            normalized,
            [
                r"\bconsensus(?:\s+(?:at|of|for|is|was))?\s+(?P<value>[+-]?\d+(?:\.\d+)?\s?(?:%|K|M|B|bps)?)",
            ],
        ),
        "previous": _extract_first(
            normalized,
            [
                r"\bprevious(?:\s+(?:reading|was|at|of|is))?\s+(?P<value>[+-]?\d+(?:\.\d+)?\s?(?:%|K|M|B|bps)?)",
                r"\bprior(?:\s+(?:reading|was|at|of|is))?\s+(?P<value>[+-]?\d+(?:\.\d+)?\s?(?:%|K|M|B|bps)?)",
                r"\bprior\s+reading\s+(?:was|at|is)\s+(?P<value>[+-]?\d+(?:\.\d+)?\s?(?:%|K|M|B|bps)?)",
            ],
        ),
        "actual": _extract_first(
            normalized,
            [
                r"\bactual(?:\s+(?:reading|was|at|of|is))?\s+(?P<value>[+-]?\d+(?:\.\d+)?\s?(?:%|K|M|B|bps)?)",
            ],
        ),
        "extracted_text": normalized,
    }


def _extract_first(text: str, patterns: list[str]) -> str | None:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group("value").replace(" ", "")
    return None


def _json_payloads(text: str) -> list[Any]:
    payloads: list[Any] = []
    script_re = re.compile(r"<script[^>]*>(.*?)</script>", re.IGNORECASE | re.DOTALL)
    for match in script_re.finditer(text):
        candidate = match.group(1).strip()
        if not candidate or not any(token in candidate.lower() for token in ("forecast", "previous", "actual")):
            continue
        for json_text in _possible_json_texts(candidate):
            try:
                payloads.append(json.loads(json_text))
            except json.JSONDecodeError:
                continue
    stripped = text.strip()
    if stripped.startswith(("{", "[")):
        try:
            payloads.append(json.loads(stripped))
        except json.JSONDecodeError:
            pass
    return payloads


def _event_period(event) -> str:
    text = f"{event.name} {event.category}"
    quarter = re.search(
        r"\b(Q[1-4]|first quarter|second quarter|third quarter|fourth quarter|1st quarter|2nd quarter|3rd quarter|4th quarter)\s+(\d{4})\b",
        text,
        re.I,
    )
    if quarter:
        label = quarter.group(1)
        year = quarter.group(2)
        aliases = {
            "first quarter": "Q1",
            "second quarter": "Q2",
            "third quarter": "Q3",
            "fourth quarter": "Q4",
            "1st quarter": "Q1",
            "2nd quarter": "Q2",
            "3rd quarter": "Q3",
            "4th quarter": "Q4",
        }
        return f"{aliases.get(label.lower(), label.upper())} {year}"
    month = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b",
        text,
        re.I,
    )
    if month:
        return f"{month.group(1).title()} {month.group(2)}"
    try:
        dt = datetime.fromisoformat(str(event.date))
    except ValueError:
        dt = event.time_utc or datetime.now(UTC)
    return dt.strftime("%B %Y")


def _period_matches(text: str, period: str) -> bool:
    lowered = text.lower()
    period_lower = period.lower()
    if period_lower in lowered:
        return True
    quarter = re.match(r"q([1-4])\s+(\d{4})", period_lower)
    if quarter:
        quarter_words = {
            "1": "first quarter",
            "2": "second quarter",
            "3": "third quarter",
            "4": "fourth quarter",
        }
        return f"{quarter_words[quarter.group(1)]} {quarter.group(2)}" in lowered
    return False


def _valid_source_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _domain_label(value: str) -> str | None:
    parsed = urlparse(value)
    if not parsed.netloc:
        return None
    return parsed.netloc.replace("www.", "")


def _strip_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", " ", value)


def _parse_rss_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _has_extracted_value(values: dict[str, str | None]) -> bool:
    return any(values.get(key) for key in ("forecast", "previous", "consensus", "actual"))


def _possible_json_texts(value: str) -> list[str]:
    if value.startswith(("{", "[")):
        return [value]
    matches = re.findall(r"({.*})", value, flags=re.DOTALL)
    return matches[:3]


def _items_from_json(
    payload: Any,
    source: str,
    source_url: str,
    reliability: float,
) -> list[EnrichmentItem]:
    rows: list[dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            lowered = {str(key).lower(): key for key in node}
            if (
                any(key in lowered for key in ("event", "title", "name", "description"))
                and any(key in lowered for key in ("forecast", "previous", "actual", "consensus"))
            ):
                rows.append(node)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(payload)
    items = []
    for row in rows:
        item = _item_from_mapping(row, source, source_url, reliability)
        if item:
            items.append(item)
    return items


def _items_from_tables(
    text: str,
    source: str,
    source_url: str,
    reliability: float,
) -> tuple[list[EnrichmentItem], list[str]]:
    parser = _TableParser()
    parser.feed(text)
    items: list[EnrichmentItem] = []
    for table in parser.tables:
        if not table:
            continue
        header = [_normalize_header(cell) for cell in table[0]]
        if not any(cell in {"event", "name", "title"} for cell in header):
            continue
        for row in table[1:]:
            mapping = {header[idx]: row[idx] for idx in range(min(len(header), len(row)))}
            item = _item_from_mapping(mapping, source, source_url, reliability)
            if item:
                items.append(item)
    return items, []


def _item_from_mapping(
    row: dict[str, Any],
    source: str,
    source_url: str,
    reliability: float,
) -> EnrichmentItem | None:
    name = _first(row, "event", "name", "title", "description")
    if not name:
        return None
    country = normalize_country(_first(row, "country", "currency", "ccy", "market"))
    if country != "US":
        return None
    date_raw = _first(row, "date", "releaseDate", "release_date", "datetime", "time")
    time_raw = _first(row, "time", "datetime", "dateTime", "timestamp", "date")
    parsed_dt = parse_candidate_datetime(date_raw, time_raw)
    date_value = parsed_dt.date().isoformat() if parsed_dt else _parse_date_only(date_raw)
    if not date_value:
        return None
    return EnrichmentItem(
        name=str(name).strip(),
        country=country,
        category=infer_event_category(str(name)),
        date=date_value,
        time_utc=parsed_dt,
        actual=_clean_value(_first(row, "actual")),
        forecast=_clean_value(_first(row, "forecast")),
        previous=_clean_value(_first(row, "previous", "prior")),
        consensus=_clean_value(_first(row, "consensus")),
        source=source,
        source_url=source_url,
        provider_type=ProviderType.SCRAPER,
        reliability=reliability,
    )


def _manual_item_from_mapping(row: dict[str, Any], source_url: str) -> EnrichmentItem | None:
    name = _first(row, "name", "event", "category") or _first(row, "category")
    category = infer_event_category(str(_first(row, "category") or name or ""))
    date_value = _parse_date_only(_first(row, "date"))
    if not name or not date_value:
        return None
    country = normalize_country(_first(row, "country"))
    source = str(_first(row, "source") or "manual")
    reliability = _float_or_default(_first(row, "reliability"), 0.6)
    return EnrichmentItem(
        name=str(name),
        country=country,
        category=category,
        date=date_value,
        time_utc=parse_candidate_datetime(_first(row, "time_utc", "datetime", "time")),
        actual=_clean_value(_first(row, "actual")),
        forecast=_clean_value(_first(row, "forecast")),
        previous=_clean_value(_first(row, "previous")),
        consensus=_clean_value(_first(row, "consensus")),
        source=source,
        source_url=str(_first(row, "source_url") or source_url),
        provider_type=ProviderType.CACHE,
        reliability=reliability,
    )


def parse_candidate_datetime(date_value: Any, time_value: Any = None) -> datetime | None:
    for value in [date_value, time_value, f"{date_value or ''} {time_value or ''}".strip()]:
        if not value:
            continue
        text = str(value).strip()
        if not text:
            continue
        normalized = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            pass
        for fmt in (
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %I:%M %p",
            "%b %d %Y %H:%M",
            "%b %d %Y %I:%M %p",
            "%B %d %Y %H:%M",
            "%B %d %Y %I:%M %p",
            "%m/%d/%Y %H:%M",
            "%m/%d/%Y %I:%M %p",
        ):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=UTC)
            except ValueError:
                continue
    return None


def infer_event_category(name: str) -> str:
    lowered = name.lower()
    rules = [
        ("Core CPI", ("core cpi", "core consumer price")),
        ("CPI", ("consumer price index", " cpi", "cpi ")),
        ("PPI", ("producer price index", " ppi", "ppi ")),
        ("NFP", ("nonfarm payroll", "payrolls", "employment situation")),
        ("Unemployment Rate", ("unemployment",)),
        ("Jobless Claims", ("initial jobless", "jobless claims")),
        ("Core PCE", ("core pce", "core personal consumption")),
        ("PCE", ("personal consumption expenditures", "pce price", " pce", "pce ")),
        ("GDP", ("gross domestic product", " gdp", "gdp ")),
        ("Retail Sales", ("retail sales",)),
        ("ISM Manufacturing", ("ism manufacturing", "manufacturing pmi")),
        ("ISM Services", ("ism services", "services pmi")),
        ("FOMC", ("fomc", "fed interest rate", "fed rate", "powell", "minutes")),
    ]
    padded = f" {lowered} "
    for category, keywords in rules:
        if any(keyword in padded for keyword in keywords):
            return category
    return "OTHER"


def normalize_country(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"US", "USA", "USD", "UNITED STATES", "UNITED STATES OF AMERICA"}:
        return "US"
    return text or "US"


def _first(row: dict[str, Any], *keys: str) -> Any | None:
    lowered = {str(key).lower().replace(" ", "_"): value for key, value in row.items()}
    for key in keys:
        normalized = key.lower().replace(" ", "_")
        if normalized in lowered and lowered[normalized] not in (None, ""):
            return lowered[normalized]
    return None


def _clean_value(value: Any | None) -> Any | None:
    if value in (None, "", "-", "--", "N/A"):
        return None
    return str(value).strip() if isinstance(value, str) else value


def _parse_date_only(value: Any | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%b %d %Y", "%B %d %Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _float_or_default(value: Any | None, default: float) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _date_in_range(date_value: str, start: datetime, end: datetime) -> bool:
    try:
        event_date = datetime.fromisoformat(date_value).date()
    except ValueError:
        return False
    return start.date() <= event_date <= end.date()


def _looks_blocked(text: str) -> bool:
    lowered = text.lower()
    return any(
        marker in lowered
        for marker in (
            "captcha",
            "access denied",
            "cloudflare",
            "cf-challenge",
            "too many requests",
            "forbidden",
            "enable cookies",
            "checking your browser",
        )
    )


def _dedupe_items(items: list[EnrichmentItem]) -> list[EnrichmentItem]:
    deduped: list[EnrichmentItem] = []
    seen: set[tuple[str, str, str]] = set()
    for item in items:
        key = (item.date, item.name.lower(), item.source)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _dedupe(values: list[str]) -> list[str]:
    deduped = []
    for value in values:
        if value and value not in deduped:
            deduped.append(value)
    return deduped


def _normalize_header(value: str) -> str:
    lowered = value.strip().lower()
    aliases = {
        "event": "event",
        "name": "event",
        "title": "event",
        "currency": "currency",
        "country": "country",
        "time": "time",
        "date": "date",
        "actual": "actual",
        "forecast": "forecast",
        "consensus": "consensus",
        "previous": "previous",
        "prior": "previous",
    }
    for needle, replacement in aliases.items():
        if needle in lowered:
            return replacement
    return lowered.replace(" ", "_")


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._current_table: list[list[str]] | None = None
        self._current_row: list[str] | None = None
        self._current_cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs) -> None:
        lowered = tag.lower()
        if lowered == "table":
            self._current_table = []
        elif lowered == "tr" and self._current_table is not None:
            self._current_row = []
        elif lowered in {"td", "th"} and self._current_row is not None:
            self._current_cell = []

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"td", "th"} and self._current_cell is not None and self._current_row is not None:
            self._current_row.append(" ".join("".join(self._current_cell).split()))
            self._current_cell = None
        elif lowered == "tr" and self._current_row is not None and self._current_table is not None:
            if self._current_row:
                self._current_table.append(self._current_row)
            self._current_row = None
        elif lowered == "table" and self._current_table is not None:
            self.tables.append(self._current_table)
            self._current_table = None

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None:
            self._current_cell.append(data)
