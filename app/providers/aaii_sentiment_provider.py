from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS

AAII_SENTIMENT_URL = "https://www.aaii.com/sentimentsurvey"


class AaiiSentimentProvider:
    source = "AAII Sentiment Survey"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_aaii_sentiment:
            return _status("disabled", "aaii_sentiment_disabled", started, diagnostics=_diagnostics())
        diagnostics = _diagnostics()
        response_text = ""
        try:
            async with httpx.AsyncClient(timeout=self.settings.timeout_sentiment_seconds) as client:
                response = await asyncio.wait_for(
                    client.get(
                        AAII_SENTIMENT_URL,
                        headers=REQUEST_HEADERS,
                        timeout=min(float(self.settings.timeout_sentiment_seconds), 8.0),
                    ),
                    timeout=min(float(self.settings.timeout_sentiment_seconds), 8.0),
                )
                diagnostics["http_status_code"] = response.status_code
                response.raise_for_status()
                response_text = response.text
        except TimeoutError:
            diagnostics["http_error"] = "timeout"
            return _status("provider_timeout", "AAII request timed out", started, diagnostics=diagnostics)
        except Exception as exc:
            diagnostics["http_error"] = str(exc) or type(exc).__name__
            return _status("provider_failed", str(exc) or "AAII request failed", started, diagnostics=diagnostics)

        diagnostics["http_blocked"] = is_aaii_blocked_html(response_text)
        parsed, parse_diagnostics = _parse_aaii_sentiment_with_diagnostics(response_text)
        diagnostics.update(parse_diagnostics)
        if not parsed:
            browser_html, browser_diagnostics = await fetch_aaii_with_browser(
                AAII_SENTIMENT_URL,
                timeout_seconds=min(float(self.settings.timeout_sentiment_seconds), 12.0),
            )
            diagnostics.update(browser_diagnostics)
            if browser_html:
                parsed, parse_diagnostics = _parse_aaii_sentiment_with_diagnostics(browser_html)
                diagnostics.update(parse_diagnostics)
                diagnostics["browser_success"] = bool(parsed)
                if not parsed and not diagnostics.get("browser_error"):
                    diagnostics["browser_error"] = (
                        "browser_page_loaded_but_sentiment_selectors_not_found"
                        if not diagnostics.get("selector_found")
                        else "browser_page_loaded_but_sentiment_values_not_parsed"
                    )
            elif not diagnostics.get("browser_error"):
                diagnostics["browser_error"] = "browser_returned_no_html"
            if not parsed:
                status = "access_restricted" if diagnostics.get("http_blocked") else "not_found"
                reason = diagnostics.get("browser_error") or "AAII sentiment percentages were not visible in the public page."
                return _status(status, str(reason), started, diagnostics=diagnostics)
        total = parsed["bullish_pct"] + parsed["neutral_pct"] + parsed["bearish_pct"]
        if not 98.5 <= total <= 101.5:
            return _status("rejected_invalid_sentiment_total", f"AAII percentages total {total:.2f}, outside tolerance.", started, diagnostics=diagnostics)
        survey_date = _parse_date(parsed.get("survey_date"))
        if survey_date and survey_date > datetime.now(UTC).date():
            return _status("rejected_future_survey_date", "AAII survey date is in the future.", started, diagnostics=diagnostics)
        now = datetime.now(UTC)
        return {
            "status": "found",
            **parsed,
            "bull_bear_spread": round(parsed["bullish_pct"] - parsed["bearish_pct"], 2),
            "source": "AAII",
            "source_url": AAII_SENTIMENT_URL,
            "retrieved_at": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "valid_until": (now + timedelta(days=7)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "attempted_sources": [AAII_SENTIMENT_URL],
            "reason": None,
            "next_retry_at": (now + timedelta(days=7)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "reliability": 0.82,
            "duration_ms": int((now - started).total_seconds() * 1000),
            "diagnostics": diagnostics,
            "warnings": [],
            "errors": [],
        }


def parse_aaii_sentiment(text: str) -> dict[str, Any] | None:
    parsed, _ = _parse_aaii_sentiment_with_diagnostics(text)
    return parsed


def _parse_aaii_sentiment_with_diagnostics(text: str) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    diagnostics = {
        "selector_found": bool(re.search(r'class=["\'][^"\']*(results|weekending)[^"\']*["\']', text, re.IGNORECASE)),
        "weekly_rows_found": 0,
        "historical_averages_found": False,
        "parse_source": None,
    }
    dom = _parse_results_dom(text)
    if dom:
        diagnostics["weekly_rows_found"] = len(dom.get("latest_four_weeks") or [dom])
        diagnostics["historical_averages_found"] = bool(dom.get("historical_averages"))
        diagnostics["parse_source"] = "section.results"
        return dom, diagnostics
    chart = _parse_chart_data(text)
    if chart:
        diagnostics["weekly_rows_found"] = len(chart.get("latest_four_weeks") or [chart])
        diagnostics["historical_averages_found"] = bool(chart.get("historical_averages"))
        diagnostics["parse_source"] = "window.dataChart5"
        return chart, diagnostics
    stripped = re.sub(r"<[^>]+>", " ", text)
    stripped = re.sub(r"\s+", " ", stripped)
    values: dict[str, float] = {}
    for label in ("Bullish", "Neutral", "Bearish"):
        match = re.search(label + r"[^0-9]{0,80}([0-9]+(?:\.[0-9]+)?)\s*%", stripped, re.IGNORECASE)
        if match:
            values[label.lower()] = float(match.group(1))
    if not {"bullish", "neutral", "bearish"}.issubset(values):
        return None, diagnostics
    survey_date = None
    date_match = re.search(r"([A-Z][a-z]+ \d{1,2}, 20\d{2})", stripped)
    if date_match:
        try:
            survey_date = datetime.strptime(date_match.group(1), "%B %d, %Y").date().isoformat()
        except ValueError:
            survey_date = None
    if survey_date is None:
        numeric_date = re.search(r"\b(\d{1,2}/\d{1,2}/20\d{2})\b", stripped)
        if numeric_date:
            try:
                survey_date = datetime.strptime(numeric_date.group(1), "%m/%d/%Y").date().isoformat()
            except ValueError:
                survey_date = None
    averages = _historical_averages(stripped)
    diagnostics["weekly_rows_found"] = 1
    diagnostics["historical_averages_found"] = bool(averages)
    diagnostics["parse_source"] = "visible_text"
    return {
        "survey_date": survey_date,
        "bullish_pct": values["bullish"],
        "neutral_pct": values["neutral"],
        "bearish_pct": values["bearish"],
        "historical_averages": averages,
    }, diagnostics


async def fetch_aaii_with_browser(url: str, *, timeout_seconds: float) -> tuple[str | None, dict[str, Any]]:
    diagnostics: dict[str, Any] = {
        "browser_attempted": True,
        "browser_success": False,
        "browser_error": None,
        "browser_closed": False,
        "selector_found": False,
    }
    browser = None
    playwright = None
    try:
        from playwright.async_api import async_playwright  # type: ignore
    except Exception as exc:
        diagnostics["browser_error"] = f"playwright_unavailable:{exc or type(exc).__name__}"
        diagnostics["browser_closed"] = True
        return None, diagnostics
    try:
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=int(timeout_seconds * 1000))
        try:
            await page.wait_for_selector("section.results, .weekending", timeout=int(min(timeout_seconds, 8.0) * 1000))
            diagnostics["selector_found"] = True
        except Exception:
            diagnostics["selector_found"] = False
        html = await page.content()
        diagnostics["browser_success"] = bool(html)
        return html, diagnostics
    except Exception as exc:
        diagnostics["browser_error"] = str(exc) or type(exc).__name__
        return None, diagnostics
    finally:
        try:
            if browser is not None:
                await browser.close()
        finally:
            if playwright is not None:
                await playwright.stop()
            diagnostics["browser_closed"] = True


def is_aaii_blocked_html(text: str) -> bool:
    lowered = text.lower()
    tokens = (
        "incapsula",
        "visid_incap",
        "_incapsula_resource",
        "request unsuccessful",
        "incident id",
        "captcha",
        "access denied",
        "login",
        "sign in",
        "member login",
    )
    return any(token in lowered for token in tokens)


def _parse_results_dom(text: str) -> dict[str, Any] | None:
    sections = [
        match.group(1)
        for match in re.finditer(
            r"<section[^>]*class=[\"'][^\"']*results[^\"']*[\"'][^>]*>(.*?)</section>",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
    ] or [text]
    rows: list[dict[str, Any]] = []
    for section in sections:
        starts = [match.start() for match in re.finditer(r"class=[\"'][^\"']*weekending[^\"']*[\"']", section, flags=re.IGNORECASE)]
        if not starts and "weekending" in section.lower():
            starts = [0]
        for index, start in enumerate(starts):
            end = starts[index + 1] if index + 1 < len(starts) else min(len(section), start + 4000)
            row = _parse_weekending_block(section[start:end])
            if row:
                rows.append(row)
    unique: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("survey_date") or row)
        unique[key] = row
    rows = list(unique.values())
    if not rows:
        return None
    rows.sort(key=lambda item: item.get("survey_date") or "", reverse=True)
    latest = rows[0]
    latest["latest_four_weeks"] = rows[:4]
    averages = _historical_averages(_visible_text(text))
    latest["historical_averages"] = averages
    return latest


def _parse_weekending_block(block: str) -> dict[str, Any] | None:
    stripped = _visible_text(block)
    values: dict[str, float] = {}
    for label in ("Bullish", "Neutral", "Bearish"):
        match = re.search(label + r"[^0-9]{0,160}([0-9]+(?:\.[0-9]+)?)\s*%", stripped, re.IGNORECASE)
        if match:
            values[label.lower()] = float(match.group(1))
    if not {"bullish", "neutral", "bearish"}.issubset(values):
        return None
    survey_date = None
    for pattern, fmt in (
        (r"([A-Z][a-z]+ \d{1,2}, 20\d{2})", "%B %d, %Y"),
        (r"\b(\d{1,2}/\d{1,2}/20\d{2})\b", "%m/%d/%Y"),
        (r"\b(20\d{2}-\d{2}-\d{2})\b", "%Y-%m-%d"),
    ):
        match = re.search(pattern, stripped)
        if match:
            try:
                survey_date = datetime.strptime(match.group(1), fmt).date().isoformat()
                break
            except ValueError:
                continue
    return {
        "survey_date": survey_date,
        "bullish_pct": values["bullish"],
        "neutral_pct": values["neutral"],
        "bearish_pct": values["bearish"],
    }


def _parse_chart_data(text: str) -> dict[str, Any] | None:
    match = re.search(r"window\.dataChart5\s*=\s*(\[.*?\]);", text, flags=re.DOTALL)
    if not match:
        return None
    try:
        rows = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None
    parsed = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        bullish = _num(row.get("bullish"))
        neutral = _num(row.get("neutral"))
        bearish = _num(row.get("bearish"))
        if bullish is None or neutral is None or bearish is None:
            continue
        parsed.append(
            {
                "survey_date": _date_text(row.get("date_") or row.get("date")),
                "bullish_pct": bullish,
                "neutral_pct": neutral,
                "bearish_pct": bearish,
                "bull_bear_spread": _num(row.get("spread")),
            }
        )
    if not parsed:
        return None
    parsed.sort(key=lambda item: item.get("survey_date") or "", reverse=True)
    latest = parsed[0]
    latest["latest_four_weeks"] = parsed[:4]
    return latest


def _visible_text(text: str) -> str:
    stripped = re.sub(r"<script\b.*?</script>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    stripped = re.sub(r"<style\b.*?</style>", " ", stripped, flags=re.IGNORECASE | re.DOTALL)
    stripped = re.sub(r"<[^>]+>", " ", stripped)
    return re.sub(r"\s+", " ", stripped)


def _historical_averages(text: str) -> dict[str, float | None]:
    if "historical" not in text.lower():
        return {}
    output: dict[str, float | None] = {}
    for label in ("Bullish", "Neutral", "Bearish"):
        matches = re.findall(label + r"[^0-9]{0,80}([0-9]+(?:\.[0-9]+)?)\s*%", text, re.IGNORECASE)
        output[label.lower()] = float(matches[-1]) if len(matches) > 1 else None
    return {key: value for key, value in output.items() if value is not None}


def _num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace("%", "").replace(",", ""))
    except ValueError:
        return None


def _date_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value)
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return text


def _parse_date(value: Any):
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except ValueError:
        return None


def _diagnostics() -> dict[str, Any]:
    return {
        "http_attempted": True,
        "http_status_code": None,
        "http_blocked": False,
        "http_error": None,
        "browser_attempted": False,
        "browser_success": False,
        "browser_error": None,
        "browser_closed": False,
        "selector_found": False,
        "weekly_rows_found": 0,
        "historical_averages_found": False,
        "parse_source": None,
    }


def _status(status: str, reason: str, started: datetime, *, diagnostics: dict[str, Any] | None = None) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "status": status,
        "survey_date": None,
        "bullish_pct": None,
        "neutral_pct": None,
        "bearish_pct": None,
        "bull_bear_spread": None,
        "source": "AAII",
        "source_url": AAII_SENTIMENT_URL,
        "retrieved_at": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "valid_until": (now + timedelta(hours=6)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "attempted_sources": [AAII_SENTIMENT_URL],
        "reason": reason,
        "next_retry_at": (now + timedelta(hours=6)).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "reliability": 0.0,
        "duration_ms": int((now - started).total_seconds() * 1000),
        "diagnostics": diagnostics or _diagnostics(),
        "warnings": [reason],
        "errors": [] if status in {"not_found", "access_restricted"} else [reason],
    }
