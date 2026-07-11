import logging
import re
from datetime import UTC, datetime, timedelta

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.models.common import ProviderType
from app.models.events import EconomicEvent, EventEnrichment
from app.providers.event_enrichment import (
    CalendarEnrichmentProvider,
    EnrichmentItem,
    infer_event_category,
)

logger = logging.getLogger(__name__)

CACHE_PREFIX = "macro_event_enrichment:v1"
CACHE_PREFIX_V2 = "macro_event_enrichment:v2"
CACHE_MERGED_PREFIX_V2 = "macro_event_enrichment_merged:v2"
CACHE_PREFIX_V3 = "macro_event_enrichment:v3"
PROVIDER_FAILURE_CACHE_PREFIX = "event_enrichment_provider_failure:v1"
PROVIDER_FAILURE_TTL_MINUTES = 45
TIME_WINDOW_MINUTES = 15
ENRICHABLE_CATEGORIES = {
    "CPI",
    "CORE CPI",
    "PPI",
    "NFP",
    "NONFARM PAYROLLS",
    "GDP",
    "PCE",
    "CORE PCE",
    "FOMC",
    "RETAIL SALES",
    "ISM MANUFACTURING",
    "ISM SERVICES",
    "JOBLESS CLAIMS",
    "JOLTS",
}

CATEGORY_ALIASES = {
    "CORE CPI": {"CORE CPI", "CPI"},
    "CPI": {"CPI", "CORE CPI"},
    "PPI": {"PPI"},
    "NFP": {"NFP", "NONFARM PAYROLLS"},
    "NONFARM PAYROLLS": {"NFP", "NONFARM PAYROLLS"},
    "UNEMPLOYMENT RATE": {"UNEMPLOYMENT RATE"},
    "JOBLESS CLAIMS": {"JOBLESS CLAIMS"},
    "PCE": {"PCE", "CORE PCE"},
    "CORE PCE": {"CORE PCE", "PCE"},
    "GDP": {"GDP"},
    "RETAIL SALES": {"RETAIL SALES"},
    "ISM MANUFACTURING": {"ISM MANUFACTURING"},
    "ISM SERVICES": {"ISM SERVICES"},
    "FOMC": {"FOMC"},
}


class EventEnrichmentService:
    def __init__(
        self,
        cache: ProviderCacheProtocol,
        providers: list[CalendarEnrichmentProvider],
    ) -> None:
        self.cache = cache
        self.providers = providers

    async def enrich_events(
        self,
        events: list[EconomicEvent],
        country: str,
        start: datetime,
        end: datetime,
    ) -> tuple[list[EconomicEvent], dict[str, object]]:
        if not events:
            return [], {
                "enriched_count": 0,
                "missing_enrichment_count": 0,
                "provider_statuses": [],
                "providers_attempted": 0,
                "providers_succeeded": 0,
                "providers_failed": 0,
                "provider_errors": [],
                "fallback_used": False,
                "cache_used": False,
                "browser_scraping_enabled": False,
                "browser_scraping_used": False,
                "structured_sources_blocked": False,
                "targeted_search_enabled": False,
                "targeted_search_used": False,
                "targeted_search_queries": [],
                "targeted_search_matches": 0,
                "targeted_search_no_match_count": 0,
            }

        events_to_enrich = _events_to_enrich(events, _first_settings(self.providers))
        candidates, provider_errors, fallback_used, provider_statuses = await self._load_live_candidates(
            country,
            start,
            end,
            events_to_enrich,
        )
        candidates_from_cache = False
        if not _useful_candidates(candidates):
            cached = self._load_cached_candidates(country, start, end)
            if _useful_candidates(cached):
                candidates = cached
                candidates_from_cache = True
                fallback_used = True

        enriched_events: list[EconomicEvent] = []
        enriched_count = 0
        cache_used = False
        no_live_candidates = not any(status["status"] == "succeeded" for status in provider_statuses)
        provider_unavailable_warning = _provider_unavailable_warning(provider_statuses)
        enrichable_ids = {event.event_id for event in events_to_enrich}
        for event in events:
            updated = event.model_copy(deep=True)
            if event.event_id not in enrichable_ids:
                enrichment = EventEnrichment(warnings=["no_data_available: enrichment skipped by high-impact filter"])
            else:
                enrichment = self._match_event(
                    updated,
                    candidates,
                    provider_unavailable_warning=provider_unavailable_warning,
                    no_live_candidates=no_live_candidates,
                )
            if candidates_from_cache and enrichment.source:
                cache_used = True
                enrichment.provider_type = ProviderType.CACHE
                enrichment.warnings.append("cache_used: using cached enrichment")
            if enrichment.source:
                enriched_count += 1
            updated.enrichment = enrichment
            enriched_events.append(updated)

        providers_failed = sum(
            1
            for status in provider_statuses
            if status["status"] in {"provider_unavailable", "provider_failed"}
        )
        return enriched_events, {
            "enriched_count": enriched_count,
            "missing_enrichment_count": len(enriched_events) - enriched_count,
            "provider_statuses": provider_statuses,
            "providers_attempted": len(provider_statuses),
            "providers_succeeded": sum(1 for status in provider_statuses if status["status"] == "succeeded"),
            "providers_failed": providers_failed,
            "provider_errors": _dedupe(provider_errors),
            "fallback_used": fallback_used,
            "cache_used": cache_used,
            "browser_scraping_enabled": any(
                bool(getattr(getattr(provider, "settings", None), "enable_browser_scraping", False))
                for provider in self.providers
            ),
            "browser_scraping_used": any(
                status["status"] != "skipped" and str(status["source"]).startswith("Playwright")
                for status in provider_statuses
            ),
            "structured_sources_blocked": any(
                status["status"] == "provider_unavailable"
                and _is_structured_source(str(status["source"]))
                for status in provider_statuses
            ),
            "targeted_search_enabled": any(
                bool(getattr(getattr(provider, "settings", None), "enable_targeted_search_enrichment", False))
                for provider in self.providers
            ),
            "targeted_search_used": any(
                status["status"] != "skipped" and str(status["source"]) == "Targeted Search Event Enrichment"
                for status in provider_statuses
            ),
            "targeted_search_queries": _targeted_queries(self.providers),
            "targeted_search_matches": sum(int(getattr(provider, "last_match_count", 0)) for provider in self.providers),
            "targeted_search_no_match_count": sum(int(getattr(provider, "last_no_match_count", 0)) for provider in self.providers),
        }

    async def _load_live_candidates(
        self,
        country: str,
        start: datetime,
        end: datetime,
        events_to_enrich: list[EconomicEvent],
    ) -> tuple[list[EnrichmentItem], list[str], bool, list[dict[str, object]]]:
        provider_errors: list[str] = []
        provider_statuses: list[dict[str, object]] = []
        fallback_used = False
        for provider_index, provider in enumerate(self.providers):
            source = getattr(provider, "source", provider.__class__.__name__)
            failure_key = f"{source}:{provider.__class__.__name__}:{provider_index}"
            cached_failure = self._provider_failure_cache(failure_key, source)
            if cached_failure:
                provider_statuses.append(
                    {
                        "source": source,
                        "status": "skipped",
                        "item_count": 0,
                        "errors": [cached_failure],
                    }
                )
                provider_errors.append(cached_failure)
                continue
            if not bool(getattr(provider, "enabled", True)):
                provider_statuses.append(
                    {
                        "source": source,
                        "status": "skipped",
                        "item_count": 0,
                        "errors": [],
                    }
                )
                continue
            if hasattr(provider, "fetch_for_events"):
                items, errors = await provider.fetch_for_events(
                    events=events_to_enrich,
                    country=country,
                    start=start,
                    end=end,
                )
            else:
                items, errors = await provider.fetch(country=country, start=start, end=end)
            provider_errors.extend(errors)
            status = _provider_status(source, items, errors)
            provider_statuses.append(status)
            if status["status"] in {"provider_unavailable", "provider_failed"}:
                self._store_provider_failure(failure_key, source, status)
            if _useful_candidates(items):
                self._store_candidates(country, items)
                return items, provider_errors, fallback_used, provider_statuses
            if errors:
                fallback_used = True
        return [], provider_errors, fallback_used, provider_statuses

    def _store_candidates(self, country: str, items: list[EnrichmentItem]) -> None:
        by_date: dict[str, list[dict[str, object]]] = {}
        by_provider_date: dict[tuple[str, str], list[dict[str, object]]] = {}
        for item in items:
            dumped = item.model_dump(mode="json")
            by_date.setdefault(item.date, []).append(dumped)
            by_provider_date.setdefault((_provider_key(item.source), item.date), []).append(dumped)
        for date_value, payload in by_date.items():
            self.cache.set(_cache_key_v2(country, date_value), payload)
            self.cache.set(_cache_key(country, date_value), payload)
        for (provider_key, date_value), payload in by_provider_date.items():
            self.cache.set(_provider_cache_key_v2(provider_key, country, date_value), payload)
        for item in items:
            if item.provider_type == ProviderType.SEARCH_SNIPPET:
                self.cache.set(
                    _targeted_cache_key_v3(country, item.date, _normalized_category(item.category)),
                    [item.model_dump(mode="json")],
                )

    def _load_cached_candidates(
        self,
        country: str,
        start: datetime,
        end: datetime,
    ) -> list[EnrichmentItem]:
        candidates: list[EnrichmentItem] = []
        current = start.date()
        end_date = end.date()
        while current <= end_date:
            payload = self._cache_payload(_cache_key_v2(country, current.isoformat()))
            if payload is None:
                payload = self._cache_payload(_cache_key(country, current.isoformat()))
            if isinstance(payload, list):
                for item in payload:
                    try:
                        candidates.append(EnrichmentItem.model_validate(item))
                    except Exception as exc:
                        logger.warning(
                            "event_enrichment_cache_row_invalid",
                            extra={"_error": str(exc) or type(exc).__name__},
                        )
            for category in ENRICHABLE_CATEGORIES:
                targeted_payload = self._cache_payload(
                    _targeted_cache_key_v3(country, current.isoformat(), category)
                )
                if isinstance(targeted_payload, list):
                    for item in targeted_payload:
                        try:
                            candidates.append(EnrichmentItem.model_validate(item))
                        except Exception as exc:
                            logger.warning(
                                "event_enrichment_cache_row_invalid",
                                extra={"_error": str(exc) or type(exc).__name__},
                            )
            current = current.fromordinal(current.toordinal() + 1)
        return candidates

    def _cache_payload(self, cache_key: str):
        entry = self.cache.get_entry(cache_key)
        if not entry:
            return None
        updated = datetime.fromisoformat(str(entry["updated_at"]))
        ttl_hours = _ttl_hours(self.providers)
        if ttl_hours > 0 and datetime.now(UTC) - updated.astimezone(UTC) > timedelta(hours=ttl_hours):
            return None
        return entry["payload"]

    def _provider_failure_cache(self, cache_identity: str, source: str) -> str | None:
        entry = self.cache.get_entry(_provider_failure_cache_key(cache_identity))
        if not entry:
            return None
        try:
            updated = datetime.fromisoformat(str(entry["updated_at"]))
        except ValueError:
            return None
        if datetime.now(UTC) - updated.astimezone(UTC) > timedelta(minutes=PROVIDER_FAILURE_TTL_MINUTES):
            return None
        payload = entry.get("payload") or {}
        reason = payload.get("reason") if isinstance(payload, dict) else None
        return f"provider_failure_cache: {source} skipped after recent failure{': ' + reason if reason else ''}"

    def _store_provider_failure(self, cache_identity: str, source: str, status: dict[str, object]) -> None:
        errors = status.get("errors", [])
        reason = "; ".join(str(error) for error in errors)[:500] if isinstance(errors, list) else str(errors)
        self.cache.set(
            _provider_failure_cache_key(cache_identity),
            {
                "source": source,
                "status": status.get("status"),
                "reason": reason,
                "ttl_minutes": PROVIDER_FAILURE_TTL_MINUTES,
            },
        )

    def _match_event(
        self,
        event: EconomicEvent,
        candidates: list[EnrichmentItem],
        provider_unavailable_warning: str | None = None,
        no_live_candidates: bool = False,
    ) -> EventEnrichment:
        if not event.time_utc:
            return EventEnrichment(
                warnings=["event time unavailable; enrichment skipped"],
            )
        matches: list[tuple[int, EnrichmentItem, str, list[str]]] = []
        for candidate in candidates:
            score, matched_by, warnings = _score_candidate(event, candidate)
            if score > 0:
                matches.append((score, candidate, matched_by, warnings))
        if not matches:
            if provider_unavailable_warning and no_live_candidates:
                return EventEnrichment(
                    warnings=[
                        "provider_unavailable: Structured enrichment providers unavailable",
                        provider_unavailable_warning,
                    ]
                )
            if candidates:
                return EventEnrichment(warnings=["no_match_found: No enrichment match found among provider results"])
            return EventEnrichment(warnings=["no_data_available: No enrichment data available"])
        _, candidate, matched_by, warnings = sorted(matches, key=lambda item: item[0], reverse=True)[0]
        if candidate.provider_type == ProviderType.SEARCH_SNIPPET:
            matched_by = f"targeted_search:{candidate.source}"
        return EventEnrichment(
            forecast=candidate.forecast,
            previous=candidate.previous,
            consensus=candidate.consensus,
            actual=candidate.actual,
            source=candidate.source,
            source_url=candidate.source_url,
            provider_type=candidate.provider_type,
            retrieved_at=candidate.retrieved_at,
            reliability=candidate.reliability,
            matched_by=matched_by,
            warnings=_dedupe(warnings + candidate.warnings),
            errors=candidate.errors,
        )


def _score_candidate(
    event: EconomicEvent,
    candidate: EnrichmentItem,
) -> tuple[int, str, list[str]]:
    if event.country.upper() != candidate.country.upper():
        return 0, "", []
    if event.date != candidate.date:
        return 0, "", []
    minutes = 0.0
    candidate_has_time = candidate.time_utc is not None
    if candidate_has_time:
        minutes = abs((event.time_utc.astimezone(UTC) - candidate.time_utc.astimezone(UTC)).total_seconds()) / 60
        if minutes > TIME_WINDOW_MINUTES:
            return 0, "", []

    event_category = _normalized_category(event.category or event.name)
    candidate_category = _normalized_category(candidate.category or candidate.name)
    if candidate_category not in CATEGORY_ALIASES.get(event_category, {event_category}):
        if not _keyword_match(event.name, candidate.name):
            return 0, "", []

    warnings = []
    score = 100
    matched_by = "country_date_time_category_keywords"
    if not candidate_has_time:
        score -= 10
        matched_by = "country_date_category_keywords"
        warnings.append("enrichment source did not provide event time; official event time retained")
    if minutes:
        score -= int(minutes)
        matched_by = f"country_date_time_window_{int(minutes)}m_category_keywords"
        warnings.append(f"enrichment time differs from official time by {int(minutes)} minutes")
    if _keyword_match(event.name, candidate.name):
        score += 20
    return score, matched_by, warnings


def _normalized_category(value: str) -> str:
    inferred = infer_event_category(value)
    return (inferred if inferred != "OTHER" else value).upper()


def _keyword_match(left: str, right: str) -> bool:
    left_tokens = _tokens(left)
    right_tokens = _tokens(right)
    if not left_tokens or not right_tokens:
        return False
    return bool(left_tokens & right_tokens)


def _tokens(value: str) -> set[str]:
    ignored_tokens = {"the", "and", "rate", "index", "price", "report", "release", "monthly"}
    return {
        token
        for token in re_split(value.lower())
        if len(token) >= 3 and token not in ignored_tokens
    }


def re_split(value: str) -> list[str]:
    return [token for token in "".join(char if char.isalnum() else " " for char in value).split()]


def _cache_key(country: str, date_value: str) -> str:
    return f"{CACHE_PREFIX}:{country.upper()}:{date_value}"


def _cache_key_v2(country: str, date_value: str) -> str:
    return f"{CACHE_MERGED_PREFIX_V2}:{country.upper()}:{date_value}"


def _provider_cache_key_v2(provider: str, country: str, date_value: str) -> str:
        return f"{CACHE_PREFIX_V2}:{provider}:{country.upper()}:{date_value}"


def _targeted_cache_key_v3(country: str, date_value: str, category: str) -> str:
    return f"{CACHE_PREFIX_V3}:targeted_search:{country.upper()}:{date_value}:{_provider_key(category)}"


def _provider_failure_cache_key(source: str) -> str:
    return f"{PROVIDER_FAILURE_CACHE_PREFIX}:{_provider_key(source)}"


def _provider_status(
    source: str,
    items: list[EnrichmentItem],
    errors: list[str],
) -> dict[str, object]:
    if _useful_candidates(items):
        status = "succeeded"
    elif any(_is_unavailable(error) for error in errors):
        status = "provider_unavailable"
    elif errors:
        status = "provider_failed"
    else:
        status = "no_data_available"
    return {
        "source": source,
        "status": status,
        "item_count": len(items),
        "errors": _dedupe(errors),
    }


def _provider_unavailable_warning(provider_statuses: list[dict[str, object]]) -> str | None:
    unavailable = [
        _status_summary(status)
        for status in provider_statuses
        if status["status"] in {"provider_unavailable", "provider_failed"}
        and _is_structured_source(str(status["source"]))
    ]
    if not unavailable:
        return None
    return f"Structured providers unavailable: {', '.join(unavailable)}"


def _status_summary(status: dict[str, object]) -> str:
    source = str(status["source"])
    label = source.replace(" Economic Calendar", "").replace(" Calendar", "")
    errors = " ".join(str(error) for error in status.get("errors", []))
    code = _status_code(errors)
    return f"{label} {code}".strip()


def _status_code(value: str) -> str:
    match = re.search(r"\b(403|429|500|502|503|504)\b", value)
    if match:
        return match.group(1)
    if "forbidden" in value.lower():
        return "403"
    if "too many requests" in value.lower():
        return "429"
    return "unavailable"


def _is_unavailable(error: str) -> bool:
    lowered = error.lower()
    return (
        "403" in lowered
        or "429" in lowered
        or "forbidden" in lowered
        or "too many requests" in lowered
        or "provider_unavailable" in lowered
    )


def _is_structured_source(source: str) -> bool:
    return any(name in source for name in ("DailyFX", "ForexFactory", "Investing"))


def _useful_candidates(candidates: list[EnrichmentItem]) -> bool:
    return any(
        item.forecast is not None
        or item.previous is not None
        or item.consensus is not None
        or item.actual is not None
        for item in candidates
    )


def _provider_key(source: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", source.lower()).strip("_") or "unknown"


def _ttl_hours(providers) -> int:
    for provider in providers:
        settings = getattr(provider, "settings", None)
        if settings is not None:
            return int(getattr(settings, "event_enrichment_cache_ttl_hours", 24))
    return 24


def _first_settings(providers):
    for provider in providers:
        settings = getattr(provider, "settings", None)
        if settings is not None:
            return settings
    return None


def _events_to_enrich(events: list[EconomicEvent], settings) -> list[EconomicEvent]:
    selected = []
    only_high = bool(getattr(settings, "enrich_only_high_impact", True))
    max_events = int(getattr(settings, "enrichment_max_events", 10))
    for event in events:
        if event.country.upper() != "US":
            continue
        category = _normalized_category(event.category or event.name)
        if category not in ENRICHABLE_CATEGORIES:
            continue
        if only_high and str(event.impact).split(".")[-1] != "HIGH":
            continue
        selected.append(event)
        if len(selected) >= max_events:
            break
    return selected


def _targeted_queries(providers) -> list[str]:
    queries: list[str] = []
    for provider in providers:
        for query in getattr(provider, "last_queries", []) or []:
            if query not in queries:
                queries.append(query)
    return queries


def _dedupe(values: list[str]) -> list[str]:
    deduped = []
    for value in values:
        if value and value not in deduped:
            deduped.append(value)
    return deduped
