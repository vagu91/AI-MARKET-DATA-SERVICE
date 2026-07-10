from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings


POLYMARKET_TAG_SLUGS = ("finance", "economy", "economics", "fed", "fed-rates", "business", "geopolitics")

CATEGORY_KEYWORDS = {
    "NASDAQ_DIRECT": ("nasdaq", "nasdaq-100", "nasdaq 100"),
    "QQQ": ("qqq", "invesco qqq"),
    "NQ_MNQ": ("nq futures", "e-mini nasdaq", "micro nasdaq"),
    "MEGA_CAP": ("apple", "microsoft", "nvidia", "amazon", "meta", "tesla", "google", "alphabet"),
    "SEMICONDUCTORS": ("semiconductor", "chip", "nvidia", "amd", "intel", "broadcom", "tsmc"),
    "FED": ("fomc", "federal reserve", "powell", "fed rate", "fed rates"),
    "INTEREST_RATES": ("interest rate", "interest rates", "rate cut", "rate cuts", "rate hike", "yield"),
    "CPI_INFLATION": ("cpi", "inflation", "pce"),
    "LABOR": ("jobs report", "payroll", "unemployment"),
    "GDP_RECESSION": ("gdp", "recession"),
    "US_ECONOMY": ("us economy", "u.s. economy", "government shutdown"),
    "GOVERNMENT_SHUTDOWN": ("government shutdown", "shutdown"),
    "TECH_REGULATION": ("antitrust", "tech regulation", "ai regulation"),
    "GEOPOLITICAL_MARKET_RELEVANT": ("tariff", "sanction", "oil", "nato", "china", "taiwan"),
}

CATEGORY_PRIORITY = {
    "NASDAQ_DIRECT": 1,
    "QQQ": 2,
    "NQ_MNQ": 2,
    "FED": 3,
    "INTEREST_RATES": 3,
    "CPI_INFLATION": 4,
    "LABOR": 4,
    "GDP_RECESSION": 4,
    "MEGA_CAP": 5,
    "SEMICONDUCTORS": 6,
    "GEOPOLITICAL_MARKET_RELEVANT": 7,
    "US_ECONOMY": 8,
    "GOVERNMENT_SHUTDOWN": 8,
    "TECH_REGULATION": 9,
    "OTHER_RELEVANT": 10,
}

MEGA_CAP_ENTITIES = {
    "apple": "AAPL",
    "microsoft": "MSFT",
    "nvidia": "NVDA",
    "amazon": "AMZN",
    "meta": "META",
    "tesla": "TSLA",
    "google": "GOOGL",
    "alphabet": "GOOGL",
    "broadcom": "AVGO",
    "amd": "AMD",
    "netflix": "NFLX",
}

DIRECT_COMPANY_OUTCOME_TERMS = (
    "stock",
    "share price",
    "market cap",
    "earnings",
    "revenue",
    "profit",
    "ipo",
    "acquisition",
    "acquire",
    "merger",
    "bankruptcy",
    "bankrupt",
    "product launch",
    "chip sales",
)

MARKET_IMPACT_TERMS = (
    "market",
    "markets",
    "stock",
    "stocks",
    "equity",
    "equities",
    "rate",
    "rates",
    "inflation",
    "tariff",
    "sanction",
    "oil",
    "semiconductor",
    "supply chain",
    "recession",
    "gdp",
)


class PolymarketPredictionProvider:
    source = "Polymarket Public Markets"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_polymarket:
            return _status("disabled", "polymarket_disabled", started)
        raw_events: list[dict[str, Any]] = []
        errors: list[str] = []
        try:
            async with httpx.AsyncClient(timeout=self.settings.polymarket_timeout_seconds) as client:
                for tag_slug in POLYMARKET_TAG_SLUGS:
                    try:
                        response = await asyncio.wait_for(
                            client.get(
                                f"{self.settings.polymarket_gamma_base_url.rstrip('/')}/events",
                                params={"active": "true", "closed": "false", "limit": 50, "tag_slug": tag_slug},
                            ),
                            timeout=min(float(self.settings.polymarket_timeout_seconds), 10.0),
                        )
                        response.raise_for_status()
                        payload = response.json()
                        if isinstance(payload, list):
                            raw_events.extend(item for item in payload if isinstance(item, dict))
                    except Exception as exc:
                        errors.append(f"{tag_slug}:{exc or type(exc).__name__}")
        except TimeoutError:
            return _status("provider_timeout", "Polymarket discovery timed out", started)

        candidates, rejected, rejected_samples = _markets_from_events(raw_events, self.settings)
        candidates = _dedupe_markets(candidates)
        grouped_events = group_polymarket_events(candidates, max_events=max(0, self.settings.polymarket_max_markets))
        accepted = [market for event in grouped_events for market in event.get("markets", [])]
        now = datetime.now(UTC)
        return {
            "status": "found" if grouped_events else "not_found",
            "provider": self.source,
            "source": "Polymarket Gamma API",
            "source_url": self.settings.polymarket_gamma_base_url,
            "retrieved_at": _iso(now),
            "valid_until": _iso(now + timedelta(minutes=self.settings.polymarket_cache_minutes)),
            "markets": accepted,
            "events": grouped_events,
            "labeling": {
                "probability_type": "market_implied",
                "objective_probability": False,
                "sensitive_to_liquidity_and_spread": True,
                "signal": False,
            },
            "diagnostics": {
                "events_searched": len(raw_events),
                "markets_searched": sum(len(event.get("markets") or []) for event in raw_events),
                "relevant_candidates": len(candidates),
                "accepted": len(accepted),
                "accepted_events": len(grouped_events),
                "rejected_irrelevant": rejected.get("irrelevant", 0),
                "rejected_weak_indirect": rejected.get("weak_indirect", 0),
                "rejected_low_relevance": rejected.get("low_relevance", 0),
                "rejected_rules_only": rejected.get("rules_only", 0),
                "rejected_low_liquidity": rejected.get("low_liquidity", 0),
                "rejected_low_volume": rejected.get("low_volume", 0),
                "rejected_wide_spread": rejected.get("wide_spread", 0),
                "rejected_invalid_probability": rejected.get("invalid_probability", 0),
                "rejected_expired": rejected.get("expired", 0),
                "rejected_samples": rejected_samples,
                "pricing_failures": 0,
                "order_book_failures": 0,
                "auth_used": False,
            },
            "warnings": errors[:10] + ([] if grouped_events else ["polymarket_no_relevant_markets_after_filters"]),
            "errors": [],
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
        }


def normalize_polymarket_market(market: dict[str, Any], event: dict[str, Any] | None = None) -> tuple[dict[str, Any] | None, str | None]:
    event = event or {}
    assessment = assess_polymarket_relevance(market, event)
    if assessment["category"] is None:
        return None, assessment["rejection_reason"] or "irrelevant"
    if assessment["directness"] == "WEAK_INDIRECT":
        return None, "weak_indirect"
    if float(assessment["relevance_score"]) < 0.62:
        return None, "low_relevance"
    if assessment["matched_category_evidence"] == "rules_only":
        return None, "rules_only"
    category = assessment["category"]
    if not bool(market.get("active", True)) or bool(market.get("closed", False)):
        return None, "expired"
    end_date = market.get("endDate") or event.get("endDate")
    if _is_past(end_date):
        return None, "expired"
    rules = market.get("rules") or market.get("description") or event.get("description")
    if not rules:
        return None, "missing_rules"
    outcomes = _json_list(market.get("outcomes"))
    prices = [_float(item) for item in _json_list(market.get("outcomePrices"))]
    if not outcomes or len(outcomes) != len(prices) or any(price is None or price < 0 or price > 1 for price in prices):
        return None, "invalid_probability"
    probability_sum = sum(float(price) for price in prices if price is not None)
    if len(prices) > 1 and not 0.9 <= probability_sum <= 1.1:
        return None, "invalid_probability"
    volume = _float(market.get("volumeNum") or market.get("volume") or event.get("volume"))
    liquidity = _float(market.get("liquidityNum") or market.get("liquidity") or event.get("liquidityClob") or event.get("liquidity"))
    if volume is not None and volume < 0:
        volume = None
    if liquidity is not None and liquidity < 0:
        liquidity = None
    best_bid = _float(market.get("bestBid"))
    best_ask = _float(market.get("bestAsk"))
    spread = round(best_ask - best_bid, 6) if best_bid is not None and best_ask is not None else None
    output = {
        "provider": "Polymarket",
        "market_id": str(market.get("id") or ""),
        "event_id": str(event.get("id") or ""),
        "slug": market.get("slug"),
        "title": event.get("title") or market.get("title") or market.get("question"),
        "question": market.get("question"),
        "category": category,
        "relevance_score": assessment["relevance_score"],
        "relevance_reason": assessment["relevance_reason"],
        "matched_entities": assessment["matched_entities"],
        "matched_category_evidence": assessment["matched_category_evidence"],
        "directness": assessment["directness"],
        "rejection_reason": None,
        "outcomes": outcomes,
        "outcome_prices": prices,
        "implied_probabilities": prices,
        "probability_sum": round(probability_sum, 6),
        "probability_label": "market_implied",
        "objective_probability": False,
        "volume": volume,
        "liquidity": liquidity,
        "open_interest": _float(market.get("openInterest") or event.get("openInterest")),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "midpoint": round((best_bid + best_ask) / 2, 6) if best_bid is not None and best_ask is not None else None,
        "spread": spread,
        "end_date": end_date,
        "rules": rules,
        "resolution_source": market.get("resolutionSource") or event.get("resolutionSource"),
        "active": bool(market.get("active", True)),
        "closed": bool(market.get("closed", False)),
        "resolved": bool(market.get("resolved", False)),
        "quality_flags": _quality_flags(volume=volume, liquidity=liquidity, spread=spread),
        "retrieved_at": _iso(datetime.now(UTC)),
        "valid_until": _iso(datetime.now(UTC) + timedelta(minutes=15)),
    }
    return output, None


def assess_polymarket_relevance(market: dict[str, Any], event: dict[str, Any] | None = None) -> dict[str, Any]:
    event = event or {}
    question = str(market.get("question") or market.get("title") or "")
    title = str(event.get("title") or market.get("title") or "")
    slug = str(market.get("slug") or event.get("slug") or "")
    tags = " ".join(_tag_texts(event.get("tags")) + _tag_texts(market.get("tags")))
    metadata = " ".join(str(value or "") for value in ((event.get("eventMetadata") or {}).values() if isinstance(event.get("eventMetadata"), dict) else []))
    primary_text = " ".join((question, title, slug, tags, metadata)).lower()
    rules_text = " ".join(str(value or "") for value in (market.get("rules"), market.get("description"), event.get("description"), event.get("resolutionSource"))).lower()
    explicit_resolution = " ".join((question, title, slug)).lower()
    entities = _matched_entities(primary_text)

    if any(token in explicit_resolution for token in ("nobel", "peace prize")):
        return _assessment(None, 0.0, "Nobel/award markets are not direct market context.", entities, "primary_subject", "WEAK_INDIRECT", "irrelevant")
    if any(token in explicit_resolution for token in ("xi jinping", "out before", "removed from office")) and not any(term in explicit_resolution for term in MARKET_IMPACT_TERMS):
        return _assessment(None, 0.0, "Political leadership market has no direct market/economic dependency.", entities, "primary_subject", "WEAK_INDIRECT", "irrelevant")
    if "peace deal" in explicit_resolution and not any(term in explicit_resolution for term in MARKET_IMPACT_TERMS):
        return _assessment(None, 0.0, "Geopolitical peace-deal market lacks direct market/economic linkage.", entities, "primary_subject", "WEAK_INDIRECT", "irrelevant")

    category, evidence, reason = _classify_primary(primary_text, explicit_resolution, entities)
    if category is None:
        rules_category = _classify_rules_only(rules_text)
        if rules_category:
            return _assessment(None, 0.0, "Rules-only keyword match is insufficient for relevance.", entities, "rules_only", "WEAK_INDIRECT", "rules_only")
        return _assessment(None, 0.0, "No direct Nasdaq, macro, rates, inflation, labor, or market-relevant subject found.", entities, "none", "WEAK_INDIRECT", "irrelevant")

    directness = "DIRECT" if category in {"NASDAQ_DIRECT", "QQQ", "NQ_MNQ", "FED", "INTEREST_RATES", "CPI_INFLATION", "LABOR", "GDP_RECESSION", "MEGA_CAP", "SEMICONDUCTORS"} else "STRONG_INDIRECT"
    score = 0.9 if directness == "DIRECT" else 0.7
    if evidence == "tag_or_metadata":
        score -= 0.08
    if category == "GEOPOLITICAL_MARKET_RELEVANT":
        directness = "STRONG_INDIRECT"
        score = 0.68
    return _assessment(category, round(score, 2), reason, entities, evidence, directness, None)


def _markets_from_events(events: list[dict[str, Any]], settings: Settings) -> tuple[list[dict[str, Any]], dict[str, int], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    rejected: dict[str, int] = {}
    rejected_samples: list[dict[str, Any]] = []
    for event in events:
        for market in event.get("markets") or []:
            if not isinstance(market, dict):
                continue
            normalized, reason = normalize_polymarket_market(market, event)
            if normalized is None:
                rejected[reason or "other"] = rejected.get(reason or "other", 0) + 1
                _append_rejected_sample(rejected_samples, market, event, reason or "other")
                continue
            volume = normalized.get("volume")
            liquidity = normalized.get("liquidity")
            spread = normalized.get("spread")
            if liquidity is not None and liquidity < settings.polymarket_min_liquidity_usd:
                rejected["low_liquidity"] = rejected.get("low_liquidity", 0) + 1
                _append_rejected_sample(rejected_samples, market, event, "low_liquidity")
                continue
            if volume is not None and volume < settings.polymarket_min_volume_usd:
                rejected["low_volume"] = rejected.get("low_volume", 0) + 1
                _append_rejected_sample(rejected_samples, market, event, "low_volume")
                continue
            if spread is not None and spread > settings.polymarket_max_spread:
                rejected["wide_spread"] = rejected.get("wide_spread", 0) + 1
                _append_rejected_sample(rejected_samples, market, event, "wide_spread")
                continue
            accepted.append(normalized)
    return accepted, rejected, rejected_samples


def group_polymarket_events(markets: list[dict[str, Any]], *, max_events: int) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for market in markets:
        key = _event_group_key(market)
        grouped.setdefault(key, []).append(market)
    events = []
    for key, group in grouped.items():
        group.sort(key=lambda item: float(item.get("volume") or 0), reverse=True)
        first = group[0]
        market_level_volume = sum(float(item.get("volume") or 0) for item in group)
        event_liquidity_values = [float(item.get("liquidity") or 0) for item in group if item.get("liquidity") is not None]
        outcome_distribution = [
            {
                "market_id": item.get("market_id"),
                "question": item.get("question"),
                "outcomes": item.get("outcomes"),
                "implied_probabilities": item.get("implied_probabilities"),
            }
            for item in group
        ]
        events.append(
            {
                "event_id": first.get("event_id") or key,
                "event_title": first.get("title") or first.get("question"),
                "event_family": key,
                "category": first.get("category"),
                "directness": first.get("directness"),
                "relevance_score": first.get("relevance_score"),
                "relevance_reason": first.get("relevance_reason"),
                "matched_entities": sorted({entity for item in group for entity in item.get("matched_entities", [])}),
                "markets": group,
                "market_count": len(group),
                "outcome_distribution": outcome_distribution,
                "total_event_volume": market_level_volume,
                "event_liquidity": max(event_liquidity_values) if event_liquidity_values else None,
                "value_scope": {
                    "volume": "market_level_sum",
                    "liquidity": "event_level_or_max_market_level_not_summed",
                    "open_interest": "event_level_not_summed",
                },
                "retrieved_at": first.get("retrieved_at"),
            }
        )
    events.sort(key=lambda item: (CATEGORY_PRIORITY.get(str(item.get("category")), 99), -float(item.get("total_event_volume") or 0)))
    return events[:max_events] if max_events else events


def _dedupe_markets(markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    output: list[dict[str, Any]] = []
    for market in sorted(markets, key=lambda item: float(item.get("volume") or 0), reverse=True):
        key = str(market.get("market_id") or market.get("slug") or market.get("question"))
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(market)
    return output


def _classify_primary(primary_text: str, explicit_resolution: str, entities: list[str]) -> tuple[str | None, str, str]:
    if any(token in explicit_resolution for token in CATEGORY_KEYWORDS["NASDAQ_DIRECT"]):
        return "NASDAQ_DIRECT", "question_title_slug", "Nasdaq subject appears in the market question/title/slug."
    if any(token in explicit_resolution for token in CATEGORY_KEYWORDS["QQQ"]):
        return "QQQ", "question_title_slug", "QQQ subject appears in the market question/title/slug."
    if any(token in explicit_resolution for token in CATEGORY_KEYWORDS["NQ_MNQ"]):
        return "NQ_MNQ", "question_title_slug", "NQ/MNQ subject appears in the market question/title/slug."
    if entities and any(term in explicit_resolution for term in DIRECT_COMPANY_OUTCOME_TERMS):
        if any(term in explicit_resolution for term in ("stock", "share price", "market cap", "earnings", "revenue", "profit")):
            return "MEGA_CAP", "question_title_slug", "Mega-cap market depends directly on company/security outcome."
        if any(entity in {"NVDA", "AMD", "INTC", "AVGO"} for entity in entities):
            return "SEMICONDUCTORS", "question_title_slug", "Semiconductor company market depends directly on company/security outcome."
        return "MEGA_CAP", "question_title_slug", "Mega-cap market depends directly on company/security outcome."
    for category in ("FED", "INTEREST_RATES", "CPI_INFLATION", "LABOR", "GDP_RECESSION", "US_ECONOMY", "GOVERNMENT_SHUTDOWN", "TECH_REGULATION"):
        if any(token in explicit_resolution for token in CATEGORY_KEYWORDS[category]):
            return category, "question_title_slug", f"{category} subject appears in the market question/title/slug."
    if any(token in explicit_resolution for token in CATEGORY_KEYWORDS["GEOPOLITICAL_MARKET_RELEVANT"]) and any(term in explicit_resolution for term in MARKET_IMPACT_TERMS):
        return "GEOPOLITICAL_MARKET_RELEVANT", "question_title_slug", "Geopolitical market has explicit market/economic linkage in the subject."
    for category, tokens in CATEGORY_KEYWORDS.items():
        if any(token in primary_text for token in tokens):
            if category == "MEGA_CAP" and not any(term in primary_text for term in DIRECT_COMPANY_OUTCOME_TERMS):
                continue
            if category == "GEOPOLITICAL_MARKET_RELEVANT" and not any(term in primary_text for term in MARKET_IMPACT_TERMS):
                continue
            return category, "tag_or_metadata", f"{category} evidence found in tags or event metadata."
    return None, "none", ""


def _classify_rules_only(rules_text: str) -> str | None:
    for category, tokens in CATEGORY_KEYWORDS.items():
        if any(token in rules_text for token in tokens):
            return category
    return None


def _append_rejected_sample(samples: list[dict[str, Any]], market: dict[str, Any], event: dict[str, Any], reason: str) -> None:
    if len(samples) >= 20:
        return
    assessment = assess_polymarket_relevance(market, event)
    samples.append(
        {
            "market_id": str(market.get("id") or ""),
            "event_id": str(event.get("id") or ""),
            "title": event.get("title") or market.get("title") or market.get("question"),
            "question": market.get("question"),
            "slug": market.get("slug") or event.get("slug"),
            "rejection_reason": reason,
            "relevance_score": assessment.get("relevance_score"),
            "matched_entities": assessment.get("matched_entities") or [],
            "matched_category_evidence": assessment.get("matched_category_evidence"),
            "directness": assessment.get("directness"),
        }
    )


def _assessment(
    category: str | None,
    score: float,
    reason: str,
    entities: list[str],
    evidence: str,
    directness: str,
    rejection_reason: str | None,
) -> dict[str, Any]:
    return {
        "category": category,
        "relevance_score": score,
        "relevance_reason": reason,
        "matched_entities": entities,
        "matched_category_evidence": evidence,
        "directness": directness,
        "rejection_reason": rejection_reason,
    }


def _matched_entities(text: str) -> list[str]:
    return sorted({ticker for name, ticker in MEGA_CAP_ENTITIES.items() if name in text})


def _tag_texts(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    output = []
    for item in value:
        if isinstance(item, dict):
            output.extend(str(item.get(key) or "") for key in ("slug", "label", "name"))
        else:
            output.append(str(item))
    return output


def _event_group_key(market: dict[str, Any]) -> str:
    event_id = str(market.get("event_id") or "")
    end_date = str(market.get("end_date") or "")[:10]
    title = _family_title(str(market.get("title") or market.get("question") or ""))
    if event_id:
        return f"{event_id}:{end_date}:{title}"
    return f"{end_date}:{title}"


def _family_title(value: str) -> str:
    text = value.lower()
    text = text.replace("?", "")
    text = " ".join(text.split())
    for token in (" in 2026", " in 2027", " by 2026", " by 2027"):
        text = text.replace(token, "")
    return text[:100]


def _quality_flags(*, volume: float | None, liquidity: float | None, spread: float | None) -> list[str]:
    flags = ["market_implied_not_objective_probability"]
    if volume is None:
        flags.append("volume_unavailable")
    if liquidity is None:
        flags.append("liquidity_unavailable")
    if spread is None:
        flags.append("spread_unavailable")
    elif spread > 0.15:
        flags.append("wide_spread")
    return flags


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    try:
        decoded = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return decoded if isinstance(decoded, list) else []


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def _is_past(value: Any) -> bool:
    if not value:
        return True
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC) < datetime.now(UTC)


def _status(status: str, reason: str, started: datetime) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "status": status,
        "provider": "Polymarket Public Markets",
        "source": "Polymarket Gamma API",
        "source_url": "https://gamma-api.polymarket.com",
        "retrieved_at": _iso(now),
        "valid_until": _iso(now + timedelta(minutes=15)),
        "markets": [],
        "events": [],
        "labeling": {"probability_type": "market_implied", "objective_probability": False, "signal": False},
        "diagnostics": {"accepted": 0, "accepted_events": 0, "rejected_samples": [], "auth_used": False},
        "warnings": [reason] if status != "provider_failed" else [],
        "errors": [reason] if status == "provider_failed" else [],
        "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
    }


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")
