from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import Counter
from datetime import UTC, datetime, timedelta
from html.parser import HTMLParser
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from app.services.data_freshness_service import parse_datetime
from app.services.data_integrity_service import clean_text, news_content_status

logger = logging.getLogger(__name__)

PIPELINE_VERSION = "mnq_news_intelligence_v1"
ENTITY_MAP_VERSION = "mnq_entities_v1"

ENTITY_ALIASES: dict[str, tuple[str, ...]] = {
    "AAPL": ("apple", "apple inc", "aapl"),
    "MSFT": ("microsoft", "msft"),
    "AMZN": ("amazon", "amzn"),
    "META": ("meta platforms", "facebook", "meta"),
    "GOOGL": ("alphabet", "google", "googl"),
    "GOOG": ("alphabet class c", "goog"),
    "NVDA": ("nvidia", "nvda"),
    "AVGO": ("broadcom", "avgo"),
    "TSLA": ("tesla", "tsla"),
    "NFLX": ("netflix", "nflx"),
    "AMD": ("advanced micro devices", "amd"),
    "COST": ("costco", "cost"),
    "MU": ("micron", "mu"),
    "INTC": ("intel", "intc"),
    "QCOM": ("qualcomm", "qcom"),
    "AMAT": ("applied materials", "amat"),
    "ASML": ("asml",),
    "ARM": ("arm holdings", "arm"),
    "QQQ": ("invesco qqq", "qqq"),
}

SEMICONDUCTOR_SYMBOLS = {"NVDA", "AVGO", "AMD", "MU", "INTC", "QCOM", "AMAT", "ASML", "ARM"}

OFFICIAL_DOMAINS = {
    "federalreserve.gov", "bls.gov", "bea.gov", "treasury.gov", "sec.gov", "cftc.gov", "cboe.com", "nasdaq.com",
}
OFFICIAL_NAMES = {
    "federal reserve", "federal reserve rss", "board of governors of the federal reserve system",
    "bls", "bls rss", "bureau of labor statistics", "bea", "bea rss", "bureau of economic analysis",
    "us treasury", "u s treasury", "department of the treasury", "sec", "cftc", "nasdaq official",
}
MAJOR_AGENCIES = {"reuters", "associated press", "ap news", "bloomberg", "dow jones"}
MAJOR_MEDIA = {"financial times", "wall street journal", "wsj", "cnbc", "marketwatch", "barron", "barron's", "investor's business daily", "investors business daily"}
SECONDARY_MEDIA = {"insider monkey", "seeking alpha", "marketbeat", "benzinga", "the motley fool", "motley fool", "investopedia", "thestreet", "the street", "moneywise"}
AGGREGATORS = {"yahoo finance", "yahoo", "google news", "msn", "aol", "newsbreak"}

PERSONAL_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"\b(cd|certificate of deposit)s?\b.*\brate", "deposit_rates"),
    (r"\b(high[- ]yield savings|savings account|savings rate|best savings)\b", "deposit_rates"),
    (r"\b(mortgage|refinanc(?:e|ing)|home loan)\b", "mortgage"),
    (r"\bheloc\b", "personal_finance"),
    (r"\b(personal loan|credit card|retirement planning|bank offer|bank bonus)\b", "personal_finance"),
)

TRACKING_QUERY_KEYS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "gclid", "fbclid", "guccounter",
}

SOURCE_BASE_RELIABILITY = {
    "official_source": 0.98,
    "primary_market_source": 0.94,
    "major_news_agency": 0.92,
    "major_financial_media": 0.84,
    "secondary_financial_media": 0.66,
    "aggregator": 0.56,
    "personal_finance": 0.25,
    "low_quality_or_unknown": 0.45,
}

DIAGNOSTIC_KEYS = (
    "raw_article_count", "metadata_complete_count", "published_at_found_count", "summary_found_count",
    "accepted_count", "excluded_count", "excluded_personal_finance_count", "excluded_low_relevance_count",
    "excluded_missing_timestamp_count", "duplicate_count", "syndicated_duplicate_count", "cluster_count",
    "confirmed_cluster_count", "official_source_count", "high_reliability_source_count",
)


class _MetadataParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.meta: dict[str, str] = {}
        self._in_ld_json = False
        self._ld_buffer: list[str] = []
        self.ld_json: list[dict[str, Any]] = []
        self.canonical_url: str | None = None
        self.markup_time: str | None = None
        self._article_depth = 0
        self._article_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {str(key).lower(): str(value or "") for key, value in attrs}
        if tag.lower() == "meta":
            key = (values.get("property") or values.get("name") or values.get("itemprop") or "").lower()
            if key and values.get("content"):
                self.meta[key] = values["content"].strip()
        if tag.lower() == "link" and "canonical" in values.get("rel", "").lower() and values.get("href"):
            self.canonical_url = values["href"].strip()
        if tag.lower() == "time" and values.get("datetime") and self.markup_time is None:
            self.markup_time = values["datetime"].strip()
        if tag.lower() == "article":
            self._article_depth += 1
        if tag.lower() == "script" and values.get("type", "").lower() == "application/ld+json":
            self._in_ld_json = True
            self._ld_buffer = []

    def handle_data(self, data: str) -> None:
        if self._in_ld_json:
            self._ld_buffer.append(data)
        if self._article_depth:
            self._article_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "article" and self._article_depth:
            self._article_depth -= 1
        if tag.lower() != "script" or not self._in_ld_json:
            return
        self._in_ld_json = False
        try:
            payload = json.loads("".join(self._ld_buffer).strip())
        except (json.JSONDecodeError, TypeError):
            return
        rows = payload if isinstance(payload, list) else [payload]
        self.ld_json.extend(row for row in rows if isinstance(row, dict))

    @property
    def article_text(self) -> str | None:
        text = re.sub(r"\s+", " ", " ".join(self._article_text)).strip()
        return text[:2000] if len(text) >= 80 else None


def extract_page_metadata(html_text: str, *, page_url: str) -> dict[str, Any]:
    parser = _MetadataParser()
    parser.feed(html_text or "")
    ld_rows = [row for row in parser.ld_json if _ld_type(row) in {"newsarticle", "article", "report", "webpage"}]
    ld = ld_rows[0] if ld_rows else (parser.ld_json[0] if parser.ld_json else {})
    published_at = (
        ld.get("datePublished")
        or parser.meta.get("article:published_time")
        or parser.meta.get("og:published_time")
        or parser.meta.get("date")
        or parser.markup_time
    )
    url_date = _date_from_url(page_url) if not published_at else None
    published_at = published_at or url_date
    description = ld.get("description") or parser.meta.get("description") or parser.meta.get("og:description") or parser.article_text
    summary_source_type = "json_ld_description" if ld.get("description") else "meta_description" if parser.meta.get("description") or parser.meta.get("og:description") else "page_text_excerpt" if parser.article_text else None
    return {
        "published_at": _iso_datetime(published_at),
        "published_at_source": "json_ld" if ld.get("datePublished") else "opengraph_or_article_meta" if published_at and not url_date and not parser.markup_time else "markup_time" if parser.markup_time else "url_date_pattern" if url_date else None,
        "summary": _clean_summary(description),
        "summary_source_type": summary_source_type,
        "summary_source_url": page_url if description else None,
        "author": _author_name(ld.get("author")) or parser.meta.get("author"),
        "canonical_url": canonicalize_url(parser.canonical_url or parser.meta.get("og:url") or page_url),
    }


def canonicalize_url(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    query = urlencode([(key, val) for key, val in parse_qsl(parsed.query, keep_blank_values=True) if key.lower() not in TRACKING_QUERY_KEYS])
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path.rstrip("/") or "/", "", query, ""))


def classify_news_source(article: dict[str, Any]) -> dict[str, Any]:
    title = str(article.get("title") or "")
    source = str(article.get("source") or "").strip()
    source_url = canonicalize_url(article.get("source_url") or article.get("url"))
    canonical_url = canonicalize_url(article.get("canonical_url"))
    url = canonical_url or source_url
    domain = (urlparse(url).netloc.lower().removeprefix("www.") if url else "")
    aggregator_url = canonicalize_url(article.get("aggregator_url"))
    if not aggregator_url and any(token in domain for token in ("yahoo.com", "news.google.com", "msn.com", "aol.com")):
        aggregator_url = source_url
        if canonical_url == source_url:
            canonical_url = None
    original = str(article.get("original_publisher") or source or "").strip()
    inferred = _publisher_from_title(title)
    if original.lower() in AGGREGATORS and inferred:
        original = inferred
    classification_text = original.lower()
    content_text = f"{title} {article.get('summary') or ''} {source}".lower()
    personal_reason = _personal_finance_reason(content_text)

    if personal_reason or "personal finance" in classification_text:
        classification = "personal_finance"
    elif classification_text in OFFICIAL_NAMES or any(domain == official or domain.endswith(f".{official}") for official in OFFICIAL_DOMAINS):
        classification = "official_source"
    elif "nasdaq.com" in domain and any(token in (url or "").lower() for token in ("press-release", "company-news", "market-activity")):
        classification = "official_source"
    elif _is_company_ir(article, domain):
        classification = "primary_market_source"
    elif classification_text == "ap" or any(name in classification_text for name in MAJOR_AGENCIES):
        classification = "major_news_agency"
    elif any(name in classification_text for name in MAJOR_MEDIA):
        classification = "major_financial_media"
    elif any(name in classification_text for name in SECONDARY_MEDIA):
        classification = "secondary_financial_media"
    elif any(name in classification_text for name in AGGREGATORS) or aggregator_url:
        classification = "aggregator"
    else:
        classification = "low_quality_or_unknown"

    display_source = original or source or domain or "Unknown"
    is_official = classification == "official_source"
    is_primary = classification in {"official_source", "primary_market_source"}
    return {
        "source": display_source,
        "original_publisher": display_source,
        "source_classification": classification,
        "source_url": source_url,
        "canonical_url": canonical_url or (None if aggregator_url else source_url),
        "aggregator_url": aggregator_url,
        "is_official_source": is_official,
        "is_primary_source": is_primary,
        "is_official": is_official,
        "source_tier": (
            1
            if classification in {"official_source", "primary_market_source", "major_news_agency"}
            else 2
            if classification == "major_financial_media"
            else 3
        ),
        "data_origin_is_official": is_official,
        "distribution_source_is_official": is_official,
        "source_is_primary_originator": is_primary,
        "source_is_official_redistributor": False,
        "source_reliability_base": SOURCE_BASE_RELIABILITY[classification],
    }


def extract_entities(article: dict[str, Any]) -> dict[str, Any]:
    text = _normalized_text(f"{article.get('title') or ''} {article.get('summary') or ''}")
    symbols = {str(symbol).upper() for symbol in article.get("symbols") or [] if str(symbol).upper() in ENTITY_ALIASES}
    matched_entities: list[str] = []
    matched_terms: list[str] = []
    for symbol, aliases in ENTITY_ALIASES.items():
        for alias in aliases:
            if re.search(rf"(?<![a-z0-9]){re.escape(alias.lower())}(?![a-z0-9])", text):
                symbols.add(symbol)
                matched_entities.append("Alphabet" if symbol in {"GOOG", "GOOGL"} else alias.title())
                matched_terms.append(alias)
                if symbol == "GOOGL" and alias in {"alphabet", "google"}:
                    symbols.add("GOOG")
                break
    for ticker in re.findall(r"\(([A-Z]{1,5})\)", str(article.get("title") or "")):
        if ticker in ENTITY_ALIASES:
            symbols.add(ticker)
            matched_terms.append(f"({ticker})")
    return {
        "symbols": sorted(symbols),
        "entities": sorted(set(matched_entities)),
        "matched_entities": sorted(set(matched_entities)),
        "entity_matched_terms": sorted(set(matched_terms)),
        "entity_map_version": ENTITY_MAP_VERSION,
    }


def classify_news_topics(article: dict[str, Any]) -> list[dict[str, Any]]:
    title = _normalized_text(article.get("title"))
    summary = _normalized_text(article.get("summary"))
    text = f"{title} {summary}".strip()
    symbols = set(article.get("symbols") or [])
    rows: list[dict[str, Any]] = []

    def add(topic: str, score: float, reason: str, terms: tuple[str, ...]) -> None:
        matched = [term for term in terms if term in text]
        if matched:
            rows.append({
                "topic": topic,
                "topic_score": round(score, 3),
                "topic_reason": reason,
                "matched_entities": sorted(symbols),
                "matched_terms": matched,
            })

    personal = _personal_finance_reason(text)
    yield_exclusions = ("dividend yield", "earnings yield", "rental yield", "mortgage", "cd rate", "savings rate", "corporate bond yield")
    institutional_yields = (
        "treasury yield", "treasury yields", "10 year yield", "10y yield", "2 year yield", "2y yield",
        "30 year yield", "yield curve", "2s10s", "treasury auction", "bond selloff", "bond rally",
        "real yields", "sofr", "swap rates", "fed repricing", "government debt",
    )
    if not personal and not any(term in text for term in yield_exclusions):
        add("yields", 0.9, "institutional_treasury_or_rates_context", institutional_yields)
    fed_terms = (
        "fomc", "federal open market committee", "monetary policy", "rate decision", "interest rate outlook",
        "fed chair", "fed speaker", "fed minutes", "fed policy", "jerome powell", "fed governor",
        "fed repricing", "fed testimony", "fed press conference", "balance sheet policy",
    )
    add("fed", 0.94, "federal_reserve_policy_or_communication", fed_terms)

    inflation_terms = (
        "consumer price index", " cpi ", "producer price index", " ppi ", "pce price", "core pce",
        "inflation expectations", "breakeven inflation", "wage inflation", "inflation report",
    )
    add("inflation", 0.93, "aggregate_inflation_data_or_expectations", inflation_terms)
    macro_terms = (
        "consumer price index", " cpi ", "producer price index", " ppi ", "pce price", " pce ",
        "gross domestic product", " gdp ", "nonfarm payroll", " nfp ", "unemployment rate",
        "average hourly earnings", "retail sales", " ism ",
        "consumer confidence", "fiscal policy", "fomc", "federal open market committee", "fed minutes", "fed policy", "monetary policy",
        "bank failure", "financial stability", "systemic risk", "credit crisis",
    )
    add("macro", 0.9, "us_macro_data_or_policy_fact", macro_terms)
    if any(row["topic"] == "fed" for row in rows) and not any(row["topic"] == "macro" for row in rows):
        rows.append({
            "topic": "macro",
            "topic_score": 0.86,
            "topic_reason": "federal_reserve_policy_has_macro_market_impact",
            "matched_entities": sorted(symbols),
            "matched_terms": [term for term in fed_terms if term in text],
        })
    earnings_terms = ("earnings", "quarterly results", "revenue", " eps ", "guidance", "profit forecast", "capital expenditure", "capex")
    if symbols:
        add("earnings", 0.88, "company_results_guidance_or_financial_outlook", earnings_terms)
    if symbols.intersection(ENTITY_ALIASES):
        rows.append({
            "topic": "mega-cap",
            "topic_score": 0.9 if symbols.intersection({"AAPL", "MSFT", "AMZN", "META", "GOOGL", "GOOG", "NVDA", "AVGO", "TSLA", "NFLX"}) else 0.72,
            "topic_reason": "mnq_watchlist_company_mentioned",
            "matched_entities": sorted(symbols),
            "matched_terms": sorted(symbols),
        })
    semi_terms = ("semiconductor", "chip", "gpu", "foundry", "advanced packaging", "ai accelerator", "wafer", "chip export", "export control")
    if symbols.intersection(SEMICONDUCTOR_SYMBOLS) or any(term in text for term in semi_terms):
        add("semiconductors", 0.92, "semiconductor_company_supply_chain_or_demand", semi_terms + tuple(symbol.lower() for symbol in SEMICONDUCTOR_SYMBOLS))
        if not any(row["topic"] == "semiconductors" for row in rows):
            rows.append({"topic": "semiconductors", "topic_score": 0.82, "topic_reason": "semiconductor_watchlist_entity", "matched_entities": sorted(symbols), "matched_terms": sorted(symbols.intersection(SEMICONDUCTOR_SYMBOLS))})
    add("geopolitics", 0.82, "policy_or_geopolitical_channel_with_mnq_impact", ("export control", "export restriction", "us china", "taiwan", "antitrust", "trade restriction", "sanctions"))
    add("energy", 0.78, "energy_price_or_supply_channel_with_macro_impact", ("oil price", "crude oil", "opec", "strait of hormuz", "energy supply disruption"))
    return _dedupe_topic_rows(rows)


def _recover_timestamp(article: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    json_ld = article.get("json_ld") if isinstance(article.get("json_ld"), dict) else {}
    opengraph = article.get("opengraph") if isinstance(article.get("opengraph"), dict) else {}
    metadata = article.get("article_metadata") if isinstance(article.get("article_metadata"), dict) else {}
    candidates = (
        (json_ld.get("datePublished"), "json_ld", True, False, 0.98),
        (opengraph.get("article:published_time") or opengraph.get("published_time"), "opengraph", True, False, 0.95),
        (metadata.get("datePublished") or metadata.get("published_at"), "article_metadata", True, False, 0.94),
        (article.get("rss_published_at") or article.get("pub_date"), "rss", True, False, 0.92),
        (article.get("api_published_at") or article.get("structured_timestamp"), "structured_api", True, False, 0.92),
        (article.get("published_at"), str(article.get("published_at_source") or "provider_timestamp"), bool(article.get("published_at_verified", True)), bool(article.get("timestamp_inferred")), float(article.get("timestamp_confidence") or 0.9)),
    )
    for value, source, verified, inferred, confidence in candidates:
        if parsed := _iso_datetime(value):
            if source != "provider_timestamp" or article.get("published_at_source"):
                logger.info("news_timestamp_recovered", extra={"published_at_source": source, "published_at": parsed})
            return {
                "published_at": parsed,
                "published_at_source": source,
                "published_at_verified": verified,
                "timestamp_inferred": inferred,
                "timestamp_confidence": round(confidence, 3),
                "timestamp_status": "INFERRED" if inferred else "VERIFIED" if verified else "UNVERIFIED",
            }
    source_url = str(article.get("canonical_url") or article.get("source_url") or article.get("url") or "")
    if url_date := _date_from_url(source_url):
        parsed = _iso_datetime(url_date)
        logger.info("news_timestamp_recovered", extra={"published_at_source": "url_date", "published_at": parsed})
        return {
            "published_at": parsed,
            "published_at_source": "url_date",
            "published_at_verified": False,
            "timestamp_inferred": True,
            "timestamp_confidence": 0.62,
            "timestamp_status": "INFERRED",
        }
    for value, source, confidence in (
        (article.get("aggregator_published_at"), "aggregator_timestamp", 0.72),
        (article.get("source_page_published_at"), "source_page", 0.82),
        (article.get("retrieved_at"), "retrieved_at_fallback", 0.35),
    ):
        if parsed := _iso_datetime(value):
            logger.info("news_timestamp_recovered", extra={"published_at_source": source, "published_at": parsed})
            return {
                "published_at": parsed,
                "published_at_source": source,
                "published_at_verified": False,
                "timestamp_inferred": True,
                "timestamp_confidence": confidence,
                "timestamp_status": "INFERRED",
            }
    return {
        "published_at": None,
        "published_at_source": None,
        "published_at_verified": False,
        "timestamp_inferred": False,
        "timestamp_confidence": 0.0,
        "timestamp_status": "MISSING",
    }


def normalize_news_article(raw: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    article = dict(raw)
    article["title"] = clean_text(article.get("title")) or ""
    article["summary"] = _clean_summary(article.get("summary") or article.get("content_snippet"))
    article["retrieved_at"] = _iso_datetime(article.get("retrieved_at")) or now.replace(microsecond=0).isoformat()
    article.update(_recover_timestamp(article, now=now))
    article["provider_type"] = str(article.get("provider_type") or "RSS").split(".")[-1]
    logger.info("news_article_received", extra=_log_fields(article))

    article.update(classify_news_source(article))
    logger.info("news_source_classified", extra=_log_fields(article))
    article.update(extract_entities(article))
    article["topic_classifications"] = classify_news_topics(article)
    article["topics"] = [row["topic"] for row in article["topic_classifications"]]
    logger.info("news_topic_classified", extra=_log_fields(article))

    summary_type = article.get("summary_source_type")
    if article["summary"] and not summary_type:
        summary_type = "provider_metadata"
    article["summary_source_type"] = summary_type
    article["summary_source_url"] = article.get("summary_source_url") or (article.get("source_url") if article["summary"] else None)
    article["summary_is_generated"] = bool(article.get("summary_is_generated") or str(summary_type or "").startswith("ai_"))
    article["summary_quality"] = _summary_quality(article["summary"])
    article["summary_reliability"] = round(
        max(0.0, min(1.0, article["summary_quality"] * (0.88 if article["summary_is_generated"] else 1.0))), 3
    )
    article["source_text_available"] = bool(article.get("source_text_available") or article["summary"])
    article["canonical_status"] = "canonical_resolved" if article.get("canonical_url") else "canonical_unresolved" if article.get("aggregator_url") else "canonical_unavailable"
    logger.info("news_metadata_extracted", extra=_log_fields(article))

    article.update(_score_article(article, now=now))
    logger.info("news_article_relevance_scored", extra=_log_fields(article))
    article["exclusion_reason"] = _exclusion_reason(article, now=now)
    article["accepted"] = article["exclusion_reason"] is None
    article["content_status"] = "invalid_content" if news_content_status(article) == "invalid_content" else "valid"
    article["article_id"] = _stable_hash(article.get("canonical_url") or f"{article.get('original_publisher')}:{_normalized_title(article.get('title'))}")
    article["duplicate_group_id"] = _syndication_key(article)
    article["syndication_group"] = article["duplicate_group_id"]
    article["is_duplicate"] = False
    article["duplicate_of"] = None
    article["independent_source_count"] = 1
    article["pipeline_version"] = PIPELINE_VERSION
    article["warnings"] = _article_warnings(article)
    logger.info("news_article_accepted" if article["accepted"] else "news_article_rejected", extra=_log_fields(article))
    return article


def build_news_context(news_items: list[dict[str, Any]], *, limit: int = 12, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    normalized = [normalize_news_article(_raw_article(item), now=now) for item in news_items]
    accepted = [item for item in normalized if item["accepted"]]
    excluded = [_compact_exclusion(item) for item in normalized if not item["accepted"]]
    representatives, duplicates = _deduplicate_articles(accepted)
    excluded.extend(_compact_exclusion(item) for item in duplicates)
    clusters = _build_clusters(representatives)
    cluster_by_article = {article_id: cluster["cluster_id"] for cluster in clusters for article_id in cluster["article_ids"]}
    for item in representatives:
        item["cluster_id"] = cluster_by_article.get(item["article_id"])
    representatives.sort(key=lambda item: (item.get("relevance_score") or 0, item.get("published_at") or ""), reverse=True)
    latest = representatives[:limit]
    by_topic = _group_items(latest, "topics")
    by_symbol = _group_items(latest, "symbols")
    official = [item for item in latest if item.get("is_official_source")]
    market = [item for item in latest if not item.get("is_official_source")]
    diagnostics = _diagnostics(normalized, representatives, excluded, duplicates, clusters)
    quality = _news_quality(normalized, representatives, excluded, clusters)
    context = {
        "status": "available" if latest else "no_data_available",
        "pipeline_version": PIPELINE_VERSION,
        "latest": latest,
        "directly_relevant": [item for item in latest if item.get("relevance_tier") == "direct"],
        "supporting": [item for item in latest if item.get("relevance_tier") == "supporting"],
        "by_topic": by_topic,
        "by_symbol": by_symbol,
        "official_sources": official,
        "market_sources": market,
        "excluded": excluded[:100],
        "duplicates": [_compact_duplicate(item) for item in duplicates[:100]],
        "clusters": clusters,
        "diagnostics": diagnostics,
        "quality": quality,
        "summary_coverage_pct": quality["summary_coverage_pct"],
        "published_at_coverage_pct": quality["published_at_coverage_pct"],
        "canonical_url_coverage_pct": quality["canonical_url_coverage_pct"],
    }
    context["digest"] = build_news_digest(context)
    return context


def build_news_digest(news_context: dict[str, Any], *, coverage_window_hours: int = 24) -> dict[str, Any]:
    latest = list(news_context.get("latest") or [])
    clusters = list(news_context.get("clusters") or [])
    diagnostics = dict(news_context.get("diagnostics") or {})
    quality = dict(news_context.get("quality") or {})
    topic_counts = Counter(topic for item in latest for topic in item.get("topics") or [])
    topic_rows = [
        {
            "topic": topic,
            "article_count": count,
            "weighted_relevance": round(sum(float(item.get("relevance_score") or 0) for item in latest if topic in (item.get("topics") or [])) / count, 3),
        }
        for topic, count in sorted(topic_counts.items())
    ]
    drivers = [_cluster_driver(cluster) for cluster in clusters if cluster.get("representative_articles")]
    drivers.sort(key=lambda row: (row["confidence"], row["reliability"]), reverse=True)
    reliability = round(sum(float(item.get("reliability") or 0) for item in latest) / len(latest), 3) if latest else 0.0
    confirmation_ratio = sum(1 for cluster in clusters if cluster.get("confirmed")) / len(clusters) if clusters else 0.0
    confidence = round(min(0.98, reliability * 0.55 + float(quality.get("news_quality_score") or 0) * 0.3 + confirmation_ratio * 0.15), 3) if latest else 0.0
    return {
        "status": "available" if latest else "no_data_available",
        "pipeline_version": PIPELINE_VERSION,
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "coverage_window_hours": coverage_window_hours,
        "candidate_article_count": int(diagnostics.get("raw_article_count") or 0),
        "accepted_article_count": int(diagnostics.get("accepted_count") or len(latest)),
        "excluded_article_count": int(diagnostics.get("excluded_count") or 0),
        "duplicate_article_count": int(diagnostics.get("duplicate_count") or 0),
        "cluster_count": len(clusters),
        "official_source_count": int(diagnostics.get("official_source_count") or 0),
        "high_reliability_source_count": int(diagnostics.get("high_reliability_source_count") or 0),
        "source_count": len({item.get("original_publisher") for item in latest if item.get("original_publisher")}),
        "reliability": reliability,
        "confidence": confidence,
        "topics": topic_rows,
        "drivers": drivers,
        "excluded_summary": dict(diagnostics.get("exclusion_breakdown") or {}),
        "quality": quality,
        "warnings": [] if drivers else ["news_digest_insufficient_sources"],
    }


def news_snapshot_valid_until(context: dict[str, Any], *, now: datetime | None = None) -> str:
    now = now or datetime.now(UTC)
    latest = context.get("latest") or []
    topics = {topic for item in latest for topic in item.get("topics") or []}
    high_impact = any(
        float(item.get("relevance_score") or 0) >= 0.8
        or (
            item.get("is_primary_source")
            and set(item.get("topics") or []).intersection({"macro", "inflation", "fed", "yields"})
        )
        for item in latest
    )
    if high_impact:
        hours = 2
    elif topics.intersection({"macro", "fed", "yields", "mega-cap", "semiconductors"}):
        hours = 6
    elif "earnings" in topics:
        hours = 8
    else:
        hours = 12
    return (now + timedelta(hours=hours)).replace(microsecond=0).isoformat()


def _score_article(article: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    topics = set(article.get("topics") or [])
    symbols = set(article.get("symbols") or [])
    source_quality = float(article.get("source_reliability_base") or 0)
    direct = 1.0 if topics.intersection({"macro", "fed", "yields"}) or symbols.intersection(ENTITY_ALIASES) else 0.75 if topics.intersection({"geopolitics", "energy"}) else 0.0
    macro = 1.0 if "macro" in topics or "inflation" in topics else 0.0
    mega = 1.0 if symbols.intersection(ENTITY_ALIASES) else 0.0
    semi = 1.0 if "semiconductors" in topics else 0.0
    rates = 1.0 if topics.intersection({"fed", "yields"}) else 0.0
    freshness = _freshness_quality(article.get("published_at"), now=now)
    completeness = (0.55 if article.get("summary") else 0.0) + (0.25 if article.get("published_at") else 0.0) + (0.2 if article.get("canonical_url") else 0.0)
    score = direct * 0.24 + macro * 0.15 + mega * 0.15 + semi * 0.15 + rates * 0.13 + source_quality * 0.1 + freshness * 0.04 + completeness * 0.04
    if article.get("is_primary_source") and topics.intersection({"macro", "inflation", "fed", "yields"}):
        score += 0.22
    if "geopolitics" in topics and ("semiconductors" in topics or symbols):
        score += 0.1
    if "earnings" in topics and symbols:
        score += 0.12
    if "yields" in topics and article.get("source_classification") in {"official_source", "major_news_agency", "major_financial_media"}:
        score += 0.14
    personal_reason = _personal_finance_reason(f"{article.get('title')} {article.get('summary') or ''}".lower())
    if personal_reason:
        score = 0.0
    if _analyst_rating_only(article):
        score = min(score, 0.28)
    if _promotional_or_listicle(article):
        score = min(score, 0.3)
    if not article.get("published_at"):
        score = max(0.0, score - 0.22)
    elif article.get("timestamp_inferred"):
        score = max(0.0, score - (1.0 - float(article.get("timestamp_confidence") or 0.0)) * 0.16)
    if not article.get("summary"):
        score = max(0.0, score - 0.12)
    reliability_factors = {
        "source_base": source_quality,
        "published_at_penalty": -0.12 if not article.get("published_at") else 0.0,
        "summary_penalty": -0.08 if not article.get("summary") else 0.0,
        "canonical_penalty": -0.05 if not article.get("canonical_url") else 0.0,
        "content_access_penalty": -0.03 if not article.get("source_text_available") else 0.0,
    }
    reliability = max(0.0, min(1.0, sum(reliability_factors.values())))
    score = min(score, 1.0)
    level = "HIGH" if score >= 0.68 else "MEDIUM" if score >= 0.52 else "LOW"
    reasons = [
        name for name, value in {
            "direct_mnq_relevance": direct,
            "macro_impact": macro,
            "mega_cap_impact": mega,
            "semiconductor_impact": semi,
            "fed_rates_impact": rates,
            "source_quality": source_quality,
            "freshness_quality": freshness,
            "content_completeness": completeness,
        }.items() if value >= 0.5
    ]
    market_impact = max(macro, mega, semi, rates, 0.75 if topics.intersection({"geopolitics", "energy"}) else 0.0)
    topic_score = max((float(row.get("topic_score") or 0) for row in article.get("topic_classifications") or []), default=0.0)
    noise_penalty = 1.0 if personal_reason else 0.7 if _promotional_or_listicle(article) else 0.5 if _analyst_rating_only(article) else 0.0
    return {
        "direct_mnq_relevance": direct,
        "macro_impact": macro,
        "mega_cap_impact": mega,
        "semiconductor_impact": semi,
        "fed_rates_impact": rates,
        "source_quality": source_quality,
        "freshness_quality": freshness,
        "content_completeness": round(completeness, 3),
        "relevance_score": round(score, 3),
        "relevance": level,
        "relevance_tier": "direct" if score >= 0.75 else "supporting" if score >= 0.52 else "excluded",
        "relevance_reasons": reasons,
        "reliability": round(reliability, 3),
        "confidence": round(min(score, reliability), 3),
        "reliability_factors": reliability_factors,
        "mnq_relevance_score": round(direct, 3),
        "market_impact_score": round(market_impact, 3),
        "source_quality_score": round(source_quality, 3),
        "recency_score": round(freshness, 3),
        "topic_score": round(topic_score, 3),
        "duplicate_penalty": 0.0,
        "noise_penalty": noise_penalty,
        "final_acceptance_score": round(score, 3),
    }


def _exclusion_reason(article: dict[str, Any], *, now: datetime) -> str | None:
    if news_content_status(article) == "invalid_content":
        return "missing_content"
    personal = _personal_finance_reason(f"{article.get('title')} {article.get('summary') or ''}".lower())
    if personal:
        return personal
    if _analyst_rating_only(article):
        return "analyst_rating_only"
    if _promotional_or_listicle(article):
        return "low_relevance"
    if not article.get("published_at"):
        return "missing_timestamp"
    if (
        article.get("published_at_source") == "retrieved_at_fallback"
        and article.get("source_classification") in {"aggregator", "secondary_financial_media", "low_quality_or_unknown"}
    ):
        return "timestamp_unverified"
    published = parse_datetime(article.get("published_at"))
    if published and published > now + timedelta(minutes=5):
        return "invalid_timestamp"
    valid_until = parse_datetime(article.get("valid_until"))
    weekend_last_session = bool(
        now.weekday() >= 5
        and published
        and now - published <= timedelta(hours=72)
    )
    if valid_until and valid_until < now and not weekend_last_session:
        return "stale_or_expired"
    if published and now - published > timedelta(days=14):
        return "stale_or_expired"
    if not article.get("summary") and article.get("source_classification") in {"aggregator", "secondary_financial_media", "low_quality_or_unknown"}:
        return "missing_content"
    if not article.get("topics") and not article.get("symbols"):
        return "ambiguous_topic"
    if article.get("source_classification") == "low_quality_or_unknown" and float(article.get("reliability") or 0) < 0.4:
        return "low_source_quality"
    if float(article.get("relevance_score") or 0) < 0.52:
        return "irrelevant_company" if article.get("symbols") else "low_relevance"
    return None


def _deduplicate_articles(articles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    aliases: dict[str, str] = {}
    for article in articles:
        canonical_key = _stable_hash(f"url:{article.get('canonical_url')}") if article.get("canonical_url") else None
        publisher_key = _stable_hash(
            f"publisher_title:{str(article.get('original_publisher') or article.get('source') or 'unknown').lower()}:{_normalized_title(article.get('title'))}"
        )
        known = next((aliases[key] for key in (canonical_key, publisher_key) if key and key in aliases), None)
        group_id = known or canonical_key or publisher_key
        groups.setdefault(group_id, []).append(article)
        aliases[publisher_key] = group_id
        if canonical_key:
            aliases[canonical_key] = group_id
        article["duplicate_group_id"] = group_id
        article["syndication_group"] = group_id
    representatives: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    for group_id, items in groups.items():
        items.sort(key=lambda item: (float(item.get("reliability") or 0), bool(item.get("canonical_url")), bool(item.get("summary"))), reverse=True)
        representative = items[0]
        representative["independent_source_count"] = 1
        representatives.append(representative)
        for duplicate in items[1:]:
            duplicate["is_duplicate"] = True
            duplicate["duplicate_of"] = representative["article_id"]
            duplicate["exclusion_reason"] = "duplicate"
            duplicate["accepted"] = False
            duplicates.append(duplicate)
            logger.info("news_article_deduplicated", extra=_log_fields(duplicate))
    return representatives, duplicates


def _build_clusters(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for article in articles:
        fingerprint = _event_fingerprint(article)
        article["event_fingerprint"] = fingerprint
        groups.setdefault(fingerprint, []).append(article)
    clusters = []
    for fingerprint, items in groups.items():
        publishers = {str(item.get("original_publisher") or item.get("source") or "").lower() for item in items}
        official_count = len({item.get("original_publisher") for item in items if item.get("is_official_source")})
        high_count = len({item.get("original_publisher") for item in items if float(item.get("reliability") or 0) >= 0.8})
        primary_present = any(item.get("is_primary_source") for item in items)
        independent_count = len(publishers)
        multiple = independent_count >= 2
        confirmed = multiple or primary_present
        best = max(items, key=lambda item: (float(item.get("reliability") or 0), float(item.get("relevance_score") or 0)))
        reliability = min(0.99, sum(float(item.get("reliability") or 0) for item in items) / len(items) + (0.04 if multiple else 0.0))
        confidence = min(0.98, reliability * (0.95 if confirmed else 0.72))
        cluster_id = _stable_hash(f"cluster:{fingerprint}")
        cluster = {
            "cluster_id": cluster_id,
            "event_fingerprint": fingerprint,
            "headline": best.get("title"),
            "summary": next((item.get("summary") for item in sorted(items, key=lambda row: float(row.get("summary_quality") or 0), reverse=True) if item.get("summary")), None),
            "topics": sorted({topic for item in items for topic in item.get("topics") or []}),
            "entities": sorted({entity for item in items for entity in item.get("entities") or []}),
            "symbols": sorted({symbol for item in items for symbol in item.get("symbols") or []}),
            "article_count": len(items),
            "independent_source_count": independent_count,
            "official_source_count": official_count,
            "high_reliability_source_count": high_count,
            "confidence": round(confidence, 3),
            "reliability": round(reliability, 3),
            "confirmed": confirmed,
            "is_confirmed_by_multiple_sources": multiple,
            "primary_source_present": primary_present,
            "article_ids": [item["article_id"] for item in items],
            "representative_articles": [
                {"article_id": item["article_id"], "title": item.get("title"), "source": item.get("source"), "source_url": item.get("canonical_url") or item.get("source_url")}
                for item in sorted(items, key=lambda row: float(row.get("reliability") or 0), reverse=True)[:3]
            ],
            "published_at_latest": max((item.get("published_at") for item in items if item.get("published_at")), default=None),
        }
        clusters.append(cluster)
        logger.info("news_cluster_created" if confirmed else "news_cluster_rejected", extra={"cluster_id": cluster_id, "event_fingerprint": fingerprint, "independent_source_count": independent_count, "reliability": cluster["reliability"]})
    return clusters


def _diagnostics(
    normalized: list[dict[str, Any]],
    accepted: list[dict[str, Any]],
    excluded: list[dict[str, Any]],
    duplicates: list[dict[str, Any]],
    clusters: list[dict[str, Any]],
) -> dict[str, Any]:
    reasons = Counter(str(item.get("reason") or "unknown") for item in excluded)
    values = {key: 0 for key in DIAGNOSTIC_KEYS}
    values.update({
        "raw_article_count": len(normalized),
        "metadata_complete_count": sum(bool(item.get("published_at") and item.get("summary") and item.get("canonical_url")) for item in normalized),
        "published_at_found_count": sum(bool(item.get("published_at")) for item in normalized),
        "summary_found_count": sum(bool(item.get("summary")) for item in normalized),
        "accepted_count": len(accepted),
        "excluded_count": len(excluded),
        "excluded_personal_finance_count": sum(reasons[name] for name in ("personal_finance", "mortgage", "deposit_rates")),
        "excluded_low_relevance_count": reasons["low_relevance"] + reasons["irrelevant_company"] + reasons["analyst_rating_only"],
        "excluded_missing_timestamp_count": reasons["missing_timestamp"],
        "duplicate_count": len(duplicates),
        "syndicated_duplicate_count": sum(1 for item in duplicates if item.get("aggregator_url")),
        "cluster_count": len(clusters),
        "confirmed_cluster_count": sum(bool(cluster.get("confirmed")) for cluster in clusters),
        "official_source_count": len({item.get("original_publisher") for item in accepted if item.get("is_official_source")}),
        "high_reliability_source_count": len({item.get("original_publisher") for item in accepted if float(item.get("reliability") or 0) >= 0.8}),
        "exclusion_breakdown": dict(sorted(reasons.items())),
    })
    return values


def _news_quality(
    normalized: list[dict[str, Any]],
    accepted: list[dict[str, Any]],
    excluded: list[dict[str, Any]],
    clusters: list[dict[str, Any]],
) -> dict[str, Any]:
    total = len(accepted)
    summary_pct = _pct(sum(bool(item.get("summary")) for item in accepted), total)
    published_pct = _pct(sum(bool(item.get("published_at")) for item in accepted), total)
    verified_published_pct = _pct(sum(bool(item.get("published_at_verified")) for item in accepted), total)
    timestamp_quality_pct = round(
        sum(float(item.get("timestamp_confidence") or 0.0) for item in accepted) / max(total, 1) * 100,
        2,
    )
    canonical_pct = _pct(sum(bool(item.get("canonical_url")) for item in accepted), total)
    high_pct = _pct(sum(float(item.get("reliability") or 0) >= 0.8 for item in accepted), total)
    official_count = len({item.get("original_publisher") for item in accepted if item.get("is_official_source")})
    high_count = len({item.get("original_publisher") for item in accepted if float(item.get("reliability") or 0) >= 0.8})
    independent_clusters = sum(int(cluster.get("independent_source_count") or 0) >= 2 for cluster in clusters)
    harmful_reasons = {"low_relevance", "irrelevant_company", "analyst_rating_only", "missing_content", "low_source_quality", "ambiguous_topic"}
    harmful_noise = sum(1 for item in excluded if item.get("reason") in harmful_reasons)
    noise_pct = _pct(harmful_noise, len(normalized))
    source_strength = min(1.0, (official_count * 0.5 + high_count * 0.3) / max(total, 1))
    cluster_strength = min(1.0, independent_clusters / max(len(clusters), 1))
    score = (
        summary_pct / 100 * 0.2
        + timestamp_quality_pct / 100 * 0.2
        + canonical_pct / 100 * 0.15
        + high_pct / 100 * 0.2
        + source_strength * 0.15
        + cluster_strength * 0.1
        - min(noise_pct / 100 * 0.1, 0.1)
    )
    score = round(max(0.0, min(0.98, score)), 3)
    return {
        "news_quality_score": score,
        "completeness_score": score,
        "summary_coverage_pct": summary_pct,
        "published_at_coverage_pct": published_pct,
        "published_at_verified_coverage_pct": verified_published_pct,
        "timestamp_quality_pct": timestamp_quality_pct,
        "canonical_url_coverage_pct": canonical_pct,
        "high_quality_article_pct": high_pct,
        "high_reliability_source_count": high_count,
        "official_source_count": official_count,
        "independent_cluster_count": independent_clusters,
        "noise_rejection_count": harmful_noise,
        "noise_rejection_pct": noise_pct,
        "missing_fields": [] if total else ["latest"],
    }


def _cluster_driver(cluster: dict[str, Any]) -> dict[str, Any]:
    representatives = cluster.get("representative_articles") or []
    urls = [item.get("source_url") for item in representatives if item.get("source_url")]
    return {
        "driver_id": cluster.get("cluster_id"),
        "category": _driver_category(cluster.get("topics") or []),
        "headline": cluster.get("headline"),
        "summary": cluster.get("summary"),
        "affected_symbols": cluster.get("symbols") or [],
        "source_count": cluster.get("article_count"),
        "independent_source_count": cluster.get("independent_source_count"),
        "source_urls": urls,
        "confidence": cluster.get("confidence"),
        "reliability": cluster.get("reliability"),
        "is_confirmed_by_multiple_sources": cluster.get("is_confirmed_by_multiple_sources"),
        "primary_source_present": cluster.get("primary_source_present"),
        "published_at_latest": cluster.get("published_at_latest"),
    }


def _event_fingerprint(article: dict[str, Any]) -> str:
    text = _normalized_text(f"{article.get('title')} {article.get('summary') or ''}")
    signatures = (
        ("cpi_release", ("consumer price index", " cpi ")),
        ("ppi_release", ("producer price index", " ppi ")),
        ("pce_release", ("pce price", "core pce")),
        ("nfp_release", ("nonfarm payroll", "employment situation")),
        ("fomc", ("fomc", "federal reserve decision")),
        ("treasury_auction", ("treasury auction",)),
        ("treasury_yield_move", ("treasury yield", "bond selloff", "bond rally", "yield curve")),
        ("export_controls", ("export control", "export restriction")),
        ("earnings_results", ("earnings", "quarterly results", "revenue", " eps ")),
        ("earnings_guidance", ("guidance", "profit forecast", "capex")),
        ("antitrust", ("antitrust",)),
    )
    signature = next((name for name, terms in signatures if any(term in text for term in terms)), None)
    if signature is None:
        meaningful = [token for token in text.split() if len(token) > 4 and token not in {"nasdaq", "market", "stocks", "today", "latest"}]
        signature = "title:" + ":".join(meaningful[:6])
    entities = ":".join(sorted(article.get("symbols") or [])) or "systemic"
    return _stable_hash(f"{signature}:{entities}")


def _syndication_key(article: dict[str, Any]) -> str:
    canonical = article.get("canonical_url")
    if canonical:
        return _stable_hash(f"url:{canonical}")
    publisher = str(article.get("original_publisher") or article.get("source") or "unknown").lower()
    return _stable_hash(f"syndication:{publisher}:{_normalized_title(article.get('title'))}")


def _compact_exclusion(article: dict[str, Any]) -> dict[str, Any]:
    return {
        "article_id": article.get("article_id"),
        "title": article.get("title"),
        "source": article.get("source"),
        "source_classification": article.get("source_classification"),
        "reason": article.get("exclusion_reason"),
        "relevance_score": article.get("relevance_score"),
        "published_at": article.get("published_at"),
    }


def _compact_duplicate(article: dict[str, Any]) -> dict[str, Any]:
    return {
        **_compact_exclusion(article),
        "duplicate_group_id": article.get("duplicate_group_id"),
        "duplicate_of": article.get("duplicate_of"),
        "syndication_group": article.get("syndication_group"),
    }


def _raw_article(item: dict[str, Any]) -> dict[str, Any]:
    raw = item.get("raw_payload") if isinstance(item.get("raw_payload"), dict) else {}
    return {**item, **raw}


def _group_items(items: list[dict[str, Any]], field: str) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        for value in item.get(field) or []:
            key = str(value).lower().replace(" ", "_") if field == "topics" else str(value).upper()
            output.setdefault(key, []).append(item)
    return output


def _article_warnings(article: dict[str, Any]) -> list[str]:
    warnings = list(article.get("warnings") or [])
    for condition, warning in (
        (not article.get("published_at"), "published_at_missing"),
        (not article.get("summary"), "summary_missing"),
        (not article.get("summary"), "summary_source_unavailable"),
        (not article.get("canonical_url"), "canonical_unresolved"),
    ):
        if condition and warning not in warnings:
            warnings.append(warning)
    return warnings


def _personal_finance_reason(text: str) -> str | None:
    for pattern, reason in PERSONAL_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return reason
    return None


def _analyst_rating_only(article: dict[str, Any]) -> bool:
    text = _normalized_text(f"{article.get('title')} {article.get('summary') or ''}")
    rating = any(term in text for term in ("price target", "analyst rating", "upgrades", "downgrades", "reiterates", "initiates coverage"))
    material = any(term in text for term in ("earnings", "guidance", "revenue", "export control", "acquisition", "sec filing"))
    return rating and not material


def _promotional_or_listicle(article: dict[str, Any]) -> bool:
    text = _normalized_text(article.get("title"))
    return any(
        term in text
        for term in (
            "top stock pick", "stocks to buy", "best stocks", "millionaire maker", "buy this stock",
            "could make you rich", "protect your riches", "must own stocks", "best growth stocks",
        )
    )


def _is_company_ir(article: dict[str, Any], domain: str) -> bool:
    text = f"{article.get('source') or ''} {article.get('original_publisher') or ''} {article.get('source_url') or ''}".lower()
    return "investor relations" in text or "/investor" in text or domain.startswith("investor.") or domain.startswith("ir.")


def _publisher_from_title(title: str) -> str | None:
    match = re.search(r"\s[-|]\s(Reuters|Associated Press|AP News|Bloomberg|Dow Jones)\s*$", title, flags=re.IGNORECASE)
    return match.group(1) if match else None


def _normalized_title(value: Any) -> str:
    text = _normalized_text(value)
    text = re.sub(r"\s(?:reuters|associated press|ap news|yahoo finance|msn)$", "", text)
    return text


def _normalized_text(value: Any) -> str:
    return f" {re.sub(r'[^a-z0-9]+', ' ', str(value or '').lower()).strip()} "


def _clean_summary(value: Any) -> str | None:
    text = clean_text(value)
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    if not text or len(text) < 20:
        return None
    return text[:2000]


def _summary_quality(summary: str | None) -> float:
    if not summary:
        return 0.0
    length = len(summary)
    return 0.95 if length >= 160 else 0.82 if length >= 80 else 0.65


def _freshness_quality(value: Any, *, now: datetime) -> float:
    published = parse_datetime(value)
    if published is None:
        return 0.0
    hours = max(0.0, (now - published).total_seconds() / 3600)
    return 1.0 if hours <= 6 else 0.9 if hours <= 24 else 0.7 if hours <= 72 else 0.45 if hours <= 24 * 7 else 0.2


def _iso_datetime(value: Any) -> str | None:
    parsed = parse_datetime(value)
    return parsed.replace(microsecond=0).isoformat() if parsed else None


def _ld_type(row: dict[str, Any]) -> str:
    value = row.get("@type")
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value or "").lower()


def _author_name(value: Any) -> str | None:
    if isinstance(value, dict):
        return str(value.get("name") or "") or None
    if isinstance(value, list):
        names = [_author_name(item) for item in value]
        return ", ".join(name for name in names if name) or None
    return str(value or "") or None


def _date_from_url(value: str) -> str | None:
    match = re.search(r"/(20\d{2})/(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])(?:/|$)", value)
    if not match:
        return None
    try:
        return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)), tzinfo=UTC).isoformat()
    except ValueError:
        return None


def _dedupe_topic_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        existing = output.get(row["topic"])
        if existing is None or row["topic_score"] > existing["topic_score"]:
            output[row["topic"]] = row
    return [output[key] for key in sorted(output)]


def _driver_category(topics: list[str]) -> str:
    for topic, category in (("semiconductors", "SEMICONDUCTORS"), ("earnings", "EARNINGS"), ("fed", "FED"), ("yields", "RATES"), ("macro", "MACRO"), ("inflation", "MACRO")):
        if topic in topics:
            return category
    return "MEGA_CAP"


def _pct(count: int, total: int) -> float:
    return round(count / total * 100, 2) if total else 0.0


def _stable_hash(value: Any) -> str:
    return hashlib.sha1(str(value or "").encode("utf-8")).hexdigest()[:16]


def _log_fields(article: dict[str, Any]) -> dict[str, Any]:
    return {
        "article_id": article.get("article_id"),
        "title": str(article.get("title") or "")[:180],
        "source": article.get("source"),
        "original_publisher": article.get("original_publisher"),
        "source_classification": article.get("source_classification"),
        "published_at": article.get("published_at"),
        "topics": article.get("topics") or [],
        "symbols": article.get("symbols") or [],
        "relevance_score": article.get("relevance_score"),
        "reliability": article.get("reliability"),
        "exclusion_reason": article.get("exclusion_reason"),
        "duplicate_group_id": article.get("duplicate_group_id"),
        "cluster_id": article.get("cluster_id"),
    }
