from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from app.core.config import Settings
from app.providers.news_provider import parse_rss_articles
from app.services.market_fact_repository import MarketFactRepository
from app.services.news_intelligence_runtime_service import NEWS_SNAPSHOT_KEY, NewsIntelligenceRuntimeService
from app.services.news_intelligence_service import (
    build_news_context,
    build_news_digest,
    classify_news_source,
    extract_entities,
    extract_page_metadata,
    news_snapshot_valid_until,
    normalize_news_article,
)

NOW = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
PUBLISHED = "2026-07-11T10:00:00Z"


def cfg(tmp_path) -> Settings:
    return Settings(_env_file=None, database_path=tmp_path / "market.sqlite")


def article(
    title: str,
    *,
    source: str = "Reuters",
    summary: str = "A verified report describes a material development affecting United States markets.",
    url: str | None = None,
    published_at: str | None = PUBLISHED,
    **extra,
) -> dict:
    slug = abs(hash((title, source)))
    return {
        "title": title,
        "source": source,
        "source_url": url or f"https://example.test/{slug}",
        "url": url or f"https://example.test/{slug}",
        "summary": summary,
        "published_at": published_at,
        "retrieved_at": NOW.isoformat(),
        "provider_type": "RSS",
        **extra,
    }


@pytest.mark.parametrize(
    ("raw", "topics", "excluded", "absent"),
    [
        (article("Treasury 10Y yield rises after bond selloff"), {"yields"}, None, set()),
        (article("Treasury auction draws weak demand and lifts government debt yields"), {"yields"}, None, set()),
        (article("Fed repricing drives Treasury yields higher after FOMC comments"), {"yields", "fed"}, None, set()),
        (article("Best CD rates today", source="Yahoo Personal Finance"), set(), "deposit_rates", set()),
        (article("Top high-yield savings rates this week", source="Yahoo Personal Finance"), set(), "deposit_rates", set()),
        (article("Mortgage rates and refinancing offers today", source="Yahoo Personal Finance"), set(), "mortgage", set()),
        (article("How to choose a HELOC", source="Yahoo Personal Finance"), set(), "personal_finance", set()),
        (article("Apple dividend yield reaches a five-year high"), {"mega-cap"}, None, {"yields"}),
        (article("Analyst reiterates Nvidia price target"), {"mega-cap"}, "analyst_rating_only", {"yields"}),
        (article("BLS CPI release shows inflation cooling", source="BLS", url="https://www.bls.gov/news.release/cpi.htm"), {"inflation", "macro"}, None, set()),
        (article("Apple earnings margins pressured by fuel costs"), {"earnings", "mega-cap"}, None, {"inflation"}),
        (article("Apple reports earnings and raises guidance"), {"earnings", "mega-cap"}, None, set()),
        (article("Nvidia faces new China export controls on AI chips"), {"semiconductors", "mega-cap", "geopolitics"}, None, set()),
        (article("Tiny mining company opens a local office"), set(), "ambiguous_topic", set()),
    ],
)
def test_topic_classification_matrix(raw, topics, excluded, absent):
    normalized = normalize_news_article(raw, now=NOW)
    assert topics.issubset(set(normalized["topics"]))
    assert absent.isdisjoint(set(normalized["topics"]))
    assert normalized["exclusion_reason"] == excluded


def test_federal_reserve_enforcement_action_is_not_policy_news():
    normalized = normalize_news_article(article("Federal Reserve Board issues enforcement action with a small community bank"), now=NOW)
    assert "fed" not in normalized["topics"]
    assert "macro" not in normalized["topics"]
    assert normalized["exclusion_reason"] == "ambiguous_topic"


@pytest.mark.parametrize(
    ("raw", "classification", "official", "primary", "source"),
    [
        (article("FOMC statement", source="Federal Reserve", url="https://www.federalreserve.gov/newsevents/pressreleases/test.htm"), "official_source", True, True, "Federal Reserve"),
        (article("CPI release", source="BLS", url="https://www.bls.gov/news.release/cpi.htm"), "official_source", True, True, "BLS"),
        (article("Nvidia export story", source="Reuters", url="https://finance.yahoo.com/news/nvidia-story"), "major_news_agency", False, False, "Reuters"),
        (article("Fed story - AP News", source="Yahoo Finance", url="https://finance.yahoo.com/news/fed-story"), "major_news_agency", False, False, "AP News"),
        (article("Stock screen", source="Insider Monkey"), "secondary_financial_media", False, False, "Insider Monkey"),
        (article("Best savings rates", source="Yahoo Personal Finance", url="https://finance.yahoo.com/personal-finance"), "personal_finance", False, False, "Yahoo Personal Finance"),
        (article("Apple quarterly results", source="Apple Investor Relations", url="https://investor.apple.com/results"), "primary_market_source", False, True, "Apple Investor Relations"),
        (article("Unknown market post", source="Random Blog", url="https://randomblog.test/post"), "low_quality_or_unknown", False, False, "Random Blog"),
    ],
)
def test_source_taxonomy_matrix(raw, classification, official, primary, source):
    classified = classify_news_source(raw)
    assert classified["source_classification"] == classification
    assert classified["is_official_source"] is official
    assert classified["is_primary_source"] is primary
    assert classified["original_publisher"] == source
    if "yahoo.com" in raw["source_url"]:
        assert classified["aggregator_url"] == raw["source_url"]


def test_exact_official_publisher_name_is_sufficient_without_wrapper_domain():
    classified = classify_news_source(article("CPI release", source="BLS", url="https://feed-proxy.test/item"))
    assert classified["source_classification"] == "official_source"


def test_rss_pubdate_is_extracted_before_other_timestamp_sources():
    xml = """<rss><channel><item><title>CPI report</title><link>https://bls.gov/cpi</link><pubDate>Sat, 11 Jul 2026 10:00:00 GMT</pubDate><description>Consumer Price Index details from the official release.</description></item></channel></rss>"""
    rows, _ = parse_rss_articles(xml, symbols=[], limit=5, source_name="BLS RSS", reliability=0.9)
    assert rows[0]["published_at"] == "2026-07-11T10:00:00+00:00"
    assert rows[0]["published_at_source"] == "rss_pub_date"


def test_atom_published_is_extracted():
    xml = """<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Fed update</title><link href="https://fed.test/a"/><published>2026-07-11T09:00:00Z</published><summary>Federal Reserve policy update with attributable details.</summary></entry></feed>"""
    rows, _ = parse_rss_articles(xml, symbols=[], limit=5, source_name="Federal Reserve RSS", reliability=0.9)
    assert rows[0]["published_at"] == "2026-07-11T09:00:00+00:00"
    assert rows[0]["published_at_source"] == "atom_published"


def test_json_ld_date_published_is_extracted():
    html = '<script type="application/ld+json">{"@type":"NewsArticle","datePublished":"2026-07-11T08:00:00Z","description":"A detailed market report from a named publisher.","author":{"name":"Jane Doe"}}</script>'
    metadata = extract_page_metadata(html, page_url="https://news.test/story")
    assert metadata["published_at"] == "2026-07-11T08:00:00+00:00"
    assert metadata["published_at_source"] == "json_ld"
    assert metadata["author"] == "Jane Doe"


def test_open_graph_published_time_is_fallback():
    html = '<meta property="article:published_time" content="2026-07-11T07:00:00Z"><meta property="og:description" content="A sufficiently detailed OpenGraph summary for this market article.">'
    metadata = extract_page_metadata(html, page_url="https://news.test/story")
    assert metadata["published_at"] == "2026-07-11T07:00:00+00:00"
    assert metadata["published_at_source"] == "opengraph_or_article_meta"


def test_markup_time_canonical_link_and_article_text_are_fallbacks():
    html = '<link rel="canonical" href="https://news.test/canonical"><time datetime="2026-07-11T06:00:00Z"></time><article>This attributable article body contains enough detailed market information to create a faithful excerpt without using the title alone.</article>'
    metadata = extract_page_metadata(html, page_url="https://news.test/wrapper")
    assert metadata["published_at_source"] == "markup_time"
    assert metadata["canonical_url"] == "https://news.test/canonical"
    assert metadata["summary_source_type"] == "page_text_excerpt"


def test_retrieved_at_is_last_resort_inferred_published_at():
    normalized = normalize_news_article(article("Apple earnings", published_at=None), now=NOW)
    assert normalized["published_at"] == NOW.isoformat()
    assert normalized["published_at_source"] == "retrieved_at_fallback"
    assert normalized["published_at_verified"] is False
    assert normalized["timestamp_inferred"] is True
    assert normalized["retrieved_at"] == NOW.isoformat()


def test_summary_from_rss_description_is_attributed():
    xml = """<rss><channel><item><title>Nvidia update</title><link>https://news.test/nvda</link><pubDate>Sat, 11 Jul 2026 10:00:00 GMT</pubDate><description>Reuters reports a material Nvidia semiconductor development.</description></item></channel></rss>"""
    rows, _ = parse_rss_articles(xml, symbols=[], limit=5, source_name="Reuters", reliability=0.9)
    assert rows[0]["summary"].startswith("Reuters reports")
    assert rows[0]["summary_source_type"] == "rss_description"


def test_summary_from_meta_description_is_attributed():
    html = '<meta name="description" content="A detailed attributable description for the market article.">'
    metadata = extract_page_metadata(html, page_url="https://news.test/story")
    assert metadata["summary_source_type"] == "meta_description"
    assert metadata["summary_source_url"] == "https://news.test/story"


def test_ai_summary_is_explicitly_marked_generated():
    normalized = normalize_news_article(article("Apple earnings", summary_source_type="ai_fallback", summary_is_generated=True), now=NOW)
    assert normalized["summary_is_generated"] is True
    assert normalized["summary_reliability"] < normalized["summary_quality"]


def test_title_only_does_not_create_summary():
    normalized = normalize_news_article(article("Nvidia export controls", summary=None), now=NOW)
    assert normalized["summary"] is None
    assert normalized["summary_source_type"] is None


@pytest.mark.parametrize(
    ("title", "symbols"),
    [
        ("Apple launches new product", {"AAPL"}),
        ("Broadcom raises AI chip outlook", {"AVGO"}),
        ("Nvidia export restrictions expand", {"NVDA"}),
        ("Alphabet updates cloud guidance", {"GOOG", "GOOGL"}),
        ("Microsoft (MSFT) reports results", {"MSFT"}),
        ("Armchair investors discuss yields", set()),
    ],
)
def test_entity_symbol_extraction_matrix(title, symbols):
    extracted = extract_entities(article(title))
    assert set(extracted["symbols"]) == symbols


@pytest.mark.parametrize(
    ("raw", "minimum", "excluded"),
    [
        (article("BLS CPI release shows inflation cooling", source="BLS", url="https://www.bls.gov/news.release/cpi.htm"), 0.75, None),
        (article("Federal Reserve issues FOMC policy decision", source="Federal Reserve", url="https://www.federalreserve.gov/newsevents/pressreleases/test.htm"), 0.75, None),
        (article("Nvidia faces new China export controls on AI chips"), 0.75, None),
        (article("Best CD rates today", source="Yahoo Personal Finance"), 0.0, "deposit_rates"),
        (article("Mortgage refinancing offers", source="Yahoo Personal Finance"), 0.0, "mortgage"),
        (article("Analyst reiterates Nvidia price target"), 0.0, "analyst_rating_only"),
        (article("Apple earnings update", summary=None, published_at=None), 0.0, "irrelevant_company"),
        (article("Major US bank failure raises systemic risk concerns"), 0.52, None),
    ],
)
def test_relevance_scoring_matrix(raw, minimum, excluded):
    normalized = normalize_news_article(raw, now=NOW)
    assert normalized["relevance_score"] >= minimum
    assert normalized["exclusion_reason"] == excluded


def _dedupe_fixture() -> list[dict]:
    return [
        article("Nvidia faces export controls", source="Reuters", url="https://reuters.test/nvda"),
        article("Nvidia faces export controls", source="Reuters", url="https://reuters.test/nvda"),
    ]


def test_equal_url_is_deduplicated():
    context = build_news_context(_dedupe_fixture(), now=NOW)
    assert len(context["latest"]) == 1
    assert context["diagnostics"]["duplicate_count"] == 1


def test_equal_normalized_title_and_publisher_is_deduplicated():
    rows = [article("Nvidia faces export controls", url="https://one.test/a"), article("NVIDIA faces export controls!", url="https://two.test/a")]
    context = build_news_context(rows, now=NOW)
    assert context["diagnostics"]["duplicate_count"] == 1


def test_reuters_through_two_aggregators_is_one_independent_source():
    rows = [
        article("Nvidia faces export controls", source="Reuters", url="https://finance.yahoo.com/news/a"),
        article("Nvidia faces export controls", source="Reuters", url="https://msn.com/news/a"),
    ]
    context = build_news_context(rows, now=NOW)
    assert context["diagnostics"]["duplicate_count"] == 1
    assert context["latest"][0]["independent_source_count"] == 1


def test_independent_sources_on_same_fact_remain_articles():
    rows = [article("Nvidia faces export controls", source="Reuters"), article("Nvidia faces export controls", source="Associated Press")]
    context = build_news_context(rows, now=NOW)
    assert len(context["latest"]) == 2
    assert context["clusters"][0]["independent_source_count"] == 2


def test_same_topic_different_facts_are_not_clustered():
    rows = [article("Treasury auction draws weak demand"), article("Treasury yields fall in a bond rally")]
    context = build_news_context(rows, now=NOW)
    assert context["diagnostics"]["cluster_count"] == 2


def test_same_corporate_event_forms_one_cluster():
    context = build_news_context([article("Nvidia faces export controls", source="Reuters"), article("US expands Nvidia export restrictions", source="Associated Press")], now=NOW)
    assert len(context["clusters"]) == 1


def test_different_events_for_same_company_form_different_clusters():
    context = build_news_context([article("Nvidia reports earnings beat"), article("Nvidia faces export controls")], now=NOW)
    assert len(context["clusters"]) == 2


def test_personal_finance_does_not_create_operational_cluster():
    context = build_news_context([article("Best CD rates today", source="Yahoo Personal Finance")], now=NOW)
    assert context["clusters"] == []


def test_confirmed_cluster_requires_independent_publishers():
    single = build_news_context([article("Nvidia faces export controls", source="Reuters")], now=NOW)["clusters"][0]
    multiple = build_news_context([article("Nvidia faces export controls", source="Reuters"), article("US expands Nvidia export restrictions", source="Associated Press")], now=NOW)["clusters"][0]
    assert single["confirmed"] is False
    assert multiple["confirmed"] is True


def test_single_primary_source_is_confirmed_without_false_multi_source():
    cluster = build_news_context([article("BLS CPI release", source="BLS", url="https://www.bls.gov/news.release/cpi.htm")], now=NOW)["clusters"][0]
    assert cluster["confirmed"] is True
    assert cluster["primary_source_present"] is True
    assert cluster["is_confirmed_by_multiple_sources"] is False


def test_low_summary_coverage_reduces_quality():
    complete = build_news_context([article("Apple reports earnings")], now=NOW)["quality"]
    sparse = build_news_context([article("Apple reports earnings", summary=None)], now=NOW)["quality"]
    assert sparse["news_quality_score"] < complete["news_quality_score"]


def test_low_published_at_coverage_reduces_quality():
    complete = build_news_context([article("Apple reports earnings")], now=NOW)["quality"]
    sparse = build_news_context([article("Apple reports earnings", published_at=None)], now=NOW)["quality"]
    assert sparse["news_quality_score"] < complete["news_quality_score"]


def test_correct_personal_finance_exclusions_do_not_count_as_harmful_noise():
    context = build_news_context([article("Apple reports earnings"), article("Best CD rates today", source="Yahoo Personal Finance")], now=NOW)
    assert context["quality"]["noise_rejection_count"] == 0


def test_ambiguous_noise_reduces_quality():
    clean = build_news_context([article("Apple reports earnings")], now=NOW)["quality"]
    noisy = build_news_context([article("Apple reports earnings"), article("Unrelated local business story")], now=NOW)["quality"]
    assert noisy["noise_rejection_count"] == 1
    assert noisy["news_quality_score"] < clean["news_quality_score"]


def test_official_source_increases_digest_reliability():
    official = build_news_context([article("BLS CPI release", source="BLS", url="https://www.bls.gov/news.release/cpi.htm")], now=NOW)["digest"]
    unknown = build_news_context([article("Apple reports earnings", source="Random Blog")], now=NOW)["digest"]
    assert official["reliability"] > unknown["reliability"]


def test_news_completeness_never_false_one():
    quality = build_news_context([article("BLS CPI release", source="BLS", url="https://www.bls.gov/news.release/cpi.htm")], now=NOW)["quality"]
    assert quality["completeness_score"] < 1.0


def _runtime_rows() -> list[dict]:
    return [
        article("BLS CPI release shows inflation cooling", source="BLS", url="https://www.bls.gov/news.release/cpi.htm"),
        article("Nvidia faces China export controls", source="Reuters"),
        article("Best CD rates today", source="Yahoo Personal Finance"),
    ]


def test_force_persists_digest(tmp_path):
    runtime = NewsIntelligenceRuntimeService(cfg(tmp_path))
    context, metrics = runtime.materialize(_runtime_rows(), refresh_mode="force")
    assert metrics["persisted_count"] == 1
    assert context["digest"]["accepted_article_count"] == 2


def test_new_connection_reads_digest(tmp_path):
    settings = cfg(tmp_path)
    NewsIntelligenceRuntimeService(settings).materialize(_runtime_rows(), refresh_mode="force")
    fact = MarketFactRepository(settings).get_fact(NEWS_SNAPSHOT_KEY)
    assert fact["raw_payload"]["news_digest"]["accepted_article_count"] == 2


def test_new_runtime_instance_materializes_digest(tmp_path):
    settings = cfg(tmp_path)
    NewsIntelligenceRuntimeService(settings).materialize(_runtime_rows(), refresh_mode="force")
    context, metrics = NewsIntelligenceRuntimeService(settings).materialize([], refresh_mode="false")
    assert metrics["cache_status"] == "hit"
    assert context["latest"]


def test_simulated_restart_false_preserves_values(tmp_path):
    settings = cfg(tmp_path)
    force, _ = NewsIntelligenceRuntimeService(settings).materialize(_runtime_rows(), refresh_mode="force")
    cached, _ = NewsIntelligenceRuntimeService(settings).materialize([], refresh_mode="false")
    assert force["digest"]["drivers"] == cached["digest"]["drivers"]


def test_provenance_survives_persistence(tmp_path):
    settings = cfg(tmp_path)
    NewsIntelligenceRuntimeService(settings).materialize(_runtime_rows(), refresh_mode="force")
    cached, _ = NewsIntelligenceRuntimeService(settings).materialize([], refresh_mode="false")
    assert cached["latest"][0]["source_classification"]
    assert "original_publisher" in cached["latest"][0]


def test_clusters_survive_persistence(tmp_path):
    settings = cfg(tmp_path)
    NewsIntelligenceRuntimeService(settings).materialize(_runtime_rows(), refresh_mode="force")
    cached, _ = NewsIntelligenceRuntimeService(settings).materialize([], refresh_mode="false")
    assert cached["clusters"]


def test_excluded_breakdown_survives_persistence(tmp_path):
    settings = cfg(tmp_path)
    NewsIntelligenceRuntimeService(settings).materialize(_runtime_rows(), refresh_mode="force")
    cached, _ = NewsIntelligenceRuntimeService(settings).materialize([], refresh_mode="false")
    assert cached["diagnostics"]["exclusion_breakdown"]["deposit_rates"] == 1


def test_legacy_news_rows_remain_readable():
    context = build_news_context([article("Apple reports earnings", topics=["earnings"], symbols=["AAPL"], reliability=0.7)], now=NOW)
    assert context["latest"][0]["symbols"] == ["AAPL"]


def test_refresh_false_declares_zero_network_browser_and_ai(tmp_path):
    settings = cfg(tmp_path)
    NewsIntelligenceRuntimeService(settings).materialize(_runtime_rows(), refresh_mode="force")
    context, _ = NewsIntelligenceRuntimeService(settings).materialize([], refresh_mode="false")
    assert context["metadata"]["provider_calls"] == 0
    assert context["metadata"]["browser_calls"] == 0
    assert context["metadata"]["AI_called"] is False


def test_refresh_auto_uses_valid_snapshot(tmp_path):
    settings = cfg(tmp_path)
    force, _ = NewsIntelligenceRuntimeService(settings).materialize(_runtime_rows(), refresh_mode="force")
    auto, metrics = NewsIntelligenceRuntimeService(settings).materialize([article("Apple reports earnings")], refresh_mode="auto")
    assert metrics["cache_status"] == "hit"
    assert auto["digest"]["drivers"] == force["digest"]["drivers"]


def test_refresh_force_bypasses_valid_snapshot(tmp_path):
    settings = cfg(tmp_path)
    NewsIntelligenceRuntimeService(settings).materialize(_runtime_rows(), refresh_mode="force")
    forced, metrics = NewsIntelligenceRuntimeService(settings).materialize([article("Apple reports earnings")], refresh_mode="force")
    assert metrics["cache_status"] == "refreshed"
    assert forced["latest"][0]["symbols"] == ["AAPL"]


def test_single_runtime_materialization_has_one_persistence(tmp_path):
    _, metrics = NewsIntelligenceRuntimeService(cfg(tmp_path)).materialize(_runtime_rows(), refresh_mode="force")
    assert metrics["persisted_count"] == 1
    assert metrics["read_back_count"] == 1


def test_expired_snapshot_refreshes_in_auto(tmp_path):
    settings = cfg(tmp_path)
    runtime = NewsIntelligenceRuntimeService(settings)
    runtime.materialize(_runtime_rows(), refresh_mode="force")
    fact = MarketFactRepository(settings).get_fact(NEWS_SNAPSHOT_KEY)
    MarketFactRepository(settings).upsert_fact({**fact, "valid_until": (NOW - timedelta(hours=1)).isoformat(), "raw_payload_json": fact["raw_payload"], "warnings_json": fact["warnings"], "errors_json": fact["errors"]})
    refreshed, metrics = NewsIntelligenceRuntimeService(settings).materialize([article("Apple reports earnings")], refresh_mode="auto")
    assert metrics["cache_status"] == "refreshed"
    assert refreshed["latest"][0]["symbols"] == ["AAPL"]


def test_high_impact_ttl_is_shorter_than_context_ttl():
    high = build_news_context([article("BLS CPI release", source="BLS", url="https://www.bls.gov/news.release/cpi.htm")], now=NOW)
    context = {"latest": [{"topics": ["earnings"], "relevance_score": 0.55}]}
    assert datetime.fromisoformat(news_snapshot_valid_until(high, now=NOW)) - NOW == timedelta(hours=2)
    assert datetime.fromisoformat(news_snapshot_valid_until(context, now=NOW)) - NOW == timedelta(hours=8)


def test_false_with_legacy_db_rows_still_has_no_external_calls(tmp_path):
    context, metrics = NewsIntelligenceRuntimeService(cfg(tmp_path)).materialize(_runtime_rows(), refresh_mode="false")
    assert metrics["cache_status"] == "legacy_db_materialized"
    assert context["metadata"]["provider_calls"] == 0


def test_serialized_body_contains_news_intelligence_fields():
    context = build_news_context(_runtime_rows(), now=NOW)
    body = json.loads(json.dumps({"news_context": context, "news_digest": build_news_digest(context)}, default=str))
    assert body["news_context"]["quality"]["news_quality_score"] >= 0
    assert body["news_digest"]["drivers"]


def test_contract_preserves_legacy_latest_key():
    context = build_news_context(_runtime_rows(), now=NOW)
    assert "latest" in context
    assert "by_topic" in context


def test_noise_is_absent_from_operational_latest():
    context = build_news_context(_runtime_rows(), now=NOW)
    assert all("CD rates" not in item["title"] for item in context["latest"])


def test_diagnostics_are_available():
    context = build_news_context(_runtime_rows(), now=NOW)
    assert set(("raw_article_count", "accepted_count", "excluded_count", "exclusion_breakdown")).issubset(context["diagnostics"])


def test_official_source_count_is_correct():
    context = build_news_context(_runtime_rows(), now=NOW)
    assert context["diagnostics"]["official_source_count"] == 1


def test_high_reliability_source_count_is_correct():
    context = build_news_context(_runtime_rows(), now=NOW)
    assert context["diagnostics"]["high_reliability_source_count"] == 2


def test_independent_source_count_is_correct():
    context = build_news_context([article("Nvidia faces export controls", source="Reuters"), article("US expands Nvidia export restrictions", source="Associated Press")], now=NOW)
    assert context["clusters"][0]["independent_source_count"] == 2
