from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest
import respx

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.providers.qqq_holdings_provider import QQQHoldingsProvider
from app.providers.sec_class_shares_provider import (
    latest_periodic_filing,
    parse_inline_xbrl_class_shares,
)
from app.services.context_extensions_service import enrich_nasdaq_context
from app.services.nasdaq_multiclass_service import (
    SEMANTICS_ISSUER_DUPLICATED,
    SEMANTICS_ISSUER_PROBABLE,
    SEMANTICS_SECURITY_VERIFIED,
    SEMANTICS_UNKNOWN,
    apply_multi_class_adjustments,
    classify_group_semantics,
    detect_multi_class_groups,
    normalize_issuer_name,
)
from app.services.qqq_weight_intelligence_service import (
    RECONSTRUCTED_MARKET_CAP_WEIGHT,
    concentration_metrics,
    reconstruct_market_cap_weights,
    sector_weight_exposure,
    weight_quality_score,
    weighted_contributions,
)


def alphabet_rows(*, cap_a=4_327_592_880_000, cap_c=4_301_543_480_000):
    return [
        {
            "symbol": "GOOGL",
            "companyName": "Alphabet Inc. Class A Common Stock",
            "marketCap": cap_a,
            "lastSalePrice": 357.18,
            "percentageChange": -0.48,
        },
        {
            "symbol": "GOOG",
            "companyName": "Alphabet Inc. Class C Capital Stock",
            "marketCap": cap_c,
            "lastSalePrice": 355.03,
            "percentageChange": -0.34,
        },
    ]


def sec_snapshot(*, verified=True):
    return {
        "status": "found" if verified else "not_found",
        "source": "SEC inline XBRL filing",
        "source_url": "https://sec.test/filing",
        "class_shares": {"A": 5_824_000_000, "B": 836_000_000, "C": 5_456_000_000},
        "listed_shares": {"GOOGL": 5_824_000_000, "GOOG": 5_456_000_000} if verified else {},
        "unlisted_classes": ["B"],
        "shares_as_of": "2026-04-22",
        "retrieved_at": "2026-07-11T00:00:00Z",
        "valid_until": "2026-07-12T00:00:00Z",
        "verified": verified,
    }


def test_goog_and_googl_grouped_same_issuer():
    groups = detect_multi_class_groups(alphabet_rows())
    assert len(groups) == 1
    assert groups[0]["issuer_group"] == "alphabet"
    assert set(groups[0]["symbols"]) == {"GOOG", "GOOGL"}


def test_different_companies_are_not_grouped():
    rows = [
        {"symbol": "AAA", "companyName": "Alpha Corp Class A"},
        {"symbol": "BBB", "companyName": "Beta Corp Class B"},
    ]
    assert detect_multi_class_groups(rows) == []


def test_company_name_fallback_is_prudent():
    rows = [
        {"symbol": "AAA", "companyName": "Example Inc. Class A Common Stock"},
        {"symbol": "AAC", "companyName": "Example Inc. Class C Capital Stock"},
    ]
    groups = detect_multi_class_groups(rows)
    assert len(groups) == 1
    assert groups[0]["detection_method"] == "normalized_company_name_with_explicit_share_classes"


def test_similar_names_without_share_classes_are_not_grouped():
    rows = [
        {"symbol": "AAA", "companyName": "Example Holdings"},
        {"symbol": "BBB", "companyName": "Example Holdings"},
    ]
    assert detect_multi_class_groups(rows) == []


@pytest.mark.parametrize(("cap_c", "expected"), [(4_327_592_880_000, True), (4_301_543_480_000, True)])
def test_equal_or_near_equal_caps_detected_as_duplicate(cap_c, expected):
    rows = alphabet_rows(cap_c=cap_c)
    group = detect_multi_class_groups(rows)[0]
    result = classify_group_semantics(group, {row["symbol"]: row for row in rows}, sec_snapshot())
    assert result["classification"] == SEMANTICS_ISSUER_DUPLICATED
    assert result["market_cap_equal_or_near_equal"] is expected


def test_security_level_distinct_caps_are_verified_against_class_shares():
    rows = alphabet_rows(
        cap_a=357.18 * 5_824_000_000,
        cap_c=355.03 * 5_456_000_000,
    )
    group = detect_multi_class_groups(rows)[0]
    result = classify_group_semantics(group, {row["symbol"]: row for row in rows}, sec_snapshot())
    assert result["classification"] == SEMANTICS_SECURITY_VERIFIED


def test_unknown_semantics_is_preserved_without_enough_evidence():
    rows = alphabet_rows(cap_a=100, cap_c=25)
    group = detect_multi_class_groups(rows)[0]
    result = classify_group_semantics(group, {row["symbol"]: row for row in rows}, None)
    assert result["classification"] == SEMANTICS_UNKNOWN


def test_near_equal_without_shares_is_only_probable():
    rows = alphabet_rows()
    group = detect_multi_class_groups(rows)[0]
    result = classify_group_semantics(group, {row["symbol"]: row for row in rows}, None)
    assert result["classification"] == SEMANTICS_ISSUER_PROBABLE


def test_price_times_class_shares_adjustment():
    adjusted, quality = apply_multi_class_adjustments(alphabet_rows(), {"alphabet": sec_snapshot()})
    by_symbol = {row["symbol"]: row for row in adjusted}
    assert by_symbol["GOOGL"]["security_market_cap"] == pytest.approx(357.18 * 5_824_000_000)
    assert by_symbol["GOOG"]["security_market_cap"] == pytest.approx(355.03 * 5_456_000_000)
    assert quality["multi_class_adjustment_count"] == 2


def test_unlisted_class_b_is_not_added_to_index_rows():
    adjusted, quality = apply_multi_class_adjustments(alphabet_rows(), {"alphabet": sec_snapshot()})
    assert {row["symbol"] for row in adjusted} == {"GOOG", "GOOGL"}
    assert quality["multi_class_diagnostics"][0]["unlisted_classes"] == ["B"]


def test_no_fifty_fifty_or_price_ratio_split():
    adjusted, _ = apply_multi_class_adjustments(alphabet_rows(), {"alphabet": sec_snapshot()})
    by_symbol = {row["symbol"]: row for row in adjusted}
    caps = [by_symbol["GOOGL"]["security_market_cap"], by_symbol["GOOG"]["security_market_cap"]]
    assert caps[0] != caps[1]
    assert caps[0] / sum(caps) != pytest.approx(0.5)
    assert caps[0] / caps[1] != pytest.approx(357.18 / 355.03)


def test_ambiguous_group_is_not_verified_or_assigned_cap():
    adjusted, quality = apply_multi_class_adjustments(alphabet_rows(), {})
    assert quality["multi_class_unresolved_count"] == 1
    assert all(row["market_cap"] is None for row in adjusted)
    assert all(row["market_cap_verified"] is False for row in adjusted)


def test_full_denominator_is_recalculated():
    rows = alphabet_rows(cap_a=300, cap_c=300) + [
        {"symbol": "OTHER", "companyName": "Other Inc.", "marketCap": 100, "lastSalePrice": 10}
    ]
    snapshot = sec_snapshot()
    snapshot["listed_shares"] = {"GOOGL": 1, "GOOG": 2}
    snapshot["class_shares"] = {"A": 1, "B": 1, "C": 2}
    rows[0]["lastSalePrice"] = 10
    rows[1]["lastSalePrice"] = 10
    adjusted, _ = apply_multi_class_adjustments(rows, {"alphabet": snapshot})
    result = reconstruct_market_cap_weights(adjusted, source="Nasdaq", source_url="x", as_of="2026-07-11", maximum_constituent_pct=80)
    by_symbol = {row["symbol"]: row for row in result["holdings"]}
    assert by_symbol["OTHER"]["weight_pct"] == pytest.approx(100 / 130 * 100)
    assert by_symbol["GOOGL"]["weight_pct"] == pytest.approx(10 / 130 * 100)
    assert by_symbol["GOOG"]["weight_pct"] == pytest.approx(20 / 130 * 100)


def test_goog_googl_remain_separate_with_issuer_aggregate():
    adjusted, _ = apply_multi_class_adjustments(alphabet_rows(), {"alphabet": sec_snapshot()})
    result = reconstruct_market_cap_weights(adjusted, source="Nasdaq", source_url="x", as_of="2026-07-11")
    by_symbol = {row["symbol"]: row for row in result["holdings"]}
    assert set(by_symbol) == {"GOOG", "GOOGL"}
    assert by_symbol["GOOG"]["issuer_aggregate_weight_pct"] == pytest.approx(100)
    assert by_symbol["GOOGL"]["issuer_aggregate_weight_pct"] == pytest.approx(100)


def test_contributions_use_adjusted_separate_weights():
    adjusted, _ = apply_multi_class_adjustments(alphabet_rows(), {"alphabet": sec_snapshot()})
    result = reconstruct_market_cap_weights(adjusted, source="Nasdaq", source_url="x", as_of="2026-07-11")
    contribution = weighted_contributions(result["holdings"])
    assert {row["symbol"] for row in contribution["contributors"]} == {"GOOG", "GOOGL"}


def test_concentration_recomputed_after_adjustment():
    adjusted, _ = apply_multi_class_adjustments(alphabet_rows(), {"alphabet": sec_snapshot()})
    result = reconstruct_market_cap_weights(adjusted, source="Nasdaq", source_url="x", as_of="2026-07-11")
    concentration = concentration_metrics(result["holdings"])
    assert concentration["top_1_weight_pct"] < 60
    assert concentration["top_3_weight_pct"] == pytest.approx(100)


def test_sector_exposure_uses_adjusted_weights():
    rows = alphabet_rows()
    for row in rows:
        row["sector"] = "Communication Services"
    adjusted, _ = apply_multi_class_adjustments(rows, {"alphabet": sec_snapshot()})
    result = reconstruct_market_cap_weights(adjusted, source="Nasdaq", source_url="x", as_of="2026-07-11")
    exposure = sector_weight_exposure(result["holdings"])
    assert exposure["sectors"][0]["weight_pct"] == pytest.approx(100)


def test_quality_penalty_for_unknown_semantics():
    verified = weight_quality_score(
        method=RECONSTRUCTED_MARKET_CAP_WEIGHT,
        weight_coverage_pct=100,
        price_coverage_pct=100,
        sector_coverage_pct=100,
        stale=False,
        issuer_semantics_quality_score=1.0,
    )
    unknown = weight_quality_score(
        method=RECONSTRUCTED_MARKET_CAP_WEIGHT,
        weight_coverage_pct=100,
        price_coverage_pct=100,
        sector_coverage_pct=100,
        stale=False,
        issuer_semantics_quality_score=0.0,
    )
    assert unknown["weight_quality_score"] < verified["weight_quality_score"]


def test_sec_parser_maps_listed_classes_and_excludes_b():
    parsed = parse_inline_xbrl_class_shares(_xbrl_fixture(), listed_class_symbols={"A": "GOOGL", "C": "GOOG"})
    assert parsed["listed_shares"] == {"GOOGL": 5_824_000_000.0, "GOOG": 5_456_000_000.0}
    assert parsed["unlisted_classes"] == ["B"]
    assert parsed["shares_as_of"] == "2026-04-22"


def test_sec_parser_missing_listed_class_is_error():
    parsed = parse_inline_xbrl_class_shares(_xbrl_fixture(include_c=False), listed_class_symbols={"A": "GOOGL", "C": "GOOG"})
    assert "listed_class_missing:C" in parsed["errors"]


def test_latest_periodic_filing_prefers_latest_date():
    payload = {
        "filings": {"recent": {
            "form": ["8-K", "10-K", "10-Q"],
            "accessionNumber": ["x", "old", "new"],
            "primaryDocument": ["x.htm", "old.htm", "new.htm"],
            "filingDate": ["2026-05-01", "2026-02-01", "2026-04-30"],
        }}
    }
    assert latest_periodic_filing(payload)["accession"] == "new"


@pytest.mark.asyncio
async def test_provider_persists_verified_multiclass_and_cache_only_zero_network(tmp_path):
    cfg = Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        alpha_vantage_api_key=None,
        invesco_qqq_holdings_url="https://invesco.test/qqq",
        nasdaq_100_constituents_url="https://nasdaq.test/ndx",
        sec_submissions_base_url="https://sec.test/submissions",
        sec_archives_base_url="https://sec.test/archives",
    )
    provider = QQQHoldingsProvider(ProviderCacheRepository(cfg.database_path), cfg)
    payload = _nasdaq_payload()
    with respx.mock(assert_all_called=True) as router:
        router.get("https://invesco.test/qqq").mock(return_value=httpx.Response(406))
        router.get("https://nasdaq.test/ndx").mock(return_value=httpx.Response(200, json=payload))
        router.get("https://sec.test/submissions/CIK0001652044.json").mock(return_value=httpx.Response(200, json=_submissions()))
        router.get("https://sec.test/archives/1652044/000165204426000048/goog-20260331.htm").mock(return_value=httpx.Response(200, text=_xbrl_fixture()))
        first = await provider.fetch_safe()
    with respx.mock(assert_all_called=False) as router:
        router.get("https://invesco.test/qqq").mock(return_value=httpx.Response(500))
        second = await provider.fetch_safe()
    assert first.data["weight_method"] == RECONSTRUCTED_MARKET_CAP_WEIGHT
    assert first.data["data_quality"]["multi_class_adjustment_count"] == 2
    assert first.data["data_quality"]["issuer_semantics_quality_score"] == 1.0
    assert second.metadata.provider_type == "CACHE"
    assert second.data["data_quality"]["actual_network_calls"] == 0


def test_http_materialization_keeps_multiclass_fields():
    adjusted, quality = apply_multi_class_adjustments(alphabet_rows(), {"alphabet": sec_snapshot()})
    result = reconstruct_market_cap_weights(adjusted, source="Nasdaq", source_url="x", as_of="2026-07-11")
    context = enrich_nasdaq_context(
        {
            "qqq_holdings": {
                "holdings": result["holdings"],
                "top_holdings": result["holdings"],
                "weight_method": RECONSTRUCTED_MARKET_CAP_WEIGHT,
                "data_quality": quality,
            },
            "mega_cap_snapshot": {"stocks": []},
            "mega_cap_breadth": {},
        },
        {},
    )
    assert context["qqq_holdings"]["holdings"][0]["issuer_group"] == "alphabet"
    assert context["concentration"]["weight_method"] == RECONSTRUCTED_MARKET_CAP_WEIGHT


def test_no_false_official_weight_after_multiclass_fix():
    adjusted, _ = apply_multi_class_adjustments(alphabet_rows(), {"alphabet": sec_snapshot()})
    result = reconstruct_market_cap_weights(adjusted, source="Nasdaq", source_url="x", as_of="2026-07-11")
    assert result["weight_is_official"] is False
    assert all(row["weight_is_official"] is False for row in result["holdings"])


def test_normalize_issuer_name_removes_only_share_class_markers():
    assert normalize_issuer_name("Alphabet Inc. Class A Common Stock") == "Alphabet Inc"
    assert normalize_issuer_name("Alphabetical Systems") == "Alphabetical Systems"


def _xbrl_fixture(*, include_c=True):
    contexts = "".join(
        [
            '<xbrli:context id="a"><xbrli:entity><xbrli:segment><xbrldi:explicitMember>us-gaap:CommonClassAMember</xbrldi:explicitMember></xbrli:segment></xbrli:entity><xbrli:period><xbrli:instant>2026-04-22</xbrli:instant></xbrli:period></xbrli:context>',
            '<xbrli:context id="b"><xbrli:entity><xbrli:segment><xbrldi:explicitMember>us-gaap:CommonClassBMember</xbrldi:explicitMember></xbrli:segment></xbrli:entity><xbrli:period><xbrli:instant>2026-04-22</xbrli:instant></xbrli:period></xbrli:context>',
            '<xbrli:context id="c"><xbrli:entity><xbrli:segment><xbrldi:explicitMember>goog:CapitalClassCMember</xbrldi:explicitMember></xbrli:segment></xbrli:entity><xbrli:period><xbrli:instant>2026-04-22</xbrli:instant></xbrli:period></xbrli:context>',
        ]
    )
    facts = [
        '<ix:nonFraction contextRef="a" name="dei:EntityCommonStockSharesOutstanding" scale="6">5,824</ix:nonFraction>',
        '<ix:nonFraction contextRef="b" name="dei:EntityCommonStockSharesOutstanding" scale="6">836</ix:nonFraction>',
    ]
    if include_c:
        facts.append('<ix:nonFraction contextRef="c" name="dei:EntityCommonStockSharesOutstanding" scale="6">5,456</ix:nonFraction>')
    return contexts + "".join(facts)


def _submissions():
    return {"filings": {"recent": {
        "form": ["10-Q"],
        "accessionNumber": ["0001652044-26-000048"],
        "primaryDocument": ["goog-20260331.htm"],
        "filingDate": ["2026-04-30"],
    }}}


def _nasdaq_payload():
    rows = alphabet_rows()
    for index in range(101):
        rows.append(
            {
                "symbol": f"S{index:03d}",
                "companyName": f"Issuer {index}",
                "marketCap": (101 - index) * 10_000_000_000,
                "lastSalePrice": 100,
                "percentageChange": 1,
                "sector": "Technology",
            }
        )
    return {"data": {"date": "Jul 11, 2026", "data": {"rows": rows}}}
