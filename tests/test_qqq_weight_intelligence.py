from __future__ import annotations

import io
import zipfile
from datetime import UTC, datetime

import httpx
import pytest
import respx

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.models.nasdaq import QQQHolding, QQQHoldingsResponse
from app.providers.qqq_holdings_provider import QQQHoldingsProvider
from app.services.market_context_builder import build_overall_quality
from app.services.qqq_weight_intelligence_service import (
    EQUAL_WEIGHT_PROXY,
    OFFICIAL_NASDAQ100_WEIGHT,
    OFFICIAL_QQQ_WEIGHT,
    RECONSTRUCTED_MARKET_CAP_WEIGHT,
    VENDOR_QQQ_WEIGHT,
    apply_weight_provenance,
    concentration_metrics,
    parse_csv_holdings,
    parse_json_holdings,
    parse_market_value,
    parse_weight_value,
    parse_xlsx_holdings,
    reconstruct_market_cap_weights,
    sector_weight_exposure,
    select_weight_candidate,
    validate_weight_set,
    weight_quality_score,
    weighted_contributions,
)


def holding(symbol: str, weight: float | None, **extra):
    return {"symbol": symbol, "weight": weight, "weight_pct": weight, **extra}


def candidate(method: str, *, valid: bool = True, as_of: str = "2026-07-10"):
    return {
        "weight_method": method,
        "weight_confidence": 0.9,
        "as_of": as_of,
        "validation": {"valid": valid},
    }


@pytest.mark.parametrize(
    ("methods", "expected"),
    [
        ([VENDOR_QQQ_WEIGHT, OFFICIAL_QQQ_WEIGHT], OFFICIAL_QQQ_WEIGHT),
        ([RECONSTRUCTED_MARKET_CAP_WEIGHT, OFFICIAL_NASDAQ100_WEIGHT], OFFICIAL_NASDAQ100_WEIGHT),
        ([RECONSTRUCTED_MARKET_CAP_WEIGHT, VENDOR_QQQ_WEIGHT], VENDOR_QQQ_WEIGHT),
        ([EQUAL_WEIGHT_PROXY, RECONSTRUCTED_MARKET_CAP_WEIGHT], RECONSTRUCTED_MARKET_CAP_WEIGHT),
        ([OFFICIAL_QQQ_WEIGHT, VENDOR_QQQ_WEIGHT, EQUAL_WEIGHT_PROXY], OFFICIAL_QQQ_WEIGHT),
        ([OFFICIAL_QQQ_WEIGHT, OFFICIAL_NASDAQ100_WEIGHT], OFFICIAL_QQQ_WEIGHT),
        ([EQUAL_WEIGHT_PROXY], EQUAL_WEIGHT_PROXY),
    ],
)
def test_source_ranking(methods, expected):
    selected = select_weight_candidate([candidate(method) for method in methods])
    assert selected["weight_method"] == expected


def test_invalid_higher_rank_candidate_does_not_replace_valid_lower_rank():
    selected = select_weight_candidate(
        [candidate(OFFICIAL_QQQ_WEIGHT, valid=False), candidate(VENDOR_QQQ_WEIGHT)]
    )
    assert selected["weight_method"] == VENDOR_QQQ_WEIGHT


def test_newer_candidate_wins_within_same_method():
    selected = select_weight_candidate(
        [candidate(VENDOR_QQQ_WEIGHT, as_of="2026-07-09"), candidate(VENDOR_QQQ_WEIGHT, as_of="2026-07-10")]
    )
    assert selected["as_of"] == "2026-07-10"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("7.25%", 7.25),
        ("7.25", 7.25),
        (7.25, 7.25),
        ("1,234.5", 1234.5),
        (None, None),
        ("", None),
        ("N/A", None),
        ("nan", None),
    ],
)
def test_parse_weight_value(raw, expected):
    assert parse_weight_value(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("$4,631,217,093,920", 4631217093920.0),
        ("123.4", 123.4),
        (123, 123.0),
        ("(25)", -25.0),
        ("N/A", None),
    ],
)
def test_parse_market_value(raw, expected):
    assert parse_market_value(raw) == expected


def test_parse_official_csv_with_percent_and_share_class():
    text = "As of,07/10/2026\nTicker,Name,Weight (%),Sector\nGOOGL,Alphabet Class A,60%,Communication Services\nGOOG,Alphabet Class C,40%,Communication Services\n"
    rows, as_of, errors = parse_csv_holdings(text)
    assert errors == []
    assert as_of == "07/10/2026"
    assert [row["share_class"] for row in rows] == ["A", "C"]
    assert sum(row["weight"] for row in rows) == 100


def test_parse_csv_schema_change_is_diagnostic():
    rows, _, errors = parse_csv_holdings("Code,Issuer\nAAPL,Apple")
    assert rows == []
    assert errors == ["schema_changed:weight_header_not_found"]


def test_parse_empty_csv_is_rejected():
    rows, _, errors = parse_csv_holdings("")
    assert rows == []
    assert errors


def test_parse_json_holdings():
    rows, errors = parse_json_holdings({"holdings": [{"symbol": "AAPL", "weight": "55%"}, {"symbol": "MSFT", "weight": 45}]})
    assert errors == []
    assert [row["weight"] for row in rows] == [55.0, 45.0]


def test_parse_json_schema_change():
    rows, errors = parse_json_holdings({"items": {}})
    assert rows == []
    assert errors == []


def test_parse_xlsx_holdings():
    content = _xlsx([["Ticker", "Name", "Weight (%)"], ["AAPL", "Apple", "60"], ["MSFT", "Microsoft", "40"]])
    rows, errors = parse_xlsx_holdings(content)
    assert errors == []
    assert [(row["symbol"], row["weight"]) for row in rows] == [("AAPL", 60.0), ("MSFT", 40.0)]


def test_invalid_xlsx_is_rejected():
    rows, errors = parse_xlsx_holdings(b"not-a-zip")
    assert rows == []
    assert errors == ["invalid_xlsx:BadZipFile"]


@pytest.mark.parametrize(
    ("rows", "kwargs", "valid", "reason"),
    [
        ([holding("A", 60), holding("B", 40)], {"maximum_constituent_pct": 70}, True, None),
        ([holding("A", -1), holding("B", 101)], {"maximum_constituent_pct": 110}, False, "negative_weight"),
        ([holding("A", 80), holding("B", 10)], {"maximum_constituent_pct": 90}, False, "invalid_total_weight"),
        ([holding("A", 60), holding("A", 40)], {"maximum_constituent_pct": 70}, False, "duplicate_symbols"),
        ([holding("A", 60), holding("B", None)], {"expected_symbols": {"A", "B"}, "maximum_constituent_pct": 70}, False, "missing_constituents"),
        ([holding("A", 100.4)], {"maximum_constituent_pct": 101}, True, None),
        ([holding("A", 100.0)], {"maximum_constituent_pct": 25}, False, "weight_outlier"),
        ([], {}, False, None),
        ([holding("GOOG", 50), holding("GOOGL", 50)], {"maximum_constituent_pct": 60}, True, None),
        ([holding("A", 0), holding("B", 100)], {"maximum_constituent_pct": 101}, True, None),
    ],
)
def test_weight_set_validation(rows, kwargs, valid, reason):
    result = validate_weight_set(rows, **kwargs)
    assert result["valid"] is valid
    if reason:
        assert reason in result["rejection_reasons"]


def test_normalization_only_within_tolerance():
    rows = [holding("A", 50.2), holding("B", 50.2)]
    result = validate_weight_set(rows, maximum_constituent_pct=60)
    assert result["normalization_applied"] is True
    assert result["total_weight_pct"] == 100.0


def test_gravely_incomplete_total_is_not_normalized():
    rows = [holding("A", 60), holding("B", 20)]
    result = validate_weight_set(rows, maximum_constituent_pct=70)
    assert result["normalization_applied"] is False
    assert result["total_weight_pct"] == 80


@pytest.mark.parametrize("missing_index", [None, 0, 1, 2, 3])
def test_market_cap_reconstruction(missing_index):
    rows = [
        {"symbol": "GOOG", "companyName": "Alphabet Class C", "marketCap": 300, "lastSalePrice": 100, "percentageChange": 1},
        {"symbol": "GOOGL", "companyName": "Alphabet Class A", "marketCap": 300, "lastSalePrice": 100, "percentageChange": -1},
        {"symbol": "AAPL", "companyName": "Apple", "marketCap": 200, "lastSalePrice": 100, "percentageChange": 2},
        {"symbol": "MSFT", "companyName": "Microsoft", "marketCap": 200, "lastSalePrice": 100, "percentageChange": -2},
    ]
    if missing_index is not None:
        rows[missing_index]["marketCap"] = None
    result = reconstruct_market_cap_weights(rows, source="Nasdaq", source_url="https://nasdaq.test", as_of="2026-07-10")
    weighted = [row for row in result["holdings"] if row["weight"] is not None]
    assert sum(row["weight"] for row in weighted) == pytest.approx(100.0)
    assert result["weight_method"] == RECONSTRUCTED_MARKET_CAP_WEIGHT
    assert result["weight_is_official"] is False
    assert result["weight_is_reconstructed"] is True
    assert {row["symbol"] for row in result["holdings"]} == {"GOOG", "GOOGL", "AAPL", "MSFT"}


@pytest.mark.parametrize("bad_cap", [0, -1, "N/A", None])
def test_bad_market_cap_is_missing_not_fabricated(bad_cap):
    result = reconstruct_market_cap_weights(
        [{"symbol": "A", "marketCap": 100}, {"symbol": "B", "marketCap": bad_cap}],
        source="Nasdaq",
        source_url="https://nasdaq.test",
        as_of="2026-07-10",
    )
    row = next(item for item in result["holdings"] if item["symbol"] == "B")
    assert row["weight"] is None
    assert "market_cap_missing_or_invalid" in row["warnings"]


def test_reconstruction_deduplicates_symbols():
    result = reconstruct_market_cap_weights(
        [{"symbol": "A", "marketCap": 100}, {"symbol": "A", "marketCap": 50}],
        source="Nasdaq", source_url="https://nasdaq.test", as_of="2026-07-10"
    )
    assert len(result["holdings"]) == 1
    assert result["validation"]["duplicate_symbol_count"] == 1


@pytest.mark.parametrize(
    ("weights", "changes", "expected_net", "covered"),
    [
        ([60, 40], [2, -1], 0.8, 100),
        ([10, 5], [2, -1], 0.15, 15),
        ([10, 5], [-2, -1], -0.25, 15),
        ([10, 5], [0, 0], 0.0, 15),
        ([None, 5], [2, 1], 0.05, 5),
        ([10, 5], [None, 1], 0.05, 5),
    ],
)
def test_weighted_contribution_math(weights, changes, expected_net, covered):
    holdings = [holding("A", weights[0], weight_source="weights"), holding("B", weights[1], weight_source="weights")]
    prices = [{"symbol": "A", "change_pct": changes[0], "source": "prices"}, {"symbol": "B", "change_pct": changes[1], "source": "prices"}]
    result = weighted_contributions(holdings, prices)
    assert result["weighted_net_contribution"] == pytest.approx(expected_net)
    assert result["covered_weight_pct"] == pytest.approx(covered)


def test_top_contributor_is_sorted_by_contribution_not_change():
    result = weighted_contributions(
        [holding("BIG", 50), holding("SMALL", 1)],
        [{"symbol": "BIG", "change_pct": 1}, {"symbol": "SMALL", "change_pct": 10}],
    )
    assert result["contributors"][0]["symbol"] == "BIG"
    assert result["contributors"][0]["contribution_rank"] == 1


def test_goog_and_googl_contributions_are_separate():
    result = weighted_contributions(
        [holding("GOOG", 4), holding("GOOGL", 3)],
        [{"symbol": "GOOG", "change_pct": 1}, {"symbol": "GOOGL", "change_pct": -1}],
    )
    assert {row["symbol"] for row in result["contributors"]} == {"GOOG", "GOOGL"}


@pytest.mark.parametrize(
    ("weights", "expected"),
    [
        ([], "UNKNOWN"),
        ([10] * 10, "VERY_HIGH"),
        ([6] * 10 + [2] * 20, "VERY_HIGH"),
        ([4.5] * 10 + [1.1] * 50, "HIGH"),
        ([3] * 10 + [0.7] * 100, "MODERATE"),
        ([1] * 100, "LOW"),
    ],
)
def test_concentration_classification(weights, expected):
    result = concentration_metrics([holding(f"S{i}", weight) for i, weight in enumerate(weights)])
    assert result["classification"] == expected


def test_concentration_top_buckets_hhi_and_effective_count():
    result = concentration_metrics([holding("A", 40), holding("B", 30), holding("C", 20), holding("D", 10)])
    assert result["top_1_weight_pct"] == 40
    assert result["top_3_weight_pct"] == 90
    assert result["top_5_weight_pct"] == 100
    assert result["herfindahl_hirschman_index"] == pytest.approx(0.3)
    assert result["effective_number_of_constituents"] == pytest.approx(3.3333)


@pytest.mark.parametrize(
    ("rows", "coverage", "complete"),
    [
        ([holding("A", 60, sector="Technology"), holding("B", 40, sector="Health Care")], 100, True),
        ([holding("A", 60, sector="Technology"), holding("B", 40, sector=None)], 60, False),
        ([holding("A", None, sector="Technology")], 0, False),
        ([], 0, False),
    ],
)
def test_sector_exposure(rows, coverage, complete):
    result = sector_weight_exposure(rows)
    assert result["sector_weight_coverage_pct"] == coverage
    assert result["complete_portfolio_coverage"] is complete


def test_sector_top_constituents():
    result = sector_weight_exposure([holding("A", 60, sector="Technology"), holding("B", 40, sector="Technology")])
    assert result["sectors"][0]["top_constituents"][0]["symbol"] == "A"


@pytest.mark.parametrize(
    ("method", "stale", "expected_floor", "expected_ceiling"),
    [
        (OFFICIAL_QQQ_WEIGHT, False, 0.9, 1.0),
        (OFFICIAL_NASDAQ100_WEIGHT, False, 0.85, 1.0),
        (VENDOR_QQQ_WEIGHT, False, 0.75, 0.95),
        (RECONSTRUCTED_MARKET_CAP_WEIGHT, False, 0.55, 0.85),
        (EQUAL_WEIGHT_PROXY, False, 0.0, 0.5),
        (OFFICIAL_QQQ_WEIGHT, True, 0.7, 0.9),
    ],
)
def test_quality_method_and_stale_penalties(method, stale, expected_floor, expected_ceiling):
    result = weight_quality_score(
        method=method,
        weight_coverage_pct=100,
        price_coverage_pct=100,
        sector_coverage_pct=100,
        stale=stale,
    )
    assert expected_floor <= result["weight_quality_score"] <= expected_ceiling


@pytest.mark.parametrize("field", ["weight_coverage_pct", "price_coverage_pct", "sector_coverage_pct"])
def test_missing_coverage_reduces_quality(field):
    kwargs = {"weight_coverage_pct": 100, "price_coverage_pct": 100, "sector_coverage_pct": 100}
    full = weight_quality_score(method=VENDOR_QQQ_WEIGHT, stale=False, **kwargs)["weight_quality_score"]
    kwargs[field] = 0
    degraded = weight_quality_score(method=VENDOR_QQQ_WEIGHT, stale=False, **kwargs)["weight_quality_score"]
    assert degraded < full


def test_canonical_holding_serializes_all_provenance_fields():
    rows = [holding("AAPL", 100)]
    apply_weight_provenance(
        rows,
        method=OFFICIAL_QQQ_WEIGHT,
        source="Invesco",
        source_url="https://invesco.test",
        as_of="2026-07-10",
        retrieved_at=datetime(2026, 7, 10, tzinfo=UTC),
        valid_until=datetime(2026, 7, 11, tzinfo=UTC),
        official=True,
        reconstructed=False,
        confidence=0.98,
    )
    model = QQQHolding.model_validate(rows[0]).model_dump(mode="json")
    for field in (
        "weight_pct", "weight_source", "weight_source_url", "weight_method", "weight_as_of",
        "weight_retrieved_at", "weight_valid_until", "weight_verified", "weight_is_official",
        "weight_is_reconstructed", "weight_confidence", "source", "source_url", "as_of",
        "retrieved_at", "valid_until", "is_official", "is_reconstructed", "confidence",
    ):
        assert field in model


@pytest.mark.asyncio
async def test_reconstructed_provider_result_is_persisted_and_cache_read_has_zero_network(tmp_path):
    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        invesco_qqq_holdings_url="https://invesco.test/qqq",
        nasdaq_100_constituents_url="https://nasdaq.test/ndx",
        alpha_vantage_api_key=None,
    )
    provider = QQQHoldingsProvider(ProviderCacheRepository(settings.database_path), settings)
    payload = _nasdaq_payload(103)
    with respx.mock(assert_all_called=True) as router:
        invesco = router.get("https://invesco.test/qqq").mock(return_value=httpx.Response(406))
        nasdaq = router.get("https://nasdaq.test/ndx").mock(return_value=httpx.Response(200, json=payload))
        first = await provider.fetch_safe()
    with respx.mock(assert_all_called=False) as router:
        router.get("https://invesco.test/qqq").mock(return_value=httpx.Response(500))
        second = await provider.fetch_safe()
    assert invesco.call_count == 1
    assert nasdaq.call_count == 1
    assert first.data["weight_method"] == RECONSTRUCTED_MARKET_CAP_WEIGHT
    assert first.data["data_quality"]["weight_coverage_pct"] == 100
    assert second.metadata.provider_type == "CACHE"
    assert second.data["data_quality"]["actual_network_calls"] == 0


@pytest.mark.asyncio
async def test_force_bypasses_valid_provider_cache(tmp_path):
    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "market.sqlite",
        invesco_qqq_holdings_url="https://invesco.test/qqq",
        nasdaq_100_constituents_url="https://nasdaq.test/ndx",
        alpha_vantage_api_key=None,
    )
    provider = QQQHoldingsProvider(ProviderCacheRepository(settings.database_path), settings)
    with respx.mock(assert_all_called=True) as router:
        invesco = router.get("https://invesco.test/qqq").mock(return_value=httpx.Response(406))
        nasdaq = router.get("https://nasdaq.test/ndx").mock(return_value=httpx.Response(200, json=_nasdaq_payload(103)))
        await provider.fetch_safe()
        await provider.fetch_safe(force=True)
    assert invesco.call_count == 2
    assert nasdaq.call_count == 2


def test_response_contract_is_backward_compatible():
    fields = QQQHoldingsResponse.model_fields
    for legacy in ("status", "as_of", "source", "holdings_count", "weight_data_available", "official_etf_holdings", "holdings", "data_quality"):
        assert legacy in fields
    for added in ("weight_method", "weight_source", "weight_as_of", "weight_verified", "weight_is_official", "weight_is_reconstructed", "weight_confidence"):
        assert added in fields


def test_all_invalid_candidates_return_none():
    assert select_weight_candidate([candidate(OFFICIAL_QQQ_WEIGHT, valid=False)]) is None


def test_validation_reports_foreign_symbols():
    result = validate_weight_set(
        [holding("A", 50), holding("FOREIGN", 50)],
        expected_symbols={"A", "B"},
        maximum_constituent_pct=60,
        minimum_coverage_pct=40,
    )
    assert result["foreign_symbols"] == ["FOREIGN"]
    assert result["missing_symbols"] == ["B"]


def test_reconstructed_confidence_is_below_official():
    reconstructed = weight_quality_score(
        method=RECONSTRUCTED_MARKET_CAP_WEIGHT,
        weight_coverage_pct=100,
        price_coverage_pct=100,
        sector_coverage_pct=100,
        stale=False,
    )
    official = weight_quality_score(
        method=OFFICIAL_QQQ_WEIGHT,
        weight_coverage_pct=100,
        price_coverage_pct=100,
        sector_coverage_pct=100,
        stale=False,
    )
    assert reconstructed["weight_quality_score"] < official["weight_quality_score"]


def test_equal_weight_proxy_never_scores_complete():
    result = weight_quality_score(
        method=EQUAL_WEIGHT_PROXY,
        weight_coverage_pct=100,
        price_coverage_pct=100,
        sector_coverage_pct=100,
        stale=False,
    )
    assert result["weight_quality_score"] < 0.5


def test_concentration_without_weights_is_unknown():
    result = concentration_metrics([holding("A", None)])
    assert result["classification"] == "UNKNOWN"
    assert result["herfindahl_hirschman_index"] is None


def test_contribution_falls_back_to_holding_price_change():
    result = weighted_contributions([holding("A", 20, change_pct=2, price_source="Nasdaq")])
    assert result["weighted_net_contribution"] == pytest.approx(0.4)
    assert result["contributors"][0]["price_source"] == "Nasdaq"


def test_missing_price_reduces_contribution_coverage():
    result = weighted_contributions(
        [holding("A", 60), holding("B", 40)],
        [{"symbol": "A", "change_pct": 1}],
    )
    assert result["covered_weight_pct"] == 60
    assert result["uncovered_weight_pct"] == 40
    assert result["missing_price_symbols"] == ["B"]


def test_sector_unknown_weight_is_explicit():
    result = sector_weight_exposure([holding("A", 60, sector="Technology"), holding("B", 40)])
    assert result["unknown_weight_pct"] == 40
    assert any(row["sector"] == "Unknown" for row in result["sectors"])


def test_reconstruction_does_not_invent_shares_outstanding():
    result = reconstruct_market_cap_weights(
        [{"symbol": "A", "marketCap": 100}],
        source="Nasdaq",
        source_url="https://nasdaq.test",
        as_of="2026-07-10",
    )
    assert result["holdings"][0].get("shares_outstanding") is None


def test_provenance_marks_vendor_as_non_official():
    rows = [holding("A", 100)]
    apply_weight_provenance(
        rows,
        method=VENDOR_QQQ_WEIGHT,
        source="Vendor",
        source_url="https://vendor.test",
        as_of="2026-07-10",
        retrieved_at=datetime(2026, 7, 10, tzinfo=UTC),
        valid_until=datetime(2026, 7, 11, tzinfo=UTC),
        official=False,
        reconstructed=False,
        confidence=0.88,
    )
    assert rows[0]["weight_is_official"] is False
    assert rows[0]["weight_method"] == VENDOR_QQQ_WEIGHT


@pytest.mark.parametrize(("score", "blocked"), [(0.81, False), (0.49, True)])
def test_reconstructed_quality_is_degraded_without_being_missing(score, blocked):
    section_quality = {
        "macro_snapshot": {"completeness_score": 1.0, "missing_fields": []},
        "critical_macro_events": {"completeness_score": 1.0, "missing_fields": []},
        "nasdaq_context": {"completeness_score": score, "missing_fields": []},
        "news_context": {"completeness_score": 1.0, "missing_fields": []},
    }
    result = build_overall_quality(section_quality)
    assert ("nasdaq_context_insufficient" in result["blocking_reasons"]) is blocked


def _nasdaq_payload(count: int) -> dict:
    rows = []
    for index in range(count):
        symbol = ["NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "GOOG", "AVGO", "TSLA", "AMD", "NFLX", "COST"][index] if index < 12 else f"S{index:03d}"
        rows.append(
            {
                "symbol": symbol,
                "companyName": symbol,
                "marketCap": str((count - index) * 1_000_000),
                "lastSalePrice": "$100",
                "percentageChange": "+1%" if index % 2 == 0 else "-1%",
                "sector": "",
            }
        )
    return {"data": {"date": "Jul 10, 2026", "data": {"rows": rows}}}


def _xlsx(rows: list[list[str]]) -> bytes:
    shared = [value for row in rows for value in row]
    shared_xml = "<sst xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">" + "".join(f"<si><t>{value}</t></si>" for value in shared) + "</sst>"
    index = 0
    xml_rows = []
    for row_number, row in enumerate(rows, start=1):
        cells = []
        for column, _ in enumerate(row):
            cells.append(f"<c r=\"{chr(65 + column)}{row_number}\" t=\"s\"><v>{index}</v></c>")
            index += 1
        xml_rows.append(f"<row r=\"{row_number}\">{''.join(cells)}</row>")
    sheet_xml = "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>" + "".join(xml_rows) + "</sheetData></worksheet>"
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("xl/sharedStrings.xml", shared_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return output.getvalue()
