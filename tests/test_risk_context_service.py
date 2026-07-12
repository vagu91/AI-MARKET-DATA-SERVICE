from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from app.core.config import Settings
from app.providers.cboe_put_call_provider import (
    normalize_cboe_put_call,
    parse_cboe_daily_statistics_html,
)
from app.providers.cboe_risk_indices_provider import parse_index_history_csv
from app.providers.cboe_vix_futures_provider import parse_vix_futures_csv
from app.services.ai_trader_contract_service import build_ai_trader_market_context
from app.services.risk_context_normalization_service import (
    RiskContextNormalizationService,
    build_legacy_risk_sentiment,
    calculate_risk_quality,
    calculate_temporal_alignment,
    classify_curve,
    compact_history,
    composite_status,
    historical_statistics,
    normalize_put_call,
    normalize_risk_index,
    normalize_vix_curve,
    qqq_put_call_ratios,
    ratio_history,
    relative_regime,
    select_ranked_source,
)
from app.services.risk_context_repository import RiskContextHistoryRepository
from app.services.risk_context_runtime_service import RiskContextRuntimeService


NOW = datetime(2026, 7, 11, 12, tzinfo=UTC)


def settings(tmp_path) -> Settings:
    return Settings(_env_file=None, database_path=tmp_path / "market.sqlite")


def history(count: int = 260, *, start: float = 80.0, step: float = 0.1) -> list[dict]:
    return [
        {"data_as_of": (NOW.date() - timedelta(days=count - index)).isoformat(), "value": start + index * step}
        for index in range(count)
    ]


def risk_indices_payload() -> dict:
    return {
        "status": "found",
        "provider_calls": 1,
        "indices": {
            "vvix": {
                "current_price": 90.0,
                "previous_close": 89.0,
                "change": 1.0,
                "percentage_change": 1.1236,
                "provider_timestamp": "2026-07-10 16:15:00",
                "retrieved_at": "2026-07-11T12:00:00Z",
                "source": "CBOE",
                "source_url": "https://cboe.test/vvix",
                "reliability": 0.86,
                "is_official_source": True,
            },
            "skew": {
                "current_price": 145.0,
                "previous_close": 144.0,
                "change": 1.0,
                "percentage_change": 0.6944,
                "provider_timestamp": "2026-07-10 17:00:00",
                "retrieved_at": "2026-07-11T12:00:00Z",
                "source": "CBOE",
                "source_url": "https://cboe.test/skew",
                "reliability": 0.86,
                "is_official_source": True,
            },
        },
        "history": {"vvix": history(start=70), "skew": history(start=120), "vix": history(start=10, step=0.03)},
        "warnings": [],
        "errors": [],
        "diagnostics": {"actual_network_calls": 5},
    }


def futures_payload(prices=(17.0, 18.0, 19.0, 20.0, 21.0, 22.0)) -> dict:
    contracts = []
    expirations = ("2026-07-22", "2026-08-19", "2026-09-16", "2026-10-21", "2026-11-18", "2026-12-16")
    for index, (expiration, price) in enumerate(zip(expirations, prices), start=1):
        contracts.append(
            {
                "contract_symbol": f"VX/{index}",
                "expiration_date": expiration,
                "last_price": price,
                "previous_close": price - 0.25,
                "change": 0.25,
                "change_pct": 1.0,
                "volume": None,
                "open_interest": None,
                "data_as_of": "2026-07-10",
                "source": "Cboe Futures Exchange",
                "source_url": "https://cboe.test/futures",
                "provider_type": "OFFICIAL_EXCHANGE_SETTLEMENT",
                "is_official_source": True,
            }
        )
    return {
        "status": "found",
        "source": "Cboe Futures Exchange",
        "source_url": "https://cboe.test/futures",
        "is_official_source": True,
        "data_as_of": "2026-07-10",
        "retrieved_at": "2026-07-11T12:00:00Z",
        "valid_until": "2099-01-01T00:00:00Z",
        "contracts": contracts,
        "warnings": [],
        "errors": [],
        "diagnostics": {"actual_network_calls": 4, "delayed_feed_status": "access_restricted"},
    }


def cboe_ratios_payload() -> dict:
    rows = []
    for ratio_id, scope, basis, puts, calls in (
        ("total_volume_put_call", "total", "volume", 80, 100),
        ("equity_volume_put_call", "equity", "volume", 60, 100),
        ("index_volume_put_call", "index", "volume", 110, 100),
        ("spx_volume_put_call", "spx", "volume", 120, 100),
        ("total_open_interest_put_call", "total", "open_interest", 90, 100),
    ):
        rows.append(
            {
                "ratio_id": ratio_id,
                "scope": scope,
                "basis": basis,
                "put_value": puts,
                "call_value": calls,
                "ratio": puts / calls,
                "data_as_of": "2026-07-10",
                "source": "Cboe Daily Market Statistics",
                "source_url": "https://cboe.test/putcall",
                "provider_type": "OFFICIAL_EXCHANGE_STATISTICS",
                "retrieved_at": "2026-07-11T12:00:00Z",
                "valid_until": "2099-01-01T00:00:00Z",
                "freshness": "END_OF_DAY",
                "reliability": 0.96,
                "confidence": 0.95,
                "is_official_source": True,
                "warnings": [],
                "errors": [],
            }
        )
    return {"status": "found", "ratios": rows, "warnings": [], "errors": [], "diagnostics": {"actual_network_calls": 1}}


def qqq_payload(*, complete: bool = True) -> dict:
    aggregates = {
        "scope": {"full_chain_complete": complete},
        "call_volume": 100,
        "put_volume": 70,
        "call_open_interest": 200,
        "put_open_interest": 180,
    }
    return {
        "status": "found" if complete else "partial",
        "provider_calls": 1,
        "source_url": "https://nasdaq.test/qqq",
        "retrieved_at": "2026-07-11T12:00:00Z",
        "valid_until": "2099-01-01T00:00:00Z",
        "snapshot": {"source_timestamp": "2026-07-10T20:00:00Z"},
        "global_aggregates": aggregates if complete else None,
        "observed_aggregates": {
            "observed_call_volume": 100,
            "observed_put_volume": 70,
            "observed_call_open_interest": 200,
            "observed_put_open_interest": 180,
        },
        "contracts": [{}],
        "diagnostics": {"actual_network_calls": 1},
    }


def macro_snapshot() -> dict:
    return {
        "financial_conditions": {
            "VIXCLS": {
                "value": 15.0,
                "data_as_of": "2026-07-10",
                "source": "FRED",
                "source_url": "https://fred.test/vix",
                "actual_is_official": True,
                "reliability": 0.95,
                "freshness": "RECENT",
            }
        }
    }


def canonical(settings_value: Settings | None = None, *, snapshots=None) -> dict:
    cfg = settings_value or Settings(_env_file=None)
    return RiskContextNormalizationService(cfg).build(
        risk_indices=risk_indices_payload(),
        vix_futures=futures_payload(),
        cboe_put_call=cboe_ratios_payload(),
        qqq_options=qqq_payload(),
        macro_snapshot=macro_snapshot(),
        snapshot_history=snapshots or [],
        now=NOW,
    )


@pytest.mark.parametrize("symbol", ["vvix", "skew", "vix"])
def test_cboe_history_parser_reads_positive_values(symbol) -> None:
    key = symbol.upper() if symbol != "vix" else "CLOSE"
    text = f"DATE,{key}\n07/09/2026,10.0\n07/10/2026,11.5\n"
    rows = parse_index_history_csv(text, key=symbol)
    assert rows[-1] == {"data_as_of": "2026-07-10", "value": 11.5}


@pytest.mark.parametrize("bad", ["", "x", "-1", "0", "nan", "inf"])
def test_cboe_history_parser_rejects_invalid_values(bad) -> None:
    assert parse_index_history_csv(f"DATE,VVIX\n07/10/2026,{bad}\n", key="vvix") == []


def test_vvix_valid_previous_change_and_official_source() -> None:
    item = normalize_risk_index("VVIX", risk_indices_payload()["indices"]["vvix"], history=history(), history_min=60, now=NOW)
    assert item["status"] == "found"
    assert item["previous_close"] == 89
    assert item["change"] == 1
    assert item["is_official_source"] is True


@pytest.mark.parametrize("value", [-10, 0, None, "bad"])
def test_risk_index_invalid_values_are_rejected(value) -> None:
    raw = {**risk_indices_payload()["indices"]["vvix"], "current_price": value}
    assert normalize_risk_index("VVIX", raw, history=[], history_min=60, now=NOW)["status"] == "not_found"


def test_risk_index_future_timestamp_is_rejected() -> None:
    raw = {**risk_indices_payload()["indices"]["vvix"], "provider_timestamp": "2099-01-01T00:00:00Z"}
    item = normalize_risk_index("VVIX", raw, history=[], history_min=60, now=NOW)
    assert item["status"] == "not_found"
    assert "invalid_timestamp" in item["errors"]


def test_inconsistent_previous_close_is_reconstructed_with_warning() -> None:
    raw = {**risk_indices_payload()["indices"]["vvix"], "previous_close": 90.0, "current_price": 90.0, "change": 1.0}
    item = normalize_risk_index("VVIX", raw, history=history(), history_min=60, now=NOW)
    assert item["previous_close"] == 89.0
    assert "provider_previous_close_inconsistent_derived_from_change" in item["warnings"]


def test_stale_index_retained_with_lower_confidence() -> None:
    raw = {**risk_indices_payload()["indices"]["skew"], "stale": True}
    item = normalize_risk_index("SKEW", raw, history=history(), history_min=60, now=NOW)
    assert item["status"] == "found"
    assert item["freshness"] == "STALE"
    assert item["confidence"] == 0.55


@pytest.mark.parametrize("count", [0, 1, 2, 5, 20, 59])
def test_history_insufficient_keeps_percentile_null(count) -> None:
    stats = historical_statistics(90, history(count), min_points=60)
    assert stats["percentile_1y"] is None
    assert stats["z_score_1y"] is None


@pytest.mark.parametrize("value", [80, 85, 90, 95, 100, 105, 110, 115, 120, 125])
def test_history_sufficient_produces_bounded_percentile_and_zscore(value) -> None:
    stats = historical_statistics(value, history(), min_points=60)
    assert 0 <= stats["percentile_1y"] <= 100
    assert stats["z_score_1y"] is not None
    assert stats["history_depth"] == 260


@pytest.mark.parametrize(
    ("percentile", "expected"),
    [(None, "UNKNOWN"), (0, "LOW_RELATIVE"), (9.99, "LOW_RELATIVE"), (10, "NORMAL_RELATIVE"), (50, "NORMAL_RELATIVE"), (74.99, "NORMAL_RELATIVE"), (75, "HIGH_RELATIVE"), (94.99, "HIGH_RELATIVE"), (95, "EXTREME_RELATIVE"), (100, "EXTREME_RELATIVE")],
)
def test_relative_regime_is_distribution_based(percentile, expected) -> None:
    assert relative_regime(percentile) == expected


def test_skew_tail_regime_has_no_crash_probability() -> None:
    item = normalize_risk_index("SKEW", risk_indices_payload()["indices"]["skew"], history=history(start=120), history_min=60, now=NOW)
    assert item["tail_risk_regime"] in {"LOW_RELATIVE", "NORMAL_RELATIVE", "ELEVATED_RELATIVE", "EXTREME_RELATIVE"}
    assert "crash_probability" not in item


def test_vix_futures_parser_keeps_monthlies_orders_expiration_and_excludes_weeklies() -> None:
    text = "Product,Symbol,Expiration Date,Price\nVX,VX28/N6,2026-07-15,17\nVX,VX/Q6,2026-08-19,18\nVX,VX/N6,2026-07-22,17\n"
    rows, diagnostics = parse_vix_futures_csv(text, data_as_of="2026-07-10")
    assert [row["contract_symbol"] for row in rows] == ["VX/N6", "VX/Q6"]
    assert diagnostics["weekly_contract_excluded_count"] == 1


@pytest.mark.parametrize(
    ("symbol", "expiration", "price", "expected_count"),
    [
        ("VX/N6", "2026-07-22", "17", 1),
        ("VX/N6", "2026-07-09", "17", 0),
        ("VX/N6", "2026-07-22", "-1", 0),
        ("VX/N6", "bad", "17", 0),
        ("VX28/N6", "2026-07-22", "17", 0),
        ("VXM/N6", "2026-07-22", "17", 0),
        ("VX/N6", "2026-07-22", "bad", 0),
    ],
)
def test_vix_futures_parser_validation_cases(symbol, expiration, price, expected_count) -> None:
    text = f"Product,Symbol,Expiration Date,Price\nVX,{symbol},{expiration},{price}\n"
    rows, _ = parse_vix_futures_csv(text, data_as_of="2026-07-10")
    assert len(rows) == expected_count


def test_vix_futures_duplicate_is_excluded() -> None:
    text = "Product,Symbol,Expiration Date,Price\nVX,VX/N6,2026-07-22,17\nVX,VX/N6,2026-07-22,17\n"
    rows, diagnostics = parse_vix_futures_csv(text, data_as_of="2026-07-10")
    assert len(rows) == 1
    assert diagnostics["duplicate_contract_count"] == 1


@pytest.mark.parametrize(
    ("spreads", "expected"),
    [
        ([], "UNKNOWN"),
        ([0.0], "FLAT"),
        ([0.2], "FLAT"),
        ([-0.2], "FLAT"),
        ([0.3], "CONTANGO"),
        ([1, 2, 3], "CONTANGO"),
        ([-0.3], "BACKWARDATION"),
        ([-1, -2], "BACKWARDATION"),
        ([1, -1], "MIXED"),
        ([-1, 1], "MIXED"),
        ([1, 0, 2], "CONTANGO"),
        ([-1, 0, -2], "BACKWARDATION"),
    ],
)
def test_curve_classification_tolerance_and_mixed(spreads, expected) -> None:
    assert classify_curve(spreads, tolerance_pct=0.25) == expected


@pytest.mark.parametrize("m2", [17.5, 18, 18.5, 19, 19.5, 20, 20.5, 21])
def test_curve_spread_points_and_percent_are_exact(m2) -> None:
    curve = normalize_vix_curve(futures_payload(prices=(17, m2, 20))["contracts"], vix_spot=15, flat_tolerance_pct=.25, source_payload=futures_payload(), now=NOW)
    assert curve["m1_m2_spread_points"] == pytest.approx(m2 - 17)
    assert curve["m1_m2_spread_pct"] == pytest.approx((m2 / 17 - 1) * 100)
    assert curve["spot"] == 15


def test_curve_missing_m2_is_partial() -> None:
    payload = futures_payload()
    curve = normalize_vix_curve(payload["contracts"][:1], vix_spot=15, flat_tolerance_pct=.25, source_payload=payload, now=NOW)
    assert curve["status"] == "partial"
    assert curve["structure"] == "UNKNOWN"


def test_curve_m1_m3_m6_slopes_and_optional_fields() -> None:
    payload = futures_payload()
    curve = normalize_vix_curve(payload["contracts"], vix_spot=15, flat_tolerance_pct=.25, source_payload=payload, now=NOW)
    assert curve["curve_slope_m1_m3"] == 2
    assert curve["curve_slope_m1_m6"] == 5
    assert curve["weighted_front_30d"] is None
    assert curve["contracts"][0]["volume"] is None


def test_put_call_html_parser_reads_structured_next_payload() -> None:
    options = {"ratios": [], "SUM OF ALL PRODUCTS": [{"name": "VOLUME", "call": 100, "put": 80, "total": 180}]}
    decoded = f'prefix "optionsData":{json.dumps(options)},"selectedDate":"2026-07-10" suffix'
    html = f"<script>self.__next_f.push({json.dumps([1, decoded])})</script>"
    parsed = parse_cboe_daily_statistics_html(html)
    assert parsed["selectedDate"] == "2026-07-10"
    assert parsed["SUM OF ALL PRODUCTS"][0]["put"] == 80


def test_put_call_html_schema_change_returns_empty() -> None:
    assert parse_cboe_daily_statistics_html("<html>changed</html>") == {}


def test_put_call_normalizer_keeps_scope_and_basis_separate() -> None:
    payload = {
        "selectedDate": "2026-07-10",
        "SUM OF ALL PRODUCTS": [{"name": "VOLUME", "call": 100, "put": 80}, {"name": "OPEN INTEREST", "call": 200, "put": 180}],
        "EQUITY OPTIONS": [{"name": "VOLUME", "call": 100, "put": 60}],
        "INDEX OPTIONS": [{"name": "VOLUME", "call": 100, "put": 110}],
        "SPX + SPXW": [{"name": "VOLUME", "call": 100, "put": 120}],
    }
    rows, rejected = normalize_cboe_put_call(payload, retrieved_at="now", valid_until="later")
    assert rejected == 0
    assert {row["ratio_id"] for row in rows} == {"total_volume_put_call", "total_open_interest_put_call", "equity_volume_put_call", "index_volume_put_call", "spx_volume_put_call"}
    assert all(row["ratio"] == row["put_value"] / row["call_value"] for row in rows)


@pytest.mark.parametrize("calls", [0, -1, None, "bad"])
def test_zero_or_invalid_call_denominator_is_rejected(calls) -> None:
    payload = {"SUM OF ALL PRODUCTS": [{"name": "VOLUME", "call": calls, "put": 10}]}
    rows, rejected = normalize_cboe_put_call(payload, retrieved_at="now", valid_until="later")
    assert rows == []
    assert rejected == 1


def test_negative_put_value_is_rejected() -> None:
    rows, rejected = normalize_cboe_put_call({"SUM OF ALL PRODUCTS": [{"name": "VOLUME", "call": 10, "put": -1}]}, retrieved_at="now", valid_until="later")
    assert rows == [] and rejected == 1


@pytest.mark.parametrize(("complete", "reliability", "warning"), [(True, .82, False), (False, .58, True)])
def test_qqq_volume_and_open_interest_ratios_are_separate_and_scoped(complete, reliability, warning) -> None:
    rows = qqq_put_call_ratios(qqq_payload(complete=complete), now=NOW)
    assert {row["ratio_id"] for row in rows} == {"qqq_volume_put_call", "qqq_open_interest_put_call"}
    assert all(row["scope"] == "qqq" for row in rows)
    assert all(row["reliability"] == reliability for row in rows)
    assert bool(rows[0]["warnings"]) is warning
    assert all(row["data_as_of"] == "2026-07-10" for row in rows)


def test_ratio_history_never_mixes_equity_and_index() -> None:
    snapshots = [{"put_call": {"by_id": {"equity_volume_put_call": {"ratio": .6}, "index_volume_put_call": {"ratio": 1.2}}}}]
    assert ratio_history(snapshots, "equity_volume_put_call") == []


def test_put_call_statistics_5d_20d_percentile_same_series() -> None:
    snapshots = []
    for index in range(70):
        observed = (NOW.date() - timedelta(days=index + 1)).isoformat()
        snapshots.append({"put_call": {"by_id": {"equity_volume_put_call": {"ratio": .5 + index / 100, "data_as_of": observed}}}})
    result = normalize_put_call(cboe_ratios_payload()["ratios"], qqq_options={}, snapshot_history=snapshots, history_min=60, now=NOW)
    equity = result["by_id"]["equity_volume_put_call"]
    assert equity["moving_average_5d"] is not None
    assert equity["moving_average_20d"] is not None
    assert equity["percentile_1y"] is not None
    assert "contrarian" not in str(equity).lower()


@pytest.mark.parametrize("hours", [0, 1, 6, 12, 23])
def test_temporal_alignment_within_tolerance(hours) -> None:
    metrics = [{"status": "found", "data_as_of": "2026-07-10T20:00:00Z"}, {"status": "found", "data_as_of": (datetime(2026, 7, 10, 20, tzinfo=UTC) + timedelta(hours=hours)).isoformat()}]
    assert calculate_temporal_alignment(metrics, now=NOW, max_gap_minutes=1440)["aligned"] is True


@pytest.mark.parametrize("hours", [25, 30, 48, 72])
def test_temporal_alignment_excessive_gap(hours) -> None:
    metrics = [{"status": "found", "data_as_of": "2026-07-07T20:00:00Z"}, {"status": "found", "data_as_of": (datetime(2026, 7, 7, 20, tzinfo=UTC) + timedelta(hours=hours)).isoformat()}]
    result = calculate_temporal_alignment(metrics, now=datetime(2026, 7, 8, 15, tzinfo=UTC), max_gap_minutes=1440)
    assert result["aligned"] is False
    assert "temporal_misalignment" in result["warnings"]


def test_weekend_allows_last_session_without_false_stale() -> None:
    metrics = [{"status": "found", "data_as_of": "2026-07-10T16:00:00Z"}, {"status": "found", "data_as_of": "2026-07-10T21:00:00Z"}]
    result = calculate_temporal_alignment(metrics, now=NOW, max_gap_minutes=60)
    assert result["aligned"] is True
    assert result["market_session_status"] == "weekend"


def test_alignment_reports_oldest_newest_and_gap() -> None:
    result = calculate_temporal_alignment([{"status": "found", "data_as_of": "2026-07-10T10:00:00Z"}, {"status": "found", "data_as_of": "2026-07-10T11:30:00Z"}], now=NOW, max_gap_minutes=120)
    assert result["max_timestamp_gap_minutes"] == 90
    assert result["oldest_metric_at"].endswith("10:00:00Z")
    assert result["newest_metric_at"].endswith("11:30:00Z")


@pytest.mark.parametrize(
    ("family", "classes", "winner"),
    [
        ("risk_indices", ["secondary_provider", "official_cboe_current"], "official_cboe_current"),
        ("risk_indices", ["last_known_good_official", "official_cboe_historical"], "official_cboe_historical"),
        ("vix_futures", ["secondary_provider", "official_cfe_cboe"], "official_cfe_cboe"),
        ("vix_futures", ["last_known_good_official", "verified_futures_market_provider"], "verified_futures_market_provider"),
        ("put_call", ["secondary_provider", "official_cboe_statistics"], "official_cboe_statistics"),
        ("put_call", ["last_known_good_official", "verified_exchange_derived_provider"], "verified_exchange_derived_provider"),
    ],
)
def test_source_ranking_is_deterministic(family, classes, winner) -> None:
    candidates = [{"status": "found", "ranking_class": value, "name": value} for value in classes]
    assert select_ranked_source(candidates, family=family)["name"] == winner


def test_source_ranking_never_averages_different_scopes() -> None:
    candidates = [{"status": "found", "ranking_class": "official_cboe_statistics", "scope": "equity"}, {"status": "found", "ranking_class": "secondary_provider", "scope": "index"}]
    assert select_ranked_source(candidates, family="put_call")["scope"] == "equity"


def test_complete_canonical_block_has_neutral_derived_context_and_no_trading_fields() -> None:
    result = canonical()
    assert result["status"] == "available"
    assert result["derived_context"]["composite_status"] == "COMPLETE"
    forbidden = ("risk_on", "risk_off", "buy", "sell", "bullish", "bearish", "panic", "complacent")
    assert not any(word in str(result).lower() for word in forbidden)
    assert result["quality"]["put_call_scope_coverage_pct"] == 100
    assert result["quality"]["official_source_coverage_pct"] == 100


def test_canonical_semantics_vvix_skew_and_curve_are_explicit() -> None:
    result = canonical()
    assert result["derived_context"]["volatility_of_volatility"]["source_metric"] == "VVIX"
    assert result["derived_context"]["tail_risk"]["source_metric"] == "SKEW"
    assert result["vix_term_structure"]["spot"] != result["vix_term_structure"]["front_month"]["last_price"]


@pytest.mark.parametrize("missing", ["vix", "vvix", "skew", "curve", "put_call", "alignment"])
def test_quality_penalties_and_composite_degrade_for_missing_components(missing) -> None:
    result = canonical()
    vix, vvix, skew = result["vix"], result["vvix"], result["skew"]
    curve, put_call, alignment = result["vix_term_structure"], result["put_call"], result["data_alignment"]
    target = {"vix": vix, "vvix": vvix, "skew": skew, "curve": curve, "put_call": put_call}.get(missing)
    if target is not None:
        target["status"] = "not_found"
        if missing == "curve":
            target["coverage_pct"] = 0
        if missing == "put_call":
            target["scope_coverage_pct"] = 0
    else:
        alignment["aligned"] = False
        alignment["session_consistent"] = False
    quality = calculate_risk_quality(vix, vvix, skew, curve, put_call, alignment)
    assert quality["quality_score"] < result["quality"]["quality_score"]


def test_only_vix_is_degraded_and_low_quality() -> None:
    found = {"status": "found", "is_official_source": True, "history_depth": 260, "stale": False}
    missing = {"status": "not_found", "is_official_source": False, "history_depth": 0, "stale": False}
    curve = {"status": "not_found", "coverage_pct": 0, "is_official_source": False}
    pc = {"status": "not_found", "scope_coverage_pct": 0, "ratios": []}
    alignment = {"aligned": False, "session_consistent": False}
    assert composite_status(found, missing, missing, curve, pc, alignment) == "DEGRADED"
    assert calculate_risk_quality(found, missing, missing, curve, pc, alignment)["quality_score"] < .3


def test_legacy_fields_are_populated_and_deprecated_without_breaking_fear_greed() -> None:
    result = canonical()
    legacy = build_legacy_risk_sentiment(result, {"fear_greed": {"value": None}})
    assert legacy["vix_term_structure"]["front_month"] == 17
    assert legacy["put_call_ratio"]["value"] == .8
    assert "deprecated" in legacy["vix_term_structure"]
    assert "fear_greed" in legacy


def test_compact_history_is_bounded_and_preserves_core_metrics() -> None:
    rows = compact_history([canonical() for _ in range(20)])
    assert len(rows) == 10
    assert {"vix", "vvix", "skew", "curve_regime", "quality_score"} <= set(rows[0])


def test_repository_persistence_readback_history_and_provenance_survive(tmp_path) -> None:
    cfg = settings(tmp_path)
    repo = RiskContextHistoryRepository(cfg)
    payload = canonical(cfg)
    repo.append(payload)
    read_back = RiskContextHistoryRepository(cfg).latest()
    assert repo.count() == 1
    assert read_back["vvix"]["value"] == 90
    assert len(read_back["vix_term_structure"]["contracts"]) == 6
    assert read_back["put_call"]["by_id"]["equity_volume_put_call"]["scope"] == "equity"
    assert read_back["source_summary"] == payload["source_summary"]


@pytest.mark.asyncio
async def test_runtime_force_persists_reads_back_and_deduplicates_preloaded_providers(tmp_path) -> None:
    cfg = settings(tmp_path)
    service = RiskContextRuntimeService(cfg)

    async def fail_preloaded():
        raise AssertionError("preloaded providers must not be called")

    async def futures_ok():
        return futures_payload()

    async def pc_ok():
        return cboe_ratios_payload()

    service.risk_indices_provider.fetch = fail_preloaded
    service.qqq_options_provider.fetch = fail_preloaded
    service.vix_futures_provider.fetch = futures_ok
    service.put_call_provider.fetch = pc_ok
    result, legacy = await service.snapshot(refresh="force", macro_snapshot=macro_snapshot(), preloaded_risk_indices=risk_indices_payload(), preloaded_qqq_options=qqq_payload())
    assert result["diagnostics"]["persisted_count"] == 1
    assert result["diagnostics"]["read_back_count"] == 1
    assert result["diagnostics"]["provider_calls"] == 4
    assert legacy["vix_term_structure"]["structure"] == "CONTANGO"


@pytest.mark.asyncio
async def test_restart_refresh_false_is_zero_network_browser_ai_and_same_data(tmp_path) -> None:
    cfg = settings(tmp_path)
    first = RiskContextRuntimeService(cfg)
    first.vix_futures_provider.fetch = lambda: _async_value(futures_payload())
    first.put_call_provider.fetch = lambda: _async_value(cboe_ratios_payload())
    forced, _ = await first.snapshot(refresh="force", macro_snapshot=macro_snapshot(), preloaded_risk_indices=risk_indices_payload(), preloaded_qqq_options=qqq_payload())
    restarted = RiskContextRuntimeService(cfg)
    restarted.vix_futures_provider.fetch = _fail_network
    restarted.put_call_provider.fetch = _fail_network
    cached, _ = await restarted.snapshot(refresh="false", macro_snapshot={})
    assert cached["vvix"]["value"] == forced["vvix"]["value"]
    def stable(rows):
        return [{key: value for key, value in row.items() if key != "cache_status"} for row in rows]
    assert stable(cached["vix_term_structure"]["contracts"]) == stable(forced["vix_term_structure"]["contracts"])
    assert cached["diagnostics"]["provider_calls"] == 0
    assert cached["diagnostics"]["actual_network_calls"] == 0
    assert cached["diagnostics"]["browser_calls"] == 0
    assert cached["diagnostics"]["AI_called"] is False
    assert cached["diagnostics"]["cache_used"] is True


@pytest.mark.asyncio
async def test_auto_uses_valid_cache_and_force_bypasses_it(tmp_path) -> None:
    cfg = settings(tmp_path)
    service = RiskContextRuntimeService(cfg)
    service.vix_futures_provider.fetch = lambda: _async_value(futures_payload())
    service.put_call_provider.fetch = lambda: _async_value(cboe_ratios_payload())
    await service.snapshot(refresh="force", macro_snapshot=macro_snapshot(), preloaded_risk_indices=risk_indices_payload(), preloaded_qqq_options=qqq_payload())
    restarted = RiskContextRuntimeService(cfg)
    restarted.vix_futures_provider.fetch = _fail_network
    cached, _ = await restarted.snapshot(refresh="auto", macro_snapshot={})
    assert cached["status"] == "available"
    assert RiskContextHistoryRepository(cfg).count() == 1


@pytest.mark.asyncio
async def test_last_known_good_is_preserved_when_new_candidate_is_worse(tmp_path) -> None:
    cfg = settings(tmp_path)
    repo = RiskContextHistoryRepository(cfg)
    good = canonical(cfg)
    repo.append(good)
    service = RiskContextRuntimeService(cfg, repository=repo)
    service.vix_futures_provider.fetch = lambda: _async_value({"status": "not_found", "contracts": [], "diagnostics": {}})
    service.put_call_provider.fetch = lambda: _async_value({"status": "not_found", "ratios": [], "diagnostics": {}})
    service.risk_indices_provider.fetch = lambda: _async_value({"status": "not_found", "indices": {}, "history": {}, "diagnostics": {}})
    service.qqq_options_provider.fetch = lambda: _async_value({"status": "not_found", "contracts": [], "diagnostics": {}})
    result, _ = await service.snapshot(refresh="force", macro_snapshot={}, preloaded_risk_indices={"status": "not_found"}, preloaded_qqq_options={"status": "not_found"})
    assert result["diagnostics"]["last_known_good_used"] is True
    assert repo.count() == 1


def test_http_consumer_serializes_canonical_and_legacy_blocks() -> None:
    result = canonical()
    full = {
        "symbol": "MNQ",
        "generated_at_utc": "2026-07-11T12:00:00Z",
        "service_role": "data provider only",
        "risk_context": result,
        "risk_sentiment": build_legacy_risk_sentiment(result),
        "data_quality": {},
    }
    consumer = build_ai_trader_market_context(full)
    json.dumps(consumer)
    assert consumer["risk_context"]["vvix"]["value"] == 90
    assert consumer["risk_sentiment"]["put_call_ratio"]["value"] == .8
    assert consumer["risk_context"]["diagnostics"]["AI_called"] is False


@pytest.mark.parametrize("field", ["source_summary", "quality", "diagnostics", "history", "derived_context", "data_alignment", "put_call", "vix_term_structure", "vvix", "skew", "vix"])
def test_http_contract_required_fields_present(field) -> None:
    assert field in canonical()


async def _async_value(value):
    return value


async def _fail_network():
    raise AssertionError("network must not be called")
