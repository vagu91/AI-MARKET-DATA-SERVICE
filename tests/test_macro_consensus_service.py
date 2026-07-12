from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.core.config import Settings
from app.models.common import Impact, ProviderType
from app.models.events import EconomicEvent, EventEnrichment
from app.providers.investing_economic_calendar_provider import _normalize as normalize_investing
from app.services.economic_event_materialization_service import EconomicEventMaterializationService
from app.services.macro_consensus_service import (
    MacroConsensusService,
    _consensus_valid_until,
    _log_context,
    candidate_metric_id,
    match_consensus_candidate,
    merge_consensus_provider_payloads,
)
from app.services.market_context_builder import _critical_event_quality, _enrichment_summary, _normalize_metric
from app.services.market_fact_repository import MarketFactRepository
from app.services.multi_source_runtime_service import MultiSourceRuntimeService


RELEASE = datetime(2099, 7, 14, 12, 30, tzinfo=UTC)


def settings(tmp_path) -> Settings:
    return Settings(_env_file=None, database_path=tmp_path / "market.sqlite")


def official_event(
    category: str = "CPI",
    name: str = "Consumer Price Index (June 2099)",
    *,
    release: datetime = RELEASE,
    event_id: str = "official-event",
) -> EconomicEvent:
    return EconomicEvent(
        event_id=event_id,
        name=name,
        country="US",
        category=category,
        date=release.date().isoformat(),
        time_utc=release,
        impact=Impact.HIGH,
        source="BLS" if category != "GDP" else "BEA",
        source_url="https://www.bls.gov/news.release/" if category != "GDP" else "https://www.bea.gov/news/",
        reliability=0.98,
        event_risk_level=Impact.HIGH,
    )


def occurrence(
    name: str = "CPI (MoM)",
    *,
    period: str = "Jun",
    consensus: object = 0.3,
    unit: str = "%",
    release: datetime = RELEASE,
    occurrence_id: str | None = None,
) -> dict:
    return {
        "occurrence_id": occurrence_id or f"{name}:{period}",
        "event_name": name,
        "category": "economic",
        "country": "US",
        "release_at": release.isoformat().replace("+00:00", "Z"),
        "forecast": None,
        "consensus": consensus,
        "consensus_verified": True,
        "previous": 0.2,
        "reference_period": period,
        "unit": unit,
        "source": "Investing Economic Calendar",
        "source_url": "https://www.investing.com/economic-calendar/",
        "consensus_source": "Investing Economic Calendar",
        "consensus_source_url": "https://www.investing.com/economic-calendar/",
        "consensus_retrieved_at": "2099-07-10T10:00:00Z",
        "estimate_count": None,
        "estimate_low": None,
        "estimate_high": None,
        "median_estimate": None,
        "average_estimate": None,
        "status": "UNMATCHED",
    }


@pytest.mark.parametrize(
    ("category", "official_name", "candidate_name", "period", "unit", "metric_id"),
    [
        ("CPI", "Consumer Price Index (June 2099)", "CPI (MoM)", "Jun", "%", "headline_cpi_mom"),
        ("CPI", "Consumer Price Index (June 2099)", "Core CPI (YoY)", "Jun", "%", "core_cpi_yoy"),
        ("PPI", "Producer Price Index (June 2099)", "PPI (MoM)", "Jun", "%", "headline_ppi_mom"),
        ("PPI", "Producer Price Index (June 2099)", "Core PPI (YoY)", "Jun", "%", "core_ppi_yoy"),
        ("PCE", "Personal Income and Outlays (June 2099)", "PCE Price Index (MoM)", "Jun", "%", "headline_pce_mom"),
        ("PCE", "Personal Income and Outlays (June 2099)", "Core PCE Price Index (YoY)", "Jun", "%", "core_pce_yoy"),
        ("GDP", "Gross Domestic Product, 2nd Quarter 2099", "GDP (QoQ)", "Q2", "%", "real_gdp_annualized_qoq"),
        ("NFP", "Employment Situation (June 2099)", "Nonfarm Payrolls", "Jun", "K", "nonfarm_payrolls_change"),
        ("NFP", "Employment Situation (June 2099)", "Unemployment Rate", "Jun", "%", "unemployment_rate"),
        ("NFP", "Employment Situation (June 2099)", "Average Hourly Earnings (MoM)", "Jun", "%", "average_hourly_earnings_mom"),
        ("Initial Jobless Claims", "Initial Jobless Claims (July 2099)", "Initial Jobless Claims", "Jul", "K", "initial_jobless_claims"),
    ],
)
def test_semantic_temporal_matching_for_critical_metrics(category, official_name, candidate_name, period, unit, metric_id):
    event = official_event(category, official_name)
    candidate = occurrence(candidate_name, period=period, unit=unit)

    match = match_consensus_candidate(event, candidate)

    assert match.accepted is True
    assert match.metric_id == metric_id
    assert match.match_score == 1.0


def test_core_headline_and_frequency_are_distinct_metrics():
    assert candidate_metric_id(occurrence("CPI (MoM)")) == "headline_cpi_mom"
    assert candidate_metric_id(occurrence("CPI (YoY)")) == "headline_cpi_yoy"
    assert candidate_metric_id(occurrence("Core CPI (MoM)")) == "core_cpi_mom"
    assert candidate_metric_id(occurrence("PPI (MoM)")) == "headline_ppi_mom"
    assert candidate_metric_id(occurrence("Core PPI (MoM)")) == "core_ppi_mom"


def test_pce_does_not_match_personal_spending():
    assert candidate_metric_id(occurrence("Personal Spending (MoM)")) is None
    assert match_consensus_candidate(
        official_event("PCE", "Personal Income and Outlays (June 2099)"),
        occurrence("Personal Spending (MoM)"),
    ).accepted is False


@pytest.mark.parametrize(
    "name",
    ["Atlanta Fed GDPNow", "GDP Sales", "GDP Price Index (QoQ)", "PPI ex. Food/Energy/Transport (MoM)", "ADP Nonfarm Employment Change"],
)
def test_related_but_non_headline_metrics_are_excluded(name):
    assert candidate_metric_id(occurrence(name)) is None


@pytest.mark.parametrize(
    ("candidate", "reason"),
    [
        (occurrence(period="May"), "period_mismatch"),
        (occurrence(release=RELEASE + timedelta(hours=2)), "release_time_mismatch"),
        ({**occurrence(), "country": "CA"}, "country_mismatch"),
        ({**occurrence(), "consensus": "not-a-number"}, "consensus_not_numeric"),
        ({**occurrence(), "consensus_verified": False}, "consensus_not_verified"),
        ({**occurrence(), "source_url": None, "consensus_source_url": None}, "source_url_missing"),
        (occurrence(unit="K"), "unit_mismatch"),
    ],
)
def test_invalid_candidates_are_rejected_with_specific_reason(candidate, reason):
    match = match_consensus_candidate(official_event(), candidate)
    assert match.accepted is False
    assert match.rejection_reason == reason


def test_similar_family_does_not_cross_match():
    match = match_consensus_candidate(official_event("PPI", "Producer Price Index (June 2099)"), occurrence("CPI (MoM)"))
    assert match.accepted is False
    assert match.rejection_reason == "event_family_mismatch"


def test_future_actual_is_rejected_and_previous_is_not_promoted():
    provider_event = {"event_id": "101", "event_translated": "CPI (MoM)", "country_id": "5"}
    raw = {
        "event_id": "101",
        "occurrence_id": "future-cpi",
        "occurrence_time": "2099-07-14T12:30:00Z",
        "actual": "0.4%",
        "forecast": "--",
        "previous": "0.2%",
        "period": "Jun",
        "unit": "%",
    }

    normalized = normalize_investing(provider_event, raw, now=datetime(2099, 7, 10, tzinfo=UTC))

    assert normalized["status"] == "REJECTED_TEMPORAL"
    assert "forecast" not in normalized


def test_provider_maps_aggregate_to_consensus_without_duplicate_forecast():
    provider_event = {"event_id": "101", "event_translated": "CPI (MoM)", "country_id": "5"}
    raw = {
        "event_id": "101",
        "occurrence_id": "released-cpi",
        "occurrence_time": "2099-07-14T12:30:00Z",
        "actual": "--",
        "forecast": "0.3%",
        "previous": "0.2%",
        "period": "Jun",
        "unit": "%",
    }

    normalized = normalize_investing(provider_event, raw, now=datetime(2099, 7, 10, tzinfo=UTC))

    assert normalized["forecast"] is None
    assert normalized["consensus"] == 0.3
    assert normalized["consensus_verified"] is True
    assert normalized["consensus_source"] == "Investing Economic Calendar"


def test_range_and_distribution_are_preserved_without_averaging(tmp_path):
    candidate = {
        **occurrence(),
        "estimate_count": 42,
        "estimate_low": 0.1,
        "estimate_high": 0.5,
        "median_estimate": 0.3,
        "average_estimate": 0.31,
    }
    enriched, metrics, _ = MacroConsensusService(settings(tmp_path)).enrich_and_persist(
        [official_event()],
        {"status": "found", "items": [candidate]},
        refresh_mode="force",
    )

    metric = enriched[0].enrichment.metrics[0]
    assert metrics["consensus_match_count"] == 1
    assert metric["consensus"] == 0.3
    assert metric["estimate_low"] == 0.1
    assert metric["estimate_high"] == 0.5
    assert metric["median_estimate"] == 0.3
    assert metric["average_estimate"] == 0.31


def test_xtb_candidate_uses_explicit_metric_and_persists_lineage(tmp_path):
    candidate = {
        **occurrence("Titolo localizzato", period="Jun"),
        "metric_id": "headline_cpi_mom",
        "source": "XTB Economic Calendar",
        "consensus_source": "XTB Economic Calendar",
        "source_url": "https://www.xtb.com/it/calendario-economico",
        "consensus_source_url": "https://www.xtb.com/it/calendario-economico",
        "reliability": 0.80,
        "actual": 0.4,
    }
    enriched, metrics, _ = MacroConsensusService(settings(tmp_path)).enrich_and_persist(
        [official_event()],
        {"status": "found", "items": [candidate]},
        refresh_mode="force",
    )
    metric = enriched[0].enrichment.metrics[0]
    assert metrics["consensus_persisted_count"] == 1
    assert metrics["consensus_read_back_count"] == 1
    assert metric["consensus_source"] == "XTB Economic Calendar"
    assert metric["actual"] == 0.4
    assert metric["field_lineage"]["actual"]["source"] == "XTB Economic Calendar"


def test_ranked_consensus_preserves_higher_rank_and_uses_xtb_actual(tmp_path):
    investing = {"status": "found", "source": "Investing Economic Calendar", "items": [occurrence(consensus=0.3)]}
    xtb_candidate = {
        **occurrence("Titolo localizzato", consensus=0.4, occurrence_id="xtb-1"),
        "metric_id": "headline_cpi_mom",
        "source": "XTB Economic Calendar",
        "consensus_source": "XTB Economic Calendar",
        "source_url": "https://www.xtb.com/it/calendario-economico",
        "consensus_source_url": "https://www.xtb.com/it/calendario-economico",
        "reliability": 0.80,
        "actual": 0.5,
    }
    merged = merge_consensus_provider_payloads(investing, {"status": "found", "source": "XTB Economic Calendar", "items": [xtb_candidate]})
    enriched, _, _ = MacroConsensusService(settings(tmp_path)).enrich_and_persist(
        [official_event()], merged, refresh_mode="force"
    )
    metric = enriched[0].enrichment.metrics[0]
    assert metric["consensus"] == 0.3
    assert metric["consensus_source"] == "Investing Economic Calendar"
    assert metric["actual"] == 0.5
    assert metric["field_lineage"]["actual"]["source"] == "XTB Economic Calendar"
    assert any("consensus_conflict" in warning for warning in enriched[0].enrichment.warnings)


def test_lower_precedence_consensus_does_not_erase_actual_forecast_or_stronger_consensus(tmp_path):
    event = official_event()
    event.enrichment = EventEnrichment(
        forecast=0.25,
        forecast_origin="single_institution",
        actual=0.4,
        consensus=0.28,
        consensus_verified=True,
        metrics=[{
            "metric_id": "headline_cpi_mom",
            "forecast": 0.25,
            "actual": 0.4,
            "consensus": 0.28,
            "consensus_verified": True,
            "source": "Higher Priority Survey",
            "source_url": "https://survey.test/cpi",
            "reliability": 0.95,
            "field_semantics": {"consensus_verified": True, "actual_is_official": True},
        }],
        source="BLS",
        source_url="https://www.bls.gov/news.release/cpi.htm",
        provider_type=ProviderType.API,
        reliability=0.95,
        confidence=0.95,
    )

    enriched, _, _ = MacroConsensusService(settings(tmp_path)).enrich_and_persist(
        [event], {"status": "found", "items": [occurrence(consensus=0.3)]}, refresh_mode="auto"
    )

    metric = enriched[0].enrichment.metrics[0]
    assert metric["actual"] == 0.4
    assert metric["forecast"] == 0.25
    assert metric["consensus"] == 0.28
    assert metric["field_semantics"]["actual_is_official"] is True
    assert metric["actual_source"] == "Higher Priority Survey"
    assert any("consensus_conflict" in warning for warning in enriched[0].enrichment.warnings)
    assert len(metric["provenance"]) == 2


def test_mixed_ai_and_deterministic_consensus_has_field_level_lineage(tmp_path):
    cfg = settings(tmp_path)
    event = official_event()
    event.enrichment = EventEnrichment(
        forecast=0.25,
        previous=0.5,
        metrics=[
            {
                "metric_id": "headline_cpi_mom",
                "forecast": 0.25,
                "previous": 0.5,
                "source": "AI researched source",
                "source_url": "https://research.test/cpi",
                "provider_type": "AI_RESEARCHER_CODEX_CLI",
                "reliability": 0.9,
                "confidence": 0.9,
                "evidence": "The cited source reports the forecast and previous value.",
                "validation": {"status": "accepted", "reasons": []},
            }
        ],
        source="AI researched source",
        source_url="https://research.test/cpi",
        provider_type=ProviderType.AI_RESEARCHER_CODEX_CLI,
        reliability=0.9,
        confidence=0.9,
        validation={"status": "accepted", "reasons": []},
    )

    enriched, _, _ = MacroConsensusService(cfg).enrich_and_persist(
        [event], {"status": "found", "items": [occurrence(consensus=0.3)]}, refresh_mode="force"
    )

    result = enriched[0].enrichment
    metric = result.metrics[0]
    assert result.provider_type == ProviderType.MIXED
    assert metric["provider_type"] == "MIXED"
    assert metric["field_lineage"]["forecast"]["provider_type"] == "AI_RESEARCHER_CODEX_CLI"
    assert metric["field_lineage"]["previous"]["evidence"]
    assert metric["field_lineage"]["consensus"]["provider_type"] == "API"
    fact = MarketFactRepository(cfg).get_event_enrichment_fact(
        EconomicEventMaterializationService(cfg).fact_key(event)
    )
    assert fact["provider_type"] == "MIXED"


def test_persistence_readback_new_service_and_json_fields_survive(tmp_path):
    cfg = settings(tmp_path)
    candidate = {
        **occurrence(),
        "estimate_count": 25,
        "estimate_low": 0.1,
        "estimate_high": 0.4,
        "median_estimate": 0.3,
        "average_estimate": 0.29,
    }
    service = MacroConsensusService(cfg)
    enriched, counters, _ = service.enrich_and_persist(
        [official_event()], {"status": "found", "items": [candidate]}, refresh_mode="force"
    )
    fact_key = service.materializer.fact_key(enriched[0])

    fact = MarketFactRepository(cfg).get_event_enrichment_fact(fact_key)
    restarted = EconomicEventMaterializationService(cfg, facts=MarketFactRepository(cfg))
    restored = restarted.materialize_event(
        official_event(), refresh_mode="false", metrics=restarted.empty_metrics()
    )

    assert counters["consensus_persisted_count"] == 1
    assert counters["consensus_read_back_count"] == 1
    assert fact is not None
    assert float(fact["consensus"]) == 0.3
    assert fact["raw_payload"]["consensus"] == 0.3
    assert restored.enrichment.consensus == 0.3
    assert restored.enrichment.consensus_verified is True
    assert restored.enrichment.estimate_count == 25
    assert restored.enrichment.estimate_low == 0.1
    assert restored.enrichment.metrics[0]["field_semantics"]["consensus_origin"] == "aggregated_economic_calendar"
    assert restored.enrichment.cache_status == "hit"


def test_matching_diagnostics_and_occurrence_annotations(tmp_path):
    payload = {
        "status": "found",
        "items": [occurrence(), occurrence(period="May", occurrence_id="wrong-period")],
        "diagnostics": {},
    }
    enriched, counters, annotated = MacroConsensusService(settings(tmp_path)).enrich_and_persist(
        [official_event()], payload, refresh_mode="force"
    )

    by_id = {item["occurrence_id"]: item for item in annotated["items"]}
    assert enriched[0].enrichment.consensus == 0.3
    assert enriched[0].enrichment.previous == 0.2
    assert counters == {
        "consensus_lookup_count": 1,
        "consensus_candidate_count": 2,
        "consensus_match_count": 1,
        "consensus_rejected_count": 1,
        "consensus_persisted_count": 1,
        "consensus_read_back_count": 1,
        "consensus_materialized_count": 1,
        "consensus_missing_count": 0,
    }
    assert by_id["CPI (MoM):Jun"]["status"] == "MATCHED"
    assert by_id["wrong-period"]["status"] == "REJECTED"
    assert by_id["wrong-period"]["rejection_reason"] == "period_mismatch"


def test_structured_log_context_includes_official_reference_period_and_source():
    context = _log_context(official_event())
    assert context["reference_period"] == "month:2099:6"
    assert context["source"] == "BLS"


def test_dynamic_ttl_tightens_near_release_and_retains_post_release_baseline():
    now = datetime(2099, 7, 1, 12, 0, tzinfo=UTC)

    far = datetime.fromisoformat(_consensus_valid_until(official_event(release=now + timedelta(days=10)), now=now))
    week = datetime.fromisoformat(_consensus_valid_until(official_event(release=now + timedelta(days=5)), now=now))
    near = datetime.fromisoformat(_consensus_valid_until(official_event(release=now + timedelta(hours=24)), now=now))
    imminent = datetime.fromisoformat(_consensus_valid_until(official_event(release=now + timedelta(hours=4)), now=now))
    released = datetime.fromisoformat(_consensus_valid_until(official_event(release=now - timedelta(hours=1)), now=now))

    assert far - now == timedelta(hours=24)
    assert week - now == timedelta(hours=12)
    assert near - now == timedelta(hours=4)
    assert imminent - now == timedelta(hours=1)
    assert released - now == timedelta(days=30)


async def test_runtime_auto_cache_force_bypass_and_false_zero_network(tmp_path):
    runtime = MultiSourceRuntimeService(settings(tmp_path))
    calls = 0

    async def fetch():
        nonlocal calls
        calls += 1
        return {
            "status": "found",
            "source": "Investing Economic Calendar",
            "source_url": "https://www.investing.com/economic-calendar/",
            "retrieved_at": datetime.now(UTC).isoformat(),
            "valid_until": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            "items": [occurrence()],
            "warnings": [],
            "errors": [],
        }

    runtime.investing_calendar.fetch = fetch
    first = await runtime.provider("investing_economic_calendar", refresh="auto")
    second = await runtime.provider("investing_economic_calendar", refresh="auto")
    forced = await runtime.provider("investing_economic_calendar", refresh="force")
    cache_only = await runtime.provider("investing_economic_calendar", refresh="false")

    assert calls == 2
    assert first["provider_calls"] == 1
    assert second["provider_calls"] == 0
    assert forced["provider_calls"] == 1
    assert cache_only["provider_calls"] == 0
    assert cache_only["cache_used"] is True


def test_needs_refresh_only_for_eligible_periodic_events_without_consensus():
    event = official_event()
    assert MacroConsensusService.needs_refresh([event]) is True
    event.enrichment.consensus = 0.3
    event.enrichment.consensus_verified = True
    assert MacroConsensusService.needs_refresh([event]) is False
    assert MacroConsensusService.needs_refresh([official_event(name="Consumer Price Index")]) is False


def test_quality_indicators_and_surprise_use_consensus_not_previous():
    previous_only = official_event()
    previous_only.enrichment.previous = 0.2
    consensus = official_event(event_id="consensus")
    consensus.enrichment = EventEnrichment(
        previous=0.2,
        consensus=0.3,
        consensus_verified=True,
        metrics=[{
            "metric_id": "headline_cpi_mom",
            "previous": 0.2,
            "consensus": 0.3,
            "consensus_verified": True,
            "estimate_low": 0.1,
            "estimate_high": 0.5,
            "field_semantics": {"consensus_verified": True},
        }],
    )

    previous_quality = _critical_event_quality([previous_only], refresh_mode="false")
    consensus_quality = _critical_event_quality([consensus], refresh_mode="false")
    summary = _enrichment_summary(consensus.enrichment.model_dump(mode="json"))

    assert previous_quality["completeness_score"] == 0.5
    assert previous_quality["complete_event_count"] == 0
    assert consensus_quality["completeness_score"] == 1.0
    assert summary["has_verified_consensus"] is True
    assert summary["has_estimate_distribution"] is True
    assert summary["has_single_source_forecast"] is False
    assert summary["surprise_ready"] is False


def test_official_actual_enables_surprise_and_previous_never_becomes_baseline():
    released = official_event(release=datetime(2026, 6, 14, 12, 30, tzinfo=UTC))
    metric = _normalize_metric(
        {
            "metric_id": "headline_cpi_mom",
            "unit": "percent",
            "actual": 0.4,
            "consensus": 0.3,
            "previous": 0.2,
            "consensus_verified": True,
            "source": "BLS",
            "source_url": "https://www.bls.gov/news.release/cpi.htm",
            "field_semantics": {"consensus_verified": True, "actual_is_official": True},
        },
        released,
    )
    previous_only = _normalize_metric(
        {
            "metric_id": "headline_cpi_mom",
            "actual": 0.4,
            "previous": 0.2,
            "source": "BLS",
            "source_url": "https://www.bls.gov/news.release/cpi.htm",
            "field_semantics": {"actual_is_official": True},
        },
        released,
    )

    assert metric["surprise"]["surprise_value"] == pytest.approx(0.1)
    assert metric["surprise"]["surprise_baseline"] == "consensus"
    assert metric["surprise"]["surprise_direction"] == "above_consensus"
    assert "surprise" not in previous_only


def test_event_json_contract_is_backward_compatible_and_exposes_provenance_fields(tmp_path):
    enriched, _, _ = MacroConsensusService(settings(tmp_path)).enrich_and_persist(
        [official_event()], {"status": "found", "items": [occurrence()]}, refresh_mode="force"
    )
    body = enriched[0].model_dump(mode="json")
    enrichment = body["enrichment"]

    assert body["forecast"] is None
    assert enrichment["forecast"] is None
    assert enrichment["consensus"] == 0.3
    assert enrichment["consensus_verified"] is True
    assert enrichment["consensus_source"] == "Investing Economic Calendar"
    assert enrichment["consensus_source_url"].startswith("https://")
    assert enrichment["metrics"][0]["period"] == "Jun"
    assert enrichment["metrics"][0]["field_semantics"]["forecast_is_consensus"] is False


def test_single_institution_forecast_is_not_marked_as_aggregate_consensus():
    enrichment = EventEnrichment(
        forecast=0.25,
        forecast_origin="single_institution",
        metrics=[{
            "metric_id": "headline_cpi_mom",
            "forecast": 0.25,
            "forecast_origin": "single_institution",
            "consensus": None,
            "field_semantics": {"forecast_origin": "single_institution", "consensus_verified": False},
        }],
    )
    summary = _enrichment_summary(enrichment.model_dump(mode="json"))
    assert summary["has_single_source_forecast"] is True
    assert summary["has_verified_consensus"] is False


def test_missing_provider_data_never_invents_consensus(tmp_path):
    enriched, counters, annotated = MacroConsensusService(settings(tmp_path)).enrich_and_persist(
        [official_event()], {"status": "not_found", "items": []}, refresh_mode="force"
    )
    assert enriched[0].enrichment.consensus is None
    assert enriched[0].enrichment.forecast is None
    assert counters["consensus_missing_count"] == 1
    assert counters["consensus_persisted_count"] == 0
    assert annotated["items"] == []


def test_repeated_auto_merge_deduplicates_provenance(tmp_path):
    service = MacroConsensusService(settings(tmp_path))
    first, _, _ = service.enrich_and_persist(
        [official_event()], {"status": "found", "items": [occurrence()]}, refresh_mode="force"
    )
    second, _, _ = service.enrich_and_persist(
        first, {"status": "found", "items": [occurrence()]}, refresh_mode="auto"
    )
    assert len(second[0].enrichment.metrics[0]["provenance"]) == 1


def test_employment_situation_materializes_nfp_unemployment_and_ahe(tmp_path):
    event = official_event("NFP", "Employment Situation (June 2099)")
    payload = {
        "status": "found",
        "items": [
            occurrence("Nonfarm Payrolls", unit="K", consensus=180, occurrence_id="nfp"),
            occurrence("Unemployment Rate", consensus=4.1, occurrence_id="unemployment"),
            occurrence("Average Hourly Earnings (MoM)", consensus=0.3, occurrence_id="ahe"),
        ],
    }
    enriched, counters, _ = MacroConsensusService(settings(tmp_path)).enrich_and_persist(
        [event], payload, refresh_mode="force"
    )
    metrics = {metric["metric_id"]: metric for metric in enriched[0].enrichment.metrics}
    assert counters["consensus_match_count"] == 3
    assert set(metrics) == {"nonfarm_payrolls_change", "unemployment_rate", "average_hourly_earnings_mom"}
    assert enriched[0].enrichment.consensus == 180


def test_post_release_actual_and_consensus_are_complete_and_surprise_ready():
    event = official_event(release=datetime(2026, 6, 14, 12, 30, tzinfo=UTC))
    event.enrichment = EventEnrichment(
        previous=0.2,
        consensus=0.3,
        actual=0.4,
        consensus_verified=True,
        metrics=[{
            "metric_id": "headline_cpi_mom",
            "previous": 0.2,
            "consensus": 0.3,
            "actual": 0.4,
            "consensus_verified": True,
            "field_semantics": {"consensus_verified": True, "actual_is_official": True},
        }],
    )
    quality = _critical_event_quality([event], refresh_mode="false")
    summary = _enrichment_summary(event.enrichment.model_dump(mode="json"))
    assert quality["completeness_score"] == 1.0
    assert quality["complete_event_count"] == 1
    assert summary["surprise_ready"] is True


async def test_preloaded_investing_block_is_not_called_again_in_snapshot(tmp_path):
    runtime = MultiSourceRuntimeService(settings(tmp_path))

    async def fail_fetch():
        raise AssertionError("preloaded provider must not be called")

    runtime.investing_calendar.fetch = fail_fetch
    preloaded = {"status": "found", "items": [occurrence()], "provider_calls": 1, "cache_used": False}
    snapshot = await runtime.snapshot(
        refresh="false",
        preloaded_blocks={"investing_economic_calendar": preloaded},
    )
    assert snapshot["blocks"]["investing_economic_calendar"] is preloaded
    assert snapshot["data_quality"]["blocks"]["investing_economic_calendar"]["provider_calls"] == 1


def test_five_critical_event_families_receive_only_matching_consensus(tmp_path):
    events = [
        official_event("CPI", "Consumer Price Index (June 2099)", event_id="cpi"),
        official_event("PPI", "Producer Price Index (June 2099)", event_id="ppi"),
        official_event("GDP", "Gross Domestic Product, 2nd Quarter 2099", event_id="gdp"),
        official_event("PCE", "Personal Income and Outlays (June 2099)", event_id="pce"),
        official_event("NFP", "Employment Situation (June 2099)", event_id="nfp"),
    ]
    payload = {
        "status": "found",
        "items": [
            occurrence("CPI (MoM)", consensus=0.3, occurrence_id="cpi"),
            occurrence("PPI (MoM)", consensus=0.2, occurrence_id="ppi"),
            occurrence("GDP (QoQ)", period="Q2", consensus=2.4, occurrence_id="gdp"),
            occurrence("PCE Price Index (MoM)", consensus=0.2, occurrence_id="pce"),
            occurrence("Nonfarm Payrolls", unit="K", consensus=180, occurrence_id="nfp"),
        ],
    }
    enriched, counters, _ = MacroConsensusService(settings(tmp_path)).enrich_and_persist(
        events, payload, refresh_mode="force"
    )
    assert counters["consensus_match_count"] == 5
    assert {event.event_id: event.enrichment.consensus for event in enriched} == {
        "cpi": 0.3,
        "ppi": 0.2,
        "gdp": 2.4,
        "pce": 0.2,
        "nfp": 180,
    }
