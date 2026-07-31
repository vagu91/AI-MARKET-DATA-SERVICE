from datetime import UTC, datetime

import pytest

from app.models.common import Impact, ProviderType
from app.models.events import EconomicEvent, EventEnrichment
from app.services.deterministic_actual_resolver import official_actual_mapping
from app.services.market_context_builder import build_event_calendar
from app.services.official_actual_semantics import (
    metric_change_basis_from_text,
    metric_semantics_mismatch_reason,
)


def _pce_event(*, metric_id: str | None) -> EconomicEvent:
    release_at = datetime(2026, 7, 30, 12, 30, tzinfo=UTC)
    return EconomicEvent(
        event_id="xtb:145296:2026-07-30",
        occurrence_id="xtb:145296:2026-07-30",
        name="PCE A/A",
        category="PCE",
        metric_id=metric_id,
        reference_period="2026-06",
        frequency="monthly",
        date=release_at.date().isoformat(),
        time_utc=release_at,
        impact=Impact.HIGH,
        source="XTB Economic Calendar",
        source_url="https://www.xtb.com/it/analisi-di-mercato/calendario-economico",
        reliability=0.7,
        event_risk_level=Impact.HIGH,
        enrichment=EventEnrichment(
            actual=-0.1,
            previous=4.1,
            source="XTB Economic Calendar",
            source_url="https://www.xtb.com/it/analisi-di-mercato/calendario-economico",
            provider_type=ProviderType.API,
            reliability=0.7,
            confidence=0.7,
        ),
    )


def test_legacy_pce_aa_never_becomes_mom() -> None:
    event = _pce_event(metric_id=None)

    calendar = build_event_calendar([event])

    metrics = calendar["critical_macro_events"][0].enrichment.metrics
    assert [metric["metric_id"] for metric in metrics] == ["headline_pce_yoy"]


def test_legacy_pce_aa_with_conflicting_explicit_metric_fails_closed() -> None:
    event = _pce_event(metric_id="headline_pce_mom")

    calendar = build_event_calendar([event])

    assert calendar["critical_macro_events"][0].enrichment.metrics == []


def test_legacy_pce_without_change_basis_fails_closed() -> None:
    event = _pce_event(metric_id=None)
    event.name = "PCE"

    calendar = build_event_calendar([event])

    assert calendar["critical_macro_events"][0].enrichment.metrics == []


@pytest.mark.parametrize(
    ("name", "metric_id"),
    [
        ("PCE A/A", "headline_pce_mom"),
        ("CPI (YoY)", "headline_cpi_mom"),
        ("PPI (MoM)", "headline_ppi_yoy"),
        ("Core PCE A/A", "headline_pce_yoy"),
        ("PCE base A/A", "headline_pce_yoy"),
        ("PCE A/A and M/M", "headline_pce_mom"),
        (
            "Indice dei prezzi al consumo A/A",
            "headline_pce_yoy",
        ),
    ],
)
def test_official_actual_mapping_rejects_frequency_mismatch(
    name: str,
    metric_id: str,
) -> None:
    assert official_actual_mapping({"name": name, "metric_id": metric_id}) is None


def test_official_actual_mapping_accepts_matching_pce_yoy() -> None:
    mapping = official_actual_mapping(
        {
            "occurrence_id": "xtb:145296:2026-07-30",
            "name": "PCE A/A",
            "metric_id": "headline_pce_yoy",
            "frequency": "monthly",
        }
    )

    assert mapping == {
        "metric_id": "headline_pce_yoy",
        "provider": "BEA",
        "source_series": "BEA:PCE_PRICE_INDEX",
        "frequency": "monthly",
        "unit": "percent",
    }


def test_official_mapping_uses_all_frequency_evidence() -> None:
    assert (
        official_actual_mapping(
            {
                "name": "PCE",
                "metric_id": "headline_pce_mom",
                "frequency": "monthly",
                "evaluation_method": "A/A",
            }
        )
        is None
    )


def test_change_basis_parser_rejects_ambiguity_and_substrings() -> None:
    assert metric_change_basis_from_text("momentum indicator") is None
    assert metric_change_basis_from_text("PCE A/A and M/M") is None
    assert metric_semantics_mismatch_reason(
        "headline_pce_mom",
        name="PCE A/A and M/M",
    ) == "EVENT_METRIC_FREQUENCY_AMBIGUOUS"


def test_change_basis_parser_preserves_xtb_italian_markers() -> None:
    assert metric_change_basis_from_text("PCE annuale") == "yoy"
    assert metric_change_basis_from_text("PCE mensile") == "mom"
