from datetime import UTC, datetime, timedelta

from app.services.ai_research_validation_service import (
    ValidationRequest,
    validate_ai_research_result,
)


def base_item(**overrides):
    item = {
        "status": "found",
        "data_type": "macro_forecast",
        "metric_id": "headline_cpi_mom",
        "period": "June 2026",
        "frequency": "MoM",
        "unit": "percent",
        "forecast": 0.3,
        "source_url": "https://www.reuters.com/markets/us/cpi-preview",
        "evidence_text": "Economists expect June 2026 headline CPI to rise 0.3%.",
        "confidence": 0.8,
    }
    item.update(overrides)
    return item


def request(**overrides):
    data = {
        "data_type": "macro_forecast",
        "expected_period": "June 2026",
        "release_at": datetime.now(UTC) + timedelta(days=1),
        "min_confidence": 0.5,
        "require_evidence": True,
    }
    data.update(overrides)
    return ValidationRequest(**data)


def test_missing_evidence_rejected():
    result = validate_ai_research_result(base_item(evidence_text=None), request())
    assert result.status == "rejected_missing_evidence"


def test_invalid_source_rejected():
    result = validate_ai_research_result(base_item(source_url="not-a-url"), request())
    assert result.status == "rejected_invalid_source"


def test_invalid_period_rejected():
    result = validate_ai_research_result(base_item(period="May 2026"), request())
    assert result.status == "rejected_invalid_period"


def test_future_actual_rejected():
    result = validate_ai_research_result(base_item(actual=0.4), request())
    assert result.status == "rejected_future_actual"


def test_unverified_consensus_rejected():
    result = validate_ai_research_result(base_item(consensus=0.3, evidence_text="A forecast points to 0.3% CPI."), request())
    assert result.status == "rejected_unverified_consensus"


def test_summary_without_source_text_rejected():
    result = validate_ai_research_result(
        base_item(data_type="news_summary", summary="A factual summary.", source_text=None),
        request(data_type="news_summary", expected_period=None, release_at=None),
    )
    assert result.status == "rejected_summary_without_source_text"


def test_invalid_canonical_rejected():
    result = validate_ai_research_result(
        base_item(data_type="canonical_url", source_url="https://www.google.com/search?q=story"),
        request(data_type="canonical_url", expected_period=None, release_at=None),
    )
    assert result.status == "rejected_invalid_canonical_url"


def test_invalid_earnings_date_rejected():
    result = validate_ai_research_result(
        base_item(data_type="earnings", earnings_date="2020-01-01", confirmed=True, estimated=False),
        request(data_type="earnings", expected_period=None, release_at=None),
    )
    assert result.status == "rejected_invalid_earnings_date"

