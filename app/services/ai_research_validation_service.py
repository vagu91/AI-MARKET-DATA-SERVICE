from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from app.services.data_freshness_service import parse_datetime

ACCEPTED = "accepted"

CONSENSUS_TERMS = (
    "consensus",
    "median estimate",
    "economists expect",
    "poll estimate",
    "surveyed economists",
    "economists surveyed",
)


@dataclass(frozen=True)
class ValidationRequest:
    data_type: str
    expected_period: str | None = None
    expected_metric_id: str | None = None
    preferred_domains: tuple[str, ...] = ()
    release_at: datetime | None = None
    min_confidence: float = 0.5
    require_evidence: bool = True


@dataclass
class ValidationResult:
    status: str
    reasons: list[str] = field(default_factory=list)

    @property
    def accepted(self) -> bool:
        return self.status == ACCEPTED


def validate_ai_research_result(
    item: dict[str, Any],
    request: ValidationRequest,
    *,
    now: datetime | None = None,
) -> ValidationResult:
    now = now or datetime.now(UTC)
    status = str(item.get("status") or "found").lower()
    if status != "found":
        return ValidationResult(status if status in {"not_found", "ambiguous", "blocked", "access_restricted"} else "not_found")

    source_url = str(item.get("source_url") or _first_metric_field(item, "source_url") or "").strip()
    if not _valid_http_url(source_url):
        return ValidationResult("rejected_invalid_source", ["missing_or_invalid_source_url"])

    if request.preferred_domains and not _domain_allowed(source_url, request.preferred_domains):
        return ValidationResult("rejected_invalid_source", ["domain_not_allowed"])

    confidence = _float(item.get("confidence"))
    if confidence is None or confidence < request.min_confidence:
        return ValidationResult("rejected_missing_evidence", ["confidence_below_minimum"])

    data_type = request.data_type.lower()
    if data_type in {"macro_forecast", "macro_consensus", "macro_previous", "forecast_macro", "consensus_macro"}:
        macro = _validate_macro(item, request, now=now)
        if not macro.accepted:
            return macro

    if data_type in {"news_summary", "summary"} and not item.get("source_text"):
        return ValidationResult("rejected_summary_without_source_text", ["source_text_required"])

    if data_type in {"canonical_url", "news_canonical"} and not _valid_canonical_url(source_url):
        return ValidationResult("rejected_invalid_canonical_url", ["canonical_url_not_publisher_article"])

    if data_type == "earnings":
        earnings = _validate_earnings(item, now=now)
        if not earnings.accepted:
            return earnings

    if request.require_evidence and _has_numeric_value(item) and not _evidence_text(item):
        return ValidationResult("rejected_missing_evidence", ["evidence_text_required"])

    return ValidationResult(ACCEPTED)


def _validate_macro(item: dict[str, Any], request: ValidationRequest, *, now: datetime) -> ValidationResult:
    if not item.get("metric_id") and not _metrics(item):
        return ValidationResult("rejected_ambiguous_metric", ["metric_id_required"])
    if not item.get("unit") and not _first_metric_field(item, "unit"):
        return ValidationResult("rejected_ambiguous_metric", ["unit_required"])
    if not item.get("frequency") and not _first_metric_field(item, "frequency"):
        return ValidationResult("rejected_ambiguous_metric", ["frequency_required"])
    period = str(item.get("period") or _first_metric_field(item, "period") or "").strip()
    if request.expected_period and period and request.expected_period.lower() not in period.lower():
        return ValidationResult("rejected_invalid_period", ["period_mismatch"])
    if request.expected_period and not period:
        return ValidationResult("rejected_invalid_period", ["period_required"])
    actual = item.get("actual")
    if actual not in (None, "") and request.release_at and now < request.release_at:
        return ValidationResult("rejected_future_actual", ["actual_before_release"])
    consensus = item.get("consensus")
    if consensus in (None, ""):
        consensus = _first_metric_field(item, "consensus")
    if consensus not in (None, ""):
        evidence = _evidence_text(item).lower()
        if not any(term in evidence for term in CONSENSUS_TERMS):
            return ValidationResult("rejected_unverified_consensus", ["consensus_terms_missing"])
    if request.require_evidence and _has_numeric_value(item) and not _evidence_text(item):
        return ValidationResult("rejected_missing_evidence", ["evidence_text_required"])
    return ValidationResult(ACCEPTED)


def _validate_earnings(item: dict[str, Any], *, now: datetime) -> ValidationResult:
    earnings_date = parse_datetime(item.get("earnings_date") or item.get("date"))
    if earnings_date is None:
        return ValidationResult("rejected_invalid_earnings_date", ["earnings_date_required"])
    if earnings_date.date() < now.date():
        return ValidationResult("rejected_invalid_earnings_date", ["past_date_not_upcoming"])
    if item.get("confirmed") is True and item.get("estimated") is True:
        return ValidationResult("rejected_invalid_earnings_date", ["confirmed_and_estimated_conflict"])
    if item.get("confirmed") is True and "estimated" in _evidence_text(item).lower():
        return ValidationResult("rejected_invalid_earnings_date", ["estimated_marked_confirmed"])
    return ValidationResult(ACCEPTED)


def _valid_http_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _valid_canonical_url(value: str) -> bool:
    if not _valid_http_url(value):
        return False
    parsed = urlparse(value)
    host = parsed.netloc.lower()
    path = parsed.path.strip("/")
    if not path:
        return False
    return not any(token in host for token in ("google.", "bing.", "search.yahoo.")) and "search" not in path.lower()


def _domain_allowed(value: str, domains: tuple[str, ...]) -> bool:
    host = urlparse(value).netloc.lower()
    return any(host == domain.lower() or host.endswith("." + domain.lower()) for domain in domains)


def _metrics(item: dict[str, Any]) -> list[dict[str, Any]]:
    return [metric for metric in item.get("metrics") or [] if isinstance(metric, dict)]


def _first_metric_field(item: dict[str, Any], field: str) -> Any:
    for metric in _metrics(item):
        if metric.get(field) not in (None, ""):
            return metric.get(field)
    return None


def _has_numeric_value(item: dict[str, Any]) -> bool:
    fields = ("value", "previous", "forecast", "consensus", "actual")
    if any(_float(item.get(field)) is not None for field in fields):
        return True
    return any(any(_float(metric.get(field)) is not None for field in fields) for metric in _metrics(item))


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "").replace("%", ""))
    except ValueError:
        return None


def _evidence_text(item: dict[str, Any]) -> str:
    return str(
        item.get("evidence_text")
        or item.get("extracted_text")
        or item.get("source_text")
        or _first_metric_field(item, "evidence_text")
        or ""
    )
