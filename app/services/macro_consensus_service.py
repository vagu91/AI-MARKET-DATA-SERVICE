from __future__ import annotations

import copy
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

from app.core.config import Settings
from app.models.common import ProviderType
from app.models.events import EconomicEvent
from app.services.data_freshness_service import parse_datetime
from app.services.economic_event_materialization_service import EconomicEventMaterializationService
from app.services.market_fact_repository import MarketFactRepository

logger = logging.getLogger(__name__)

MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}

PRIMARY_METRIC = {
    "CPI": "headline_cpi_mom",
    "PPI": "headline_ppi_mom",
    "PCE": "headline_pce_mom",
    "GDP": "real_gdp_annualized_qoq",
    "NFP": "nonfarm_payrolls_change",
}

METRIC_META = {
    "headline_cpi_mom": ("CPI", "Headline CPI MoM", "MoM", "percent"),
    "headline_cpi_yoy": ("CPI", "Headline CPI YoY", "YoY", "percent"),
    "core_cpi_mom": ("CPI", "Core CPI MoM", "MoM", "percent"),
    "core_cpi_yoy": ("CPI", "Core CPI YoY", "YoY", "percent"),
    "headline_ppi_mom": ("PPI", "Headline PPI MoM", "MoM", "percent"),
    "headline_ppi_yoy": ("PPI", "Headline PPI YoY", "YoY", "percent"),
    "core_ppi_mom": ("PPI", "Core PPI MoM", "MoM", "percent"),
    "core_ppi_yoy": ("PPI", "Core PPI YoY", "YoY", "percent"),
    "headline_pce_mom": ("PCE", "Headline PCE Price Index MoM", "MoM", "percent"),
    "headline_pce_yoy": ("PCE", "Headline PCE Price Index YoY", "YoY", "percent"),
    "core_pce_mom": ("PCE", "Core PCE Price Index MoM", "MoM", "percent"),
    "core_pce_yoy": ("PCE", "Core PCE Price Index YoY", "YoY", "percent"),
    "real_gdp_annualized_qoq": ("GDP", "Real GDP Annualized QoQ", "QoQ annualized", "percent"),
    "nonfarm_payrolls_change": ("NFP", "Nonfarm Payrolls Change", "monthly", "thousands of jobs"),
    "unemployment_rate": ("NFP", "Unemployment Rate", "monthly", "percent"),
    "average_hourly_earnings_mom": ("NFP", "Average Hourly Earnings MoM", "MoM", "percent"),
    "average_hourly_earnings_yoy": ("NFP", "Average Hourly Earnings YoY", "YoY", "percent"),
}

DIAGNOSTIC_FIELDS = (
    "consensus_lookup_count",
    "consensus_candidate_count",
    "consensus_match_count",
    "consensus_rejected_count",
    "consensus_persisted_count",
    "consensus_read_back_count",
    "consensus_materialized_count",
    "consensus_missing_count",
)


@dataclass(frozen=True)
class ConsensusMatch:
    accepted: bool
    metric_id: str | None = None
    match_score: float = 0.0
    rejection_reason: str | None = None
    expected_period: str | None = None
    candidate_period: str | None = None


class MacroConsensusService:
    def __init__(self, settings: Settings, *, facts: MarketFactRepository | None = None) -> None:
        self.settings = settings
        self.facts = facts or MarketFactRepository(settings)
        self.materializer = EconomicEventMaterializationService(settings, facts=self.facts)

    @staticmethod
    def needs_refresh(events: list[EconomicEvent]) -> bool:
        for event in events:
            release = parse_datetime(event.time_utc)
            if (
                event_family(event) in PRIMARY_METRIC
                and reference_period(event.name, release_at=release) is not None
                and not _has_verified_consensus(event)
            ):
                return True
        return False

    def enrich_and_persist(
        self,
        events: list[EconomicEvent],
        provider_payload: dict[str, Any] | None,
        *,
        refresh_mode: str,
    ) -> tuple[list[EconomicEvent], dict[str, Any], dict[str, Any]]:
        payload = copy.deepcopy(provider_payload or {})
        items = [item for item in payload.get("items") or [] if isinstance(item, dict)]
        metrics = {name: 0 for name in DIAGNOSTIC_FIELDS}
        metrics["consensus_lookup_count"] = len([event for event in events if event_family(event) in PRIMARY_METRIC])
        targeted = [item for item in items if candidate_metric_id(item) is not None]
        metrics["consensus_candidate_count"] = len(targeted)
        output: list[EconomicEvent] = []
        matched_occurrences: set[Any] = set()

        for event in events:
            family = event_family(event)
            if family not in PRIMARY_METRIC:
                output.append(event)
                continue
            logger.info("macro_consensus_lookup_started", extra=_log_context(event, event_type=family))
            updated = event.model_copy(deep=True)
            event_matches = 0
            for candidate in targeted:
                candidate_key = _candidate_key(candidate)
                if candidate_key in matched_occurrences:
                    continue
                match = match_consensus_candidate(event, candidate)
                if not match.accepted:
                    if _same_family(event, candidate):
                        candidate.setdefault("rejection_reason", match.rejection_reason)
                        candidate["match_score"] = max(float(candidate.get("match_score") or 0), match.match_score)
                        metrics["consensus_rejected_count"] += 1
                        log_name = {
                            "period_mismatch": "macro_consensus_period_mismatch",
                            "release_time_mismatch": "macro_consensus_release_time_mismatch",
                        }.get(match.rejection_reason, "macro_consensus_candidate_rejected")
                        logger.info(log_name, extra=_log_context(event, candidate, family, match))
                    continue
                metrics["consensus_match_count"] += 1
                event_matches += 1
                matched_occurrences.add(candidate_key)
                logger.info("macro_consensus_candidate_found", extra=_log_context(event, candidate, family, match))
                _merge_candidate(updated, candidate, match.metric_id or "")
                candidate.update({
                    "status": "MATCHED",
                    "matched_event_id": event.event_id,
                    "matched_metric_id": match.metric_id,
                    "match_score": match.match_score,
                    "rejection_reason": None,
                })
                logger.info("macro_consensus_match_succeeded", extra=_log_context(event, candidate, family, match))

            if event_matches == 0 and not _has_verified_consensus(updated):
                metrics["consensus_missing_count"] += 1
                logger.info("macro_consensus_not_found", extra=_log_context(event, event_type=family))
                output.append(updated)
                continue

            _promote_primary_metric(updated)
            changed = updated.enrichment.model_dump(mode="json") != event.enrichment.model_dump(mode="json")
            if changed or refresh_mode == "force":
                fact = self._fact_from_event(updated)
                self.facts.upsert_fact(fact)
                metrics["consensus_persisted_count"] += 1
                logger.info("macro_consensus_persisted", extra=_log_context(updated, event_type=family))
                read_back = self.facts.get_event_enrichment_fact(self.materializer.fact_key(updated))
                if read_back:
                    metrics["consensus_read_back_count"] += 1
                    logger.info("macro_consensus_read_back", extra=_log_context(updated, event_type=family))
                    updated = self.materializer.apply_fact(
                        updated,
                        read_back,
                        cache_status="refreshed",
                        warnings=[],
                        refresh_mode=refresh_mode,
                    )
            if _has_verified_consensus(updated):
                metrics["consensus_materialized_count"] += 1
                logger.info("macro_consensus_materialized", extra=_log_context(updated, event_type=family))
            output.append(updated)

        for item in targeted:
            if _candidate_key(item) in matched_occurrences:
                continue
            item["status"] = "REJECTED"
            item.setdefault("rejection_reason", "no_compatible_official_event")
            logger.info("macro_consensus_candidate_rejected", extra={
                "event_id": None,
                "event_name": item.get("event_name"),
                "event_type": candidate_family(item),
                "release_at": item.get("release_at"),
                "reference_period": item.get("reference_period"),
                "source": item.get("source"),
                "value": item.get("consensus"),
                "unit": item.get("unit"),
                "match_score": 0.0,
                "rejection_reason": item.get("rejection_reason"),
            })

        diagnostics = dict(payload.get("diagnostics") or {})
        diagnostics.update({
            "matched_count": metrics["consensus_match_count"],
            "unmatched_count": max(len(items) - metrics["consensus_match_count"], 0),
            "rejected_count": metrics["consensus_rejected_count"],
        })
        payload["diagnostics"] = diagnostics
        payload["items"] = items
        return output, metrics, payload

    def _fact_from_event(self, event: EconomicEvent) -> dict[str, Any]:
        enrichment = event.enrichment
        valid_until = _consensus_valid_until(event)
        raw = enrichment.model_dump(mode="json")
        return {
            "fact_key": self.materializer.fact_key(event),
            "fact_type": "macro_event_enrichment",
            "country": event.country,
            "category": event.category,
            "event_name": event.name,
            "forecast": enrichment.forecast,
            "previous": enrichment.previous,
            "consensus": enrichment.consensus,
            "actual": enrichment.actual,
            "source": enrichment.consensus_source or enrichment.source,
            "source_url": enrichment.consensus_source_url or enrichment.source_url,
            "provider_type": (
                enrichment.provider_type.value
                if isinstance(enrichment.provider_type, ProviderType)
                else str(enrichment.provider_type or ProviderType.API.value)
            ),
            "reliability": enrichment.reliability,
            "confidence": enrichment.confidence,
            "retrieved_at": enrichment.consensus_retrieved_at or enrichment.retrieved_at or datetime.now(UTC),
            "release_at": event.time_utc,
            "valid_until": valid_until,
            "next_refresh_at": valid_until,
            "status": "active",
            "raw_payload_json": raw,
            "warnings_json": enrichment.warnings,
            "errors_json": enrichment.errors,
        }


def match_consensus_candidate(event: EconomicEvent, candidate: dict[str, Any]) -> ConsensusMatch:
    metric_id = candidate_metric_id(candidate)
    if metric_id is None or event_family(event) != candidate_family(candidate):
        return ConsensusMatch(False, rejection_reason="event_family_mismatch")
    if str(candidate.get("country") or "").upper() != event.country.upper():
        return ConsensusMatch(False, metric_id=metric_id, rejection_reason="country_mismatch")
    if _number(candidate.get("consensus")) is None:
        return ConsensusMatch(False, metric_id=metric_id, rejection_reason="consensus_not_numeric")
    if not bool(candidate.get("consensus_verified")):
        return ConsensusMatch(False, metric_id=metric_id, rejection_reason="consensus_not_verified")
    if not _valid_url(candidate.get("source_url")):
        return ConsensusMatch(False, metric_id=metric_id, rejection_reason="source_url_missing")
    release = parse_datetime(event.time_utc)
    candidate_release = parse_datetime(candidate.get("release_at"))
    if release is None or candidate_release is None or abs((release - candidate_release).total_seconds()) > 90 * 60:
        return ConsensusMatch(False, metric_id=metric_id, rejection_reason="release_time_mismatch")
    expected_period = reference_period(event.name, release_at=release)
    candidate_period = reference_period(candidate.get("reference_period"), release_at=candidate_release)
    if expected_period is None or candidate_period is None or expected_period != candidate_period:
        return ConsensusMatch(
            False,
            metric_id=metric_id,
            rejection_reason="period_mismatch",
            expected_period=_period_label(expected_period),
            candidate_period=_period_label(candidate_period),
        )
    if not _unit_compatible(metric_id, candidate.get("unit")):
        return ConsensusMatch(False, metric_id=metric_id, rejection_reason="unit_mismatch")
    return ConsensusMatch(
        True,
        metric_id=metric_id,
        match_score=1.0,
        expected_period=_period_label(expected_period),
        candidate_period=_period_label(candidate_period),
    )


def event_family(event: EconomicEvent) -> str | None:
    text = _normalized(f"{event.category} {event.name}")
    if "consumer price index" in text or re.search(r"\bcpi\b", text):
        return "CPI"
    if "producer price index" in text or re.search(r"\bppi\b", text):
        return "PPI"
    if "personal income and outlays" in text or "pce price" in text or re.search(r"\bpce\b", text):
        return "PCE"
    if re.search(r"\bgdp\b", text) or "gross domestic product" in text:
        return "GDP"
    if "employment situation" in text or "nonfarm payroll" in text or "non farm payroll" in text:
        return "NFP"
    return None


def candidate_family(candidate: dict[str, Any]) -> str | None:
    metric_id = candidate_metric_id(candidate)
    return METRIC_META.get(metric_id, (None,))[0] if metric_id else None


def candidate_metric_id(candidate: dict[str, Any]) -> str | None:
    text = _normalized(candidate.get("event_name"))
    compact = text.replace(" ", "")
    frequency = "mom" if "mom" in compact else "yoy" if "yoy" in compact else "qoq" if "qoq" in compact else None
    if "cleveland" in text or "index n s a" in text or "index s a" in text:
        return None
    if "average hourly earnings" in text:
        return f"average_hourly_earnings_{frequency}" if frequency in {"mom", "yoy"} else None
    if "unemployment rate" in text:
        return "unemployment_rate"
    if ("nonfarm payroll" in text or "non farm payroll" in text) and "adp" not in text:
        return "nonfarm_payrolls_change"
    if (
        (re.fullmatch(r"gdp qoq", text) or "gross domestic product" in text)
        and not any(token in text for token in ("price", "sales", "gdpnow", "tracker"))
    ):
        return "real_gdp_annualized_qoq"
    if ("core pce" in text or "core personal consumption expenditure" in text) and frequency in {"mom", "yoy"}:
        return f"core_pce_{frequency}"
    if ("pce price" in text or "personal consumption expenditure price" in text) and frequency in {"mom", "yoy"}:
        return f"headline_pce_{frequency}"
    if "personal spending" in text or "personal income" in text:
        return None
    if "core ppi" in text and frequency in {"mom", "yoy"}:
        return f"core_ppi_{frequency}"
    if "ppi ex" in text or "ppi excluding" in text:
        return None
    if (re.search(r"\bppi\b", text) or "producer price index" in text) and frequency in {"mom", "yoy"}:
        return f"headline_ppi_{frequency}"
    if "core cpi" in text and frequency in {"mom", "yoy"}:
        return f"core_cpi_{frequency}"
    if (re.search(r"\bcpi\b", text) or "consumer price index" in text) and frequency in {"mom", "yoy"}:
        return f"headline_cpi_{frequency}"
    return None


def reference_period(value: Any, *, release_at: datetime | None) -> tuple[str, int, int] | None:
    text = _normalized(value)
    year_match = re.search(r"\b(20\d{2})\b", text)
    year = int(year_match.group(1)) if year_match else None
    quarter_match = re.search(r"\bq([1-4])\b|\b([1-4])(?:st|nd|rd|th)? quarter\b", text)
    if quarter_match:
        quarter = int(quarter_match.group(1) or quarter_match.group(2))
        return ("quarter", year or (release_at.year if release_at else 0), quarter)
    month = next((number for name, number in MONTHS.items() if re.search(rf"\b{re.escape(name)}\b", text)), None)
    if month is None:
        return None
    if year is None and release_at is not None:
        year = release_at.year - 1 if month > release_at.month else release_at.year
    return ("month", year or 0, month)


def _merge_candidate(event: EconomicEvent, candidate: dict[str, Any], metric_id: str) -> None:
    _family, label, frequency, unit = METRIC_META[metric_id]
    valid_until = _consensus_valid_until(event)
    incoming = {
        "metric_id": metric_id,
        "label": label,
        "value_type": "thousands" if metric_id == "nonfarm_payrolls_change" else "percent",
        "frequency": frequency,
        "forecast": None,
        "consensus": candidate.get("consensus"),
        "previous": candidate.get("previous"),
        "actual": None,
        "estimate_count": candidate.get("estimate_count"),
        "estimate_low": candidate.get("estimate_low"),
        "estimate_high": candidate.get("estimate_high"),
        "median_estimate": candidate.get("median_estimate"),
        "average_estimate": candidate.get("average_estimate"),
        "forecast_origin": None,
        "consensus_verified": True,
        "consensus_source": "Investing Economic Calendar",
        "consensus_source_url": candidate.get("consensus_source_url") or candidate.get("source_url"),
        "source": "Investing Economic Calendar",
        "source_url": candidate.get("consensus_source_url") or candidate.get("source_url"),
        "provider_type": ProviderType.API.value,
        "validation": {
            "status": "deterministic_verified",
            "checks": ["event_family", "country", "release_time", "reference_period", "unit", "consensus_numeric"],
        },
        "retrieved_at": candidate.get("consensus_retrieved_at"),
        "valid_until": valid_until,
        "period": candidate.get("reference_period"),
        "unit": unit,
        "reliability": 0.84,
        "confidence": 0.84,
        "field_semantics": {
            "forecast_is_consensus": False,
            "forecast_origin": None,
            "consensus_verified": True,
            "consensus_origin": "aggregated_economic_calendar",
            "source_scope": "aggregated_market_expectation",
            "period_match": True,
            "release_time_match": True,
            "actual_is_official": False,
        },
        "warnings": [],
        "provenance": [{
            "source": "Investing Economic Calendar",
            "source_url": candidate.get("consensus_source_url") or candidate.get("source_url"),
            "value": candidate.get("consensus"),
            "retrieved_at": candidate.get("consensus_retrieved_at"),
        }],
    }
    existing = next((metric for metric in event.enrichment.metrics if metric.get("metric_id") == metric_id), None)
    if existing is None:
        event.enrichment.metrics.append(incoming)
        return
    existing_consensus = _number(existing.get("consensus"))
    incoming_consensus = _number(incoming.get("consensus"))
    if existing_consensus is not None and existing_consensus != incoming_consensus:
        warning = f"consensus_conflict:{metric_id}:{existing_consensus}:{incoming_consensus}"
        if warning not in event.enrichment.warnings:
            event.enrichment.warnings.append(warning)
    existing_provenance = list(existing.get("provenance") or [])
    if not existing_provenance and existing.get("source"):
        existing_provenance.append({
            "source": existing.get("source"),
            "source_url": existing.get("source_url"),
            "value": existing_consensus,
            "retrieved_at": existing.get("retrieved_at"),
            "fields": [
                field for field in ("actual", "forecast", "previous", "consensus")
                if existing.get(field) not in (None, "")
            ],
        })
    provenance = _dedupe_provenance([*existing_provenance, *incoming["provenance"]])
    existing_verified = bool(
        (existing.get("field_semantics") or {}).get("consensus_verified")
        or existing.get("consensus_verified")
    )
    existing_reliability = float(existing.get("reliability") or 0)
    preserve_existing_consensus = existing_verified and existing_consensus is not None and existing_reliability >= incoming["reliability"]
    merged = dict(incoming)
    for field in ("actual", "forecast", "previous"):
        if existing.get(field) not in (None, ""):
            merged[field] = existing[field]
    if preserve_existing_consensus:
        for field in (
            "consensus", "consensus_verified", "consensus_source", "consensus_source_url",
            "source", "source_url", "retrieved_at", "valid_until", "reliability", "confidence",
            "estimate_count", "estimate_low", "estimate_high", "median_estimate", "average_estimate",
        ):
            if field in existing:
                merged[field] = existing[field]
        merged["field_semantics"] = dict(existing.get("field_semantics") or incoming["field_semantics"])
    else:
        for field in ("estimate_count", "estimate_low", "estimate_high", "median_estimate", "average_estimate"):
            if merged.get(field) is None and existing.get(field) is not None:
                merged[field] = existing[field]
    existing_semantics = dict(existing.get("field_semantics") or {})
    merged_semantics = {**incoming["field_semantics"], **existing_semantics}
    merged_semantics["consensus_verified"] = True
    merged_semantics["consensus_origin"] = "aggregated_economic_calendar"
    merged_semantics["period_match"] = True
    merged_semantics["release_time_match"] = True
    if existing.get("actual") not in (None, ""):
        merged["actual_source"] = existing.get("actual_source") or existing.get("source")
        merged["actual_source_url"] = existing.get("actual_source_url") or existing.get("source_url")
    if existing.get("forecast") not in (None, ""):
        merged["forecast_source"] = existing.get("forecast_source") or existing.get("source")
        merged["forecast_source_url"] = existing.get("forecast_source_url") or existing.get("source_url")
    merged["field_semantics"] = merged_semantics
    merged["provenance"] = provenance
    field_lineage = dict(existing.get("field_lineage") or {})
    for field in ("actual", "forecast", "previous"):
        if existing.get(field) in (None, ""):
            continue
        field_lineage[field] = {
            "source": existing.get(f"{field}_source") or existing.get("source"),
            "source_url": existing.get(f"{field}_source_url") or existing.get("source_url"),
            "provider_type": existing.get("provider_type"),
            "confidence": existing.get("confidence"),
            "reliability": existing.get("reliability"),
            "evidence": existing.get("evidence") or existing.get("evidence_text"),
            "validation": existing.get("validation") or {},
        }
    if preserve_existing_consensus:
        field_lineage["consensus"] = {
            "source": existing.get("consensus_source") or existing.get("source"),
            "source_url": existing.get("consensus_source_url") or existing.get("source_url"),
            "provider_type": existing.get("provider_type"),
            "confidence": existing.get("confidence"),
            "reliability": existing.get("reliability"),
            "evidence": existing.get("evidence") or existing.get("evidence_text"),
            "validation": existing.get("validation") or {},
        }
    else:
        field_lineage["consensus"] = {
            "source": incoming["source"],
            "source_url": incoming["source_url"],
            "provider_type": ProviderType.API.value,
            "confidence": incoming["confidence"],
            "reliability": incoming["reliability"],
            "evidence": candidate.get("evidence") or candidate.get("evidence_text"),
            "validation": incoming["validation"],
        }
    existing_provider = str(existing.get("provider_type") or "")
    preserved_existing_fields = any(existing.get(field) not in (None, "") for field in ("actual", "forecast", "previous"))
    if preserve_existing_consensus:
        merged["provider_type"] = existing_provider or ProviderType.API.value
        merged["validation"] = existing.get("validation") or incoming["validation"]
    elif preserved_existing_fields and existing_provider not in {"", ProviderType.API.value}:
        merged["provider_type"] = ProviderType.MIXED.value
        merged["validation"] = {
            "status": "field_level_validated",
            "provider_types": sorted({existing_provider, ProviderType.API.value}),
        }
    else:
        merged["provider_type"] = ProviderType.API.value
        merged["validation"] = incoming["validation"]
    merged["field_lineage"] = field_lineage
    existing.update(merged)


def _dedupe_provenance(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        key = (item.get("source"), item.get("source_url"), item.get("value"), item.get("retrieved_at"))
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _promote_primary_metric(event: EconomicEvent) -> None:
    family = event_family(event)
    primary_id = PRIMARY_METRIC.get(family or "")
    primary = next((metric for metric in event.enrichment.metrics if metric.get("metric_id") == primary_id), None)
    if primary is None:
        return
    enrichment = event.enrichment
    if enrichment.forecast in (None, ""):
        enrichment.forecast = primary.get("forecast")
    if enrichment.previous in (None, ""):
        enrichment.previous = primary.get("previous")
    if enrichment.actual in (None, ""):
        enrichment.actual = primary.get("actual")
    enrichment.consensus = primary.get("consensus")
    enrichment.estimate_count = primary.get("estimate_count")
    enrichment.estimate_low = primary.get("estimate_low")
    enrichment.estimate_high = primary.get("estimate_high")
    enrichment.median_estimate = primary.get("median_estimate")
    enrichment.average_estimate = primary.get("average_estimate")
    enrichment.forecast_origin = primary.get("forecast_origin")
    enrichment.consensus_source = primary.get("consensus_source") or primary.get("source")
    enrichment.consensus_source_url = primary.get("consensus_source_url") or primary.get("source_url")
    enrichment.consensus_retrieved_at = parse_datetime(primary.get("retrieved_at"))
    enrichment.consensus_valid_until = parse_datetime(primary.get("valid_until"))
    enrichment.consensus_verified = bool(primary.get("consensus_verified"))
    enrichment.source = enrichment.consensus_source
    enrichment.source_url = enrichment.consensus_source_url
    enrichment.reliability = max(enrichment.reliability, float(primary.get("reliability") or 0))
    enrichment.confidence = max(enrichment.confidence, float(primary.get("confidence") or 0))
    enrichment.field_lineage = dict(primary.get("field_lineage") or {})
    enrichment.validation = dict(primary.get("validation") or enrichment.validation)
    try:
        enrichment.provider_type = ProviderType(str(primary.get("provider_type") or enrichment.provider_type or ProviderType.API.value))
    except ValueError:
        enrichment.provider_type = ProviderType.MIXED


def _has_verified_consensus(event: EconomicEvent) -> bool:
    return bool(event.enrichment.consensus_verified and _number(event.enrichment.consensus) is not None) or any(
        bool(metric.get("consensus_verified")) and _number(metric.get("consensus")) is not None
        for metric in event.enrichment.metrics
    )


def _same_family(event: EconomicEvent, candidate: dict[str, Any]) -> bool:
    return event_family(event) == candidate_family(candidate)


def _candidate_key(candidate: dict[str, Any]) -> tuple[Any, ...]:
    occurrence_id = candidate.get("occurrence_id")
    if occurrence_id not in (None, ""):
        return ("occurrence_id", occurrence_id)
    return (
        "semantic_occurrence",
        candidate.get("event_name"),
        candidate.get("release_at"),
        candidate.get("reference_period"),
        candidate.get("frequency"),
    )


def _consensus_valid_until(event: EconomicEvent, *, now: datetime | None = None) -> str:
    now = now or datetime.now(UTC)
    release = parse_datetime(event.time_utc)
    if release is None:
        return (now + timedelta(hours=6)).replace(microsecond=0).isoformat()
    delta = release - now
    if delta.total_seconds() <= 0:
        return (now + timedelta(days=30)).replace(microsecond=0).isoformat()
    hours = delta.total_seconds() / 3600
    ttl = timedelta(hours=1 if hours <= 6 else 4 if hours <= 48 else 12 if hours <= 24 * 7 else 24)
    return (now + ttl).replace(microsecond=0).isoformat()


def _unit_compatible(metric_id: str, unit: Any) -> bool:
    normalized = str(unit or "").strip().lower()
    if metric_id == "nonfarm_payrolls_change":
        return normalized in {"k", "thousand", "thousands", "thousands of jobs"}
    return normalized in {"%", "percent", "percentage", "pct"}


def _number(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(str(value).replace(",", "").replace("%", ""))
    except ValueError:
        return None


def _valid_url(value: Any) -> bool:
    parsed = urlparse(str(value or ""))
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _period_label(period: tuple[str, int, int] | None) -> str | None:
    return f"{period[0]}:{period[1]}:{period[2]}" if period else None


def _normalized(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _log_context(
    event: EconomicEvent,
    candidate: dict[str, Any] | None = None,
    event_type: str | None = None,
    match: ConsensusMatch | None = None,
) -> dict[str, Any]:
    release = parse_datetime(event.time_utc)
    event_period = _period_label(reference_period(event.name, release_at=release))
    return {
        "event_id": event.event_id,
        "event_name": event.name,
        "event_type": event_type or event_family(event),
        "release_at": str(event.time_utc or ""),
        "reference_period": (candidate or {}).get("reference_period") or event_period,
        "source": (candidate or {}).get("source") or event.source,
        "value": (candidate or {}).get("consensus") if candidate else event.enrichment.consensus,
        "unit": (candidate or {}).get("unit"),
        "match_score": match.match_score if match else None,
        "rejection_reason": match.rejection_reason if match else None,
    }
