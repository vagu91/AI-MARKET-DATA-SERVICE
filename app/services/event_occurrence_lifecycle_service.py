from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any


SCHEDULE_ONLY_ENTITY_TYPE = "schedule_only"
OPERATIONAL_CALENDAR_ENTITY_TYPES = frozenset(
    {
        "macro_actual",
        "earnings_actual",
        "fomc_decision",
        "fomc_communication",
    }
)

_MACRO_TOKENS = frozenset(
    {
        "CPI",
        "PPI",
        "PCE",
        "GDP",
        "NFP",
        "NONFARM",
        "PAYROLL",
        "UNEMPLOYMENT",
        "EMPLOYMENT",
        "RETAIL_SALES",
        "INDUSTRIAL_PRODUCTION",
        "DURABLE_GOODS",
        "HOUSING_STARTS",
        "JOBLESS_CLAIMS",
        "INFLATION",
        "MACRO",
        "ECONOMIC_INDICATOR",
    }
)
_MACRO_SOURCES = frozenset({"BLS", "BEA", "CENSUS", "FRED"})
_EARNINGS_TOKENS = frozenset(
    {"EARNINGS", "EARNINGS_RELEASE", "EARNINGS_REPORT"}
)
_FOMC_DECISION_TOKENS = frozenset(
    {
        "FOMC_DECISION",
        "FED_RATE_DECISION",
        "FED_RATES",
        "RATE_DECISION",
        "TARGET_RATE_DECISION",
    }
)
_FOMC_COMMUNICATION_TOKENS = frozenset(
    {
        "FOMC_COMMUNICATION",
        "FOMC_MINUTES",
        "FOMC_STATEMENT",
        "FED_COMMUNICATION",
        "FED_SPEECH",
        "PRESS_CONFERENCE",
    }
)
_NON_OUTCOME_TOKENS = frozenset(
    {
        "REGULATORY",
        "REGULATION",
        "GEOPOLITICAL",
        "GEOPOLITICS",
        "SCHEDULED_REGULATORY_EVENT",
        "SCHEDULED_GEOPOLITICAL_EVENT",
    }
)


@dataclass(frozen=True)
class OccurrenceLifecycleClassification:
    entity_type: str
    operational: bool
    outcome_fields: tuple[str, ...]
    outcome_contract: str
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity_type": self.entity_type,
            "operational": self.operational,
            "outcome_fields": list(self.outcome_fields),
            "outcome_contract": self.outcome_contract,
            "reason": self.reason,
        }


def classify_occurrence_lifecycle(
    occurrence: dict[str, Any],
) -> OccurrenceLifecycleClassification:
    """Classify one scheduled occurrence without an implicit macro fallback."""

    tokens = _classification_tokens(occurrence)
    if tokens & _NON_OUTCOME_TOKENS:
        return _schedule_only("scheduled_event_has_no_numeric_outcome_contract")
    if tokens & _EARNINGS_TOKENS or occurrence.get("issuer_event_id"):
        return OccurrenceLifecycleClassification(
            entity_type="earnings_actual",
            operational=True,
            outcome_fields=(
                "eps_actual",
                "revenue_actual",
            ),
            outcome_contract="earnings_reported_results",
            reason="explicit_earnings_occurrence",
        )
    if tokens & _FOMC_DECISION_TOKENS or (
        "FOMC" in tokens and "DECISION" in tokens
    ):
        return OccurrenceLifecycleClassification(
            entity_type="fomc_decision",
            operational=True,
            outcome_fields=("actual",),
            outcome_contract="official_fomc_decision",
            reason="explicit_fomc_decision",
        )
    if tokens & _FOMC_COMMUNICATION_TOKENS or (
        "FOMC" in tokens
        and tokens
        & {
            "MINUTES",
            "STATEMENT",
            "SPEECH",
            "COMMUNICATION",
            "PRESS_CONFERENCE",
        }
    ):
        return OccurrenceLifecycleClassification(
            entity_type="fomc_communication",
            operational=True,
            outcome_fields=("published_at", "source_url"),
            outcome_contract="official_fomc_publication",
            reason="explicit_fomc_communication",
        )
    if _is_numeric_macro_contract(occurrence, tokens=tokens):
        return OccurrenceLifecycleClassification(
            entity_type="macro_actual",
            operational=True,
            outcome_fields=("actual",),
            outcome_contract="numeric_macro_actual",
            reason="explicit_numeric_macro_contract",
        )
    return _schedule_only("unknown_or_unverifiable_outcome_contract")


def _schedule_only(reason: str) -> OccurrenceLifecycleClassification:
    return OccurrenceLifecycleClassification(
        entity_type=SCHEDULE_ONLY_ENTITY_TYPE,
        operational=False,
        outcome_fields=(),
        outcome_contract="schedule_only",
        reason=reason,
    )


def _classification_tokens(occurrence: dict[str, Any]) -> set[str]:
    values = (
        occurrence.get("event_type"),
        occurrence.get("event_kind"),
        occurrence.get("category"),
        occurrence.get("classification_hint"),
        occurrence.get("source"),
        occurrence.get("title"),
        occurrence.get("name"),
    )
    tokens: set[str] = set()
    for value in values:
        normalized = (
            str(value or "")
            .upper()
            .replace("-", "_")
            .replace("/", "_")
            .replace(" ", "_")
        )
        if not normalized:
            continue
        tokens.add(normalized)
        tokens.update(part for part in normalized.split("_") if part)
    return tokens


def _is_numeric_macro_contract(
    occurrence: dict[str, Any],
    *,
    tokens: set[str],
) -> bool:
    source = str(occurrence.get("source") or "").upper()
    if source in _MACRO_SOURCES or bool(tokens & _MACRO_TOKENS):
        return True
    if occurrence.get("metric_id") or occurrence.get("reference_period"):
        return True
    return any(
        _numeric(occurrence.get(field))
        for field in ("actual", "forecast", "previous")
    )


def _numeric(value: Any) -> bool:
    if value in (None, "") or isinstance(value, bool):
        return False
    try:
        Decimal(str(value).replace(",", "").strip().rstrip("%"))
    except (InvalidOperation, ValueError):
        return False
    return True
