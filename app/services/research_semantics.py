from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.text_normalization import contains_mojibake, normalize_payload_text
from app.services.data_freshness_service import parse_datetime


AI_RESEARCH_SEMANTICS = (
    "scheduled_event",
    "official_calendar_event",
    "issuer_announcement",
    "earnings_schedule",
    "current_news",
    "current_market_context",
    "verified_market_metric",
    "verified_corporate_metric",
    "exploratory_context",
    "forecast",
    "consensus",
    "previous",
    "outcome",
    "transcript_url",
)
SERVICE_ONLY_SEMANTICS = ("official_actual",)
EVENT_SEMANTICS = {"scheduled_event", "official_calendar_event"}
ISSUER_EVENT_SEMANTICS = {"issuer_announcement", "earnings_schedule"}
CURRENT_SEMANTICS = {
    "current_news",
    "current_market_context",
    "verified_market_metric",
    "verified_corporate_metric",
}
EVENT_IDENTITY_SEMANTICS = EVENT_SEMANTICS | ISSUER_EVENT_SEMANTICS
OBSERVATION_SEMANTICS = {
    "current_market_context",
    "verified_market_metric",
    "verified_corporate_metric",
}
EVENT_VALUE_SEMANTICS = {
    "actual",
    "official_actual",
    "forecast",
    "consensus",
    "previous",
    "outcome",
    "transcript_url",
}

_ISSUERS_BY_SYMBOL = {
    "AAPL": "Apple",
    "AMZN": "Amazon",
    "AMD": "AMD",
    "AMAT": "Applied Materials",
    "AVGO": "Broadcom",
    "GOOG": "Alphabet",
    "GOOGL": "Alphabet",
    "META": "Meta",
    "MU": "Micron",
    "MSFT": "Microsoft",
    "NFLX": "Netflix",
    "NVDA": "NVIDIA",
    "QCOM": "Qualcomm",
    "TSLA": "Tesla",
}


def normalize_research_claim(
    raw_claim: dict[str, Any],
    *,
    policy: Any,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Normalize text, legacy semantics and lifecycle fields before policy checks."""

    reference = _utc(now or datetime.now(UTC))
    claim = normalize_payload_text(dict(raw_claim))
    input_semantics = str(
        claim.get("field_semantics") or claim.get("field") or "exploratory_context"
    ).lower()
    prior_normalization = (
        claim.get("_semantic_normalization")
        if isinstance(claim.get("_semantic_normalization"), dict)
        else {}
    )
    original_semantics = str(
        prior_normalization.get("original") or input_semantics
    ).lower()
    semantics = _classify_semantics(claim, input_semantics, policy)
    claim["field_semantics"] = semantics
    if semantics in OBSERVATION_SEMANTICS:
        claim["observation_key"] = canonical_observation_key(claim)

    if semantics in EVENT_IDENTITY_SEMANTICS:
        event_value = (
            claim.get("event_at")
            or claim.get("release_at")
            or claim.get("valid_until")
        )
        event_at = parse_datetime(event_value)
        if event_at is not None:
            event_iso = _iso(event_at)
            claim["event_at"] = event_iso
            if semantics in EVENT_SEMANTICS:
                claim["release_at"] = event_iso
            lifecycle_minutes = int(
                policy.semantic_policy(semantics).get("lifecycle_minutes") or 60
            )
            claim["valid_until"] = _iso(event_at + timedelta(minutes=lifecycle_minutes))
            claim["next_refresh_at"] = event_iso
            claim["lifecycle_status"] = (
                "UPCOMING"
                if reference < event_at
                else "AWAITING_OUTCOME"
                if _requires_outcome_refresh(claim)
                else "AWAITING_ACTUAL"
            )
            claim["post_event_semantics"] = (
                "outcome" if _requires_outcome_refresh(claim) else "official_actual"
            )

    if semantics in ISSUER_EVENT_SEMANTICS and not claim.get("issuer"):
        claim["issuer"] = _issuer_for_claim(claim, policy)

    if semantics in CURRENT_SEMANTICS:
        published = parse_datetime(claim.get("published_at")) or next(
            (
                parse_datetime(evidence.get("published_at"))
                for evidence in claim.get("evidence") or []
                if isinstance(evidence, dict) and parse_datetime(evidence.get("published_at"))
            ),
            None,
        )
        if published is not None:
            ttl_minutes = int(policy.semantic_policy(semantics).get("ttl_minutes") or 0)
            valid_until = published + timedelta(minutes=ttl_minutes)
            claim["valid_until"] = _iso(valid_until)
            claim["next_refresh_at"] = _iso(valid_until)
            claim["lifecycle_status"] = (
                "CURRENT" if reference < valid_until else "EXPIRED"
            )

    claim["_semantic_normalization"] = {
        "original": original_semantics,
        "effective": semantics,
        "service_owned": True,
    }
    return claim


def semantic_validation_warnings(
    claim: dict[str, Any],
    *,
    policy: Any,
    now: datetime | None = None,
) -> list[str]:
    reference = _utc(now or datetime.now(UTC))
    semantics = str(claim.get("field_semantics") or "").lower()
    if contains_mojibake(claim):
        return ["mojibake_rejected"]
    if is_not_applicable(claim):
        return (
            []
            if claim.get("_bounded_search_documented") is True
            else ["not_applicable_requires_bounded_search"]
        )

    warnings: list[str] = []
    if semantics in EVENT_IDENTITY_SEMANTICS:
        if not str(claim.get("event_key") or "").strip():
            warnings.append("event_key_required")
        event_at = parse_datetime(claim.get("event_at") or claim.get("release_at"))
        if event_at is None:
            warnings.append("event_at_required")
        elif reference >= event_at:
            warnings.append("scheduled_event_elapsed_refresh_required")
        if not claim.get("valid_until"):
            warnings.append("event_valid_until_required")
    if semantics in ISSUER_EVENT_SEMANTICS and not str(claim.get("issuer") or "").strip():
        warnings.append("issuer_required")
    if semantics == "current_news" and parse_datetime(claim.get("published_at")) is None:
        warnings.append("current_news_published_at_required")
    if semantics == "exploratory_context":
        warnings.append("exploratory_context_not_current")
    if semantics == "official_actual":
        warnings.append("official_actual_requires_deterministic_resolver")
    if not policy.semantic_policy(semantics):
        warnings.append("unsupported_field_semantics")
    return warnings


def canonical_observation_key(claim: dict[str, Any]) -> str:
    """Return a stable service-owned identity for a non-event observation."""

    observation_at = (
        claim.get("period")
        or claim.get("data_as_of")
        or claim.get("valid_from")
        or claim.get("event_at")
        or claim.get("release_at")
        or _first_evidence_time(claim)
        or "undated"
    )
    seed = {
        "field_semantics": str(claim.get("field_semantics") or "").lower(),
        "frequency": str(claim.get("frequency") or "").lower(),
        "metric_id": str(claim.get("metric_id") or "").lower(),
        "observation_at": str(observation_at),
        "symbol": str(claim.get("symbol") or "MNQ").upper(),
        "topic": str(claim.get("topic") or "").lower(),
        "unit": str(claim.get("unit") or "").lower(),
    }
    digest = hashlib.sha256(
        json.dumps(seed, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    return f"observation:{digest[:32]}"


def requires_event_value_projection(claims: list[dict[str, Any]]) -> bool:
    """Return whether accepted claims must update an identified event record."""

    return any(
        str(claim.get("field") or claim.get("field_semantics") or "").lower()
        in EVENT_VALUE_SEMANTICS
        for claim in claims
    )


def document_not_applicable_claims(
    claims: list[dict[str, Any]],
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    plan = payload.get("plan") if isinstance(payload.get("plan"), dict) else {}
    planned = [item for item in plan.get("queries") or [] if isinstance(item, dict)]
    completed_queries = {
        str(item.get("query") or "").strip()
        for item in payload.get("searches") or []
        if isinstance(item, dict) and str(item.get("query") or "").strip()
    }
    output: list[dict[str, Any]] = []
    for raw_claim in claims:
        claim = dict(raw_claim)
        if is_not_applicable(claim):
            topic = str(claim.get("topic") or "")
            documented = [
                str(item.get("query") or "").strip()
                for item in planned
                if topic
                in {
                    str(item.get("topic") or ""),
                    *{
                        str(value)
                        for value in item.get("topics") or []
                        if str(value)
                    },
                }
                and str(item.get("query") or "").strip() in completed_queries
            ]
            claim["_bounded_search_queries"] = sorted(set(documented))
            claim["_bounded_search_documented"] = bool(documented)
        output.append(claim)
    return output


def is_not_applicable(claim: dict[str, Any]) -> bool:
    status = str(claim.get("topic_status") or claim.get("status") or "").upper()
    value = str(claim.get("value") or "").upper()
    return status == "NOT_APPLICABLE" or value == "NOT_APPLICABLE"


def _classify_semantics(claim: dict[str, Any], semantics: str, policy: Any) -> str:
    aliases = {
        "actual": "official_actual",
        "earnings": "earnings_schedule",
        "market_context": "current_market_context",
    }
    if semantics in aliases:
        return aliases[semantics]
    if semantics != "news":
        return semantics

    topic = str(claim.get("topic") or "").lower()
    event_key = str(claim.get("event_key") or "").lower()
    rules = _evidence_rules(claim, policy)
    issuer_official = any(bool(rule.get("issuer_official")) for rule in rules)
    official_event = any(
        int(rule.get("tier") or 5) == 1
        and "event" in {str(item).lower() for item in rule.get("data_types") or []}
        for rule in rules
    )
    if topic == "earnings" or "earnings" in event_key:
        return "earnings_schedule"
    if issuer_official and claim.get("event_key"):
        return "issuer_announcement"
    if official_event and topic in {"macro", "fed_rates", "events"}:
        return "official_calendar_event"
    return "current_news"


def _evidence_rules(claim: dict[str, Any], policy: Any) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    for evidence in claim.get("evidence") or []:
        if not isinstance(evidence, dict):
            continue
        rule = policy.rule_for(
            evidence.get("canonical_url") or evidence.get("source_url"),
            evidence.get("publisher"),
        )
        if rule is not None:
            rules.append(rule)
    return rules


def _issuer_for_claim(claim: dict[str, Any], policy: Any) -> str | None:
    symbol = str(claim.get("symbol") or "").upper()
    if symbol in _ISSUERS_BY_SYMBOL:
        return _ISSUERS_BY_SYMBOL[symbol]
    for rule in _evidence_rules(claim, policy):
        if rule.get("issuer"):
            return str(rule["issuer"])
    return None


def _requires_outcome_refresh(claim: dict[str, Any]) -> bool:
    topic = str(claim.get("topic") or "").lower()
    event_key = str(claim.get("event_key") or "").lower()
    return topic == "fed_rates" or "fomc" in event_key or "speech" in event_key


def _first_evidence_time(claim: dict[str, Any]) -> str | None:
    for evidence in claim.get("evidence") or []:
        if not isinstance(evidence, dict):
            continue
        value = evidence.get("published_at") or evidence.get("retrieved_at")
        parsed = parse_datetime(value)
        if parsed is not None:
            return _iso(parsed)
    return None


def _utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _iso(value: datetime) -> str:
    return _utc(value).replace(microsecond=0).isoformat()
