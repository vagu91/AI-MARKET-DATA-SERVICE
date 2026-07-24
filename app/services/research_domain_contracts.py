from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from app.services.data_freshness_service import parse_datetime


AGENTIC_DOMAIN_FIELDS: dict[str, tuple[str, ...]] = {
    "options_positioning": (
        "total_put_call_ratio",
        "index_put_call_ratio",
        "equity_put_call_ratio",
        "qqq_put_call_ratio",
        "call_volume",
        "put_volume",
        "dominant_expirations",
        "highest_volume_strikes",
        "highest_open_interest_strikes",
        "atm_implied_volatility",
        "put_skew",
        "call_skew",
        "term_structure",
        "estimated_gamma_exposure",
        "estimated_gamma_concentration",
        "coverage_ratio",
    ),
    "market_internals": (
        "advancers",
        "decliners",
        "unchanged",
        "advance_decline_ratio",
        "up_volume",
        "down_volume",
        "up_down_volume_ratio",
        "percent_above_open",
        "percent_above_previous_close",
        "percent_above_vwap",
        "semiconductor_breadth",
        "leadership_concentration",
        "dispersion",
        "new_highs",
        "new_lows",
    ),
    "cross_asset_context": (
        "instrument",
        "symbol_or_series",
        "value",
        "change",
        "change_percent",
        "timeframe",
        "relationship_to_mnq",
        "divergence_status",
    ),
    "earnings_intelligence": (
        "issuer",
        "ticker",
        "event_at",
        "timing_status",
        "expected_eps",
        "expected_revenue",
        "actual_eps",
        "actual_revenue",
        "eps_surprise",
        "revenue_surprise",
        "guidance_direction",
        "guidance_summary",
        "management_commentary",
        "filing_type",
        "lifecycle",
        "next_refresh_at",
    ),
}

DOMAIN_PROFILE_TOPICS = {
    "OPTIONS_POSITIONING_RESEARCH": "options_positioning",
    "MARKET_INTERNALS_RESEARCH": "market_internals",
    "CROSS_ASSET_CONTEXT_RESEARCH": "cross_asset_context",
    "EARNINGS_INTELLIGENCE_RESEARCH": "earnings_intelligence",
}
DOMAIN_TOPICS = frozenset(AGENTIC_DOMAIN_FIELDS)
ACQUISITION_METHODS = frozenset({"agent_web", "public_endpoint", "api_provider"})
GAMMA_METRICS = frozenset(
    {"estimated_gamma_exposure", "estimated_gamma_concentration"}
)
SURPRISE_METRICS = frozenset({"eps_surprise", "revenue_surprise"})
QUALITATIVE_METRICS = frozenset(
    {
        "dominant_expirations",
        "term_structure",
        "instrument",
        "symbol_or_series",
        "timeframe",
        "relationship_to_mnq",
        "divergence_status",
        "issuer",
        "ticker",
        "event_at",
        "timing_status",
        "guidance_direction",
        "guidance_summary",
        "management_commentary",
        "filing_type",
        "lifecycle",
        "next_refresh_at",
    }
)
DOMAIN_FRESHNESS_MINUTES = {
    "options_positioning": 1440,
    "market_internals": 60,
    "cross_asset_context": 60,
    "earnings_intelligence": 43200,
}


def is_agentic_domain(topic: Any) -> bool:
    return str(topic or "").lower() in DOMAIN_TOPICS


def domain_claim_warnings(
    claim: dict[str, Any],
    evidence: list[dict[str, Any]],
    *,
    now: datetime,
    persisted_input_refs: set[str] | None = None,
) -> list[str]:
    topic = str(claim.get("topic") or "").lower()
    if topic not in DOMAIN_TOPICS:
        return []
    metric = str(claim.get("metric_id") or "").lower()
    warnings: list[str] = []
    if metric not in AGENTIC_DOMAIN_FIELDS[topic]:
        warnings.append("domain_metric_not_allowed")
    data_as_of = parse_datetime(claim.get("data_as_of"))
    if data_as_of is None:
        warnings.append("domain_data_as_of_required")
    else:
        reference = _aware(now)
        maximum_age = timedelta(minutes=_freshness_minutes(topic, claim))
        if data_as_of > reference:
            warnings.append("domain_data_as_of_in_future")
        elif reference - data_as_of > maximum_age:
            warnings.append("domain_evidence_stale")
    method = str(claim.get("acquisition_method") or "")
    if method not in ACQUISITION_METHODS:
        warnings.append("unsupported_acquisition_method")
    if str(claim.get("verification_status") or "") != "VERIFIED":
        warnings.append("domain_value_not_server_verified")
    if metric not in QUALITATIVE_METRICS and claim.get("value") is not None:
        warnings.extend(_numeric_evidence_warnings(claim, evidence))
    if metric in GAMMA_METRICS and claim.get("value") is not None:
        required = {
            "methodology": claim.get("methodology"),
            "inputs": claim.get("inputs"),
            "assumptions": claim.get("assumptions"),
            "chain_coverage": claim.get("chain_coverage"),
        }
        if (
            not required["methodology"]
            or not isinstance(required["inputs"], list)
            or not required["inputs"]
            or not isinstance(required["assumptions"], list)
            or not required["assumptions"]
            or required["chain_coverage"] in (None, "")
        ):
            warnings.append("estimated_gamma_inputs_incomplete")
        input_refs = {
            str(item).strip()
            for item in required["inputs"] or []
            if str(item).strip()
        }
        if (
            len(input_refs) < 4
            or persisted_input_refs is None
            or not input_refs.issubset(persisted_input_refs)
        ):
            warnings.append("estimated_gamma_inputs_not_verified_and_persisted")
        if str(claim.get("quality") or "") != "ESTIMATED":
            warnings.append("estimated_gamma_quality_required")
    if metric in SURPRISE_METRICS and claim.get("value") is not None:
        expected_basis = _basis(claim.get("expected_basis"))
        actual_basis = _basis(claim.get("actual_basis"))
        if not expected_basis or not actual_basis:
            warnings.append("earnings_comparison_basis_required")
        elif expected_basis != actual_basis:
            warnings.append("earnings_comparison_basis_incompatible")
    if topic == "earnings_intelligence":
        if not str(claim.get("issuer") or "").strip():
            warnings.append("earnings_issuer_required")
        if not str(claim.get("symbol") or claim.get("ticker") or "").strip():
            warnings.append("earnings_ticker_required")
    return sorted(set(warnings))


def enrich_domain_claim(
    claim: dict[str, Any],
    evidence_rows: list[dict[str, Any]],
    *,
    now: datetime,
    acquisition_method: str = "agent_web",
) -> dict[str, Any]:
    topic = str(claim.get("topic") or "").lower()
    if topic not in DOMAIN_TOPICS:
        return claim
    enriched = dict(claim)
    primary = (
        min(evidence_rows, key=lambda item: int(item.get("source_tier") or 5))
        if evidence_rows
        else {}
    )
    published = next(
        (
            row.get("published_at")
            for row in evidence_rows
            if parse_datetime(row.get("published_at")) is not None
        ),
        claim.get("data_as_of") or claim.get("published_at"),
    )
    observed = next(
        (
            row.get("retrieved_at")
            for row in evidence_rows
            if parse_datetime(row.get("retrieved_at")) is not None
        ),
        _iso(now),
    )
    data_as_of = parse_datetime(published)
    freshness = (
        "FRESH"
        if data_as_of is not None
        and _aware(now) - _aware(data_as_of)
        <= timedelta(minutes=_freshness_minutes(topic, claim))
        else "STALE"
        if data_as_of is not None
        else "UNKNOWN"
    )
    evidence_hash = hashlib.sha256(
        "|".join(
            sorted(
                str(row.get("source_content_hash") or row.get("content_checksum") or "")
                for row in evidence_rows
            )
        ).encode("utf-8")
    ).hexdigest()
    metric = str(claim.get("metric_id") or "").lower()
    enriched.update(
        {
            "data_as_of": _iso(data_as_of) if data_as_of else None,
            "observed_at": _iso(parse_datetime(observed) or now),
            "source_url": primary.get("source_url"),
            "canonical_url": primary.get("canonical_url"),
            "source_domain": primary.get("source_domain"),
            "source_tier": primary.get("source_tier"),
            "freshness_status": freshness,
            "verification_status": "VERIFIED" if evidence_rows else "UNVERIFIED",
            "quality": (
                "ESTIMATED"
                if metric in GAMMA_METRICS and claim.get("value") is not None
                else "VERIFIED"
            ),
            "acquisition_method": (
                acquisition_method
                if acquisition_method in ACQUISITION_METHODS
                else "agent_web"
            ),
            "evidence_hash": evidence_hash if evidence_rows else None,
        }
    )
    return enriched


def field_states(
    topic: str,
    value: Any,
    *,
    now: datetime,
) -> dict[str, str]:
    fields = AGENTIC_DOMAIN_FIELDS.get(topic, ())
    return {
        field: _field_state(_field_value(value, field), topic=topic, now=now)
        for field in fields
    }


def build_domain_projection(
    topic: str,
    claims: list[dict[str, Any]],
    *,
    status: str,
    no_data_reason: str | None = None,
) -> dict[str, Any]:
    fields: dict[str, Any] = {
        field: None for field in AGENTIC_DOMAIN_FIELDS.get(topic, ())
    }
    items: list[dict[str, Any]] = []
    item_keys: set[str] = set()
    accepted = 0
    for claim in claims:
        payload = (
            claim.get("payload")
            if isinstance(claim.get("payload"), dict)
            else claim
        )
        metric = str(
            claim.get("metric_id") or payload.get("metric_id") or ""
        ).lower()
        if metric not in fields:
            continue
        item = {
            "metric_id": metric,
            "value": claim.get("value", payload.get("value")),
            "unit": claim.get("unit") or payload.get("unit"),
            "symbol": claim.get("symbol") or payload.get("symbol"),
            "issuer": claim.get("issuer") or payload.get("issuer"),
            "data_as_of": payload.get("data_as_of"),
            "observed_at": payload.get("observed_at"),
            "source_url": payload.get("canonical_url") or payload.get("source_url"),
            "source_domain": payload.get("source_domain"),
            "source_tier": payload.get("source_tier"),
            "freshness_status": payload.get("freshness_status"),
            "verification_status": payload.get("verification_status"),
            "quality": payload.get("quality"),
            "acquisition_method": payload.get("acquisition_method"),
            "evidence_hash": payload.get("evidence_hash"),
        }
        fields[metric] = item
        if topic in {"cross_asset_context", "earnings_intelligence"}:
            item_key = canonical_json(
                {
                    key: item.get(key)
                    for key in (
                        "metric_id",
                        "symbol",
                        "issuer",
                        "data_as_of",
                        "value",
                    )
                }
            )
            if item_key not in item_keys:
                item_keys.add(item_key)
                items.append(item)
        accepted += 1
    data_as_of = max(
        (
            str(item.get("data_as_of"))
            for item in fields.values()
            if isinstance(item, dict) and item.get("data_as_of")
        ),
        default=None,
    )
    return {
        "status": status,
        "data_as_of": data_as_of,
        "accepted_claims": accepted,
        "no_data_reason": no_data_reason if status == "NO_DATA" else None,
        "fields": fields,
        "items": items,
    }


def compact_domain_projection(value: dict[str, Any]) -> dict[str, Any]:
    compact_fields: dict[str, Any] = {}
    for field, item in (value.get("fields") or {}).items():
        if not isinstance(item, dict):
            compact_fields[field] = None
            continue
        compact_fields[field] = _compact_item(item, include_metric=False)
    compact_items = [
        _compact_item(item, include_metric=True)
        for item in (value.get("items") or [])[:64]
        if isinstance(item, dict)
    ]
    return {
        "status": value.get("status"),
        "data_as_of": value.get("data_as_of"),
        "no_data_reason": value.get("no_data_reason"),
        "fields": compact_fields,
        "items": compact_items,
    }


def no_data_contract(
    *,
    searched_at: str,
    sources_attempted: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "status": "NO_DATA",
        "reason": "no_fresh_verified_source",
        "value": None,
        "searched_at": searched_at,
        "sources_attempted": [
            {
                "source_url": item.get("canonical_url")
                or item.get("requested_url"),
                "source_domain": item.get("source_domain"),
                "fetch_status": item.get("fetch_status"),
                "verification_status": item.get("verification_status"),
                "reason": item.get("rejection_reason"),
            }
            for item in sources_attempted[:20]
        ],
    }


def _numeric_evidence_warnings(
    claim: dict[str, Any],
    evidence: list[dict[str, Any]],
) -> list[str]:
    claimed = _numeric_values(claim.get("value"))
    if not claimed:
        return ["domain_numeric_value_invalid"]
    supported: set[Decimal] = set()
    for item in evidence:
        verification = (
            item.get("_service_verification")
            if isinstance(item.get("_service_verification"), dict)
            else {}
        )
        if verification.get("accepted") is not True:
            continue
        supported.update(_numeric_values(item.get("evidence_text")))
    return (
        []
        if claimed.issubset(supported)
        else ["domain_numeric_value_not_in_verified_evidence"]
    )


def _numeric_values(value: Any) -> set[Decimal]:
    output: set[Decimal] = set()
    for raw in re.findall(r"(?<![A-Za-z])[-+]?\d[\d,]*(?:\.\d+)?", str(value or "")):
        try:
            output.add(Decimal(raw.replace(",", "").lstrip("+")))
        except InvalidOperation:
            continue
    return output


def _field_value(value: Any, field: str) -> Any:
    if not isinstance(value, dict):
        return None
    fields = value.get("fields") if isinstance(value.get("fields"), dict) else {}
    return fields.get(field, value.get(field))


def _field_state(value: Any, *, topic: str, now: datetime) -> str:
    if value is None:
        return "MISSING"
    if not isinstance(value, dict):
        return "MISSING"
    status = str(
        value.get("field_state")
        or value.get("verification_status")
        or value.get("status")
        or ""
    ).upper()
    if status in {"QUARANTINED", "REJECTED", "INVALID"}:
        return "QUARANTINED"
    if status in {"NOT_APPLICABLE", "N/A"}:
        return "NOT_APPLICABLE"
    data_as_of = parse_datetime(value.get("data_as_of"))
    if data_as_of is None:
        return "MISSING"
    if _aware(now) - _aware(data_as_of) > timedelta(
        minutes=DOMAIN_FRESHNESS_MINUTES[topic]
    ):
        return "STALE"
    if str(value.get("verification_status") or "").upper() not in {
        "VERIFIED",
        "SUPPORTED",
    }:
        return "MISSING"
    return "SATISFIED"


def _compact_item(item: dict[str, Any], *, include_metric: bool) -> dict[str, Any]:
    keys = (
        "metric_id",
        "value",
        "unit",
        "symbol",
        "issuer",
        "data_as_of",
        "freshness_status",
        "verification_status",
        "quality",
        "acquisition_method",
        "source_domain",
        "source_tier",
    )
    return {
        key: item.get(key)
        for key in keys
        if (include_metric or key != "metric_id") and item.get(key) is not None
    }


def _freshness_minutes(topic: str, claim: dict[str, Any]) -> int:
    if topic == "options_positioning" and str(
        claim.get("frequency") or ""
    ).lower() in {"intraday", "realtime"}:
        return 60
    if topic == "earnings_intelligence" and str(
        claim.get("metric_id") or ""
    ).lower() in {"event_at", "timing_status", "next_refresh_at"}:
        return 1440
    return DOMAIN_FRESHNESS_MINUTES[topic]


def _basis(value: Any) -> str:
    return " ".join(str(value or "").upper().replace("_", " ").split())


def _aware(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _iso(value: datetime) -> str:
    return _aware(value).replace(microsecond=0).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
