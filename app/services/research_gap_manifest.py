from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Callable

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.data_freshness_service import parse_datetime
from app.services.source_policy_service import SourcePolicyService
from app.services.temporal_validation_service import TemporalValidationService
from app.services.research_domain_contracts import (
    AGENTIC_DOMAIN_FIELDS,
    DOMAIN_TOPICS,
    field_states,
)


MNQ_TOPICS = (
    "macro_events",
    "fed_rates",
    "vix_risk",
    "cot_positioning",
    "nasdaq_100",
    "mega_cap_semiconductors",
    "earnings",
    "news",
    "geopolitical_regulatory_risk",
    "options_positioning",
    "market_internals",
    "cross_asset_context",
    "earnings_intelligence",
    "market_schedule",
)

TOPIC_PROFILES = {
    "macro_events": "MACRO_EVENTS_RESEARCH",
    "fed_rates": "FED_RATES_RESEARCH",
    "vix_risk": "VIX_RISK_RESEARCH",
    "cot_positioning": "COT_POSITIONING_RESEARCH",
    "nasdaq_100": "NASDAQ_100_RESEARCH",
    "mega_cap_semiconductors": "MEGA_CAP_SEMICONDUCTORS_RESEARCH",
    "earnings": "EARNINGS_RESEARCH",
    "news": "NEWS_RESEARCH",
    "geopolitical_regulatory_risk": "GEOPOLITICAL_REGULATORY_RISK_RESEARCH",
    "options_positioning": "OPTIONS_POSITIONING_RESEARCH",
    "market_internals": "MARKET_INTERNALS_RESEARCH",
    "cross_asset_context": "CROSS_ASSET_CONTEXT_RESEARCH",
    "earnings_intelligence": "EARNINGS_INTELLIGENCE_RESEARCH",
}


@dataclass(frozen=True)
class ResearchGapItem:
    topic: str
    applicability: str
    deterministic_status: str
    freshness: str
    data_as_of: str | None
    valid_until: str | None
    completeness: float
    missing_fields: tuple[str, ...]
    source_lineage: tuple[dict[str, Any], ...]
    required_action: str
    reason: str
    field_states: dict[str, str]


class ResearchGapManifestBuilder:
    """Builds and persists the server-owned decision made before any AI job exists."""

    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))
        self.policy = SourcePolicyService(settings.source_policy_path)
        migrate_database(settings.database_path)
        self.temporal_validation = TemporalValidationService(
            settings,
            clock=self.clock,
        )

    def build(
        self,
        *,
        snapshot: dict[str, Any] | None,
        components: dict[str, Any] | None = None,
        persist: bool = True,
    ) -> dict[str, Any]:
        now = _aware(self.clock()).replace(microsecond=0)
        context = self.temporal_validation.sanitize_payload(
            dict(components or ((snapshot or {}).get("debug_payload") or {})),
            entity_table="research_gap_manifest_input",
        )
        context = self.policy.sanitize_operational_payload(context) or {}
        items = [
            self._evaluate_topic(topic, context, now)
            for topic in MNQ_TOPICS
        ]
        body = {
            "symbol": "MNQ",
            "source_snapshot_id": (snapshot or {}).get("snapshot_id"),
            "generated_at": now.isoformat(),
            "policy_version": self.policy.policy_version,
            "items": [asdict(item) for item in items],
            "provider_stage": {
                "contract_version": "provider_gap_v1",
                "precedence": [
                    "api_provider",
                    "public_endpoint",
                    "agent_web_for_residual_gaps_only",
                ],
                "configured": bool(
                    ((context.get("research_provider_stage") or {}).get("configured"))
                ),
                "completed": bool(
                    ((context.get("research_provider_stage") or {}).get("completed"))
                ),
            },
        }
        checksum = hashlib.sha256(_json(body).encode("utf-8")).hexdigest()
        manifest = {
            "manifest_id": f"rgm-{uuid.uuid4()}",
            **body,
            "checksum": checksum,
            "agent_topics": [
                item.topic
                for item in items
                if item.required_action == "AGENT_RESEARCH"
            ],
            "deterministic_refresh_topics": [
                item.topic
                for item in items
                if item.required_action == "DETERMINISTIC_REFRESH"
            ],
        }
        if persist:
            self._persist(manifest)
        return manifest

    def _evaluate_topic(
        self,
        topic: str,
        context: dict[str, Any],
        now: datetime,
    ) -> ResearchGapItem:
        value = _topic_value(topic, context)
        lineage = tuple(_source_lineage(value))
        data_as_of, valid_until = _timestamps(value)
        freshness = _freshness(data_as_of, valid_until, now)
        completeness, missing = _completeness(topic, value)
        states = field_states(topic, value, now=now) if topic in DOMAIN_TOPICS else {}
        explicit_status = str(value.get("status") or "").upper() if isinstance(value, dict) else ""
        if topic == "market_schedule":
            return ResearchGapItem(
                topic,
                "APPLICABLE",
                "NEEDS_DETERMINISTIC_REFRESH",
                freshness,
                data_as_of,
                valid_until,
                completeness,
                tuple(missing),
                lineage,
                "DETERMINISTIC_REFRESH",
                "session_state_is_clock_dependent",
                states,
            )
        if topic in DOMAIN_TOPICS:
            residual = [
                field
                for field, state in states.items()
                if state not in {"SATISFIED", "NOT_APPLICABLE"}
            ]
            satisfied = [
                field for field, state in states.items() if state == "SATISFIED"
            ]
            if not residual:
                return ResearchGapItem(
                    topic,
                    "APPLICABLE",
                    "SATISFIED_FRESH_DB",
                    "FRESH",
                    data_as_of,
                    valid_until,
                    1.0,
                    (),
                    lineage,
                    "NONE",
                    "provider_or_database_fields_satisfied_before_agent_stage",
                    states,
                )
            state_names = {states[field] for field in residual}
            return ResearchGapItem(
                topic,
                "APPLICABLE",
                (
                    "NEEDS_AGENT_RESEARCH"
                    if satisfied
                    else "MISSING"
                ),
                (
                    "STALE"
                    if "STALE" in state_names
                    else "UNKNOWN"
                ),
                data_as_of,
                valid_until,
                len(satisfied) / max(len(states), 1),
                tuple(residual),
                lineage,
                "AGENT_RESEARCH",
                "agent_receives_only_residual_provider_neutral_field_gaps",
                states,
            )
        if explicit_status in {
            "NO_CURRENT_ITEM",
            "NO_EVENTS_SCHEDULED",
            "NO_RELEVANT_NEWS",
            "NO_RELEVANT_DATA",
        }:
            return ResearchGapItem(
                topic,
                "APPLICABLE",
                "NO_CURRENT_ITEM",
                freshness,
                data_as_of,
                valid_until,
                1.0,
                (),
                lineage,
                "NONE",
                "bounded_deterministic_query_proved_no_current_item",
                states,
            )
        if explicit_status == "NOT_CONFIGURED":
            return ResearchGapItem(
                topic,
                "APPLICABLE",
                "NOT_CONFIGURED",
                freshness,
                data_as_of,
                valid_until,
                completeness,
                tuple(missing),
                lineage,
                "AGENT_RESEARCH",
                "deterministic_source_not_configured",
                states,
            )
        if not _has_data(value):
            return ResearchGapItem(
                topic,
                "APPLICABLE",
                "MISSING",
                "UNKNOWN",
                data_as_of,
                valid_until,
                0.0,
                tuple(missing),
                lineage,
                "AGENT_RESEARCH",
                "no_committed_deterministic_data",
                states,
            )
        if completeness < 1.0:
            return ResearchGapItem(
                topic,
                "APPLICABLE",
                "NEEDS_AGENT_RESEARCH",
                freshness,
                data_as_of,
                valid_until,
                completeness,
                tuple(missing),
                lineage,
                "AGENT_RESEARCH",
                "required_fields_incomplete",
                states,
            )
        if freshness == "STALE":
            return ResearchGapItem(
                topic,
                "APPLICABLE",
                "SATISFIED_STALE_LKG",
                freshness,
                data_as_of,
                valid_until,
                completeness,
                (),
                lineage,
                "DETERMINISTIC_REFRESH",
                "committed_last_known_good_requires_provider_refresh",
                states,
            )
        return ResearchGapItem(
            topic,
            "APPLICABLE",
            "SATISFIED_FRESH_DB",
            freshness,
            data_as_of,
            valid_until,
            completeness,
            (),
            lineage,
            "NONE",
            "committed_deterministic_data_is_complete_and_fresh",
            states,
        )

    def _persist(self, manifest: dict[str, Any]) -> None:
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO research_gap_manifests(
                  manifest_id,parent_run_id,symbol,source_snapshot_id,generated_at,
                  policy_version,checksum,manifest_json,created_at
                ) VALUES (?,NULL,?,?,?,?,?,?,?)
                """,
                (
                    manifest["manifest_id"],
                    manifest["symbol"],
                    manifest["source_snapshot_id"],
                    manifest["generated_at"],
                    manifest["policy_version"],
                    manifest["checksum"],
                    _json(manifest),
                    manifest["generated_at"],
                ),
            )
            for item in manifest["items"]:
                conn.execute(
                    """
                    INSERT INTO research_gap_items(
                      manifest_id,topic,applicability,deterministic_status,freshness,
                      data_as_of,valid_until,completeness,missing_fields_json,
                      source_lineage_json,required_action,reason
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        manifest["manifest_id"],
                        item["topic"],
                        item["applicability"],
                        item["deterministic_status"],
                        item["freshness"],
                        item["data_as_of"],
                        item["valid_until"],
                        item["completeness"],
                        _json(item["missing_fields"]),
                        _json(item["source_lineage"]),
                        item["required_action"],
                        item["reason"],
                    ),
                )
            conn.commit()


def _topic_value(topic: str, context: dict[str, Any]) -> Any:
    mappings = {
        "macro_events": context.get("event_calendar") or context.get("macro_snapshot"),
        "fed_rates": context.get("rates_expectations"),
        "vix_risk": context.get("risk_context"),
        "cot_positioning": context.get("positioning"),
        "nasdaq_100": context.get("nasdaq_context"),
        "mega_cap_semiconductors": (
            (context.get("nasdaq_context") or {}).get("mega_cap_semiconductors")
            or context.get("mega_cap_semiconductors")
        ),
        "earnings": (
            (context.get("nasdaq_context") or {}).get("earnings")
            or context.get("corporate_events")
        ),
        "news": context.get("news_context") or context.get("news_digest"),
        "geopolitical_regulatory_risk": (
            context.get("geopolitical_regulatory_risk")
            or (context.get("risk_context") or {}).get("geopolitical_regulatory")
        ),
        "options_positioning": context.get("options_positioning"),
        "market_internals": context.get("market_internals"),
        "cross_asset_context": context.get("cross_asset_context"),
        "earnings_intelligence": context.get("earnings_intelligence"),
        "market_schedule": context.get("market_schedule"),
    }
    return mappings.get(topic)


def _completeness(topic: str, value: Any) -> tuple[float, list[str]]:
    if topic in DOMAIN_TOPICS:
        if not isinstance(value, dict):
            return 0.0, list(AGENTIC_DOMAIN_FIELDS[topic])
        fields = value.get("fields") if isinstance(value.get("fields"), dict) else value
        missing = [
            field
            for field in AGENTIC_DOMAIN_FIELDS[topic]
            if not _has_data(fields.get(field))
        ]
        return (
            len(AGENTIC_DOMAIN_FIELDS[topic]) - len(missing)
        ) / len(AGENTIC_DOMAIN_FIELDS[topic]), missing
    if not _has_data(value):
        return 0.0, ["current_data"]
    if topic == "macro_events" and isinstance(value, dict):
        active_events = [
            item
            for section in (
                "critical_macro_events",
                "fed_communications",
                "other_economic_events",
            )
            for item in (value.get(section) or [])
            if isinstance(item, dict)
        ]
        if not active_events:
            return 0.0, ["current_events"]
    required = {
        "vix_risk": ("vix", "vvix", "skew", "term_structure", "put_call"),
        "cot_positioning": ("report_date",),
        "nasdaq_100": ("constituents",),
        "earnings": ("upcoming",),
        "news": ("articles",),
    }.get(topic, ())
    if not required or not isinstance(value, dict):
        return 1.0, []
    aliases = {
        "term_structure": ("term_structure", "vix_term_structure"),
        "put_call": ("put_call", "put_call_ratio"),
        "articles": ("articles", "latest"),
        "constituents": ("constituents", "holdings"),
        "upcoming": ("upcoming", "events"),
    }
    missing = [
        field
        for field in required
        if not any(_has_data(value.get(alias)) for alias in aliases.get(field, (field,)))
    ]
    return (len(required) - len(missing)) / len(required), missing


def _has_data(value: Any) -> bool:
    if value in (None, "", [], {}):
        return False
    if isinstance(value, dict):
        status = str(value.get("status") or "").upper()
        if status in {"NOT_AVAILABLE", "PIPELINE_ERROR", "FAILED"}:
            return False
        return any(
            _has_data(item)
            for key, item in value.items()
            if key not in {"status", "errors", "warnings", "configured"}
        ) or status in {"AVAILABLE", "LAST_KNOWN_GOOD", "FOUND"}
    return True


def _timestamps(value: Any) -> tuple[str | None, str | None]:
    found: dict[str, str] = {}

    def walk(item: Any) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                if key in {"data_as_of", "retrieved_at", "updated_at", "as_of"} and child:
                    found.setdefault("data_as_of", str(child))
                if key in {"valid_until", "fresh_until"} and child:
                    found.setdefault("valid_until", str(child))
                if isinstance(child, (dict, list)):
                    walk(child)
        elif isinstance(item, list):
            for child in item[:100]:
                walk(child)

    walk(value)
    return found.get("data_as_of"), found.get("valid_until")


def _freshness(
    data_as_of: str | None,
    valid_until: str | None,
    now: datetime,
) -> str:
    expiry = parse_datetime(valid_until)
    if expiry is not None:
        return "FRESH" if _aware(expiry) > now else "STALE"
    observed = parse_datetime(data_as_of)
    if observed is None:
        return "UNKNOWN"
    return "FRESH" if _aware(observed).date() >= now.date() else "STALE"


def _source_lineage(value: Any) -> list[dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}

    def walk(item: Any) -> None:
        if isinstance(item, dict):
            url = item.get("canonical_url") or item.get("source_url")
            source = item.get("source") or item.get("publisher")
            if url:
                output[str(url)] = {
                    "source": source,
                    "source_url": str(url),
                    "retrieved_at": item.get("retrieved_at"),
                }
            for child in item.values():
                if isinstance(child, (dict, list)):
                    walk(child)
        elif isinstance(item, list):
            for child in item[:100]:
                walk(child)

    walk(value)
    return list(output.values())[:20]


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
