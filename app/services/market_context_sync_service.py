from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from app.core.config import Settings
from app.core.redaction import redact_payload
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.data_freshness_service import parse_datetime


CONTRACT = "ai_trader_market_context_sync"
SCHEMA_VERSION = "1.0"
SYMBOL = "MNQ"
SECTION_NAMES = (
    "macro",
    "macro_actuals",
    "event_calendar",
    "fed",
    "rates",
    "risk",
    "vix",
    "positioning",
    "nasdaq",
    "news",
    "market_schedule",
    "options_positioning",
    "market_internals",
    "cross_asset_context",
    "earnings",
    "earnings_intelligence",
    "geopolitical_regulatory_risk",
)
SECTION_SET = frozenset(SECTION_NAMES)
AVAILABLE_STATUSES = frozenset(
    {
        "AVAILABLE",
        "NO_DATA_EXPECTED",
        "NO_RELEVANT_DATA",
        "NO_RELEVANT_MARKETS",
    }
)
UNAVAILABLE_STATUSES = frozenset(
    {"NO_DATA", "UNAVAILABLE", "QUARANTINED", "BACKOFF", "DISABLED"}
)
DEGRADED_STATUSES = frozenset(
    {"PARTIAL", "LAST_KNOWN_GOOD", "STALE", "EXPIRED", "DUE"}
)
ACTIVE_WORK_STATUSES = frozenset({"PENDING", "RUNNING", "WAITING_BACKOFF"})
MAX_CONTROL_PAYLOAD_BYTES = 262_144
MAX_IDENTIFIER_LENGTH = 128
MAX_ACK_CLOCK_SKEW_SECONDS = 300
MAX_DELIVERY_ATTEMPTS = 8
MATERIAL_TRIGGER_REASONS = frozenset(
    {
        "MARKET_TRIGGER",
        "MACRO_ACTUAL",
        "MACRO_REVISION",
        "FOMC_DECISION",
        "FED_COMMUNICATION",
        "NEW_MATERIAL_NEWS",
        "GEOPOLITICAL_DEVELOPMENT",
        "REGULATORY_DEVELOPMENT",
        "EARNINGS_ACTUAL",
        "EARNINGS_REVISION",
        "DATA_INVALIDATED",
        "MARKET_SCHEDULE_CHANGE",
    }
)
ORDER_INSENSITIVE_ID_KEYS = (
    "record_id",
    "provider_record_id",
    "occurrence_id",
    "event_id",
    "news_key",
    "claim_id",
    "symbol",
)
VOLATILE_KEYS = frozenset(
    {
        "audit",
        "telemetry",
        "diagnostics",
        "generated_at",
        "generated_at_utc",
        "retrieved_at",
        "retrieved_at_utc",
        "observed_at",
        "checked_at",
        "checked_at_utc",
        "searched_at",
        "created_at",
        "updated_at",
        "duration_ms",
        "attempt_count",
        "provider_calls",
        "trace_id",
        "span_id",
        "parent_span_id",
        "heartbeat_at",
        "lease_owner",
        "lease_expires_at",
        "network_called",
        "cache_hit",
    }
)
TEMPORAL_KEYS = frozenset(
    {
        "acknowledged_at",
        "as_of",
        "checked_at",
        "created_at",
        "data_as_of",
        "event_at",
        "fresh_until",
        "generated_at",
        "generated_at_utc",
        "next_refresh_at",
        "observed_at",
        "published_at",
        "release_at",
        "retrieved_at",
        "scheduled_at",
        "updated_at",
        "valid_from",
        "valid_until",
    }
)
_WITHHELD = object()
_LOCAL_PATH_RE = re.compile(
    r"(?i)(?:\b[A-Z]:[\\/]|\\\\|file://|/(?:home|root|Users|tmp)/)"
    r"[^\s\"'<>]*"
)


def persist_sync_sections_in_transaction(
    conn: Any,
    *,
    symbol: str,
    snapshot_id: str,
    snapshot_revision: int,
    debug_payload: dict[str, Any],
    data_as_of: str | None,
    created_at: str,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Persist immutable section views and stable material revisions."""

    sections = extract_sync_sections(debug_payload)
    metadata: dict[str, dict[str, Any]] = {}
    changed: list[str] = []
    for section_name in SECTION_NAMES:
        payload = sections[section_name]
        fingerprint = material_fingerprint(payload)
        previous = conn.execute(
            """
            SELECT section_revision,fingerprint
            FROM market_context_sync_sections
            WHERE symbol=? AND section_name=? AND snapshot_revision<?
            ORDER BY snapshot_revision DESC LIMIT 1
            """,
            (symbol.upper(), section_name, int(snapshot_revision)),
        ).fetchone()
        if previous is not None and str(previous["fingerprint"]) == fingerprint:
            section_revision = int(previous["section_revision"])
        else:
            maximum = conn.execute(
                """
                SELECT COALESCE(MAX(section_revision),0)
                FROM market_context_sync_sections
                WHERE symbol=? AND section_name=?
                """,
                (symbol.upper(), section_name),
            ).fetchone()[0]
            section_revision = int(maximum or 0) + 1
            changed.append(section_name)
        status, reason = section_status(payload)
        valid_until = _find_temporal_value(payload, ("valid_until", "fresh_until"))
        section_data_as_of = _find_temporal_value(
            payload,
            ("data_as_of", "as_of", "published_at", "event_at"),
        ) or data_as_of
        freshness = section_freshness(
            status=status,
            valid_until=valid_until,
            payload=payload,
            reference=parse_datetime(created_at) or datetime.now(UTC),
        )
        record_count = record_count_for(payload)
        encoded_payload = canonical_json(payload)
        conn.execute(
            """
            INSERT INTO market_context_sync_sections(
              symbol,snapshot_id,snapshot_revision,section_name,section_revision,
              fingerprint,record_count,data_as_of,freshness,valid_until,status,
              reason,payload_json,created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(snapshot_id,section_name) DO NOTHING
            """,
            (
                symbol.upper(),
                snapshot_id,
                int(snapshot_revision),
                section_name,
                section_revision,
                fingerprint,
                record_count,
                section_data_as_of,
                freshness,
                valid_until,
                status,
                reason,
                encoded_payload,
                created_at,
            ),
        )
        persisted = conn.execute(
            """
            SELECT section_revision,fingerprint,record_count,data_as_of,
                   freshness,valid_until,status,reason,payload_json
            FROM market_context_sync_sections
            WHERE snapshot_id=? AND section_name=?
            """,
            (snapshot_id, section_name),
        ).fetchone()
        expected_immutable = (
            section_revision,
            fingerprint,
            record_count,
            section_data_as_of,
            freshness,
            valid_until,
            status,
            reason,
            encoded_payload,
        )
        actual_immutable = (
            int(persisted["section_revision"]),
            str(persisted["fingerprint"]),
            int(persisted["record_count"]),
            persisted["data_as_of"],
            str(persisted["freshness"]),
            persisted["valid_until"],
            str(persisted["status"]),
            persisted["reason"],
            str(persisted["payload_json"]),
        )
        if actual_immutable != expected_immutable:
            raise RuntimeError("sync_section_immutability_conflict")
        metadata[section_name] = {
            "section_revision": section_revision,
            "fingerprint": fingerprint,
            "record_count": record_count,
            "data_as_of": section_data_as_of,
            "freshness": freshness,
            "valid_until": valid_until,
            "status": status,
            "reason": reason,
        }
    return metadata, changed


def extract_sync_sections(full: dict[str, Any]) -> dict[str, Any]:
    """Build complete, non-compacted section payloads from one immutable snapshot."""

    event_calendar = {
        "window": full.get("event_calendar_window") or {},
        "calendar": full.get("event_calendar") or {},
        "economic_calendar_enrichment": (
            full.get("economic_calendar_enrichment") or {}
        ),
        "event_windows": full.get("event_windows") or {},
        "market_calendar": full.get("market_calendar") or {},
        "events_today": full.get("events_today") or [],
        "events_today_context": full.get("events_today_context") or {},
        "next_24h_events": full.get("next_24h_events") or [],
        "next_7d_critical_events": full.get("next_7d_critical_events") or [],
        "recently_released_events": full.get("recently_released_events") or [],
        "upcoming_high_impact_events": (
            full.get("upcoming_high_impact_events") or []
        ),
    }
    news_context = full.get("news_context") or {}
    nasdaq_context = full.get("nasdaq_context") or {}
    earnings = {
        "corporate_events": full.get("corporate_events") or {},
        "nasdaq_earnings": (
            nasdaq_context.get("earnings")
            if isinstance(nasdaq_context, dict)
            else {}
        )
        or {},
    }
    fed = {
        "fomc_context": full.get("fomc_context") or {},
        "fed_communications": full.get("fed_communications_today") or [],
        "calendar": (
            (full.get("event_calendar") or {}).get("fed_communications")
            if isinstance(full.get("event_calendar"), dict)
            else []
        )
        or [],
    }
    news = {
        "context": news_context,
        "latest": full.get("latest_news") or [],
        "digest": full.get("news_digest") or {},
        "current_company_news": full.get("current_company_news") or {},
    }
    risk = {
        "risk_context": full.get("risk_context") or {},
        "risk_sentiment": full.get("risk_sentiment") or {},
        "sentiment": full.get("sentiment") or {},
        "sentiment_context": full.get("sentiment_context") or {},
        "social_sentiment": full.get("social_sentiment") or {},
    }
    vix = _extract_vix(full)
    geopolitical = {
        "geopolitical_risk": full.get("geopolitical_risk") or {},
        "regulatory_risk": full.get("regulatory_risk") or {},
        "geopolitical_regulatory_risk": (
            full.get("geopolitical_regulatory_risk") or {}
        ),
        "research_domains": {
            key: value
            for key, value in full.items()
            if key in {"geopolitical_risk", "regulatory_risk"}
        },
    }
    raw_sections = {
        "macro": {
            "snapshot": full.get("macro_snapshot") or {},
            "provider_payload": full.get("macro") or {},
        },
        "macro_actuals": full.get("macro_actuals") or {},
        "event_calendar": event_calendar,
        "fed": fed,
        "rates": {
            "context": full.get("rates_context") or {},
            "expectations": full.get("rates_expectations") or {},
        },
        "risk": risk,
        "vix": vix,
        "positioning": full.get("positioning") or {},
        "nasdaq": nasdaq_context,
        "news": news,
        "market_schedule": full.get("market_schedule") or {},
        "options_positioning": full.get("options_positioning") or {},
        "market_internals": full.get("market_internals") or {},
        "cross_asset_context": full.get("cross_asset_context") or {},
        "earnings": earnings,
        "earnings_intelligence": full.get("earnings_intelligence") or {},
        "geopolitical_regulatory_risk": geopolitical,
    }
    return {
        name: withhold_quarantined_payload(payload)
        for name, payload in raw_sections.items()
    }


def withhold_quarantined_payload(value: Any) -> Any:
    """Remove rejected records while disclosing only aggregate quarantine state."""

    quarantined: dict[str, set[str]] = {
        "fingerprints": set(),
        "reasons": set(),
    }
    cleaned = _withhold_quarantined(
        _redact_contract_local_paths(redact_payload(value)),
        quarantined=quarantined,
    )
    if cleaned is _WITHHELD:
        cleaned = {}
    if not isinstance(cleaned, dict):
        cleaned = {"records": cleaned}
    if quarantined["fingerprints"]:
        cleaned = dict(cleaned)
        cleaned["producer_disclosures"] = {
            "quarantine": {
                "status": "WITHHELD",
                "record_count": len(quarantined["fingerprints"]),
                "reasons": sorted(quarantined["reasons"]),
            }
        }
    return cleaned


def _redact_contract_local_paths(value: Any) -> Any:
    if isinstance(value, str):
        return _LOCAL_PATH_RE.sub("<redacted-local-path>", value)
    if isinstance(value, list):
        return [_redact_contract_local_paths(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_contract_local_paths(item) for item in value)
    if isinstance(value, dict):
        return {
            key: _redact_contract_local_paths(item)
            for key, item in value.items()
        }
    return value


def _withhold_quarantined(
    value: Any,
    *,
    quarantined: dict[str, set[str]],
    parent_key: str | None = None,
) -> Any:
    if _quarantine_container(parent_key):
        quarantined["fingerprints"].add(material_fingerprint(value))
        quarantined["reasons"].update(_quarantine_reasons(value))
        return _WITHHELD
    if isinstance(value, dict):
        if _node_is_quarantined(value):
            quarantined["fingerprints"].add(material_fingerprint(value))
            quarantined["reasons"].update(_quarantine_reasons(value))
            return _WITHHELD
        output: dict[str, Any] = {}
        for key, item in value.items():
            cleaned = _withhold_quarantined(
                item,
                quarantined=quarantined,
                parent_key=str(key),
            )
            if cleaned is not _WITHHELD:
                output[str(key)] = cleaned
        return output
    if isinstance(value, list):
        output = []
        for item in value:
            cleaned = _withhold_quarantined(
                item,
                quarantined=quarantined,
                parent_key=parent_key,
            )
            if cleaned is not _WITHHELD:
                output.append(cleaned)
        return output
    return value


def _node_is_quarantined(value: dict[str, Any]) -> bool:
    validation = value.get("validation")
    states = {
        str(value.get("validation_status") or "").lower(),
        str(value.get("source_audit_status") or "").lower(),
        str(value.get("audit_status") or "").lower(),
        (
            str(validation.get("status") or "").lower()
            if isinstance(validation, dict)
            else ""
        ),
    }
    if str(value.get("status") or "").upper() == "QUARANTINED":
        states.add("quarantined")
    return bool(states.intersection({"rejected", "invalid", "quarantined"}))


def _quarantine_container(key: str | None) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(key or "").lower())
    return normalized.startswith("quarantined") and normalized not in {
        "quarantined_record_count",
    }


def _quarantine_reasons(value: Any) -> set[str]:
    if not isinstance(value, dict):
        return {"SOURCE_OR_VALIDATION_QUARANTINE"}
    reasons: set[str] = set()
    for key in ("reason", "reason_code", "rejection_reason"):
        if value.get(key):
            reasons.add(str(value[key])[:160])
    for key in ("reasons", "rejection_reasons", "warnings"):
        raw = value.get(key)
        if isinstance(raw, list):
            reasons.update(str(item)[:160] for item in raw if item)
    return reasons or {"SOURCE_OR_VALIDATION_QUARANTINE"}


class MarketContextSyncService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        migrate_database(settings.database_path)

    def manifest(
        self,
        *,
        symbol: str = SYMBOL,
        snapshot_revision: int | None = None,
    ) -> dict[str, Any]:
        validate_symbol(symbol)
        snapshot, rows = self._snapshot_and_sections(
            symbol=symbol,
            snapshot_revision=snapshot_revision,
        )
        sections = {
            str(row["section_name"]): self._section_metadata(row)
            for row in rows
        }
        return {
            "contract": CONTRACT,
            "schema_version": SCHEMA_VERSION,
            "symbol": str(snapshot["symbol"]),
            "snapshot_id": str(snapshot["snapshot_id"]),
            "snapshot_revision": int(snapshot["revision"]),
            "generated_at": snapshot["generated_at"],
            "data_as_of": snapshot["data_as_of"],
            "market_session": market_session_from_sections(rows),
            "sections": sections,
        }

    def full(
        self,
        *,
        symbol: str = SYMBOL,
        snapshot_revision: int | None = None,
    ) -> dict[str, Any]:
        validate_symbol(symbol)
        snapshot, rows = self._snapshot_and_sections(
            symbol=symbol,
            snapshot_revision=snapshot_revision,
        )
        manifest = self.manifest(
            symbol=symbol,
            snapshot_revision=int(snapshot["revision"]),
        )
        sections = {
            str(row["section_name"]): self._delivery_section(row, include_lineage=True)
            for row in rows
        }
        readiness = delivery_readiness(sections)
        response = {
            "contract": CONTRACT,
            "schema_version": SCHEMA_VERSION,
            "delivery_type": "FULL_SNAPSHOT",
            "symbol": str(snapshot["symbol"]),
            "snapshot_id": str(snapshot["snapshot_id"]),
            "snapshot_revision": int(snapshot["revision"]),
            "generated_at": snapshot["generated_at"],
            "data_as_of": snapshot["data_as_of"],
            "context_fingerprint": context_fingerprint(rows),
            "manifest": manifest,
            "readiness": readiness,
            "sections": sections,
            "checksum_scope": "CANONICAL_DELIVERY_WITHOUT_MEASUREMENT_FIELDS",
            "payload_size_bytes": 0,
        }
        return finalize_delivery(response)

    def sections(
        self,
        *,
        consumer_id: str,
        target_snapshot_revision: int,
        sections: Iterable[str],
        include_lineage: bool = False,
        symbol: str = SYMBOL,
    ) -> dict[str, Any]:
        validate_symbol(symbol)
        normalized = validate_sections(sections)
        validate_identifier(consumer_id, field="consumer_id")
        try:
            snapshot, rows = self._snapshot_and_sections(
                symbol=symbol,
                snapshot_revision=int(target_snapshot_revision),
            )
        except SyncContractError as exc:
            if exc.code == "snapshot_revision_not_available":
                return {
                    "delivery_type": "SECTION_SYNC",
                    "status": "RESYNC_REQUIRED",
                    "requires_full_resync": True,
                    "target_snapshot_revision": int(target_snapshot_revision),
                }
            raise
        by_name = {str(row["section_name"]): row for row in rows}
        delivered = {
            name: self._delivery_section(
                by_name[name],
                include_lineage=include_lineage,
            )
            for name in normalized
        }
        response = {
            "contract": CONTRACT,
            "schema_version": SCHEMA_VERSION,
            "delivery_type": "SECTION_SYNC",
            "symbol": str(snapshot["symbol"]),
            "snapshot_id": str(snapshot["snapshot_id"]),
            "snapshot_revision": int(snapshot["revision"]),
            "context_fingerprint": context_fingerprint(rows),
            "sections": delivered,
            "checksum_scope": "CANONICAL_DELIVERY_WITHOUT_MEASUREMENT_FIELDS",
            "payload_size_bytes": 0,
        }
        return finalize_delivery(response)

    def sections_request(
        self,
        payload: dict[str, Any],
        *,
        symbol: str = SYMBOL,
    ) -> dict[str, Any]:
        validate_control_payload(
            payload,
            allowed={
                "consumer_id",
                "target_snapshot_revision",
                "sections",
                "include_lineage",
            },
        )
        include_lineage = payload.get("include_lineage", False)
        if not isinstance(include_lineage, bool):
            raise SyncContractError("include_lineage_must_be_boolean", 422)
        return self.sections(
            consumer_id=validate_identifier(
                payload.get("consumer_id"),
                field="consumer_id",
            ),
            target_snapshot_revision=parse_positive_int(
                payload.get("target_snapshot_revision"),
                field="target_snapshot_revision",
            ),
            sections=(
                payload["sections"]
                if "sections" in payload
                else []
            ),
            include_lineage=include_lineage,
            symbol=symbol,
        )

    def plan(
        self,
        payload: dict[str, Any],
        *,
        symbol: str = SYMBOL,
    ) -> dict[str, Any]:
        validate_control_payload(
            payload,
            allowed={
                "consumer_id",
                "request_id",
                "analysis_profile",
                "required_sections",
                "known_snapshot_revision",
                "known_sections",
            },
        )
        validate_symbol(symbol)
        validate_identifier(
            payload.get("consumer_id"),
            field="consumer_id",
        )
        validate_identifier(
            payload.get("request_id"),
            field="request_id",
        )
        required = validate_sections(
            payload["required_sections"]
            if "required_sections" in payload
            else SECTION_NAMES
        )
        manifest = self.manifest(symbol=symbol)
        target_revision = int(manifest["snapshot_revision"])
        known_revision = payload.get("known_snapshot_revision")
        known_sections = payload.get("known_sections")
        if known_sections is not None and not isinstance(known_sections, dict):
            raise SyncContractError("known_sections_must_be_object", 422)
        known_sections = known_sections or {}
        if known_revision is not None:
            known_revision = parse_positive_int(
                known_revision,
                field="known_snapshot_revision",
            )
        for name, known in known_sections.items():
            if name not in SECTION_SET:
                raise SyncContractError(f"unknown_section:{name}", 422)
            if not isinstance(known, (dict, int)) or isinstance(known, bool):
                raise SyncContractError(
                    f"known_section_inventory_invalid:{name}",
                    422,
                )
        unavailable = [
            {
                "section": name,
                "classification": producer_availability_classification(
                    manifest["sections"][name]
                ),
                "status": manifest["sections"][name]["status"],
                "freshness": manifest["sections"][name]["freshness"],
                "reason": manifest["sections"][name].get("reason"),
            }
            for name in required
            if producer_availability_classification(
                manifest["sections"][name]
            )
            is not None
        ]
        if known_revision is None or not known_sections:
            return {
                "sync_mode": "FULL",
                "target_snapshot_revision": target_revision,
                "reason": "NO_CONSUMER_CONTEXT",
                "required_sections": required,
                "unavailable_at_producer": unavailable,
            }
        if known_revision > target_revision or not self._revision_exists(
            symbol,
            known_revision,
        ):
            return {
                "sync_mode": "FULL",
                "target_snapshot_revision": target_revision,
                "reason": "REVISION_GAP",
                "requires_full_resync": True,
                "unavailable_at_producer": unavailable,
            }
        fetch: list[dict[str, Any]] = []
        unchanged: list[str] = []
        for name in required:
            current = manifest["sections"][name]
            if producer_availability_classification(current) is not None:
                continue
            known = known_sections.get(name)
            if isinstance(known, int):
                known = {"section_revision": known}
            if not isinstance(known, dict):
                fetch.append(
                    {
                        "section": name,
                        "classification": "MISSING_AT_CONSUMER",
                        "reason": "SECTION_MISSING",
                        "current_revision": current["section_revision"],
                    }
                )
                continue
            try:
                known_section_revision = parse_positive_int(
                    known.get("section_revision"),
                    field=f"known_sections.{name}.section_revision",
                )
            except SyncContractError:
                fetch.append(
                    {
                        "section": name,
                        "classification": "MISSING_AT_CONSUMER",
                        "reason": "SECTION_INVENTORY_INVALID",
                        "current_revision": current["section_revision"],
                    }
                )
                continue
            if known_section_revision != int(current["section_revision"]):
                fetch.append(
                    {
                        "section": name,
                        "classification": "MISSING_AT_CONSUMER",
                        "reason": "REVISION_CHANGED",
                        "current_revision": current["section_revision"],
                    }
                )
                continue
            known_fingerprint = known.get("fingerprint")
            if not isinstance(known_fingerprint, str) or not re.fullmatch(
                r"[0-9a-fA-F]{64}",
                known_fingerprint,
            ):
                fetch.append(
                    {
                        "section": name,
                        "classification": "MISSING_AT_CONSUMER",
                        "reason": "FINGERPRINT_MISSING_OR_INVALID",
                        "current_revision": current["section_revision"],
                    }
                )
                continue
            if known_fingerprint.lower() != str(current["fingerprint"]).lower():
                fetch.append(
                    {
                        "section": name,
                        "classification": "MISSING_AT_CONSUMER",
                        "reason": "FINGERPRINT_INCOMPATIBLE",
                        "current_revision": current["section_revision"],
                    }
                )
                continue
            unchanged.append(name)
        if fetch:
            return {
                "sync_mode": "SELECTIVE",
                "target_snapshot_revision": target_revision,
                "sections_to_fetch": fetch,
                "unchanged_sections": unchanged,
                "unavailable_at_producer": unavailable,
            }
        return {
            "sync_mode": "NONE",
            "target_snapshot_revision": target_revision,
            "context_current": not unavailable,
            "unchanged_sections": unchanged,
            "unavailable_at_producer": unavailable,
        }

    def changes(
        self,
        *,
        since_revision: int,
        symbol: str = SYMBOL,
    ) -> dict[str, Any]:
        validate_symbol(symbol)
        since_revision = parse_positive_int(
            since_revision,
            field="since_revision",
        )
        target = self.manifest(symbol=symbol)
        target_revision = int(target["snapshot_revision"])
        if int(since_revision) == target_revision:
            return {
                "delivery_type": "DELTA_MANIFEST",
                "base_revision": target_revision,
                "target_revision": target_revision,
                "changed_sections": [],
                "requires_full_resync": False,
            }
        if int(since_revision) > target_revision or not self._revision_exists(
            symbol,
            int(since_revision),
        ):
            return {
                "delivery_type": "DELTA_MANIFEST",
                "base_revision": int(since_revision),
                "target_revision": target_revision,
                "changed_sections": [],
                "requires_full_resync": True,
                "reason": "REVISION_GAP",
            }
        base = self.manifest(
            symbol=symbol,
            snapshot_revision=int(since_revision),
        )
        changed: list[dict[str, Any]] = []
        for name in SECTION_NAMES:
            previous = base["sections"][name]
            current = target["sections"][name]
            if (
                previous["fingerprint"] == current["fingerprint"]
                and previous["section_revision"]
                == current["section_revision"]
            ):
                continue
            changed.append(
                {
                    "section": name,
                    "previous_revision": previous["section_revision"],
                    "current_revision": current["section_revision"],
                    "change_type": _change_type(previous, current),
                    "reason": _change_reason(previous, current),
                }
            )
        return {
            "delivery_type": "DELTA_MANIFEST",
            "base_revision": int(since_revision),
            "target_revision": target_revision,
            "changed_sections": changed,
            "requires_full_resync": False,
        }

    def request_refresh(
        self,
        payload: dict[str, Any],
        *,
        symbol: str = SYMBOL,
    ) -> tuple[int, dict[str, Any]]:
        validate_control_payload(
            payload,
            allowed={
                "consumer_id",
                "request_id",
                "reason",
                "required_sections",
                "known_snapshot_revision",
                "known_sections",
                "trigger",
            },
        )
        validate_symbol(symbol)
        consumer_id = validate_identifier(
            payload.get("consumer_id"),
            field="consumer_id",
        )
        request_id = validate_identifier(
            payload.get("request_id"),
            field="request_id",
        )
        reason = str(payload.get("reason") or "CONTEXT_REQUIRED").strip().upper()
        requested = validate_sections(
            payload["required_sections"]
            if "required_sections" in payload
            else SECTION_NAMES
        )
        known_revision = (
            parse_positive_int(
                payload["known_snapshot_revision"],
                field="known_snapshot_revision",
            )
            if payload.get("known_snapshot_revision") is not None
            else None
        )
        if payload.get("known_sections") is not None and not isinstance(
            payload.get("known_sections"),
            dict,
        ):
            raise SyncContractError("known_sections_must_be_object", 422)
        idempotency_fingerprint = control_request_fingerprint(
            {
                key: value
                for key, value in payload.items()
                if key != "request_id"
            }
        )
        manifest = self.manifest(symbol=symbol)
        if (
            (
                reason not in MATERIAL_TRIGGER_REASONS
                or (
                    known_revision is not None
                    and known_revision < int(manifest["snapshot_revision"])
                )
            )
            and all(
                producer_availability_classification(
                    manifest["sections"][name]
                )
                is None
                for name in requested
            )
        ):
            return 200, {
                "status": "READY",
                "snapshot_revision": manifest["snapshot_revision"],
                "sync_plan_url": f"/market-context/{symbol.lower()}/sync/plan",
            }
        if known_revision is not None and (
            known_revision > int(manifest["snapshot_revision"])
            or not self._revision_exists(symbol, known_revision)
        ):
            return 200, {
                "status": "RESYNC_REQUIRED",
                "snapshot_revision": manifest["snapshot_revision"],
                "requires_full_resync": True,
                "reason": "REVISION_GAP",
                "sync_plan_url": f"/market-context/{symbol.lower()}/sync/plan",
            }
        now = _iso(datetime.now(UTC))
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing_waiter = conn.execute(
                """
                SELECT w.*,wait.request_fingerprint AS waiter_request_fingerprint
                FROM market_context_sync_waiters wait
                JOIN market_context_sync_refresh_work w ON w.work_id=wait.work_id
                WHERE wait.consumer_id=? AND wait.request_id=?
                """,
                (consumer_id, request_id),
            ).fetchone()
            if existing_waiter is not None:
                if (
                    str(existing_waiter["waiter_request_fingerprint"])
                    != idempotency_fingerprint
                ):
                    conn.rollback()
                    raise SyncContractError(
                        "refresh_request_idempotency_conflict",
                        409,
                    )
                conn.commit()
                return (
                    200
                    if str(existing_waiter["status"]) == "COMPLETED"
                    else 202
                ), self._work_response(
                    existing_waiter,
                    attached=True,
                    new_job_created=False,
                )
            active = conn.execute(
                """
                SELECT * FROM market_context_sync_refresh_work
                WHERE symbol=? AND status IN ('PENDING','RUNNING','WAITING_BACKOFF')
                ORDER BY generation DESC
                """,
                (symbol.upper(),),
            ).fetchall()
            requested_set = set(requested)
            for row in active:
                current_sections = set(_loads(row["sections_json"], []))
                row_status = str(row["status"])
                row_reasons = set(_loads(row["trigger_reasons_json"], []))
                overlap = requested_set & current_sections
                if row_status == "WAITING_BACKOFF" and overlap:
                    union = sorted(requested_set | current_sections)
                    reasons = sorted(row_reasons | {reason})
                    conn.execute(
                        """
                        UPDATE market_context_sync_refresh_work
                        SET sections_json=?,residual_sections_json=?,
                            trigger_reasons_json=?,updated_at=?
                        WHERE work_id=?
                        """,
                        (
                            canonical_json(union),
                            canonical_json(union),
                            canonical_json(reasons),
                            now,
                            row["work_id"],
                        ),
                    )
                    self._attach_waiter(
                        conn,
                        row["work_id"],
                        consumer_id,
                        request_id,
                        requested,
                        now,
                        idempotency_fingerprint,
                    )
                    updated = conn.execute(
                        """
                        SELECT * FROM market_context_sync_refresh_work
                        WHERE work_id=?
                        """,
                        (row["work_id"],),
                    ).fetchone()
                    conn.commit()
                    return 202, self._work_response(
                        updated,
                        attached=True,
                        new_job_created=False,
                    )
                if row_status == "PENDING":
                    union = sorted(requested_set | current_sections)
                    reasons = sorted(row_reasons | {reason})
                    fingerprint = refresh_fingerprint(
                        symbol=symbol,
                        reason="|".join(reasons),
                        sections=union,
                    )
                    conn.execute(
                        """
                        UPDATE market_context_sync_refresh_work
                        SET sections_json=?,residual_sections_json=?,
                            request_fingerprint=?,trigger_reasons_json=?,
                            updated_at=?
                        WHERE work_id=?
                        """,
                        (
                            canonical_json(union),
                            canonical_json(
                                sorted(requested_set - current_sections)
                            ),
                            fingerprint,
                            canonical_json(reasons),
                            now,
                            row["work_id"],
                        ),
                    )
                    self._attach_waiter(
                        conn,
                        row["work_id"],
                        consumer_id,
                        request_id,
                        requested,
                        now,
                        idempotency_fingerprint,
                    )
                    updated = conn.execute(
                        """
                        SELECT * FROM market_context_sync_refresh_work
                        WHERE work_id=?
                        """,
                        (row["work_id"],),
                    ).fetchone()
                    conn.commit()
                    return 202, self._work_response(
                        updated,
                        attached=True,
                        new_job_created=False,
                    )
                if (
                    row_status == "RUNNING"
                    and requested_set <= current_sections
                    and reason in row_reasons
                ):
                    self._attach_waiter(
                        conn,
                        row["work_id"],
                        consumer_id,
                        request_id,
                        requested,
                        now,
                        idempotency_fingerprint,
                    )
                    conn.commit()
                    return 202, self._work_response(
                        row,
                        attached=True,
                        new_job_created=False,
                    )
            running = next(
                (row for row in active if str(row["status"]) == "RUNNING"),
                None,
            )
            residual = (
                sorted(requested_set - set(_loads(running["sections_json"], [])))
                if running is not None
                else requested
            )
            if not residual:
                residual = requested
            generation = int(
                conn.execute(
                    """
                    SELECT MAX(value) FROM (
                      SELECT COALESCE(MAX(revision),0) AS value
                      FROM market_context_snapshots WHERE symbol=?
                      UNION ALL
                      SELECT COALESCE(MAX(generation),0) AS value
                      FROM market_context_sync_refresh_work WHERE symbol=?
                    )
                    """,
                    (symbol.upper(), symbol.upper()),
                ).fetchone()[0]
                or 0
            ) + 1
            request_fingerprint = refresh_fingerprint(
                symbol=symbol,
                reason=reason,
                sections=residual,
            )
            work_id = f"mcw-{uuid.uuid4()}"
            conn.execute(
                """
                INSERT INTO market_context_sync_refresh_work(
                  work_id,symbol,generation,refresh_reason,request_fingerprint,
                  status,sections_json,completed_sections_json,
                  residual_sections_json,trigger_reasons_json,parent_work_id,
                  created_at,updated_at
                ) VALUES (?,?,?,?,?,'PENDING',?,'[]',?,?,?,?,?)
                """,
                (
                    work_id,
                    symbol.upper(),
                    generation,
                    reason,
                    request_fingerprint,
                    canonical_json(residual),
                    canonical_json(residual),
                    canonical_json([reason]),
                    str(running["work_id"]) if running is not None else None,
                    now,
                    now,
                ),
            )
            self._attach_waiter(
                conn,
                work_id,
                consumer_id,
                request_id,
                requested,
                now,
                idempotency_fingerprint,
            )
            row = conn.execute(
                "SELECT * FROM market_context_sync_refresh_work WHERE work_id=?",
                (work_id,),
            ).fetchone()
            conn.commit()
        return 202, self._work_response(
            row,
            attached=False,
            new_job_created=True,
        )

    def work_status(self, work_id: str) -> dict[str, Any]:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                "SELECT * FROM market_context_sync_refresh_work WHERE work_id=?",
                (work_id,),
            ).fetchone()
            if row is None:
                raise SyncContractError("refresh_work_not_found", 404)
            waiter_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM market_context_sync_waiters
                    WHERE work_id=?
                    """,
                    (work_id,),
                ).fetchone()[0]
            )
        output = self._work_row(row)
        output["waiter_count"] = waiter_count
        return output

    def claim_next_refresh_work(
        self,
        *,
        owner: str,
        lease_seconds: int = 120,
        symbol: str = SYMBOL,
    ) -> dict[str, Any] | None:
        owner = str(owner or "").strip()
        if not owner:
            raise SyncContractError("lease_owner_required", 422)
        now = datetime.now(UTC)
        timestamp = _iso(now)
        lease_until = _iso(now + timedelta(seconds=max(lease_seconds, 1)))
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE market_context_sync_refresh_work
                SET status='PENDING',lease_owner=NULL,lease_expires_at=NULL,
                    updated_at=?
                WHERE symbol=? AND status='RUNNING'
                  AND lease_expires_at IS NOT NULL AND lease_expires_at<=?
                """,
                (timestamp, symbol.upper(), timestamp),
            )
            conn.execute(
                """
                UPDATE market_context_sync_refresh_work
                SET status='PENDING',next_retry_at=NULL,updated_at=?
                WHERE symbol=? AND status='WAITING_BACKOFF'
                  AND next_retry_at IS NOT NULL AND next_retry_at<=?
                """,
                (timestamp, symbol.upper(), timestamp),
            )
            row = conn.execute(
                """
                SELECT * FROM market_context_sync_refresh_work
                WHERE symbol=? AND status='PENDING'
                ORDER BY generation,created_at LIMIT 1
                """,
                (symbol.upper(),),
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            conn.execute(
                """
                UPDATE market_context_sync_refresh_work
                SET status='RUNNING',lease_owner=?,lease_expires_at=?,
                    updated_at=?
                WHERE work_id=? AND status='PENDING'
                """,
                (owner, lease_until, timestamp, row["work_id"]),
            )
            claimed = conn.execute(
                "SELECT * FROM market_context_sync_refresh_work WHERE work_id=?",
                (row["work_id"],),
            ).fetchone()
            conn.commit()
        return self._work_row(claimed)

    def heartbeat_refresh_work(
        self,
        work_id: str,
        *,
        owner: str,
        lease_seconds: int = 120,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        timestamp = _iso(now)
        lease_until = _iso(now + timedelta(seconds=max(lease_seconds, 1)))
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """
                UPDATE market_context_sync_refresh_work
                SET lease_expires_at=?,updated_at=?
                WHERE work_id=? AND status='RUNNING' AND lease_owner=?
                """,
                (lease_until, timestamp, work_id, owner),
            )
            if int(cursor.rowcount or 0) != 1:
                conn.rollback()
                raise SyncContractError("refresh_work_lease_mismatch", 409)
            row = conn.execute(
                "SELECT * FROM market_context_sync_refresh_work WHERE work_id=?",
                (work_id,),
            ).fetchone()
            conn.commit()
        return self._work_row(row)

    def complete_refresh_work(
        self,
        work_id: str,
        *,
        owner: str,
        snapshot_id: str,
    ) -> dict[str, Any]:
        timestamp = _iso(datetime.now(UTC))
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            work = conn.execute(
                "SELECT * FROM market_context_sync_refresh_work WHERE work_id=?",
                (work_id,),
            ).fetchone()
            if work is None:
                conn.rollback()
                raise SyncContractError("refresh_work_not_found", 404)
            if (
                str(work["status"]) != "RUNNING"
                or str(work["lease_owner"] or "") != str(owner)
            ):
                conn.rollback()
                raise SyncContractError("refresh_work_lease_mismatch", 409)
            snapshot = conn.execute(
                """
                SELECT * FROM market_context_snapshots
                WHERE snapshot_id=? AND symbol=? AND audit_status='ACTIVE'
                  AND source_audit_status='ACTIVE'
                """,
                (snapshot_id, str(work["symbol"])),
            ).fetchone()
            if snapshot is None:
                conn.rollback()
                raise SyncContractError("refresh_result_snapshot_not_found", 409)
            if int(snapshot["revision"]) < int(work["generation"]):
                conn.rollback()
                raise SyncContractError(
                    "refresh_result_generation_mismatch",
                    409,
                )
            self._ensure_sections_through(
                conn,
                symbol=str(work["symbol"]),
                target_revision=int(snapshot["revision"]),
            )
            section_rows = conn.execute(
                """
                SELECT section_name FROM market_context_sync_sections
                WHERE snapshot_id=?
                """,
                (snapshot_id,),
            ).fetchall()
            available = {str(row["section_name"]) for row in section_rows}
            requested = set(_loads(work["sections_json"], []))
            if not requested <= available:
                conn.rollback()
                raise SyncContractError(
                    "refresh_result_sections_incomplete",
                    409,
                )
            conn.execute(
                """
                UPDATE market_context_sync_refresh_work
                SET status='COMPLETED',completed_sections_json=sections_json,
                    residual_sections_json='[]',target_snapshot_revision=?,
                    result_snapshot_id=?,lease_owner=NULL,
                    lease_expires_at=NULL,updated_at=?,completed_at=?
                WHERE work_id=?
                """,
                (
                    int(snapshot["revision"]),
                    snapshot_id,
                    timestamp,
                    timestamp,
                    work_id,
                ),
            )
            row = conn.execute(
                "SELECT * FROM market_context_sync_refresh_work WHERE work_id=?",
                (work_id,),
            ).fetchone()
            conn.commit()
        return self._work_row(row)

    def backoff_refresh_work(
        self,
        work_id: str,
        *,
        owner: str,
        next_retry_at: str,
        error: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if parse_datetime(next_retry_at) is None:
            raise SyncContractError("next_retry_at_invalid", 422)
        timestamp = _iso(datetime.now(UTC))
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """
                UPDATE market_context_sync_refresh_work
                SET status='WAITING_BACKOFF',next_retry_at=?,error_json=?,
                    lease_owner=NULL,lease_expires_at=NULL,updated_at=?
                WHERE work_id=? AND status='RUNNING' AND lease_owner=?
                """,
                (
                    next_retry_at,
                    canonical_json(error or {}),
                    timestamp,
                    work_id,
                    owner,
                ),
            )
            if int(cursor.rowcount or 0) != 1:
                conn.rollback()
                raise SyncContractError("refresh_work_lease_mismatch", 409)
            row = conn.execute(
                "SELECT * FROM market_context_sync_refresh_work WHERE work_id=?",
                (work_id,),
            ).fetchone()
            conn.commit()
        return self._work_row(row)

    def acknowledge(
        self,
        payload: dict[str, Any],
        *,
        symbol: str = SYMBOL,
    ) -> dict[str, Any]:
        validate_control_payload(
            payload,
            allowed={
                "consumer_id",
                "delivery_id",
                "snapshot_revision",
                "status",
                "section_revisions",
                "acknowledged_at",
            },
        )
        validate_symbol(symbol)
        consumer_id = validate_identifier(
            payload.get("consumer_id"),
            field="consumer_id",
        )
        delivery_id = validate_identifier(
            payload.get("delivery_id"),
            field="delivery_id",
        )
        status = str(payload.get("status") or "").strip().upper()
        acknowledged_at = str(payload.get("acknowledged_at") or "").strip()
        section_revisions = payload.get("section_revisions")
        if status != "PERSISTED":
            raise SyncContractError("ack_status_must_be_persisted", 422)
        acknowledged_datetime = parse_datetime(acknowledged_at)
        if acknowledged_datetime is None:
            raise SyncContractError("acknowledged_at_invalid", 422)
        if not isinstance(section_revisions, dict) or not section_revisions:
            raise SyncContractError("section_revisions_required", 422)
        revision = parse_positive_int(
            payload.get("snapshot_revision"),
            field="snapshot_revision",
        )
        normalized_revisions: dict[str, int] = {}
        for name, value in section_revisions.items():
            if name not in SECTION_SET:
                raise SyncContractError("ack_unknown_section", 422)
            normalized_revisions[name] = parse_positive_int(
                value,
                field=f"section_revisions.{name}",
            )
        received_datetime = datetime.now(UTC)
        received_at = _iso(received_datetime)
        fingerprint = hashlib.sha256(
            canonical_json(
                {
                    "consumer_id": consumer_id,
                    "delivery_id": delivery_id,
                    "snapshot_revision": revision,
                    "status": status,
                    "section_revisions": normalized_revisions,
                    "acknowledged_at": acknowledged_at,
                }
            ).encode("utf-8")
        ).hexdigest()
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            delivery = conn.execute(
                "SELECT * FROM market_context_outbox WHERE event_id=?",
                (delivery_id,),
            ).fetchone()
            if delivery is None:
                conn.rollback()
                raise SyncContractError("delivery_not_found", 404)
            if int(delivery["snapshot_revision"]) != revision:
                conn.rollback()
                raise SyncContractError("ack_snapshot_revision_mismatch", 409)
            created_datetime = parse_datetime(str(delivery["created_at"]))
            if (
                created_datetime is None
                or _aware(acknowledged_datetime) < _aware(created_datetime)
            ):
                conn.rollback()
                raise SyncContractError("ack_precedes_delivery", 409)
            if _aware(acknowledged_datetime) > (
                received_datetime
                + timedelta(seconds=MAX_ACK_CLOCK_SKEW_SECONDS)
            ):
                conn.rollback()
                raise SyncContractError("ack_timestamp_in_future", 422)
            target = conn.execute(
                """
                SELECT * FROM market_context_delivery_targets
                WHERE delivery_id=? AND consumer_id=?
                """,
                (delivery_id, consumer_id),
            ).fetchone()
            if target is None:
                conn.rollback()
                raise SyncContractError("ack_consumer_not_notified", 409)
            if str(target["status"]) not in {
                "NOTIFIED",
                "DEAD_LETTER",
                "ACKNOWLEDGED",
            }:
                conn.rollback()
                raise SyncContractError("ack_precedes_delivery", 409)
            changed_sections = {
                str(name)
                for name in _loads(delivery["changed_sections_json"], [])
            }
            if set(normalized_revisions) != changed_sections:
                conn.rollback()
                raise SyncContractError("ack_partial_persistence", 409)
            expected_rows = conn.execute(
                """
                SELECT section_name,section_revision
                FROM market_context_sync_sections
                WHERE symbol=? AND snapshot_revision=?
                """,
                (symbol.upper(), revision),
            ).fetchall()
            expected = {
                str(row["section_name"]): int(row["section_revision"])
                for row in expected_rows
            }
            for name, value in normalized_revisions.items():
                if expected.get(name) != value:
                    conn.rollback()
                    raise SyncContractError("ack_section_revision_mismatch", 409)
            existing = conn.execute(
                """
                SELECT * FROM market_context_delivery_acks
                WHERE consumer_id=? AND delivery_id=?
                """,
                (consumer_id, delivery_id),
            ).fetchone()
            if existing is not None:
                if str(existing["payload_fingerprint"]) != fingerprint:
                    conn.rollback()
                    raise SyncContractError("ack_idempotency_conflict", 409)
                conn.commit()
                return {
                    "status": "ACKNOWLEDGED",
                    "idempotent_replay": True,
                    "delivery_id": delivery_id,
                    "snapshot_revision": revision,
                    "superseded_delivery": bool(
                        target["superseded_by_delivery_id"]
                    ),
                }
            conn.execute(
                """
                INSERT INTO market_context_delivery_acks(
                  consumer_id,delivery_id,snapshot_revision,status,
                  section_revisions_json,acknowledged_at,received_at,
                  payload_fingerprint
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    consumer_id,
                    delivery_id,
                    revision,
                    status,
                    canonical_json(normalized_revisions),
                    acknowledged_at,
                    received_at,
                    fingerprint,
                ),
            )
            conn.execute(
                """
                UPDATE market_context_delivery_targets
                SET status='ACKNOWLEDGED',acknowledged_at=?,
                    updated_at=?
                WHERE delivery_id=? AND consumer_id=?
                """,
                (received_at, received_at, delivery_id, consumer_id),
            )
            pending = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM market_context_delivery_targets
                    WHERE consumer_id=? AND status IN ('PENDING','NOTIFIED')
                      AND superseded_by_delivery_id IS NULL
                    """,
                    (consumer_id,),
                ).fetchone()[0]
            )
            current_state = conn.execute(
                """
                SELECT last_snapshot_revision_acknowledged,
                       section_revisions_json
                FROM market_context_consumer_state
                WHERE consumer_id=?
                """,
                (consumer_id,),
            ).fetchone()
            superseded = bool(
                current_state is not None
                and current_state["last_snapshot_revision_acknowledged"]
                is not None
                and int(current_state["last_snapshot_revision_acknowledged"])
                > revision
            )
            prior_inventory = (
                _loads(current_state["section_revisions_json"], {})
                if current_state is not None
                else {}
            )
            merged_inventory = {
                str(name): int(value)
                for name, value in prior_inventory.items()
                if name in SECTION_SET
                and isinstance(value, int)
                and not isinstance(value, bool)
                and value > 0
            }
            for name, value in normalized_revisions.items():
                merged_inventory[name] = max(
                    int(merged_inventory.get(name, 0)),
                    value,
                )
            conn.execute(
                """
                INSERT INTO market_context_consumer_state(
                  consumer_id,last_delivery_created,last_delivery_notified,
                  last_delivery_acknowledged,last_snapshot_revision_acknowledged,
                  section_revisions_json,pending_delivery_count,retry_count,
                  gap_detected,resync_required,updated_at
                ) VALUES (?,?,?,?,?,?,?,0,0,0,?)
                ON CONFLICT(consumer_id) DO UPDATE SET
                  last_delivery_acknowledged=CASE
                    WHEN market_context_consumer_state.
                      last_snapshot_revision_acknowledged IS NULL
                      OR excluded.last_snapshot_revision_acknowledged>=
                        market_context_consumer_state.
                          last_snapshot_revision_acknowledged
                    THEN excluded.last_delivery_acknowledged
                    ELSE market_context_consumer_state.
                      last_delivery_acknowledged
                  END,
                  last_snapshot_revision_acknowledged=MAX(
                    COALESCE(
                      market_context_consumer_state.
                        last_snapshot_revision_acknowledged,
                      0
                    ),
                    excluded.last_snapshot_revision_acknowledged
                  ),
                  section_revisions_json=CASE
                    WHEN market_context_consumer_state.
                      last_snapshot_revision_acknowledged IS NULL
                      OR excluded.last_snapshot_revision_acknowledged>=
                        market_context_consumer_state.
                          last_snapshot_revision_acknowledged
                    THEN excluded.section_revisions_json
                    ELSE market_context_consumer_state.section_revisions_json
                  END,
                  pending_delivery_count=excluded.pending_delivery_count,
                  gap_detected=0,resync_required=0,updated_at=excluded.updated_at
                """,
                (
                    consumer_id,
                    delivery_id,
                    delivery_id,
                    delivery_id,
                    revision,
                    canonical_json(merged_inventory),
                    pending,
                    received_at,
                ),
            )
            conn.commit()
        return {
            "status": "ACKNOWLEDGED",
            "idempotent_replay": False,
            "delivery_id": delivery_id,
            "snapshot_revision": revision,
            "superseded_delivery": superseded,
        }

    def consumer_state(self, consumer_id: str) -> dict[str, Any]:
        consumer_id = validate_identifier(consumer_id, field="consumer_id")
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                "SELECT * FROM market_context_consumer_state WHERE consumer_id=?",
                (consumer_id,),
            ).fetchone()
            pending_rows = conn.execute(
                """
                SELECT target.delivery_id AS event_id,
                       outbox.snapshot_revision,target.attempt_count,
                       target.next_retry_at AS next_attempt_at,
                       target.status,target.superseded_by_delivery_id,
                       target.created_at
                FROM market_context_delivery_targets target
                JOIN market_context_outbox outbox
                  ON outbox.event_id=target.delivery_id
                WHERE target.consumer_id=?
                  AND target.status IN ('PENDING','NOTIFIED')
                  AND target.superseded_by_delivery_id IS NULL
                ORDER BY target.created_at,target.delivery_id
                """,
                (consumer_id,),
            ).fetchall()
        state = dict(row) if row is not None else {
            "consumer_id": consumer_id,
            "last_delivery_created": None,
            "last_delivery_notified": None,
            "last_delivery_acknowledged": None,
            "last_snapshot_revision_acknowledged": None,
            "section_revisions_json": "{}",
            "pending_delivery_count": len(pending_rows),
            "retry_count": 0,
            "last_error_json": None,
            "gap_detected": 0,
            "resync_required": 0,
            "updated_at": None,
        }
        state["section_revisions"] = _loads(
            state.pop("section_revisions_json", "{}"),
            {},
        )
        state["last_error"] = _loads(state.pop("last_error_json", None), None)
        state["gap_detected"] = bool(state["gap_detected"])
        state["resync_required"] = bool(state["resync_required"])
        state["pending_deliveries"] = [dict(item) for item in pending_rows]
        state["pending_delivery_count"] = len(pending_rows)
        return state

    def notification(self, delivery_id: str) -> dict[str, Any]:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                "SELECT * FROM market_context_outbox WHERE event_id=?",
                (delivery_id,),
            ).fetchone()
        if row is None:
            raise SyncContractError("delivery_not_found", 404)
        return notification_envelope(row)

    def record_delivery_attempt(
        self,
        *,
        delivery_id: str,
        consumer_id: str,
        status: str,
        next_retry_at: str | None = None,
        error: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        consumer_id = validate_identifier(consumer_id, field="consumer_id")
        delivery_id = validate_identifier(delivery_id, field="delivery_id")
        normalized_status = str(status or "").strip().upper()
        if normalized_status not in {"NOTIFIED", "FAILED"}:
            raise SyncContractError("delivery_attempt_status_invalid", 422)
        if next_retry_at and parse_datetime(next_retry_at) is None:
            raise SyncContractError("next_retry_at_invalid", 422)
        attempted_at = _iso(datetime.now(UTC))
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            delivery = conn.execute(
                "SELECT * FROM market_context_outbox WHERE event_id=?",
                (delivery_id,),
            ).fetchone()
            if delivery is None:
                conn.rollback()
                raise SyncContractError("delivery_not_found", 404)
            existing_target = conn.execute(
                """
                SELECT * FROM market_context_delivery_targets
                WHERE delivery_id=? AND consumer_id=?
                """,
                (delivery_id, consumer_id),
            ).fetchone()
            if (
                existing_target is not None
                and str(existing_target["status"]) == "ACKNOWLEDGED"
            ):
                conn.rollback()
                raise SyncContractError(
                    "delivery_already_acknowledged",
                    409,
                )
            attempt_number = (
                int(existing_target["attempt_count"]) + 1
                if existing_target is not None
                else 1
            )
            if normalized_status == "FAILED" and next_retry_at is None:
                next_retry_at = _iso(
                    datetime.now(UTC)
                    + timedelta(
                        seconds=min(
                            30 * (2 ** min(attempt_number - 1, 5)),
                            900,
                        )
                    )
                )
            target_status = (
                "NOTIFIED"
                if normalized_status == "NOTIFIED"
                else (
                    "DEAD_LETTER"
                    if attempt_number >= MAX_DELIVERY_ATTEMPTS
                    else "PENDING"
                )
            )
            attempt_id = (
                "delivery-attempt-"
                + hashlib.sha256(
                    f"{delivery_id}|{consumer_id}|{attempt_number}".encode(
                        "utf-8"
                    )
                ).hexdigest()[:32]
            )
            conn.execute(
                """
                INSERT INTO market_context_delivery_attempts(
                  attempt_id,delivery_id,consumer_id,attempt_number,status,
                  attempted_at,next_retry_at,error_json
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    attempt_id,
                    delivery_id,
                    consumer_id,
                    attempt_number,
                    normalized_status,
                    attempted_at,
                    next_retry_at,
                    canonical_json(redact_payload(error or {})),
                ),
            )
            conn.execute(
                """
                INSERT INTO market_context_delivery_targets(
                  delivery_id,consumer_id,status,attempt_count,next_retry_at,
                  last_error_json,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(delivery_id,consumer_id) DO UPDATE SET
                  status=excluded.status,
                  attempt_count=excluded.attempt_count,
                  next_retry_at=excluded.next_retry_at,
                  last_error_json=excluded.last_error_json,
                  updated_at=excluded.updated_at
                """,
                (
                    delivery_id,
                    consumer_id,
                    target_status,
                    attempt_number,
                    next_retry_at if target_status == "PENDING" else None,
                    canonical_json(redact_payload(error or {})),
                    attempted_at,
                    attempted_at,
                ),
            )
            conn.execute(
                """
                UPDATE market_context_delivery_targets
                SET superseded_by_delivery_id=?,updated_at=?
                WHERE consumer_id=? AND delivery_id<>?
                  AND status IN ('PENDING','NOTIFIED')
                  AND delivery_id IN (
                    SELECT older.event_id FROM market_context_outbox older
                    WHERE older.snapshot_revision<?
                  )
                """,
                (
                    delivery_id,
                    attempted_at,
                    consumer_id,
                    delivery_id,
                    int(delivery["snapshot_revision"]),
                ),
            )
            conn.execute(
                """
                UPDATE market_context_outbox
                SET attempt_count=attempt_count+1,next_attempt_at=?
                WHERE event_id=?
                """,
                (
                    next_retry_at if target_status == "PENDING" else None,
                    delivery_id,
                ),
            )
            pending_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM market_context_delivery_targets
                    WHERE consumer_id=?
                      AND status IN ('PENDING','NOTIFIED')
                      AND superseded_by_delivery_id IS NULL
                    """,
                    (consumer_id,),
                ).fetchone()[0]
            )
            conn.execute(
                """
                INSERT INTO market_context_consumer_state(
                  consumer_id,last_delivery_created,last_delivery_notified,
                  section_revisions_json,pending_delivery_count,retry_count,
                  last_error_json,gap_detected,resync_required,updated_at
                ) VALUES (?,?,?,'{}',?,?,?,0,0,?)
                ON CONFLICT(consumer_id) DO UPDATE SET
                  last_delivery_created=excluded.last_delivery_created,
                  last_delivery_notified=CASE
                    WHEN ?='NOTIFIED' THEN excluded.last_delivery_notified
                    ELSE market_context_consumer_state.last_delivery_notified
                  END,
                  pending_delivery_count=excluded.pending_delivery_count,
                  retry_count=market_context_consumer_state.retry_count+?,
                  last_error_json=excluded.last_error_json,
                  updated_at=excluded.updated_at
                """,
                (
                    consumer_id,
                    delivery_id,
                    delivery_id if normalized_status == "NOTIFIED" else None,
                    pending_count,
                    int(normalized_status == "FAILED"),
                    canonical_json(redact_payload(error or {})),
                    attempted_at,
                    normalized_status,
                    int(normalized_status == "FAILED"),
                ),
            )
            conn.commit()
        return {
            "attempt_id": attempt_id,
            "delivery_id": delivery_id,
            "consumer_id": consumer_id,
            "attempt_number": attempt_number,
            "status": target_status,
            "attempt_status": normalized_status,
            "next_retry_at": (
                next_retry_at if target_status == "PENDING" else None
            ),
        }

    def _snapshot_and_sections(
        self,
        *,
        symbol: str,
        snapshot_revision: int | None,
    ) -> tuple[Any, list[Any]]:
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            if snapshot_revision is None:
                snapshot = conn.execute(
                    """
                    SELECT * FROM market_context_snapshots
                    WHERE symbol=? AND audit_status='ACTIVE'
                      AND source_audit_status='ACTIVE'
                    ORDER BY revision DESC LIMIT 1
                    """,
                    (symbol.upper(),),
                ).fetchone()
            else:
                snapshot = conn.execute(
                    """
                    SELECT * FROM market_context_snapshots
                    WHERE symbol=? AND revision=? AND audit_status='ACTIVE'
                      AND source_audit_status='ACTIVE'
                    """,
                    (symbol.upper(), int(snapshot_revision)),
                ).fetchone()
            if snapshot is None:
                raise SyncContractError("snapshot_revision_not_available", 404)
            self._ensure_sections_through(
                conn,
                symbol=symbol,
                target_revision=int(snapshot["revision"]),
            )
            rows = conn.execute(
                """
                SELECT * FROM market_context_sync_sections
                WHERE symbol=? AND snapshot_revision=?
                ORDER BY section_name
                """,
                (symbol.upper(), int(snapshot["revision"])),
            ).fetchall()
            conn.commit()
        if len(rows) != len(SECTION_NAMES):
            raise SyncContractError("snapshot_sections_incomplete", 500)
        return snapshot, rows

    def _ensure_sections_through(
        self,
        conn: Any,
        *,
        symbol: str,
        target_revision: int,
    ) -> None:
        snapshots = conn.execute(
            """
            SELECT * FROM market_context_snapshots s
            WHERE symbol=? AND revision<=? AND audit_status='ACTIVE'
              AND source_audit_status='ACTIVE'
              AND (
                SELECT COUNT(*) FROM market_context_sync_sections section
                WHERE section.snapshot_id=s.snapshot_id
              )<?
            ORDER BY revision
            """,
            (symbol.upper(), int(target_revision), len(SECTION_NAMES)),
        ).fetchall()
        for snapshot in snapshots:
            debug = _loads(snapshot["debug_payload_json"], {})
            persist_sync_sections_in_transaction(
                conn,
                symbol=symbol,
                snapshot_id=str(snapshot["snapshot_id"]),
                snapshot_revision=int(snapshot["revision"]),
                debug_payload=debug,
                data_as_of=snapshot["data_as_of"],
                created_at=str(snapshot["created_at"]),
            )

    def _revision_exists(self, symbol: str, revision: int) -> bool:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                """
                SELECT 1 FROM market_context_snapshots
                WHERE symbol=? AND revision=? AND audit_status='ACTIVE'
                  AND source_audit_status='ACTIVE'
                """,
                (symbol.upper(), int(revision)),
            ).fetchone()
        return row is not None

    @staticmethod
    def _section_metadata(row: Any) -> dict[str, Any]:
        return {
            "section_revision": int(row["section_revision"]),
            "fingerprint": str(row["fingerprint"]),
            "record_count": int(row["record_count"]),
            "data_as_of": row["data_as_of"],
            "valid_until": row["valid_until"],
            "freshness": str(row["freshness"]),
            "status": str(row["status"]),
            "reason": row["reason"],
        }

    @classmethod
    def _delivery_section(
        cls,
        row: Any,
        *,
        include_lineage: bool,
    ) -> dict[str, Any]:
        payload = _loads(row["payload_json"], {})
        if isinstance(payload, dict):
            delivered = dict(payload)
        else:
            delivered = {"records": payload}
        delivered["sync"] = cls._section_metadata(row)
        if include_lineage:
            delivered["sync"]["lineage"] = {
                "snapshot_id": str(row["snapshot_id"]),
                "snapshot_revision": int(row["snapshot_revision"]),
                "section": str(row["section_name"]),
            }
        return delivered

    @staticmethod
    def _attach_waiter(
        conn: Any,
        work_id: str,
        consumer_id: str,
        request_id: str,
        requested: list[str],
        attached_at: str,
        request_fingerprint: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO market_context_sync_waiters(
              work_id,consumer_id,request_id,requested_sections_json,
              request_fingerprint,attached_at
            ) VALUES (?,?,?,?,?,?)
            """,
            (
                work_id,
                consumer_id,
                request_id,
                canonical_json(requested),
                request_fingerprint,
                attached_at,
            ),
        )

    @classmethod
    def _work_response(
        cls,
        row: Any,
        *,
        attached: bool,
        new_job_created: bool,
    ) -> dict[str, Any]:
        status = str(row["status"])
        if status == "COMPLETED":
            return {
                "status": "COMPLETED",
                "work_id": str(row["work_id"]),
                "target_generation": int(row["generation"]),
                "snapshot_id": row["result_snapshot_id"],
                "snapshot_revision": row["target_snapshot_revision"],
                "attached_to_existing_work": attached,
                "new_job_created": False,
                "sync_plan_url": "/market-context/mnq/sync/plan",
                "status_url": (
                    f"/market-context/mnq/sync/requests/{row['work_id']}"
                ),
            }
        if status == "WAITING_BACKOFF":
            return {
                "status": status,
                "work_id": str(row["work_id"]),
                "target_generation": int(row["generation"]),
                "next_retry_at": row["next_retry_at"],
                "new_job_created": new_job_created,
                "attached_to_existing_work": attached,
                "status_url": (
                    f"/market-context/mnq/sync/requests/{row['work_id']}"
                ),
            }
        return {
            "status": "IN_PROGRESS",
            "work_id": str(row["work_id"]),
            "target_generation": int(row["generation"]),
            "attached_to_existing_work": attached,
            "new_job_created": new_job_created,
            "sections": _loads(row["sections_json"], []),
            "residual_sections": _loads(row["residual_sections_json"], []),
            "status_url": f"/market-context/mnq/sync/requests/{row['work_id']}",
        }

    @staticmethod
    def _work_row(row: Any) -> dict[str, Any]:
        output = {
            "work_id": str(row["work_id"]),
            "symbol": str(row["symbol"]),
            "generation": int(row["generation"]),
            "refresh_reason": str(row["refresh_reason"]),
            "refresh_reasons": _loads(row["trigger_reasons_json"], []),
            "status": str(row["status"]),
            "sections": _loads(row["sections_json"], []),
            "completed_sections": _loads(
                row["completed_sections_json"],
                [],
            ),
            "residual_sections": _loads(row["residual_sections_json"], []),
            "parent_work_id": row["parent_work_id"],
            "target_snapshot_revision": row["target_snapshot_revision"],
            "result_snapshot_id": row["result_snapshot_id"],
            "next_retry_at": row["next_retry_at"],
            "error": _public_work_error(_loads(row["error_json"], None)),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "completed_at": row["completed_at"],
        }
        output["status_url"] = (
            f"/market-context/mnq/sync/requests/{output['work_id']}"
        )
        return output


class SyncContractError(ValueError):
    def __init__(self, code: str, status_code: int) -> None:
        super().__init__(code)
        self.code = code
        self.status_code = status_code


def validate_sections(sections: Iterable[str]) -> list[str]:
    if isinstance(sections, (str, bytes)) or not isinstance(sections, Iterable):
        raise SyncContractError("sections_must_be_array", 422)
    normalized = list(dict.fromkeys(str(item).strip() for item in sections))
    unknown = sorted(name for name in normalized if name not in SECTION_SET)
    if unknown:
        raise SyncContractError(f"unknown_sections:{','.join(unknown)}", 422)
    if not normalized:
        raise SyncContractError("sections_required", 422)
    return normalized


def validate_symbol(symbol: Any) -> str:
    normalized = str(symbol or "").strip().upper()
    if normalized != SYMBOL:
        raise SyncContractError("symbol_not_supported", 422)
    return normalized


def validate_identifier(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise SyncContractError(f"{field}_required", 422)
    if len(normalized) > MAX_IDENTIFIER_LENGTH or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9._:@/-]*",
        normalized,
    ):
        raise SyncContractError(f"{field}_invalid", 422)
    return normalized


def parse_positive_int(value: Any, *, field: str) -> int:
    if isinstance(value, bool):
        raise SyncContractError(f"{field}_invalid", 422)
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise SyncContractError(f"{field}_invalid", 422) from exc
    if parsed < 1 or str(value).strip() != str(parsed):
        raise SyncContractError(f"{field}_invalid", 422)
    return parsed


def validate_control_payload(
    payload: Any,
    *,
    allowed: set[str],
) -> None:
    if not isinstance(payload, dict):
        raise SyncContractError("request_body_must_be_object", 422)
    encoded = canonical_json(payload).encode("utf-8")
    if len(encoded) > MAX_CONTROL_PAYLOAD_BYTES:
        raise SyncContractError("control_payload_too_large", 413)
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise SyncContractError(
            "unknown_request_fields:" + ",".join(unknown),
            422,
        )


def control_request_fingerprint(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        canonical_json(_material_value(payload)).encode("utf-8")
    ).hexdigest()


def material_fingerprint(value: Any) -> str:
    return hashlib.sha256(
        canonical_json(_material_value(value)).encode("utf-8")
    ).hexdigest()


def _material_value(value: Any, *, field_name: str | None = None) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _material_value(item, field_name=str(key).lower())
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key).lower() not in VOLATILE_KEYS
        }
    if isinstance(value, list):
        normalized = [
            _material_value(item, field_name=field_name)
            for item in value
        ]
        if all(
            isinstance(item, dict)
            and any(item.get(key) not in (None, "") for key in ORDER_INSENSITIVE_ID_KEYS)
            for item in normalized
        ):
            ordered = sorted(normalized, key=canonical_json)
            deduplicated: list[Any] = []
            seen: set[str] = set()
            for item in ordered:
                technical_identity = canonical_json(
                    {
                        "provider": item.get("provider"),
                        "source": item.get("source"),
                        "identifier": next(
                            (
                                [key, item[key]]
                                for key in ORDER_INSENSITIVE_ID_KEYS
                                if item.get(key) not in (None, "")
                            ),
                            None,
                        ),
                        "occurrence_id": item.get("occurrence_id")
                        or item.get("related_occurrence_id"),
                        "version": item.get("version"),
                        "technical_fingerprint": item.get("technical_fingerprint")
                        or item.get("fingerprint")
                        or canonical_json(item),
                    }
                )
                if technical_identity not in seen:
                    seen.add(technical_identity)
                    deduplicated.append(item)
            return deduplicated
        return normalized
    if isinstance(value, datetime):
        return {"__datetime_utc__": _iso(_aware(value))}
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        return {"__number__": _canonical_number(value)}
    if isinstance(value, str) and field_name in TEMPORAL_KEYS:
        parsed = parse_datetime(value)
        if parsed is not None:
            return {"__datetime_utc__": _iso(_aware(parsed))}
    return value


def _canonical_number(value: int | float | Decimal) -> str:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("non_finite_number_not_supported")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("invalid_numeric_value") from exc
    if not number.is_finite():
        raise ValueError("non_finite_number_not_supported")
    normalized = number.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")


def producer_availability_classification(
    metadata: dict[str, Any],
) -> str | None:
    status = str(metadata.get("status") or "UNAVAILABLE").upper()
    freshness = str(metadata.get("freshness") or "UNKNOWN").upper()
    if status == "QUARANTINED" or freshness == "QUARANTINED":
        return "QUARANTINED_AT_PRODUCER"
    if status == "PARTIAL":
        return "PARTIAL_AT_PRODUCER"
    if status in UNAVAILABLE_STATUSES:
        return "UNAVAILABLE_AT_PRODUCER"
    if status in DEGRADED_STATUSES or freshness in DEGRADED_STATUSES:
        return "STALE_AT_PRODUCER"
    return None


def _quarantine_disclosure(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"record_count": 0, "reasons": []}
    disclosures = payload.get("producer_disclosures")
    quarantine = (
        disclosures.get("quarantine")
        if isinstance(disclosures, dict)
        and isinstance(disclosures.get("quarantine"), dict)
        else {}
    )
    return {
        "record_count": int(quarantine.get("record_count") or 0),
        "reasons": list(quarantine.get("reasons") or []),
    }


def _without_disclosures(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    return {
        key: value
        for key, value in payload.items()
        if key != "producer_disclosures"
    }


def _public_work_error(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return {
        key: redact_payload(value[key])
        for key in ("code", "error_type", "attempt")
        if value.get(key) is not None
    }


def record_count_for(value: Any) -> int:
    identities: set[str] = set()

    def walk(item: Any) -> None:
        if isinstance(item, dict):
            identifier = next(
                (
                    (key, item[key])
                    for key in ORDER_INSENSITIVE_ID_KEYS
                    if item.get(key) not in (None, "")
                ),
                None,
            )
            if identifier is not None:
                key, identifier_value = identifier
                identities.add(
                    canonical_json(
                        {
                            "provider": item.get("provider"),
                            "source": item.get("source"),
                            "identifier_key": key,
                            "identifier_value": identifier_value,
                            "occurrence_id": item.get("occurrence_id")
                            or item.get("related_occurrence_id"),
                            "version": item.get("version"),
                            "technical_fingerprint": (
                                item.get("technical_fingerprint")
                                or item.get("fingerprint")
                                or material_fingerprint(item)
                            ),
                        }
                    )
                )
                return
            for child in item.values():
                walk(child)
        elif isinstance(item, list):
            for child in item:
                walk(child)

    walk(value)
    if identities:
        return len(identities)
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        for key in ("items", "records", "events", "articles", "series"):
            items = value.get(key)
            if isinstance(items, list):
                return len(items)
    return 1 if _has_material_data(value) else 0


def section_status(payload: Any) -> tuple[str, str | None]:
    quarantine = _quarantine_disclosure(payload)
    material_payload = _without_disclosures(payload)
    if not _has_material_data(material_payload):
        if quarantine["record_count"]:
            return "QUARANTINED", "VALIDATED_DATA_WITHHELD"
        return "NO_DATA", "NO_VALIDATED_DATA"
    statuses = {
        str(value).upper()
        for value in _find_key_values(payload, "status")
        if value not in (None, "")
    }
    validations = {
        str(value).lower()
        for value in _find_key_values(payload, "validation_status")
        + _find_nested_validation_statuses(payload)
        if value not in (None, "")
    }
    top_validation = (
        str((payload.get("validation") or {}).get("status") or "").lower()
        if isinstance(payload, dict)
        and isinstance(payload.get("validation"), dict)
        else ""
    )
    if top_validation in {"rejected", "invalid", "quarantined"}:
        return "QUARANTINED", "NO_ACCEPTED_SOURCE"
    if "QUARANTINED" in statuses:
        return "QUARANTINED", "SOURCE_OR_VALIDATION_QUARANTINE"
    source_coverage = {
        str(value).upper()
        for value in _find_key_values(payload, "source_coverage_status")
        if value not in (None, "")
    }
    lifecycle = {
        str(value).upper()
        for value in _find_key_values(payload, "lifecycle_status")
        + _find_key_values(payload, "freshness_state")
        if value not in (None, "")
    }
    if quarantine["record_count"]:
        return "PARTIAL", "CONTAINS_WITHHELD_QUARANTINED_RECORDS"
    if validations.intersection({"rejected", "invalid", "quarantined"}):
        return "PARTIAL", "CONTAINS_QUARANTINED_RECORDS"
    if source_coverage.intersection({"PARTIAL", "UNVERIFIED_EMPTY"}):
        return "PARTIAL", "SOURCE_COVERAGE_INCOMPLETE"
    if lifecycle.intersection({"AWAITING_ACTUAL", "DUE", "EXPIRED"}):
        return "PARTIAL", "LIFECYCLE_DATA_INCOMPLETE"
    recognized = statuses.intersection(
        {"BACKOFF", "UNAVAILABLE", "NO_DATA", "PARTIAL", "AVAILABLE"}
    )
    available = "AVAILABLE" in recognized
    unavailable = recognized.intersection({"BACKOFF", "UNAVAILABLE", "NO_DATA"})
    if "PARTIAL" in recognized or (available and unavailable):
        return "PARTIAL", _find_reason(payload) or "MIXED_COMPONENT_STATUS"
    if available:
        return "AVAILABLE", _find_reason(payload)
    for status in ("BACKOFF", "UNAVAILABLE", "NO_DATA"):
        if status in recognized:
            return status, _find_reason(payload)
    return "AVAILABLE", None


def section_freshness(
    *,
    status: str,
    valid_until: str | None,
    payload: Any,
    reference: datetime,
) -> str:
    if status == "QUARANTINED":
        return "QUARANTINED"
    if status in {"NO_DATA", "UNAVAILABLE", "BACKOFF", "DISABLED"}:
        return status
    explicit = next(
        (
            str(item).upper()
            for item in _find_key_values(payload, "freshness")
            + _find_key_values(payload, "freshness_state")
            if item not in (None, "")
        ),
        None,
    )
    expiry = parse_datetime(valid_until)
    if expiry is not None and expiry <= _aware(reference):
        return "EXPIRED"
    if explicit in {
        "CURRENT",
        "FRESH",
        "CURRENT_SESSION",
        "LAST_KNOWN_GOOD",
        "STALE",
        "EXPIRED",
        "DUE",
    }:
        return "CURRENT" if explicit in {"FRESH", "CURRENT_SESSION"} else explicit
    return "CURRENT"


def market_session_from_sections(rows: list[Any]) -> dict[str, Any]:
    schedule_row = next(
        (row for row in rows if str(row["section_name"]) == "market_schedule"),
        None,
    )
    payload = _loads(schedule_row["payload_json"], {}) if schedule_row else {}
    nasdaq = (
        payload.get("nasdaq_cash")
        or payload.get("nasdaq_cash_session")
        or payload.get("nasdaq")
        or {}
    )
    mnq = (
        payload.get("mnq_futures")
        or payload.get("mnq_futures_session")
        or payload.get("globex")
        or {}
    )
    nasdaq_status = _session_status(nasdaq) or _session_status(payload) or "UNKNOWN"
    mnq_status = _session_status(mnq) or "UNKNOWN"
    reason = (
        payload.get("reason")
        or payload.get("market_session_status")
        or payload.get("status")
        or "UNKNOWN"
    )
    return {
        "nasdaq_cash": nasdaq_status,
        "mnq_futures": mnq_status,
        "reason": str(reason).upper(),
        "timezone": payload.get("timezone") or "America/New_York",
    }


def delivery_readiness(sections: dict[str, dict[str, Any]]) -> dict[str, Any]:
    metadata = {
        name: dict(value.get("sync") or {})
        for name, value in sections.items()
    }
    statuses = {
        name: str(value.get("status") or "UNAVAILABLE")
        for name, value in metadata.items()
    }
    classifications = {
        name: producer_availability_classification(value)
        for name, value in metadata.items()
    }
    available = [
        name for name, classification in classifications.items()
        if classification is None
    ]
    degraded = [
        name for name, classification in classifications.items()
        if classification is not None
    ]
    ratio = round(len(available) / max(len(statuses), 1), 4)
    return {
        "status": "READY" if not degraded else "PARTIAL",
        "calculated_from_delivered_payload": True,
        "available_section_count": len(available),
        "unavailable_section_count": len(degraded),
        "section_count": len(statuses),
        "coverage_ratio": ratio,
        "sections_available": available,
        "sections_unavailable": degraded,
        "section_status": statuses,
        "producer_classification": classifications,
    }


def context_fingerprint(rows: list[Any]) -> str:
    inventory = {
        str(row["section_name"]): {
            "section_revision": int(row["section_revision"]),
            "fingerprint": str(row["fingerprint"]),
        }
        for row in rows
    }
    return hashlib.sha256(canonical_json(inventory).encode("utf-8")).hexdigest()


def finalize_delivery(response: dict[str, Any]) -> dict[str, Any]:
    checksum_payload = {
        key: value
        for key, value in response.items()
        if key not in {"checksum", "payload_size_bytes"}
    }
    response["checksum"] = hashlib.sha256(
        canonical_json(checksum_payload).encode("utf-8")
    ).hexdigest()
    while True:
        measured = len(canonical_json(response).encode("utf-8"))
        if int(response["payload_size_bytes"]) == measured:
            return response
        response["payload_size_bytes"] = measured


def refresh_fingerprint(*, symbol: str, reason: str, sections: list[str]) -> str:
    return hashlib.sha256(
        canonical_json(
            {
                "symbol": symbol.upper(),
                "reason": reason.upper(),
                "sections": sorted(sections),
            }
        ).encode("utf-8")
    ).hexdigest()


def notification_envelope(row: Any) -> dict[str, Any]:
    changed_sections = _loads(row["changed_sections_json"], [])
    triggers = (
        _loads(row["triggers_json"], [])
        if "triggers_json" in row.keys()
        else []
    )
    if not triggers:
        triggers = [
            {
                "type": str(row["trigger_type"]).upper(),
                "entity_id": row["trigger_entity"],
                "occurred_at": row["created_at"],
            }
        ]
    symbol = SYMBOL
    return {
        "event_type": "MARKET_CONTEXT_UPDATED",
        "contract_version": (
            str(row["contract_version"])
            if "contract_version" in row.keys()
            else SCHEMA_VERSION
        ),
        "delivery_id": str(row["event_id"]),
        "symbol": symbol,
        "base_revision": (
            int(row["base_revision"])
            if "base_revision" in row.keys() and row["base_revision"] is not None
            else max(int(row["snapshot_revision"]) - 1, 0)
        ),
        "target_revision": int(row["snapshot_revision"]),
        "changed_sections": changed_sections,
        "triggers": triggers,
        "manifest_url": (
            row["manifest_url"]
            if "manifest_url" in row.keys() and row["manifest_url"]
            else f"/market-context/{symbol.lower()}/sync/manifest"
        ),
        "changes_url": (
            row["changes_url"]
            if "changes_url" in row.keys() and row["changes_url"]
            else (
                f"/market-context/{symbol.lower()}/sync/changes"
                f"?since_revision={max(int(row['snapshot_revision']) - 1, 0)}"
            )
        ),
        "created_at": row["created_at"],
    }


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    )


def _extract_vix(full: dict[str, Any]) -> dict[str, Any]:
    candidates: dict[str, Any] = {}
    for source_name in ("risk_context", "risk_sentiment", "deterministic_domains"):
        source = full.get(source_name)
        if not isinstance(source, dict):
            continue
        for key, value in source.items():
            if "vix" in str(key).lower() or "volatil" in str(key).lower():
                candidates[str(key)] = value
    return candidates


def _find_temporal_value(payload: Any, keys: tuple[str, ...]) -> str | None:
    values: list[tuple[datetime, str]] = []
    for key in keys:
        for raw in _find_key_values(payload, key):
            parsed = parse_datetime(raw)
            if parsed is not None:
                values.append((_aware(parsed), str(raw)))
    return max(values, default=(None, None), key=lambda item: item[0] or datetime.min.replace(tzinfo=UTC))[1]


def _find_key_values(value: Any, key: str) -> list[Any]:
    output: list[Any] = []
    if isinstance(value, dict):
        for current_key, item in value.items():
            if str(current_key).lower() == key.lower():
                output.append(item)
            output.extend(_find_key_values(item, key))
    elif isinstance(value, list):
        for item in value:
            output.extend(_find_key_values(item, key))
    return output


def _find_nested_validation_statuses(value: Any) -> list[Any]:
    output: list[Any] = []
    if isinstance(value, dict):
        validation = value.get("validation")
        if isinstance(validation, dict) and "status" in validation:
            output.append(validation["status"])
        for item in value.values():
            output.extend(_find_nested_validation_statuses(item))
    elif isinstance(value, list):
        for item in value:
            output.extend(_find_nested_validation_statuses(item))
    return output


def _find_reason(payload: Any) -> str | None:
    for key in ("reason", "no_data_reason", "unavailable_reason", "status_reason"):
        values = _find_key_values(payload, key)
        if values:
            return str(values[0])
    return None


def _has_material_data(value: Any) -> bool:
    if value in (None, "", [], {}):
        return False
    if isinstance(value, dict):
        ignored = {
            "status",
            "reason",
            "warnings",
            "validation",
            "freshness",
            "valid_until",
            "data_as_of",
        }
        return any(
            _has_material_data(item)
            for key, item in value.items()
            if str(key).lower() not in ignored
        )
    if isinstance(value, list):
        return any(_has_material_data(item) for item in value)
    return True


def _session_status(value: Any) -> str | None:
    if isinstance(value, str):
        return value.upper()
    if not isinstance(value, dict):
        return None
    raw = (
        value.get("session_status")
        or value.get("market_session_status")
        or value.get("status")
        or value.get("current_status")
    )
    return str(raw).upper() if raw not in (None, "") else None


def _change_type(previous: dict[str, Any], current: dict[str, Any]) -> str:
    if previous["status"] in UNAVAILABLE_STATUSES and current["status"] in AVAILABLE_STATUSES:
        return "ADDED"
    if previous["status"] in AVAILABLE_STATUSES and current["status"] in UNAVAILABLE_STATUSES:
        return "INVALIDATED"
    return "UPDATED"


def _change_reason(previous: dict[str, Any], current: dict[str, Any]) -> str:
    change_type = _change_type(previous, current)
    if change_type == "INVALIDATED":
        return "DATA_INVALIDATED"
    if change_type == "ADDED":
        return "DATA_BECAME_AVAILABLE"
    return "MATERIAL_CONTENT_CHANGED"


def _loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return default


def _aware(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _iso(value: datetime) -> str:
    return _aware(value).replace(microsecond=0).isoformat()
