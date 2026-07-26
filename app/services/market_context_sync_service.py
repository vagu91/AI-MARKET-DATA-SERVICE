from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable

from app.core.config import Settings
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
        "PARTIAL",
        "LAST_KNOWN_GOOD",
        "NO_DATA_EXPECTED",
        "NO_RELEVANT_DATA",
        "NO_RELEVANT_MARKETS",
    }
)
UNAVAILABLE_STATUSES = frozenset(
    {"NO_DATA", "UNAVAILABLE", "QUARANTINED", "BACKOFF", "DISABLED"}
)
ACTIVE_WORK_STATUSES = frozenset({"PENDING", "RUNNING", "WAITING_BACKOFF"})
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
        conn.execute(
            """
            INSERT OR REPLACE INTO market_context_sync_sections(
              symbol,snapshot_id,snapshot_revision,section_name,section_revision,
              fingerprint,record_count,data_as_of,freshness,valid_until,status,
              reason,payload_json,created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                canonical_json(payload),
                created_at,
            ),
        )
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
    return {
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
        normalized = validate_sections(sections)
        if not str(consumer_id or "").strip():
            raise SyncContractError("consumer_id_required", 422)
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

    def plan(
        self,
        payload: dict[str, Any],
        *,
        symbol: str = SYMBOL,
    ) -> dict[str, Any]:
        consumer_id = str(payload.get("consumer_id") or "").strip()
        request_id = str(payload.get("request_id") or "").strip()
        if not consumer_id:
            raise SyncContractError("consumer_id_required", 422)
        if not request_id:
            raise SyncContractError("request_id_required", 422)
        required = validate_sections(payload.get("required_sections") or SECTION_NAMES)
        manifest = self.manifest(symbol=symbol)
        target_revision = int(manifest["snapshot_revision"])
        known_revision = payload.get("known_snapshot_revision")
        known_sections = payload.get("known_sections")
        if not isinstance(known_sections, dict):
            known_sections = {}
        unavailable = [
            {
                "section": name,
                "classification": "UNAVAILABLE_AT_PRODUCER",
                "status": manifest["sections"][name]["status"],
                "reason": manifest["sections"][name].get("reason"),
            }
            for name in required
            if manifest["sections"][name]["status"] in UNAVAILABLE_STATUSES
        ]
        if known_revision is None or not known_sections:
            return {
                "sync_mode": "FULL",
                "target_snapshot_revision": target_revision,
                "reason": "NO_CONSUMER_CONTEXT",
                "required_sections": required,
                "unavailable_at_producer": unavailable,
            }
        if not self._revision_exists(symbol, int(known_revision)):
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
            if current["status"] in UNAVAILABLE_STATUSES:
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
            if int(known.get("section_revision") or -1) != int(
                current["section_revision"]
            ):
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
            if known_fingerprint and str(known_fingerprint) != str(
                current["fingerprint"]
            ):
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
            if previous["fingerprint"] == current["fingerprint"]:
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
        consumer_id = str(payload.get("consumer_id") or "").strip()
        request_id = str(payload.get("request_id") or "").strip()
        reason = str(payload.get("reason") or "CONTEXT_REQUIRED").strip().upper()
        if not consumer_id:
            raise SyncContractError("consumer_id_required", 422)
        if not request_id:
            raise SyncContractError("request_id_required", 422)
        requested = validate_sections(payload.get("required_sections") or SECTION_NAMES)
        manifest = self.manifest(symbol=symbol)
        if reason not in MATERIAL_TRIGGER_REASONS and all(
            manifest["sections"][name]["status"] in AVAILABLE_STATUSES
            and manifest["sections"][name]["freshness"] == "CURRENT"
            for name in requested
        ):
            return 200, {
                "status": "READY",
                "snapshot_revision": manifest["snapshot_revision"],
                "sync_plan_url": f"/market-context/{symbol.lower()}/sync/plan",
            }
        now = _iso(datetime.now(UTC))
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing_waiter = conn.execute(
                """
                SELECT w.* FROM market_context_sync_waiters wait
                JOIN market_context_sync_refresh_work w ON w.work_id=wait.work_id
                WHERE wait.consumer_id=? AND wait.request_id=?
                """,
                (consumer_id, request_id),
            ).fetchone()
            if existing_waiter is not None:
                conn.commit()
                return 202, self._work_response(
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
                if str(row["status"]) == "WAITING_BACKOFF" and requested_set <= current_sections:
                    self._attach_waiter(
                        conn,
                        row["work_id"],
                        consumer_id,
                        request_id,
                        requested,
                        now,
                    )
                    conn.commit()
                    return 202, self._work_response(
                        row,
                        attached=True,
                        new_job_created=False,
                    )
                if requested_set <= current_sections:
                    self._attach_waiter(
                        conn,
                        row["work_id"],
                        consumer_id,
                        request_id,
                        requested,
                        now,
                    )
                    conn.commit()
                    return 202, self._work_response(
                        row,
                        attached=True,
                        new_job_created=False,
                    )
                overlap = requested_set & current_sections
                if overlap and str(row["status"]) == "PENDING":
                    union = sorted(requested_set | current_sections)
                    fingerprint = refresh_fingerprint(
                        symbol=symbol,
                        reason=reason,
                        sections=union,
                    )
                    conn.execute(
                        """
                        UPDATE market_context_sync_refresh_work
                        SET sections_json=?,residual_sections_json=?,
                            request_fingerprint=?,updated_at=?
                        WHERE work_id=?
                        """,
                        (
                            canonical_json(union),
                            canonical_json(sorted(requested_set - current_sections)),
                            fingerprint,
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
                    )
                    updated = conn.execute(
                        "SELECT * FROM market_context_sync_refresh_work WHERE work_id=?",
                        (row["work_id"],),
                    ).fetchone()
                    conn.commit()
                    return 202, self._work_response(
                        updated,
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
                  residual_sections_json,parent_work_id,created_at,updated_at
                ) VALUES (?,?,?,?,?,'PENDING',?,'[]',?,?,?,?)
                """,
                (
                    work_id,
                    symbol.upper(),
                    generation,
                    reason,
                    request_fingerprint,
                    canonical_json(residual),
                    canonical_json(residual),
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
            waiters = conn.execute(
                """
                SELECT consumer_id,request_id,requested_sections_json,attached_at
                FROM market_context_sync_waiters WHERE work_id=?
                ORDER BY attached_at,consumer_id,request_id
                """,
                (work_id,),
            ).fetchall()
        output = self._work_row(row)
        output["waiters"] = [
            {
                **dict(waiter),
                "requested_sections": _loads(
                    waiter["requested_sections_json"],
                    [],
                ),
            }
            for waiter in waiters
        ]
        for waiter in output["waiters"]:
            waiter.pop("requested_sections_json", None)
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
        consumer_id = str(payload.get("consumer_id") or "").strip()
        delivery_id = str(payload.get("delivery_id") or "").strip()
        status = str(payload.get("status") or "").strip().upper()
        acknowledged_at = str(payload.get("acknowledged_at") or "").strip()
        section_revisions = payload.get("section_revisions")
        if not consumer_id:
            raise SyncContractError("consumer_id_required", 422)
        if not delivery_id:
            raise SyncContractError("delivery_id_required", 422)
        if status != "PERSISTED":
            raise SyncContractError("ack_status_must_be_persisted", 422)
        if parse_datetime(acknowledged_at) is None:
            raise SyncContractError("acknowledged_at_invalid", 422)
        if not isinstance(section_revisions, dict):
            raise SyncContractError("section_revisions_required", 422)
        revision = int(payload.get("snapshot_revision") or 0)
        normalized_revisions = {
            name: int(value)
            for name, value in section_revisions.items()
            if name in SECTION_SET
        }
        if len(normalized_revisions) != len(section_revisions):
            raise SyncContractError("ack_unknown_section", 422)
        received_at = _iso(datetime.now(UTC))
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
                UPDATE market_context_outbox
                SET delivery_status='ACKNOWLEDGED',acknowledged_at=?,
                    acknowledged_by=?
                WHERE event_id=?
                """,
                (received_at, consumer_id, delivery_id),
            )
            pending = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM market_context_outbox o
                    WHERE o.delivery_status='PENDING'
                      AND NOT EXISTS (
                        SELECT 1 FROM market_context_delivery_acks a
                        WHERE a.delivery_id=o.event_id AND a.consumer_id=?
                      )
                    """,
                    (consumer_id,),
                ).fetchone()[0]
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
                  last_delivery_acknowledged=excluded.last_delivery_acknowledged,
                  last_snapshot_revision_acknowledged=
                    excluded.last_snapshot_revision_acknowledged,
                  section_revisions_json=excluded.section_revisions_json,
                  pending_delivery_count=excluded.pending_delivery_count,
                  gap_detected=0,resync_required=0,updated_at=excluded.updated_at
                """,
                (
                    consumer_id,
                    delivery_id,
                    delivery_id,
                    delivery_id,
                    revision,
                    canonical_json(normalized_revisions),
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
        }

    def consumer_state(self, consumer_id: str) -> dict[str, Any]:
        consumer_id = str(consumer_id or "").strip()
        if not consumer_id:
            raise SyncContractError("consumer_id_required", 422)
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                "SELECT * FROM market_context_consumer_state WHERE consumer_id=?",
                (consumer_id,),
            ).fetchone()
            pending_rows = conn.execute(
                """
                SELECT event_id,snapshot_revision,attempt_count,next_attempt_at,
                       created_at
                FROM market_context_outbox o
                WHERE o.delivery_status='PENDING'
                  AND NOT EXISTS (
                    SELECT 1 FROM market_context_delivery_acks a
                    WHERE a.delivery_id=o.event_id AND a.consumer_id=?
                  )
                ORDER BY created_at,event_id
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
        normalized_status = str(status or "").strip().upper()
        if normalized_status not in {"NOTIFIED", "FAILED", "ACKNOWLEDGED"}:
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
            attempt_number = int(
                conn.execute(
                    """
                    SELECT COALESCE(MAX(attempt_number),0)+1
                    FROM market_context_delivery_attempts
                    WHERE delivery_id=? AND consumer_id=?
                    """,
                    (delivery_id, consumer_id),
                ).fetchone()[0]
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
                    canonical_json(error or {}),
                ),
            )
            conn.execute(
                """
                UPDATE market_context_outbox
                SET attempt_count=attempt_count+1,next_attempt_at=?
                WHERE event_id=?
                """,
                (next_retry_at, delivery_id),
            )
            conn.execute(
                """
                INSERT INTO market_context_consumer_state(
                  consumer_id,last_delivery_created,last_delivery_notified,
                  section_revisions_json,pending_delivery_count,retry_count,
                  last_error_json,gap_detected,resync_required,updated_at
                ) VALUES (?,?,?,'{}',1,?,?,0,0,?)
                ON CONFLICT(consumer_id) DO UPDATE SET
                  last_delivery_created=excluded.last_delivery_created,
                  last_delivery_notified=CASE
                    WHEN ?='NOTIFIED' THEN excluded.last_delivery_notified
                    ELSE market_context_consumer_state.last_delivery_notified
                  END,
                  pending_delivery_count=1,
                  retry_count=market_context_consumer_state.retry_count+?,
                  last_error_json=excluded.last_error_json,
                  updated_at=excluded.updated_at
                """,
                (
                    consumer_id,
                    delivery_id,
                    delivery_id if normalized_status == "NOTIFIED" else None,
                    int(normalized_status == "FAILED"),
                    canonical_json(error or {}),
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
            "status": normalized_status,
            "next_retry_at": next_retry_at,
        }

    def _snapshot_and_sections(
        self,
        *,
        symbol: str,
        snapshot_revision: int | None,
    ) -> tuple[Any, list[Any]]:
        with connect_sqlite(self.settings.database_path) as conn:
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
    ) -> None:
        conn.execute(
            """
            INSERT INTO market_context_sync_waiters(
              work_id,consumer_id,request_id,requested_sections_json,attached_at
            ) VALUES (?,?,?,?,?)
            """,
            (
                work_id,
                consumer_id,
                request_id,
                canonical_json(requested),
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
        output = dict(row)
        for source, target, default in (
            ("sections_json", "sections", []),
            ("completed_sections_json", "completed_sections", []),
            ("residual_sections_json", "residual_sections", []),
            ("error_json", "error", None),
        ):
            output[target] = _loads(output.pop(source), default)
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


def material_fingerprint(value: Any) -> str:
    return hashlib.sha256(
        canonical_json(_material_value(value)).encode("utf-8")
    ).hexdigest()


def _material_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _material_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key).lower() not in VOLATILE_KEYS
        }
    if isinstance(value, list):
        normalized = [_material_value(item) for item in value]
        if all(
            isinstance(item, dict)
            and any(item.get(key) not in (None, "") for key in ORDER_INSENSITIVE_ID_KEYS)
            for item in normalized
        ):
            return sorted(normalized, key=canonical_json)
        return normalized
    return value


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
    if not _has_material_data(payload):
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
    if validations.intersection({"rejected", "invalid", "quarantined"}):
        return "PARTIAL", "CONTAINS_QUARANTINED_RECORDS"
    for status in ("BACKOFF", "UNAVAILABLE", "NO_DATA", "PARTIAL", "AVAILABLE"):
        if status in statuses:
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
    statuses = {
        name: str((value.get("sync") or {}).get("status") or "UNAVAILABLE")
        for name, value in sections.items()
    }
    unavailable = [
        name for name, status in statuses.items() if status in UNAVAILABLE_STATUSES
    ]
    available = [name for name in statuses if name not in unavailable]
    ratio = round(len(available) / max(len(statuses), 1), 4)
    return {
        "status": "READY" if not unavailable else "PARTIAL",
        "calculated_from_delivered_payload": True,
        "available_section_count": len(available),
        "unavailable_section_count": len(unavailable),
        "section_count": len(statuses),
        "coverage_ratio": ratio,
        "sections_available": available,
        "sections_unavailable": unavailable,
        "section_status": statuses,
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
