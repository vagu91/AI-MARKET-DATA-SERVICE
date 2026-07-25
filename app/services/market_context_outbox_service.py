from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.event_driven_lifecycle_service import (
    TRIGGER_CLASS_BY_ENTITY,
    material_changes,
    materiality_fingerprint,
)


@dataclass(frozen=True)
class TriggerEnvelope:
    trigger_type: str
    trigger_entity: str | None = None
    trace_id: str | None = None
    correlation_id: str | None = None

    @classmethod
    def from_mapping(cls, value: Any) -> TriggerEnvelope | None:
        if not isinstance(value, dict) or not value.get("trigger_type"):
            return None
        return cls(
            trigger_type=str(value["trigger_type"]),
            trigger_entity=(
                str(value["trigger_entity"])
                if value.get("trigger_entity") is not None
                else None
            ),
            trace_id=str(value["trace_id"]) if value.get("trace_id") else None,
            correlation_id=(
                str(value["correlation_id"])
                if value.get("correlation_id")
                else None
            ),
        )

    def snapshot_arguments(self) -> dict[str, str | None]:
        return {
            "trigger_type": self.trigger_type,
            "trigger_entity": self.trigger_entity,
            "trace_id": self.trace_id,
            "correlation_id": self.correlation_id,
        }


class MarketContextOutboxRepository:
    """Persistent outbox only; live delivery intentionally does not exist."""

    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))
        migrate_database(settings.database_path)

    def emit_in_transaction(
        self,
        conn: Any,
        *,
        trigger_type: str,
        trigger_entity: str | None,
        snapshot_id: str,
        snapshot_revision: int,
        current_payload: dict[str, Any],
        previous_snapshot_id: str | None,
        previous_payload: dict[str, Any] | None,
        trace_id: str | None,
        correlation_id: str | None,
        data_as_of: str | None,
        created_at: str,
        parent_run_id: str | None = None,
        component_versions: dict[str, Any] | None = None,
        reason: str | None = None,
        trigger_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        trigger_class = TRIGGER_CLASS_BY_ENTITY.get(
            str(trigger_type or "").lower(),
            "NON_TRIGGERING",
        )
        if trigger_class != "TRIGGER":
            return None
        changed_sections, changes = material_changes(
            previous_payload,
            current_payload,
        )
        if not changed_sections:
            return None
        payload_hash = materiality_fingerprint(current_payload)
        idempotency_seed = "|".join(
            (
                "market_context.updated",
                str(trigger_type),
                str(trigger_entity or ""),
                str(previous_snapshot_id or ""),
                payload_hash,
            )
        )
        idempotency_key = hashlib.sha256(
            idempotency_seed.encode("utf-8")
        ).hexdigest()
        event_id = f"outbox-{uuid.uuid5(uuid.NAMESPACE_URL, idempotency_key)}"
        persisted_changes = [
            *changes,
            {
                "section": "_outbox_lineage",
                "parent_run_id": parent_run_id,
                "component_versions": component_versions or {},
                "reason": reason or trigger_type,
                "changed_event_ids": sorted(
                    {
                        str(value)
                        for value in (
                            (trigger_metadata or {}).get(
                                "changed_event_ids"
                            )
                            or []
                        )
                        if value
                    }
                ),
                "trigger_causes": sorted(
                    {
                        str(value)
                        for value in (
                            (trigger_metadata or {}).get("causes") or []
                        )
                        if value
                    }
                ),
                "coalesced": bool(
                    (trigger_metadata or {}).get("coalesced")
                ),
            },
        ]
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO market_context_outbox(
              event_id,event_type,trace_id,correlation_id,trigger_type,
              trigger_entity,snapshot_id,snapshot_revision,previous_snapshot_id,
              changed_sections_json,material_changes_json,data_as_of,created_at,
              delivery_status,attempt_count,next_attempt_at,idempotency_key,
              payload_hash
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'PENDING',0,?,?,?)
            """,
            (
                event_id,
                "market_context.updated",
                trace_id,
                correlation_id,
                trigger_type,
                trigger_entity,
                snapshot_id,
                snapshot_revision,
                previous_snapshot_id,
                _json(changed_sections),
                _json(persisted_changes),
                data_as_of,
                created_at,
                created_at,
                idempotency_key,
                payload_hash,
            ),
        )
        row = conn.execute(
            "SELECT * FROM market_context_outbox WHERE idempotency_key=?",
            (idempotency_key,),
        ).fetchone()
        if row is None:
            return None
        output = _row(row)
        output["created"] = int(cursor.rowcount or 0) == 1
        output["deduplicated"] = not output["created"]
        return output

    def list_events(
        self,
        *,
        status: str | None = "PENDING",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                """
                SELECT * FROM market_context_outbox
                WHERE (? IS NULL OR delivery_status=?)
                ORDER BY created_at,event_id LIMIT ?
                """,
                (status, status, min(max(int(limit), 1), 500)),
            ).fetchall()
        return [_row(row) for row in rows]

    def get(self, event_id: str) -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                "SELECT * FROM market_context_outbox WHERE event_id=?",
                (event_id,),
            ).fetchone()
        return _row(row) if row else None

    def acknowledge(
        self,
        event_id: str,
        *,
        consumer_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        consumer_id = str(consumer_id or "").strip()[:120]
        if not consumer_id:
            raise ValueError("outbox_consumer_id_required")
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM market_context_outbox WHERE event_id=?",
                (event_id,),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise ValueError("outbox_event_not_found")
            if str(row["idempotency_key"]) != str(idempotency_key):
                conn.rollback()
                raise ValueError("outbox_idempotency_key_mismatch")
            if str(row["delivery_status"]) == "ACKNOWLEDGED":
                if str(row["acknowledged_by"]) != consumer_id:
                    conn.rollback()
                    raise ValueError("outbox_already_acknowledged_by_other_consumer")
                conn.commit()
                return _row(row)
            acknowledged_at = _iso(self.clock())
            conn.execute(
                """
                UPDATE market_context_outbox
                SET delivery_status='ACKNOWLEDGED',acknowledged_at=?,
                    acknowledged_by=?
                WHERE event_id=? AND delivery_status='PENDING'
                """,
                (acknowledged_at, consumer_id, event_id),
            )
            conn.commit()
            restored = conn.execute(
                "SELECT * FROM market_context_outbox WHERE event_id=?",
                (event_id,),
            ).fetchone()
        return _row(restored)


def _row(row: Any) -> dict[str, Any]:
    output = dict(row)
    output["changed_sections"] = json.loads(
        output.pop("changed_sections_json") or "[]"
    )
    material_changes = json.loads(
        output.pop("material_changes_json") or "[]"
    )
    lineage = next(
        (
            item
            for item in material_changes
            if isinstance(item, dict)
            and item.get("section") == "_outbox_lineage"
        ),
        {},
    )
    output["material_changes"] = [
        item
        for item in material_changes
        if not (
            isinstance(item, dict)
            and item.get("section") == "_outbox_lineage"
        )
    ]
    output["parent_run_id"] = lineage.get("parent_run_id")
    output["component_versions"] = lineage.get("component_versions") or {}
    output["reason"] = lineage.get("reason") or output.get("trigger_type")
    output["changed_event_ids"] = lineage.get("changed_event_ids") or []
    output["trigger_causes"] = lineage.get("trigger_causes") or []
    output["coalesced"] = bool(lineage.get("coalesced"))
    return output


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _iso(value: datetime) -> str:
    return (
        value.astimezone(UTC)
        if value.tzinfo
        else value.replace(tzinfo=UTC)
    ).replace(microsecond=0).isoformat()
