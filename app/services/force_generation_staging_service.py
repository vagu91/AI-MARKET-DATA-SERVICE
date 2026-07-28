from __future__ import annotations

import gc
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.infrastructure.persistence.database import (
    connect_sqlite,
    connect_sqlite_read_only,
)
from app.infrastructure.persistence.migrations import migrate_database


_CANONICAL_TABLE_KEYS: dict[str, tuple[str, ...]] = {
    "event_calendar_coverage": (
        "coverage_date",
        "data_domain",
        "entity_type",
        "provider_name",
        "query_scope",
        "symbol",
        "contract_version",
        "policy_version",
    ),
    "economic_events_history": ("event_key",),
    "datum_lifecycle_items": ("entity_type", "entity_key"),
}
_SURROGATE_COLUMNS = {
    "economic_events_history": {"id"},
}
_IMMUTABLE_UPDATE_COLUMNS = {
    "economic_events_history": {"created_at"},
    "datum_lifecycle_items": {"item_id", "created_at"},
}


class ForceGenerationStaging:
    """Isolate force discovery writes until the snapshot transaction commits.

    The SQLite backup is taken before provider I/O. Discovery then writes to the
    private copy, and only a whitelisted canonical delta is returned. The caller
    must publish that delta inside the short snapshot/outbox transaction.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        source = Path(settings.database_path)
        self.path = source.with_name(
            f".{source.stem}.force-stage-{uuid.uuid4().hex}.sqlite"
        )
        self.stage_settings = settings.model_copy(
            update={"database_path": self.path}
        )

    def __enter__(self) -> ForceGenerationStaging:
        migrate_database(self.settings.database_path)
        with connect_sqlite_read_only(
            self.settings.database_path
        ) as source:
            with connect_sqlite(self.path) as destination:
                source.backup(destination)
        return self

    def __exit__(self, *_args: object) -> None:
        gc.collect()
        for path in (
            self.path,
            Path(f"{self.path}-wal"),
            Path(f"{self.path}-shm"),
        ):
            for attempt in range(5):
                try:
                    path.unlink(missing_ok=True)
                    break
                except PermissionError:
                    if attempt == 4:
                        raise
                    gc.collect()
                    time.sleep(0.01)

    def generation_plan(self) -> dict[str, Any]:
        source_path = Path(self.settings.database_path)
        tables: dict[str, list[dict[str, Any]]] = {}
        with connect_sqlite_read_only(
            source_path
        ) as source, connect_sqlite(
            self.path
        ) as staged:
            for table, key_columns in _CANONICAL_TABLE_KEYS.items():
                source_rows = {
                    _row_key(dict(row), key_columns): dict(row)
                    for row in source.execute(f"SELECT * FROM {table}")
                }
                changed: list[dict[str, Any]] = []
                for row in staged.execute(f"SELECT * FROM {table}"):
                    values = dict(row)
                    if (
                        table == "datum_lifecycle_items"
                        and str(
                            values.get("entity_type") or ""
                        ).lower()
                        == "macro_actual"
                    ):
                        # Official actual lifecycle is owned by the
                        # provider-force reconciliation plan. Discovery may
                        # project a generic row in staging, but it must never
                        # overwrite fresh/backoff/terminal actual state.
                        continue
                    key = _row_key(values, key_columns)
                    if _comparable(
                        table,
                        source_rows.get(key),
                    ) == _comparable(table, values):
                        continue
                    for column in _SURROGATE_COLUMNS.get(table, set()):
                        values.pop(column, None)
                    changed.append(values)
                tables[table] = changed
        return {
            "generation_id": f"force-generation-{uuid.uuid4()}",
            "tables": tables,
            "coverage_write_count": len(
                tables["event_calendar_coverage"]
            ),
            "occurrence_write_count": len(
                tables["economic_events_history"]
            ),
            "discovery_lifecycle_write_count": len(
                tables["datum_lifecycle_items"]
            ),
        }


def publish_force_generation_in_transaction(
    connection: sqlite3.Connection,
    plan: dict[str, Any] | None,
) -> dict[str, int]:
    """Publish the staged canonical delta on the caller's transaction."""

    counts = {
        "coverage_write_count": 0,
        "occurrence_write_count": 0,
        "discovery_lifecycle_write_count": 0,
    }
    tables = (
        plan.get("tables")
        if isinstance(plan, dict)
        and isinstance(plan.get("tables"), dict)
        else {}
    )
    for table, key_columns in _CANONICAL_TABLE_KEYS.items():
        rows = tables.get(table) or []
        for raw in rows:
            if not isinstance(raw, dict) or not raw:
                continue
            values = dict(raw)
            columns = list(values)
            placeholders = ",".join("?" for _ in columns)
            conflict = ",".join(key_columns)
            immutable = {
                *key_columns,
                *_IMMUTABLE_UPDATE_COLUMNS.get(table, set()),
            }
            updates = [
                column for column in columns if column not in immutable
            ]
            update_sql = ",".join(
                f"{column}=excluded.{column}" for column in updates
            )
            connection.execute(
                f"""
                INSERT INTO {table}({",".join(columns)})
                VALUES ({placeholders})
                ON CONFLICT({conflict}) DO UPDATE SET {update_sql}
                """,
                tuple(values[column] for column in columns),
            )
        if table == "event_calendar_coverage":
            counts["coverage_write_count"] = len(rows)
        elif table == "economic_events_history":
            counts["occurrence_write_count"] = len(rows)
        else:
            counts["discovery_lifecycle_write_count"] = len(rows)
    return counts


def _row_key(
    row: dict[str, Any],
    columns: tuple[str, ...],
) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in columns)


def _comparable(
    table: str,
    row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if row is None:
        return None
    output = dict(row)
    for column in _SURROGATE_COLUMNS.get(table, set()):
        output.pop(column, None)
    return output
