from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from typing import Any, Callable, Iterable

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.services.data_freshness_service import parse_datetime


TERMINAL_COVERAGE = frozenset({"VERIFIED_COMPLETE", "VERIFIED_EMPTY"})
EVENT_CALENDAR_CONTRACT_VERSION = "event-calendar-coverage-v2"


class EventCalendarCoverageRepository:
    """Persistent, scope-specific proof of calendar acquisition coverage."""

    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))
        migrate_database(settings.database_path)

    def missing_dates(
        self,
        dates: Iterable[date],
        *,
        provider_name: str,
        query_scope: str,
        now: datetime,
        symbol: str = "",
        contract_version: str = EVENT_CALENDAR_CONTRACT_VERSION,
        policy_version: str = "source-policy-v5",
    ) -> list[date]:
        requested = sorted(set(dates))
        if not requested:
            return []
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                """
                SELECT coverage_date,status,valid_until,
                       next_revision_check_at,next_retry_at
                FROM event_calendar_coverage
                WHERE data_domain='macro_calendar'
                  AND entity_type='economic_event'
                  AND provider_name=? AND query_scope=?
                  AND symbol=? AND contract_version=? AND policy_version=?
                  AND coverage_date BETWEEN ? AND ?
                """,
                (
                    provider_name,
                    query_scope,
                    symbol,
                    contract_version,
                    policy_version,
                    requested[0].isoformat(),
                    requested[-1].isoformat(),
                ),
            ).fetchall()
        by_date = {str(row["coverage_date"]): row for row in rows}
        return [
            day
            for day in requested
            if self._is_due(by_date.get(day.isoformat()), now=now)
        ]

    def record_day(
        self,
        day: date,
        *,
        provider_name: str,
        query_scope: str,
        window_start: datetime,
        window_end: datetime,
        status: str,
        record_count: int,
        provider_called: bool,
        scope_verified: bool,
        proof: dict[str, bool] | None = None,
        lineage: dict[str, Any] | None = None,
        valid_until: datetime | None = None,
        next_revision_check_at: datetime | None = None,
        next_retry_at: datetime | None = None,
        symbol: str = "",
        contract_version: str = EVENT_CALENDAR_CONTRACT_VERSION,
        policy_version: str = "source-policy-v5",
    ) -> bool:
        normalized_status = str(status).upper()
        proof_value = {
            key: bool((proof or {}).get(key))
            for key in (
                "request_succeeded",
                "pagination_complete",
                "parsing_succeeded",
                "records_valid",
                "expected_sources_complete",
                "authentic_empty",
            )
        }
        verified = normalized_status in TERMINAL_COVERAGE
        common_proof = (
            provider_called
            and scope_verified
            and proof_value["request_succeeded"]
            and proof_value["pagination_complete"]
            and proof_value["parsing_succeeded"]
            and proof_value["records_valid"]
            and proof_value["expected_sources_complete"]
        )
        if verified and (not common_proof or valid_until is None):
            raise ValueError("verified_coverage_requires_complete_positive_proof")
        if normalized_status == "VERIFIED_EMPTY" and (
            record_count != 0 or not proof_value["authentic_empty"]
        ):
            raise ValueError("verified_empty_requires_authentic_empty_proof")
        if normalized_status == "VERIFIED_COMPLETE" and record_count <= 0:
            raise ValueError("verified_complete_requires_records")
        if verified and next_revision_check_at is None:
            next_revision_check_at = valid_until
        now_text = self.clock().astimezone(UTC).replace(
            microsecond=0
        ).isoformat()
        lineage_value = dict(lineage or {})
        material = {
            "status": normalized_status,
            "record_count": int(record_count),
            "provider_called": bool(provider_called),
            "scope_verified": bool(scope_verified),
            "proof": proof_value,
            "valid_until": _iso(valid_until),
            "next_revision_check_at": _iso(next_revision_check_at),
            "next_retry_at": _iso(next_retry_at),
            "lineage": lineage_value,
        }
        fingerprint = hashlib.sha256(
            json.dumps(
                material,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        key = (
            day.isoformat(),
            "macro_calendar",
            "economic_event",
            provider_name,
            query_scope,
            symbol,
            contract_version,
            policy_version,
        )
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """
                SELECT content_fingerprint
                FROM event_calendar_coverage
                WHERE coverage_date=? AND data_domain=? AND entity_type=?
                  AND provider_name=? AND query_scope=?
                  AND symbol=? AND contract_version=? AND policy_version=?
                """,
                key,
            ).fetchone()
            if (
                existing is not None
                and str(existing["content_fingerprint"]) == fingerprint
            ):
                conn.rollback()
                return False
            conn.execute(
                """
                INSERT INTO event_calendar_coverage(
                  coverage_date,data_domain,entity_type,provider_name,
                  query_scope,symbol,contract_version,policy_version,
                  window_start,window_end,status,record_count,
                  provider_called,scope_verified,request_succeeded,
                  pagination_complete,parsing_succeeded,
                  records_valid,expected_sources_complete,authentic_empty,
                  retrieved_at,valid_until,
                  next_revision_check_at,next_retry_at,lineage_json,
                  content_fingerprint,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(
                  coverage_date,data_domain,entity_type,provider_name,
                  query_scope,symbol,contract_version,policy_version
                ) DO UPDATE SET
                  window_start=excluded.window_start,
                  window_end=excluded.window_end,
                  status=excluded.status,
                  record_count=excluded.record_count,
                  provider_called=excluded.provider_called,
                  scope_verified=excluded.scope_verified,
                  request_succeeded=excluded.request_succeeded,
                  pagination_complete=excluded.pagination_complete,
                  parsing_succeeded=excluded.parsing_succeeded,
                  records_valid=excluded.records_valid,
                  expected_sources_complete=excluded.expected_sources_complete,
                  authentic_empty=excluded.authentic_empty,
                  retrieved_at=excluded.retrieved_at,
                  valid_until=excluded.valid_until,
                  next_revision_check_at=excluded.next_revision_check_at,
                  next_retry_at=excluded.next_retry_at,
                  lineage_json=excluded.lineage_json,
                  content_fingerprint=excluded.content_fingerprint,
                  updated_at=excluded.updated_at
                """,
                (
                    *key,
                    window_start.astimezone(UTC).isoformat(),
                    window_end.astimezone(UTC).isoformat(),
                    normalized_status,
                    int(record_count),
                    int(provider_called),
                    int(scope_verified),
                    int(proof_value["request_succeeded"]),
                    int(proof_value["pagination_complete"]),
                    int(proof_value["parsing_succeeded"]),
                    int(proof_value["records_valid"]),
                    int(proof_value["expected_sources_complete"]),
                    int(proof_value["authentic_empty"]),
                    now_text if provider_called else None,
                    material["valid_until"],
                    material["next_revision_check_at"],
                    material["next_retry_at"],
                    json.dumps(lineage_value, sort_keys=True),
                    fingerprint,
                    now_text,
                    now_text,
                ),
            )
            conn.commit()
        return True

    def matrix(
        self,
        *,
        start_date: date,
        end_date: date,
        provider_name: str,
        query_scope: str,
        now: datetime,
        symbol: str = "",
        contract_version: str = EVENT_CALENDAR_CONTRACT_VERSION,
        policy_version: str = "source-policy-v5",
    ) -> dict[str, Any]:
        days = [
            start_date + timedelta(days=offset)
            for offset in range((end_date - start_date).days + 1)
        ]
        with connect_sqlite(self.settings.database_path) as conn:
            rows = conn.execute(
                """
                SELECT * FROM event_calendar_coverage
                WHERE data_domain='macro_calendar'
                  AND entity_type='economic_event'
                  AND provider_name=? AND query_scope=?
                  AND symbol=? AND contract_version=? AND policy_version=?
                  AND coverage_date BETWEEN ? AND ?
                ORDER BY coverage_date
                """,
                (
                    provider_name,
                    query_scope,
                    symbol,
                    contract_version,
                    policy_version,
                    start_date.isoformat(),
                    end_date.isoformat(),
                ),
            ).fetchall()
        by_date = {str(row["coverage_date"]): dict(row) for row in rows}
        unknown = [
            day.isoformat()
            for day in days
            if day.isoformat() not in by_date
            or self._is_due(by_date[day.isoformat()], now=now)
        ]
        partial = [
            key
            for key, row in by_date.items()
            if str(row["status"]) in {
                "PARTIAL",
                "PROVIDER_UNAVAILABLE",
                "QUARANTINED",
            }
        ]
        return {
            "status": (
                "VERIFIED_COMPLETE"
                if not unknown and not partial
                else "PARTIAL"
            ),
            "by_date": {
                key: {
                    "status": row["status"],
                    "record_count": row["record_count"],
                    "provider_called": bool(row["provider_called"]),
                    "scope_verified": bool(row["scope_verified"]),
                    "contract_version": row["contract_version"],
                    "policy_version": row["policy_version"],
                    "valid_until": row["valid_until"],
                    "next_revision_check_at": row[
                        "next_revision_check_at"
                    ],
                    "next_retry_at": row["next_retry_at"],
                }
                for key, row in by_date.items()
            },
            "unknown_coverage_days": unknown,
            "partial_coverage_days": sorted(partial),
        }

    @staticmethod
    def _is_due(row: Any | None, *, now: datetime) -> bool:
        if row is None or str(row["status"]) not in TERMINAL_COVERAGE:
            retry = parse_datetime(row["next_retry_at"]) if row else None
            return retry is None or retry <= now
        valid_until = parse_datetime(row["valid_until"])
        if valid_until is None or valid_until <= now:
            return True
        for field in ("next_revision_check_at",):
            due = parse_datetime(row[field])
            if due is not None and due <= now:
                return True
        return False


def _iso(value: datetime | None) -> str | None:
    return (
        value.astimezone(UTC).replace(microsecond=0).isoformat()
        if value is not None
        else None
    )
