from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.config import Settings
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.market_context_sync_service import MarketContextSyncService


class MarketContextSyncRefreshWorker:
    """Process one durable sync refresh generation outside the request path."""

    def __init__(
        self,
        settings: Settings,
        *,
        deterministic_runtime: Any,
        clock=None,
    ) -> None:
        self.settings = settings
        self.deterministic_runtime = deterministic_runtime
        self.clock = clock or (lambda: datetime.now(UTC))
        self.sync = MarketContextSyncService(settings)
        self.snapshots = MarketContextSnapshotRepository(settings)

    def run_once(self, *, owner: str | None = None) -> dict[str, Any]:
        lease_owner = owner or f"market-context-sync-{uuid.uuid4()}"
        work = self.sync.claim_next_refresh_work(owner=lease_owner)
        if work is None:
            return {"status": "IDLE", "work_id": None}
        try:
            components = self.snapshots.latest_components(str(work["symbol"]))
            if not components:
                raise RuntimeError("committed_market_context_components_unavailable")
            trigger_type = _trigger_type_for_work(work)
            refreshed = self.deterministic_runtime.enrich_market_context_sync(
                components,
                refresh="auto",
                trigger_type=trigger_type,
            )
            snapshot = self.snapshots.save_next(
                symbol=str(work["symbol"]),
                refresh_mode="market_context_sync_refresh",
                debug_payload=refreshed,
                ai_enrichment={"status": "NOT_REQUIRED"},
                trigger_type=trigger_type,
                correlation_id=str(work["work_id"]),
                trigger_metadata={
                    "coalesced": True,
                    "causes": [str(work["refresh_reason"])],
                },
            )
            completed = self.sync.complete_refresh_work(
                str(work["work_id"]),
                owner=lease_owner,
                snapshot_id=str(snapshot["snapshot_id"]),
            )
            return {
                "status": "COMPLETED",
                "work_id": work["work_id"],
                "snapshot_id": snapshot["snapshot_id"],
                "snapshot_revision": snapshot["revision"],
                "work": completed,
            }
        except Exception as exc:
            retry_at = self.clock() + timedelta(seconds=_retry_delay(work))
            prior_error = work.get("error")
            prior_attempt = (
                int(prior_error.get("attempt") or 0)
                if isinstance(prior_error, dict)
                else 0
            )
            waiting = self.sync.backoff_refresh_work(
                str(work["work_id"]),
                owner=lease_owner,
                next_retry_at=retry_at.astimezone(UTC).replace(
                    microsecond=0
                ).isoformat(),
                error={
                    "code": "SYNC_REFRESH_WORK_FAILED",
                    "error_type": type(exc).__name__,
                    "attempt": prior_attempt + 1,
                },
            )
            return {
                "status": "WAITING_BACKOFF",
                "work_id": work["work_id"],
                "next_retry_at": waiting["next_retry_at"],
                "error": waiting["error"],
            }


def _retry_delay(work: dict[str, Any]) -> int:
    error = work.get("error")
    prior_attempt = (
        int(error.get("attempt") or 0)
        if isinstance(error, dict)
        else 0
    )
    return min(30 * (2 ** min(prior_attempt, 5)), 900)


def _trigger_type_for_work(work: dict[str, Any]) -> str | None:
    reason = str(work.get("refresh_reason") or "").lower()
    direct = {
        "actual_macro": "macro_actual",
        "macro_actual": "macro_actual",
        "macro_revision": "macro_actual_revised",
        "fomc_decision": "fomc_decision",
        "fed_communication": "fomc_communication",
        "new_material_news": "breaking_news",
        "earnings_actual": "earnings_actual",
        "earnings_revision": "earnings_revision",
        "geopolitical_development": "geopolitical_development",
        "regulatory_development": "regulatory_development",
        "data_invalidated": "data_invalidated",
        "market_schedule_change": "market_schedule_change",
    }
    if reason in direct:
        return direct[reason]
    if reason != "market_trigger":
        return None
    sections = set(work.get("sections") or [])
    for section, trigger_type in (
        ("news", "breaking_news"),
        ("macro_actuals", "macro_actual"),
        ("fed", "fomc_communication"),
        ("earnings", "earnings_actual"),
        ("geopolitical_regulatory_risk", "geopolitical_development"),
        ("market_schedule", "market_schedule_change"),
    ):
        if section in sections:
            return trigger_type
    return None
