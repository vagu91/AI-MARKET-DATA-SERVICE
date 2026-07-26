from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from threading import Event, Thread
from typing import Any

from app.core.config import Settings
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.market_context_sync_service import (
    MarketContextSyncService,
    SyncContractError,
)


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
        lease_owner = (
            f"{owner or 'market-context-sync'}-{uuid.uuid4()}"
        )
        lease_seconds = 120
        work = self.sync.claim_next_refresh_work(
            owner=lease_owner,
            lease_seconds=lease_seconds,
        )
        if work is None:
            return {"status": "IDLE", "work_id": None}
        heartbeat = _LeaseHeartbeat(
            self.sync,
            work_id=str(work["work_id"]),
            owner=lease_owner,
            lease_seconds=lease_seconds,
        )
        heartbeat.start()
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
            heartbeat.assert_owned()
            snapshot = self.snapshots.save_next(
                symbol=str(work["symbol"]),
                refresh_mode="market_context_sync_refresh",
                debug_payload=refreshed,
                ai_enrichment={"status": "NOT_REQUIRED"},
                trigger_type=trigger_type,
                correlation_id=str(work["work_id"]),
                trigger_metadata={
                    "coalesced": True,
                    "causes": list(work["refresh_reasons"]),
                },
            )
            heartbeat.assert_owned()
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
            heartbeat.stop()
            if heartbeat.lost:
                return {
                    "status": "LEASE_LOST",
                    "work_id": work["work_id"],
                }
            retry_at = self.clock() + timedelta(seconds=_retry_delay(work))
            prior_error = work.get("error")
            prior_attempt = (
                int(prior_error.get("attempt") or 0)
                if isinstance(prior_error, dict)
                else 0
            )
            try:
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
            except SyncContractError:
                return {
                    "status": "LEASE_LOST",
                    "work_id": work["work_id"],
                }
            return {
                "status": "WAITING_BACKOFF",
                "work_id": work["work_id"],
                "next_retry_at": waiting["next_retry_at"],
                "error": waiting["error"],
            }
        finally:
            heartbeat.stop()


class _LeaseHeartbeat:
    def __init__(
        self,
        sync: MarketContextSyncService,
        *,
        work_id: str,
        owner: str,
        lease_seconds: int,
    ) -> None:
        self.sync = sync
        self.work_id = work_id
        self.owner = owner
        self.lease_seconds = lease_seconds
        self._stop = Event()
        self._lost = Event()
        self._thread = Thread(
            target=self._run,
            name=f"sync-heartbeat-{work_id}",
            daemon=True,
        )

    @property
    def lost(self) -> bool:
        return self._lost.is_set()

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=2)

    def assert_owned(self) -> None:
        if self.lost:
            raise RuntimeError("refresh_work_lease_lost")

    def _run(self) -> None:
        interval = max(self.lease_seconds // 3, 1)
        while not self._stop.wait(interval):
            try:
                self.sync.heartbeat_refresh_work(
                    self.work_id,
                    owner=self.owner,
                    lease_seconds=self.lease_seconds,
                )
            except Exception:
                self._lost.set()
                return


def _retry_delay(work: dict[str, Any]) -> int:
    error = work.get("error")
    prior_attempt = (
        int(error.get("attempt") or 0)
        if isinstance(error, dict)
        else 0
    )
    return min(30 * (2 ** min(prior_attempt, 5)), 900)


def _trigger_type_for_work(work: dict[str, Any]) -> str | None:
    reasons = [
        str(item).lower()
        for item in work.get("refresh_reasons") or []
    ] or [str(work.get("refresh_reason") or "").lower()]
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
    direct_reason = next((item for item in reasons if item in direct), None)
    if direct_reason is not None:
        return direct[direct_reason]
    if "market_trigger" not in reasons:
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
