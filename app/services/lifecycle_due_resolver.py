from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Callable

from app.core.config import Settings
from app.services.event_driven_lifecycle_service import compute_datum_lifecycle


class DeterministicLifecycleDueResolver:
    """Resolve due rows from committed provider output without invoking AI.

    This adapter deliberately does not invent a provider for entity types that
    have no configured deterministic acquisition path. Those rows are deferred
    to a bounded next check and cannot fall through to AI.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))

    def resolve(self, item: dict[str, Any]) -> dict[str, Any]:
        datum = item.get("payload")
        if not isinstance(datum, dict) or not datum:
            return {
                "status": "DEFERRED",
                "reason": "deterministic_resolver_has_no_committed_payload",
                "ai_eligible": False,
            }
        lifecycle = compute_datum_lifecycle(
            str(item.get("entity_type") or "unknown"),
            str(item.get("entity_key") or ""),
            datum,
            settings=self.settings,
            now=self.clock(),
            attempt_count=int(item.get("attempt_count") or 0),
            session_state=item.get("session_state"),
            triggering_event=item.get("triggering_event"),
            refresh_reason="deterministic_committed_payload_revalidated",
        )
        if lifecycle.freshness_state == "FRESH":
            return {
                "status": "RESOLVED",
                "reason": "committed_provider_payload_is_fresh",
                "datum": datum,
                "lifecycle": lifecycle,
                "next_refresh_at": lifecycle.next_refresh_at,
                "ai_eligible": False,
            }
        return {
            "status": "DEFERRED",
            "reason": "no_configured_deterministic_provider_for_due_entity",
            "datum": datum,
            "lifecycle": lifecycle,
            "ai_eligible": False,
        }
