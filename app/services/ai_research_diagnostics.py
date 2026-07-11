from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.core.redaction import redact_payload, redact_sensitive


class AIResearchDiagnostics:
    """Opt-in, file-only diagnostics for tracing one AI enrichment run."""

    def __init__(self, settings: Any, *, artifact_dir: str | Path | None = None) -> None:
        self.enabled = bool(getattr(settings, "ai_diagnostics", False))
        if not self.enabled:
            self.root: Path | None = None
            return
        configured = artifact_dir or getattr(settings, "ai_diagnostics_dir", None)
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        self.root = Path(configured) if configured else Path("data/diagnostics") / f"ai_enrichment_root_cause_{timestamp}"
        self.root.mkdir(parents=True, exist_ok=True)

    @property
    def artifact_dir(self) -> str | None:
        return str(self.root.resolve()) if self.root else None

    def event_dir(self, event_id: str | None) -> Path | None:
        if not self.root:
            return None
        safe_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", event_id or "unknown_event")
        path = self.root / "events" / safe_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_json(self, relative: str | Path, payload: Any) -> None:
        if not self.root:
            return
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(redact_payload(payload), indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    def write_text(self, relative: str | Path, value: str | None) -> None:
        if not self.root:
            return
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(redact_sensitive(value or ""), encoding="utf-8")

    def event_json(self, event_id: str | None, name: str, payload: Any) -> None:
        path = self.event_dir(event_id)
        if path:
            self.write_json(path.relative_to(self.root) / name, payload)

    def event_text(self, event_id: str | None, name: str, value: str | None) -> None:
        path = self.event_dir(event_id)
        if path:
            self.write_text(path.relative_to(self.root) / name, value)

    def existing_event_path(self, event_id: str | None) -> Path | None:
        if not self.root:
            return None
        safe_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", event_id or "unknown_event")
        path = self.root / "events" / safe_id
        return path if path.exists() else None


def record_final_consumer_events(settings: Any, artifact_dir: str | Path | None, consumer: dict[str, Any]) -> None:
    diagnostics = AIResearchDiagnostics(settings, artifact_dir=artifact_dir)
    if not diagnostics.enabled:
        return
    calendar = consumer.get("event_calendar") or {}
    for events in calendar.values():
        if not isinstance(events, list):
            continue
        for event in events:
            if isinstance(event, dict) and event.get("event_id"):
                path = diagnostics.existing_event_path(event.get("event_id"))
                if path:
                    diagnostics.write_json(path.relative_to(diagnostics.root) / "final_consumer_event.json", event)
