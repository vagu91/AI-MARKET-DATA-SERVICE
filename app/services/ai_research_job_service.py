from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Iterable

from app.core.config import Settings
from app.models.events import EconomicEvent
from app.services.ai_research_job_repository import AIResearchJobRepository
from app.services.source_policy_service import SourcePolicyService
from app.services.temporal_domain_service import canonical_event_key, temporal_event_state
from app.services.research_profiles import profile_for_job


PROMPT_VERSION = "ai_research_job_v1"


class AIResearchJobService:
    def __init__(
        self,
        settings: Settings,
        *,
        repository: AIResearchJobRepository | None = None,
        source_policy: SourcePolicyService | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository or AIResearchJobRepository(settings)
        self.source_policy = source_policy or SourcePolicyService(settings.source_policy_path)
        self.clock = clock or (lambda: datetime.now(UTC))

    def enqueue_missing_events(
        self,
        events: Iterable[EconomicEvent],
        *,
        correlation_id: str | None = None,
        force: bool = False,
    ) -> list[dict[str, Any]]:
        if not self.settings.enable_ai_researcher:
            return []
        output: list[dict[str, Any]] = []
        for event in events:
            pending = [
                field for field in ("forecast", "consensus", "previous")
                if getattr(event.enrichment, field) in (None, "")
            ]
            if not pending:
                continue
            event_payload = event.model_dump(mode="json")
            event_key = canonical_event_key(event_payload)
            payload = self._payload(
                job_type="MISSING_EVENT_RESEARCH",
                symbol="MNQ",
                event=event_payload,
                pending_fields=pending,
            )
            scope_key = self._scope_key("MISSING_EVENT_RESEARCH", event_key, pending)
            generation, run_window = self._generation("MISSING_EVENT_RESEARCH", force=force)
            idem = self._idempotency_key(scope_key, generation)
            profile = profile_for_job("MISSING_EVENT_RESEARCH")
            job, _ = self.repository.enqueue(
                idempotency_key=idem,
                job_type="MISSING_EVENT_RESEARCH",
                symbol="MNQ",
                event_key=event_key,
                correlation_id=correlation_id or f"market-context-{uuid.uuid4()}",
                request_payload=payload,
                policy_version=self.source_policy.policy_version,
                prompt_version=profile.prompt_version,
                priority=50,
                pending_fields=pending,
                scope_key=scope_key,
                generation=generation,
                run_window=run_window,
                allow_requeue_terminal=force,
                profile_id=profile.profile_id,
                input_fingerprint=self._fingerprint(payload),
            )
            output.append(job)
        return output

    def enqueue_temporal_refreshes(
        self,
        events: Iterable[EconomicEvent],
        *,
        correlation_id: str | None = None,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for event in events:
            state = temporal_event_state(event, now=now)
            if state["temporal_status"] not in {"AWAITING_ACTUAL", "AWAITING_OUTCOME"}:
                continue
            job_type = "RELEASE_ACTUAL_REFRESH" if state["temporal_status"] == "AWAITING_ACTUAL" else "SPEECH_OUTCOME_REFRESH"
            pending = ["actual"] if job_type == "RELEASE_ACTUAL_REFRESH" else ["outcome", "transcript_url"]
            event_payload = event.model_dump(mode="json")
            event_key = state["canonical_event_key"]
            payload = self._payload(
                job_type=job_type,
                symbol="MNQ",
                event=event_payload,
                pending_fields=pending,
                temporal_state=state,
            )
            scope_key = self._scope_key(job_type, event_key, pending)
            generation, run_window = self._generation(job_type)
            profile = profile_for_job(job_type)
            job, _ = self.repository.enqueue(
                idempotency_key=self._idempotency_key(scope_key, generation),
                job_type=job_type,
                symbol="MNQ",
                event_key=event_key,
                correlation_id=correlation_id or f"release-refresh-{uuid.uuid4()}",
                request_payload=payload,
                policy_version=self.source_policy.policy_version,
                prompt_version="official_actual_semantics_v1" if job_type == "RELEASE_ACTUAL_REFRESH" else profile.prompt_version,
                priority=10 if job_type == "RELEASE_ACTUAL_REFRESH" else 30,
                max_attempts=self.settings.official_actual_max_attempts if job_type == "RELEASE_ACTUAL_REFRESH" else self.settings.ai_job_max_attempts,
                pending_fields=pending,
                scope_key=scope_key,
                generation=generation,
                run_window=run_window,
                allow_requeue_terminal=False,
                profile_id="OFFICIAL_ACTUAL" if job_type == "RELEASE_ACTUAL_REFRESH" else profile.profile_id,
                input_fingerprint=self._fingerprint(payload),
                retry_class="OFFICIAL_ACTUAL" if job_type == "RELEASE_ACTUAL_REFRESH" else "GENERIC_AI",
                retry_deadline_at=(
                    (
                        datetime.fromisoformat(str(state["release_at"]).replace("Z", "+00:00"))
                        + timedelta(hours=self.settings.official_feed_delay_hours)
                    ).astimezone(UTC).replace(microsecond=0).isoformat()
                    if job_type == "RELEASE_ACTUAL_REFRESH" and state.get("release_at") else None
                ),
            )
            output.append(job)
        return output

    def enqueue_explicit(
        self,
        *,
        job_type: str,
        symbol: str,
        correlation_id: str,
        request_payload: dict[str, Any],
        event_key: str | None = None,
        pending_fields: list[str] | None = None,
        force: bool = False,
    ) -> tuple[dict[str, Any], bool]:
        pending = pending_fields or list(request_payload.get("pending_fields") or [])
        identity = event_key or hashlib.sha256(self._canonical(request_payload).encode("utf-8")).hexdigest()[:24]
        payload = {
            **request_payload,
            "job_type": job_type,
            "symbol": symbol.upper(),
            "pending_fields": pending,
            "source_policy": self.source_policy.prompt_projection(),
            "policy_version": self.source_policy.policy_version,
            "prompt_version": profile_for_job(job_type).prompt_version,
        }
        scope_key = self._scope_key(job_type, identity, pending)
        generation, run_window = self._generation(job_type, force=force)
        profile = profile_for_job(job_type)
        return self.repository.enqueue(
            idempotency_key=self._idempotency_key(scope_key, generation),
            job_type=job_type,
            symbol=symbol,
            event_key=event_key,
            correlation_id=correlation_id,
            request_payload=payload,
            policy_version=self.source_policy.policy_version,
            prompt_version=profile.prompt_version,
            pending_fields=pending,
            scope_key=scope_key,
            generation=generation,
            run_window=run_window,
            allow_requeue_terminal=force,
            profile_id=profile.profile_id,
            input_fingerprint=self._fingerprint(payload),
        )

    def enrichment_status(
        self,
        symbol: str = "MNQ",
        *,
        snapshot_id: str | None = None,
        event_keys: list[str] | None = None,
    ) -> dict[str, Any]:
        jobs = self.repository.latest(
            limit=100,
            symbol=symbol,
            snapshot_id=snapshot_id,
            event_keys=event_keys,
        )
        latest_by_scope: dict[str, dict[str, Any]] = {}
        for job in jobs:
            identity = str(job.get("scope_key") or job["job_id"])
            latest_by_scope.setdefault(identity, job)
        jobs = list(latest_by_scope.values())
        active = [job for job in jobs if job["status"] in {"PENDING", "RUNNING", "RETRY_SCHEDULED"}]
        latest = jobs[0] if jobs else None
        statuses = {str(job["status"]) for job in jobs}
        if active:
            status = "RUNNING" if any(job["status"] == "RUNNING" for job in active) else "PENDING"
        elif statuses.intersection({"FAILED", "TIMED_OUT", "REJECTED", "CANCELLED"}):
            status = "FAILED"
        elif "NO_DATA" in statuses:
            status = "NO_DATA"
        elif statuses and statuses == {"SUCCEEDED"}:
            status = "SUCCEEDED"
        else:
            status = "NOT_REQUIRED"
        return {
            "status": status,
            "job_ids": [job["job_id"] for job in active[:20]],
            "requested_at": min((job["created_at"] for job in active), default=None),
            "completed_at": latest.get("completed_at") if latest and not active else None,
            "pending_fields": sorted({field for job in active for field in job.get("pending_fields") or []}),
            "accepted_fields": list(latest.get("accepted_fields") or []) if latest else [],
            "rejected_fields": list(latest.get("rejected_fields") or []) if latest else [],
            "policy_version": self.source_policy.policy_version,
            "prompt_version": PROMPT_VERSION,
            "last_error": latest.get("last_error") if latest else None,
        }

    def _payload(
        self,
        *,
        job_type: str,
        symbol: str,
        event: dict[str, Any],
        pending_fields: list[str],
        temporal_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        prompt_version = (
            "official_actual_semantics_v1"
            if job_type == "RELEASE_ACTUAL_REFRESH"
            else profile_for_job(job_type).prompt_version
        )
        return {
            "job_type": job_type,
            "symbol": symbol,
            "target_market": "MNQ/Nasdaq futures context",
            "missing_fields": pending_fields,
            "event": event,
            "expected_period": event.get("period"),
            "release_at": event.get("time_utc"),
            "temporal_state": temporal_state or temporal_event_state(event),
            "sources_already_queried": list((event.get("enrichment") or {}).get("field_lineage") or {}),
            "existing_database_results": event.get("enrichment") or {},
            "source_policy": self.source_policy.prompt_projection(),
            "policy_version": self.source_policy.policy_version,
            "prompt_version": prompt_version,
        }

    def _scope_key(self, job_type: str, identity: str, fields: list[str]) -> str:
        value = f"{job_type}|{identity}|{','.join(sorted(fields))}|{self.source_policy.policy_version}"
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _generation(self, job_type: str, *, force: bool = False) -> tuple[str, str]:
        duration_minutes = {
            "NEWS_DRIVER_RESEARCH": self.settings.ai_run_window_news_minutes,
            "MISSING_EVENT_RESEARCH": self.settings.ai_run_window_missing_event_minutes,
            "SPEECH_OUTCOME_REFRESH": self.settings.ai_run_window_speech_minutes,
            "EARNINGS_CONTEXT": self.settings.ai_run_window_earnings_minutes,
            "RELEASE_ACTUAL_REFRESH": self.settings.ai_run_window_actual_refresh_minutes,
        }.get(job_type, self.settings.ai_run_window_general_market_minutes)
        current = self.clock().astimezone(UTC)
        window_seconds = int(duration_minutes) * 60
        window_epoch = int(current.timestamp()) // window_seconds * window_seconds
        run_window = datetime.fromtimestamp(window_epoch, tz=UTC).isoformat()
        generation = f"force-{uuid.uuid4()}" if force else run_window
        return generation, run_window

    @staticmethod
    def _idempotency_key(scope_key: str, generation: str) -> str:
        return hashlib.sha256(f"{scope_key}|{generation}".encode("utf-8")).hexdigest()

    def _fingerprint(self, payload: dict[str, Any]) -> str:
        return hashlib.sha256(self._canonical(payload).encode("utf-8")).hexdigest()

    @staticmethod
    def _canonical(value: Any) -> str:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
