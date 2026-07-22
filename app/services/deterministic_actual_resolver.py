from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.providers.bea import BeaProvider
from app.providers.bls import BlsProvider
from app.services.event_value_candidate_repository import EventValueCandidateRepository
from app.services.macro_consensus_service import candidate_metric_id
from app.services.official_actual_semantics import (
    OFFICIAL_METRICS,
    UNSUPPORTED_OFFICIAL_METRICS,
    derive_official_actual,
)


PROVIDERS = {"BLS": BlsProvider, "BEA": BeaProvider}


class DeterministicActualResolver:
    """Resolve event-semantic actuals from official observations, never raw macro levels."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.candidates = EventValueCandidateRepository(settings)
        self.cache = ProviderCacheRepository(settings.database_path)

    def __call__(self, job: dict[str, Any], workspace: Path, timeout_seconds: int) -> dict[str, Any]:
        del workspace, timeout_seconds
        event_key = str(job.get("event_key") or "")
        existing = self.candidates.accepted_official_actual(event_key) if event_key else None
        if existing is not None:
            return {"status": "SUCCEEDED", "results": [existing], "resolution": "persisted_candidate"}

        request = job.get("request_payload") or {}
        event = request.get("event") or {}
        temporal = request.get("temporal_state") or {}
        metric_id = _semantic_metric_id(event)
        if metric_id in UNSUPPORTED_OFFICIAL_METRICS:
            return {
                "status": "NO_DATA", "results": [],
                "error": f"official_metric_unsupported:{metric_id}:{UNSUPPORTED_OFFICIAL_METRICS[metric_id]}",
            }
        spec = OFFICIAL_METRICS.get(metric_id or "")
        if spec is None:
            return {
                "status": "NO_DATA", "results": [],
                "error": f"official_metric_unsupported:{metric_id or 'UNKNOWN'}",
            }
        provider_type = PROVIDERS.get(spec.provider)
        if provider_type is None:
            return {"status": "NO_DATA", "results": [], "error": "official_provider_adapter_unavailable"}
        try:
            result = asyncio.run(provider_type(self.cache, self.settings).fetch())
        except Exception as exc:
            return _feed_delayed(f"official_provider_unavailable:{type(exc).__name__}")
        rows = result.data if isinstance(result.data, dict) else {}
        series = rows.get(spec.source_series_id)
        if not isinstance(series, dict):
            return _feed_delayed("official_series_not_available")
        expected_adapter = f"{spec.provider}_OFFICIAL_API"
        if (
            result.metadata.source != spec.provider
            or series.get("official_adapter") is not True
            or series.get("provider_adapter") != expected_adapter
        ):
            return _feed_delayed(
                f"official_adapter_required:observed={series.get('provider_adapter') or result.metadata.source}"
            )
        source_adjustment = str(series.get("seasonal_adjustment") or "").upper()
        if source_adjustment and source_adjustment != spec.seasonal_adjustment:
            return {"status": "NO_DATA", "results": [], "error": "seasonal_adjustment_mismatch"}
        retrieved_at = result.metadata.retrieved_at.isoformat()
        release_timestamp = temporal.get("release_at") or event.get("time_utc")
        expected_period = event.get("reference_period") or event.get("period") or request.get("expected_period")
        try:
            candidate = derive_official_actual(
                spec,
                series,
                expected_period=expected_period,
                retrieved_at=retrieved_at,
                release_timestamp=release_timestamp,
            )
        except ValueError as exc:
            if str(exc) in {"period_mismatch", "insufficient_official_observations", "official_observations_missing"}:
                return _feed_delayed(str(exc))
            return {"status": "NO_DATA", "retryable": False, "results": [], "error": str(exc)}
        source_url = str(series.get("source_url") or "")
        canonical_url = str(series.get("canonical_url") or spec.canonical_url)
        candidate.update({
            "source": str(series.get("source") or result.metadata.source),
            "publisher": result.metadata.source,
            "source_url": source_url,
            "canonical_url": canonical_url,
            "source_domain": series.get("source_domain"),
            "provider_adapter": series.get("provider_adapter"),
            "evidence_text": (
                f"Official {result.metadata.source} adapter {expected_adapter}, series {spec.source_series_id}; "
                f"{spec.transformation} for {candidate['reference_period']}."
            ),
            "reliability": result.metadata.reliability,
            "confidence": result.metadata.reliability,
            "published_at": release_timestamp or datetime.now(UTC).replace(microsecond=0).isoformat(),
        })
        restored = self.candidates.persist_candidate(
            event_key=event_key,
            candidate=candidate,
            release_at=release_timestamp,
            expected_metric_id=spec.event_metric_id,
            expected_period=expected_period,
            expected_unit=spec.unit,
        )
        if restored["validation_status"] != "accepted":
            return {"status": "NO_DATA", "results": [], "error": "official_candidate_rejected", "candidate": restored}
        accepted = self.candidates.accepted_official_actual(event_key)
        if accepted is None:
            return {"status": "FAILED", "results": [], "error": "official_candidate_read_back_failed"}
        return {"status": "SUCCEEDED", "results": [accepted], "resolution": "official_provider"}


def _semantic_metric_id(event: dict[str, Any]) -> str | None:
    explicit = str(event.get("metric_id") or "")
    if explicit in OFFICIAL_METRICS or explicit in UNSUPPORTED_OFFICIAL_METRICS:
        return explicit
    for metric in (event.get("enrichment") or {}).get("metrics") or []:
        if isinstance(metric, dict):
            metric_id = str(metric.get("metric_id") or "")
            if metric_id in OFFICIAL_METRICS or metric_id in UNSUPPORTED_OFFICIAL_METRICS:
                return metric_id
    return candidate_metric_id({
        "metric_id": explicit,
        "event_name": event.get("name") or event.get("event_name"),
    })


def _feed_delayed(reason: str) -> dict[str, Any]:
    return {
        "status": "OFFICIAL_FEED_DELAYED", "retryable": True, "results": [],
        "error": reason, "delay_reason": reason,
    }
