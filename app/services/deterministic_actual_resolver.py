from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.providers.bea import BeaProvider
from app.providers.bls import BlsProvider
from app.providers.census import CensusProvider
from app.providers.fred import FredProvider
from app.providers.sp_global_pmi import SpGlobalPmiProvider
from app.services.event_value_candidate_repository import EventValueCandidateRepository
from app.services.macro_consensus_service import candidate_metric_id
from app.services.official_actual_semantics import (
    OFFICIAL_METRICS,
    UNSUPPORTED_OFFICIAL_METRICS,
    derive_official_actual,
    normalize_reference_period,
)


PROVIDERS = {
    "BLS": BlsProvider,
    "BEA": BeaProvider,
    "CENSUS": CensusProvider,
    "FRED": FredProvider,
    "SPGLOBAL": SpGlobalPmiProvider,
}


class DeterministicActualResolver:
    """Resolve event-semantic actuals from official observations, never raw macro levels."""

    def __init__(
        self,
        settings: Settings,
        *,
        providers: Mapping[str, Any] | None = None,
    ) -> None:
        self.settings = settings
        self.candidates = EventValueCandidateRepository(settings)
        self.cache = ProviderCacheRepository(settings.database_path)
        self.providers = dict(providers or PROVIDERS)

    def __call__(self, job: dict[str, Any], workspace: Path, timeout_seconds: int) -> dict[str, Any]:
        del workspace, timeout_seconds
        event_key = str(job.get("event_key") or "")
        request = job.get("request_payload") or {}
        event = request.get("event") or {}
        temporal = request.get("temporal_state") or {}
        return self.resolve_event(
            event_key=event_key,
            event=event,
            temporal_state=temporal,
            expected_period=request.get("expected_period"),
        )

    def resolve_event(
        self,
        *,
        event_key: str,
        event: dict[str, Any],
        temporal_state: dict[str, Any],
        expected_period: Any = None,
    ) -> dict[str, Any]:
        existing = (
            self.candidates.accepted_official_actual(event_key)
            if event_key
            else None
        )
        if existing is not None:
            return {
                "status": "SUCCEEDED",
                "results": [existing],
                "resolution": "persisted_candidate",
            }
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
        provider_config = self.providers.get(spec.provider)
        if provider_config is None:
            return {"status": "NO_DATA", "results": [], "error": "official_provider_adapter_unavailable"}
        provider = (
            provider_config(self.cache, self.settings)
            if isinstance(provider_config, type)
            else provider_config
        )
        expected_period = (
            event.get("reference_period")
            or event.get("period")
            or expected_period
        )
        release_timestamp = (
            temporal_state.get("release_at")
            or event.get("release_at")
            or event.get("time_utc")
        )
        release_date = _release_date(release_timestamp)
        expected_period = normalize_reference_period(
            expected_period,
            frequency=spec.frequency,
            release_date=release_date,
        )
        if expected_period is None:
            return {
                "status": "NO_DATA",
                "retryable": False,
                "results": [],
                "error": "official_reference_period_missing",
            }
        try:
            if spec.provider == "CENSUS":
                dataset = spec.source_series_id.split(":", 2)[1]
                result = asyncio.run(
                    provider.fetch(
                        period=expected_period,
                        datasets=[dataset],
                    )
                )
            elif spec.provider == "FRED":
                result = asyncio.run(
                    provider.fetch(series_ids=[spec.source_series_id])
                )
            elif spec.provider == "SPGLOBAL":
                if release_date is None:
                    return {
                        "status": "NO_DATA",
                        "retryable": False,
                        "results": [],
                        "error": "official_release_date_missing",
                    }
                result = asyncio.run(
                    provider.fetch(
                        expected_period=expected_period,
                        release_date=release_date.isoformat(),
                    )
                )
            else:
                result = asyncio.run(provider.fetch())
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
            "released_at": series.get("release_timestamp") or release_timestamp,
            "validation_timestamp": retrieved_at,
            "raw_lineage_redacted": series.get("raw_lineage_redacted"),
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
    occurrence = str(
        event.get("occurrence_id")
        or event.get("event_id")
        or event.get("canonical_event_key")
        or ""
    ).casefold()
    if "xtb:146392:2026-07-24" in occurrence:
        return "new_home_sales"
    if "xtb:146945:2026-07-24" in occurrence:
        return "flash_services_pmi"
    name = str(event.get("name") or event.get("event_name") or "").casefold()
    if "new home sales" in name or "vendite di nuove abitazioni" in name:
        return "new_home_sales"
    if "flash services pmi" in name or "pmi servizi flash" in name:
        return "flash_services_pmi"
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


def _release_date(value: Any):
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        return None
