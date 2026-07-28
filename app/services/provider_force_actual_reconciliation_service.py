from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Callable

from app.core.config import Settings
from app.services.data_freshness_service import parse_datetime
from app.services.deterministic_actual_resolver import (
    official_actual_mapping,
)
from app.services.event_driven_lifecycle_service import (
    DatumLifecycle,
    LifecycleRepository,
)
from app.services.official_actual_semantics import (
    normalize_reference_period,
)


CALENDAR_SECTIONS = (
    "critical_macro_events",
    "fed_communications",
    "other_economic_events",
)


class ProviderForceActualReconciliationService:
    """Prepare due official actual mutations for one route finalization."""

    def __init__(
        self,
        settings: Settings,
        *,
        lifecycle_resolver: Any,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.lifecycle_resolver = lifecycle_resolver
        self.clock = clock or (lambda: datetime.now(UTC))
        self.lifecycle = LifecycleRepository(settings, clock=self.clock)

    def prepare(
        self,
        contract: dict[str, Any],
    ) -> dict[str, Any]:
        now = self.clock()
        output = dict(contract)
        events = _contract_occurrences(output)
        lifecycle_by_key = {
            str(item.get("entity_key") or ""): item
            for item in self.lifecycle.list_items()
            if str(item.get("entity_type") or "").lower()
            == "macro_actual"
        }
        resolved_items: list[
            tuple[DatumLifecycle, dict[str, Any], str]
        ] = []
        canonical_reconciliations: list[dict[str, Any]] = []
        audits: list[dict[str, Any]] = []

        for occurrence_id, contract_event in events.items():
            mapping = official_actual_mapping(contract_event)
            if mapping is None:
                continue
            existing = lifecycle_by_key.get(occurrence_id)
            existing_payload = (
                dict((existing or {}).get("payload") or {})
                if isinstance((existing or {}).get("payload"), dict)
                else {}
            )
            if contract_event.get("actual") not in (None, ""):
                persisted_audit = existing_payload.get(
                    "actual_resolution"
                )
                if isinstance(persisted_audit, dict):
                    enrichment = dict(
                        contract_event.get("enrichment") or {}
                    )
                    enrichment["summary"] = {
                        **dict(enrichment.get("summary") or {}),
                        "actual_resolution": persisted_audit,
                    }
                    output = _replace_occurrence(
                        output,
                        occurrence_id=occurrence_id,
                        event={
                            **contract_event,
                            **{
                                key: existing_payload.get(key)
                                for key in (
                                    "comparison_lineage",
                                    "release_status",
                                    "status",
                                    "actual_source",
                                    "actual_source_url",
                                    "actual_is_official",
                                    "source_lineage",
                                    "acquisition_method",
                                    "freshness_state",
                                    "time_utc",
                                    "release_at",
                                )
                                if existing_payload.get(key)
                                not in (None, "")
                            },
                            "freshness_state": (
                                existing_payload.get("freshness_state")
                                or "CURRENT_RELEASE"
                            ),
                            "actual_resolution": persisted_audit,
                            "enrichment": enrichment,
                        },
                    )
                    audits.append(dict(persisted_audit))
                continue
            release = parse_datetime(
                contract_event.get("release_at")
                or contract_event.get("scheduled_at_utc")
                or contract_event.get("time_utc")
            )
            if release is None or release > now:
                continue
            payload = dict(
                (existing or {}).get("payload")
                if isinstance((existing or {}).get("payload"), dict)
                else contract_event
            )
            payload = {
                **payload,
                "occurrence_id": occurrence_id,
                "event_id": occurrence_id,
                "metric_id": mapping["metric_id"],
                "frequency": mapping["frequency"],
                "reference_period": normalize_reference_period(
                    payload.get("reference_period")
                    or contract_event.get("reference_period"),
                    frequency=mapping["frequency"],
                    release_date=release,
                ),
            }
            if _negative_cache_active(existing, now=now):
                persisted = payload.get("actual_resolution")
                if isinstance(persisted, dict):
                    output = _replace_occurrence(
                        output,
                        occurrence_id=occurrence_id,
                        event=payload,
                    )
                    audits.append(dict(persisted))
                continue

            item = {
                **dict(existing or {}),
                "entity_type": "macro_actual",
                "entity_key": occurrence_id,
                "event_at": release.isoformat(),
                "fields_attempted": ["actual"],
                "payload": payload,
                "resolution_mode": "prepare_atomic_provider_force",
            }
            result = self.lifecycle_resolver.resolve(item)
            status = str(result.get("status") or "NO_DATA").upper()
            candidate = (
                dict(result.get("candidate") or {})
                if isinstance(result.get("candidate"), dict)
                else None
            )
            resolved = (
                status == "RESOLVED"
                and isinstance(result.get("datum"), dict)
                and result["datum"].get("actual") not in (None, "")
            )
            reason_code = str(
                result.get("reason_code")
                or result.get("reason")
                or (
                    "OFFICIAL_ACTUAL_RESOLVED"
                    if resolved
                    else "OFFICIAL_ACTUAL_UNAVAILABLE"
                )
            )
            audit = {
                "occurrence_id": occurrence_id,
                "resolver_invoked": True,
                "mapping_selected": mapping["metric_id"],
                "provider_attempted": (
                    result.get("provider") or mapping["provider"]
                ),
                "source_series": (
                    result.get("source_series")
                    or mapping["source_series"]
                ),
                "provider_call_count": int(
                    result.get("provider_call_count")
                    or (
                        1
                        if result.get("provider_request_attempted")
                        else 0
                    )
                ),
                "provider_http_outcome": result.get(
                    "provider_http_outcome"
                )
                or (
                    "SUCCESS"
                    if resolved
                    else "SOURCE_UNAVAILABLE"
                ),
                "candidate_count": 1 if candidate is not None else 0,
                "candidate_validation": (
                    result.get("candidate_validation")
                    or (
                        candidate.get("validation_status")
                        if candidate
                        else "NOT_AVAILABLE"
                    )
                ),
                "reconciliation_outcome": (
                    "RELEASED" if resolved else "FAIL_CLOSED"
                ),
                "persistence_outcome": "ATOMIC_COMMIT_WITH_SNAPSHOT",
                "reason_code": reason_code,
                "retryable": bool(
                    result.get("retryable")
                    or status == "DEFERRED"
                ),
                "actual_still_missing": not resolved,
                "attempted_at": now.replace(
                    microsecond=0
                ).isoformat(),
                "canonical_write_count": 1,
                "lifecycle_write_count": 1,
            }
            if resolved:
                resolved_enrichment = dict(
                    (result["datum"].get("enrichment") or {})
                    if isinstance(
                        result["datum"].get("enrichment"), dict
                    )
                    else {}
                )
                resolved_enrichment["summary"] = {
                    **dict(resolved_enrichment.get("summary") or {}),
                    "actual_resolution": audit,
                }
                distributor = (
                    result["datum"].get("distributor")
                    or payload.get("source")
                    or payload.get("provider")
                )
                distributor_url = (
                    result["datum"].get("distributor_url")
                    or payload.get("source_url")
                )
                publisher = (
                    result["datum"].get("actual_source")
                    or result["datum"].get("source")
                )
                datum = {
                    **payload,
                    **dict(result["datum"]),
                    "occurrence_id": occurrence_id,
                    "event_id": occurrence_id,
                    "canonical_event_key": occurrence_id,
                    "reference_period": payload["reference_period"],
                    "frequency": mapping["frequency"],
                    "release_status": "RELEASED",
                    "temporal_status": "RELEASED",
                    "freshness_state": "CURRENT_RELEASE",
                    "source": distributor,
                    "source_url": distributor_url,
                    "publisher": publisher,
                    "distributor": distributor,
                    "distributor_url": distributor_url,
                    "actual_resolution": audit,
                    "enrichment": resolved_enrichment,
                }
                work_status = "COMPLETED"
            else:
                failed_enrichment = dict(
                    payload.get("enrichment") or {}
                )
                failed_enrichment["summary"] = {
                    **dict(failed_enrichment.get("summary") or {}),
                    "actual_resolution": audit,
                }
                datum = {
                    **payload,
                    "actual": None,
                    "release_status": "AWAITING_ACTUAL",
                    "actual_resolution_status": "PROVIDER_UNAVAILABLE",
                    "actual_resolution": audit,
                    "enrichment": failed_enrichment,
                }
                work_status = "BACKOFF"
            lifecycle = result.get("lifecycle")
            if isinstance(lifecycle, dict):
                lifecycle = DatumLifecycle(**lifecycle)
            if not isinstance(lifecycle, DatumLifecycle):
                raise RuntimeError(
                    "provider_force_actual_lifecycle_missing:"
                    f"{occurrence_id}:{status}:{reason_code}"
                )
            resolved_items.append((lifecycle, datum, work_status))
            canonical_reconciliations.append(
                {
                    "occurrence_id": occurrence_id,
                    "resolved": resolved,
                    "datum": datum,
                    "candidate": candidate,
                    "audit": audit,
                    "policy_version": (
                        candidate.get("policy_version")
                        if candidate
                        else None
                    ),
                }
            )
            output = _replace_occurrence(
                output,
                occurrence_id=occurrence_id,
                event=datum,
            )
            audits.append(audit)

        output["macro_actuals"] = _project_macro_actuals(output)
        deterministic_domains = dict(
            output.get("deterministic_domains") or {}
        )
        domains = dict(deterministic_domains.get("domains") or {})
        if output["macro_actuals"].get("status") == "AVAILABLE":
            domains["macro_actuals"] = {
                **dict(domains.get("macro_actuals") or {}),
                "execution_status": "SUCCEEDED",
                "data_coverage_status": "COMPLETE",
                "coverage": 1.0,
                "warnings": [],
            }
            deterministic_domains["domains"] = domains
            output["deterministic_domains"] = deterministic_domains
        data_quality = dict(output.get("data_quality") or {})
        data_quality["actual_reconciliation"] = {
            "mode": "PROVIDER_FORCE_DB_FIRST_ATOMIC",
            "occurrences": audits,
            "resolver_invocation_count": sum(
                1 for item in audits if item.get("resolver_invoked")
            ),
            "provider_call_count": sum(
                int(item.get("provider_call_count") or 0)
                for item in audits
            ),
            "canonical_write_count": sum(
                int(item.get("canonical_write_count") or 0)
                for item in audits
            ),
            "lifecycle_write_count": sum(
                int(item.get("lifecycle_write_count") or 0)
                for item in audits
            ),
            "snapshot_finalization_count": (
                1 if canonical_reconciliations else 0
            ),
            "ai_job_count": 0,
            "backend_invocation_count": 0,
        }
        output["data_quality"] = data_quality
        return {
            "contract": output,
            "resolved_items": resolved_items,
            "canonical_reconciliations": canonical_reconciliations,
            "audit": data_quality["actual_reconciliation"],
        }


def _contract_occurrences(
    contract: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    calendar = (
        contract.get("event_calendar")
        if isinstance(contract.get("event_calendar"), dict)
        else {}
    )
    selected: dict[str, dict[str, Any]] = {}
    for section in CALENDAR_SECTIONS:
        for event in calendar.get(section) or []:
            if not isinstance(event, dict):
                continue
            occurrence_id = str(
                event.get("occurrence_id")
                or event.get("event_id")
                or ""
            )
            if occurrence_id:
                selected[occurrence_id] = dict(event)
    return selected


def _replace_occurrence(
    contract: dict[str, Any],
    *,
    occurrence_id: str,
    event: dict[str, Any],
) -> dict[str, Any]:
    output = dict(contract)
    calendar = dict(output.get("event_calendar") or {})
    replaced = False
    for section in CALENDAR_SECTIONS:
        rows = list(calendar.get(section) or [])
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            row_id = str(
                row.get("occurrence_id") or row.get("event_id") or ""
            )
            if row_id != occurrence_id:
                continue
            rows[index] = {**row, **event}
            replaced = True
        calendar[section] = rows
    if not replaced:
        calendar.setdefault("other_economic_events", []).append(event)
    output["event_calendar"] = calendar
    return output


def _negative_cache_active(
    item: dict[str, Any] | None,
    *,
    now: datetime,
) -> bool:
    if not item:
        return False
    retry_at = parse_datetime(
        item.get("negative_cache_expires_at")
        or item.get("next_retry_at")
    )
    return bool(
        str(item.get("freshness_state") or "") == "NO_DATA_BACKOFF"
        and retry_at is not None
        and retry_at > now
    )


def _project_macro_actuals(
    contract: dict[str, Any],
) -> dict[str, Any]:
    existing = (
        dict(contract.get("macro_actuals") or {})
        if isinstance(contract.get("macro_actuals"), dict)
        else {}
    )
    selected = {
        str(item.get("occurrence_id") or item.get("event_id") or ""): dict(
            item
        )
        for item in existing.get("items") or []
        if isinstance(item, dict)
        and (item.get("occurrence_id") or item.get("event_id"))
    }
    for occurrence_id, event in _contract_occurrences(contract).items():
        if event.get("actual") in (None, ""):
            continue
        selected[occurrence_id] = _macro_actual_item(
            occurrence_id,
            event,
        )
    items = sorted(
        selected.values(),
        key=lambda item: str(
            item.get("release_at")
            or item.get("time_utc")
            or item.get("occurrence_id")
            or ""
        ),
    )
    if not items:
        return existing
    data_as_of = max(
        (
            str(
                item.get("reference_period")
                or item.get("release_at")
                or ""
            )
            for item in items
        ),
        default="",
    )
    return {
        **existing,
        "status": "AVAILABLE",
        "execution_status": "SUCCEEDED",
        "data_coverage_status": "COMPLETE",
        "items": items,
        "provider": "OFFICIAL_ACTUAL_RESOLVERS",
        "data_as_of": data_as_of or None,
        "freshness": "CURRENT",
        "coverage": 1.0,
        "trigger_class": "TRIGGER",
        "warnings": [],
    }


def _macro_actual_item(
    occurrence_id: str,
    event: dict[str, Any],
) -> dict[str, Any]:
    enrichment = dict(event.get("enrichment") or {})
    field_lineage = dict(enrichment.get("field_lineage") or {})
    actual_lineage = dict(field_lineage.get("actual") or {})
    summary = dict(enrichment.get("summary") or {})
    audit = (
        event.get("actual_resolution")
        if isinstance(event.get("actual_resolution"), dict)
        else summary.get("actual_resolution")
        if isinstance(summary.get("actual_resolution"), dict)
        else None
    )
    return {
        "occurrence_id": occurrence_id,
        "event_id": occurrence_id,
        "canonical_event_key": occurrence_id,
        "name": event.get("name") or event.get("event_name"),
        "country": event.get("country"),
        "category": event.get("category"),
        "date": event.get("date"),
        "release_at": event.get("release_at") or event.get("time_utc"),
        "actual": event.get("actual"),
        "forecast": (
            event.get("forecast")
            if event.get("forecast") not in (None, "")
            else enrichment.get("forecast")
        ),
        "previous": (
            event.get("previous")
            if event.get("previous") not in (None, "")
            else enrichment.get("previous")
        ),
        "reference_period": event.get("reference_period"),
        "frequency": event.get("frequency"),
        "unit": event.get("unit") or actual_lineage.get("unit"),
        "source": event.get("source") or event.get("provider"),
        "source_url": event.get("source_url"),
        "actual_source": (
            event.get("actual_source") or actual_lineage.get("source")
        ),
        "actual_source_url": (
            event.get("actual_source_url")
            or actual_lineage.get("source_url")
        ),
        "actual_is_official": True,
        "awaiting_actual": False,
        "status": "RELEASED",
        "release_status": "RELEASED",
        "enrichment": {
            "actual": event.get("actual"),
            "forecast": (
                event.get("forecast")
                if event.get("forecast") not in (None, "")
                else enrichment.get("forecast")
            ),
            "previous": (
                event.get("previous")
                if event.get("previous") not in (None, "")
                else enrichment.get("previous")
            ),
            "field_lineage": field_lineage,
            "summary": (
                {"actual_resolution": audit} if audit else {}
            ),
        },
        "actual_resolution": audit,
    }
