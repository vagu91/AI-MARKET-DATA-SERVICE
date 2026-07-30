from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.core.config import Settings
from app.services.data_freshness_service import (
    CanonicalFreshnessPolicy,
    evaluate_canonical_freshness,
    parse_datetime,
)
from app.services.deterministic_actual_resolver import (
    official_actual_mapping,
)
from app.services.event_driven_lifecycle_service import (
    DatumLifecycle,
    LifecycleRepository,
    compute_datum_lifecycle,
)
from app.services.market_fact_repository import MarketFactRepository
from app.services.observability_contract_service import TelemetryRepository
from app.services.official_actual_semantics import (
    normalize_reference_period,
)
from app.services.request_provider_accounting import (
    RequestProviderAccountingCollector,
    _observed_lifecycle_skip_flow_valid,
    provider_attempt,
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
        generation_id: str | None = None,
        coverage_write_count: int = 0,
        accounting_collector: (
            RequestProviderAccountingCollector | None
        ) = None,
        force_refresh: bool = False,
    ) -> None:
        self.settings = settings
        self.lifecycle_resolver = lifecycle_resolver
        self.clock = clock or (lambda: datetime.now(UTC))
        self.lifecycle = LifecycleRepository(settings, clock=self.clock)
        self.facts = MarketFactRepository(settings, clock=self.clock)
        self.telemetry = TelemetryRepository(settings)
        self.generation_id = generation_id
        self.coverage_write_count = max(
            int(coverage_write_count),
            0,
        )
        self.accounting_collector = accounting_collector
        self.force_refresh = force_refresh
        self.request_id = (
            accounting_collector.request_id
            if accounting_collector is not None
            else None
        )
        self.correlation_id = (
            accounting_collector.correlation_id
            if accounting_collector is not None
            else None
        )

    def prepare(
        self,
        contract: dict[str, Any],
    ) -> dict[str, Any]:
        now = self.clock()
        output = dict(contract)
        events = _contract_occurrences(output)
        canonical_events = _canonical_occurrences(
            self.facts.economic_event_records(country="US")
        )
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
            canonical_event = canonical_events.get(occurrence_id)
            if canonical_event is not None:
                contract_event = _merge_canonical_occurrence(
                    contract_event,
                    canonical_event,
                )
                output = _replace_occurrence(
                    output,
                    occurrence_id=occurrence_id,
                    event=contract_event,
                )
            mapping = official_actual_mapping(contract_event)
            if mapping is None:
                continue
            existing = _merge_canonical_lifecycle_evidence(
                lifecycle_by_key.get(occurrence_id),
                canonical_event,
                observed_at=now,
            )
            existing_payload = (
                dict((existing or {}).get("payload") or {})
                if isinstance((existing or {}).get("payload"), dict)
                else {}
            )
            lifecycle_before = _lifecycle_before(existing)
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
                    audits.append(
                        _request_scoped_database_audit(
                            persisted_audit,
                            occurrence_id=occurrence_id,
                            lifecycle_before=lifecycle_before,
                            mapping_selected=mapping["metric_id"],
                            observed_at=now,
                            value_present=True,
                            request_id=self.request_id,
                            correlation_id=self.correlation_id,
                        )
                    )
                continue
            release = parse_datetime(
                contract_event.get("release_at")
                or contract_event.get("scheduled_at_utc")
                or contract_event.get("time_utc")
            )
            if release is None or release > now:
                audits.append(
                    self._emit_decision(
                        occurrence_id=occurrence_id,
                        lifecycle_before=lifecycle_before,
                        eligibility="FUTURE_NOT_DUE",
                        reclaim_reason="occurrence_not_published",
                        resolver_invoked=False,
                        provider_attempted=False,
                        provider=mapping["provider"],
                        mapping_selected=mapping["metric_id"],
                        source_series=mapping["source_series"],
                        reconciliation_outcome="SKIPPED",
                        reason_code="OCCURRENCE_NOT_PUBLISHED",
                        observed_at=now,
                    )
                )
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
            eligibility, reclaim_reason = _eligibility(
                existing,
                now=now,
                force_refresh=self.force_refresh,
                request_id=self.request_id,
                correlation_id=self.correlation_id,
            )
            if eligibility != "RECLAIMABLE":
                persisted = payload.get("actual_resolution")
                if _same_request_provider_audit(
                    persisted,
                    request_id=self.request_id,
                    correlation_id=self.correlation_id,
                ):
                    reused_audit = {
                        **dict(persisted),
                        "occurrence_id": occurrence_id,
                        "lifecycle_before": lifecycle_before,
                        "eligibility": eligibility,
                        "reclaim_reason": reclaim_reason,
                        "mapping_selected": mapping["metric_id"],
                        "reconciliation_outcome": (
                            "SAME_REQUEST_PROVIDER_EVIDENCE_REUSED"
                        ),
                        "finalization_status": "NO_OP",
                        "canonical_write_count": 0,
                        "lifecycle_write_count": 0,
                        "coverage_write_count": 0,
                        "snapshot_write_count": 0,
                        "outbox_write_count": 0,
                    }
                    output = _replace_occurrence(
                        output,
                        occurrence_id=occurrence_id,
                        event=payload,
                    )
                    audits.append(reused_audit)
                    self._emit_prepared(reused_audit)
                    continue
                if isinstance(persisted, dict):
                    output = _replace_occurrence(
                        output,
                        occurrence_id=occurrence_id,
                        event=payload,
                    )
                audits.append(
                    self._emit_decision(
                        occurrence_id=occurrence_id,
                        lifecycle_before=lifecycle_before,
                        eligibility=eligibility,
                        reclaim_reason=reclaim_reason,
                        resolver_invoked=False,
                        provider_attempted=False,
                        provider=mapping["provider"],
                        mapping_selected=mapping["metric_id"],
                        source_series=mapping["source_series"],
                        reconciliation_outcome="SKIPPED",
                        reason_code=reclaim_reason,
                        observed_at=now,
                    )
                )
                continue

            item = {
                **dict(existing or {}),
                "entity_type": "macro_actual",
                "entity_key": occurrence_id,
                "event_at": release.isoformat(),
                "fields_attempted": ["actual"],
                "payload": payload,
                "resolution_mode": "prepare_atomic_provider_force",
                "force_refresh": self.force_refresh,
                "request_id": self.request_id,
                "correlation_id": self.correlation_id,
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
            provider_attempts = _request_scoped_provider_attempts(
                result.get("provider_attempts") or [],
                request_id=self.request_id,
                correlation_id=self.correlation_id,
                observed_at=now,
            )
            audit = {
                "occurrence_id": occurrence_id,
                "request_id": self.request_id,
                "correlation_id": self.correlation_id,
                "lifecycle_before": lifecycle_before,
                "eligibility": eligibility,
                "reclaim_reason": reclaim_reason,
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
                "provider_request_attempted": bool(
                    result.get("provider_request_attempted")
                ),
                "provider_attempts": provider_attempts,
                "provider_negative_cache_hit": bool(
                    result.get("provider_negative_cache_hit")
                ),
                "provider_negative_cache_bypassed": bool(
                    result.get("provider_negative_cache_bypassed")
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
                "coverage_write_count": self.coverage_write_count,
                "snapshot_write_count": 1,
                "outbox_write_count": 1,
                "generation_id": self.generation_id,
                "finalization_status": "PREPARED",
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
                    result["datum"].get("publisher")
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
                work_status = (
                    "EXHAUSTED_NO_DATA"
                    if status == "EXHAUSTED_NO_DATA"
                    or bool(result.get("retry_deadline_exhausted"))
                    else "BACKOFF"
                )
            lifecycle = result.get("lifecycle")
            if isinstance(lifecycle, dict):
                lifecycle = DatumLifecycle(**lifecycle)
            if not isinstance(lifecycle, DatumLifecycle):
                lifecycle = _failure_lifecycle(
                    self.settings,
                    occurrence_id=occurrence_id,
                    payload=datum,
                    existing=existing,
                    now=now,
                    reason_code=reason_code,
                    terminal=work_status == "EXHAUSTED_NO_DATA",
                )
            datum = {
                **datum,
                "valid_until": lifecycle.valid_until,
                "next_refresh_at": lifecycle.next_refresh_at,
                "content_valid_until": lifecycle.valid_until,
                "refresh_due_at": lifecycle.next_refresh_at,
            }
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
            self._emit_prepared(audit)

        if audits or canonical_reconciliations:
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
        reconciliation_audit = {
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
        if audits or canonical_reconciliations:
            data_quality["actual_reconciliation"] = (
                reconciliation_audit
            )
            output["data_quality"] = data_quality
        self._record_flash_pmi_accounting(
            contract=output,
            audits=audits,
        )
        return {
            "contract": output,
            "resolved_items": resolved_items,
            "canonical_reconciliations": canonical_reconciliations,
            "audit": reconciliation_audit,
        }

    def _record_flash_pmi_accounting(
        self,
        *,
        contract: dict[str, Any],
        audits: list[dict[str, Any]],
    ) -> None:
        collector = self.accounting_collector
        if collector is None:
            return
        occurrences_by_id = {
            occurrence_id: event
            for occurrence_id, event in _contract_occurrences(
                contract
            ).items()
            if (
                (official_actual_mapping(event) or {}).get(
                    "metric_id"
                )
                == "flash_services_pmi"
            )
        }
        occurrences = list(occurrences_by_id.values())
        relevant = [
            item
            for item in audits
            if item.get("mapping_selected") == "flash_services_pmi"
        ]
        observed_at = parse_datetime(
            getattr(
                self,
                "clock",
                lambda: datetime.now(UTC),
            )()
        ) or datetime.now(UTC)
        target = _latest_flash_services_occurrence(
            occurrences,
            observed_at=observed_at,
        )
        target_id = str(
            (target or {}).get("occurrence_id")
            or (target or {}).get("event_id")
            or ""
        )
        target_audit = next(
            (
                item
                for item in relevant
                if str(item.get("occurrence_id") or "")
                == target_id
            ),
            None,
        )
        raw_attempts = [
            dict(attempt)
            for attempt in (
                (target_audit or {}).get("provider_attempts")
                or []
            )
            if isinstance(attempt, dict)
        ]
        attempts_by_provider = {
            str(item.get("provider") or ""): item
            for item in raw_attempts
        }
        provider_calls = int(
            (target_audit or {}).get("provider_call_count")
            or 0
        )
        selected = (
            target
            if target
            and target.get("actual") not in (None, "")
            else None
        )
        lifecycle_before = (
            (target_audit or {}).get("lifecycle_before")
            if isinstance(
                (target_audit or {}).get("lifecycle_before"),
                dict,
            )
            else None
        )
        db_found = lifecycle_before is not None
        data_as_of = (
            target.get("release_at")
            or target.get("reference_period")
            if target
            else None
        )
        database_row = (
            {
                "database_data_as_of": data_as_of,
                "database_content_valid_until": (
                    lifecycle_before.get("valid_until")
                ),
                "database_refresh_due_at": (
                    lifecycle_before.get("next_refresh_at")
                ),
                "database_lifecycle_status": lifecycle_before.get(
                    "freshness_state"
                ),
            }
            if db_found and lifecycle_before
            else None
        )
        database_decision = evaluate_canonical_freshness(
            database_row,
            policy=CanonicalFreshnessPolicy(
                max_age=timedelta(days=45),
                data_reference_mode="official_release",
            ),
            observed_at=observed_at,
        )
        database_valid = bool(
            db_found and database_decision.usable
        )
        if database_valid:
            primary = provider_attempt(
                "SPGLOBAL",
                called=False,
                attempts=0,
                result="NOT_CALLED",
                not_called_reason="VALID_DATABASE_RECORD_SELECTED",
                execution_origin="CACHE_DECISION",
            )
            fallback = provider_attempt(
                "INVESTING_EVENT_1062",
                called=False,
                attempts=0,
                result="NOT_CALLED",
                not_called_reason="VALID_DATABASE_RECORD_SELECTED",
                execution_origin="CACHE_DECISION",
            )
        else:
            primary = _actual_attempt(
                "SPGLOBAL",
                attempts_by_provider.get("SPGLOBAL"),
                skipped_reason=(
                    "NO_FLASH_SERVICES_PMI_OCCURRENCE"
                    if not occurrences
                    else "PROVIDER_ATTEMPT_EVIDENCE_MISSING"
                ),
            )
            fallback = _actual_attempt(
                "INVESTING_EVENT_1062",
                attempts_by_provider.get("INVESTING_EVENT_1062"),
                skipped_reason=(
                    "NO_FLASH_SERVICES_PMI_OCCURRENCE"
                    if not occurrences
                    else "PRIMARY_SUCCEEDED"
                    if attempts_by_provider.get(
                        "SPGLOBAL", {}
                    ).get("result")
                    == "SUCCESS"
                    else "PROVIDER_ATTEMPT_EVIDENCE_MISSING"
                ),
            )
        evidence_complete = bool(
            target is not None
            and target_audit is not None
            and _audit_correlated(
                target_audit,
                request_id=collector.request_id,
                correlation_id=collector.correlation_id,
            )
            and database_decision.complete
            and (
                database_valid
                or _actual_provider_chain_complete(
                    raw_attempts,
                    provider_calls=provider_calls,
                    request_id=collector.request_id,
                    correlation_id=collector.correlation_id,
                )
            )
        )
        collector.record(
            "flash_services_pmi",
            acquisition_id=(
                "flash_services_pmi_actual_resolution:"
                f"{target_id or 'NO_OCCURRENCE'}"
            ),
            shared_dataset_ids=("flash_services_pmi",),
            database_lookup_performed=True,
            database_lookup_reason=(
                "OFFICIAL_ACTUAL_CANDIDATE_DATABASE_LOOKUP"
            ),
            database_record_found=db_found,
            database_data_as_of=data_as_of if db_found else None,
            database_content_valid_until=(
                database_decision.content_valid_until
                if db_found
                else None
            ),
            database_refresh_due_at=(
                database_decision.refresh_due_at
                if db_found
                else None
            ),
            database_lifecycle_status=(
                lifecycle_before.get("freshness_state")
                if lifecycle_before
                else None
            ),
            database_record_expired=(
                database_decision.expired
                if db_found
                else False
            ),
            database_freshness_evaluation=(
                database_decision.evaluation
                if db_found
                else "NOT_FOUND"
            ),
            primary_provider=primary,
            fallbacks=[fallback],
            acquisition_selected_source=(
                (target_audit or {}).get("provider_attempted")
                or selected.get("actual_source")
                or selected.get("acquisition_provider")
                if selected
                else None
            ),
            acquisition_reason_code=(
                "FLASH_SERVICES_PMI_DELIVERABLE_ACQUIRED"
                if selected
                else "FLASH_SERVICES_PMI_NOT_DUE"
                if target is None
                else "FLASH_SERVICES_PMI_ALL_PROVIDERS_FAILED"
                if _provider_chain_all_failed(raw_attempts)
                else "FLASH_SERVICES_PMI_VALUE_NOT_AVAILABLE"
            ),
            observed_at=observed_at,
            evidence_complete=evidence_complete,
        )

    def _emit_prepared(self, audit: dict[str, Any]) -> None:
        try:
            self.telemetry.emit(
                "provider_force_actual_reconciliation",
                identifiers={
                    "generation_id": self.generation_id,
                    "occurrence_id": audit.get("occurrence_id"),
                },
                decision_summary=str(
                    audit.get("reconciliation_outcome")
                    or "provider force reconciliation prepared"
                ),
                stop_reason=str(
                    audit.get("reason_code") or "PREPARED"
                ),
                payload=audit,
            )
        except Exception:
            # Observability must never change provider-force correctness.
            return

    def _emit_decision(
        self,
        *,
        occurrence_id: str,
        lifecycle_before: dict[str, Any] | None,
        eligibility: str,
        reclaim_reason: str,
        resolver_invoked: bool,
        provider_attempted: bool,
        provider: str,
        mapping_selected: str,
        source_series: str | None,
        reconciliation_outcome: str,
        reason_code: str,
        observed_at: datetime,
    ) -> dict[str, Any]:
        providers = [provider]
        if mapping_selected == "flash_services_pmi":
            providers.append("INVESTING_EVENT_1062")
        provider_attempts = _request_scoped_provider_attempts(
            [
                {
                    "provider": item,
                    "called": False,
                    "attempts": 0,
                    "result": "NOT_CALLED",
                    "not_called_reason": reason_code,
                    "execution_origin": "OBSERVED_SKIP",
                }
                for item in providers
            ],
            request_id=self.request_id,
            correlation_id=self.correlation_id,
            observed_at=observed_at,
        )
        audit = {
            "occurrence_id": occurrence_id,
            "request_id": self.request_id,
            "correlation_id": self.correlation_id,
            "lifecycle_before": lifecycle_before,
            "eligibility": eligibility,
            "reclaim_reason": reclaim_reason,
            "resolver_invoked": resolver_invoked,
            "mapping_selected": mapping_selected,
            "provider_attempted": provider_attempted,
            "provider_call_count": 0,
            "provider_request_attempted": False,
            "provider_attempts": provider_attempts,
            "provider_http_outcome": "NOT_CALLED",
            "source_series": source_series,
            "reconciliation_outcome": reconciliation_outcome,
            "generation_id": self.generation_id,
            "finalization_status": "NO_OP",
            "canonical_write_count": 0,
            "lifecycle_write_count": 0,
            "coverage_write_count": 0,
            "snapshot_write_count": 0,
            "outbox_write_count": 0,
            "reason_code": reason_code,
            "attempted_at": observed_at.replace(
                microsecond=0
            ).isoformat(),
        }
        self._emit_prepared(audit)
        return audit


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


def _canonical_occurrences(
    records: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        occurrence_id = str(
            record.get("occurrence_id")
            or record.get("event_id")
            or ""
        )
        if occurrence_id:
            selected[occurrence_id] = dict(record)
    return selected


def _merge_canonical_occurrence(
    projected: dict[str, Any],
    canonical: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(projected)
    for field, value in canonical.items():
        if value not in (None, "", [], {}):
            merged[field] = value
    projected_enrichment = (
        dict(projected.get("enrichment") or {})
        if isinstance(projected.get("enrichment"), dict)
        else {}
    )
    canonical_enrichment = (
        dict(canonical.get("enrichment") or {})
        if isinstance(canonical.get("enrichment"), dict)
        else {}
    )
    merged["enrichment"] = {
        **projected_enrichment,
        **{
            field: value
            for field, value in canonical_enrichment.items()
            if value not in (None, "", [], {})
        },
    }
    return merged


def _merge_canonical_lifecycle_evidence(
    lifecycle: dict[str, Any] | None,
    canonical: dict[str, Any] | None,
    *,
    observed_at: datetime,
) -> dict[str, Any] | None:
    if canonical is None:
        return lifecycle
    output = dict(lifecycle or {})
    prior_payload = (
        dict(output.get("payload") or {})
        if isinstance(output.get("payload"), dict)
        else {}
    )
    payload = _merge_canonical_occurrence(
        prior_payload,
        canonical,
    )
    output["payload"] = payload
    for target, candidates in {
        "valid_until": (
            "content_valid_until",
            "valid_until",
        ),
        "next_refresh_at": (
            "refresh_due_at",
            "next_refresh_at",
        ),
        "next_retry_at": ("next_retry_at",),
        "negative_cache_expires_at": (
            "negative_cache_expires_at",
        ),
    }.items():
        value = next(
            (
                payload.get(field)
                for field in candidates
                if payload.get(field) not in (None, "")
            ),
            None,
        )
        if value is not None:
            output[target] = value
    if (
        not lifecycle
        or str(lifecycle.get("work_status") or "").upper()
        == "SUPERSEDED"
    ):
        actual = payload.get("actual")
        audit = payload.get("actual_resolution")
        valid_until = parse_datetime(output.get("valid_until"))
        refresh_due_at = parse_datetime(
            output.get("next_refresh_at")
        )
        if actual not in (None, ""):
            output["freshness_state"] = "CURRENT_RELEASE"
            output["work_status"] = "COMPLETED"
        elif (
            isinstance(audit, dict)
            and audit.get("actual_still_missing") is True
            and valid_until is not None
            and refresh_due_at is not None
            and valid_until > observed_at
            and refresh_due_at > observed_at
        ):
            output["freshness_state"] = "FRESH_NO_DATA"
            output["work_status"] = "COMPLETED"
        output["superseded_by"] = None
    return output


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


def _actual_attempt(
    provider: str,
    raw: dict[str, Any] | None,
    *,
    skipped_reason: str,
) -> dict[str, Any]:
    if raw is not None:
        if raw.get("called") is False:
            attempt = provider_attempt(
                provider,
                called=False,
                attempts=0,
                result=str(raw.get("result") or "NOT_CALLED"),
                not_called_reason=(
                    str(raw.get("not_called_reason") or "")
                    or skipped_reason
                ),
                execution_origin=str(
                    raw.get("execution_origin")
                    or "OBSERVED_SKIP"
                ),
            )
        else:
            attempt = provider_attempt(
                provider,
                called=True,
                attempts=max(int(raw.get("attempts") or 0), 1),
                result=str(raw.get("result") or "UNKNOWN"),
                execution_origin="PROVIDER_CALL",
            )
        attempt.update(
            {
                key: raw.get(key)
                for key in (
                    "request_id",
                    "correlation_id",
                    "observed_at",
                )
                if raw.get(key) not in (None, "")
            }
        )
        return attempt
    return provider_attempt(
        provider,
        called=False,
        attempts=0,
        result="NOT_CALLED",
        not_called_reason=skipped_reason,
        execution_origin="OBSERVED_SKIP",
    )


def _request_scoped_provider_attempts(
    attempts: Any,
    *,
    request_id: str | None,
    correlation_id: str | None,
    observed_at: datetime,
) -> list[dict[str, Any]]:
    timestamp = observed_at.replace(microsecond=0).isoformat()
    output: list[dict[str, Any]] = []
    for raw in attempts if isinstance(attempts, (list, tuple)) else []:
        if not isinstance(raw, dict) or not raw.get("provider"):
            continue
        attempt_count = max(int(raw.get("attempts") or 0), 0)
        called = (
            raw.get("called")
            if type(raw.get("called")) is bool
            else attempt_count > 0
        )
        execution_origin = str(
            raw.get("execution_origin")
            or ("PROVIDER_CALL" if called else "OBSERVED_SKIP")
        )
        normalized = provider_attempt(
            str(raw["provider"]),
            called=called,
            attempts=max(attempt_count, 1) if called else 0,
            result=str(raw.get("result") or "UNKNOWN"),
            not_called_reason=(
                None
                if called
                else str(
                    raw.get("not_called_reason")
                    or "PROVIDER_NOT_CALLED"
                )
            ),
            execution_origin=execution_origin,
        )
        normalized.update(
            {
                "request_id": request_id,
                "correlation_id": correlation_id,
                "observed_at": str(raw.get("observed_at") or timestamp),
            }
        )
        output.append(normalized)
    return output


def _latest_flash_services_occurrence(
    occurrences: list[dict[str, Any]],
    *,
    observed_at: datetime,
) -> dict[str, Any] | None:
    if not occurrences:
        return None
    floor = datetime.min.replace(tzinfo=UTC)
    ceiling = datetime.max.replace(tzinfo=UTC)
    released = [
        item
        for item in occurrences
        if (
            parse_datetime(
                item.get("release_at")
                or item.get("scheduled_at_utc")
                or item.get("time_utc")
            )
            or ceiling
        )
        <= observed_at
    ]
    if released:
        return max(
            released,
            key=lambda item: (
                parse_datetime(
                    item.get("release_at")
                    or item.get("scheduled_at_utc")
                    or item.get("time_utc")
                )
                or floor,
                str(
                    item.get("occurrence_id")
                    or item.get("event_id")
                    or ""
                ),
            ),
        )
    return min(
        occurrences,
        key=lambda item: (
            parse_datetime(
                item.get("release_at")
                or item.get("scheduled_at_utc")
                or item.get("time_utc")
            )
            or ceiling,
            str(
                item.get("occurrence_id")
                or item.get("event_id")
                or ""
            ),
        ),
    )


def _actual_provider_chain_complete(
    attempts: list[dict[str, Any]],
    *,
    provider_calls: int,
    request_id: str,
    correlation_id: str,
) -> bool:
    providers = [
        str(item.get("provider") or "")
        for item in attempts
    ]
    if providers not in (
        ["SPGLOBAL"],
        ["SPGLOBAL", "INVESTING_EVENT_1062"],
    ):
        return False
    if any(
        item.get("request_id") != request_id
        or item.get("correlation_id") != correlation_id
        or parse_datetime(item.get("observed_at")) is None
        for item in attempts
    ):
        return False
    if (
        providers == ["SPGLOBAL", "INVESTING_EVENT_1062"]
        and provider_calls == 0
        and _observed_lifecycle_skip_flow_valid(attempts)
    ):
        return True
    by_provider = {
        str(item.get("provider") or ""): item
        for item in attempts
    }
    primary = by_provider.get("SPGLOBAL")
    if primary is None or primary.get("called") is not True:
        return False
    observed_calls = sum(
        max(int(item.get("attempts") or 0), 1)
        for item in attempts
        if item.get("called") is not False
    )
    if provider_calls != observed_calls:
        return False
    primary_succeeded = _actual_result_succeeded(
        primary.get("result")
    )
    fallback = by_provider.get("INVESTING_EVENT_1062")
    if primary_succeeded:
        return fallback is None or fallback.get("called") is False
    return bool(
        fallback is not None
        and fallback.get("called") is True
    )


def _provider_chain_all_failed(
    attempts: list[dict[str, Any]],
) -> bool:
    return bool(
        [
            str(item.get("provider") or "")
            for item in attempts
        ]
        == ["SPGLOBAL", "INVESTING_EVENT_1062"]
        and all(
            item.get("called") is True
            and not _actual_result_succeeded(item.get("result"))
            for item in attempts
        )
    )


def _audit_correlated(
    audit: dict[str, Any],
    *,
    request_id: str,
    correlation_id: str,
) -> bool:
    return bool(
        audit.get("request_id") == request_id
        and audit.get("correlation_id") == correlation_id
    )


def _actual_result_succeeded(value: Any) -> bool:
    result = str(value or "").upper()
    return bool(
        any(
            token in result
            for token in ("SUCCESS", "FOUND", "AVAILABLE", "VALID")
        )
        and not any(
            token in result
            for token in (
                "FAIL",
                "ERROR",
                "NO_DATA",
                "NOT_AVAILABLE",
                "UNAVAILABLE",
                "TIMEOUT",
            )
        )
    )


def _request_scoped_database_audit(
    _persisted: dict[str, Any],
    *,
    occurrence_id: str,
    lifecycle_before: dict[str, Any] | None,
    mapping_selected: str,
    observed_at: datetime,
    value_present: bool,
    request_id: str | None,
    correlation_id: str | None,
) -> dict[str, Any]:
    return {
        "occurrence_id": occurrence_id,
        "request_id": request_id,
        "correlation_id": correlation_id,
        "lifecycle_before": lifecycle_before,
        "eligibility": "VALID_DATABASE_RECORD",
        "reclaim_reason": "CANONICAL_ACTUAL_PRESENT",
        "resolver_invoked": False,
        "mapping_selected": mapping_selected,
        "provider_attempted": False,
        "provider_call_count": 0,
        "provider_request_attempted": False,
        "provider_attempts": [],
        "provider_http_outcome": "NOT_CALLED_DATABASE_SELECTED",
        "candidate_count": 0,
        "candidate_validation": "NOT_REQUESTED",
        "reconciliation_outcome": "DATABASE_SELECTED",
        "reason_code": (
            "VALID_CANONICAL_ACTUAL_SELECTED"
            if value_present
            else "CANONICAL_ACTUAL_NOT_AVAILABLE"
        ),
        "actual_still_missing": not value_present,
        "attempted_at": observed_at.replace(
            microsecond=0
        ).isoformat(),
        "canonical_write_count": 0,
        "lifecycle_write_count": 0,
        "coverage_write_count": 0,
        "snapshot_write_count": 0,
        "outbox_write_count": 0,
        "finalization_status": "NO_OP",
        "database_lookup_observed_at": observed_at.replace(
            microsecond=0
        ).isoformat(),
    }


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


def _eligibility(
    item: dict[str, Any] | None,
    *,
    now: datetime,
    force_refresh: bool = False,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> tuple[str, str]:
    if not item:
        return "RECLAIMABLE", "MISSING_LIFECYCLE_INITIALIZATION"
    freshness = str(
        item.get("freshness_state") or ""
    ).upper()
    work_status = str(item.get("work_status") or "").upper()
    if (
        freshness == "EXHAUSTED_NO_DATA"
        or work_status == "EXHAUSTED_NO_DATA"
    ):
        return "EXHAUSTED_NO_DATA", "TERMINAL_NO_DATA"
    if _negative_cache_active(item, now=now):
        persisted = (
            item.get("payload", {}).get("actual_resolution")
            if isinstance(item.get("payload"), dict)
            else None
        )
        if force_refresh and not _same_request_provider_audit(
            persisted,
            request_id=request_id,
            correlation_id=correlation_id,
        ):
            return (
                "RECLAIMABLE",
                "FORCE_REFRESH_PRIOR_REQUEST_NEGATIVE_CACHE_BYPASSED",
            )
        return "BACKOFF_ACTIVE", "NEXT_RETRY_IN_FUTURE"
    valid_until = parse_datetime(item.get("valid_until"))
    next_refresh = parse_datetime(item.get("next_refresh_at"))
    if (
        freshness in {"NO_DATA", "NO_DATA_FRESH"}
        and (
            (valid_until is not None and valid_until > now)
            or (next_refresh is not None and next_refresh > now)
        )
    ):
        return "FRESH_NO_DATA", "NO_DATA_STILL_FRESH"
    return "RECLAIMABLE", "STALE_NO_DATA_RETRY_DUE"


def _same_request_provider_audit(
    value: Any,
    *,
    request_id: str | None,
    correlation_id: str | None,
) -> bool:
    return bool(
        request_id
        and correlation_id
        and isinstance(value, dict)
        and value.get("request_id") == request_id
        and value.get("correlation_id") == correlation_id
        and isinstance(value.get("provider_attempts"), list)
        and value.get("provider_attempts")
    )


def _lifecycle_before(
    item: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not item:
        return None
    return {
        key: item.get(key)
        for key in (
            "freshness_state",
            "work_status",
            "attempt_count",
            "valid_until",
            "next_refresh_at",
            "next_retry_at",
            "negative_cache_expires_at",
            "refresh_reason",
        )
    }


def _failure_lifecycle(
    settings: Settings,
    *,
    occurrence_id: str,
    payload: dict[str, Any],
    existing: dict[str, Any] | None,
    now: datetime,
    reason_code: str,
    terminal: bool,
) -> DatumLifecycle:
    lifecycle = compute_datum_lifecycle(
        "macro_actual",
        occurrence_id,
        payload,
        settings=settings,
        now=now,
        attempt_count=int((existing or {}).get("attempt_count") or 0)
        + 1,
        no_data=not terminal,
        fields_attempted=["actual"],
        retry_class=(
            "EXHAUSTED_NO_DATA" if terminal else "PROVIDER_TEMPORARY"
        ),
        refresh_reason=reason_code,
    )
    if not terminal:
        return lifecycle
    return replace(
        lifecycle,
        freshness_state="EXHAUSTED_NO_DATA",
        next_refresh_at=None,
        next_retry_at=None,
        negative_cache_key=None,
        negative_cache_expires_at=None,
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
        "provider_event_id": event.get("provider_event_id"),
        "provider_occurrence_id": event.get("provider_occurrence_id"),
        "name": event.get("name") or event.get("event_name"),
        "country": event.get("country"),
        "category": event.get("category"),
        "metric_id": event.get("metric_id"),
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
        "actual_is_official": (
            event.get("actual_is_official")
            if event.get("actual_is_official") is not None
            else True
        ),
        "freshness_state": event.get("freshness_state"),
        "valid_until": (
            event.get("valid_until")
            or event.get("content_valid_until")
        ),
        "content_valid_until": (
            event.get("content_valid_until")
            or event.get("valid_until")
        ),
        "next_refresh_at": (
            event.get("next_refresh_at")
            or event.get("refresh_due_at")
        ),
        "awaiting_actual": False,
        "status": "RELEASED",
        "release_status": "RELEASED",
        "field_lineage": field_lineage,
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
