from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import asyncio
from datetime import UTC, datetime
from datetime import timedelta
from pathlib import Path
from time import perf_counter
from typing import Any

from app.core.config import Settings
from app.providers.base import redact_sensitive
from app.services.market_fact_repository import now_iso
from app.services.data_integrity_service import reject_future_actual
from app.services.ai_research_validation_service import ValidationRequest, validate_ai_research_result
from app.services.ai_research_diagnostics import AIResearchDiagnostics
from app.services.codex_runtime_contract import (
    build_codex_exec_command,
    canonicalize_workspace,
    classify_codex_failure,
    inherited_instruction_files,
    legacy_research_output_schema,
    safe_subprocess_environment,
    validate_isolated_command,
    validate_output_schema,
    validate_payload,
)

VALUE_FIELDS = ("forecast", "previous", "consensus", "actual")
PRIMARY_METRIC_IDS = {
    "CPI": ("headline_cpi_mom",),
    "PPI": ("headline_ppi_mom", "ppi_final_demand_mom", "final_demand_ppi_mom"),
    "PCE": ("headline_pce_mom",),
    "GDP": ("real_gdp_annualized_qoq", "real_gdp_qoq_saar"),
    "NFP": ("nonfarm_payrolls_change",),
}
FORBIDDEN_TRADING_TERMS = {
    "buy", "sell", "long", "short", "no_trade", "entry", "target", "stop", "recommendation",
}
TRADING_TEXT_FIELDS = {"notes", "fomc_context"}


class AIResearcherProvider:
    source = "AI Researcher"
    provider_type = "AI_RESEARCHER"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def research(self, events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if not self.settings.enable_ai_researcher:
            return [], {"status": "skipped", "warning": "ai_researcher_disabled"}
        selected = events[: min(self.settings.ai_researcher_max_events, self.settings.ai_researcher_max_macro_events)]
        if self.settings.ai_researcher_mode == "codex_cli":
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(self._codex_cli, selected),
                    timeout=max(float(self.settings.timeout_ai_research_seconds), 1.0),
                )
            except TimeoutError:
                return [], {
                    "status": "provider_failed",
                    "failure_reason": "ai_research_timeout",
                    "timeout_seconds": self.settings.timeout_ai_research_seconds,
                }
        if self.settings.ai_researcher_mode == "openai_api":
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(self._openai_api, selected),
                    timeout=max(float(self.settings.timeout_ai_research_seconds), 1.0),
                )
            except TimeoutError:
                return [], {
                    "status": "provider_failed",
                    "failure_reason": "ai_research_timeout",
                    "timeout_seconds": self.settings.timeout_ai_research_seconds,
                }
        return [], {"status": "provider_unavailable", "error": f"unsupported_mode:{self.settings.ai_researcher_mode}"}

    def _codex_cli(self, events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        diagnostics = AIResearchDiagnostics(self.settings)
        try:
            workspace = canonicalize_workspace(
                Path(self.settings.codex_workspace_dir)
            )
        except (OSError, ValueError) as exc:
            return [], {
                "status": "provider_failed",
                "failure_reason": "codex_path_invalid",
                "error_category": "PATH_INVALID",
                "error": redact_sensitive(str(exc)),
                "retry_classification": "NON_RETRYABLE",
            }
        if inherited_instruction_files(workspace):
            return [], {
                "status": "provider_failed",
                "failure_reason": "codex_workspace_inherits_agent_instructions",
                "retry_classification": "NON_RETRYABLE",
            }
        input_path = workspace / "research_input.json"
        output_path = workspace / "research_output.json"
        schema_path = workspace / "research_output_schema.json"
        diagnostics_path = workspace / "last_codex_run.json"
        research_input = {"generated_at": now_iso(), "events": events}
        input_path.write_text(json.dumps(research_input, indent=2, default=str, ensure_ascii=False), encoding="utf-8")
        prompt_path = Path("app/prompts/ai_research_macro_event.md")
        prompt = prompt_path.read_text(encoding="utf-8") if prompt_path.exists() else "Research events and write research_output.json."
        final_prompt = build_codex_research_prompt(prompt, research_input)
        command_prefix = _resolve_command(self.settings.codex_cli_command)
        schema = legacy_research_output_schema()
        try:
            validate_output_schema(schema)
        except ValueError as exc:
            return [], {
                "status": "provider_failed",
                "failure_reason": "codex_output_schema_invalid",
                "error": redact_sensitive(str(exc)),
                "retry_classification": "NON_RETRYABLE",
            }
        schema_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")
        output_path.unlink(missing_ok=True)
        command = build_codex_exec_command(
            command_prefix or [self.settings.codex_cli_command],
            workspace=workspace,
            schema_path=schema_path,
            output_path=output_path,
        )
        validate_isolated_command(command, final_prompt, cwd=workspace)
        started = perf_counter()
        cwd = workspace
        run_info: dict[str, Any] = {
            "command": _safe_command(command),
            "cwd": str(cwd),
            "input_path": str(input_path.resolve()),
            "output_path": str(output_path.resolve()),
            "status": "started",
            **_prompt_diagnostics(final_prompt, research_input),
            "web_search_enabled": True,
            "web_search_note": "Codex CLI web search is explicitly enabled for this isolated invocation.",
            "user_config_ignored": True,
            "rules_ignored": True,
            "sandbox": "read-only",
        }
        if diagnostics.enabled:
            for event in events:
                event_id = event.get("event_id")
                diagnostics.event_json(event_id, "input.json", research_input)
                diagnostics.event_json(
                    event_id,
                    "command.json",
                    {"command": _safe_command(command), "cwd": str(cwd), "timeout_seconds": self.settings.codex_research_timeout_seconds},
                )
            diagnostics.write_json("run_summary.json", {**run_info, "started_at": now_iso(), "events": events})
        if (
            run_info["prompt_length_chars"] <= 2000
            or run_info["prompt_line_count"] <= 20
            or not run_info["prompt_contains_input"]
        ):
            run_info.update(
                {
                    "status": "provider_failed",
                    "failure_reason": "codex_prompt_incomplete",
                    "error": "Prompt did not include enough instructions and input JSON.",
                }
            )
            diagnostics_path.write_text(json.dumps(run_info, indent=2, default=str), encoding="utf-8")
            _record_diagnostic_failure(diagnostics, events, run_info)
            return [], run_info
        try:
            completed = subprocess.run(
                command,
                cwd=cwd,
                input=final_prompt,
                timeout=self.settings.codex_research_timeout_seconds,
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=safe_subprocess_environment(),
            )
        except subprocess.TimeoutExpired as exc:
            run_info.update(
                {
                    "status": "timeout",
                    "duration_ms": int((perf_counter() - started) * 1000),
                    "timeout_seconds": self.settings.codex_research_timeout_seconds,
                    "stdout": _brief(exc.stdout),
                    "stderr": _brief(exc.stderr),
                    "error": redact_sensitive(str(exc)),
                }
            )
            diagnostics_path.write_text(json.dumps(run_info, indent=2, default=str), encoding="utf-8")
            _record_diagnostic_failure(diagnostics, events, run_info)
            return [], {**run_info, "status": "provider_failed", "failure_reason": "codex_cli_timeout"}
        except (FileNotFoundError, PermissionError, OSError) as exc:
            category, retryable = classify_codex_failure(
                exit_code=None,
                stderr=str(exc),
            )
            run_info.update(
                {
                    "status": "provider_unavailable",
                    "duration_ms": int((perf_counter() - started) * 1000),
                    "error": redact_sensitive(str(exc)),
                    "failure_reason": (
                        "codex_path_invalid"
                        if category == "PATH_INVALID"
                        else "codex_cli_unavailable"
                    ),
                    "error_category": category,
                    "retry_classification": (
                        "RETRYABLE" if retryable else "NON_RETRYABLE"
                    ),
                }
            )
            diagnostics_path.write_text(json.dumps(run_info, indent=2, default=str), encoding="utf-8")
            _record_diagnostic_failure(diagnostics, events, run_info)
            return [], run_info
        duration_ms = int((perf_counter() - started) * 1000)
        run_info.update(
            {
                "duration_ms": duration_ms,
                "exit_code": completed.returncode,
                "stdout": _brief(completed.stdout),
                "stderr": _brief(completed.stderr),
                "stdout_length": len(completed.stdout or ""),
            }
        )
        if diagnostics.enabled:
            for event in events:
                diagnostics.event_text(event.get("event_id"), "stdout.txt", completed.stdout)
                diagnostics.event_text(event.get("event_id"), "stderr.txt", completed.stderr)
        if completed.returncode != 0:
            run_info.update(
                {
                    "status": "provider_failed",
                    "error": redact_sensitive(completed.stderr or completed.stdout),
                    "failure_reason": "codex_cli_non_zero_exit",
                }
            )
            diagnostics_path.write_text(json.dumps(run_info, indent=2, default=str), encoding="utf-8")
            _record_diagnostic_failure(diagnostics, events, run_info)
            return [], run_info
        if output_path.exists():
            try:
                loaded = json.loads(output_path.read_text(encoding="utf-8"))
                payload = loaded if isinstance(loaded, dict) else None
                parse_error = None if payload is not None else "output_file_json_not_object"
            except (OSError, json.JSONDecodeError) as exc:
                payload = None
                parse_error = f"output_file_invalid:{type(exc).__name__}:{exc}"
        else:
            payload = None
            parse_error = "structured_output_file_missing"
        if payload is None:
            generic_failure = _generic_non_research_response(completed.stdout)
            run_info.update(
                {
                    "status": "provider_failed",
                    "error": parse_error,
                    "failure_reason": "codex_did_not_execute_research" if generic_failure else "codex_stdout_json_parse_failed",
                    "json_found": False,
                    "parsed_result_count": 0,
                    "validation_errors": [parse_error],
                }
            )
            diagnostics_path.write_text(json.dumps(run_info, indent=2, default=str), encoding="utf-8")
            _record_diagnostic_failure(diagnostics, events, run_info)
            return [], run_info
        try:
            validate_payload(payload, schema)
        except ValueError as exc:
            run_info.update(
                {
                    "status": "provider_failed",
                    "error": redact_sensitive(str(exc)),
                    "failure_reason": "codex_output_contract_incompatible",
                    "retry_classification": "NON_RETRYABLE",
                }
            )
            diagnostics_path.write_text(
                json.dumps(run_info, indent=2, default=str),
                encoding="utf-8",
            )
            _record_diagnostic_failure(diagnostics, events, run_info)
            return [], run_info
        if not output_path.exists():
            output_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        facts, status = self.load_payload(payload)
        _record_diagnostic_success(diagnostics, self.settings, events, payload, facts, status)
        run_info.update(status)
        if diagnostics.artifact_dir:
            run_info["diagnostic_artifact_dir"] = diagnostics.artifact_dir
        run_info["status"] = status.get("status", "success")
        run_info["results_valid"] = len(facts)
        run_info["json_found"] = True
        run_info["parsed_result_count"] = len(payload.get("results", [])) if isinstance(payload.get("results"), list) else 0
        run_info["validation_errors"] = status.get("warnings", [])
        if diagnostics.enabled:
            diagnostics.write_json("run_summary.json", {**run_info, "completed_at": now_iso()})
        diagnostics_path.write_text(json.dumps(run_info, indent=2, default=str), encoding="utf-8")
        return facts, run_info

    def _openai_api(self, events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if not self.settings.openai_api_key:
            return [], {"status": "skipped", "warning": "openai_api_key_missing"}
        return [], {"status": "provider_unavailable", "warning": "openai_api_mode_scaffolded_not_executed"}

    def load_output(self, output_path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        try:
            payload = json.loads(output_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return [], {"status": "provider_failed", "error": f"invalid_json:{exc}"}
        return self.load_payload(payload)

    def load_payload(self, payload: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if _contains_forbidden_terms(payload):
            return [], {"status": "provider_failed", "error": "research_output_contains_trading_terms"}
        facts: list[dict[str, Any]] = []
        warnings: list[str] = []
        rejected = 0
        future_actual_rejected = 0
        for item in payload.get("results", []):
            item, rejected_future_actual = reject_future_actual(item)
            if rejected_future_actual:
                future_actual_rejected += 1
                warnings.append(f"future_actual_rejected:{item.get('fact_key')}")
            validation = validate_ai_research_result(
                {
                    **item,
                    "status": item.get("status") or ("found" if _item_has_values(item) else "not_found"),
                    "data_type": item.get("data_type") or "macro_forecast",
                    "evidence_text": item.get("evidence_text") or item.get("extracted_text"),
                },
                ValidationRequest(
                    data_type=str(item.get("data_type") or "macro_forecast"),
                    expected_period=item.get("expected_period") or item.get("period"),
                    release_at=_parse_event_time(item.get("time_utc") or item.get("date")),
                    min_confidence=float(self.settings.ai_researcher_min_confidence),
                    require_evidence=bool(self.settings.ai_researcher_require_evidence),
                ),
            )
            if validation.status.startswith("rejected"):
                status_code = validation.status
                if status_code == "rejected_invalid_source" and not (item.get("source_url") or _first_metric_field(item, "source_url")):
                    status_code = "rejected_missing_source_url"
                warnings.append(f"{status_code}:{item.get('fact_key')}")
                rejected += 1
                continue
            has_value = _item_has_values(item)
            has_source_url = bool(item.get("source_url") or _first_metric_field(item, "source_url"))
            has_source = bool(item.get("source") or _first_metric_field(item, "source"))
            required_missing = [
                key
                for key in ("fact_key", "country", "date", "time_utc", "category", "event_name", "valid_until")
                if not item.get(key)
            ]
            reject_item = False
            if has_value and self.settings.ai_researcher_require_source_url and not has_source_url:
                warnings.append(f"rejected_missing_source_url:{item.get('fact_key')}")
                reject_item = True
            if has_value and not has_source:
                warnings.append(f"rejected_missing_source:{item.get('fact_key')}")
                reject_item = True
            if has_value and not item.get("valid_until"):
                warnings.append(f"rejected_missing_valid_until:{item.get('fact_key')}")
                reject_item = True
            if required_missing:
                warnings.append(f"rejected_missing_required:{item.get('fact_key')}:{','.join(required_missing)}")
                reject_item = True
            if reject_item:
                rejected += 1
                continue
            reliability = _primary_or_top(item, "reliability") or 0
            confidence = _primary_or_top(item, "confidence") or 0
            if has_value and (reliability <= 0 or confidence <= 0):
                warnings.append(f"rejected_low_reliability_or_confidence:{item.get('fact_key')}")
                rejected += 1
                continue
            valid_until = item.get("valid_until") if has_value else _negative_cache_valid_until(item)
            validated_payload = {
                **item,
                "provider_type": "AI_RESEARCHER_CODEX_CLI",
                "evidence": (
                    item.get("evidence")
                    or item.get("evidence_text")
                    or item.get("extracted_text")
                    or _first_metric_field(item, "evidence_text")
                ),
                "validation": {
                    "status": validation.status,
                    "reasons": validation.reasons,
                    "validated_at": now_iso(),
                },
                "metrics": [
                    {
                        **metric,
                        "provider_type": "AI_RESEARCHER_CODEX_CLI",
                        "evidence": metric.get("evidence") or metric.get("evidence_text"),
                        "validation": {
                            "status": validation.status,
                            "reasons": validation.reasons,
                        },
                    }
                    for metric in item.get("metrics") or []
                    if isinstance(metric, dict)
                ],
            }
            facts.append(
                {
                    "fact_key": item.get("fact_key"),
                    "fact_type": "macro_event_enrichment",
                    "country": item.get("country"),
                    "category": item.get("category"),
                    "event_name": item.get("event_name"),
                    "period": item.get("period"),
                    "unit": _primary_or_top(item, "unit"),
                    "forecast": _top_or_metric(item, "forecast"),
                    "previous": _top_or_metric(item, "previous"),
                    "consensus": _top_or_metric(item, "consensus"),
                    "actual": _top_or_metric(item, "actual"),
                    "source": _primary_or_top(item, "source"),
                    "source_url": _primary_or_top(item, "source_url"),
                    "provider_type": "AI_RESEARCHER_CODEX_CLI",
                    "reliability": reliability,
                    "confidence": confidence,
                    "retrieved_at": payload.get("generated_at") or now_iso(),
                    "release_at": item.get("time_utc"),
                    "valid_until": valid_until,
                    "next_refresh_at": valid_until,
                    "status": "active" if has_value else "no_data_available",
                    "notes": item.get("notes"),
                    "warnings_json": item.get("warnings") or [],
                    "raw_payload_json": validated_payload,
                }
            )
        valid_with_values = sum(1 for fact in facts if _item_has_values(fact) or _item_has_values(fact.get("raw_payload_json") or {}))
        return facts, {
            "status": "success" if facts else "no_data_available",
            "warnings": warnings,
            "results_valid": len(facts),
            "results_used": valid_with_values,
            "results_rejected": rejected,
            "future_actual_rejected": future_actual_rejected,
        }


def _record_diagnostic_failure(
    diagnostics: AIResearchDiagnostics,
    events: list[dict[str, Any]],
    run_info: dict[str, Any],
) -> None:
    if not diagnostics.enabled:
        return
    for event in events:
        event_id = event.get("event_id")
        diagnostics.event_text(event_id, "stdout.txt", str(run_info.get("stdout") or ""))
        diagnostics.event_text(event_id, "stderr.txt", str(run_info.get("stderr") or ""))
        diagnostics.event_json(
            event_id,
            "validation_report.json",
            {
                "status": run_info.get("status"),
                "failure_reason": run_info.get("failure_reason") or run_info.get("error"),
                "fields": [],
            },
        )
    diagnostics.write_json("run_summary.json", {**run_info, "completed_at": now_iso()})


def _record_diagnostic_success(
    diagnostics: AIResearchDiagnostics,
    settings: Settings,
    events: list[dict[str, Any]],
    payload: dict[str, Any],
    facts: list[dict[str, Any]],
    status: dict[str, Any],
) -> None:
    if not diagnostics.enabled:
        return
    raw_results = payload.get("results") if isinstance(payload.get("results"), list) else []
    facts_by_key = {str(fact.get("fact_key")): fact for fact in facts}
    for event in events:
        event_id = event.get("event_id")
        fact_key = str(event.get("fact_key") or "")
        raw_item = next((item for item in raw_results if isinstance(item, dict) and str(item.get("fact_key") or "") == fact_key), None)
        normalized = None
        report: dict[str, Any]
        if raw_item is None:
            report = {
                "status": "schema_field_missing",
                "reasons": ["result_for_fact_key_not_found"],
                "fields": [],
            }
        else:
            normalized, rejected_future_actual = reject_future_actual(dict(raw_item))
            validation = validate_ai_research_result(
                {
                    **normalized,
                    "status": normalized.get("status") or ("found" if _item_has_values(normalized) else "not_found"),
                    "data_type": normalized.get("data_type") or "macro_forecast",
                    "evidence_text": normalized.get("evidence_text") or normalized.get("extracted_text"),
                },
                _validation_request(settings, normalized),
            )
            report = {
                "status": validation.status,
                "reasons": validation.reasons,
                "rejected_future_actual": rejected_future_actual,
                "fields": _field_validation_rows(normalized, validation),
            }
        diagnostics.event_json(event_id, "extracted_candidate.json", raw_item)
        diagnostics.event_json(event_id, "parsed_result.json", raw_item)
        diagnostics.event_json(event_id, "normalized_result.json", normalized)
        diagnostics.event_json(event_id, "validation_report.json", report)
        fact = facts_by_key.get(fact_key)
        diagnostics.event_json(
            event_id,
            "persistence_payload.json",
            fact if fact else {"status": "not_persisted", "reason": report["status"], "warnings": status.get("warnings") or []},
        )


def _validation_request(settings: Settings, item: dict[str, Any]) -> ValidationRequest:
    return ValidationRequest(
        data_type=str(item.get("data_type") or "macro_forecast"),
        expected_period=item.get("expected_period") or item.get("period"),
        release_at=_parse_event_time(item.get("time_utc") or item.get("date")),
        min_confidence=float(settings.ai_researcher_min_confidence),
        require_evidence=bool(settings.ai_researcher_require_evidence),
    )


def _field_validation_rows(item: dict[str, Any], validation: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    expected_period = str(item.get("expected_period") or item.get("period") or "")
    metric_rows = item.get("metrics") if isinstance(item.get("metrics"), list) else []
    candidates = [("top_level", item), *[(str(metric.get("metric_id") or "metric"), metric) for metric in metric_rows if isinstance(metric, dict)]]
    for scope, candidate in candidates:
        source_url = candidate.get("source_url") or item.get("source_url")
        period = str(candidate.get("period") or item.get("period") or "")
        for field in VALUE_FIELDS:
            raw_value = candidate.get(field)
            if raw_value in (None, ""):
                continue
            rows.append(
                {
                    "field": f"{scope}.{field}",
                    "raw_value": raw_value,
                    "normalized_value": raw_value,
                    "accepted": validation.accepted,
                    "rejection_code": None if validation.accepted else validation.status,
                    "rejection_reason": [] if validation.accepted else validation.reasons,
                    "source_url": source_url,
                    "confidence": candidate.get("confidence") or item.get("confidence"),
                    "reliability": candidate.get("reliability") or item.get("reliability"),
                    "temporal_match": not (field == "actual" and raw_value not in (None, "") and _parse_event_time(item.get("time_utc") or item.get("date")) and datetime.now(UTC) < _parse_event_time(item.get("time_utc") or item.get("date"))),
                    "period_match": bool(period) and (not expected_period or expected_period.lower() in period.lower()),
                    "schema_match": bool(source_url and candidate.get("unit") and (candidate.get("frequency") or item.get("frequency"))),
                }
            )
    return rows


def _item_has_values(item: dict[str, Any]) -> bool:
    if any(item.get(field) not in (None, "") for field in VALUE_FIELDS):
        return True
    for metric in item.get("metrics") or []:
        if isinstance(metric, dict) and any(metric.get(field) not in (None, "") for field in VALUE_FIELDS):
            return True
    return False


def _first_metric_field(item: dict[str, Any], field: str) -> Any:
    for metric in item.get("metrics") or []:
        if isinstance(metric, dict) and metric.get(field) not in (None, ""):
            return metric.get(field)
    return None


def _top_or_metric(item: dict[str, Any], field: str) -> Any:
    primary = _primary_metric(item)
    if primary is not None:
        return primary.get(field)
    value = item.get(field)
    return value if value not in (None, "") else _first_metric_field(item, field)


def _primary_or_top(item: dict[str, Any], field: str) -> Any:
    primary = _primary_metric(item)
    if primary is not None and primary.get(field) not in (None, ""):
        return primary.get(field)
    value = item.get(field)
    return value if value not in (None, "") else _first_metric_field(item, field)


def _primary_metric(item: dict[str, Any]) -> dict[str, Any] | None:
    category = str(item.get("category") or "").upper()
    if "NONFARM" in category or "PAYROLL" in category:
        category = "NFP"
    expected = PRIMARY_METRIC_IDS.get(category, ())
    metrics = [metric for metric in item.get("metrics") or [] if isinstance(metric, dict)]
    return next((metric for metric_id in expected for metric in metrics if metric.get("metric_id") == metric_id), None)


def _contains_forbidden_terms(payload: Any) -> bool:
    text = "\n".join(_iter_trading_text(payload)).lower()
    return any(re.search(rf"(?<!\w){re.escape(term)}(?!\w)", text) for term in FORBIDDEN_TRADING_TERMS)


def _iter_trading_text(payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        return []
    text: list[str] = []
    for field in TRADING_TEXT_FIELDS:
        value = payload.get(field)
        if isinstance(value, str):
            text.append(value)
    for item in payload.get("results") or []:
        if isinstance(item, dict):
            text.extend(_iter_trading_text(item))
    return text


def _negative_cache_valid_until(item: dict[str, Any]) -> str:
    event_time = _parse_event_time(item.get("time_utc") or item.get("date"))
    now = datetime.now(UTC)
    if event_time is None:
        return (now + timedelta(hours=6)).isoformat()
    delta = event_time - now
    if delta <= timedelta(hours=48):
        ttl = timedelta(hours=2)
    elif delta <= timedelta(days=7):
        ttl = timedelta(hours=6)
    else:
        ttl = timedelta(hours=24)
    return (now + ttl).replace(microsecond=0).isoformat()


def _parse_event_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def parse_json_from_stdout(stdout: str) -> tuple[dict[str, Any] | None, str | None]:
    text = (stdout or "").strip()
    if not text:
        return None, "empty_stdout"
    try:
        payload = json.loads(text)
        return payload if isinstance(payload, dict) else None, None if isinstance(payload, dict) else "stdout_json_not_object"
    except json.JSONDecodeError:
        pass
    fenced = _strip_code_fence(text)
    if fenced != text:
        try:
            payload = json.loads(fenced)
            return payload if isinstance(payload, dict) else None, None if isinstance(payload, dict) else "stdout_json_not_object"
        except json.JSONDecodeError:
            pass
    extracted = _extract_first_json_object(text)
    if extracted:
        try:
            payload = json.loads(extracted)
            return payload if isinstance(payload, dict) else None, None if isinstance(payload, dict) else "stdout_json_not_object"
        except json.JSONDecodeError as exc:
            return None, f"invalid_extracted_json:{exc}"
    return None, "no_json_object_found_in_stdout"


def build_codex_research_prompt(prompt_template: str, research_input: dict[str, Any]) -> str:
    input_json = json.dumps(research_input, ensure_ascii=False, indent=2, default=str)
    output_schema = {
        "generated_at": "ISO-UTC",
        "results": [
            {
                "fact_key": "...",
                "country": "US",
                "date": "YYYY-MM-DD",
                "time_utc": "ISO-UTC",
                "category": "CPI",
                "event_name": "...",
                "period": "...",
                "forecast": None,
                "previous": None,
                "consensus": None,
                "actual": None,
                "unit": None,
                "source": None,
                "source_url": None,
                "extracted_text": None,
                "reliability": 0.0,
                "confidence": 0.0,
                "valid_until": "ISO-UTC",
                "notes": None,
                "warnings": [],
                "metrics": [
                    {
                        "metric_id": "headline_cpi_mom",
                        "label": "Headline CPI MoM",
                        "value_type": "percent",
                        "frequency": "MoM",
                        "forecast": None,
                        "consensus": None,
                        "previous": None,
                        "actual": None,
                        "unit": "percent",
                        "source": None,
                        "source_url": None,
                        "evidence_text": None,
                        "retrieved_at": None,
                        "valid_until": "ISO-UTC",
                        "reliability": 0.0,
                        "confidence": 0.0,
                        "field_semantics": {
                            "forecast_is_consensus": False,
                            "forecast_origin": None,
                            "period_match": True,
                        },
                        "warnings": [],
                    }
                ],
                "fomc_context": None,
            }
        ],
    }
    schema_json = json.dumps(output_schema, ensure_ascii=False, indent=2)
    return f"""{prompt_template.strip()}

COMPITO
Ricerca forecast, previous e consensus per gli eventi macro contenuti nell'input.

REGOLE OPERATIVE
- Non inventare dati.
- Non stimare valori.
- Lascia sempre actual a null: gli actual numerici sono risolti solo dal servizio deterministico ufficiale.
- Ogni valore numerico deve avere source, source_url e evidence_text.
- Il periodo della fonte deve coincidere con il periodo dell'evento.
- Se un dato non e' verificabile, lascialo null.
- Nessuna trading logic.
- Nessun buy, sell, long, short, entry, stop, target o recommendation.
- Restituisci esclusivamente JSON valido.
- Non aggiungere markdown.
- Non aggiungere code fence.
- Non aggiungere spiegazioni prima o dopo il JSON.
- Se non hai accesso web reale per verificare fonti, restituisci valori null e warning codex_web_research_unavailable.
- Restituisci metrics[] per ogni dato numerico verificabile.
- Ogni metrica deve avere metric_id, unit, frequency, source_url, evidence_text e field_semantics.
- Non usare forecast come consensus se la fonte non dice esplicitamente consensus.
- Dai priorita' a PCE se presente nel batch.
- Per FOMC usa fomc_context e non metriche CPI-like.

INPUT JSON
{input_json}

OUTPUT JSON ATTESO
{schema_json}

IMPORTANTE
Esegui ora la ricerca per tutti gli eventi dell'input e restituisci soltanto l'oggetto JSON finale.
"""


def _prompt_diagnostics(prompt: str, research_input: dict[str, Any]) -> dict[str, Any]:
    input_event_count = len(research_input.get("events", []))
    fact_keys = [str(event.get("fact_key", "")) for event in research_input.get("events", [])]
    return {
        "prompt_length_chars": len(prompt),
        "prompt_line_count": len(prompt.splitlines()),
        "prompt_contains_input": bool(fact_keys) and all(key and key in prompt for key in fact_keys),
        "input_event_count": input_event_count,
    }


def _strip_code_fence(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return text
    lines = stripped.splitlines()
    if len(lines) >= 3 and lines[-1].strip() == "```":
        return "\n".join(lines[1:-1]).strip()
    return text


def _extract_first_json_object(text: str) -> str | None:
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_string = False
    escape = False
    for index, char in enumerate(text[start:], start=start):
        if escape:
            escape = False
            continue
        if char == "\\" and in_string:
            escape = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return None


def _resolve_command(command: str) -> list[str] | None:
    resolved = shutil.which(command)
    if resolved:
        return [resolved]
    appdata = os.environ.get("APPDATA")
    if appdata and Path(command).name.lower() in {"codex", "codex.cmd", "codex.exe", "codex.ps1"}:
        codex_js = Path(appdata) / "npm" / "node_modules" / "@openai" / "codex" / "bin" / "codex.js"
        node = shutil.which("node")
        if codex_js.exists() and node:
            return [node, str(codex_js)]
        npm_cmd = Path(appdata) / "npm" / "codex.CMD"
        if npm_cmd.exists():
            return [str(npm_cmd)]
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        candidates = list(Path(local_app_data).glob("Microsoft/WindowsApps/codex*"))
        if candidates:
            return [str(candidates[0])]
    return None


def _generic_non_research_response(stdout: str) -> bool:
    lowered = (stdout or "").lower()
    return (
        "ricevuto" in lowered
        or "operer" in lowered
        or "opererò" in lowered
        or "ai researcher data-only" in lowered and "results" not in lowered
    )


def _safe_command(command: list[str]) -> list[str]:
    safe = []
    for index, part in enumerate(command):
        safe.append("<prompt>" if index == len(command) - 1 else redact_sensitive(str(part)))
    return safe


def _brief(value: Any, limit: int = 4000) -> str:
    text = "" if value is None else str(value)
    return redact_sensitive(text[:limit])
