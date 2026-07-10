from __future__ import annotations

import json
import os
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

VALUE_FIELDS = ("forecast", "previous", "consensus", "actual")
FORBIDDEN_TRADING_TERMS = {
    "buy", "sell", "long", "short", "no_trade", "entry", "target", "stop", "recommendation",
}


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
        workspace = Path(self.settings.codex_workspace_dir)
        workspace.mkdir(parents=True, exist_ok=True)
        input_path = workspace / "research_input.json"
        output_path = workspace / "research_output.json"
        diagnostics_path = workspace / "last_codex_run.json"
        research_input = {"generated_at": now_iso(), "events": events}
        input_path.write_text(json.dumps(research_input, indent=2, default=str, ensure_ascii=False), encoding="utf-8")
        prompt_path = Path("app/prompts/ai_research_macro_event.md")
        prompt = prompt_path.read_text(encoding="utf-8") if prompt_path.exists() else "Research events and write research_output.json."
        final_prompt = build_codex_research_prompt(prompt, research_input)
        command_prefix = _resolve_command(self.settings.codex_cli_command)
        command = [
            *(command_prefix or [self.settings.codex_cli_command]),
            "exec",
            "--skip-git-repo-check",
            final_prompt,
        ]
        started = perf_counter()
        cwd = Path.cwd()
        run_info: dict[str, Any] = {
            "command": _safe_command(command),
            "cwd": str(cwd),
            "input_path": str(input_path.resolve()),
            "output_path": str(output_path.resolve()),
            "status": "started",
            **_prompt_diagnostics(final_prompt, research_input),
            "web_search_enabled": False,
            "web_search_note": "Codex CLI web access is controlled by the local Codex installation; no separate web-search flag is configured by this service.",
        }
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
            return [], run_info
        try:
            completed = subprocess.run(
                command,
                cwd=cwd,
                timeout=self.settings.codex_research_timeout_seconds,
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
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
            return [], {**run_info, "status": "provider_failed", "failure_reason": "codex_cli_timeout"}
        except (FileNotFoundError, PermissionError, OSError) as exc:
            run_info.update(
                {
                    "status": "provider_unavailable",
                    "duration_ms": int((perf_counter() - started) * 1000),
                    "error": redact_sensitive(str(exc)),
                    "failure_reason": "codex_cli_unavailable",
                }
            )
            diagnostics_path.write_text(json.dumps(run_info, indent=2, default=str), encoding="utf-8")
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
        if completed.returncode != 0:
            run_info.update(
                {
                    "status": "provider_failed",
                    "error": redact_sensitive(completed.stderr or completed.stdout),
                    "failure_reason": "codex_cli_non_zero_exit",
                }
            )
            diagnostics_path.write_text(json.dumps(run_info, indent=2, default=str), encoding="utf-8")
            return [], run_info
        payload, parse_error = parse_json_from_stdout(completed.stdout)
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
            return [], run_info
        output_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        facts, status = self.load_payload(payload)
        run_info.update(status)
        run_info["status"] = status.get("status", "success")
        run_info["results_valid"] = len(facts)
        run_info["json_found"] = True
        run_info["parsed_result_count"] = len(payload.get("results", [])) if isinstance(payload.get("results"), list) else 0
        run_info["validation_errors"] = status.get("warnings", [])
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
            if _item_has_values(item) and not validation.accepted:
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
            reliability = item.get("reliability") or _first_metric_field(item, "reliability") or 0
            confidence = item.get("confidence") or _first_metric_field(item, "confidence") or 0
            if has_value and (reliability <= 0 or confidence <= 0):
                warnings.append(f"rejected_low_reliability_or_confidence:{item.get('fact_key')}")
                rejected += 1
                continue
            valid_until = item.get("valid_until") if has_value else _negative_cache_valid_until(item)
            facts.append(
                {
                    "fact_key": item.get("fact_key"),
                    "fact_type": "ai_research_result",
                    "country": item.get("country"),
                    "category": item.get("category"),
                    "event_name": item.get("event_name"),
                    "period": item.get("period"),
                    "unit": item.get("unit"),
                    "forecast": item.get("forecast"),
                    "previous": item.get("previous"),
                    "consensus": item.get("consensus"),
                    "actual": item.get("actual"),
                    "source": item.get("source") or _first_metric_field(item, "source"),
                    "source_url": item.get("source_url") or _first_metric_field(item, "source_url"),
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
                    "raw_payload_json": item,
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


def _item_has_values(item: dict[str, Any]) -> bool:
    if any(item.get(field) not in (None, "") for field in VALUE_FIELDS):
        return True
    for metric in item.get("metrics") or []:
        if isinstance(metric, dict) and any(metric.get(field) not in (None, "") for field in VALUE_FIELDS):
            return True
    return False


def _first_metric_field(item: dict[str, Any], field: str) -> Any:
    for metric in item.get("metrics") or []:
        if isinstance(metric, dict) and metric.get(field):
            return metric.get(field)
    return None


def _contains_forbidden_terms(payload: Any) -> bool:
    text = json.dumps(payload, default=str).lower()
    return any(term in text for term in FORBIDDEN_TRADING_TERMS)


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
Ricerca forecast, previous, consensus e actual per gli eventi macro contenuti nell'input.

REGOLE OPERATIVE
- Non inventare dati.
- Non stimare valori.
- Ogni valore numerico deve avere source e source_url.
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
- Ogni metrica deve avere metric_id, unit, frequency, source_url e field_semantics.
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
        "prompt_preview_start": prompt[:700],
        "prompt_preview_end": prompt[-700:],
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
    appdata = os.environ.get("APPDATA")
    if appdata:
        codex_js = Path(appdata) / "npm" / "node_modules" / "@openai" / "codex" / "bin" / "codex.js"
        node = shutil.which("node")
        if codex_js.exists() and node:
            return [node, str(codex_js)]
        npm_cmd = Path(appdata) / "npm" / "codex.CMD"
        if npm_cmd.exists():
            return [str(npm_cmd)]
    resolved = shutil.which(command)
    if resolved:
        return [resolved]
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
