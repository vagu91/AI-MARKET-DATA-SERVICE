from __future__ import annotations

import json
import os
import signal
import subprocess
import threading
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.core.redaction import redact_payload
from app.providers.ai_researcher_provider import _resolve_command, parse_json_from_stdout
from app.services.research_profiles import profile_for_job, prompt_context


class PersistentAIJobExecutor:
    """Codex job executor with a per-job workspace and a real process watchdog."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = threading.Lock()
        self._active: dict[int, subprocess.Popen[str]] = {}

    def __call__(self, job: dict[str, Any], workspace: Path, watchdog_seconds: int) -> dict[str, Any]:
        if not self.settings.enable_ai_researcher:
            return {"status": "REJECTED", "error": "ai_researcher_disabled", "results": []}
        if not self.settings.ai_research_web_access_enabled:
            return {"status": "REJECTED", "error": "research_web_access_not_verified", "results": []}
        if self.settings.ai_researcher_mode != "codex_cli":
            return {"status": "REJECTED", "error": "persistent_worker_requires_codex_cli", "results": []}
        command_prefix = _resolve_command(self.settings.codex_cli_command)
        if not command_prefix:
            return {"status": "REJECTED", "error": "codex_cli_unavailable", "results": []}

        workspace.mkdir(parents=True, exist_ok=True)
        input_path = workspace / "research_input.json"
        output_path = workspace / "research_output.json"
        input_path.write_text(
            json.dumps(redact_payload(job["request_payload"]), indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        prompt = build_job_prompt(job)
        command = [*command_prefix, "exec", "--skip-git-repo-check", "-"]
        return self._invoke(command, prompt, workspace, output_path, watchdog_seconds)

    def execute_step(
        self,
        *,
        job: dict[str, Any],
        run: dict[str, Any],
        step_name: str,
        context: dict[str, Any],
        workspace: Path,
        watchdog_seconds: int,
    ) -> dict[str, Any]:
        if not self.settings.enable_ai_researcher:
            raise RuntimeError("ai_researcher_disabled")
        if not self.settings.ai_research_web_access_enabled:
            raise RuntimeError("research_web_access_not_verified")
        command_prefix = _resolve_command(self.settings.codex_cli_command)
        if not command_prefix:
            raise RuntimeError("codex_cli_unavailable")
        workspace.mkdir(parents=True, exist_ok=True)
        schema_path = workspace / f"{step_name.lower()}_output_schema.json"
        schema_path.write_text(json.dumps(_step_schema(step_name), indent=2), encoding="utf-8")
        output_path = workspace / f"{step_name.lower()}_output.json"
        profile = profile_for_job(str(job["job_type"]))
        prompt = build_step_prompt(job, run, step_name, context, prompt_context(profile, job.get("request_payload") or {}))
        command = [
            *command_prefix, "--search", "-s", "read-only", "-C", str(workspace),
            "exec", "--skip-git-repo-check", "--ephemeral", "--json",
            "--output-schema", str(schema_path), "-",
        ]
        result = self._invoke(
            command, prompt, workspace, output_path, watchdog_seconds, json_event_stream=True,
        )
        if result.get("status") in {"FAILED", "REJECTED", "TIMED_OUT"}:
            raise RuntimeError(str(result.get("error") or "agent_step_failed"))
        return {key: value for key, value in result.items() if key != "status"}

    def _invoke(
        self,
        command: list[str],
        prompt: str,
        workspace: Path,
        output_path: Path,
        watchdog_seconds: int,
        json_event_stream: bool = False,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "cwd": workspace,
            "stdin": subprocess.PIPE,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "text": True,
            "encoding": "utf-8",
            "errors": "replace",
            "env": _safe_subprocess_environment(),
        }
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs["start_new_session"] = True
        process = subprocess.Popen(command, **kwargs)
        with self._lock:
            self._active[process.pid] = process
        try:
            try:
                stdout, stderr = process.communicate(input=prompt, timeout=max(int(watchdog_seconds), 1))
            except subprocess.TimeoutExpired:
                _terminate_process_group(process)
                stdout, stderr = process.communicate()
                return {
                    "status": "TIMED_OUT",
                    "error": "ai_job_watchdog_expired",
                    "stdout": stdout[-2000:],
                    "stderr": stderr[-2000:],
                    "results": [],
                }
        finally:
            with self._lock:
                self._active.pop(process.pid, None)
        if process.returncode != 0:
            return {
                "status": "FAILED",
                "error": f"codex_cli_exit_{process.returncode}",
                "stderr": stderr[-2000:],
                "results": [],
            }
        if json_event_stream:
            payload, tool_events, usage, error = parse_codex_json_event_stream(stdout)
        else:
            payload, error = parse_json_from_stdout(stdout)
            tool_events, usage = [], None
        if payload is None:
            return {"status": "REJECTED", "error": error or "invalid_json", "results": []}
        output_path.write_text(
            json.dumps(redact_payload(payload), indent=2, ensure_ascii=False, default=str), encoding="utf-8"
        )
        if json_event_stream:
            telemetry_path = output_path.with_name(f"{output_path.stem}_telemetry.json")
            telemetry_path.write_text(
                json.dumps(redact_payload(tool_events), indent=2, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
        return {
            "status": "SUCCEEDED", **payload, "_tool_events": tool_events,
            "_usage": usage, "usage_status": "available" if usage is not None else "usage_unavailable",
        }

    def cancel_all(self) -> None:
        with self._lock:
            processes = list(self._active.values())
        for process in processes:
            _terminate_process_group(process)


def build_job_prompt(job: dict[str, Any]) -> str:
    request = job.get("request_payload") or {}
    schema = {
        "status": "SUCCEEDED|NO_DATA",
        "job_id": job.get("job_id"),
        "results": [{
            "field": "forecast|consensus|previous|actual|outcome|transcript_url",
            "value": None,
            "source_domain": None,
            "source": None,
            "source_url": None,
            "canonical_url": None,
            "publisher": None,
            "source_tier": None,
            "source_classification": None,
            "published_at": None,
            "retrieved_at": None,
            "evidence_text": None,
            "metric_id": None,
            "period": None,
            "frequency": None,
            "unit": None,
            "field_semantics": None,
            "reliability": 0.0,
            "confidence": 0.0,
            "validation_status": "candidate",
            "warnings": [],
            "discordant_values": [],
            "policy_version": job.get("policy_version"),
        }],
    }
    return (
        "You are an asynchronous data-only AI Researcher. Research only the fields explicitly listed as missing.\n"
        "Never provide buy/sell, long/short, entry, stop, target, position or trading recommendations.\n"
        "Never invent or estimate data. Never overwrite official values. An actual requires an official source.\n"
        "Never return an actual before release_at. Preserve discordant candidates separately.\n"
        "A successful web result requires a URL and evidence text. Return JSON only.\n\n"
        f"JOB_ID\n{job.get('job_id')}\n\nJOB_TYPE\n{job.get('job_type')}\n\n"
        f"REQUEST\n{json.dumps(request, ensure_ascii=False, indent=2, default=str)}\n\n"
        f"OUTPUT_SCHEMA\n{json.dumps(schema, ensure_ascii=False, indent=2)}"
    )


def build_step_prompt(
    job: dict[str, Any],
    run: dict[str, Any],
    step_name: str,
    context: dict[str, Any],
    profile: dict[str, Any],
) -> str:
    prior = {
        key: value for key, value in context.items()
        if key in {"plan", "search", "open_source", "extract", "cross_check"}
    }
    return (
        "You are a persistent data-only research agent operating in a read-only isolated workspace.\n"
        "Do only the requested phase. Never modify application source code, connect to AI-TRADER, "
        "place orders, or provide buy/sell, long/short, entry, stop, target or sizing advice.\n"
        "Never expose credentials or secrets. Do not invent sources, URLs, timestamps, evidence or values.\n"
        "Source tier, classification and confirmation count are calculated by the service; do not claim them.\n"
        f"PHASE\n{step_name}\n\nRUN_ID\n{run['run_id']}\n\nJOB_ID\n{job['job_id']}\n\n"
        f"PROFILE\n{json.dumps(profile, ensure_ascii=False, indent=2, default=str)}\n\n"
        f"PRIOR_VERIFIED_PHASES\n{json.dumps(prior, ensure_ascii=False, indent=2, default=str)}\n\n"
        f"PHASE_REQUIREMENTS\n{json.dumps(_phase_requirements(step_name), ensure_ascii=False, indent=2)}\n\n"
        "Return only JSON matching the supplied output schema."
    )


def _phase_requirements(step_name: str) -> list[str]:
    return {
        "PLAN": ["identify missing topics", "produce bounded queries", "define stop conditions"],
        "SEARCH": ["run multiple bounded searches", "return query and discovered URLs"],
        "OPEN_SOURCE": ["open original sources", "record status, redirect, publisher and timestamps"],
        "EXTRACT": ["extract atomic claims and short evidence", "retain exact metric, period and unit"],
        "CROSS_CHECK": ["compare claims across sources", "mark conflicts and syndication"],
        "VALIDATE": ["return final atomic claims with nested evidence", "return NO_DATA when criteria fail"],
    }[step_name]


def _step_schema(step_name: str) -> dict[str, Any]:
    base: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "status": {"type": "string"},
            "queries": {"type": "array", "items": {"type": "string"}},
            "sources": {"type": "array", "items": {"type": "object", "additionalProperties": True}},
            "claims": {"type": "array", "items": {"type": "object", "additionalProperties": True}},
            "warnings": {"type": "array", "items": {"type": "string"}},
        },
    }
    base["required"] = ["claims"] if step_name == "VALIDATE" else ["status"]
    return base


def _safe_subprocess_environment() -> dict[str, str]:
    allowed = {
        "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP", "USERPROFILE",
        "APPDATA", "LOCALAPPDATA", "PROGRAMDATA", "CODEX_HOME", "HTTP_PROXY", "HTTPS_PROXY",
        "NO_PROXY", "LANG", "LC_ALL",
    }
    return {key: value for key, value in os.environ.items() if key.upper() in allowed}


def _terminate_process_group(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                subprocess.Popen(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                ).wait(timeout=10)
        else:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
    finally:
        if process.poll() is None:
            process.kill()


def parse_codex_json_event_stream(
    stdout: str,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], dict[str, Any] | None, str | None]:
    events: list[dict[str, Any]] = []
    final_messages: list[str] = []
    usage: dict[str, Any] | None = None
    for line in stdout.splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            event = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        event_type = str(event.get("type") or "")
        item = event.get("item") if isinstance(event.get("item"), dict) else event
        item_type = str(item.get("type") or event_type).lower()
        if item_type in {"agent_message", "message"} and "completed" in event_type:
            message = item.get("text") or item.get("content") or item.get("message")
            if isinstance(message, str):
                final_messages.append(message)
        if "usage" in event_type or event_type in {"turn.completed", "run.completed"}:
            candidate = event.get("usage") or item.get("usage")
            if isinstance(candidate, dict):
                usage = candidate
        observed_type = _observed_tool_type(item_type, event_type)
        if observed_type:
            urls = _event_urls(item)
            if observed_type == "search":
                event = _tool_event(observed_type, item, None)
                event["discovered_urls"] = urls
                events.append(event)
            else:
                events.extend(_tool_event(observed_type, item, url) for url in urls)
    for message in reversed(final_messages):
        payload, error = parse_json_from_stdout(message)
        if payload is not None:
            return payload, events, usage, None
    return None, events, usage, "codex_json_stream_missing_structured_final_message"


def _observed_tool_type(item_type: str, event_type: str) -> str | None:
    combined = f"{event_type} {item_type}".lower()
    if any(token in combined for token in ("web_open", "open_source", "source.open", "web_fetch")):
        return "open_source"
    if any(token in combined for token in ("web_search", "search_query", "source.search")):
        return "search"
    return None


def _event_urls(value: Any) -> list[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if key.lower() in {"url", "source_url", "canonical_url", "redirect_url"} and isinstance(item, str):
                if item.startswith("https://"):
                    found.add(item)
            else:
                found.update(_event_urls(item))
    elif isinstance(value, list):
        for item in value:
            found.update(_event_urls(item))
    return sorted(found)


def _tool_event(kind: str, item: dict[str, Any], url: str | None) -> dict[str, Any]:
    return {
        "event_type": kind,
        "source_url": url,
        "canonical_url": item.get("canonical_url") if isinstance(item.get("canonical_url"), str) else url,
        "redirect_url": item.get("redirect_url"),
        "observed_at": item.get("observed_at") or item.get("timestamp"),
        "content_hash": item.get("content_hash") or item.get("content_checksum"),
        "http_status": item.get("http_status") or item.get("status_code"),
        "query": item.get("query") or item.get("search_query"),
    }
