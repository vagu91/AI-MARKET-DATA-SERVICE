from __future__ import annotations

import json
import os
import signal
import subprocess
import threading
import uuid
from queue import Empty, Queue
from time import monotonic, perf_counter
from pathlib import Path
from typing import Any, Callable

from app.core.config import Settings
from app.core.redaction import redact_payload
from app.core.text_normalization import contains_mojibake, normalize_payload_text
from app.providers.ai_researcher_provider import _resolve_command, parse_json_from_stdout
from app.services.codex_runtime_contract import (
    CodexCLIError,
    agentic_research_output_schema,
    build_codex_exec_command,
    build_diagnostic,
    canonicalize_workspace,
    classify_codex_failure,
    inherited_instruction_files,
    safe_subprocess_environment,
    step_output_schema,
    validate_isolated_command,
    validate_output_schema,
    validate_payload,
)
from app.services.research_backend import ResearchBackendResult, normalized_backend_input
from app.services.research_profiles import profile_for_job, prompt_context
from app.services.research_tool_telemetry import (
    normalize_codex_event,
    normalize_usage,
)


class PersistentAIJobExecutor:
    """Codex job executor with a per-job workspace and a real process watchdog."""

    backend_name = "codex_cli"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = threading.Lock()
        self._active: dict[int, subprocess.Popen[str]] = {}

    def __call__(
        self, job: dict[str, Any], workspace: Path, watchdog_seconds: int
    ) -> dict[str, Any]:
        del job, workspace, watchdog_seconds
        return {
            "status": "REJECTED",
            "error": "persistent_step_runtime_required",
            "error_category": "CONFIG_INVALID",
            "retryable": False,
            "retry_classification": "NON_RETRYABLE",
            "results": [],
        }

    def execute_step(
        self,
        *,
        job: dict[str, Any],
        run: dict[str, Any],
        step_name: str,
        context: dict[str, Any],
        workspace: Path,
        watchdog_seconds: int,
        effective_budget: dict[str, Any] | None = None,
        event_observer: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        if not self.settings.enable_ai_researcher:
            raise RuntimeError("ai_researcher_disabled")
        if not self.settings.ai_research_web_access_enabled:
            raise RuntimeError("research_web_access_not_verified")
        command_prefix = _resolve_command(self.settings.codex_cli_command)
        if not command_prefix:
            raise self._preflight_error(
                category="EXECUTABLE_UNAVAILABLE",
                step=step_name,
                workspace=workspace,
                command=[self.settings.codex_cli_command],
            )
        try:
            workspace = canonicalize_workspace(workspace)
        except (OSError, ValueError) as exc:
            raise self._preflight_error(
                category="PATH_INVALID",
                step=step_name,
                workspace=workspace.absolute(),
                command=command_prefix,
                stderr=str(exc),
            ) from exc
        inherited_instructions = inherited_instruction_files(workspace)
        if inherited_instructions:
            raise self._preflight_error(
                category="CONFIG_INVALID",
                step=step_name,
                workspace=workspace,
                command=command_prefix,
                stderr="inherited_agent_instructions_present",
            )
        schema_path = workspace / f"{step_name.lower()}_output_schema.json"
        schema = (
            step_output_schema(step_name, effective_budget)
            if effective_budget is not None
            else step_output_schema(step_name)
        )
        try:
            validate_output_schema(schema)
        except ValueError as exc:
            raise self._preflight_error(
                category="SCHEMA_INVALID",
                step=step_name,
                workspace=workspace,
                command=command_prefix,
                stderr=str(exc),
            ) from exc
        schema_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")
        output_path = workspace / f"{step_name.lower()}_output.json"
        profile = profile_for_job(str(job["job_type"]))
        profile_payload = context.get("profile") or prompt_context(
            profile,
            job.get("request_payload") or {},
            effective_budget,
        )
        prompt = build_step_prompt(
            job,
            run,
            step_name,
            context,
            profile_payload,
        )
        command = build_codex_exec_command(
            command_prefix,
            workspace=workspace,
            schema_path=schema_path,
            output_path=output_path,
        )
        try:
            validate_isolated_command(command, prompt, cwd=workspace)
        except ValueError as exc:
            raise self._preflight_error(
                category="CONFIG_INVALID",
                step=step_name,
                workspace=workspace,
                command=command,
                stderr=str(exc),
            ) from exc
        try:
            result = self._invoke(
                command,
                prompt,
                workspace,
                output_path,
                watchdog_seconds,
                json_event_stream=True,
                schema=schema,
                step=step_name,
                executable_version=self._executable_version(command_prefix),
                event_observer=event_observer,
            )
        except CodexCLIError as exc:
            exc.diagnostic["run_id"] = str(run["run_id"])
            exc.diagnostic["job_id"] = str(job["job_id"])
            raise
        return {key: value for key, value in result.items() if key != "status"}

    def execute_research(
        self,
        *,
        job: dict[str, Any],
        run: dict[str, Any],
        profile: dict[str, Any],
        workspace: Path,
        watchdog_seconds: int,
        effective_budget: dict[str, Any],
        event_observer: Callable[[dict[str, Any]], None] | None = None,
    ) -> ResearchBackendResult:
        """Execute the optimized backend contract exactly once for a logical run."""

        if not self.settings.enable_ai_researcher:
            raise RuntimeError("ai_researcher_disabled")
        if not self.settings.ai_research_web_access_enabled:
            raise RuntimeError("research_web_access_not_verified")
        command_prefix = _resolve_command(self.settings.codex_cli_command)
        if not command_prefix:
            raise self._preflight_error(
                category="EXECUTABLE_UNAVAILABLE",
                step="AGENTIC_RESEARCH",
                workspace=workspace,
                command=[self.settings.codex_cli_command],
            )
        try:
            workspace = canonicalize_workspace(workspace)
        except (OSError, ValueError) as exc:
            raise self._preflight_error(
                category="PATH_INVALID",
                step="AGENTIC_RESEARCH",
                workspace=workspace.absolute(),
                command=command_prefix,
                stderr=str(exc),
            ) from exc
        if inherited_instruction_files(workspace):
            raise self._preflight_error(
                category="CONFIG_INVALID",
                step="AGENTIC_RESEARCH",
                workspace=workspace,
                command=command_prefix,
                stderr="inherited_agent_instructions_present",
            )
        schema = agentic_research_output_schema(effective_budget)
        validate_output_schema(schema)
        schema_path = workspace / "agentic_research_output_schema.json"
        schema_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")
        output_path = workspace / "agentic_research_output.json"
        backend_input = normalized_backend_input(
            job=job,
            run=run,
            profile=profile,
            effective_budget=effective_budget,
        )
        prompt = build_agentic_research_prompt(
            job,
            run,
            backend_input,
            effective_budget,
        )
        command = build_codex_exec_command(
            command_prefix,
            workspace=workspace,
            schema_path=schema_path,
            output_path=output_path,
        )
        validate_isolated_command(command, prompt, cwd=workspace)
        invocation_id = f"rinvoke-{uuid.uuid4()}"
        started = perf_counter()
        try:
            result = self._invoke(
                command,
                prompt,
                workspace,
                output_path,
                watchdog_seconds,
                json_event_stream=True,
                schema=schema,
                step="SEARCH",
                executable_version=self._executable_version(command_prefix),
                event_observer=event_observer,
            )
        except CodexCLIError as exc:
            exc.diagnostic["run_id"] = str(run["run_id"])
            exc.diagnostic["job_id"] = str(job["job_id"])
            raise
        payload = {
            key: value
            for key, value in result.items()
            if not key.startswith("_") and key != "usage_status"
        }
        return ResearchBackendResult(
            invocation_id=invocation_id,
            backend=self.backend_name,
            purpose="agentic_research",
            payload=payload,
            usage=dict(result.get("_usage") or {}),
            tool_events=tuple(
                item for item in result.get("_tool_events") or [] if isinstance(item, dict)
            ),
            duration_ms=int((perf_counter() - started) * 1000),
            model=None,
        )

    def _invoke(
        self,
        command: list[str],
        prompt: str,
        workspace: Path,
        output_path: Path,
        watchdog_seconds: int,
        json_event_stream: bool = False,
        schema: dict[str, Any] | None = None,
        step: str = "JOB",
        executable_version: str | None = None,
        event_observer: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        started = perf_counter()
        output_path.unlink(missing_ok=True)
        kwargs: dict[str, Any] = {
            "cwd": workspace,
            "stdin": subprocess.PIPE,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "text": True,
            "encoding": "utf-8",
            "errors": "replace",
            "env": safe_subprocess_environment(),
        }
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs["start_new_session"] = True
        try:
            process = subprocess.Popen(command, **kwargs)
        except OSError as exc:
            category, retryable = classify_codex_failure(
                exit_code=None,
                stderr=str(exc),
            )
            raise CodexCLIError(
                build_diagnostic(
                    category=("EXECUTABLE_UNAVAILABLE" if category == "UNKNOWN" else category),
                    retryable=retryable,
                    command=command,
                    step=step,
                    workspace=workspace,
                    duration_ms=int((perf_counter() - started) * 1000),
                    executable_version=executable_version,
                    stderr=str(exc),
                )
            ) from exc
        with self._lock:
            self._active[process.pid] = process
        try:
            try:
                if json_event_stream and event_observer is not None:
                    stdout, stderr = _communicate_jsonl_incrementally(
                        process,
                        prompt,
                        max(int(watchdog_seconds), 1),
                        event_observer,
                        step_name=step,
                    )
                else:
                    stdout, stderr = process.communicate(
                        input=prompt,
                        timeout=max(int(watchdog_seconds), 1),
                    )
            except subprocess.TimeoutExpired as exc:
                _terminate_process_group(process)
                if json_event_stream and event_observer is not None:
                    stdout = str(exc.output or "")
                    stderr = str(exc.stderr or "")
                else:
                    stdout, stderr = process.communicate()
                error_events = extract_codex_error_events(stdout)
                raise CodexCLIError(
                    build_diagnostic(
                        category="TIMEOUT",
                        retryable=True,
                        command=command,
                        step=step,
                        workspace=workspace,
                        duration_ms=int((perf_counter() - started) * 1000),
                        executable_version=executable_version,
                        exit_code=process.returncode,
                        stderr=stderr,
                        stdout=stdout,
                        error_events=error_events,
                    )
                )
        finally:
            with self._lock:
                self._active.pop(process.pid, None)
        error_events = extract_codex_error_events(stdout)
        if process.returncode != 0:
            category, retryable = classify_codex_failure(
                exit_code=process.returncode,
                stderr=stderr,
                error_events=error_events,
            )
            raise CodexCLIError(
                build_diagnostic(
                    category=category,
                    retryable=retryable,
                    command=command,
                    step=step,
                    workspace=workspace,
                    duration_ms=int((perf_counter() - started) * 1000),
                    executable_version=executable_version,
                    exit_code=process.returncode,
                    stderr=stderr,
                    stdout=stdout,
                    error_events=error_events,
                )
            )
        if json_event_stream:
            _ignored_payload, tool_events, usage, _stream_error = parse_codex_json_event_stream(
                stdout,
                step_name=step,
            )
            payload, error = _read_structured_output(output_path)
        else:
            payload, error = parse_json_from_stdout(stdout)
            tool_events, usage = [], None
        if payload is None:
            raise CodexCLIError(
                build_diagnostic(
                    category="OUTPUT_CONTRACT",
                    retryable=False,
                    command=command,
                    step=step,
                    workspace=workspace,
                    duration_ms=int((perf_counter() - started) * 1000),
                    executable_version=executable_version,
                    exit_code=process.returncode,
                    stderr=f"{stderr}\n{error or 'invalid_json'}",
                    stdout=stdout,
                    error_events=error_events,
                )
            )
        payload = normalize_payload_text(payload)
        if contains_mojibake(payload):
            raise CodexCLIError(
                build_diagnostic(
                    category="OUTPUT_CONTRACT",
                    retryable=False,
                    command=command,
                    step=step,
                    workspace=workspace,
                    duration_ms=int((perf_counter() - started) * 1000),
                    executable_version=executable_version,
                    exit_code=process.returncode,
                    stderr=f"{stderr}\nknown_mojibake_rejected",
                    stdout=stdout,
                    error_events=error_events,
                )
            )
        if schema is not None:
            try:
                validate_payload(payload, schema)
            except ValueError as exc:
                raise CodexCLIError(
                    build_diagnostic(
                        category="OUTPUT_CONTRACT",
                        retryable=False,
                        command=command,
                        step=step,
                        workspace=workspace,
                        duration_ms=int((perf_counter() - started) * 1000),
                        executable_version=executable_version,
                        exit_code=process.returncode,
                        stderr=f"{stderr}\n{exc}",
                        stdout=stdout,
                        error_events=error_events,
                    )
                ) from exc
        output_path.write_text(
            json.dumps(redact_payload(payload), indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        if json_event_stream:
            telemetry_path = output_path.with_name(f"{output_path.stem}_telemetry.json")
            telemetry_path.write_text(
                json.dumps(redact_payload(tool_events), indent=2, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
        return {
            "status": "SUCCEEDED",
            **payload,
            "_tool_events": tool_events,
            "_usage": usage,
            "usage_status": "available" if usage is not None else "usage_unavailable",
            "_events_persisted_incrementally": event_observer is not None,
        }

    def _preflight_error(
        self,
        *,
        category: str,
        step: str,
        workspace: Path,
        command: list[str],
        stderr: str = "",
    ) -> CodexCLIError:
        return CodexCLIError(
            build_diagnostic(
                category=category,
                retryable=False,
                command=command,
                step=step,
                workspace=workspace,
                duration_ms=0,
                executable_version=None,
                stderr=stderr,
            )
        )

    def _executable_version(self, command_prefix: list[str]) -> str | None:
        try:
            completed = subprocess.run(
                [*command_prefix, "--version"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=10,
                check=False,
                env=safe_subprocess_environment(),
            )
        except (OSError, subprocess.SubprocessError):
            return None
        if completed.returncode != 0:
            return None
        return " ".join((completed.stdout or "").split())[:120] or None

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
        "results": [
            {
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
            }
        ],
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
        key: value
        for key, value in context.items()
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


def build_agentic_research_prompt(
    job: dict[str, Any],
    run: dict[str, Any],
    profile: dict[str, Any],
    effective_budget: dict[str, Any],
) -> str:
    """Bounded one-pass prompt that never resends the full database payload."""

    request = job.get("request_payload") or {}
    database_context = (
        request.get("database_context") or request.get("existing_database_results") or {}
    )
    bounded_profile = {key: value for key, value in profile.items() if key != "database_context"}
    input_context = {
        "context_date": request.get("context_date"),
        "market_session": request.get("market_session"),
        "data_as_of": (
            database_context.get("data_as_of") if isinstance(database_context, dict) else None
        ),
        "missing_fields": (request.get("missing_fields") or request.get("pending_fields") or []),
        "database_inventory": _context_inventory(database_context),
        "sources_already_queried": (request.get("sources_already_queried") or [])[:50],
    }
    contract = {
        "backend_role": (
            "propose queries, discover real HTTPS URLs, and return candidate "
            "claims with short verbatim evidence anchors"
        ),
        "service_role": (
            "fetch every requested URL, validate policy/SSRF/HTTP/content, "
            "hash content, match anchors, enforce confirmations, persist and read back"
        ),
        "source_state_rule": (
            "never assert OPENED, FETCHED, VERIFIED, HTTP status, content hash, "
            "source tier or confirmation count"
        ),
        "actual_rule": (
            "never emit official actual semantics; deterministic official "
            "resolvers own numerical actuals"
        ),
        "evidence_rule": (
            "each claim evidence_text must be a short bounded anchor actually "
            "present on its cited acquisition URL"
        ),
        "semantic_taxonomy": {
            "scheduled_event": "future scheduled event; event_at or release_at is mandatory",
            "official_calendar_event": (
                "future event from an official calendar; event_at or release_at is mandatory"
            ),
            "issuer_announcement": (
                "official issuer announcement; issuer and event_at are mandatory"
            ),
            "earnings_schedule": (
                "official earnings timing; issuer and event_at are mandatory"
            ),
            "current_news": (
                "current material news; published_at is mandatory and two independent "
                "domains will be required by the service"
            ),
            "current_market_context": (
                "short-lived current market, volatility or positioning observation"
            ),
            "exploratory_context": (
                "historical background only; it never completes a current topic"
            ),
        },
        "search_strategy": [
            "perform and report at least one bounded query for every required topic",
            "when one query covers multiple topics, list every covered topic in plan.queries[].topics",
            "prefer sources published today or still relevant at context_date",
            "use official sources plus Reuters/AP for current news and conflicts",
            "use CFTC/CME/Cboe for positioning and volatility",
            "use Nasdaq/Invesco/issuer newsroom/issuer IR/SEC for Nasdaq-100, mega-cap and earnings",
            "do not use an old article merely to complete a current topic",
        ],
        "not_applicable_rule": (
            "NOT_APPLICABLE is permitted only when the server-owned profile declares "
            "the topic inapplicable; every MNQ topic is applicable, so a bounded empty "
            "result must be NO_CURRENT_ITEM or NO_DATA"
        ),
        "budget_rule": (
            "in observe mode numeric budgets are warning thresholds; loop detection and "
            "the emergency action ceiling remain hard safety controls"
        ),
    }
    return (
        "You are a data-only research backend for one bounded agentic invocation.\n"
        "Plan, search, identify original sources, and return candidate atomic claims in one pass.\n"
        "Never modify files, call AI-TRADER, place orders, or provide trading advice.\n"
        "Never invent URLs, evidence, values, timestamps, source state, or trust labels.\n"
        "Never label a future economic release or official calendar entry as current_news.\n"
        "Every required topic must have a documented bounded search; never use "
        "NOT_APPLICABLE for an empty MNQ query.\n"
        "Prefer current, temporally relevant sources over older accessible articles.\n"
        "The service independently acquires and verifies every source after this invocation.\n\n"
        f"RUN_ID\n{run['run_id']}\n\nJOB_ID\n{job['job_id']}\n\n"
        f"PROFILE\n{_bounded_json(bounded_profile, 24000)}\n\n"
        f"INPUT_CONTEXT\n{_bounded_json(input_context, 16000)}\n\n"
        f"EFFECTIVE_BUDGET\n{_bounded_json(effective_budget, 8000)}\n\n"
        f"TRUST_CONTRACT\n{json.dumps(contract, ensure_ascii=False, indent=2)}\n\n"
        "Return only JSON matching the supplied output schema."
    )


def _phase_requirements(step_name: str) -> list[str]:
    return {
        "PLAN": [
            "identify missing topics",
            "produce no more queries than limits.remaining_searches",
            "combine multiple query_topic_groups topics in one query when required",
            "define stop conditions that stop at the numeric budget",
        ],
        "SEARCH": [
            "execute only planned queries within limits.remaining_searches",
            "never repeat completed_queries",
            "in observe mode treat numeric budgets as warning thresholds",
            "in enforce mode stop when the numeric search budget is exhausted",
            "return query and discovered URLs",
        ],
        "OPEN_SOURCE": [
            "open original sources within limits.remaining_opened_sources",
            "never reopen URLs listed in completed_opened_sources",
            "in observe mode treat numeric budgets as warning thresholds",
            "in enforce mode stop when the numeric opened-source budget is exhausted",
            "reported OPENED/evidence/http status is model-declared until service verification",
        ],
        "EXTRACT": [
            "extract atomic claims and short evidence",
            "retain exact metric, period and unit",
        ],
        "CROSS_CHECK": ["compare claims across sources", "mark conflicts and syndication"],
        "VALIDATE": [
            "return final atomic claims with nested evidence",
            "return NO_DATA when criteria fail",
        ],
    }[step_name]


def _context_inventory(value: Any, *, depth: int = 0) -> Any:
    if depth >= 3:
        if isinstance(value, (dict, list)):
            return {"type": type(value).__name__, "count": len(value)}
        return str(value)[:120] if value is not None else None
    if isinstance(value, dict):
        output: dict[str, Any] = {}
        for key, item in list(value.items())[:80]:
            if key in {
                "raw_payload",
                "raw_payload_json",
                "evidence_text",
                "summary",
                "content",
            }:
                continue
            output[str(key)[:120]] = _context_inventory(
                item,
                depth=depth + 1,
            )
        return output
    if isinstance(value, list):
        identifiers: list[Any] = []
        for item in value[:10]:
            if isinstance(item, dict):
                identifiers.append(
                    {
                        key: item.get(key)
                        for key in (
                            "topic",
                            "category",
                            "event_name",
                            "symbol",
                            "status",
                            "date",
                        )
                        if item.get(key) is not None
                    }
                )
            else:
                identifiers.append(str(item)[:120])
        return {"count": len(value), "sample": identifiers}
    if isinstance(value, str):
        return value[:300]
    return value


def _bounded_json(value: Any, limit: int) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
        default=str,
    )
    return encoded if len(encoded) <= limit else encoded[:limit] + "\n...[bounded]"


def _communicate_jsonl_incrementally(
    process: subprocess.Popen[str],
    prompt: str,
    timeout_seconds: int,
    event_observer: Callable[[dict[str, Any]], None],
    *,
    step_name: str = "UNKNOWN",
) -> tuple[str, str]:
    if (
        process.stdin is None
        or process.stdout is None
        or process.stderr is None
        or not hasattr(process.stdout, "readline")
    ):
        stdout, stderr = process.communicate(
            input=prompt,
            timeout=timeout_seconds,
        )
        for line in stdout.splitlines():
            for event in _tool_events_from_jsonl_line(
                line,
                step_name=step_name,
            ):
                event_observer(event)
        return stdout, stderr

    stdout_lines: list[str] = []
    stderr_parts: list[str] = []
    lines: Queue[str | None] = Queue()

    def read_stdout() -> None:
        try:
            while True:
                line = process.stdout.readline()
                if line == "":
                    break
                lines.put(line)
        finally:
            lines.put(None)

    def read_stderr() -> None:
        stderr_parts.append(process.stderr.read())

    stdout_thread = threading.Thread(target=read_stdout, daemon=True)
    stderr_thread = threading.Thread(target=read_stderr, daemon=True)
    stdout_thread.start()
    stderr_thread.start()
    process.stdin.write(prompt)
    process.stdin.close()
    deadline = monotonic() + max(timeout_seconds, 1)
    completed = False
    try:
        while not completed:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(
                    process.args,
                    timeout_seconds,
                    output="".join(stdout_lines),
                    stderr="".join(stderr_parts),
                )
            try:
                line = lines.get(timeout=min(remaining, 0.1))
            except Empty:
                if process.poll() is not None and not stdout_thread.is_alive():
                    break
                continue
            if line is None:
                completed = True
                continue
            stdout_lines.append(line)
            for event in _tool_events_from_jsonl_line(
                line,
                step_name=step_name,
            ):
                try:
                    event_observer(event)
                except Exception:
                    _terminate_process_group(process)
                    raise
        process.wait(timeout=max(deadline - monotonic(), 0.1))
    finally:
        stdout_thread.join(timeout=2)
        stderr_thread.join(timeout=2)
    return "".join(stdout_lines), "".join(stderr_parts)


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
    *,
    step_name: str = "UNKNOWN",
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
                usage = normalize_usage(candidate)
        events.extend(_tool_events_from_event(event, step_name=step_name))
    for message in reversed(final_messages):
        payload, error = parse_json_from_stdout(message)
        if payload is not None:
            return payload, events, usage, None
    return None, events, usage, "codex_json_stream_missing_structured_final_message"


def _tool_events_from_jsonl_line(
    line: str,
    *,
    step_name: str = "UNKNOWN",
) -> list[dict[str, Any]]:
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return []
    return _tool_events_from_event(event, step_name=step_name) if isinstance(event, dict) else []


def _tool_events_from_event(
    event: dict[str, Any],
    *,
    step_name: str = "UNKNOWN",
) -> list[dict[str, Any]]:
    return normalize_codex_event(event, step_name=step_name)


def extract_codex_error_events(stdout: str) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        event_type = str(event.get("type") or "").lower()
        if event_type == "turn.failed" or "error" in event_type:
            errors.append(redact_payload(event))
    return errors[-10:]


def _read_structured_output(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path.exists():
        return None, "structured_output_file_missing"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"structured_output_file_invalid:{type(exc).__name__}:{exc}"
    if not isinstance(payload, dict):
        return None, "structured_output_file_not_object"
    return payload, None
