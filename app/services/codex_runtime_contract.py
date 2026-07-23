from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.core.redaction import redact_payload, redact_sensitive


EXTERNAL_RESEARCH_STEPS = (
    "PLAN",
    "SEARCH",
    "OPEN_SOURCE",
    "EXTRACT",
    "CROSS_CHECK",
    "VALIDATE",
)
DIAGNOSTIC_TEXT_LIMIT = 4000
DIAGNOSTIC_EVENT_LIMIT = 10
DIAGNOSTIC_EVENT_TEXT_LIMIT = 2000


def canonicalize_workspace(workspace: Path) -> Path:
    """Create a runtime workspace and return its one canonical absolute path."""
    workspace.mkdir(parents=True, exist_ok=True)
    canonical = workspace.resolve(strict=True)
    if not canonical.is_dir():
        raise ValueError("workspace_not_directory")
    return canonical


def _nullable(kind: str, **constraints: Any) -> dict[str, Any]:
    return {"type": [kind, "null"], **constraints}


def _string(*, nullable: bool = False, max_length: int = 1000) -> dict[str, Any]:
    if nullable:
        return _nullable("string", maxLength=max_length)
    return {"type": "string", "maxLength": max_length}


def _array(items: dict[str, Any], *, max_items: int, min_items: int = 0) -> dict[str, Any]:
    return {
        "type": "array",
        "items": items,
        "minItems": min_items,
        "maxItems": max_items,
    }


def _closed(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
        "required": list(properties),
    }


def _status(*values: str) -> dict[str, Any]:
    return {"type": "string", "enum": list(values)}


def _evidence_schema() -> dict[str, Any]:
    return _closed(
        {
            "query": _string(nullable=True, max_length=500),
            "source_url": _string(max_length=2048),
            "canonical_url": _string(nullable=True, max_length=2048),
            "publisher": _string(nullable=True, max_length=200),
            "evidence_text": _string(max_length=1000),
            "published_at": _string(nullable=True, max_length=64),
            "retrieved_at": _string(nullable=True, max_length=64),
        }
    )


def _validated_claim_schema() -> dict[str, Any]:
    return _closed(
        {
            "topic": _string(max_length=100),
            "field_semantics": {
                "type": "string",
                "enum": [
                    "forecast",
                    "consensus",
                    "previous",
                    "outcome",
                    "transcript_url",
                    "news",
                    "exploratory_context",
                ],
            },
            # Numeric official actuals are deliberately impossible in this AI contract.
            "value": _nullable("string", maxLength=1000),
            "metric_id": _string(nullable=True, max_length=120),
            "period": _string(nullable=True, max_length=80),
            "frequency": _string(nullable=True, max_length=80),
            "unit": _string(nullable=True, max_length=80),
            "event_key": _string(nullable=True, max_length=300),
            "symbol": _string(nullable=True, max_length=16),
            "valid_from": _string(nullable=True, max_length=64),
            "valid_until": _string(nullable=True, max_length=64),
            "published_at": _string(nullable=True, max_length=64),
            "retrieved_at": _string(nullable=True, max_length=64),
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "topic_status": _status("SUPPORTED", "NOT_APPLICABLE"),
            "evidence": _array(_evidence_schema(), max_items=12),
            "warnings": _array(_string(max_length=300), max_items=20),
        }
    )


def step_output_schema(
    step_name: str,
    effective_budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    budget = effective_budget or {}
    observe = str(budget.get("budget_mode") or "enforce") == "observe"
    max_searches = max(
        int(
            budget.get("max_searches", 8)
            if observe
            else budget.get("remaining_searches", 8)
        ),
        0,
    )
    max_opened_sources = max(
        int(
            budget.get("max_opened_sources", 12)
            if observe
            else budget.get("remaining_opened_sources", 12)
        ),
        0,
    )
    schemas: dict[str, dict[str, Any]] = {
        "PLAN": _closed(
            {
                "status": _status("COMPLETED", "NO_DATA"),
                "topics": _array(_string(max_length=100), max_items=20),
                "queries": _array(
                    _closed(
                        {
                            "query": _string(max_length=500),
                            "purpose": _string(max_length=300),
                            "topic": _string(max_length=100),
                        }
                    ),
                    max_items=max_searches,
                ),
                "stop_conditions": _array(_string(max_length=300), max_items=12),
                "warnings": _array(_string(max_length=300), max_items=20),
            }
        ),
        "SEARCH": _closed(
            {
                "status": _status("COMPLETED", "NO_DATA"),
                "searches": _array(
                    _closed(
                        {
                            "query": _string(max_length=500),
                            "discovered_urls": _array(
                                _string(max_length=2048),
                                max_items=20,
                            ),
                        }
                    ),
                    max_items=max_searches,
                ),
                "sources": _array(
                    _closed(
                        {
                            "query": _string(max_length=500),
                            "source_url": _string(max_length=2048),
                            "title": _string(nullable=True, max_length=500),
                            "publisher": _string(nullable=True, max_length=200),
                        }
                    ),
                    max_items=max_opened_sources,
                ),
                "warnings": _array(_string(max_length=300), max_items=20),
            }
        ),
        "OPEN_SOURCE": _closed(
            {
                "status": _status("COMPLETED", "NO_DATA"),
                "sources": _array(
                    _closed(
                        {
                            "source_url": _string(max_length=2048),
                            "canonical_url": _string(nullable=True, max_length=2048),
                            "redirect_url": _string(nullable=True, max_length=2048),
                            "publisher": _string(nullable=True, max_length=200),
                            "published_at": _string(nullable=True, max_length=64),
                            "retrieved_at": _string(nullable=True, max_length=64),
                            "http_status": _nullable("integer", minimum=100, maximum=599),
                            "source_status": _status(
                                "OPENED",
                                "NOT_FOUND",
                                "BLOCKED",
                                "UNAVAILABLE",
                            ),
                            "evidence_available": {"type": "boolean"},
                            "content_hash": _string(nullable=True, max_length=128),
                        }
                    ),
                    max_items=max_opened_sources,
                ),
                "warnings": _array(_string(max_length=300), max_items=20),
            }
        ),
        "EXTRACT": _closed(
            {
                "status": _status("COMPLETED", "NO_DATA"),
                "claims": _array(
                    _closed(
                        {
                            "claim_ref": _string(max_length=120),
                            "topic": _string(max_length=100),
                            "field_semantics": {
                                "type": "string",
                                "enum": [
                                    "forecast",
                                    "consensus",
                                    "previous",
                                    "outcome",
                                    "transcript_url",
                                    "news",
                                    "exploratory_context",
                                ],
                            },
                            "value": _nullable("string", maxLength=1000),
                            "metric_id": _string(nullable=True, max_length=120),
                            "period": _string(nullable=True, max_length=80),
                            "frequency": _string(nullable=True, max_length=80),
                            "unit": _string(nullable=True, max_length=80),
                            "evidence": _array(_evidence_schema(), max_items=12),
                            "warnings": _array(_string(max_length=300), max_items=20),
                        }
                    ),
                    max_items=40,
                ),
                "warnings": _array(_string(max_length=300), max_items=20),
            }
        ),
        "CROSS_CHECK": _closed(
            {
                "status": _status("COMPLETED", "NO_DATA"),
                "claims": _array(
                    _closed(
                        {
                            "claim_ref": _string(max_length=120),
                            "conflict": {"type": "boolean"},
                            "independent_source_urls": _array(
                                _string(max_length=2048),
                                max_items=12,
                            ),
                            "syndication_suspected": {"type": "boolean"},
                            "resolution": _status(
                                "SUPPORTED",
                                "CONFLICTING",
                                "INSUFFICIENT",
                                "NOT_APPLICABLE",
                            ),
                            "warnings": _array(_string(max_length=300), max_items=20),
                        }
                    ),
                    max_items=40,
                ),
                "warnings": _array(_string(max_length=300), max_items=20),
            }
        ),
        "VALIDATE": _closed(
            {
                "status": _status("SUCCEEDED", "PARTIAL", "NO_DATA"),
                "claims": _array(_validated_claim_schema(), max_items=40),
                "missing_topics": _array(_string(max_length=100), max_items=20),
                "blocking_gaps": _array(_string(max_length=300), max_items=20),
                "warnings": _array(_string(max_length=300), max_items=20),
            }
        ),
    }
    try:
        schema = schemas[step_name]
    except KeyError as exc:
        raise ValueError(f"unsupported_research_step:{step_name}") from exc
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        **schema,
    }


def all_step_output_schemas(
    effective_budget: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    return {
        step: step_output_schema(step, effective_budget)
        for step in EXTERNAL_RESEARCH_STEPS
    }


def legacy_research_output_schema() -> dict[str, Any]:
    metric = _closed(
        {
            "metric_id": _string(max_length=120),
            "label": _string(nullable=True, max_length=200),
            "value_type": _string(nullable=True, max_length=80),
            "frequency": _string(nullable=True, max_length=80),
            "forecast": _string(nullable=True, max_length=100),
            "consensus": _string(nullable=True, max_length=100),
            "previous": _string(nullable=True, max_length=100),
            "actual": {"type": "null"},
            "unit": _string(nullable=True, max_length=80),
            "source": _string(nullable=True, max_length=200),
            "source_url": _string(nullable=True, max_length=2048),
            "evidence_text": _string(nullable=True, max_length=1000),
            "retrieved_at": _string(nullable=True, max_length=64),
            "valid_until": _string(nullable=True, max_length=64),
            "reliability": {"type": "number", "minimum": 0, "maximum": 1},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "warnings": _array(_string(max_length=300), max_items=20),
        }
    )
    result = _closed(
        {
            "fact_key": _string(max_length=300),
            "country": _string(nullable=True, max_length=8),
            "date": _string(nullable=True, max_length=32),
            "time_utc": _string(nullable=True, max_length=64),
            "category": _string(nullable=True, max_length=120),
            "event_name": _string(nullable=True, max_length=300),
            "period": _string(nullable=True, max_length=80),
            "metric_id": _string(nullable=True, max_length=120),
            "forecast": _string(nullable=True, max_length=100),
            "previous": _string(nullable=True, max_length=100),
            "consensus": _string(nullable=True, max_length=100),
            "actual": {"type": "null"},
            "unit": _string(nullable=True, max_length=80),
            "frequency": _string(nullable=True, max_length=80),
            "source": _string(nullable=True, max_length=200),
            "source_url": _string(nullable=True, max_length=2048),
            "extracted_text": _string(nullable=True, max_length=1000),
            "evidence_text": _string(nullable=True, max_length=1000),
            "reliability": {"type": "number", "minimum": 0, "maximum": 1},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "valid_until": _string(nullable=True, max_length=64),
            "notes": _string(nullable=True, max_length=1000),
            "warnings": _array(_string(max_length=300), max_items=20),
            "metrics": _array(metric, max_items=20),
            "fomc_context": _string(nullable=True, max_length=1000),
        }
    )
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        **_closed(
            {
                "generated_at": _string(max_length=64),
                "results": _array(result, max_items=20),
            }
        ),
    }


def validate_output_schema(schema: dict[str, Any]) -> None:
    errors: list[str] = []

    def visit(node: Any, path: str) -> None:
        if not isinstance(node, dict):
            errors.append(f"{path}:schema_node_not_object")
            return
        node_type = node.get("type")
        types = {node_type} if isinstance(node_type, str) else set(node_type or [])
        if "object" in types:
            properties = node.get("properties")
            if node.get("additionalProperties") is not False:
                errors.append(f"{path}:object_not_closed")
            if not isinstance(properties, dict) or not properties:
                errors.append(f"{path}:object_properties_missing")
            else:
                required = node.get("required")
                if not isinstance(required, list) or set(required) != set(properties):
                    errors.append(f"{path}:required_must_match_properties")
                for key, child in properties.items():
                    visit(child, f"{path}.{key}")
        if "array" in types:
            if "items" not in node:
                errors.append(f"{path}:array_items_missing")
            else:
                visit(node["items"], f"{path}[]")
            if not isinstance(node.get("maxItems"), int):
                errors.append(f"{path}:array_max_items_missing")
        enum = node.get("enum")
        if isinstance(enum, list) and "actual" in enum:
            errors.append(f"{path}:ai_actual_semantics_forbidden")

    visit(schema, "$")
    if errors:
        raise ValueError("invalid_output_schema:" + ",".join(errors[:20]))


def validate_payload(payload: Any, schema: dict[str, Any]) -> None:
    errors: list[str] = []

    def matches_type(value: Any, kind: str) -> bool:
        if kind == "null":
            return value is None
        if kind == "object":
            return isinstance(value, dict)
        if kind == "array":
            return isinstance(value, list)
        if kind == "string":
            return isinstance(value, str)
        if kind == "boolean":
            return isinstance(value, bool)
        if kind == "integer":
            return isinstance(value, int) and not isinstance(value, bool)
        if kind == "number":
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        return False

    def visit(value: Any, node: dict[str, Any], path: str) -> None:
        raw_types = node.get("type")
        types = [raw_types] if isinstance(raw_types, str) else list(raw_types or [])
        if not any(matches_type(value, kind) for kind in types):
            errors.append(f"{path}:type")
            return
        if value is None:
            return
        if "enum" in node and value not in node["enum"]:
            errors.append(f"{path}:enum")
        if isinstance(value, dict):
            properties = node.get("properties") or {}
            missing = set(node.get("required") or []) - set(value)
            unexpected = set(value) - set(properties)
            if missing:
                errors.append(f"{path}:missing:{','.join(sorted(missing))}")
            if unexpected:
                errors.append(f"{path}:unexpected:{','.join(sorted(unexpected))}")
            for key in set(value).intersection(properties):
                visit(value[key], properties[key], f"{path}.{key}")
        elif isinstance(value, list):
            if len(value) < int(node.get("minItems") or 0):
                errors.append(f"{path}:minItems")
            if "maxItems" in node and len(value) > int(node["maxItems"]):
                errors.append(f"{path}:maxItems")
            for index, item in enumerate(value):
                visit(item, node["items"], f"{path}[{index}]")
        elif isinstance(value, str) and len(value) > int(node.get("maxLength") or len(value)):
            errors.append(f"{path}:maxLength")
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            if "minimum" in node and value < node["minimum"]:
                errors.append(f"{path}:minimum")
            if "maximum" in node and value > node["maximum"]:
                errors.append(f"{path}:maximum")

    visit(payload, schema, "$")
    if errors:
        raise ValueError("output_contract_incompatible:" + ",".join(errors[:20]))


def build_codex_exec_command(
    command_prefix: list[str],
    *,
    workspace: Path,
    schema_path: Path,
    output_path: Path,
) -> list[str]:
    return [
        *command_prefix,
        "--search",
        "--sandbox",
        "read-only",
        "--cd",
        str(workspace),
        "exec",
        "--skip-git-repo-check",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--json",
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(output_path),
        "--color",
        "never",
        "-",
    ]


def validate_isolated_command(
    command: list[str],
    prompt: str | None = None,
    *,
    cwd: Path | None = None,
) -> None:
    required = {
        "--search",
        "read-only",
        "--skip-git-repo-check",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--json",
        "--output-schema",
        "--output-last-message",
        "--color",
        "never",
        "-",
    }
    missing = required - set(command)
    if missing:
        raise ValueError(f"isolated_command_missing:{','.join(sorted(missing))}")
    if prompt and any(prompt == part or prompt in part for part in command):
        raise ValueError("prompt_present_in_command")
    if command[-1] != "-":
        raise ValueError("prompt_must_be_stdin")
    workspace = _command_path(command, "--cd")
    schema_path = _command_path(command, "--output-schema")
    output_path = _command_path(command, "--output-last-message")
    canonical_cwd = _canonical_command_path(
        cwd if cwd is not None else workspace,
        "cwd",
        must_exist=True,
    )
    canonical_workspace = _canonical_command_path(
        workspace,
        "workspace",
        must_exist=True,
    )
    canonical_schema = _canonical_command_path(
        schema_path,
        "schema",
        must_exist=True,
    )
    canonical_output = _canonical_command_path(
        output_path,
        "output",
        must_exist=False,
    )
    if not canonical_cwd.is_dir() or not canonical_workspace.is_dir():
        raise ValueError("workspace_not_directory")
    if not canonical_schema.is_file():
        raise ValueError("schema_file_missing")
    if not canonical_output.parent.is_dir():
        raise ValueError("output_parent_missing")
    if not _same_path(canonical_cwd, canonical_workspace):
        raise ValueError("cwd_workspace_mismatch")
    if not _is_descendant(canonical_schema, canonical_workspace):
        raise ValueError("schema_outside_workspace")
    if not _is_descendant(canonical_output, canonical_workspace):
        raise ValueError("output_outside_workspace")


def _command_path(command: list[str], flag: str) -> Path:
    try:
        index = command.index(flag)
        value = command[index + 1]
    except (ValueError, IndexError) as exc:
        raise ValueError(f"isolated_command_missing_value:{flag}") from exc
    return Path(value)


def _canonical_command_path(path: Path, label: str, *, must_exist: bool) -> Path:
    if not path.is_absolute():
        raise ValueError(f"{label}_path_relative")
    if ".." in path.parts:
        raise ValueError(f"{label}_path_traversal")
    try:
        canonical = path.resolve(strict=must_exist)
    except OSError as exc:
        raise ValueError(f"{label}_path_invalid:{type(exc).__name__}") from exc
    if not _same_path(path, canonical):
        raise ValueError(f"{label}_path_not_canonical")
    return canonical


def _same_path(left: Path, right: Path) -> bool:
    return os.path.normcase(os.path.abspath(str(left))) == os.path.normcase(
        os.path.abspath(str(right))
    )


def _is_descendant(path: Path, workspace: Path) -> bool:
    normalized_path = os.path.normcase(os.path.abspath(str(path)))
    normalized_workspace = os.path.normcase(os.path.abspath(str(workspace)))
    try:
        return os.path.commonpath((normalized_path, normalized_workspace)) == (
            normalized_workspace
        )
    except ValueError:
        return False


def inherited_instruction_files(workspace: Path) -> list[str]:
    resolved = workspace.resolve()
    candidates = [resolved, *resolved.parents]
    return [
        str(path / "AGENTS.md")
        for path in candidates
        if (path / "AGENTS.md").is_file()
    ]


def safe_subprocess_environment() -> dict[str, str]:
    allowed = {
        "PATH",
        "PATHEXT",
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "TEMP",
        "TMP",
        "USERPROFILE",
        "APPDATA",
        "LOCALAPPDATA",
        "PROGRAMDATA",
        "CODEX_HOME",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "LANG",
        "LC_ALL",
    }
    return {key: value for key, value in os.environ.items() if key.upper() in allowed}


class CodexCLIError(RuntimeError):
    def __init__(self, diagnostic: dict[str, Any]) -> None:
        self.diagnostic = sanitize_diagnostic(diagnostic)
        self.category = str(self.diagnostic["category"])
        self.retryable = bool(self.diagnostic["retryable"])
        self.retry_classification = str(self.diagnostic["retry_classification"])
        exit_code = self.diagnostic.get("exit_code")
        code = f"codex_cli:{self.category.lower()}"
        if exit_code is not None:
            code = f"{code}:exit_{exit_code}"
        self.code = code[:240]
        super().__init__(self.code)


def classify_codex_failure(
    *,
    exit_code: int | None,
    stderr: str = "",
    error_events: list[dict[str, Any]] | None = None,
    forced_category: str | None = None,
) -> tuple[str, bool]:
    if forced_category:
        category = forced_category.upper()
    else:
        combined = " ".join(
            [
                stderr,
                json.dumps(error_events or [], ensure_ascii=False, default=str),
            ]
        ).lower()
        patterns = (
            (
                "LOOP_DETECTED",
                ("research_loop_detected", "loop detected"),
            ),
            (
                "BUDGET_EXCEEDED",
                ("research_budget_exceeded", "budget exceeded"),
            ),
            (
                "PATH_INVALID",
                (
                    "os error 3",
                    "path not found",
                    "file not found",
                    "impossibile trovare il percorso specificato",
                    "impossibile trovare il file specificato",
                ),
            ),
            ("SCHEMA_INVALID", ("invalid schema", "output schema", "json schema")),
            (
                "UNSUPPORTED_ARGUMENT",
                ("unexpected argument", "unrecognized option", "unknown option", "invalid value"),
            ),
            ("CONFIG_INVALID", ("config.toml", "configuration error", "invalid config")),
            (
                "AUTH_UNAVAILABLE",
                ("not logged in", "authentication", "unauthorized", "login required", "401"),
            ),
            (
                "POLICY_REJECTION",
                ("policy rejection", "policy violation", "request rejected by policy"),
            ),
            ("RATE_LIMIT", ("rate limit", "too many requests", "429")),
            (
                "TIMEOUT",
                ("timed out", "timeout", "deadline exceeded", "watchdog"),
            ),
            (
                "NETWORK_TRANSIENT",
                (
                    "connection reset",
                    "connection refused",
                    "temporarily unavailable",
                    "temporary failure",
                    "dns",
                    "network",
                ),
            ),
            (
                "BACKEND_5XX",
                ("status 500", "status 502", "status 503", "status 504", "server error"),
            ),
            (
                "TRANSIENT_INTERRUPTION",
                ("stream interrupted", "connection closed", "try again"),
            ),
        )
        category = next(
            (name for name, tokens in patterns if any(token in combined for token in tokens)),
            "CLI_EXIT" if exit_code is not None else "UNKNOWN",
        )
    retryable = category in {
        "RATE_LIMIT",
        "TIMEOUT",
        "NETWORK_TRANSIENT",
        "BACKEND_5XX",
        "TRANSIENT_INTERRUPTION",
    }
    return category, retryable


def build_diagnostic(
    *,
    category: str,
    retryable: bool,
    command: list[str],
    step: str,
    workspace: Path,
    duration_ms: int,
    executable_version: str | None,
    exit_code: int | None = None,
    stderr: str = "",
    stdout: str = "",
    error_events: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return sanitize_diagnostic(
        {
            "category": category,
            "exit_code": exit_code,
            "stderr_tail": _tail(stderr),
            "stdout_tail": _tail(stdout),
            "error_events": (error_events or [])[-DIAGNOSTIC_EVENT_LIMIT:],
            "command_shape": [redact_sensitive(str(item)) for item in command],
            "executable_version": executable_version,
            "step": step,
            "duration_ms": max(int(duration_ms), 0),
            "workspace": str(workspace),
            "timestamp": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "retryable": retryable,
            "retry_classification": "RETRYABLE" if retryable else "NON_RETRYABLE",
        }
    )


def sanitize_diagnostic(diagnostic: dict[str, Any]) -> dict[str, Any]:
    safe = redact_payload(diagnostic)
    safe["category"] = str(safe.get("category") or "UNKNOWN")[:80].upper()
    safe["retryable"] = bool(safe.get("retryable"))
    safe["retry_classification"] = (
        "RETRYABLE" if safe["retryable"] else "NON_RETRYABLE"
    )
    for key in ("stderr_tail", "stdout_tail"):
        safe[key] = _tail(str(safe.get(key) or ""))
    events = safe.get("error_events")
    bounded_events: list[Any] = []
    if isinstance(events, list):
        for event in events[-DIAGNOSTIC_EVENT_LIMIT:]:
            serialized = json.dumps(event, ensure_ascii=False, default=str)
            if len(serialized) <= DIAGNOSTIC_EVENT_TEXT_LIMIT:
                bounded_events.append(event)
            else:
                bounded_events.append(
                    {
                        "truncated": True,
                        "tail": serialized[-DIAGNOSTIC_EVENT_TEXT_LIMIT:],
                    }
                )
    safe["error_events"] = bounded_events
    # Prompt, environment and authentication material are never valid diagnostic fields.
    for key in list(safe):
        if key.lower() in {
            "prompt",
            "environment",
            "env",
            "credentials",
            "cookies",
            "auth",
            "auth_json",
            "config",
        }:
            safe.pop(key, None)
    return safe


def _tail(value: str) -> str:
    return redact_sensitive(str(value or "")[-DIAGNOSTIC_TEXT_LIMIT:])
