from __future__ import annotations

import json
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.migrations import migrate_database
from app.providers.ai_researcher_provider import _resolve_command
from app.services.codex_runtime_contract import (
    agentic_research_output_schema,
    all_step_output_schemas,
    build_codex_exec_command,
    canonicalize_workspace,
    inherited_instruction_files,
    legacy_research_output_schema,
    safe_subprocess_environment,
    validate_isolated_command,
    validate_output_schema,
)
from app.services.source_policy_service import SourcePolicyService
from app.services.research_budget import build_effective_budget
from app.services.research_profiles import PROFILES
from app.services.research_runtime_repository import ResearchRuntimeRepository


CAPABILITY_STATES = {
    "READY_TO_SMOKE",
    "LIVE_VERIFIED",
    "NOT_CONFIGURED",
    "AUTH_UNAVAILABLE",
    "SCHEMA_INVALID",
    "EXECUTOR_UNAVAILABLE",
    "WEB_UNAVAILABLE",
    "DEGRADED",
}


class AIResearchCapabilityService:
    def __init__(
        self,
        settings: Settings,
        *,
        runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self.settings = settings
        self.runner = runner or subprocess.run
        migrate_database(settings.database_path)

    def probe(self, *, persist: bool = True) -> dict[str, Any]:
        command = _resolve_command(self.settings.codex_cli_command)
        configured = bool(
            self.settings.enable_ai_researcher and self.settings.ai_researcher_mode == "codex_cli"
        )
        executable_available = bool(command)
        version_result = self._run([*(command or []), "--version"]) if command else None
        help_result = self._run([*(command or []), "--help"]) if command else None
        exec_help_result = self._run([*(command or []), "exec", "--help"]) if command else None
        auth_result = self._run([*(command or []), "login", "status"]) if command else None
        version = _safe_version(
            version_result.stdout if version_result and version_result.returncode == 0 else ""
        )
        global_help = (
            help_result.stdout if help_result is not None and help_result.returncode == 0 else ""
        )
        exec_help = (
            exec_help_result.stdout
            if exec_help_result is not None and exec_help_result.returncode == 0
            else ""
        )
        execution_supported = "exec" in global_help and bool(
            exec_help_result is not None and exec_help_result.returncode == 0
        )
        web_flag_supported = "--search" in global_help
        required_exec_options = {
            "--skip-git-repo-check",
            "--ephemeral",
            "--ignore-user-config",
            "--ignore-rules",
            "--output-schema",
            "--output-last-message",
            "--color",
            "--json",
        }
        available_exec_options = {option for option in required_exec_options if option in exec_help}
        isolation_options_supported = available_exec_options == required_exec_options
        structured_output_supported = {
            "--output-schema",
            "--output-last-message",
            "--json",
        } <= available_exec_options
        auth_available = bool(
            auth_result is not None
            and auth_result.returncode == 0
            and "logged in" in f"{auth_result.stdout} {auth_result.stderr}".lower()
        )
        workspace_root = Path(self.settings.ai_job_workspace_root)
        workspace_writable = _workspace_writable(workspace_root)
        inherited_instructions = inherited_instruction_files(workspace_root)
        budget_repository = ResearchRuntimeRepository(self.settings)
        daily_usage = budget_repository.daily_budget_usage()
        effective_budget = build_effective_budget(
            self.settings,
            required_topics=list(PROFILES["MNQ_MARKET_RESEARCH"].required_topics),
            daily_usage=daily_usage,
            daily_runs=daily_usage["run_count"],
            runtime_seconds=self.settings.ai_job_max_runtime_seconds,
        )
        budget_contract_valid = bool(
            effective_budget["max_searches"] > 0
            and effective_budget["max_opened_sources"] > 0
            and effective_budget["query_topic_groups"]
            and (
                effective_budget["budget_mode"] == "observe"
                or effective_budget["daily_runs_remaining"] > 0
            )
        )
        schema_errors: dict[str, str] = {}
        schemas = {
            **all_step_output_schemas(effective_budget),
            "AGENTIC_RESEARCH": agentic_research_output_schema(effective_budget),
            "LEGACY_BATCH": legacy_research_output_schema(),
        }
        for step, schema in schemas.items():
            try:
                validate_output_schema(schema)
            except ValueError as exc:
                schema_errors[step] = str(exc)[:500]
        schemas_valid = not schema_errors
        command_isolated = False
        command_shape: list[str] = []
        if command and workspace_writable:
            try:
                workspace = canonicalize_workspace(workspace_root / "capability-offline-probe")
                schema_path = workspace / "output_schema.json"
                schema_path.write_text(
                    json.dumps(
                        schemas[
                            "AGENTIC_RESEARCH"
                            if self.settings.research_single_invocation_enabled
                            else "PLAN"
                        ],
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                output_path = workspace / "output.json"
                command_shape = build_codex_exec_command(
                    command,
                    workspace=workspace,
                    schema_path=schema_path,
                    output_path=output_path,
                )
                validate_isolated_command(command_shape, cwd=workspace)
                command_isolated = True
            except (OSError, ValueError):
                command_isolated = False
        try:
            policy = SourcePolicyService(self.settings.source_policy_path)
            policy_loaded = True
            policy_version = policy.policy_version
        except Exception:
            policy_loaded = False
            policy_version = None
        live_verification = self.latest_live_verification()
        live_verified = live_verification is not None
        web_configured = bool(self.settings.ai_research_web_access_enabled and web_flag_supported)
        worker_active = bool(self.settings.ai_worker_enabled)
        if not configured:
            status = "NOT_CONFIGURED"
        elif not executable_available or not execution_supported or not isolation_options_supported:
            status = "EXECUTOR_UNAVAILABLE"
        elif not auth_available:
            status = "AUTH_UNAVAILABLE"
        elif not schemas_valid:
            status = "SCHEMA_INVALID"
        elif not budget_contract_valid:
            status = "DEGRADED"
        elif not web_flag_supported or not self.settings.ai_research_web_access_enabled:
            status = "WEB_UNAVAILABLE"
        elif (
            not workspace_writable
            or not policy_loaded
            or not command_isolated
            or bool(inherited_instructions)
        ):
            status = "DEGRADED"
        elif live_verified and web_configured:
            status = "LIVE_VERIFIED"
        elif web_configured and worker_active:
            status = "READY_TO_SMOKE"
        else:
            status = "DEGRADED"
        report = {
            "status": status,
            "checked_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "backend": self.settings.ai_researcher_mode,
            "configured": configured,
            "executable_available": executable_available,
            "executable_version": version,
            "authentication_available": auth_available,
            "execution_supported": execution_supported,
            "web_search_flag_supported": web_flag_supported,
            "web_access_enabled": bool(self.settings.ai_research_web_access_enabled),
            "web_search_available": live_verified and web_configured,
            "live_web_verified": live_verified,
            "live_web_verified_at": live_verification.get("verified_at")
            if live_verification
            else None,
            "structured_output_supported": structured_output_supported,
            "output_last_message_supported": "--output-last-message" in available_exec_options,
            "jsonl_events_supported": "--json" in available_exec_options,
            "ignore_user_config_supported": "--ignore-user-config" in available_exec_options,
            "ignore_rules_supported": "--ignore-rules" in available_exec_options,
            "isolation_options_supported": isolation_options_supported,
            "isolated_command_constructed": command_isolated,
            "schema_validation_status": "VALID" if schemas_valid else "INVALID",
            "schema_errors": schema_errors,
            "effective_budget": effective_budget,
            "budget_contract_valid": budget_contract_valid,
            "workspace_writable": workspace_writable,
            "inherited_instruction_files": inherited_instructions,
            "agent_instructions_isolated": not inherited_instructions,
            "process_group_watchdog_available": True,
            "source_policy_loaded": policy_loaded,
            "source_policy_version": policy_version,
            "worker_active": worker_active,
            "secrets_exposed": False,
            "live_verification_note": (
                "READY_TO_SMOKE is an offline readiness state, not proof of live web execution."
                if status == "READY_TO_SMOKE"
                else None
            ),
        }
        if persist:
            self._persist(report, str((command or [None])[0] or ""))
        return report

    def record_live_verification(
        self,
        evidence: dict[str, Any],
        *,
        executable_version: str | None = None,
    ) -> dict[str, Any]:
        if not evidence.get("observed_search_count") or not evidence.get("opened_source_count"):
            raise ValueError(
                "live verification requires observed search and opened source telemetry"
            )
        now = datetime.now(UTC).replace(microsecond=0).isoformat()
        verification_id = f"live-{uuid.uuid4()}"
        sanitized = {
            "observed_search_count": int(evidence["observed_search_count"]),
            "opened_source_count": int(evidence["opened_source_count"]),
            "source_domains": sorted({str(item) for item in evidence.get("source_domains") or []}),
            "run_id": evidence.get("run_id"),
        }
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT INTO ai_research_live_verifications(
                  verification_id,backend,executable_version,verified_at,expires_at,evidence_json,created_at
                ) VALUES (?,?,?,?,NULL,?,?)
                """,
                (
                    verification_id,
                    self.settings.ai_researcher_mode,
                    executable_version,
                    now,
                    json.dumps(sanitized, sort_keys=True, separators=(",", ":")),
                    now,
                ),
            )
            conn.commit()
        return {"verification_id": verification_id, "verified_at": now, **sanitized}

    def latest_live_verification(self) -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                """
                SELECT * FROM ai_research_live_verifications
                WHERE backend=? AND (expires_at IS NULL OR expires_at>?)
                ORDER BY verified_at DESC,rowid DESC LIMIT 1
                """,
                (
                    self.settings.ai_researcher_mode,
                    datetime.now(UTC).replace(microsecond=0).isoformat(),
                ),
            ).fetchone()
        if row is None:
            return None
        output = dict(row)
        output["evidence"] = json.loads(output.pop("evidence_json") or "{}")
        return output

    def latest(self) -> dict[str, Any] | None:
        with connect_sqlite(self.settings.database_path) as conn:
            row = conn.execute(
                "SELECT report_json FROM ai_research_capability_reports ORDER BY created_at DESC,rowid DESC LIMIT 1"
            ).fetchone()
        return json.loads(row["report_json"]) if row else None

    def _run(self, command: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            return self.runner(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=10,
                check=False,
                env=safe_subprocess_environment(),
            )
        except (OSError, subprocess.SubprocessError) as exc:
            return subprocess.CompletedProcess(command, 1, "", type(exc).__name__)

    def _persist(self, report: dict[str, Any], executable_path: str) -> None:
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT INTO ai_research_capability_reports(
                  report_id,status,backend,executable_path,executable_version,report_json,created_at
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (
                    f"cap-{uuid.uuid4()}",
                    report["status"],
                    report["backend"],
                    executable_path,
                    report.get("executable_version"),
                    json.dumps(report, sort_keys=True, separators=(",", ":")),
                    report["checked_at"],
                ),
            )
            conn.commit()


def _workspace_writable(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / f".capability-{uuid.uuid4()}.tmp"
        probe.write_text("probe", encoding="utf-8")
        probe.unlink()
        return True
    except OSError:
        return False


def _safe_version(output: str) -> str | None:
    text = " ".join(str(output or "").split())[:120]
    return text or None
