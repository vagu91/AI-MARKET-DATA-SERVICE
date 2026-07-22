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
from app.services.source_policy_service import SourcePolicyService


CAPABILITY_STATES = {"CONFIGURED", "READY_TO_SMOKE", "LIVE_VERIFIED", "DEGRADED", "UNAVAILABLE"}


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
        configured = bool(self.settings.enable_ai_researcher and self.settings.ai_researcher_mode == "codex_cli")
        executable_available = bool(command)
        version_result = self._run([*(command or []), "--version"]) if command else None
        help_result = self._run([*(command or []), "--help"]) if command else None
        exec_help_result = self._run([*(command or []), "exec", "--help"]) if command else None
        auth_result = self._run([*(command or []), "login", "status"]) if command else None
        version = _safe_version(version_result.stdout if version_result and version_result.returncode == 0 else "")
        help_text = "\n".join(
            result.stdout for result in (help_result, exec_help_result)
            if result is not None and result.returncode == 0
        )
        execution_supported = "exec" in help_text and (exec_help_result is not None and exec_help_result.returncode == 0)
        web_flag_supported = "--search" in help_text
        structured_output_supported = "--output-schema" in help_text and "--json" in help_text
        auth_available = bool(
            auth_result is not None
            and auth_result.returncode == 0
            and "logged in" in f"{auth_result.stdout} {auth_result.stderr}".lower()
        )
        workspace_writable = _workspace_writable(Path(self.settings.ai_job_workspace_root))
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
            status = "UNAVAILABLE"
        elif not executable_available or not execution_supported or not structured_output_supported:
            status = "UNAVAILABLE"
        elif not auth_available or not workspace_writable or not policy_loaded:
            status = "DEGRADED"
        elif live_verified and web_configured:
            status = "LIVE_VERIFIED"
        elif web_configured and worker_active:
            status = "READY_TO_SMOKE"
        elif not self.settings.ai_research_web_access_enabled or not worker_active:
            status = "CONFIGURED"
        elif not workspace_writable or not policy_loaded or not worker_active:
            status = "DEGRADED"
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
            "live_web_verified_at": live_verification.get("verified_at") if live_verification else None,
            "structured_output_supported": structured_output_supported,
            "workspace_writable": workspace_writable,
            "process_group_watchdog_available": True,
            "source_policy_loaded": policy_loaded,
            "source_policy_version": policy_version,
            "worker_active": worker_active,
            "secrets_exposed": False,
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
            raise ValueError("live verification requires observed search and opened source telemetry")
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
                    verification_id, self.settings.ai_researcher_mode, executable_version,
                    now, json.dumps(sanitized, sort_keys=True, separators=(",", ":")), now,
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
                (self.settings.ai_researcher_mode, datetime.now(UTC).replace(microsecond=0).isoformat()),
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
                command, capture_output=True, text=True, encoding="utf-8", errors="replace",
                timeout=10, check=False,
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
                    f"cap-{uuid.uuid4()}", report["status"], report["backend"],
                    executable_path, report.get("executable_version"),
                    json.dumps(report, sort_keys=True, separators=(",", ":")), report["checked_at"],
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
