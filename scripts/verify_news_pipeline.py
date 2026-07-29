from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.news_pipeline_controlled_live import controlled_live


OFFLINE_TESTS = (
    "tests/test_news_pipeline_baseline.py",
    "tests/test_news_provider_fan_in_adversarial.py",
    "tests/test_lossless_news_pipeline_forensics.py",
    "tests/test_news_network_surface.py",
    "tests/test_pr29_process_lifecycle_powershell.py",
    "tests/test_pr29_validation_harness_e2e_powershell.py",
    "tests/test_verify_news_pipeline_harness.py",
)
FORBIDDEN_TEMPORARY_SCRIPT = re.compile(
    r"(?:stage2-v\d+|controlled-live-news-validation-stage)",
    re.IGNORECASE,
)
SECRET_PATTERNS = (
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    re.compile(r"(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})"),
    re.compile(r"\bsk-[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{24,}"),
    re.compile(
        r"(?i)(?:apikey|api_key|access_token|refresh_token|"
        r"client_secret|password)=[^&\s]{6,}"
    ),
)


def _run(
    command: list[str],
    *,
    cwd: Path,
    environment: dict[str, str],
) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=300,
    )
    result = {
        "command": [Path(command[0]).name, *command[1:]],
        "return_code": completed.returncode,
        "stdout": _redact(completed.stdout),
        "stderr": _redact(completed.stderr),
        "pass": completed.returncode == 0,
    }
    if completed.returncode != 0:
        raise RuntimeError(
            "OFFLINE_COMMAND_FAILED "
            f"command={result['command']} "
            f"stdout={result['stdout'][-2000:]} "
            f"stderr={result['stderr'][-2000:]}"
        )
    return result


def _forbidden_temporary_scripts(repo_root: Path) -> list[str]:
    violations: list[str] = []
    versioned = re.compile(r"stage2-v(?P<version>\d+)", re.IGNORECASE)
    for path in repo_root.rglob("*.ps1"):
        if not path.is_file():
            continue
        relative = path.relative_to(repo_root)
        version_match = versioned.search(path.name)
        if version_match:
            version = int(version_match.group("version"))
            legacy_v7_evidence = (
                relative.parts
                and relative.parts[0].casefold() == "data"
                and version <= 7
            )
            if not legacy_v7_evidence:
                violations.append(str(relative))
            continue
        if (
            FORBIDDEN_TEMPORARY_SCRIPT.search(path.name)
            and relative.parts
            and relative.parts[0].casefold() in {"scripts", "tests"}
        ):
            violations.append(str(relative))
    return sorted(violations)


def _secret_scan(root: Path) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for pattern in SECRET_PATTERNS:
            if pattern.search(text):
                findings.append(
                    {
                        "file": str(path.relative_to(root)),
                        "pattern": pattern.pattern,
                    }
                )
    return findings


def _file_manifest(root: Path) -> list[dict[str, Any]]:
    manifest: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name == "manifest.json":
            continue
        content = path.read_bytes()
        manifest.append(
            {
                "path": str(path.relative_to(root)).replace("\\", "/"),
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest().upper(),
            }
        )
    return manifest


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _redact(value: str) -> str:
    value = re.sub(
        r"(?i)(apikey|api_key|access_token|token|key)=([^&\s]+)",
        r"\1=REDACTED",
        value,
    )
    return re.sub(
        r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+",
        "Bearer REDACTED",
        value,
    )


def run_offline(
    *,
    repo_root: Path,
    output_root: Path,
    baseline_path: Path,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    baseline_bytes = baseline_path.read_bytes()
    baseline = json.loads(baseline_bytes)
    baseline_sha = hashlib.sha256(baseline_bytes).hexdigest().upper()
    fixture_path = repo_root / baseline["verification"]["golden_fixture"]

    violations = _forbidden_temporary_scripts(repo_root)
    if violations:
        raise RuntimeError(
            "TEMPORARY_VALIDATION_SCRIPT_FORBIDDEN "
            + ",".join(violations)
        )

    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(repo_root)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["AI_MARKET_SOURCE_POLICY_PATH"] = str(
        repo_root / "config" / "source_policy.json"
    )
    commands: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="news-pipeline-offline-") as name:
        isolated_cwd = Path(name)
        commands.append(
            _run(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    *[str(repo_root / item) for item in OFFLINE_TESTS],
                    "-q",
                ],
                cwd=isolated_cwd,
                environment=environment,
            )
        )
        commands.append(
            _run(
                [
                    sys.executable,
                    str(repo_root / "scripts" / "validate_news_network_surface.py"),
                    "--repo-root",
                    str(repo_root),
                    "--output",
                    str(output_root / "network-surface.json"),
                ],
                cwd=isolated_cwd,
                environment=environment,
            )
        )
        commands.append(
            _run(
                [
                    sys.executable,
                    str(repo_root / "scripts" / "replay_news_pipeline_golden.py"),
                    "--fixture",
                    str(fixture_path),
                    "--output",
                    str(output_root / "golden-replay.json"),
                ],
                cwd=isolated_cwd,
                environment=environment,
            )
        )

    report = {
        "mode": "OFFLINE",
        "result": "PASS",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "baseline": baseline["contract_id"],
        "baseline_sha256": baseline_sha,
        "golden_fixture": str(fixture_path.relative_to(repo_root)),
        "temporary_script_violations": violations,
        "network_calls": 0,
        "live_guard_consumed": False,
        "commands": commands,
    }
    _write_json(output_root / "offline-validation.json", report)
    findings = _secret_scan(output_root)
    _write_json(
        output_root / "secret-scan.json",
        {"pass": not findings, "findings": findings},
    )
    if findings:
        raise RuntimeError("OFFLINE_SECRET_SCAN_FAILED")
    _write_json(
        output_root / "manifest.json",
        {
            "baseline": baseline["contract_id"],
            "files": _file_manifest(output_root),
        },
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Permanent news-pipeline verification entrypoint."
    )
    parser.add_argument(
        "--mode",
        choices=("Offline", "ControlledLive"),
        required=True,
    )
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    args = parser.parse_args()
    repo_root = args.repo_root.resolve()
    output_root = args.output_root.resolve()
    baseline_path = args.baseline.resolve()

    offline = run_offline(
        repo_root=repo_root,
        output_root=output_root / "offline",
        baseline_path=baseline_path,
    )
    if args.mode == "ControlledLive":
        asyncio.run(
            controlled_live(
                repo_root=repo_root,
                output_root=output_root / "controlled-live",
                baseline_path=baseline_path,
            )
        )
    print(
        "NEWS_PIPELINE_VERIFICATION_PASS "
        f"mode={args.mode} baseline={offline['baseline']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
