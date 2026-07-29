from __future__ import annotations

from pathlib import Path
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_pr29_validation_harness_end_to_end_offline() -> None:
    script = (
        REPO_ROOT
        / "tests"
        / "powershell"
        / "test_pr29_validation_harness_e2e.ps1"
    )
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
        ],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert completed.returncode == 0, (
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )
    assert "OFFLINE_HARNESS_END_TO_END_PASS" in completed.stdout
    assert '"external_provider_network_calls":  0' in completed.stdout
    assert '"postflight_port_free":  true' in completed.stdout
