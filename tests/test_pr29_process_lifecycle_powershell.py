from __future__ import annotations

from pathlib import Path
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_pr29_process_lifecycle_under_windows_powershell_51() -> None:
    script = (
        REPO_ROOT
        / "tests"
        / "powershell"
        / "test_pr29_process_lifecycle.ps1"
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
    assert "POWERSHELL_5_1_STRICTMODE_TESTS_PASS=9" in completed.stdout
