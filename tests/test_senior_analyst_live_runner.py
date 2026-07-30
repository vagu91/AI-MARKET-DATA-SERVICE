from __future__ import annotations

from pathlib import Path
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "scripts" / "run-senior-analyst-live.ps1"
LEGACY_RUNNER = REPO_ROOT / "scripts" / "run-senior-analyst-live-validation.ps1"


def test_bat_compatible_runner_is_the_only_permanent_live_runner() -> None:
    assert RUNNER.is_file()
    assert not LEGACY_RUNNER.exists()
    assert sorted(
        path.name for path in (REPO_ROOT / "scripts").glob("run-senior-analyst-live*.ps1")
    ) == ["run-senior-analyst-live.ps1"]


def test_runner_publishes_exact_body_pointer_only_after_validation_pass() -> None:
    source = RUNNER.read_text(encoding="utf-8")
    assert 'data\\senior-analyst-live-latest.json"' in source
    assert "Get-FileHash -LiteralPath $PayloadPath -Algorithm SHA256" in source
    assert "full_payload_path = [IO.Path]::GetFullPath($PayloadPath)" in source
    assert "full_payload_sha256 = $payloadHash" in source
    assert 'result = "PASS"' in source
    assert '$report.status -ne "PASS"' in source
    assert "[IO.File]::Replace($temporary, $Destination, $null)" in source
    assert "[IO.File]::Move($temporary, $Destination)" in source
    assert source.index("if ($LASTEXITCODE -ne 0)") < source.index("Publish-LatestAcceptance `")


def test_bat_compatible_runner_parses_under_windows_powershell_51() -> None:
    escaped = str(RUNNER).replace("'", "''")
    command = (
        "$errors=$null;$tokens=$null;"
        "[void][System.Management.Automation.Language.Parser]::ParseFile("
        f"'{escaped}',[ref]$tokens,[ref]$errors);"
        "if($errors.Count -ne 0){"
        "$errors|ForEach-Object{Write-Error $_};exit 1};"
        "'POWERSHELL_5_1_PARSE_PASS'"
    )
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            command,
        ],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert "POWERSHELL_5_1_PARSE_PASS" in completed.stdout
