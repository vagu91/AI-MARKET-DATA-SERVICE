from __future__ import annotations

from pathlib import Path
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "scripts" / "run-provider-capability-audit.ps1"
ENTRY_POINT = REPO_ROOT / "scripts" / "provider_capability_audit.py"
BAT = REPO_ROOT / "run-provider-capability-audit.bat"


def _run_powershell(command: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
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


def test_manual_runner_and_short_root_bat_are_permanent_entry_points() -> None:
    assert RUNNER.is_file()
    assert ENTRY_POINT.is_file()
    assert BAT.is_file()
    bat = BAT.read_text(encoding="utf-8")
    assert "powershell.exe -NoLogo -NoProfile" in bat
    assert "scripts\\run-provider-capability-audit.ps1" in bat
    assert "exit /b %ERRORLEVEL%" in bat
    assert len([line for line in bat.splitlines() if line.strip()]) == 3


def test_runner_defaults_to_all_capabilities_and_includes_ai() -> None:
    source = RUNNER.read_text(encoding="utf-8")
    assert "[string[]]$Provider = @()" in source
    assert "[string[]]$Dataset = @()" in source
    assert "[string[]]$Metric = @()" in source
    assert "[bool]$IncludeAI = $true" in source
    assert '$Arguments.Add("--include-ai")' in source
    assert '$Arguments.Add("--exclude-ai")' in source
    assert '$ExpectedBranch = "codex/fix-senior-analyst-payload-quality"' in source
    assert "Stop-ControlledProcessTree" in source
    assert "provider-capability-audit-latest.json" in source
    assert '"--publish-candidate"' in source
    assert "failed strong publication" in source
    assert "$FullAudit" in source
    assert "A filtered audit must not replace the full-audit latest pointer." in source
    assert "full-audit latest pointer was not changed" in source
    assert "app.main" not in source
    assert "[int]$TimeoutSeconds = 14400" in source
    publication = source.index('"--publish-candidate"')
    assert source.index("$remaining = @(") < publication
    assert source.index("$Report = Get-Content") < publication
    assert source.index("Assert-GitScope", source.index("$Report =")) < publication


def test_runner_tracks_process_identity_instead_of_historical_bare_pids() -> None:
    source = RUNNER.read_text(encoding="utf-8")
    assert "System.Collections.Generic.HashSet[int]" not in source
    assert "$ObservedProcessStartTicks = @{}" in source
    assert "function Add-ObservedProcessIdentity" in source
    assert "function Test-ObservedProcessIdentity" in source
    assert "-ExpectedStartTicks" in source
    assert "-ObservedIdentities $ObservedProcessStartTicks" in source


def test_git_scope_check_works_when_runner_is_called_outside_repo(
    tmp_path: Path,
) -> None:
    fixture_repo = tmp_path / "fixture-repo"
    outside = tmp_path / "outside"
    fixture_repo.mkdir()
    outside.mkdir()
    commands = (
        ["git", "init", "-b", "codex/fix-senior-analyst-payload-quality"],
        ["git", "config", "user.email", "offline@example.invalid"],
        ["git", "config", "user.name", "Offline Fixture"],
    )
    for command in commands:
        completed = subprocess.run(
            command,
            cwd=fixture_repo,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert completed.returncode == 0, completed.stderr
    (fixture_repo / "tracked.txt").write_text("fixture\n", encoding="utf-8")
    for command in (
        ["git", "add", "tracked.txt"],
        ["git", "commit", "-m", "fixture"],
        [
            "git",
            "update-ref",
            (
                "refs/remotes/origin/"
                "codex/fix-senior-analyst-payload-quality"
            ),
            "HEAD",
        ],
    ):
        completed = subprocess.run(
            command,
            cwd=fixture_repo,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert completed.returncode == 0, completed.stderr
    (fixture_repo / "ai-trader-consumer-payload.json").write_text(
        "allowed untracked fixture\n",
        encoding="utf-8",
    )

    escaped_runner = str(RUNNER).replace("'", "''")
    escaped_repo = str(fixture_repo).replace("'", "''")
    escaped_outside = str(outside).replace("'", "''")
    command = (
        "Set-StrictMode -Version 2.0;"
        "$errors=$null;$tokens=$null;"
        "$ast=[System.Management.Automation.Language.Parser]::ParseFile("
        f"'{escaped_runner}',[ref]$tokens,[ref]$errors);"
        "$function=$ast.Find({param($node)"
        "$node -is [System.Management.Automation.Language.FunctionDefinitionAst] "
        "-and $node.Name -eq 'Assert-GitScope'},$true);"
        "if($null-eq $function){throw 'Assert-GitScope missing'};"
        "Invoke-Expression $function.Extent.Text;"
        f"$Repo='{escaped_repo}';"
        "$ExpectedBranch='codex/fix-senior-analyst-payload-quality';"
        f"Push-Location '{escaped_outside}';"
        "try{Assert-GitScope}finally{Pop-Location};"
        "'PROVIDER_AUDIT_EXTERNAL_CWD_GIT_SCOPE_PASS'"
    )
    completed = _run_powershell(command)
    assert completed.returncode == 0, completed.stderr
    assert "PROVIDER_AUDIT_EXTERNAL_CWD_GIT_SCOPE_PASS" in completed.stdout


def test_runner_parses_under_real_windows_powershell_51() -> None:
    escaped = str(RUNNER).replace("'", "''")
    command = (
        "$errors=$null;$tokens=$null;"
        "[void][System.Management.Automation.Language.Parser]::ParseFile("
        f"'{escaped}',[ref]$tokens,[ref]$errors);"
        "if($errors.Count -ne 0){"
        "$errors|ForEach-Object{Write-Error $_};exit 1};"
        "'PROVIDER_AUDIT_PS51_PARSE_PASS'"
    )
    completed = _run_powershell(command)
    assert completed.returncode == 0, completed.stderr
    assert "PROVIDER_AUDIT_PS51_PARSE_PASS" in completed.stdout


def test_runner_system_net_http_smoke_is_real_and_performs_no_request() -> None:
    source = RUNNER.read_text(encoding="utf-8")
    assert "Add-Type -AssemblyName System.Net.Http -ErrorAction Stop" in source
    command = (
        "$ErrorActionPreference='Stop';"
        "Add-Type -AssemblyName System.Net.Http -ErrorAction Stop;"
        "$client=[System.Net.Http.HttpClient]::new();"
        "try{"
        "if($null-eq $client){throw 'HttpClient construction failed'};"
        "}finally{$client.Dispose()};"
        "'PROVIDER_AUDIT_PS51_HTTPCLIENT_NO_NETWORK_PASS'"
    )
    completed = _run_powershell(command)
    assert completed.returncode == 0, completed.stderr
    assert "PROVIDER_AUDIT_PS51_HTTPCLIENT_NO_NETWORK_PASS" in completed.stdout


def test_python_entry_point_help_is_offline_and_documents_filters() -> None:
    completed = subprocess.run(
        [str(REPO_ROOT / ".venv" / "Scripts" / "python.exe"), str(ENTRY_POINT), "--help"],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert "--provider" in completed.stdout
    assert "--dataset" in completed.stdout
    assert "--metric" in completed.stdout
    assert "--include-ai" in completed.stdout
    assert "--exclude-ai" in completed.stdout
    assert "--verify-pointer" in completed.stdout
    assert "--publish-candidate" in completed.stdout


def test_python_pointer_verification_mode_fails_closed_without_probing(
    tmp_path: Path,
) -> None:
    pointer = tmp_path / "data" / "provider-capability-audit-latest.json"
    pointer.parent.mkdir()
    pointer.write_text(
        '{"schema_version":"provider-capability-audit-latest-v1"}\n',
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            str(REPO_ROOT / ".venv" / "Scripts" / "python.exe"),
            str(ENTRY_POINT),
            "--verify-pointer",
            str(pointer),
        ],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 1
    assert "CAPABILITY_AUDIT_POINTER_INVALID" in completed.stdout
    assert not (tmp_path / "data" / "provider-capability-audit").exists()


def test_python_candidate_publication_rejects_non_repository_data_root(
    tmp_path: Path,
) -> None:
    candidate = (
        tmp_path
        / "data"
        / ".provider-capability-audit-latest.RUN.fixture.candidate.json"
    )
    candidate.parent.mkdir()
    candidate.write_text("{}\n", encoding="utf-8")
    completed = subprocess.run(
        [
            str(REPO_ROOT / ".venv" / "Scripts" / "python.exe"),
            str(ENTRY_POINT),
            "--publish-candidate",
            str(candidate),
        ],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 1
    assert "CAPABILITY_AUDIT_CANDIDATE_LOCATION_INVALID" in completed.stdout
    assert not (
        candidate.parent / "provider-capability-audit-latest.json"
    ).exists()


def test_audit_entry_point_contains_no_trading_or_canonical_write_route() -> None:
    source = ENTRY_POINT.read_text(encoding="utf-8")
    forbidden = (
        "place_order",
        "submit_order",
        "execute_trade",
        "app.main:app",
        "market-context/mnq",
    )
    assert all(token not in source for token in forbidden)
    assert "DatabaseBundleGuard" in source
    assert "_sandbox_settings" in source
