from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "scripts" / "run-senior-analyst-live.ps1"
LEGACY_RUNNER = REPO_ROOT / "scripts" / "run-senior-analyst-live-validation.ps1"


def _invoke_publish_function(
    *,
    run_id: str,
    payload: Path,
    report: Path,
    destination: Path,
) -> subprocess.CompletedProcess[str]:
    escaped_runner = str(RUNNER).replace("'", "''")
    escaped_run_id = run_id.replace("'", "''")
    escaped_payload = str(payload).replace("'", "''")
    escaped_report = str(report).replace("'", "''")
    escaped_destination = str(destination).replace("'", "''")
    command = (
        "$ErrorActionPreference='Stop';"
        "$errors=$null;$tokens=$null;"
        "$ast=[System.Management.Automation.Language.Parser]::ParseFile("
        f"'{escaped_runner}',[ref]$tokens,[ref]$errors);"
        "if($errors.Count-ne 0){throw 'runner parse failed'};"
        "$function=$ast.Find({param($node)"
        "$node-is [System.Management.Automation.Language.FunctionDefinitionAst] "
        "-and $node.Name-eq 'Publish-LatestAcceptance'},$true);"
        "if($null-eq $function){throw 'publish function missing'};"
        "Invoke-Expression $function.Extent.Text;"
        "Publish-LatestAcceptance "
        f"-RunId '{escaped_run_id}' "
        f"-PayloadPath '{escaped_payload}' "
        f"-ValidationReportPath '{escaped_report}' "
        f"-Destination '{escaped_destination}';"
        "'PUBLISH_FUNCTION_COMPLETED'"
    )
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
    assert "run_id = $RunId" in source
    assert "body_size_bytes = $payloadSize" in source
    assert "$payloadSize -ne [long]$reportedBodySize" in source
    assert 'result = "PASS"' in source
    assert '$report.status -ne "PASS"' in source
    assert (
        "[IO.File]::Replace($temporary, $Destination, $backup)"
        in source
    )
    assert "[IO.File]::Move($temporary, $Destination)" in source
    assert source.index("if ($LASTEXITCODE -ne 0)") < source.index("Publish-LatestAcceptance `")


def test_publish_function_binds_run_id_hash_and_exact_body_size_under_ps51(
    tmp_path: Path,
) -> None:
    run_id = "20260731T211500Z"
    payload = tmp_path / "response-body.json"
    report = tmp_path / "acceptance-report.json"
    destination = tmp_path / "senior-analyst-live-latest.json"
    body = json.dumps(
        {
            "generated_at": "2026-07-31T21:15:00+00:00",
            "readiness": {"status": "PARTIAL"},
        },
        separators=(",", ":"),
    ).encode()
    payload.write_bytes(body)
    report.write_text(
        json.dumps(
            {
                "status": "PASS",
                "validated_exact_http_body": True,
                "body_sha256": hashlib.sha256(body).hexdigest(),
                "body_size_bytes": len(body),
            }
        ),
        encoding="utf-8",
    )

    completed = _invoke_publish_function(
        run_id=run_id,
        payload=payload,
        report=report,
        destination=destination,
    )

    assert completed.returncode == 0, completed.stderr
    assert "PUBLISH_FUNCTION_COMPLETED" in completed.stdout
    pointer = json.loads(destination.read_text(encoding="utf-8"))
    assert pointer["result"] == "PASS"
    assert pointer["run_id"] == run_id
    assert pointer["body_size_bytes"] == len(body)
    assert Path(pointer["full_payload_path"]) == payload.resolve()
    assert pointer["full_payload_sha256"] == hashlib.sha256(body).hexdigest()


def test_size_mismatch_does_not_replace_previous_pointer_under_ps51(
    tmp_path: Path,
) -> None:
    payload = tmp_path / "response-body.json"
    report = tmp_path / "acceptance-report.json"
    destination = tmp_path / "senior-analyst-live-latest.json"
    body = b'{"generated_at":"2026-07-31T21:15:00+00:00","readiness":{"status":"PARTIAL"}}'
    payload.write_bytes(body)
    report.write_text(
        json.dumps(
            {
                "status": "PASS",
                "validated_exact_http_body": True,
                "body_sha256": hashlib.sha256(body).hexdigest(),
                "body_size_bytes": len(body) + 1,
            }
        ),
        encoding="utf-8",
    )
    previous = b'{"result":"PASS","run_id":"PREVIOUS"}\n'
    destination.write_bytes(previous)

    completed = _invoke_publish_function(
        run_id="20260731T211500Z",
        payload=payload,
        report=report,
        destination=destination,
    )

    assert completed.returncode != 0
    assert "body size does not match" in completed.stderr.lower()
    assert destination.read_bytes() == previous
    assert not list(tmp_path.glob(".senior-analyst-live-latest.*"))


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


def test_runner_httpclient_smoke_under_windows_powershell_51_without_network(
    tmp_path: Path,
) -> None:
    source = RUNNER.read_text(encoding="utf-8")
    assert "Add-Type -AssemblyName System.Net.Http -ErrorAction Stop" in source
    assert "$client = [System.Net.Http.HttpClient]::new()" in source
    assert (
        "$body = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()"
        in source
    )
    assert "[IO.File]::WriteAllBytes($BodyPath, $body)" in source
    assert "$client.Dispose()" in source

    output = tmp_path / "response-body.bin"
    escaped_output = str(output).replace("'", "''")
    command = (
        "$ErrorActionPreference='Stop';"
        "Add-Type -AssemblyName System.Net.Http -ErrorAction Stop;"
        "$client=[System.Net.Http.HttpClient]::new();"
        "$content=$null;"
        "try{"
        "$expected=[byte[]](0,1,2,13,10,255);"
        "$content=[System.Net.Http.ByteArrayContent]::new($expected);"
        "$body=$content.ReadAsByteArrayAsync().GetAwaiter().GetResult();"
        f"[System.IO.File]::WriteAllBytes('{escaped_output}',$body);"
        "}finally{"
        "if($null-ne $content){$content.Dispose()};"
        "$client.Dispose()"
        "};"
        "'WINDOWS_POWERSHELL_5_1_HTTPCLIENT_PASS'"
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
    assert "WINDOWS_POWERSHELL_5_1_HTTPCLIENT_PASS" in completed.stdout
    assert output.read_bytes() == bytes((0, 1, 2, 13, 10, 255))


def test_runner_atomic_replace_smoke_under_windows_powershell_51(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "senior-analyst-live-latest.json"
    temporary = tmp_path / ".senior-analyst-live-latest.tmp"
    backup = tmp_path / ".senior-analyst-live-latest.bak"
    destination.write_bytes(b'{"result":"OLD"}\n')
    temporary.write_bytes(b'{"result":"PASS"}\n')
    escaped_destination = str(destination).replace("'", "''")
    escaped_temporary = str(temporary).replace("'", "''")
    escaped_backup = str(backup).replace("'", "''")
    command = (
        "$ErrorActionPreference='Stop';"
        f"$destination='{escaped_destination}';"
        f"$temporary='{escaped_temporary}';"
        f"$backup='{escaped_backup}';"
        "try{"
        "[IO.File]::Replace($temporary,$destination,$backup);"
        "}finally{"
        "if(Test-Path -LiteralPath $temporary -PathType Leaf){"
        "Remove-Item -LiteralPath $temporary -Force};"
        "if(Test-Path -LiteralPath $backup -PathType Leaf){"
        "Remove-Item -LiteralPath $backup -Force}"
        "};"
        "'WINDOWS_POWERSHELL_5_1_ATOMIC_REPLACE_PASS'"
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
    assert (
        "WINDOWS_POWERSHELL_5_1_ATOMIC_REPLACE_PASS"
        in completed.stdout
    )
    assert destination.read_bytes() == b'{"result":"PASS"}\n'
    assert not temporary.exists()
    assert not backup.exists()
