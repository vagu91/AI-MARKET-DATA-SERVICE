from __future__ import annotations

import asyncio
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess

import httpx
import pytest

from scripts.verify_news_pipeline import _forbidden_temporary_scripts


REPO_ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = REPO_ROOT / "docs" / "baselines" / "news-pipeline-v1.json"
POWERSHELL_ENTRYPOINT = REPO_ROOT / "scripts" / "verify-news-pipeline.ps1"
CONTROLLED_LIVE_PATH = (
    REPO_ROOT / "scripts" / "news_pipeline_controlled_live.py"
)


def _controlled_live_module():
    spec = importlib.util.spec_from_file_location(
        "news_pipeline_controlled_live",
        CONTROLLED_LIVE_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_permanent_powershell_entrypoint_parses_under_windows_powershell_51() -> None:
    escaped = str(POWERSHELL_ENTRYPOINT).replace("'", "''")
    command = (
        "$errors=$null;$tokens=$null;"
        f"[void][System.Management.Automation.Language.Parser]::ParseFile("
        f"'{escaped}',[ref]$tokens,[ref]$errors);"
        "if($errors.Count -ne 0){$errors|ForEach-Object{Write-Error $_};exit 1};"
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


def test_permanent_entrypoint_uses_only_repository_virtualenv_python() -> None:
    source = POWERSHELL_ENTRYPOINT.read_text(encoding="utf-8")
    assert ".venv\\Scripts\\python.exe" in source
    assert "Get-Command python" not in source
    assert "Start-Process" not in source
    assert ".env" not in source


def test_disposable_stage_v8_or_later_is_rejected_even_below_data(
    tmp_path: Path,
) -> None:
    legacy = (
        tmp_path
        / "data"
        / "preserved"
        / "pr29-controlled-live-news-validation-stage2-v7.ps1"
    )
    forbidden = (
        tmp_path
        / "data"
        / "new"
        / "pr29-controlled-live-news-validation-stage2-v8.ps1"
    )
    legacy.parent.mkdir(parents=True)
    forbidden.parent.mkdir(parents=True)
    legacy.write_text("# preserved V7 evidence\n", encoding="utf-8")
    forbidden.write_text("# forbidden disposable V8\n", encoding="utf-8")

    assert _forbidden_temporary_scripts(tmp_path) == [
        str(forbidden.relative_to(tmp_path))
    ]


def test_controlled_live_network_audit_is_fail_closed() -> None:
    module = _controlled_live_module()
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    audit = module.ControlledNetworkAudit(baseline)

    outside = audit.decision(
        httpx.Request("GET", "https://www.bls.gov/feed/bls_latest.rss")
    )
    assert outside["allowed"] is False
    assert outside["reason_code"] == "CALL_OUTSIDE_ACQUISITION"

    audit.active = True
    allowed = audit.decision(
        httpx.Request("GET", "https://www.bls.gov/feed/bls_latest.rss")
    )
    blocked = audit.decision(
        httpx.Request("GET", "https://example.com/unobserved")
    )
    assert allowed["allowed"] is True
    assert allowed["provider"] == "BLS RSS"
    assert blocked["allowed"] is False
    assert blocked["reason_code"] == "ENDPOINT_NOT_ALLOWLISTED"


def test_controlled_live_article_and_yahoo_redirect_require_raw_lineage() -> None:
    module = _controlled_live_module()
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    audit = module.ControlledNetworkAudit(baseline)
    audit.active = True
    record_id = "raw:fixture-yahoo"
    original = "https://finance.yahoo.com/technology/articles/fixture.html"
    redirected = "https://finance.yahoo.com/news/fixture.html"
    audit.register_provider_batches(
        [
            {
                "provider": "Yahoo Finance RSS",
                "_articles": [
                    {
                        "raw_record_id": record_id,
                        "source_url": original,
                    }
                ],
            }
        ]
    )
    audit.register_metadata_redirect(
        provider="Yahoo Finance RSS",
        record_id=record_id,
        original_url=original,
        redirect_url=redirected,
    )
    decision = audit.decision(httpx.Request("GET", redirected))
    assert decision["allowed"] is True
    assert decision["article_lineage_ids"] == [record_id]
    assert decision["allowlist_origin"] == "ALLOWLISTED_YAHOO_REDIRECT"

    with pytest.raises(
        RuntimeError,
        match="REDIRECT_WITHOUT_CURRENT_ACQUISITION_LINEAGE",
    ):
        audit.register_metadata_redirect(
            provider="Yahoo Finance RSS",
            record_id="raw:unknown",
            original_url="https://finance.yahoo.com/unknown",
            redirect_url="https://finance.yahoo.com/news/unknown",
        )
    with pytest.raises(
        RuntimeError,
        match="REDIRECT_WITHOUT_CURRENT_ACQUISITION_LINEAGE",
    ):
        audit.register_metadata_redirect(
            provider="Yahoo Finance RSS",
            record_id=record_id,
            original_url=original,
            redirect_url="https://example.com/news/fixture.html",
        )


def test_controlled_live_httpx_send_and_transport_are_both_observed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _controlled_live_module()
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))

    async def fake_transport(
        transport: httpx.AsyncHTTPTransport,
        request: httpx.Request,
    ) -> httpx.Response:
        del transport
        return httpx.Response(200, request=request, text="<rss/>")

    monkeypatch.setattr(
        httpx.AsyncHTTPTransport,
        "handle_async_request",
        fake_transport,
    )
    audit = module.ControlledNetworkAudit(baseline)

    async def exercise() -> None:
        audit.active = True
        try:
            with audit.instrument():
                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        "https://www.bls.gov/feed/bls_latest.rss"
                    )
                    assert response.status_code == 200
        finally:
            audit.active = False

    asyncio.run(exercise())
    verification = audit.verify()
    assert verification["pass"] is True
    assert verification["network_call_count"] == 1
    assert verification["transport_call_count"] == 1


def test_controlled_live_sandbox_uses_read_only_sqlite_backup(
    tmp_path: Path,
) -> None:
    module = _controlled_live_module()
    source = tmp_path / "operational.sqlite"
    target = tmp_path / "sandbox" / "copy.sqlite"
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE fixture (value TEXT NOT NULL)")
        connection.execute("INSERT INTO fixture VALUES ('preserved')")
    before = module._database_state(source)

    module._create_consistent_sandbox(source, target)

    after = module._database_state(source)
    with sqlite3.connect(target) as connection:
        row = connection.execute("SELECT value FROM fixture").fetchone()
    assert before == after
    assert row == ("preserved",)
