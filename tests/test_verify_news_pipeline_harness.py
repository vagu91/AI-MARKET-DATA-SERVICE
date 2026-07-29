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


def test_controlled_live_sandbox_copies_bundle_without_sqlite_source_open(
    tmp_path: Path,
) -> None:
    module = _controlled_live_module()
    source = tmp_path / "operational.sqlite"
    target = tmp_path / "sandbox" / "copy.sqlite"
    with sqlite3.connect(source) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("CREATE TABLE fixture (value TEXT NOT NULL)")
        connection.execute("INSERT INTO fixture VALUES ('preserved')")
        connection.commit()
        before = module._database_state(source)

        preflight = module._create_consistent_sandbox(
            source,
            target,
            expected_source_state=before,
        )

        after = module._database_state(source)
        with sqlite3.connect(target) as target_connection:
            row = target_connection.execute(
                "SELECT value FROM fixture"
            ).fetchone()
    assert before == after
    assert {item["role"] for item in before} == {"MAIN", "WAL", "SHM"}
    assert row == ("preserved",)
    assert preflight["method"] == (
        "FILESYSTEM_BUNDLE_COPY_WITHOUT_SQLITE_SOURCE_OPEN"
    )
    assert preflight["operational_sqlite_opened"] is False
    assert preflight["sandbox_content_matches_source"] is True


def _database_fixture() -> list[dict]:
    return [
        {
            "role": "MAIN",
            "path": "operational.sqlite",
            "bytes": 100,
            "last_write_ns": 1,
            "sha256": "A" * 64,
        },
        {
            "role": "WAL",
            "path": "operational.sqlite-wal",
            "bytes": 20,
            "last_write_ns": 2,
            "sha256": "B" * 64,
        },
        {
            "role": "SHM",
            "path": "operational.sqlite-shm",
            "bytes": 32,
            "last_write_ns": 3,
            "sha256": "C" * 64,
        },
    ]


def _account(
    provider: str,
    record_id: str | None,
    *,
    status: str = "COMPLETE",
    coverage_status: str = "COMPLETE",
    reason_code: str | None = None,
    metadata_status: str = "NOT_REQUIRED",
) -> dict:
    ids = [record_id] if record_id else []
    return {
        "provider": provider,
        "status": status,
        "availability_status": status,
        "coverage_status": coverage_status,
        "reason_code": reason_code,
        "temporary": status == "TEMPORARILY_UNAVAILABLE",
        "errors": (
            [f"{provider} {reason_code}"] if reason_code else []
        ),
        "pagination_complete": status == "COMPLETE",
        "accounting_valid": True,
        "raw_record_ids": ids,
        "parsed_record_ids": ids,
        "persisted_record_ids": ids,
        "persisted_count": len(ids),
        "technical_rejections": [],
        "persistence_rejections": [],
        "explicit_out_of_scope": [],
        "exact_technical_duplicates": [],
        "metadata_enrichment_status": metadata_status,
    }


def _usable_payload(*, gdelt_status: str = "TEMPORARILY_UNAVAILABLE") -> dict:
    primary = [
        "Federal Reserve RSS",
        "BLS RSS",
        "BEA RSS",
        "Yahoo Finance RSS",
        "MarketWatch RSS",
        "Google News RSS",
    ]
    accounts = [
        _account(provider, f"raw:{index}")
        for index, provider in enumerate(primary)
    ]
    accounts.append(
        _account(
            "GDELT Doc API",
            None,
            status=gdelt_status,
            coverage_status="FAILED",
            reason_code="GDELT_DOC_API_CONNECT_TIMEOUT_RETRY_EXHAUSTED",
        )
    )
    return {
        "data": {
            "articles": [
                {"raw_record_id": f"raw:{index}"}
                for index in range(len(primary))
            ],
            "provider_accounting": accounts,
            "data_quality": {
                "final_data_available": True,
                "readiness": "DEGRADED",
            },
        }
    }


def test_optional_gdelt_timeout_with_six_feeds_is_pass_degraded() -> None:
    module = _controlled_live_module()
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    database = module._database_invariants(
        _database_fixture(),
        _database_fixture(),
    )
    result = module._evaluate_controlled_live_acceptance(
        baseline=baseline,
        payload=_usable_payload(),
        network={"pass": True},
        database_invariants=database,
    )
    assert result["status"] == "PASS_DEGRADED"
    assert result["pass"] is True
    assert result["usable"] is True
    assert result["coverage_has_no_blocking_failure"] is True
    assert result["response_article_count"] == 6
    assert result["hard_failures"] == []


def test_all_hard_invariants_and_complete_primary_feeds_is_pass() -> None:
    module = _controlled_live_module()
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    payload = _usable_payload()
    payload["data"]["provider_accounting"] = payload["data"][
        "provider_accounting"
    ][:-1]
    payload["data"]["data_quality"]["readiness"] = "AVAILABLE"
    result = module._evaluate_controlled_live_acceptance(
        baseline=baseline,
        payload=payload,
        network={"pass": True},
        database_invariants=module._database_invariants(
            _database_fixture(),
            _database_fixture(),
        ),
    )
    assert result["status"] == "PASS"
    assert result["pass"] is True
    assert result["hard_failures"] == []
    assert result["degraded_reasons"] == []


def test_all_primary_feeds_unavailable_is_fail() -> None:
    module = _controlled_live_module()
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    primary = baseline["availability"]["required_provider_groups"][0][
        "providers"
    ]
    payload = {
        "data": {
            "articles": [],
            "provider_accounting": [
                _account(
                    provider,
                    None,
                    status="FAILED",
                    coverage_status="FAILED",
                    reason_code="OFFLINE_PROVIDER_UNAVAILABLE",
                )
                for provider in primary
            ],
            "data_quality": {
                "final_data_available": False,
                "readiness": "UNAVAILABLE",
            },
        }
    }
    result = module._evaluate_controlled_live_acceptance(
        baseline=baseline,
        payload=payload,
        network={"pass": True},
        database_invariants=module._database_invariants(
            _database_fixture(),
            _database_fixture(),
        ),
    )
    assert result["status"] == "FAIL"
    assert result["pass"] is False
    assert {
        item["reason_code"] for item in result["hard_failures"]
    } >= {
        "REQUIRED_PROVIDER_GROUP_UNAVAILABLE",
        "NO_USABLE_NEWS_PAYLOAD",
    }


def test_partial_or_failed_source_can_never_be_promoted_complete() -> None:
    module = _controlled_live_module()
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    payload = _usable_payload()
    payload["data"]["provider_accounting"][0].update(
        {
            "status": "COMPLETE",
            "coverage_status": "PARTIAL",
            "pagination_complete": False,
        }
    )
    result = module._evaluate_controlled_live_acceptance(
        baseline=baseline,
        payload=payload,
        network={"pass": True},
        database_invariants=module._database_invariants(
            _database_fixture(),
            _database_fixture(),
        ),
    )
    assert result["status"] == "FAIL"
    assert "FAILED_OR_PARTIAL_SOURCE_DECLARED_COMPLETE" in {
        item["reason_code"] for item in result["hard_failures"]
    }


def test_cross_provider_identity_collision_fails_aggregate_accounting() -> None:
    module = _controlled_live_module()
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    payload = _usable_payload()
    first = payload["data"]["provider_accounting"][0]
    second = payload["data"]["provider_accounting"][1]
    second["raw_record_ids"] = list(first["raw_record_ids"])
    second["parsed_record_ids"] = list(first["parsed_record_ids"])
    second["persisted_record_ids"] = list(first["persisted_record_ids"])
    result = module._evaluate_controlled_live_acceptance(
        baseline=baseline,
        payload=payload,
        network={"pass": True},
        database_invariants=module._database_invariants(
            _database_fixture(),
            _database_fixture(),
        ),
    )
    assert result["status"] == "FAIL"
    assert result["aggregate_accounting"][
        "cross_provider_identity_collision_ids"
    ] == ["raw:0"]
    assert "PROVIDER_ACCOUNTING_INVALID" in {
        item["reason_code"] for item in result["hard_failures"]
    }


@pytest.mark.parametrize("role_index", [0, 1, 2])
@pytest.mark.parametrize("changed_field", ["sha256", "bytes"])
def test_database_hash_or_size_change_is_fail(
    role_index: int,
    changed_field: str,
) -> None:
    module = _controlled_live_module()
    before = _database_fixture()
    after = [dict(item) for item in before]
    if changed_field == "sha256":
        after[role_index][changed_field] = "D" * 64
    else:
        after[role_index][changed_field] += 1
    invariants = module._database_invariants(before, after)
    assert invariants["classification"] == "CONTENT_CHANGED"
    assert invariants["content_unchanged"] is False
    assert invariants["semantic_database_unchanged"] is False
    assert invariants["pass"] is False
    assert invariants["changed_content_roles"] == [
        before[role_index]["role"]
    ]


def test_shm_timestamp_only_is_metadata_change_not_logical_mutation() -> None:
    module = _controlled_live_module()
    before = _database_fixture()
    after = [dict(item) for item in before]
    after[2]["last_write_ns"] += 1
    invariants = module._database_invariants(before, after)
    assert invariants["classification"] == (
        "METADATA_ONLY_SHM_TIMESTAMP_CHANGE"
    )
    assert invariants["content_unchanged"] is True
    assert invariants["metadata_unchanged"] is False
    assert invariants["semantic_database_unchanged"] is True
    assert invariants["metadata_only_shm_timestamp_change"] is True
    assert invariants["pass"] is True


def test_non_shm_metadata_change_remains_fail_closed() -> None:
    module = _controlled_live_module()
    before = _database_fixture()
    after = [dict(item) for item in before]
    after[0]["last_write_ns"] += 1
    invariants = module._database_invariants(before, after)
    assert invariants["classification"] == "UNEXPECTED_METADATA_CHANGE"
    assert invariants["content_unchanged"] is True
    assert invariants["semantic_database_unchanged"] is True
    assert invariants["pass"] is False
