from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

from app.services.provider_audit_provenance import (
    REPO_ROOT,
    audited_runtime_source_sha256,
    is_git_commit_sha,
    provider_audit_source_provenance,
)


def _source_fixture(root: Path, *, pin: str, adapter_value: int = 1) -> None:
    registry = root / "app" / "services" / "provider_capability_registry.py"
    registry.parent.mkdir(parents=True, exist_ok=True)
    registry.write_text(
        "LAST_LIVE_BASELINE_FILE_SHA256: str | None = "
        f"{pin}\nREGISTRY_VALUE = {adapter_value}\n",
        encoding="utf-8",
    )
    scripts = root / "scripts"
    scripts.mkdir()
    (scripts / "provider_capability_audit.py").write_text(
        "AUDIT_RUNTIME = True\n",
        encoding="utf-8",
    )
    prompts = root / "app" / "prompts"
    prompts.mkdir()
    (prompts / "ai_research_macro_event.md").write_text(
        "Use occurrence-specific evidence.\n",
        encoding="utf-8",
    )
    config = root / "config"
    config.mkdir()
    (config / "source_policy.json").write_text(
        '{"policy":"fail_closed"}\n',
        encoding="utf-8",
    )
    (root / "pyproject.toml").write_text(
        "[project]\nname='fixture'\n",
        encoding="utf-8",
    )
    (root / "run-provider-capability-audit.bat").write_text(
        "@echo off\n",
        encoding="utf-8",
    )


def test_runtime_digest_ignores_only_the_reviewed_live_baseline_pin(
    tmp_path: Path,
) -> None:
    _source_fixture(tmp_path, pin="None")
    before = audited_runtime_source_sha256(tmp_path)
    registry = (
        tmp_path
        / "app"
        / "services"
        / "provider_capability_registry.py"
    )
    source = registry.read_text(encoding="utf-8")
    registry.write_text(
        source.replace(
            "LAST_LIVE_BASELINE_FILE_SHA256: str | None = None",
            "LAST_LIVE_BASELINE_FILE_SHA256: str | None = "
            f'"{"a" * 64}"',
        ),
        encoding="utf-8",
    )
    assert audited_runtime_source_sha256(tmp_path) == before

    registry.write_text(
        registry.read_text(encoding="utf-8").replace(
            "REGISTRY_VALUE = 1",
            "REGISTRY_VALUE = 2",
        ),
        encoding="utf-8",
    )
    assert audited_runtime_source_sha256(tmp_path) != before


def test_runtime_digest_rejects_executable_suffix_on_baseline_pin(
    tmp_path: Path,
) -> None:
    _source_fixture(tmp_path, pin="None; EXECUTED = True")
    with pytest.raises(
        RuntimeError,
        match="baseline pin declaration is not canonical",
    ):
        audited_runtime_source_sha256(tmp_path)


def test_runtime_digest_covers_prompt_and_source_policy_and_normalizes_eol(
    tmp_path: Path,
) -> None:
    lf_root = tmp_path / "lf"
    crlf_root = tmp_path / "crlf"
    _source_fixture(lf_root, pin="None")
    _source_fixture(crlf_root, pin="None")
    for path in crlf_root.rglob("*"):
        if path.is_file():
            payload = path.read_bytes().replace(b"\r\n", b"\n")
            path.write_bytes(payload.replace(b"\n", b"\r\n"))
    assert audited_runtime_source_sha256(lf_root) == (
        audited_runtime_source_sha256(crlf_root)
    )

    before = audited_runtime_source_sha256(lf_root)
    policy = lf_root / "config" / "source_policy.json"
    policy.write_text('{"policy":"changed"}\n', encoding="utf-8")
    assert audited_runtime_source_sha256(lf_root) != before

    policy.write_text('{"policy":"fail_closed"}\n', encoding="utf-8")
    restored = audited_runtime_source_sha256(lf_root)
    prompt = lf_root / "app" / "prompts" / "ai_research_macro_event.md"
    prompt.write_text("Changed provider instructions.\n", encoding="utf-8")
    assert audited_runtime_source_sha256(lf_root) != restored


def test_runtime_provenance_binds_current_git_commit_and_source_tree() -> None:
    provenance = provider_audit_source_provenance()
    expected_head = subprocess.run(
        ["git", "rev-parse", "--verify", "HEAD"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    assert provenance.git_commit_sha == expected_head
    assert re.fullmatch(r"[0-9a-f]{64}", provenance.audited_runtime_sha256)
    assert provenance.audited_file_count > 0
    assert (
        provenance.audited_runtime_sha256
        == audited_runtime_source_sha256()
    )
    assert is_git_commit_sha("a" * 40)
    assert is_git_commit_sha("b" * 64)
    assert not is_git_commit_sha("c" * 41)
    assert not is_git_commit_sha("d" * 63)
