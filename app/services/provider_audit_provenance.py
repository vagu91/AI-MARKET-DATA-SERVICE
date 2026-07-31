from __future__ import annotations

import hashlib
import re
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
_AUDITED_SUFFIXES = frozenset(
    {".bat", ".json", ".md", ".ps1", ".py", ".toml", ".txt", ".yaml", ".yml"}
)
_AUDITED_SOURCE_ROOTS = ("app", "config", "scripts")
_AUDITED_ROOT_FILES = (
    "pyproject.toml",
    "run-provider-capability-audit.bat",
)
_BASELINE_PIN_PATTERN = re.compile(
    rb"(?m)^LAST_LIVE_BASELINE_FILE_SHA256: str \| None = "
    rb"(?:None|[\"'][0-9a-fA-F]{64}[\"'])$"
)
_BASELINE_PIN_CANONICAL = (
    b"LAST_LIVE_BASELINE_FILE_SHA256: str | None = <LIVE_BASELINE_PIN>"
)
_GIT_SHA_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")


@dataclass(frozen=True, slots=True)
class ProviderAuditSourceProvenance:
    git_commit_sha: str
    audited_runtime_sha256: str
    audited_file_count: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def provider_audit_source_provenance(
    repo_root: Path = REPO_ROOT,
) -> ProviderAuditSourceProvenance:
    root = repo_root.resolve()
    return ProviderAuditSourceProvenance(
        git_commit_sha=_git_commit_sha(root),
        audited_runtime_sha256=audited_runtime_source_sha256(root),
        audited_file_count=len(audited_runtime_source_paths(root)),
    )


def is_git_commit_sha(value: Any) -> bool:
    return bool(_GIT_SHA_PATTERN.fullmatch(str(value or "").casefold()))


def audited_runtime_source_paths(
    repo_root: Path = REPO_ROOT,
) -> tuple[Path, ...]:
    root = repo_root.resolve()
    paths: set[Path] = set()
    for relative_root in _AUDITED_SOURCE_ROOTS:
        source_root = (root / relative_root).resolve()
        if not source_root.is_dir() or not source_root.is_relative_to(root):
            raise RuntimeError(
                f"provider audit source root is unavailable: {relative_root}"
            )
        for path in source_root.rglob("*"):
            if (
                not path.is_file()
                or path.suffix.casefold() not in _AUDITED_SUFFIXES
                or "__pycache__" in path.parts
            ):
                continue
            if path.is_symlink() or not path.resolve().is_relative_to(root):
                raise RuntimeError(
                    "provider audit source symlink is not allowed: "
                    f"{path.relative_to(root).as_posix()}"
                )
            paths.add(path)
    for relative in _AUDITED_ROOT_FILES:
        path = root / relative
        if (
            not path.is_file()
            or path.is_symlink()
            or not path.resolve().is_relative_to(root)
        ):
            raise RuntimeError(
                f"provider audit source file is unavailable: {relative}"
            )
        paths.add(path)
    return tuple(
        sorted(
            paths,
            key=lambda path: path.relative_to(root).as_posix(),
        )
    )


def audited_runtime_source_sha256(
    repo_root: Path = REPO_ROOT,
) -> str:
    root = repo_root.resolve()
    digest = hashlib.sha256()
    for path in audited_runtime_source_paths(root):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = _canonical_source_bytes(
            path.relative_to(root).as_posix(),
            path.read_bytes(),
        )
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _canonical_source_bytes(relative: str, payload: bytes) -> bytes:
    normalized_text = payload.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    if relative == "app/services/provider_capability_registry.py":
        normalized, replacements = _BASELINE_PIN_PATTERN.subn(
            _BASELINE_PIN_CANONICAL,
            normalized_text,
        )
        if replacements != 1:
            raise RuntimeError(
                "provider audit baseline pin declaration is not canonical"
            )
        return normalized
    return normalized_text


def _git_commit_sha(repo_root: Path) -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(
            "provider audit Git revision is unavailable"
        ) from exc
    revision = completed.stdout.strip().casefold()
    if not is_git_commit_sha(revision):
        raise RuntimeError("provider audit Git revision is invalid")
    return revision


__all__ = [
    "ProviderAuditSourceProvenance",
    "audited_runtime_source_paths",
    "audited_runtime_source_sha256",
    "is_git_commit_sha",
    "provider_audit_source_provenance",
]
