from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from app.core.config import Settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetentionPolicy:
    category: str
    root: Path
    retention: timedelta | None
    max_total_bytes: int | None = None
    max_entries: int | None = None
    entry_kind: str = "any"


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def retention_policies(settings: Settings) -> dict[str, RetentionPolicy]:
    mb = 1024 * 1024
    return {
        "diagnostics": RetentionPolicy(
            category="diagnostics",
            root=settings.diagnostics_dir,
            retention=timedelta(days=settings.diagnostics_retention_days),
            max_total_bytes=settings.diagnostics_max_total_mb * mb,
            max_entries=settings.diagnostics_max_runs,
            entry_kind="run",
        ),
        "backups": RetentionPolicy(
            category="backups",
            root=settings.backups_dir,
            retention=timedelta(days=settings.backups_retention_days),
            max_total_bytes=settings.backups_max_total_mb * mb,
            max_entries=settings.backups_max_files,
            entry_kind="file",
        ),
        "logs": RetentionPolicy(
            category="logs",
            root=settings.logs_dir,
            retention=None,
            max_total_bytes=max(settings.log_max_file_mb * settings.log_backup_count, 1) * mb,
            max_entries=settings.log_backup_count + 1,
            entry_kind="file",
        ),
        "temp": RetentionPolicy(
            category="temp",
            root=settings.temp_dir,
            retention=timedelta(hours=settings.temp_retention_hours),
            max_total_bytes=None,
            max_entries=None,
            entry_kind="any",
        ),
    }


def retention_policy_report(settings: Settings) -> dict[str, Any]:
    return {
        name: {
            "root": str(policy.root),
            "retention_seconds": int(policy.retention.total_seconds()) if policy.retention else None,
            "max_total_bytes": policy.max_total_bytes,
            "max_entries": policy.max_entries,
            "entry_kind": policy.entry_kind,
        }
        for name, policy in retention_policies(settings).items()
    }


def cleanup_storage(
    settings: Settings,
    *,
    category: str = "all",
    dry_run: bool = True,
    now: datetime | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    repo_root = (repo_root or project_root()).resolve()
    policies = retention_policies(settings)
    selected = list(policies) if category == "all" else [category]
    report = _empty_report(dry_run=dry_run, selected=selected)
    for name in selected:
        policy = policies.get(name)
        if not policy:
            report["errors"].append(f"unknown_category:{name}")
            continue
        category_report = _cleanup_policy(policy, dry_run=dry_run, now=now, repo_root=repo_root)
        report["categories"][name] = category_report
        _merge_report(report, category_report)
    report["bytes_after"] = max(report["bytes_before"] - report["bytes_deleted"], 0)
    return report


def storage_health(settings: Settings, *, repo_root: Path | None = None) -> dict[str, Any]:
    repo_root = (repo_root or project_root()).resolve()
    usage = shutil.disk_usage(repo_root)
    free_pct = round((usage.free / usage.total) * 100, 4) if usage.total else 0.0
    warnings: list[dict[str, Any]] = []
    if usage.free <= settings.disk_critical_free_mb * 1024 * 1024:
        warnings.append({"code": "disk_free_critical", "blocking": False, "free_bytes": usage.free})
    elif usage.free <= settings.disk_warning_free_mb * 1024 * 1024:
        warnings.append({"code": "disk_free_low", "blocking": False, "free_bytes": usage.free})
    status = "degraded" if warnings else "ok"
    return {
        "status": status,
        "disk": {
            "total_bytes": usage.total,
            "used_bytes": usage.used,
            "free_bytes": usage.free,
            "free_pct": free_pct,
        },
        "paths": {
            "database": _database_size(settings.database_path, repo_root),
            "diagnostics": _path_stats(settings.diagnostics_dir, repo_root),
            "backups": _path_stats(settings.backups_dir, repo_root),
            "logs": _path_stats(settings.logs_dir, repo_root),
        },
        "thresholds": {
            "warning_free_mb": settings.disk_warning_free_mb,
            "critical_free_mb": settings.disk_critical_free_mb,
        },
        "warnings": warnings,
    }


def maybe_run_startup_cleanup(settings: Settings) -> dict[str, Any] | None:
    lock = _resolve_under(project_root(), settings.temp_dir, create_root=True) / ".startup_cleanup.lock"
    now = datetime.now(UTC)
    try:
        if lock.exists():
            stamp = datetime.fromisoformat(lock.read_text(encoding="utf-8"))
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=UTC)
            if now - stamp.astimezone(UTC) < timedelta(hours=settings.storage_cleanup_interval_hours):
                return None
        report = cleanup_storage(settings, category="temp", dry_run=False, now=now)
        lock.write_text(now.isoformat(), encoding="utf-8")
        return report
    except Exception as exc:
        logger.warning("storage_startup_cleanup_failed", extra={"_error": str(exc)})
        return None


def _cleanup_policy(policy: RetentionPolicy, *, dry_run: bool, now: datetime, repo_root: Path) -> dict[str, Any]:
    report = _empty_category_report(policy, dry_run=dry_run)
    try:
        root = _resolve_under(repo_root, policy.root, create_root=True)
    except ValueError as exc:
        report["errors"].append(str(exc))
        return report
    entries = _top_level_entries(root, report)
    report["bytes_before"] = _tree_size(root, report)
    report["files_scanned"] = _count_files(root, report)
    report["directories_scanned"] = _count_dirs(root, report)

    to_delete: list[Path] = []
    if policy.retention:
        cutoff = now - policy.retention
        to_delete.extend(entry for entry in entries if _mtime(entry) < cutoff)
    remaining = [entry for entry in entries if entry not in set(to_delete)]
    if policy.max_entries is not None and len(remaining) > policy.max_entries:
        remaining.sort(key=_mtime)
        to_delete.extend(remaining[: len(remaining) - policy.max_entries])
    projected = report["bytes_before"] - sum(_entry_size(path, report) for path in set(to_delete))
    if policy.max_total_bytes is not None and projected > policy.max_total_bytes:
        candidates = [entry for entry in entries if entry not in set(to_delete)]
        candidates.sort(key=_mtime)
        for entry in candidates:
            if projected <= policy.max_total_bytes:
                break
            size = _entry_size(entry, report)
            to_delete.append(entry)
            projected -= size
    for entry in _dedupe_paths(to_delete):
        _delete_entry(entry, root=root, dry_run=dry_run, report=report)
    report["bytes_after"] = max(report["bytes_before"] - report["bytes_deleted"], 0)
    return report


def _delete_entry(entry: Path, *, root: Path, dry_run: bool, report: dict[str, Any]) -> None:
    try:
        resolved = _validate_delete_target(root, entry)
        size = _entry_size(resolved, report)
        report["deleted_paths"].append(str(resolved))
        report["bytes_deleted"] += size
        if dry_run:
            return
        if resolved.is_dir():
            shutil.rmtree(resolved)
        else:
            resolved.unlink()
    except PermissionError as exc:
        report["skipped_paths"].append({"path": str(entry), "reason": f"locked_or_permission:{exc}"})
    except OSError as exc:
        report["skipped_paths"].append({"path": str(entry), "reason": f"delete_failed:{exc}"})
    except ValueError as exc:
        report["errors"].append(str(exc))


def _resolve_under(repo_root: Path, path: Path, *, create_root: bool = False) -> Path:
    candidate = (repo_root / path).resolve() if not path.is_absolute() else path.resolve()
    if create_root:
        candidate.mkdir(parents=True, exist_ok=True)
    if candidate in {Path(candidate.anchor), repo_root, repo_root / "data"}:
        raise ValueError(f"refusing_unsafe_root:{candidate}")
    try:
        candidate.relative_to(repo_root)
    except ValueError as exc:
        raise ValueError(f"path_outside_project:{candidate}") from exc
    return candidate


def _validate_delete_target(root: Path, path: Path) -> Path:
    if path.is_symlink():
        raise ValueError(f"refusing_symlink_delete:{path}")
    resolved = path.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"delete_target_outside_root:{resolved}") from exc
    if resolved == root:
        raise ValueError(f"refusing_delete_root:{resolved}")
    return resolved


def _top_level_entries(root: Path, report: dict[str, Any]) -> list[Path]:
    if not root.exists():
        return []
    entries: list[Path] = []
    for entry in root.iterdir():
        if entry.is_symlink():
            report["skipped_paths"].append({"path": str(entry), "reason": "symlink"})
            continue
        entries.append(entry)
    return entries


def _tree_size(root: Path, report: dict[str, Any]) -> int:
    if not root.exists():
        return 0
    total = 0
    for path in _walk_no_symlink(root, report):
        if path.is_file():
            total += _safe_size(path)
    return total


def _entry_size(entry: Path, report: dict[str, Any]) -> int:
    if entry.is_file():
        return _safe_size(entry)
    return _tree_size(entry, report) if entry.is_dir() else 0


def _count_files(root: Path, report: dict[str, Any]) -> int:
    return sum(1 for path in _walk_no_symlink(root, report) if path.is_file())


def _count_dirs(root: Path, report: dict[str, Any]) -> int:
    return sum(1 for path in _walk_no_symlink(root, report) if path.is_dir())


def _walk_no_symlink(root: Path, report: dict[str, Any]) -> Iterable[Path]:
    if not root.exists():
        return []
    output: list[Path] = []
    for path in root.rglob("*"):
        if path.is_symlink():
            report["skipped_paths"].append({"path": str(path), "reason": "symlink"})
            continue
        output.append(path)
    return output


def _safe_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _mtime(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)


def _path_stats(path: Path, repo_root: Path) -> dict[str, Any]:
    report = _empty_category_report(RetentionPolicy("stats", path, None), dry_run=True)
    try:
        root = _resolve_under(repo_root, path, create_root=False)
    except ValueError:
        return {"bytes": 0, "files": 0, "exists": False}
    return {
        "bytes": _tree_size(root, report),
        "files": _count_files(root, report),
        "exists": root.exists(),
    }


def _database_size(path: Path, repo_root: Path) -> dict[str, Any]:
    database = (repo_root / path).resolve() if not path.is_absolute() else path.resolve()
    files = [database, database.with_suffix(database.suffix + "-wal"), database.with_suffix(database.suffix + "-shm")]
    return {"bytes": sum(_safe_size(item) for item in files), "path": str(database), "exists": database.exists()}


def _dedupe_paths(paths: Iterable[Path]) -> list[Path]:
    seen: set[Path] = set()
    output: list[Path] = []
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        output.append(path)
    return output


def _empty_report(*, dry_run: bool, selected: list[str]) -> dict[str, Any]:
    return {
        "dry_run": dry_run,
        "selected_categories": selected,
        "files_scanned": 0,
        "directories_scanned": 0,
        "bytes_before": 0,
        "bytes_deleted": 0,
        "bytes_after": 0,
        "deleted_paths": [],
        "skipped_paths": [],
        "errors": [],
        "policy_applied": {},
        "categories": {},
    }


def _empty_category_report(policy: RetentionPolicy, *, dry_run: bool) -> dict[str, Any]:
    return {
        "dry_run": dry_run,
        "files_scanned": 0,
        "directories_scanned": 0,
        "bytes_before": 0,
        "bytes_deleted": 0,
        "bytes_after": 0,
        "deleted_paths": [],
        "skipped_paths": [],
        "errors": [],
        "policy_applied": {
            "category": policy.category,
            "root": str(policy.root),
            "retention_seconds": int(policy.retention.total_seconds()) if policy.retention else None,
            "max_total_bytes": policy.max_total_bytes,
            "max_entries": policy.max_entries,
            "entry_kind": policy.entry_kind,
        },
    }


def _merge_report(report: dict[str, Any], category_report: dict[str, Any]) -> None:
    for key in ("files_scanned", "directories_scanned", "bytes_before", "bytes_deleted"):
        report[key] += int(category_report.get(key) or 0)
    report["deleted_paths"].extend(category_report.get("deleted_paths") or [])
    report["skipped_paths"].extend(category_report.get("skipped_paths") or [])
    report["errors"].extend(category_report.get("errors") or [])
    report["policy_applied"][category_report["policy_applied"]["category"]] = category_report["policy_applied"]
