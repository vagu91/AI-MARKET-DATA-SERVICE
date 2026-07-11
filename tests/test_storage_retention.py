from __future__ import annotations

import json
from collections import namedtuple
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from app.api.deps import get_enrichment_orchestrator
from app.core.config import Settings
from app.core.logging import logging_rotation_config
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.database_maintenance import run_database_maintenance
from app.infrastructure.persistence.migrations import migrate_database
from app.infrastructure.storage_retention import cleanup_storage, retention_policy_report, storage_health
from app.main import app
from app.services.enrichment_orchestrator import EnrichmentOrchestrator


def _settings(tmp_path: Path, **overrides) -> Settings:
    values = {
        "_env_file": None,
        "database_path": tmp_path / "data" / "market.sqlite",
        "diagnostics_dir": tmp_path / "data" / "diagnostics",
        "backups_dir": tmp_path / "data" / "backups",
        "logs_dir": tmp_path / "logs",
        "temp_dir": tmp_path / "data" / "temp",
        "diagnostics_retention_days": 7,
        "diagnostics_max_runs": 2,
        "diagnostics_max_total_mb": 1,
        "backups_retention_days": 14,
        "backups_max_files": 2,
        "backups_max_total_mb": 1,
        "log_max_file_mb": 1,
        "log_backup_count": 2,
    }
    values.update(overrides)
    return Settings(**values)


def _write(path: Path, size: int = 8, *, age_days: int = 0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    stamp = (datetime.now(UTC) - timedelta(days=age_days)).timestamp()
    path.touch()
    import os

    os.utime(path, (stamp, stamp))
    os.utime(path.parent, (stamp, stamp))


def test_cleanup_dry_run_does_not_delete(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    old = settings.diagnostics_dir / "old_run" / "payload.json"
    _write(old, age_days=30)

    report = cleanup_storage(settings, category="diagnostics", dry_run=True, repo_root=tmp_path)

    assert old.exists()
    assert report["bytes_deleted"] > 0
    assert report["deleted_paths"]


def test_cleanup_apply_deletes_only_authorized_old_paths(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    old = settings.diagnostics_dir / "old_run" / "payload.json"
    fresh = settings.diagnostics_dir / "fresh_run" / "payload.json"
    _write(old, age_days=30)
    _write(fresh, age_days=1)

    report = cleanup_storage(settings, category="diagnostics", dry_run=False, repo_root=tmp_path)

    assert not old.exists()
    assert fresh.exists()
    assert report["errors"] == []


def test_cleanup_enforces_max_runs_max_files_and_max_bytes(tmp_path: Path) -> None:
    settings = _settings(tmp_path, diagnostics_max_total_mb=0, backups_max_total_mb=0)
    for index in range(4):
        _write(settings.diagnostics_dir / f"run_{index}" / "payload.json", size=20, age_days=4 - index)
        _write(settings.backups_dir / f"backup_{index}.sqlite", size=20, age_days=4 - index)

    report = cleanup_storage(settings, category="all", dry_run=False, repo_root=tmp_path)

    assert len([item for item in settings.diagnostics_dir.iterdir() if item.is_dir()]) <= settings.diagnostics_max_runs
    assert len(list(settings.backups_dir.glob("*.sqlite"))) <= settings.backups_max_files
    assert report["bytes_deleted"] > 0


def test_cleanup_symlink_and_path_traversal_are_protected(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    outside = tmp_path.parent / "outside_storage_retention.txt"
    outside.write_text("keep", encoding="utf-8")
    try:
        link = settings.diagnostics_dir / "outside_link"
        settings.diagnostics_dir.mkdir(parents=True, exist_ok=True)
        try:
            link.symlink_to(outside)
        except OSError:
            link.write_text("symlink unsupported", encoding="utf-8")

        report = cleanup_storage(settings, category="diagnostics", dry_run=False, repo_root=tmp_path)
        assert outside.exists()
        assert any(item.get("reason") == "symlink" for item in report["skipped_paths"]) or link.exists()

        unsafe = _settings(tmp_path, diagnostics_dir=tmp_path)
        unsafe_report = cleanup_storage(unsafe, category="diagnostics", dry_run=False, repo_root=tmp_path)
        assert unsafe_report["errors"]
    finally:
        outside.unlink(missing_ok=True)


def test_cleanup_is_idempotent_and_report_json_serializable(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _write(settings.temp_dir / "old.tmp", age_days=2)

    first = cleanup_storage(settings, category="temp", dry_run=False, repo_root=tmp_path)
    second = cleanup_storage(settings, category="temp", dry_run=False, repo_root=tmp_path)

    assert first["bytes_deleted"] > 0
    assert second["bytes_deleted"] == 0
    json.dumps(first)


def test_log_rotation_config_is_bounded(tmp_path: Path) -> None:
    settings = _settings(tmp_path, log_max_file_mb=2, log_backup_count=3)
    config = logging_rotation_config(settings)

    assert config["enabled"] is True
    assert config["max_file_bytes"] == 2 * 1024 * 1024
    assert config["backup_count"] == 3
    assert config["encoding"] == "utf-8"


def test_database_maintenance_purges_retained_tables(tmp_path: Path) -> None:
    settings = _settings(tmp_path, provider_observations_retention_days=1, enrichment_runs_retention_days=1, expired_cache_retention_days=1, market_news_retention_days=1)
    migrate_database(settings.database_path)
    old = (datetime.now(UTC) - timedelta(days=10)).replace(microsecond=0).isoformat()
    with connect_sqlite(settings.database_path) as conn:
        conn.execute("INSERT INTO provider_cache_entries(cache_key, payload_json, created_at, updated_at, valid_until, status) VALUES ('old', '{}', ?, ?, ?, 'valid_cache')", (old, old, old))
        conn.execute("INSERT INTO provider_observations(provider_name, status, retrieved_at) VALUES ('p', 'found', ?)", (old,))
        conn.execute("INSERT INTO enrichment_runs(run_id, started_at, status) VALUES ('r', ?, 'done')", (old,))
        conn.execute("INSERT INTO market_news(news_key, title, source_url, retrieved_at, valid_until) VALUES ('n', 'old', 'https://x.test', ?, ?)", (old, old))
        conn.execute("INSERT INTO provider_state(state_key, provider_name, state_type, status, updated_at, created_at, next_retry_at) VALUES ('s', 'p', 'negative_cache', 'expired', ?, ?, ?)", (old, old, old))
        conn.commit()

    dry = run_database_maintenance(settings, dry_run=True)
    applied = run_database_maintenance(settings, dry_run=False)

    assert dry["deleted_rows"] == 0
    assert applied["deleted_rows"] == 5


def test_storage_health_low_disk_warning(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path, disk_warning_free_mb=100, disk_critical_free_mb=50)
    Usage = namedtuple("usage", "total used free")
    monkeypatch.setattr("app.infrastructure.storage_retention.shutil.disk_usage", lambda path: Usage(1000, 940, 60 * 1024 * 1024))

    health = storage_health(settings, repo_root=tmp_path)

    assert health["status"] == "degraded"
    assert health["warnings"][0]["code"] == "disk_free_low"


def test_storage_endpoints_return_policy_and_health(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    orchestrator = EnrichmentOrchestrator(settings, event_enrichment_service=None)
    app.dependency_overrides[get_enrichment_orchestrator] = lambda: orchestrator
    try:
        with TestClient(app) as client:
            health = client.get("/storage/health").json()
            policy = client.get("/storage/retention-policy").json()
    finally:
        app.dependency_overrides.clear()

    assert health["status"] in {"ok", "degraded"}
    assert policy["storage_retention_enabled"] is True
    assert retention_policy_report(settings)["diagnostics"]["max_entries"] == 2
