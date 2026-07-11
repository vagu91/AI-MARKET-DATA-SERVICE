from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.core.config import Settings
from app.infrastructure.persistence import migrations
from app.infrastructure.persistence.database import database_health
from app.infrastructure.persistence.migrations import migrate_database
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.services.refresh_policy_service import RefreshPolicyService
from app.services.status_model import normalize_status


def test_settings_default_to_single_operational_database() -> None:
    settings = Settings(_env_file=None)
    assert settings.database_path == Path("data/market_data_service.sqlite")
    assert not hasattr(settings, "canonical_store_db_path")
    assert not hasattr(settings, "provider_cache_db_path")
    assert not hasattr(settings, "market_db_path")


def test_only_database_path_env_is_supported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "single.sqlite"
    monkeypatch.setenv("AI_MARKET_DATABASE_PATH", str(db_path))
    settings = Settings(_env_file=None)
    assert settings.database_path == db_path


def test_legacy_database_aliases_are_ignored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AI_MARKET_DATABASE_PATH", raising=False)
    monkeypatch.setenv("AI_MARKET_DB_PATH", str(tmp_path / "legacy.sqlite"))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "db.sqlite"))
    monkeypatch.setenv("AI_MARKET_PROVIDER_CACHE_DB_PATH", str(tmp_path / "provider.sqlite"))
    monkeypatch.setenv("AI_MARKET_CANONICAL_STORE_DB_PATH", str(tmp_path / "canonical.sqlite"))

    settings = Settings(_env_file=None)

    assert settings.database_path == Path("data/market_data_service.sqlite")


def test_migrations_are_idempotent_and_create_expected_tables(tmp_path: Path) -> None:
    database = tmp_path / "market.sqlite"
    first = migrate_database(database)
    second = migrate_database(database)

    assert first["schema_version"] == second["schema_version"]
    assert second["applied"] == []
    health = database_health(database)
    assert health["integrity_check"] == "ok"
    assert "market_facts" in health["tables"]
    assert "provider_cache_entries" in health["tables"]
    assert len(health["schema_migrations"]) >= 2


def test_migration_failure_rolls_back_partial_schema(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    database = tmp_path / "broken.sqlite"
    monkeypatch.setattr(
        migrations,
        "MIGRATIONS",
        (("001_broken", "CREATE TABLE partial_table(id INTEGER); INSERT INTO missing_table VALUES (1);"),),
    )

    with pytest.raises(sqlite3.DatabaseError):
        migrations.migrate_database(database)

    with sqlite3.connect(database) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        applied = conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
    assert "partial_table" not in tables
    assert applied == 0


def test_sqlite_cache_facade_uses_provider_cache_entries(tmp_path: Path) -> None:
    database = tmp_path / "cache.sqlite"
    cache = ProviderCacheRepository(database)
    cache.set("macro:test", {"value": 1})

    assert cache.get("macro:test") == {"value": 1}
    with sqlite3.connect(database) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        count = conn.execute("SELECT COUNT(*) FROM provider_cache_entries").fetchone()[0]
    assert "provider_cache_entries" in tables
    assert count == 1


def test_legacy_cache_entries_are_imported(tmp_path: Path) -> None:
    database = tmp_path / "legacy.sqlite"
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TABLE cache_entries(cache_key TEXT PRIMARY KEY, payload TEXT, created_at TEXT, updated_at TEXT)"
        )
        conn.execute(
            "INSERT INTO cache_entries(cache_key, payload, created_at, updated_at) VALUES (?, ?, ?, ?)",
            ("legacy:key", '{"ok": true}', "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00"),
        )
        conn.commit()

    migrate_database(database)
    assert ProviderCacheRepository(database).get("legacy:key") == {"ok": True}


def test_refresh_policy_and_status_normalization() -> None:
    assert RefreshPolicyService.require_cache_only("false") is True
    assert RefreshPolicyService.allow_network("auto") is True
    assert RefreshPolicyService.bypass_valid_cache("force") is True
    assert normalize_status("valid") == "found"
    assert normalize_status("restricted") == "access_restricted"


def test_application_sqlite_connect_is_centralized() -> None:
    allowed = {
        Path("app/infrastructure/persistence/database.py"),
    }
    offenders: list[str] = []
    for path in Path("app").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "sqlite3.connect" in text and path.as_posix() not in {item.as_posix() for item in allowed}:
            offenders.append(path.as_posix())
    assert offenders == []


def test_schema_ddl_lives_in_persistence_layer() -> None:
    offenders: list[str] = []
    for path in Path("app").rglob("*.py"):
        if path.parts[:3] == ("app", "infrastructure", "persistence"):
            continue
        if "CREATE TABLE" in path.read_text(encoding="utf-8"):
            offenders.append(path.as_posix())
    assert offenders == []


def test_application_has_no_sqlite_cache_wrapper_or_dual_db_settings() -> None:
    offenders: list[str] = []
    banned = (
        "SQLiteCache",
        "provider_cache_db_path",
        "canonical_store_db_path",
        "market_db_path",
        "AI_MARKET_PROVIDER_CACHE_DB_PATH",
        "AI_MARKET_CANONICAL_STORE_DB_PATH",
        "AI_MARKET_DB_PATH",
        "DB_PATH",
    )
    for root in (Path("app"), Path("scripts")):
        for path in root.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if any(token in text for token in banned):
                offenders.append(path.as_posix())
    assert offenders == []
