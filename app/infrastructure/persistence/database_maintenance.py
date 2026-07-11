from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite, database_health
from app.infrastructure.persistence.migrations import migrate_database


def purge_expired_cache(settings: Settings, *, dry_run: bool = True, now: datetime | None = None) -> dict[str, Any]:
    cutoff = (now or datetime.now(UTC)) - timedelta(days=settings.expired_cache_retention_days)
    return _delete_before(
        settings.database_path,
        table="provider_cache_entries",
        timestamp_column="valid_until",
        cutoff=cutoff,
        dry_run=dry_run,
        extra_where="valid_until IS NOT NULL",
    )


def purge_old_provider_observations(settings: Settings, *, dry_run: bool = True, now: datetime | None = None) -> dict[str, Any]:
    cutoff = (now or datetime.now(UTC)) - timedelta(days=settings.provider_observations_retention_days)
    return _delete_before(
        settings.database_path,
        table="provider_observations",
        timestamp_column="retrieved_at",
        cutoff=cutoff,
        dry_run=dry_run,
    )


def purge_old_enrichment_runs(settings: Settings, *, dry_run: bool = True, now: datetime | None = None) -> dict[str, Any]:
    cutoff = (now or datetime.now(UTC)) - timedelta(days=settings.enrichment_runs_retention_days)
    return _delete_before(
        settings.database_path,
        table="enrichment_runs",
        timestamp_column="started_at",
        cutoff=cutoff,
        dry_run=dry_run,
    )


def purge_old_market_news(settings: Settings, *, dry_run: bool = True, now: datetime | None = None) -> dict[str, Any]:
    cutoff = (now or datetime.now(UTC)) - timedelta(days=settings.market_news_retention_days)
    return _delete_before(
        settings.database_path,
        table="market_news",
        timestamp_column="COALESCE(valid_until, published_at, retrieved_at)",
        cutoff=cutoff,
        dry_run=dry_run,
        extra_where="COALESCE(valid_until, published_at, retrieved_at) IS NOT NULL",
    )


def purge_expired_market_facts(settings: Settings, *, dry_run: bool = True, now: datetime | None = None) -> dict[str, Any]:
    cutoff = (now or datetime.now(UTC)) - timedelta(days=settings.market_facts_retention_days)
    return _delete_before(
        settings.database_path,
        table="market_facts",
        timestamp_column="valid_until",
        cutoff=cutoff,
        dry_run=dry_run,
        extra_where="valid_until IS NOT NULL",
    )


def purge_old_economic_events(settings: Settings, *, dry_run: bool = True, now: datetime | None = None) -> dict[str, Any]:
    cutoff = (now or datetime.now(UTC)) - timedelta(days=settings.economic_events_history_retention_days)
    return _delete_before(
        settings.database_path,
        table="economic_events_history",
        timestamp_column="COALESCE(release_at, time_utc, date, updated_at, created_at)",
        cutoff=cutoff,
        dry_run=dry_run,
        extra_where="COALESCE(release_at, time_utc, date, updated_at, created_at) IS NOT NULL",
    )


def purge_old_snapshot_history(settings: Settings, *, dry_run: bool = True, now: datetime | None = None) -> dict[str, Any]:
    cutoff = (now or datetime.now(UTC)) - timedelta(days=settings.snapshot_history_retention_days)
    fed = _delete_before(
        settings.database_path,
        table="fed_expectation_snapshots",
        timestamp_column="retrieved_at",
        cutoff=cutoff,
        dry_run=dry_run,
    )
    risk = _delete_before(
        settings.database_path,
        table="risk_context_snapshots",
        timestamp_column="retrieved_at",
        cutoff=cutoff,
        dry_run=dry_run,
    )
    return {
        "table": "snapshot_history",
        "cutoff": cutoff.replace(microsecond=0).isoformat(),
        "dry_run": dry_run,
        "deleted_rows": int(fed["deleted_rows"]) + int(risk["deleted_rows"]),
        "matched_rows": int(fed["matched_rows"]) + int(risk["matched_rows"]),
        "tables": {"fed_expectation_snapshots": fed, "risk_context_snapshots": risk},
    }


def purge_temporary_provider_state(settings: Settings, *, dry_run: bool = True, now: datetime | None = None) -> dict[str, Any]:
    cutoff = (now or datetime.now(UTC)) - timedelta(days=settings.expired_cache_retention_days)
    return _delete_before(
        settings.database_path,
        table="provider_state",
        timestamp_column="COALESCE(next_retry_at, updated_at)",
        cutoff=cutoff,
        dry_run=dry_run,
        extra_where="COALESCE(next_retry_at, updated_at) IS NOT NULL",
    )


def run_database_maintenance(settings: Settings, *, dry_run: bool = True, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    migrate_database(settings.database_path)
    purges = {
        "expired_cache": purge_expired_cache(settings, dry_run=dry_run, now=now),
        "provider_observations": purge_old_provider_observations(settings, dry_run=dry_run, now=now),
        "enrichment_runs": purge_old_enrichment_runs(settings, dry_run=dry_run, now=now),
        "market_news": purge_old_market_news(settings, dry_run=dry_run, now=now),
        "market_facts": purge_expired_market_facts(settings, dry_run=dry_run, now=now),
        "economic_events_history": purge_old_economic_events(settings, dry_run=dry_run, now=now),
        "snapshot_history": purge_old_snapshot_history(settings, dry_run=dry_run, now=now),
        "provider_state": purge_temporary_provider_state(settings, dry_run=dry_run, now=now),
    }
    return {
        "dry_run": dry_run,
        "database_path": str(settings.database_path),
        "deleted_rows": sum(int(item.get("deleted_rows") or 0) for item in purges.values()),
        "purges": purges,
        "analysis": analyze_database(settings),
    }


def analyze_database(settings: Settings) -> dict[str, Any]:
    migrate_database(settings.database_path)
    with connect_sqlite(settings.database_path) as conn:
        page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        conn.execute("ANALYZE")
        conn.commit()
    return {
        "health": database_health(settings.database_path),
        "page_count": page_count,
        "freelist_count": freelist_count,
        "page_size": page_size,
        "estimated_reclaimable_bytes": freelist_count * page_size,
    }


def vacuum_database(settings: Settings, *, force: bool = False) -> dict[str, Any]:
    path = settings.database_path
    migrate_database(path)
    before_size = _db_size(path)
    analysis = analyze_database(settings)
    reclaimable = int(analysis.get("estimated_reclaimable_bytes") or 0)
    should_vacuum = force or (
        before_size >= settings.db_vacuum_min_size_mb * 1024 * 1024
        and reclaimable >= settings.db_vacuum_min_reclaimable_mb * 1024 * 1024
    )
    if should_vacuum:
        started = datetime.now(UTC)
        with connect_sqlite(path) as conn:
            conn.execute("VACUUM")
        duration_ms = int((datetime.now(UTC) - started).total_seconds() * 1000)
    else:
        duration_ms = 0
    after_size = _db_size(path)
    return {
        "vacuum_executed": should_vacuum,
        "before_bytes": before_size,
        "after_bytes": after_size,
        "bytes_recovered": max(before_size - after_size, 0),
        "duration_ms": duration_ms,
        "estimated_reclaimable_bytes": reclaimable,
    }


def _delete_before(
    database_path: Path,
    *,
    table: str,
    timestamp_column: str,
    cutoff: datetime,
    dry_run: bool,
    extra_where: str | None = None,
) -> dict[str, Any]:
    cutoff_iso = cutoff.replace(microsecond=0).isoformat()
    where = f"{timestamp_column} < ?"
    if extra_where:
        where = f"({extra_where}) AND ({where})"
    with connect_sqlite(database_path) as conn:
        count = int(conn.execute(f"SELECT COUNT(*) c FROM {table} WHERE {where}", (cutoff_iso,)).fetchone()["c"])
        if not dry_run:
            conn.execute(f"DELETE FROM {table} WHERE {where}", (cutoff_iso,))
            conn.commit()
    return {
        "table": table,
        "timestamp_column": timestamp_column,
        "cutoff": cutoff_iso,
        "dry_run": dry_run,
        "deleted_rows": 0 if dry_run else count,
        "matched_rows": count,
    }


def _db_size(path: Path) -> int:
    files = [path, path.with_suffix(path.suffix + "-wal"), path.with_suffix(path.suffix + "-shm")]
    return sum(item.stat().st_size for item in files if item.exists())
