# ADR-001: SQLite Persistence Architecture

## Status

Accepted.

## Context

The service previously allowed multiple operational SQLite paths for provider cache and canonical market data.

That split made cold-start, refresh, and backup semantics harder to reason about.

## Problem

The same request can touch provider cache, canonical market facts, news history, provider observations, and enrichment run metrics. When those stores are configured through ambiguous names, operators cannot quickly tell which file must be backed up, reset, migrated, or inspected for readiness.

## Options Considered

### Strategy A: One Physical SQLite File

Use one operational DB with separate logical tables and repositories.

Advantages:

- One file to back up, reset, validate, and migrate.
- Easier true cold-start procedure.
- No cross-file consistency problem for provider observations and canonical read-back.
- Fits the current single-process FastAPI deployment.

Disadvantages:

- Provider cache churn lives in the same file as canonical data.
- Large cache payloads must be managed with purge/TTL policy.

### Strategy B: Two Physical SQLite Files

Keep provider cache and canonical store in separate files.

Advantages:

- Cache can be reset independently at the file level.
- Different retention policies are mechanically isolated.

Disadvantages:

- More complicated health, backup, restore, migration, and cold-start operations.
- Easier to accidentally inspect the wrong DB.
- Current code already couples response cache and canonical persistence within the same request flow.

## Decision

Use one operational SQLite database only:

```env
AI_MARKET_DATABASE_PATH=./data/market_data_service.sqlite
```

`Settings.database_path` is the only runtime persistence setting. The default is `./data/market_data_service.sqlite`.

Logical ownership remains separate:

- Canonical store tables: `market_facts`, `economic_events_history`, `market_news`, `provider_observations`, `enrichment_runs`.
- Provider cache tables: `provider_cache_entries`, `provider_state`.
- Schema management: `schema_migrations`.

Runtime does not support legacy persistence aliases. Legacy source files can still be imported through the explicit migration script.

## Migration Strategy

`scripts/migrate_legacy_database.py` migrates legacy `cache_entries` into `provider_cache_entries`, preserving key, payload, timestamps, and calculating a SHA-256 checksum over the stored JSON payload. The source DB is left intact. The migration supports `--dry-run`, `--apply`, `--source`, `--target`, and `--report`.

Schema migrations are tracked in `schema_migrations` and are idempotent. Destructive migrations require an external backup step and must be added as explicit numbered migrations.

## Rollback

Rollback is operational rather than implicit:

- Take a backup with `scripts/backup_database.py` before destructive changes.
- Restore the backed-up SQLite file if validation fails.
- Keep legacy source files until the migration report and `/db/health/details` are valid.

## Test Impact

Architecture tests cover default single-DB resolution, rejection of legacy runtime aliases, idempotent migrations, legacy cache import, cache repository behavior, centralized SQLite access, and schema ownership.

## Operational Impact

Operators should use `/db/health/details`, `/db/schema-version`, and `/db/cache/stats` to verify the DB. `refresh=false` remains DB/cache-only and does not require a provider network call.

## Consequences

Provider and service code must access SQLite through `app.infrastructure.persistence`.

`refresh=false` remains cache/DB-only. `refresh=auto` is DB-first and may call providers only when needed. `refresh=force` may bypass valid cache and call providers, while still persisting and reading back through the same store.

Backups, validation, and reset scripts operate on `Settings.database_path` unless a path is provided explicitly. Legacy migration operates only on the explicit `--source` path and the configured single target DB.
