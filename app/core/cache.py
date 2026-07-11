from pathlib import Path
from typing import Any

from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository


class SQLiteCache:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.repository = ProviderCacheRepository(database_path)

    def set(self, cache_key: str, payload: Any) -> None:
        self.repository.set(cache_key, payload)

    def get(self, cache_key: str) -> dict[str, Any] | list[dict[str, Any]] | None:
        return self.repository.get(cache_key)

    def get_entry(self, cache_key: str) -> dict[str, Any] | None:
        return self.repository.get_entry(cache_key)
