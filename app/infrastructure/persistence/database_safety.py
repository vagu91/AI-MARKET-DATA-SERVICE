from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path


DEFAULT_OPERATIONAL_DATABASE = Path("data/market_data_service.sqlite")


def is_test_process() -> bool:
    return "pytest" in sys.modules or bool(os.environ.get("PYTEST_CURRENT_TEST"))


def assert_test_database_isolated(path: Path, *, environment: str | None = None) -> None:
    if not is_test_process() and str(environment or "").lower() != "test":
        return
    resolved = Path(path).resolve()
    forbidden = {DEFAULT_OPERATIONAL_DATABASE.resolve()}
    for variable in (
        "AI_MARKET_RUNTIME_DATABASE_PATH",
        "AI_MARKET_LIVE_DATABASE_PATH",
        "AI_MARKET_PRODUCTION_DATABASE_PATH",
    ):
        configured = os.environ.get(variable)
        if configured:
            forbidden.add(Path(configured).resolve())
    if resolved in forbidden:
        raise RuntimeError(f"test_database_not_isolated:{resolved}")
    if str(environment or "").lower() == "test":
        temp_root = Path(tempfile.gettempdir()).resolve()
        if temp_root != resolved and temp_root not in resolved.parents:
            raise RuntimeError(f"test_database_must_be_temporary:{resolved}")
