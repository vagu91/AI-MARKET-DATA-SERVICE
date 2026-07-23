from __future__ import annotations

import os
import tempfile
from pathlib import Path


_ISOLATED_TEST_ROOT = Path(tempfile.mkdtemp(prefix="ai-market-data-service-tests-"))
os.environ["AI_MARKET_DATABASE_PATH"] = str(_ISOLATED_TEST_ROOT / "suite.sqlite")
os.environ["AI_MARKET_ENVIRONMENT"] = "test"
