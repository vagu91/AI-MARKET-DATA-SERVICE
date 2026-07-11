import json
import logging
import sys
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from typing import Any

from app.core.config import Settings


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key.startswith("_") and key not in payload:
                payload[key[1:]] = value
        return json.dumps(payload, default=str)


def configure_logging(settings_or_level: Settings | str) -> None:
    if isinstance(settings_or_level, Settings):
        level = settings_or_level.log_level
        logs_dir = settings_or_level.logs_dir
        max_bytes = max(settings_or_level.log_max_file_mb, 1) * 1024 * 1024
        backup_count = max(settings_or_level.log_backup_count, 0)
    else:
        level = settings_or_level
        logs_dir = None
        max_bytes = 10 * 1024 * 1024
        backup_count = 5
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    if logs_dir is not None and backup_count > 0:
        logs_dir.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            logs_dir / "ai_market_data_service.log",
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(JsonFormatter())
        root.addHandler(file_handler)
    root.setLevel(level.upper())
    logging.getLogger("httpx").setLevel(logging.WARNING)


def logging_rotation_config(settings: Settings) -> dict[str, Any]:
    return {
        "enabled": settings.log_backup_count > 0,
        "log_dir": str(settings.logs_dir),
        "max_file_bytes": max(settings.log_max_file_mb, 1) * 1024 * 1024,
        "backup_count": settings.log_backup_count,
        "encoding": "utf-8",
    }
