from __future__ import annotations

from typing import Any

from app.core.redaction import redact_payload
from app.core.config import Settings
from app.services.market_fact_repository import connect_market_db, encode, init_market_db, now_iso


class ProviderObservationRepository:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        init_market_db(settings)

    def record(self, **payload: Any) -> None:
        payload = redact_payload(payload)
        payload.setdefault("retrieved_at", now_iso())
        payload.setdefault("item_count", 0)
        if "raw_payload_json" in payload:
            payload["raw_payload_json"] = encode(payload["raw_payload_json"])
        columns = list(payload)
        with connect_market_db(self.settings) as conn:
            conn.execute(
                f"INSERT INTO provider_observations ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
                [payload[column] for column in columns],
            )
            conn.commit()
