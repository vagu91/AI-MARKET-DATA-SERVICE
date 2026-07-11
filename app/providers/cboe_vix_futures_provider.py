from __future__ import annotations

import csv
import io
import re
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS


MONTHLY_VX = re.compile(r"^VX/[FGHJKMNQUVXZ]\d$")


class CboeVixFuturesProvider:
    source = "Cboe Futures Exchange"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_cboe_vix_futures:
            return _status("disabled", "cboe_vix_futures_disabled", started)
        errors: list[str] = []
        network_calls = 0
        delayed_status = "not_attempted"
        async with httpx.AsyncClient(timeout=self.settings.http_timeout_seconds) as client:
            try:
                network_calls += 1
                delayed = await client.get(
                    self.settings.cboe_vix_futures_delayed_url,
                    headers={**REQUEST_HEADERS, "Accept": "application/json"},
                )
                delayed_status = "found" if delayed.status_code == 200 else "access_restricted" if delayed.status_code == 403 else "provider_failed"
            except Exception as exc:
                delayed_status = "provider_failed"
                errors.append(f"delayed_feed_failed:{exc or type(exc).__name__}")

            latest_date, latest_text, latest_calls = await _latest_settlement(
                client,
                self.settings.cboe_vix_futures_settlement_url,
                start_date=datetime.now(UTC).date(),
            )
            network_calls += latest_calls
            if not latest_date or not latest_text:
                return _status(
                    "provider_failed",
                    "cboe_vix_futures_settlement_not_found",
                    started,
                    network_calls=network_calls,
                    delayed_status=delayed_status,
                )
            previous_date, previous_text, previous_calls = await _latest_settlement(
                client,
                self.settings.cboe_vix_futures_settlement_url,
                start_date=latest_date - timedelta(days=1),
            )
            network_calls += previous_calls

        contracts, diagnostics = parse_vix_futures_csv(latest_text, data_as_of=latest_date.isoformat())
        previous, _ = parse_vix_futures_csv(previous_text or "", data_as_of=previous_date.isoformat() if previous_date else None)
        previous_by_symbol = {item["contract_symbol"]: item for item in previous}
        now = datetime.now(UTC)
        for position, item in enumerate(contracts[:6], start=1):
            prior = previous_by_symbol.get(item["contract_symbol"])
            previous_close = prior.get("last_price") if prior else None
            item.update(
                {
                    "tenor": f"M{position}",
                    "previous_close": previous_close,
                    "change": round(item["last_price"] - previous_close, 6) if previous_close else None,
                    "change_pct": round((item["last_price"] / previous_close - 1) * 100, 6) if previous_close else None,
                    "retrieved_at": _iso(now),
                    "valid_until": _iso(now + timedelta(minutes=self.settings.risk_context_ttl_minutes)),
                    "freshness": "LAST_SESSION",
                    "reliability": 0.96,
                    "confidence": 0.94,
                    "cache_status": "provider",
                }
            )
        contracts = contracts[:6]
        status = "found" if len(contracts) >= 2 else "partial" if contracts else "not_found"
        warnings = []
        if delayed_status == "access_restricted":
            warnings.append("cboe_delayed_vix_futures_access_restricted_using_official_settlement")
        if len(contracts) < 2:
            warnings.append("vix_futures_partial_curve")
        return {
            "status": status,
            "provider": self.source,
            "source": self.source,
            "source_url": self.settings.cboe_vix_futures_settlement_url,
            "source_type": "official_cfe_daily_settlement",
            "is_official_source": True,
            "data_as_of": latest_date.isoformat(),
            "retrieved_at": _iso(now),
            "valid_until": _iso(now + timedelta(minutes=self.settings.risk_context_ttl_minutes)),
            "contracts": contracts,
            "diagnostics": {
                **diagnostics,
                "actual_network_calls": network_calls,
                "delayed_feed_status": delayed_status,
                "settlement_date": latest_date.isoformat(),
                "previous_settlement_date": previous_date.isoformat() if previous_date else None,
                "contract_count": len(contracts),
            },
            "warnings": warnings,
            "errors": errors,
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
        }


async def _latest_settlement(
    client: httpx.AsyncClient,
    base_url: str,
    *,
    start_date: date,
    max_lookback_days: int = 10,
) -> tuple[date | None, str | None, int]:
    calls = 0
    for offset in range(max_lookback_days + 1):
        candidate = start_date - timedelta(days=offset)
        calls += 1
        response = await client.get(
            base_url,
            params={"dt": candidate.isoformat()},
            headers={**REQUEST_HEADERS, "Accept": "text/csv"},
        )
        response.raise_for_status()
        if len(response.text.strip().splitlines()) > 1:
            return candidate, response.text, calls
    return None, None, calls


def parse_vix_futures_csv(text: str, *, data_as_of: str | None) -> tuple[list[dict[str, Any]], dict[str, int]]:
    contracts: list[dict[str, Any]] = []
    expired = duplicate = invalid = weekly = 0
    seen: set[str] = set()
    reference = date.fromisoformat(data_as_of) if data_as_of else datetime.now(UTC).date()
    for row in csv.DictReader(io.StringIO(text.lstrip("\ufeff"))):
        if str(row.get("Product") or "").strip() != "VX":
            continue
        symbol = str(row.get("Symbol") or "").strip()
        if not MONTHLY_VX.fullmatch(symbol):
            weekly += 1
            continue
        try:
            expiration = date.fromisoformat(str(row.get("Expiration Date") or "").strip())
            price = float(str(row.get("Price") or "").strip().replace("*", ""))
        except (TypeError, ValueError):
            invalid += 1
            continue
        if expiration <= reference or price <= 0:
            expired += 1
            continue
        if symbol in seen:
            duplicate += 1
            continue
        seen.add(symbol)
        contracts.append(
            {
                "contract_symbol": symbol,
                "expiration_date": expiration.isoformat(),
                "settlement_type": "daily_settlement",
                "last_price": price,
                "previous_close": None,
                "change": None,
                "change_pct": None,
                "volume": None,
                "open_interest": None,
                "data_as_of": data_as_of,
                "source": "Cboe Futures Exchange",
                "source_url": "https://www.cboe.com/us/futures/market_statistics/settlement/futures/daily/",
                "provider_type": "OFFICIAL_EXCHANGE_SETTLEMENT",
                "is_official_source": True,
            }
        )
    contracts.sort(key=lambda item: item["expiration_date"])
    return contracts, {
        "expired_contract_count": expired,
        "duplicate_contract_count": duplicate,
        "invalid_contract_count": invalid,
        "weekly_contract_excluded_count": weekly,
    }


def _status(
    status: str,
    reason: str,
    started: datetime,
    *,
    network_calls: int = 0,
    delayed_status: str = "not_attempted",
) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "status": status,
        "provider": "Cboe Futures Exchange",
        "source": "Cboe Futures Exchange",
        "source_url": "https://www.cboe.com/us/futures/market_statistics/settlement/futures/daily/",
        "is_official_source": True,
        "retrieved_at": _iso(now),
        "valid_until": _iso(now + timedelta(minutes=30)),
        "contracts": [],
        "diagnostics": {"actual_network_calls": network_calls, "delayed_feed_status": delayed_status, "contract_count": 0},
        "warnings": [reason] if status != "provider_failed" else [],
        "errors": [reason] if status == "provider_failed" else [],
        "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
    }


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")
