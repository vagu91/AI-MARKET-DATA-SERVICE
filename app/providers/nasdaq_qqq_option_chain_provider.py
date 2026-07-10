from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings
from app.providers.calendar_utils import REQUEST_HEADERS
from app.services.economic_value_parser import parse_economic_value, parse_int_value


class NasdaqQQQOptionChainProvider:
    source = "Nasdaq QQQ Option Chain"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self) -> dict[str, Any]:
        started = datetime.now(UTC)
        if not self.settings.enable_nasdaq_qqq_options:
            return _status("disabled", "nasdaq_qqq_options_disabled", started)
        symbol = self.settings.nasdaq_options_symbol.upper()
        today = datetime.now(UTC).date()
        to_date = today + timedelta(days=self.settings.nasdaq_options_lookahead_days)
        contracts: list[dict[str, Any]] = []
        total_records: list[int | None] = []
        pages_fetched = 0
        warnings: list[str] = []
        snapshot_started = datetime.now(UTC)
        try:
            async with httpx.AsyncClient(timeout=self.settings.timeout_nasdaq_seconds) as client:
                for page in range(max(1, self.settings.nasdaq_options_max_pages)):
                    offset = page * self.settings.nasdaq_options_page_size
                    response = await asyncio.wait_for(
                        client.get(
                            self.settings.nasdaq_qqq_option_chain_url,
                            params={
                                "assetclass": "etf",
                                "limit": self.settings.nasdaq_options_page_size,
                                "offset": offset,
                                "fromdate": today.isoformat(),
                                "todate": to_date.isoformat(),
                                "excode": "oprac",
                                "callput": "callput",
                                "money": self.settings.nasdaq_options_default_money,
                                "type": self.settings.nasdaq_options_default_type,
                            },
                            headers=_json_headers(symbol),
                        ),
                        timeout=min(float(self.settings.timeout_nasdaq_seconds), 25.0),
                    )
                    response.raise_for_status()
                    payload = response.json()
                    data = payload.get("data") or {}
                    total = _int(data.get("totalRecord"))
                    total_records.append(total)
                    rows = ((data.get("table") or {}).get("rows")) or []
                    page_contracts, page_warnings = normalize_option_rows(
                        rows,
                        underlying=symbol,
                        retrieved_at=_iso(datetime.now(UTC)),
                        source_timestamp=data.get("lastTrade"),
                    )
                    warnings.extend(page_warnings)
                    contracts.extend(page_contracts)
                    pages_fetched += 1
                    if not rows or (total is not None and len(contracts) >= total):
                        break
        except TimeoutError:
            return _status("provider_timeout", "Nasdaq QQQ option-chain request timed out", started)
        except Exception as exc:
            return _status("provider_failed", str(exc) or "Nasdaq QQQ option-chain request failed", started)

        contracts, duplicate_rows = _dedupe_contracts(contracts)
        expiration_count = len({item["expiration_date"] for item in contracts if item.get("expiration_date")})
        strike_count = len({item["strike"] for item in contracts if item.get("strike") is not None})
        non_atomic = len({item for item in total_records if item is not None}) > 1
        first_total = next((item for item in total_records if item is not None), None)
        incomplete = bool(first_total and len(contracts) < first_total and pages_fetched >= self.settings.nasdaq_options_max_pages)
        requested_expirations = sorted({item["expiration_date"] for item in contracts if item.get("expiration_date")})
        max_pages_reached = pages_fetched >= self.settings.nasdaq_options_max_pages
        coverage_contract_pct = round((len(contracts) / first_total) * 100, 4) if first_total else None
        requested_scope_complete = bool(contracts and not incomplete and not non_atomic)
        full_chain_complete = bool(first_total and len(contracts) >= first_total and not non_atomic)
        partial_reason = "max_pages_reached" if incomplete and max_pages_reached else ("non_atomic_total_record" if non_atomic else None)
        aggregates = option_chain_aggregates(
            contracts,
            total_records=first_total,
            incomplete=incomplete,
            requested_expirations=requested_expirations,
            requested_scope_complete=requested_scope_complete,
            full_chain_complete=full_chain_complete,
            max_pages_reached=max_pages_reached,
            partial_reason=partial_reason,
        )
        if non_atomic:
            warnings.append("nasdaq_options_total_record_changed_during_collection")
        if incomplete:
            warnings.append("nasdaq_options_snapshot_incomplete")
        now = datetime.now(UTC)
        return {
            "status": "partial" if incomplete else ("found" if contracts else "not_found"),
            "provider": self.source,
            "source": "Nasdaq",
            "source_url": self.settings.nasdaq_qqq_option_chain_url,
            "retrieved_at": _iso(now),
            "valid_until": _iso(now + timedelta(minutes=self.settings.nasdaq_options_cache_minutes)),
            "snapshot": {
                "underlying": symbol,
                "instrument_family": "ETF_OPTIONS",
                "proxy_for": "NASDAQ_100",
                "direct_mnq_options_data": False,
                "snapshot_started_at": _iso(snapshot_started),
                "snapshot_completed_at": _iso(now),
                "source_timestamp": contracts[0].get("source_timestamp") if contracts else None,
                "non_atomic": non_atomic,
                "incomplete": incomplete,
                "requested_scope_complete": requested_scope_complete,
                "full_chain_complete": full_chain_complete,
                "coverage_contract_pct": coverage_contract_pct,
                "covered_expirations": requested_expirations,
            },
            "contracts": contracts,
            "open_interest_matrix": aggregates["open_interest_matrix"],
            "observed_aggregates": aggregates["observed_aggregates"],
            "global_aggregates": aggregates["global_aggregates"],
            "aggregates": aggregates["observed_aggregates"],
            "diagnostics": {
                "totalRecord_first": first_total,
                "totalRecord_last": next((item for item in reversed(total_records) if item is not None), None),
                "provider_total_chain_records": first_total,
                "requested_scope_records": len(contracts),
                "pages_fetched": pages_fetched,
                "rows_fetched": len(contracts) + duplicate_rows,
                "unique_contracts": len(contracts),
                "duplicate_rows": duplicate_rows,
                "expiration_count": expiration_count,
                "strike_count": strike_count,
                "null_oi_count": sum(
                    1
                    for item in contracts
                    if item.get("call_open_interest") is None or item.get("put_open_interest") is None
                ),
                "non_atomic": non_atomic,
                "incomplete": incomplete,
                "computed_from_partial_snapshot": incomplete,
                "coverage_contract_pct": coverage_contract_pct,
                "covered_expirations": requested_expirations,
                "requested_expirations": requested_expirations,
                "missing_expirations": [] if requested_scope_complete else None,
                "max_pages_reached": max_pages_reached,
                "partial_reason": partial_reason,
                "requested_scope_complete": requested_scope_complete,
                "full_chain_complete": full_chain_complete,
            },
            "warnings": warnings,
            "errors": [],
            "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
        }


def normalize_option_rows(
    rows: list[dict[str, Any]],
    *,
    underlying: str = "QQQ",
    retrieved_at: str | None = None,
    source_timestamp: Any = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    contracts: list[dict[str, Any]] = []
    warnings: list[str] = []
    current_expiration: str | None = None
    for row in rows:
        if row.get("expirygroup"):
            current_expiration = _parse_expiry_group(row.get("expirygroup"))
            continue
        if row.get("strike") in (None, ""):
            continue
        expiration = current_expiration or _parse_abbrev_expiry(row.get("expiryDate"))
        strike = _float(row.get("strike"))
        if not expiration or strike is None:
            warnings.append("nasdaq_options_row_missing_expiration_or_strike")
            continue
        contracts.append(
            {
                "underlying": underlying,
                "instrument_family": "ETF_OPTIONS",
                "proxy_for": "NASDAQ_100",
                "direct_mnq_options_data": False,
                "expiration_date": expiration,
                "strike": strike,
                "call_last": _float(row.get("c_Last")),
                "call_change": _float(row.get("c_Change")),
                "call_bid": _float(row.get("c_Bid")),
                "call_ask": _float(row.get("c_Ask")),
                "call_volume": parse_int_value(row.get("c_Volume")),
                "call_open_interest": parse_int_value(row.get("c_Openinterest")),
                "put_last": _float(row.get("p_Last")),
                "put_change": _float(row.get("p_Change")),
                "put_bid": _float(row.get("p_Bid")),
                "put_ask": _float(row.get("p_Ask")),
                "put_volume": parse_int_value(row.get("p_Volume")),
                "put_open_interest": parse_int_value(row.get("p_Openinterest")),
                "source": "Nasdaq",
                "retrieved_at": retrieved_at,
                "source_timestamp": source_timestamp,
            }
        )
    return contracts, warnings


def option_chain_aggregates(
    contracts: list[dict[str, Any]],
    *,
    total_records: int | None = None,
    incomplete: bool = False,
    requested_expirations: list[str] | None = None,
    requested_scope_complete: bool | None = None,
    full_chain_complete: bool | None = None,
    max_pages_reached: bool = False,
    partial_reason: str | None = None,
) -> dict[str, Any]:
    by_strike: dict[float, dict[str, Any]] = {}
    by_expiration: dict[str, dict[str, Any]] = {}
    total_call_oi = total_put_oi = total_call_volume = total_put_volume = 0
    for item in contracts:
        strike = item["strike"]
        expiration = item["expiration_date"]
        call_oi = int(item.get("call_open_interest") or 0)
        put_oi = int(item.get("put_open_interest") or 0)
        call_volume = int(item.get("call_volume") or 0)
        put_volume = int(item.get("put_volume") or 0)
        strike_row = by_strike.setdefault(
            strike,
            {
                "strike": strike,
                "call_open_interest": 0,
                "put_open_interest": 0,
                "combined_open_interest": 0,
                "call_volume": 0,
                "put_volume": 0,
            },
        )
        exp_row = by_expiration.setdefault(
            expiration,
            {
                "expiration_date": expiration,
                "call_open_interest": 0,
                "put_open_interest": 0,
                "combined_open_interest": 0,
                "call_volume": 0,
                "put_volume": 0,
            },
        )
        for row in (strike_row, exp_row):
            row["call_open_interest"] += call_oi
            row["put_open_interest"] += put_oi
            row["combined_open_interest"] += call_oi + put_oi
            row["call_volume"] += call_volume
            row["put_volume"] += put_volume
        total_call_oi += call_oi
        total_put_oi += put_oi
        total_call_volume += call_volume
        total_put_volume += put_volume
    total_oi = total_call_oi + total_put_oi
    strike_rows = sorted(by_strike.values(), key=lambda item: item["strike"])
    expiration_rows = sorted(by_expiration.values(), key=lambda item: item["expiration_date"])
    for row in strike_rows:
        row["pct_observed_open_interest"] = round((row["combined_open_interest"] / total_oi) * 100, 4) if total_oi else 0.0
    for row in expiration_rows:
        row["pct_observed_open_interest"] = round((row["combined_open_interest"] / total_oi) * 100, 4) if total_oi else 0.0
    coverage_contract_pct = round((len(contracts) / total_records) * 100, 4) if total_records else None
    requested_expirations = requested_expirations or sorted({item["expiration_date"] for item in contracts if item.get("expiration_date")})
    requested_scope_complete = bool(requested_scope_complete) if requested_scope_complete is not None else not incomplete
    full_chain_complete = bool(full_chain_complete) if full_chain_complete is not None else (not incomplete if total_records else False)
    scope = {
        "snapshot_complete": not incomplete,
        "computed_from_partial_snapshot": incomplete,
        "coverage_contract_pct": coverage_contract_pct,
        "covered_expirations": requested_expirations,
        "requested_expirations": requested_expirations,
        "missing_expirations": [] if requested_scope_complete else None,
        "requested_scope_complete": requested_scope_complete,
        "full_chain_complete": full_chain_complete,
        "provider_total_chain_records": total_records,
        "requested_scope_records": len(contracts),
        "max_pages_reached": max_pages_reached,
        "partial_reason": partial_reason,
    }
    observed = {
        "scope": scope,
        "observed_call_open_interest": total_call_oi,
        "observed_put_open_interest": total_put_oi,
        "observed_put_call_open_interest_ratio": _ratio(total_put_oi, total_call_oi),
        "observed_call_volume": total_call_volume,
        "observed_put_volume": total_put_volume,
        "observed_put_call_volume_ratio": _ratio(total_put_volume, total_call_volume),
        "top_observed_call_open_interest_concentrations": _top(strike_rows, "call_open_interest"),
        "top_observed_put_open_interest_concentrations": _top(strike_rows, "put_open_interest"),
        "top_observed_combined_open_interest_concentrations": _top(strike_rows, "combined_open_interest"),
        "percentage_of_observed_open_interest_by_strike": [
            {"strike": row["strike"], "pct_observed_open_interest": row["pct_observed_open_interest"]}
            for row in sorted(strike_rows, key=lambda item: item["pct_observed_open_interest"], reverse=True)[:15]
        ],
        "percentage_of_observed_open_interest_by_expiration": [
            {"expiration_date": row["expiration_date"], "pct_observed_open_interest": row["pct_observed_open_interest"]}
            for row in sorted(expiration_rows, key=lambda item: item["pct_observed_open_interest"], reverse=True)[:15]
        ],
    }
    global_aggregates = None if incomplete else {
        "scope": scope,
        "call_open_interest": total_call_oi,
        "put_open_interest": total_put_oi,
        "put_call_open_interest_ratio": _ratio(total_put_oi, total_call_oi),
        "call_volume": total_call_volume,
        "put_volume": total_put_volume,
        "put_call_volume_ratio": _ratio(total_put_volume, total_call_volume),
    }
    return {
        "open_interest_matrix": {"by_strike": strike_rows, "by_expiration": expiration_rows},
        "observed_aggregates": observed,
        "global_aggregates": global_aggregates,
        "aggregates": observed,
    }


def _dedupe_contracts(contracts: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    seen: set[tuple[str, float]] = set()
    output: list[dict[str, Any]] = []
    duplicate_rows = 0
    for item in contracts:
        key = (str(item.get("expiration_date")), float(item.get("strike") or 0))
        if key in seen:
            duplicate_rows += 1
            continue
        seen.add(key)
        output.append(item)
    return output, duplicate_rows


def _parse_expiry_group(value: Any) -> str | None:
    if not value:
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(str(value).strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _parse_abbrev_expiry(value: Any) -> str | None:
    if not value:
        return None
    year = datetime.now(UTC).year
    for fmt in ("%b %d", "%B %d"):
        try:
            parsed = datetime.strptime(str(value).strip(), fmt).replace(year=year)
            return parsed.date().isoformat()
        except ValueError:
            continue
    return None


def _float(value: Any) -> float | None:
    parsed = parse_economic_value(value)
    return parsed["value"] if parsed["parse_status"] == "parsed" else None


def _int(value: Any) -> int | None:
    return parse_int_value(value)


def _ratio(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def _top(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    return [
        {"strike": item["strike"], field: item[field]}
        for item in sorted(rows, key=lambda row: row.get(field) or 0, reverse=True)[:15]
        if item.get(field)
    ]


def _status(status: str, reason: str, started: datetime) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "status": status,
        "provider": "Nasdaq QQQ Option Chain",
        "source": "Nasdaq",
        "source_url": "https://api.nasdaq.com/api/quote/QQQ/option-chain",
        "retrieved_at": _iso(now),
        "valid_until": _iso(now + timedelta(minutes=15)),
        "snapshot": {},
        "contracts": [],
        "open_interest_matrix": {"by_strike": [], "by_expiration": []},
        "observed_aggregates": {},
        "global_aggregates": None,
        "aggregates": {},
        "diagnostics": {
            "unique_contracts": 0,
            "pages_fetched": 0,
            "computed_from_partial_snapshot": False,
            "coverage_contract_pct": None,
            "requested_scope_complete": False,
            "full_chain_complete": False,
        },
        "warnings": [reason] if status != "provider_failed" else [],
        "errors": [reason] if status == "provider_failed" else [],
        "duration_ms": int((datetime.now(UTC) - started).total_seconds() * 1000),
    }


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_headers(symbol: str) -> dict[str, str]:
    return {
        **REQUEST_HEADERS,
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Origin": "https://www.nasdaq.com",
        "Referer": f"https://www.nasdaq.com/market-activity/etf/{symbol.lower()}/option-chain",
    }
