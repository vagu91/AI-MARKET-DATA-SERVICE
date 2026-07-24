from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from app.core.config import Settings
from app.services.data_freshness_service import parse_datetime
from app.services.event_driven_lifecycle_service import compute_datum_lifecycle


NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class LifecyclePolicy:
    refresh_policy: str
    carry_forward_allowed: bool
    stale_policy: str
    retention_policy: str


LIFECYCLE_POLICIES: dict[str, LifecyclePolicy] = {
    "news": LifecyclePolicy(
        "db_first_then_provider_on_refresh_or_expiry",
        False,
        "never_expose_outside_context_date; retain_only_as_history",
        "delete_history_after_market_news_retention_days",
    ),
    "macro_snapshot": LifecyclePolicy(
        "db_first_then_official_provider_on_expiry",
        True,
        "published_value_may_be_carried_only_with_explicit_stale_freshness",
        "replace_by_series_id; delete_expired_cache_after_market_facts_retention_days",
    ),
    "macro_consensus": LifecyclePolicy(
        "refresh_until_release_then_stop",
        False,
        "invalid_at_release_time; never_substitute_for_actual",
        "retain_with_economic_event_history",
    ),
    "macro_actual": LifecyclePolicy(
        "release_retry_only_for_scheduled_release_events",
        True,
        "published_actual_is_historical_after_event_window",
        "retain_with_economic_event_history",
    ),
    "fed_expectations": LifecyclePolicy(
        "db_first_then_provider_on_expiry",
        True,
        "stale_allowed_only_when_labeled_last_known_good",
        "delete_snapshot_history_after_snapshot_history_retention_days",
    ),
    "risk_context": LifecyclePolicy(
        "db_first_then_provider_on_expiry",
        True,
        "stale_allowed_only_when_labeled_stale_acceptable",
        "delete_snapshot_history_after_snapshot_history_retention_days",
    ),
    "vvix": LifecyclePolicy(
        "db_first_then_cboe_on_expiry",
        True,
        "never_label_stale_value_live",
        "replace_latest; retain_under_risk_snapshot_policy",
    ),
    "skew": LifecyclePolicy(
        "db_first_then_cboe_on_expiry",
        True,
        "never_label_stale_value_live",
        "replace_latest; retain_under_risk_snapshot_policy",
    ),
    "vix_futures": LifecyclePolicy(
        "db_first_then_cboe_on_expiry",
        True,
        "expired_curve_is_last_known_good_not_current_curve",
        "replace_curve; retain_under_risk_snapshot_policy",
    ),
    "put_call": LifecyclePolicy(
        "db_first_then_cboe_or_exchange_provider_on_expiry",
        True,
        "expired_ratio_is_last_known_good not current_ratio",
        "replace_latest; retain_under_risk_snapshot_policy",
    ),
    "nasdaq_weights": LifecyclePolicy(
        "db_first_then_official_holdings_then_declared_proxy",
        True,
        "stale_allowed_within_configured_tolerance and never_upgrade_proxy_to_official",
        "replace_canonical_snapshot; delete_expired_cache_after_market_facts_retention_days",
    ),
    "earnings": LifecyclePolicy(
        "db_first_then_provider_on_expiry",
        False,
        "never_expose_as_upcoming_after_event_date",
        "replace_by_issuer_and_date; delete_expired_cache_after_market_facts_retention_days",
    ),
    "cot": LifecyclePolicy(
        "db_first_then_cftc_provider_on_expiry",
        True,
        "weekly_release_may_be_carried_only_with_current_release_or_stale_label",
        "replace_latest; delete_expired_cache_after_market_facts_retention_days",
    ),
    "aaii": LifecyclePolicy(
        "db_first_then_provider_on_expiry",
        True,
        "weekly_release_may_be_carried_only_with_current_release_or_stale label",
        "replace_latest; delete_expired_cache_after_market_facts_retention_days",
    ),
    "sentiment": LifecyclePolicy(
        "db_first_then_provider_on_expiry",
        False,
        "expired discussion snapshot is not current sentiment",
        "replace_latest; delete_expired_cache_after_market_facts_retention_days",
    ),
    "prediction_markets": LifecyclePolicy(
        "db_first_then_provider_on_expiry",
        False,
        "probabilities are unusable after expiry and are never AI-filled",
        "replace_latest; delete expired cache after market_facts_retention_days",
    ),
    "holiday_calendar": LifecyclePolicy(
        "db_first_then_calendar_provider_on_expiry",
        True,
        "versioned calendar fallback must remain explicitly non_official",
        "replace overlapping dates; retain future dates and bounded history",
    ),
    "market_schedule": LifecyclePolicy(
        "recompute_at_session_transition; refresh official calendar on expiry",
        True,
        "deterministic fallback allowed only with source classification and warning",
        "replace computed session on every materialization",
    ),
}


def attach_lifecycle_metadata(
    full: dict[str, Any],
    *,
    settings: Settings,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = _aware(now or datetime.now(UTC))
    output = dict(full)
    schedule = dict(output.get("market_schedule") or {})
    context_date = str(schedule.get("context_date") or now.astimezone(NEW_YORK).date().isoformat())

    sources = _category_sources(output)
    catalog = {
        category: _record(
            category,
            sources.get(category),
            settings=settings,
            now=now,
            context_date=context_date,
            explicit_valid_until=_explicit_valid_until(category, output, now=now),
        )
        for category in LIFECYCLE_POLICIES
    }

    metadata = dict(output.get("metadata") or {})
    metadata["data_lifecycle"] = catalog
    output["metadata"] = metadata

    _attach(output, "news_context", catalog["news"])
    _attach(output, "macro_snapshot", catalog["macro_snapshot"])
    _attach(output, "rates_expectations", catalog["fed_expectations"])
    _attach(output, "risk_context", catalog["risk_context"])
    _attach(output, "positioning", catalog["cot"])
    _attach(output, "market_schedule", catalog["market_schedule"])
    schedule = dict(output.get("market_schedule") or {})
    schedule["holiday_calendar_lifecycle"] = catalog["holiday_calendar"]
    output["market_schedule"] = schedule

    risk = dict(output.get("risk_context") or {})
    _attach(risk, "vvix", catalog["vvix"])
    _attach(risk, "skew", catalog["skew"])
    _attach(risk, "vix_term_structure", catalog["vix_futures"])
    _attach(risk, "put_call", catalog["put_call"])
    output["risk_context"] = risk

    nasdaq = dict(output.get("nasdaq_context") or {})
    _attach(nasdaq, "qqq_holdings", catalog["nasdaq_weights"])
    _attach(nasdaq, "earnings", catalog["earnings"])
    _attach_earnings_items(nasdaq, settings=settings, now=now)
    output["nasdaq_context"] = nasdaq

    sentiment = dict(output.get("sentiment_context") or {})
    _attach(sentiment, "aaii", catalog["aaii"])
    _attach(sentiment, "technology_discussion", catalog["sentiment"])
    _attach(sentiment, "prediction_markets", catalog["prediction_markets"])
    output["sentiment_context"] = sentiment
    return output


def _category_sources(full: dict[str, Any]) -> dict[str, Any]:
    events = []
    for rows in (full.get("event_calendar") or {}).values():
        if isinstance(rows, list):
            events.extend(item for item in rows if isinstance(item, dict))
    return {
        "news": full.get("news_context") or {},
        "macro_snapshot": full.get("macro_snapshot") or {},
        "macro_consensus": [item for item in events if _has_event_value(item, "consensus")],
        "macro_actual": [item for item in events if _has_event_value(item, "actual")],
        "fed_expectations": full.get("rates_expectations") or {},
        "risk_context": full.get("risk_context") or {},
        "vvix": (full.get("risk_context") or {}).get("vvix") or {},
        "skew": (full.get("risk_context") or {}).get("skew") or {},
        "vix_futures": (full.get("risk_context") or {}).get("vix_term_structure") or {},
        "put_call": (full.get("risk_context") or {}).get("put_call") or {},
        "nasdaq_weights": (full.get("nasdaq_context") or {}).get("qqq_holdings") or {},
        "earnings": (full.get("nasdaq_context") or {}).get("earnings") or {},
        "cot": full.get("positioning") or {},
        "aaii": (full.get("sentiment_context") or {}).get("aaii") or {},
        "sentiment": (full.get("sentiment_context") or {}).get("technology_discussion") or full.get("social_sentiment") or {},
        "prediction_markets": (full.get("sentiment_context") or {}).get("prediction_markets") or {},
        "holiday_calendar": (full.get("market_schedule") or {}).get("holidays") or [],
        "market_schedule": full.get("market_schedule") or {},
    }


def _record(
    category: str,
    source: Any,
    *,
    settings: Settings,
    now: datetime,
    context_date: str,
    explicit_valid_until: datetime | None,
) -> dict[str, Any]:
    policy = LIFECYCLE_POLICIES[category]
    context_anchor = _context_date_anchor(context_date)
    lifecycle_input = dict(source) if isinstance(source, dict) else {"items": source}
    born_at = _latest_timestamp(
        source,
        {"retrieved_at", "created_at", "generated_at", "generated_at_utc"},
    )
    if category == "market_schedule":
        born_at = context_anchor
    valid_until = explicit_valid_until or _earliest_timestamp(
        source,
        {"valid_until", "consensus_valid_until"},
    )
    data_present = _operational_value_present(category, source)
    if born_at is not None:
        lifecycle_input.setdefault("observed_at", born_at.isoformat())
    else:
        lifecycle_input.setdefault("observed_at", context_anchor.isoformat())
    if valid_until is not None:
        lifecycle_input["valid_until"] = valid_until.isoformat()
    next_refresh = _earliest_timestamp(source, {"next_refresh", "next_refresh_at"})
    if next_refresh is not None:
        lifecycle_input["next_refresh_at"] = next_refresh.isoformat()
    lifecycle = compute_datum_lifecycle(
        category,
        category,
        lifecycle_input,
        settings=settings,
        now=now,
    )
    valid_until = parse_datetime(lifecycle.valid_until)
    next_refresh = parse_datetime(lifecycle.next_refresh_at)
    retention_days = _retention_days(category, settings)
    delete_after = valid_until + timedelta(days=retention_days) if valid_until and retention_days is not None else None
    source_status = str(
        source.get("status") if isinstance(source, dict) else ""
    ).upper()
    freshness_state = lifecycle.freshness_state
    if not data_present and source_status in {"DISABLED", "NOT_CONFIGURED"}:
        freshness_state = "NOT_CONFIGURED"
    elif not data_present and source_status in {
        "NO_DATA",
        "NOT_FOUND",
        "NO_DATA_AVAILABLE",
    }:
        freshness_state = "NO_DATA"
    return {
        "category": category,
        "born_at": lifecycle.observed_at,
        "trigger_class": lifecycle.trigger_class,
        "freshness_state": freshness_state,
        "observed_at": lifecycle.observed_at,
        "data_as_of": lifecycle.data_as_of,
        "published_at": lifecycle.published_at,
        "event_at": lifecycle.event_at,
        "valid_from": lifecycle.valid_from,
        "valid_until": lifecycle.valid_until,
        "next_refresh": lifecycle.next_refresh_at,
        "next_refresh_at": lifecycle.next_refresh_at,
        "next_retry_at": lifecycle.next_retry_at,
        "superseded_by": lifecycle.superseded_by,
        "refresh_reason": lifecycle.refresh_reason,
        "materiality_fingerprint": lifecycle.materiality_fingerprint,
        "source_lineage": list(lifecycle.source_lineage),
        "acquisition_method": lifecycle.acquisition_method,
        "retry_policy": lifecycle.retry_policy,
        "refresh_policy": policy.refresh_policy,
        "carry_forward_allowed": policy.carry_forward_allowed,
        "stale_policy": policy.stale_policy,
        "retention_policy": policy.retention_policy,
        "delete_after": _iso(delete_after),
        "context_date": context_date,
        "data_present": data_present,
        "currently_valid": bool(
            data_present and freshness_state == "FRESH"
        ),
    }


def _explicit_valid_until(category: str, full: dict[str, Any], *, now: datetime) -> datetime | None:
    schedule = full.get("market_schedule") or {}
    if category == "news":
        local = now.astimezone(NEW_YORK)
        return datetime.combine(local.date() + timedelta(days=1), datetime.min.time(), NEW_YORK).astimezone(UTC)
    if category == "market_schedule":
        transitions = [
            parse_datetime((schedule.get(session) or {}).get(field))
            for session in ("nasdaq_cash_session", "mnq_session")
            for field in ("next_open", "next_close")
        ]
        future = [item for item in transitions if item and item > now]
        return min(future, default=None)
    if category == "earnings":
        events = ((full.get("nasdaq_context") or {}).get("earnings") or {}).get("upcoming") or []
        dates = [parse_datetime(item.get("earnings_date") or item.get("date")) for item in events if isinstance(item, dict)]
        future = [item for item in dates if item and item >= now]
        return min(future, default=None)
    return None


def _attach(parent: dict[str, Any], key: str, lifecycle: dict[str, Any]) -> None:
    value = parent.get(key)
    if not isinstance(value, dict):
        return
    updated = dict(value)
    updated["lifecycle"] = lifecycle
    parent[key] = updated


def _attach_earnings_items(
    nasdaq: dict[str, Any],
    *,
    settings: Settings,
    now: datetime,
) -> None:
    earnings = nasdaq.get("earnings")
    if not isinstance(earnings, dict):
        return
    updated = dict(earnings)
    for bucket in (
        "upcoming",
        "recent",
        "items",
        "events",
        "upcoming_mega_cap_earnings_14d",
        "released_earnings",
    ):
        rows = earnings.get(bucket)
        if not isinstance(rows, list):
            continue
        enriched: list[Any] = []
        for raw in rows:
            if not isinstance(raw, dict):
                enriched.append(raw)
                continue
            item = dict(raw)
            issuer = str(
                item.get("issuer")
                or item.get("company")
                or item.get("company_name")
                or item.get("ticker")
                or item.get("symbol")
                or "unknown"
            ).strip().upper()
            event_date = str(
                item.get("earnings_date")
                or item.get("date")
                or item.get("event_date")
                or (
                    parse_datetime(item.get("event_at")).date().isoformat()
                    if parse_datetime(item.get("event_at"))
                    else None
                )
                or "unknown"
            )
            lifecycle = compute_datum_lifecycle(
                "earnings",
                f"{issuer}:{event_date}",
                item,
                settings=settings,
                now=now,
                attempt_count=int(item.get("attempt_count") or 0),
                refresh_reason="issuer_event_lifecycle",
            )
            item["lifecycle"] = lifecycle.as_dict()
            enriched.append(item)
        updated[bucket] = enriched
    nasdaq["earnings"] = updated


def _has_event_value(event: dict[str, Any], field: str) -> bool:
    enrichment = event.get("enrichment") or {}
    if enrichment.get(field) not in (None, ""):
        return True
    return any(metric.get(field) not in (None, "") for metric in enrichment.get("metrics") or [] if isinstance(metric, dict))


def _timestamps(value: Any, keys: set[str]) -> list[datetime]:
    found: list[datetime] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            if key == "lifecycle":
                continue
            if key in keys:
                parsed = parse_datetime(nested)
                if parsed:
                    found.append(parsed)
            elif isinstance(nested, (dict, list)):
                found.extend(_timestamps(nested, keys))
    elif isinstance(value, list):
        for nested in value:
            found.extend(_timestamps(nested, keys))
    return found


def _earliest_timestamp(value: Any, keys: set[str]) -> datetime | None:
    values = _timestamps(value, keys)
    return min(values, default=None)


def _latest_timestamp(value: Any, keys: set[str]) -> datetime | None:
    values = _timestamps(value, keys)
    return max(values, default=None)


def _retention_days(category: str, settings: Settings) -> int | None:
    if category == "news":
        return settings.market_news_retention_days
    if category in {"macro_consensus", "macro_actual"}:
        return settings.economic_events_history_retention_days
    if category in {"fed_expectations", "risk_context", "vvix", "skew", "vix_futures", "put_call"}:
        return settings.snapshot_history_retention_days
    if category == "market_schedule":
        return None
    return settings.market_facts_retention_days


def _default_ttl(category: str, settings: Settings) -> timedelta:
    if category == "news":
        return timedelta(hours=settings.default_news_ttl_hours)
    if category == "fed_expectations":
        return timedelta(minutes=settings.investing_fed_rate_monitor_ttl_minutes)
    if category in {"risk_context", "vvix", "skew", "vix_futures", "put_call"}:
        return timedelta(minutes=settings.risk_context_ttl_minutes)
    if category == "nasdaq_weights":
        return timedelta(hours=settings.qqq_holdings_ttl_hours)
    if category == "earnings":
        return timedelta(hours=settings.earnings_ttl_hours)
    if category in {"sentiment", "prediction_markets"}:
        return timedelta(minutes=settings.social_sentiment_ttl_minutes)
    if category in {"cot", "aaii"}:
        return timedelta(hours=6)
    if category in {"holiday_calendar", "market_schedule"}:
        return timedelta(hours=24)
    return timedelta(hours=settings.default_fact_ttl_hours)


def _has_content(value: Any) -> bool:
    if isinstance(value, dict):
        return any(
            key != "lifecycle" and _has_content(nested)
            for key, nested in value.items()
            if key not in {"warnings", "errors", "diagnostics"}
        )
    if isinstance(value, list):
        return any(_has_content(item) for item in value)
    return value not in (None, "", False)


def _operational_value_present(category: str, value: Any) -> bool:
    if not isinstance(value, dict):
        return _has_content(value)
    status = str(value.get("status") or "").upper()
    validation = (
        value.get("validation")
        if isinstance(value.get("validation"), dict)
        else {}
    )
    if (
        status in {
            "DISABLED",
            "NOT_CONFIGURED",
            "NOT_FOUND",
            "NO_DATA",
            "NO_DATA_AVAILABLE",
            "PROVIDER_FAILED",
            "QUARANTINED",
            "REJECTED",
        }
        or str(value.get("source_classification") or "").lower()
        == "invalid_source"
        or str(value.get("source_audit_status") or "").upper()
        in {"QUARANTINED", "REJECTED"}
        or str(validation.get("status") or "").lower() == "rejected"
    ):
        return False
    if category == "news":
        return bool(value.get("articles") or value.get("latest"))
    if category == "cot":
        groups = [
            value.get("asset_managers"),
            value.get("leveraged_funds"),
            value.get("dealers"),
        ]
        return bool(
            value.get("report_date")
            and (
                value.get("open_interest") not in (None, "")
                or any(
                    isinstance(group, dict)
                    and any(
                        group.get(field) not in (None, "")
                        for field in ("long", "short", "spreading", "net")
                    )
                    for group in groups
                )
            )
        )
    if category == "aaii":
        return all(
            value.get(field) not in (None, "")
            for field in ("bullish_pct", "neutral_pct", "bearish_pct")
        )
    if category == "prediction_markets":
        return bool(value.get("markets") or value.get("items"))
    if category == "sentiment":
        return bool(
            value.get("items")
            or value.get("articles")
            or value.get("score") not in (None, "")
        )
    if category == "nasdaq_weights":
        return bool(
            value.get("holdings") or value.get("top_holdings")
        ) and value.get("reliability") != 0
    if category in {"vvix", "skew"}:
        return value.get("value") not in (None, "") or value.get(
            "current_price"
        ) not in (None, "")
    if category == "vix_futures":
        return bool(value.get("contracts") or value.get("curve"))
    if category == "put_call":
        return bool(value.get("ratios") or value.get("by_id"))
    return _has_content(value)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).replace(microsecond=0).isoformat() if value else None


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def _context_date_anchor(context_date: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(context_date).date()
    except ValueError:
        parsed = datetime.now(NEW_YORK).date()
    return datetime.combine(parsed, datetime.min.time(), NEW_YORK).astimezone(UTC)
