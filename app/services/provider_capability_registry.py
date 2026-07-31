from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import pkgutil
import re
from collections import Counter
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from itertools import product
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterable

from app.services.research_agent_enablement import RESEARCH_AGENT_REGISTRY
from app.services.research_profiles import JOB_PROFILE, PROFILES


@dataclass(frozen=True, slots=True)
class CapabilityRegistration:
    dataset_id: str
    metric_id: str
    supported_fields: tuple[str, ...]
    frequency: str
    transformation: str
    probe_id: str
    field_validator_id: str
    probe_adapter_path: str | None = None
    ai_eligible: bool = False
    request_group: str | None = None
    canonical_metric_ids: tuple[str, ...] = ()
    delivery_capability_id: str | None = None
    delivery_capability_ids: tuple[str, ...] = ()
    delivery_field_map: tuple[tuple[str, str], ...] = ()
    probe_query_id: str | None = None
    degradable_quality_checks: tuple[str, ...] = ()
    runtime_profile_id: str | None = None
    runtime_job_type: str | None = None
    runtime_topic: str | None = None
    runtime_enable_setting: str | None = None
    runtime_required_fields: tuple[str, ...] = ()
    runtime_source_domains: tuple[str, ...] = ()
    audit_only_fields: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class OfficialMetricRegistration:
    canonical_metric_id: str
    dataset_id: str
    provider_id: str
    source_series_id: str
    transformation: str
    seasonal_adjustment: str
    frequency: str
    unit: str
    comparison_lag: int
    precision: str
    canonical_url: str

    @property
    def event_metric_id(self) -> str:
        """Compatibility alias for existing event reconciliation call sites."""

        return self.canonical_metric_id

    @property
    def provider(self) -> str:
        """Compatibility alias for existing provider dispatch call sites."""

        return self.provider_id


@dataclass(frozen=True, slots=True)
class ProviderRegistration:
    provider_id: str
    adapter_path: str
    provider_type: str
    capabilities: tuple[CapabilityRegistration, ...]
    allowed_roles: tuple[str, ...]
    credential_requirements: tuple[str, ...] = ()
    timeout: str | float = "http_timeout_seconds"
    max_attempts: int = 1
    retry_policy: str = "NONE"
    known_rate_limit: str = "UNKNOWN"
    probe_id: str = ""
    probe_enabled: bool = True
    enable_setting: str | None = None
    runtime_adapter: bool = False
    additional_adapter_paths: tuple[str, ...] = ()
    uncertified_runtime_leaves: tuple[str, ...] = ()
    publisher: str | None = None
    distributor: str | None = None
    acquisition_provider: str | None = None
    source_domains: tuple[str, ...] = ()
    request_group: str | None = None
    capture_mode: str = "HTTPX"
    audit_leaf_request_count: int | None = None
    audit_source_url_budget: int = 0
    configuration_setting: str | None = None
    configuration_value: str | None = None
    terminal_audit_reason: str | None = None


@dataclass(frozen=True, slots=True)
class DatasetSourcePolicy:
    dataset_id: str
    section: str
    frequency: str
    sla_seconds: int
    primary_provider: str
    fallback_providers: tuple[str, ...]
    canonical_repository: str
    ai_fallback_providers: tuple[str, ...] = ()
    provider_strategy: str = "FALLBACK"
    required_for_analysis: bool = True


@dataclass(frozen=True, slots=True)
class MarketFactRepositoryDatasetQuery:
    frequency: str
    fact_types: tuple[str, ...]
    series_ids: tuple[str, ...] = ()


OFFICIAL_METRIC_REGISTRATIONS: tuple[OfficialMetricRegistration, ...] = (
    OfficialMetricRegistration(
        "headline_cpi_mom", "cpi", "BLS", "CUSR0000SA0",
        "pct_change_mom", "SA", "monthly", "percent", 1, "0.1",
        "https://www.bls.gov/cpi/",
    ),
    OfficialMetricRegistration(
        "headline_cpi_yoy", "cpi", "BLS", "CUUR0000SA0",
        "pct_change_yoy", "NSA", "monthly", "percent", 12, "0.1",
        "https://www.bls.gov/cpi/",
    ),
    OfficialMetricRegistration(
        "core_cpi_mom", "cpi", "BLS", "CUSR0000SA0L1E",
        "pct_change_mom", "SA", "monthly", "percent", 1, "0.1",
        "https://www.bls.gov/cpi/",
    ),
    OfficialMetricRegistration(
        "core_cpi_yoy", "cpi", "BLS", "CUUR0000SA0L1E",
        "pct_change_yoy", "NSA", "monthly", "percent", 12, "0.1",
        "https://www.bls.gov/cpi/",
    ),
    OfficialMetricRegistration(
        "headline_ppi_mom", "ppi", "BLS", "WPSFD4",
        "pct_change_mom", "SA", "monthly", "percent", 1, "0.1",
        "https://www.bls.gov/ppi/",
    ),
    OfficialMetricRegistration(
        "headline_ppi_yoy", "ppi", "BLS", "WPUFD4",
        "pct_change_yoy", "NSA", "monthly", "percent", 12, "0.1",
        "https://www.bls.gov/ppi/",
    ),
    OfficialMetricRegistration(
        "nonfarm_payrolls_change", "nfp", "BLS", "CES0000000001",
        "delta", "SA", "monthly", "thousands of jobs", 1, "1",
        "https://www.bls.gov/ces/",
    ),
    OfficialMetricRegistration(
        "unemployment_rate", "employment", "BLS", "LNS14000000",
        "level", "SA", "monthly", "percent", 0, "0.1",
        "https://www.bls.gov/cps/",
    ),
    OfficialMetricRegistration(
        "average_hourly_earnings_mom", "wages", "BLS",
        "CES0500000003", "pct_change_mom", "SA", "monthly", "percent",
        1, "0.1", "https://www.bls.gov/ces/",
    ),
    OfficialMetricRegistration(
        "average_hourly_earnings_yoy", "wages", "BLS",
        "CES0500000003", "pct_change_yoy", "SA", "monthly", "percent",
        12, "0.1", "https://www.bls.gov/ces/",
    ),
    OfficialMetricRegistration(
        "employment_cost_index_qoq", "wages", "BLS",
        "CIU1010000000000A", "pct_change_qoq", "NSA", "quarterly",
        "percent", 1, "0.1", "https://www.bls.gov/eci/",
    ),
    OfficialMetricRegistration(
        "real_gdp_annualized_qoq", "gdp", "BEA", "BEA:GDP",
        "official_annualized_qoq_rate", "SAAR", "quarterly", "percent",
        0, "0.1",
        "https://www.bea.gov/data/gdp/gross-domestic-product",
    ),
    OfficialMetricRegistration(
        "real_gdp_yoy", "gdp", "BEA", "BEA:REAL_GDP",
        "pct_change_yoy", "SAAR", "quarterly", "percent", 4, "0.1",
        "https://www.bea.gov/data/gdp/gross-domestic-product",
    ),
    OfficialMetricRegistration(
        "headline_pce_mom", "pce", "BEA", "BEA:PCE_PRICE_INDEX",
        "pct_change_mom", "SA", "monthly", "percent", 1, "0.1",
        "https://www.bea.gov/data/consumer-spending/main",
    ),
    OfficialMetricRegistration(
        "headline_pce_yoy", "pce", "BEA", "BEA:PCE_PRICE_INDEX",
        "pct_change_yoy", "SA", "monthly", "percent", 12, "0.1",
        "https://www.bea.gov/data/consumer-spending/main",
    ),
    OfficialMetricRegistration(
        "core_pce_mom", "pce", "BEA", "BEA:CORE_PCE",
        "pct_change_mom", "SA", "monthly", "percent", 1, "0.1",
        (
            "https://www.bea.gov/data/personal-consumption-expenditures-"
            "price-index-excluding-food-and-energy"
        ),
    ),
    OfficialMetricRegistration(
        "core_pce_yoy", "pce", "BEA", "BEA:CORE_PCE",
        "pct_change_yoy", "SA", "monthly", "percent", 12, "0.1",
        (
            "https://www.bea.gov/data/personal-consumption-expenditures-"
            "price-index-excluding-food-and-energy"
        ),
    ),
    OfficialMetricRegistration(
        "personal_income_mom", "pce", "BEA", "BEA:PERSONAL_INCOME",
        "pct_change_mom", "SAAR", "monthly", "percent", 1, "0.1",
        "https://www.bea.gov/data/income-saving/personal-income",
    ),
    OfficialMetricRegistration(
        "personal_spending_mom", "pce", "BEA", "BEA:PERSONAL_SPENDING",
        "pct_change_mom", "SAAR", "monthly", "percent", 1, "0.1",
        "https://www.bea.gov/data/consumer-spending/main",
    ),
    OfficialMetricRegistration(
        "advance_retail_sales", "macro_calendar", "CENSUS",
        "CENSUS:MARTS:RETAIL_SALES", "level", "SA", "monthly",
        "millions_usd", 0, "0.1", "https://www.census.gov/retail/",
    ),
    OfficialMetricRegistration(
        "advance_durable_goods_orders", "macro_calendar", "CENSUS",
        "CENSUS:ADVM3:DURABLE_GOODS", "level", "SA", "monthly",
        "millions_usd", 0, "0.1",
        "https://www.census.gov/manufacturing/m3/",
    ),
    OfficialMetricRegistration(
        "housing_starts", "macro_calendar", "CENSUS",
        "CENSUS:RESCONST:HOUSING_STARTS", "level", "SAAR", "monthly",
        "thousands_annual_rate", 0, "1",
        "https://www.census.gov/construction/nrc/",
    ),
    OfficialMetricRegistration(
        "building_permits", "macro_calendar", "CENSUS",
        "CENSUS:RESCONST:BUILDING_PERMITS", "level", "SAAR", "monthly",
        "thousands_annual_rate", 0, "1",
        "https://www.census.gov/construction/nrc/",
    ),
    OfficialMetricRegistration(
        "international_trade_balance", "macro_calendar", "CENSUS",
        "CENSUS:FTD:TRADE_BALANCE", "level", "SA", "monthly",
        "millions_usd", 0, "0.1",
        "https://www.census.gov/foreign-trade/",
    ),
    OfficialMetricRegistration(
        "new_home_sales", "macro_calendar", "FRED", "HSN1F", "level",
        "SAAR", "monthly", "thousands_annual_rate", 1, "1",
        "https://fred.stlouisfed.org/series/HSN1F",
    ),
    OfficialMetricRegistration(
        "flash_services_pmi", "flash_services_pmi", "SPGLOBAL",
        "SPGLOBAL:US:FLASH_SERVICES_PMI", "level", "SA", "monthly",
        "index_points", 1, "0.1",
        "https://www.pmi.spglobal.com/Public/Home/PressRelease",
    ),
)

MACRO_CALENDAR_DELIVERY_METRIC_IDS: tuple[str, ...] = tuple(
    registration.canonical_metric_id
    for registration in OFFICIAL_METRIC_REGISTRATIONS
    if registration.dataset_id != "flash_services_pmi"
)


OFFICIAL_SOURCE_PROBE_QUERY_IDS = MappingProxyType(
    {
        "CENSUS:MARTS:RETAIL_SALES": "MARTS",
        "CENSUS:ADVM3:DURABLE_GOODS": "ADVM3",
        "CENSUS:RESCONST:HOUSING_STARTS": "RESCONST",
        "CENSUS:RESCONST:BUILDING_PERMITS": "RESCONST",
        "CENSUS:FTD:TRADE_BALANCE": "FTD",
    }
)


LAST_LIVE_BASELINE_PATH = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "baselines"
    / "provider-capability-last-live.json"
)
# Trust anchor for the exact, versioned compact baseline bytes.  It remains
# unset until a strongly verified full LIVE audit is imported.  Updating the
# baseline therefore requires an explicit, reviewable code-pin change instead
# of trusting hashes stored inside the same JSON document.
LAST_LIVE_BASELINE_FILE_SHA256: str | None = "a7f901602494f674da2f4f9f49ebca7dff5e54180b16806e685d96cf8d9aa12e"
# These schemas are part of the central capability registry rather than a
# parallel audit-only allow-list.  They describe the fields that each named
# validator can actually assess.  Registry validation rejects a capability
# that advertises a field outside its validator's contract.
FIELD_VALIDATOR_SCHEMAS = MappingProxyType(
    {
        "validate.ai_earnings_evidence.v1": frozenset(
            {
                "event_date",
                "lineage",
                "publisher",
                "source_url",
                "timing",
            }
        ),
        "validate.ai_event_evidence.v1": frozenset(
            {
                "actual",
                "consensus",
                "consensus_lineage",
                "lineage",
                "occurrence_id",
                "previous",
                "previous_lineage",
                "previous_revised",
                "previous_revised_lineage",
                "reference_period",
                "source",
                "source_url",
            }
        ),
        "validate.ai_news_evidence.v1": frozenset(
            {
                "canonical_url",
                "lineage",
                "published_at",
                "publisher",
                "title",
            }
        ),
        "validate.ai_research_runtime_profile.v1": frozenset(
            {
                "claims",
                *(
                    field_name
                    for profile in PROFILES.values()
                    for field_name in profile.required_fields
                ),
            }
        ),
        "validate.calendar_occurrence.v1": frozenset(
            {
                "actual",
                "consensus",
                "event_at",
                "importance",
                "lineage",
                "name",
                "occurrence_id",
                "previous",
                "previous_revised",
                "reference_period",
                "released_at",
                "source_url",
            }
        ),
        "validate.cboe_positioning.v1": frozenset(
            {
                "data_as_of",
                "lineage",
                "put_call_ratio",
                "settlement",
                "term_structure",
            }
        ),
        "validate.cftc_positioning.v1": frozenset(
            {
                "contract_code",
                "lineage",
                "long",
                "net_position",
                "report_date",
                "short",
            }
        ),
        "validate.constituent_membership.v1": frozenset(
            {
                "data_as_of",
                "lineage",
                "market_cap",
                "name",
                "symbol",
                "weight",
                "weight_as_of",
            }
        ),
        "validate.corporate_index.v1": frozenset(
            {"data_as_of", "lineage", "provider_timestamp", "value"}
        ),
        "validate.current_news.v1": frozenset(
            {
                "canonical_url",
                "lineage",
                "published_at",
                "publisher",
                "symbols",
                "title",
                "topics",
            }
        ),
        "validate.earnings_event.v1": frozenset(
            {
                "eps_estimate",
                "event_at",
                "event_date",
                "lineage",
                "revenue_estimate",
                "symbol",
                "temporal_precision",
                "timing",
            }
        ),
        "validate.flash_services_pmi.v1": frozenset(
            {
                "actual",
                "consensus",
                "lineage",
                "occurrence_id",
                "previous",
                "reference_period",
                "released_at",
            }
        ),
        "validate.fomc_outcome.v1": frozenset(
            {"actual", "lineage", "lower_bound", "released_at", "upper_bound"}
        ),
        "validate.fomc_probabilities.v1": frozenset(
            {
                "data_as_of",
                "lineage",
                "meeting_date",
                "probabilities",
                "target_ranges",
            }
        ),
        "validate.market_internals.v1": frozenset(
            {
                "advance_decline_ratio",
                "advancers",
                "data_as_of",
                "decliners",
                "lineage",
                "percent_advancers",
                "unchanged",
            }
        ),
        "validate.market_quote.v1": frozenset(
            {
                "change",
                "change_pct",
                "data_as_of",
                "last_price",
                "lineage",
                "symbol",
                "volume",
            }
        ),
        "validate.market_schedule.v1": frozenset(
            {
                "close_at",
                "data_as_of",
                "holiday",
                "holiday_date",
                "lineage",
                "maintenance_break",
                "market",
                "name",
                "open_at",
                "session_date",
            }
        ),
        "validate.news_candidate.v1": frozenset(
            {"headline", "lineage", "published_at", "source", "url"}
        ),
        "validate.official_actual.v1": frozenset(
            {
                "actual",
                "frequency",
                "lineage",
                "metric_id",
                "reference_period",
                "released_at",
                "transformation",
                "unit",
            }
        ),
        "validate.official_series.v1": frozenset(
            {
                "content_valid_until",
                "data_as_of",
                "frequency",
                "lineage",
                "observations",
                "released_at",
                "series_id",
                "source",
                "source_url",
                "unit",
                "units",
                "value",
            }
        ),
        "validate.option_chain.v1": frozenset(
            {
                "expiration",
                "implied_volatility",
                "lineage",
                "open_interest",
                "option_type",
                "strike",
                "volume",
            }
        ),
        "validate.option_positioning.v1": frozenset(
            {
                "data_as_of",
                "expirations",
                "iv_atm",
                "lineage",
                "open_interest",
                "skew",
                "strikes",
                "volume",
            }
        ),
        "validate.prediction_market.v1": frozenset(
            {
                "data_as_of",
                "lineage",
                "market_id",
                "probability",
                "question",
                "volume",
            }
        ),
        "validate.repository_cache_entry.v1": frozenset(
            {
                "checksum",
                "created_at",
                "payload",
                "stale_until",
                "status",
                "updated_at",
                "valid_until",
            }
        ),
        "validate.repository_calendar_coverage.v1": frozenset(
            {
                "checked_at",
                "country",
                "provider",
                "status",
                "window_end",
                "window_start",
            }
        ),
        "validate.repository_fed_expectations.v1": frozenset(
            {
                "data_as_of",
                "lineage",
                "meeting_date",
                "probabilities",
                "valid_until",
            }
        ),
        "validate.repository_market_fact.v1": frozenset(
            {
                "data_as_of",
                "fact_key",
                "fact_type",
                "lineage",
                "next_refresh_at",
                "release_at",
                "valid_until",
                "value",
            }
        ),
        "validate.repository_news.v1": frozenset(
            {
                "canonical_url",
                "lineage",
                "published_at",
                "publisher",
                "valid_until",
            }
        ),
        "validate.repository_risk_context.v1": frozenset(
            {
                "data_as_of",
                "lineage",
                "risk_score",
                "skew",
                "valid_until",
                "vvix",
            }
        ),
        "validate.repository_snapshot.v1": frozenset(
            {"checksum", "generated_at", "snapshot_id", "snapshot_revision"}
        ),
        "validate.request_accounting.v1": frozenset(
            {
                "correlation_id",
                "database_lookup",
                "provider_attempts",
                "reason_code",
                "request_id",
                "selected_source",
            }
        ),
        "validate.risk_context.v1": frozenset(
            {
                "data_as_of",
                "lineage",
                "risk_score",
                "risk_sentiment",
                "skew",
                "vvix",
            }
        ),
        "validate.sec_class_shares.v1": frozenset(
            {"class", "filing_at", "lineage", "shares_outstanding", "symbol"}
        ),
        "validate.senior_analyst_payload.v1": frozenset(
            {"analytics", "missing_data", "provider_accounting", "readiness"}
        ),
        "validate.sentiment_survey.v1": frozenset(
            {"bearish", "bullish", "lineage", "neutral", "survey_date"}
        ),
        "validate.social_sentiment.v1": frozenset(
            {"lineage", "published_at", "score", "symbols", "title", "url"}
        ),
    }
)

# Value-shape contracts live beside the validator field registry so an audit
# cannot treat a key as schema-valid merely because it exists.  Every declared
# validator field receives exactly one contract below; `scalar` remains strict
# about containers and booleans while specialized financial/temporal fields
# receive stronger validation in the probe and persisted-artifact validator.
_NUMERIC_VALUE_FIELDS = frozenset(
    {
        "actual",
        "actual_eps",
        "actual_revenue",
        "advance_decline_ratio",
        "advancers",
        "atm_implied_volatility",
        "bearish",
        "bullish",
        "call_skew",
        "call_volume",
        "change",
        "change_pct",
        "change_percent",
        "consensus",
        "coverage_ratio",
        "decliners",
        "dispersion",
        "down_volume",
        "eps_estimate",
        "eps_surprise",
        "equity_put_call_ratio",
        "estimated_gamma_concentration",
        "estimated_gamma_exposure",
        "expected_eps",
        "expected_revenue",
        "forecast",
        "implied_volatility",
        "iv_atm",
        "last_price",
        "leadership_concentration",
        "long",
        "lower_bound",
        "market_cap",
        "net_position",
        "neutral",
        "new_highs",
        "new_lows",
        "open_interest",
        "percent_above_open",
        "percent_above_previous_close",
        "percent_above_vwap",
        "percent_advancers",
        "previous",
        "previous_revised",
        "probability",
        "put_call",
        "put_call_ratio",
        "put_skew",
        "put_volume",
        "qqq_put_call_ratio",
        "revenue_estimate",
        "revenue_surprise",
        "risk_score",
        "score",
        "settlement",
        "shares_outstanding",
        "short",
        "skew",
        "strike",
        "total_put_call_ratio",
        "unchanged",
        "up_down_volume_ratio",
        "up_volume",
        "upper_bound",
        "value",
        "vix",
        "volume",
        "vvix",
        "weight",
    }
)
_TEMPORAL_VALUE_FIELDS = frozenset(
    {
        "as_of",
        "checked_at",
        "close_at",
        "content_valid_until",
        "created_at",
        "data_as_of",
        "decision_at",
        "event_at",
        "event_date",
        "event_end_at",
        "event_start_at",
        "expiration",
        "filing_at",
        "generated_at",
        "holiday_date",
        "meeting_date",
        "next_refresh_at",
        "open_at",
        "provider_timestamp",
        "published_at",
        "reference_period",
        "release_at",
        "released_at",
        "report_date",
        "session_date",
        "stale_until",
        "survey_date",
        "updated_at",
        "valid_until",
        "weight_as_of",
        "window_end",
        "window_start",
    }
)
_SEQUENCE_VALUE_FIELDS = frozenset(
    {
        "claims",
        "constituents",
        "current_news",
        "dominant_expirations",
        "earnings_schedule",
        "expirations",
        "highest_open_interest_strikes",
        "highest_volume_strikes",
        "holdings",
        "missing_data",
        "observations",
        "provider_accounting",
        "provider_attempts",
        "strikes",
        "symbols",
        "target_ranges",
        "topics",
    }
)
_MAPPING_VALUE_FIELDS = frozenset(
    {
        "analytics",
        "current_market_context",
        "database_lookup",
        "maintenance_break",
        "payload",
        "readiness",
    }
)
_MAPPING_OR_SEQUENCE_VALUE_FIELDS = frozenset(
    {
        "consensus_lineage",
        "lineage",
        "previous_lineage",
        "previous_revised_lineage",
        "probabilities",
        "term_structure",
    }
)
_BOOLEAN_VALUE_FIELDS = frozenset(
    {
        "holiday",
        "issuer_announcement",
        "scheduled_event",
        "verified_corporate_metric",
        "verified_market_metric",
    }
)


def _field_value_contract(field_name: str) -> str:
    if field_name in _NUMERIC_VALUE_FIELDS:
        return "number"
    if field_name in _TEMPORAL_VALUE_FIELDS:
        return "temporal"
    if field_name in _SEQUENCE_VALUE_FIELDS:
        return "sequence"
    if field_name in _MAPPING_VALUE_FIELDS:
        return "mapping"
    if field_name in _MAPPING_OR_SEQUENCE_VALUE_FIELDS:
        return "mapping_or_sequence"
    if field_name in _BOOLEAN_VALUE_FIELDS:
        return "boolean"
    return "scalar"


FIELD_VALUE_TYPE_CONTRACTS = MappingProxyType(
    {
        validator_id: MappingProxyType(
            {
                field_name: _field_value_contract(field_name)
                for field_name in sorted(fields)
            }
        )
        for validator_id, fields in FIELD_VALIDATOR_SCHEMAS.items()
    }
)


def field_value_type_contract(
    validator_id: str,
    field_name: str,
) -> str | None:
    """Return the central value-shape contract for one validator field."""

    contracts = FIELD_VALUE_TYPE_CONTRACTS.get(validator_id)
    return contracts.get(field_name) if contracts is not None else None


def official_metric_semantic_contract(
    metric_id: str,
    canonical_metric_ids: Iterable[str] = (),
) -> MappingProxyType:
    """Derive official unit/frequency facts for a registered capability."""

    metric_ids = {
        str(item).strip()
        for item in (metric_id, *canonical_metric_ids)
        if str(item).strip()
    }
    matches = tuple(
        registration
        for registration in OFFICIAL_METRIC_REGISTRATIONS
        if registration.canonical_metric_id in metric_ids
    )
    units = {item.unit for item in matches}
    frequencies = {item.frequency for item in matches}
    transformations = {item.transformation for item in matches}
    return MappingProxyType(
        {
            "canonical_metric_ids": tuple(
                sorted(item.canonical_metric_id for item in matches)
            ),
            "unit": next(iter(units)) if len(units) == 1 else None,
            "frequency": (
                next(iter(frequencies)) if len(frequencies) == 1 else None
            ),
            "transformation": (
                next(iter(transformations))
                if len(transformations) == 1
                else None
            ),
        }
    )


KNOWN_NUMERIC_MEASUREMENT_UNITS = frozenset(
    {
        "contracts",
        "count",
        "currency_per_share",
        "currency_usd",
        "dimensionless",
        "index_points",
        "millions_usd",
        "percent",
        "percentage_points",
        "probability_fraction",
        "ratio",
        "shares",
        "thousands_annual_rate",
        "thousands_of_jobs",
    }
)


def _capability_value(capability: Any, name: str, default: Any = None) -> Any:
    if isinstance(capability, dict):
        return capability.get(name, default)
    return getattr(capability, name, default)


def capability_measurement_contract(capability: Any) -> str:
    """Return one explicit measurement contract for every capability.

    `mixed_numeric` means the capability intentionally contains multiple known
    numeric units (for example price, percent change and volume). It still
    requires a recognized structured unit in each numeric field observation;
    arbitrary labels never satisfy the contract.
    """

    metric_id = str(_capability_value(capability, "metric_id", "") or "")
    validator_id = str(
        _capability_value(capability, "field_validator_id", "") or ""
    )
    fields = tuple(
        str(item)
        for item in (
            _capability_value(capability, "supported_fields", ()) or ()
        )
    )
    canonical_metric_ids = tuple(
        str(item)
        for item in (
            _capability_value(capability, "canonical_metric_ids", ()) or ()
        )
    )
    official = official_metric_semantic_contract(
        metric_id,
        canonical_metric_ids,
    )
    if official.get("unit"):
        return re.sub(
            r"[^a-z0-9]+",
            "_",
            str(official["unit"]).strip().casefold(),
        ).strip("_")

    numeric_fields = {
        field_name
        for field_name in fields
        if field_value_type_contract(validator_id, field_name) == "number"
    }
    if not numeric_fields:
        return "mixed_structured"

    normalized_metric = metric_id.upper()
    if any(
        token in normalized_metric
        for token in (
            "DGS",
            "DFF",
            "DFEDTAR",
            "FEDFUNDS",
            "SOFR",
            "T10Y",
            "YIELD",
        )
    ):
        return "percent"
    if any(
        token in normalized_metric
        for token in ("VIX", "VVIX", "SKEW", "NFCI", "PMI")
    ):
        return "index_points"
    if normalized_metric == "ICSA":
        return "count"
    if "PROBABIL" in normalized_metric:
        return "probability_fraction"
    if "PUT_CALL" in normalized_metric:
        return "ratio"
    if normalized_metric in {
        "PRICE_CHANGE_WEIGHT",
        "ADVANCE_DECLINE_RATIO",
        "EARNINGS_EVENT",
        "OPTION_CHAIN",
        "OPTION_POSITIONING",
    } or len(numeric_fields) > 1:
        return "mixed_numeric"
    return "dimensionless"


def capability_field_measurement_contract(
    capability: Any,
    field_name: str,
) -> str:
    """Return the measurement contract bound to one capability field."""

    validator_id = str(
        _capability_value(capability, "field_validator_id", "") or ""
    )
    shape = field_value_type_contract(validator_id, field_name)
    field_measurements = {
        "advancers": "count",
        "actual_eps": "currency_per_share",
        "actual_revenue": "currency_usd",
        "advance_decline_ratio": "ratio",
        "atm_implied_volatility": "percent",
        "bearish": "percent",
        "bullish": "percent",
        "call_skew": "dimensionless",
        "call_volume": "contracts",
        "change": "currency_usd",
        "change_pct": "percent",
        "change_percent": "percent",
        "coverage_ratio": "ratio",
        "decliners": "count",
        "dispersion": "ratio",
        "down_volume": "shares",
        "eps_estimate": "currency_per_share",
        "eps_surprise": "currency_per_share",
        "equity_put_call_ratio": "ratio",
        "estimated_gamma_concentration": "dimensionless",
        "estimated_gamma_exposure": "dimensionless",
        "expected_eps": "currency_per_share",
        "expected_revenue": "currency_usd",
        "implied_volatility": "percent",
        "iv_atm": "percent",
        "last_price": "currency_usd",
        "leadership_concentration": "ratio",
        "market_cap": "currency_usd",
        "net_position": "contracts",
        "neutral": "percent",
        "new_highs": "count",
        "new_lows": "count",
        "open_interest": "contracts",
        "percent_above_open": "percent",
        "percent_above_previous_close": "percent",
        "percent_above_vwap": "percent",
        "percent_advancers": "percent",
        "probability": "probability_fraction",
        "put_call_ratio": "ratio",
        "put_skew": "dimensionless",
        "qqq_put_call_ratio": "ratio",
        "put_volume": "contracts",
        "revenue_estimate": "currency_usd",
        "revenue_surprise": "currency_usd",
        "risk_score": "dimensionless",
        "score": "dimensionless",
        "shares_outstanding": "shares",
        "short": "contracts",
        "skew": "index_points",
        "strike": "currency_usd",
        "total_put_call_ratio": "ratio",
        "unchanged": "count",
        "up_down_volume_ratio": "ratio",
        "up_volume": "shares",
        "vix": "index_points",
        "volume": "shares",
        "vvix": "index_points",
        "weight": "percent",
        "long": "contracts",
    }
    if shape == "number" and field_name in field_measurements:
        return field_measurements[field_name]
    if shape == "number" or field_name in {"unit", "units"}:
        return capability_measurement_contract(capability)
    return {
        "temporal": "temporal",
        "boolean": "categorical",
        "mapping": "mixed_structured",
        "sequence": "mixed_structured",
        "mapping_or_sequence": "mixed_structured",
        "scalar": "categorical",
    }.get(str(shape), "mixed_structured")


def _capability(
    dataset_id: str,
    metric_id: str,
    supported_fields: Iterable[str],
    frequency: str,
    *,
    transformation: str = "identity",
    probe_id: str,
    field_validator_id: str,
    probe_adapter_path: str | None = None,
    ai_eligible: bool = False,
    request_group: str | None = None,
    canonical_metric_ids: Iterable[str] = (),
    delivery_capability_id: str | None = None,
    delivery_capability_ids: Iterable[str] = (),
    delivery_field_map: Iterable[tuple[str, str]] = (),
    probe_query_id: str | None = None,
    degradable_quality_checks: Iterable[str] = (),
    runtime_profile_id: str | None = None,
    runtime_job_type: str | None = None,
    runtime_topic: str | None = None,
    runtime_enable_setting: str | None = None,
    runtime_required_fields: Iterable[str] = (),
    runtime_source_domains: Iterable[str] = (),
    audit_only_fields: Iterable[str] = (),
) -> CapabilityRegistration:
    if probe_adapter_path is None and probe_id.startswith(
        ("probe.repository.", "probe.transform.")
    ):
        probe_adapter_path = (
            "app.services.provider_capability_probe_hooks:"
            "LocalCapabilityProbeHook"
        )
    return CapabilityRegistration(
        dataset_id=dataset_id,
        metric_id=metric_id,
        supported_fields=tuple(supported_fields),
        frequency=frequency,
        transformation=transformation,
        probe_id=probe_id,
        field_validator_id=field_validator_id,
        probe_adapter_path=probe_adapter_path,
        ai_eligible=ai_eligible,
        request_group=request_group,
        canonical_metric_ids=tuple(canonical_metric_ids),
        delivery_capability_id=delivery_capability_id,
        delivery_capability_ids=tuple(delivery_capability_ids),
        delivery_field_map=tuple(delivery_field_map),
        probe_query_id=probe_query_id,
        degradable_quality_checks=tuple(
            degradable_quality_checks
        ),
        runtime_profile_id=runtime_profile_id,
        runtime_job_type=runtime_job_type,
        runtime_topic=runtime_topic,
        runtime_enable_setting=runtime_enable_setting,
        runtime_required_fields=tuple(runtime_required_fields),
        runtime_source_domains=tuple(runtime_source_domains),
        audit_only_fields=tuple(audit_only_fields),
    )


def _provider(
    provider_id: str,
    adapter_path: str,
    provider_type: str,
    capabilities: Iterable[CapabilityRegistration],
    roles: Iterable[str],
    *,
    credentials: Iterable[str] = (),
    timeout: str | float = "http_timeout_seconds",
    max_attempts: int = 1,
    retry_policy: str = "NONE",
    known_rate_limit: str = "UNKNOWN",
    probe_id: str,
    enable_setting: str | None = None,
    runtime_adapter: bool = False,
    additional_adapter_paths: Iterable[str] = (),
    uncertified_runtime_leaves: Iterable[str] = (),
    publisher: str | None = None,
    distributor: str | None = None,
    acquisition_provider: str | None = None,
    source_domains: Iterable[str] = (),
    request_group: str | None = None,
    capture_mode: str | None = None,
    audit_leaf_request_count: int | None = None,
    audit_source_url_budget: int = 0,
    configuration_setting: str | None = None,
    configuration_value: str | None = None,
    terminal_audit_reason: str | None = None,
) -> ProviderRegistration:
    atomic_capabilities: list[CapabilityRegistration] = []
    for capability in capabilities:
        metric_ids = tuple(
            item.strip()
            for item in capability.metric_id.split(",")
            if item.strip()
        )
        if len(metric_ids) <= 1:
            atomic_capabilities.append(capability)
            continue
        request_group_id = capability.request_group or (
            f"{provider_id.casefold()}."
            f"{capability.probe_id.replace('probe.', '').replace('.', '_')}."
            f"{capability.dataset_id}.batch"
        )
        atomic_capabilities.extend(
            replace(
                capability,
                metric_id=metric_id,
                request_group=request_group_id,
            )
            for metric_id in metric_ids
        )
    return ProviderRegistration(
        provider_id=provider_id,
        adapter_path=adapter_path,
        provider_type=provider_type,
        capabilities=tuple(atomic_capabilities),
        allowed_roles=tuple(roles),
        credential_requirements=tuple(credentials),
        timeout=timeout,
        max_attempts=max_attempts,
        retry_policy=retry_policy,
        known_rate_limit=known_rate_limit,
        probe_id=probe_id,
        enable_setting=enable_setting,
        runtime_adapter=runtime_adapter,
        additional_adapter_paths=tuple(additional_adapter_paths),
        uncertified_runtime_leaves=tuple(
            uncertified_runtime_leaves
        ),
        publisher=publisher,
        distributor=distributor,
        acquisition_provider=acquisition_provider,
        source_domains=tuple(source_domains),
        request_group=request_group,
        capture_mode=capture_mode
        or (
            "SUBPROCESS"
            if provider_id == "AI_RESEARCHER"
            else (
                "LOCAL_SANDBOX"
                if provider_type
                in {
                    "MANUAL_FILE",
                    "RECONCILIATION",
                    "REPOSITORY",
                    "TRANSFORMATION",
                }
                else "HTTPX"
            )
        ),
        audit_leaf_request_count=audit_leaf_request_count,
        audit_source_url_budget=audit_source_url_budget,
        configuration_setting=configuration_setting,
        configuration_value=configuration_value,
        terminal_audit_reason=terminal_audit_reason,
    )


MARKET_FACT_REPOSITORY_DATASET_QUERIES = MappingProxyType(
    {
        "nasdaq_100": MarketFactRepositoryDatasetQuery(
            "intraday",
            (
                "qqq_holdings",
                "nasdaq_context",
                "nasdaq_100_constituents",
            ),
        ),
        "mega_cap_quotes": MarketFactRepositoryDatasetQuery(
            "intraday",
            ("mega_cap_snapshot", "mega_cap_breadth"),
        ),
        "market_internals": MarketFactRepositoryDatasetQuery(
            "intraday",
            ("deterministic_market_internals",),
        ),
        "vix": MarketFactRepositoryDatasetQuery(
            "daily",
            ("official_macro_latest",),
            ("VIXCLS",),
        ),
        "treasury_rates": MarketFactRepositoryDatasetQuery(
            "daily",
            ("official_macro_latest",),
            ("DGS2", "DGS10", "DGS30", "T10Y2Y", "T10Y3M", "NFCI"),
        ),
        "fed_funds": MarketFactRepositoryDatasetQuery(
            "daily",
            ("official_macro_latest",),
            ("DFF", "FEDFUNDS", "SOFR"),
        ),
        "target_range": MarketFactRepositoryDatasetQuery(
            "event",
            ("official_macro_latest",),
            ("DFEDTARL", "DFEDTARU"),
        ),
        "cpi": MarketFactRepositoryDatasetQuery(
            "monthly",
            ("official_macro_latest",),
            ("CUSR0000SA0", "CUSR0000SA0L1E"),
        ),
        "ppi": MarketFactRepositoryDatasetQuery(
            "monthly",
            ("official_macro_latest",),
            ("WPUFD4",),
        ),
        "pce": MarketFactRepositoryDatasetQuery(
            "monthly",
            ("official_macro_latest",),
            ("BEA:PCE", "BEA:PCE_PRICE_INDEX", "BEA:CORE_PCE"),
        ),
        "gdp": MarketFactRepositoryDatasetQuery(
            "quarterly",
            ("official_macro_latest",),
            ("BEA:GDP", "GDP"),
        ),
        "employment": MarketFactRepositoryDatasetQuery(
            "monthly",
            ("official_macro_latest",),
            ("LNS14000000", "UNRATE"),
        ),
        "wages": MarketFactRepositoryDatasetQuery(
            "monthly",
            ("official_macro_latest",),
            ("CES0500000003",),
        ),
        "nfp": MarketFactRepositoryDatasetQuery(
            "monthly",
            ("official_macro_latest",),
            ("CES0000000001", "PAYEMS"),
        ),
        "jobless_claims": MarketFactRepositoryDatasetQuery(
            "weekly",
            ("official_macro_latest",),
            ("ICSA",),
        ),
        "earnings": MarketFactRepositoryDatasetQuery(
            "event",
            ("earnings_event", "fmp_earnings_calendar"),
        ),
        "options_positioning": MarketFactRepositoryDatasetQuery(
            "intraday",
            ("deterministic_options_positioning",),
        ),
        "positioning": MarketFactRepositoryDatasetQuery(
            "weekly",
            ("cot_positioning",),
        ),
        "market_schedule": MarketFactRepositoryDatasetQuery(
            "event",
            (
                "nasdaq_market_info",
                "cme_market_schedule",
                "investing_holidays",
                "marketbeat_holidays",
            ),
        ),
    }
)


RESEARCH_BACKEND_PROFILE_IDS: tuple[str, ...] = tuple(PROFILES)
RESEARCH_RUNTIME_SOURCE_DOMAINS: tuple[str, ...] = tuple(
    sorted(
        {
            domain
            for profile in PROFILES.values()
            for domain in profile.priority_domains
        }
    )
)
RESEARCH_BACKEND_RUNTIME_ADAPTER_PATHS = MappingProxyType(
    {
        "codex_cli": (
            "app.services.ai_research_job_executor:"
            "PersistentAIJobExecutor"
        ),
        "openai_api": (
            "app.services.research_backend:"
            "OpenAIResponsesResearchBackend"
        ),
    }
)
RESEARCH_RUNTIME_COMPONENT_PATHS: tuple[str, ...] = (
    "app.services.research_source_gateway:ResearchSourceGateway",
    (
        "app.services.evidence_verification_service:"
        "DeterministicEvidenceVerifier"
    ),
    "app.services.agentic_research_runtime:AgenticResearchRuntime",
    "app.services.ai_research_worker:AIResearchWorker",
)
_RESEARCH_AGENT_BY_PROFILE = {
    registration.profile_id: registration
    for registration in RESEARCH_AGENT_REGISTRY
}


def _research_backend_capabilities(
    *,
    probe_id: str,
) -> tuple[CapabilityRegistration, ...]:
    """Expose every worker profile as a separately auditable capability.

    ``AIResearcherProvider`` is a legacy route-enrichment adapter.  These
    rows describe the distinct backend selected by ``AIResearchWorker`` and
    must not be collapsed into that provider-level row.
    """

    capabilities: list[CapabilityRegistration] = []
    for profile_id, profile in PROFILES.items():
        agent = _RESEARCH_AGENT_BY_PROFILE.get(profile_id)
        job_types = tuple(
            job_type
            for job_type, mapped_profile_id in JOB_PROFILE.items()
            if mapped_profile_id == profile_id
            and job_type != "RELEASE_ACTUAL_REFRESH"
        )
        if len(job_types) != 1:
            raise RuntimeError(
                "RESEARCH_PROFILE_AUDIT_JOB_TYPE_AMBIGUOUS:"
                f"{profile_id}:{','.join(job_types)}"
            )
        capabilities.append(
            _capability(
                "ai_research_runtime",
                profile_id.casefold(),
                profile.required_fields or ("claims",),
                "request",
                transformation="identity",
                probe_id=probe_id,
                field_validator_id=(
                    "validate.ai_research_runtime_profile.v1"
                ),
                ai_eligible=False,
                runtime_profile_id=profile_id,
                runtime_job_type=job_types[0],
                runtime_topic=(agent.topic if agent is not None else None),
                runtime_enable_setting=(
                    agent.settings_field
                    if agent is not None
                    else "research_agents_enabled"
                ),
                runtime_required_fields=profile.required_fields,
                runtime_source_domains=profile.priority_domains,
            )
        )
    return tuple(capabilities)


PROVIDER_REGISTRY: tuple[ProviderRegistration, ...] = (
    _provider(
        "INVESCO",
        "app.providers.qqq_holdings_provider:QQQHoldingsProvider",
        "DISTRIBUTOR",
        (
            _capability(
                "nasdaq_100",
                "constituent_membership",
                ("symbol", "name", "weight", "weight_as_of", "lineage"),
                "intraday",
                probe_id="probe.invesco.qqq_holdings",
                field_validator_id="validate.constituent_membership.v1",
            ),
        ),
        ("PRIMARY",),
        probe_id="probe.invesco.qqq_holdings",
        runtime_adapter=True,
        publisher="Invesco",
        distributor="Invesco",
        acquisition_provider="INVESCO",
    ),
    _provider(
        "ALPHA_VANTAGE",
        "app.providers.qqq_holdings_provider:QQQHoldingsProvider",
        "STRUCTURED_VENDOR",
        (
            _capability(
                "nasdaq_100",
                "constituent_membership",
                ("symbol", "name", "weight", "weight_as_of", "lineage"),
                "intraday",
                probe_id="probe.alpha_vantage.etf_profile",
                field_validator_id="validate.constituent_membership.v1",
            ),
            _capability(
                "mega_cap_quotes",
                "price_change_weight",
                (
                    "symbol",
                    "last_price",
                    "change",
                    "change_pct",
                    "volume",
                    "data_as_of",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.alpha_vantage.global_quote",
                field_validator_id="validate.market_quote.v1",
                probe_adapter_path=(
                    "app.providers.mega_cap_snapshot_provider:"
                    "MegaCapSnapshotProvider"
                ),
            ),
        ),
        ("FALLBACK",),
        credentials=("alpha_vantage_api_key",),
        probe_id="probe.alpha_vantage.etf_profile",
        runtime_adapter=True,
        publisher="Alpha Vantage",
        distributor="Alpha Vantage",
        acquisition_provider="ALPHA_VANTAGE",
    ),
    _provider(
        "NASDAQ",
        "app.providers.nasdaq_100_constituents_provider:Nasdaq100ConstituentsProvider",
        "OFFICIAL_EXCHANGE",
        (
            _capability(
                "nasdaq_100",
                "constituent_membership",
                ("symbol", "name", "market_cap", "data_as_of", "lineage"),
                "intraday",
                probe_id="probe.nasdaq.constituents",
                field_validator_id="validate.constituent_membership.v1",
            ),
            _capability(
                "earnings",
                "earnings_event",
                (
                    "symbol",
                    "event_date",
                    "event_at",
                    "timing",
                    "eps_estimate",
                    "revenue_estimate",
                    "temporal_precision",
                    "lineage",
                ),
                "event",
                probe_id="probe.nasdaq.earnings",
                field_validator_id="validate.earnings_event.v1",
                probe_adapter_path=(
                    "app.providers.nasdaq_earnings_provider:"
                    "NasdaqEarningsProvider"
                ),
            ),
        ),
        ("PRIMARY", "FALLBACK"),
        timeout="timeout_nasdaq_seconds",
        probe_id="probe.nasdaq.constituents",
        enable_setting="enable_nasdaq_100",
        runtime_adapter=True,
        additional_adapter_paths=(
            "app.providers.nasdaq_earnings_provider:NasdaqEarningsProvider",
        ),
        publisher="Nasdaq",
        distributor="Nasdaq",
        acquisition_provider="NASDAQ",
        source_domains=("nasdaq.com",),
    ),
    _provider(
        "SEC",
        "app.providers.sec_class_shares_provider:SecClassSharesProvider",
        "OFFICIAL_GOVERNMENT",
        (
            _capability(
                "nasdaq_100",
                "class_adjusted_shares",
                ("symbol", "class", "shares_outstanding", "filing_at", "lineage"),
                "event",
                probe_id="probe.sec.class_shares",
                field_validator_id="validate.sec_class_shares.v1",
            ),
        ),
        ("FALLBACK",),
        probe_id="probe.sec.class_shares",
        runtime_adapter=True,
        publisher="U.S. Securities and Exchange Commission",
        distributor="SEC EDGAR",
        acquisition_provider="SEC",
    ),
    _provider(
        "YAHOO_FINANCE_CHART",
        "app.providers.mega_cap_snapshot_provider:MegaCapSnapshotProvider",
        "DISTRIBUTOR",
        (
            _capability(
                "mega_cap_quotes",
                "price_change_weight",
                (
                    "symbol",
                    "last_price",
                    "change",
                    "change_pct",
                    "volume",
                    "data_as_of",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.yahoo_finance.chart",
                field_validator_id="validate.market_quote.v1",
            ),
        ),
        ("PRIMARY",),
        probe_id="probe.yahoo_finance.chart",
        runtime_adapter=True,
        publisher="Exchange-reported market data",
        distributor="Yahoo Finance",
        acquisition_provider="YAHOO_FINANCE_CHART",
    ),
    _provider(
        "STOOQ",
        "app.providers.mega_cap_snapshot_provider:MegaCapSnapshotProvider",
        "DISTRIBUTOR",
        (
            _capability(
                "mega_cap_quotes",
                "price_change_weight",
                (
                    "symbol",
                    "last_price",
                    "change",
                    "change_pct",
                    "volume",
                    "data_as_of",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.stooq.quote_csv",
                field_validator_id="validate.market_quote.v1",
            ),
        ),
        ("FALLBACK",),
        probe_id="probe.stooq.quote_csv",
        runtime_adapter=True,
        publisher="Exchange-reported market data",
        distributor="Stooq",
        acquisition_provider="STOOQ",
    ),
    _provider(
        "YAHOO_FINANCE_QUOTE",
        "app.providers.mega_cap_snapshot_provider:MegaCapSnapshotProvider",
        "DISTRIBUTOR",
        (
            _capability(
                "mega_cap_quotes",
                "price_change_weight",
                (
                    "symbol",
                    "last_price",
                    "change",
                    "change_pct",
                    "volume",
                    "data_as_of",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.yahoo_finance.quote",
                field_validator_id="validate.market_quote.v1",
            ),
        ),
        ("FALLBACK",),
        probe_id="probe.yahoo_finance.quote",
        runtime_adapter=True,
        publisher="Exchange-reported market data",
        distributor="Yahoo Finance",
        acquisition_provider="YAHOO_FINANCE_QUOTE",
    ),
    _provider(
        "TRADIER",
        "app.providers.tradier:TradierProvider",
        "LICENSED_MARKET_DATA",
        (
            _capability(
                "market_internals",
                "advance_decline_ratio",
                (
                    "advancers",
                    "decliners",
                    "unchanged",
                    "advance_decline_ratio",
                    "percent_advancers",
                    "data_as_of",
                    "lineage",
                ),
                "intraday",
                transformation="deterministic_constituent_breadth",
                probe_id="probe.tradier.quotes",
                field_validator_id="validate.market_internals.v1",
            ),
            _capability(
                "options_positioning",
                "qqq_option_positioning",
                (
                    "iv_atm",
                    "open_interest",
                    "volume",
                    "skew",
                    "expirations",
                    "strikes",
                    "data_as_of",
                    "lineage",
                ),
                "intraday",
                transformation="deterministic_option_aggregation",
                probe_id="probe.tradier.option_chain",
                field_validator_id="validate.option_positioning.v1",
            ),
        ),
        ("PRIMARY",),
        credentials=("tradier_production_token|tradier_sandbox_token",),
        timeout="tradier_timeout_seconds",
        max_attempts=3,
        retry_policy="BOUNDED_HTTP_RETRY",
        known_rate_limit="UNKNOWN",
        probe_id="probe.tradier.quotes",
        enable_setting="tradier_enabled",
        runtime_adapter=True,
        publisher="Tradier",
        distributor="Tradier",
        acquisition_provider="TRADIER",
    ),
    _provider(
        "FRED",
        "app.providers.fred:FredProvider",
        "OFFICIAL_REDISTRIBUTOR",
        (
            _capability(
                "vix",
                "VIXCLS",
                ("value", "data_as_of", "content_valid_until", "lineage"),
                "daily",
                probe_id="probe.fred.series",
                field_validator_id="validate.official_series.v1",
                delivery_capability_id="vix",
                delivery_field_map=(
                    ("value", "value"),
                    ("data_as_of", "data_as_of"),
                    ("lineage", "lineage"),
                ),
            ),
            _capability(
                "treasury_rates",
                "DGS2,DGS10,DGS30,T10Y2Y,T10Y3M",
                ("value", "unit", "data_as_of", "content_valid_until", "lineage"),
                "daily",
                probe_id="probe.fred.series",
                field_validator_id="validate.official_series.v1",
            ),
            _capability(
                "treasury_rates",
                "NFCI",
                ("value", "unit", "data_as_of", "content_valid_until", "lineage"),
                "weekly",
                probe_id="probe.fred.series",
                field_validator_id="validate.official_series.v1",
                request_group="fred.fred_series.treasury_rates.batch",
            ),
            _capability(
                "fed_funds",
                "DFF,SOFR",
                ("value", "unit", "data_as_of", "content_valid_until", "lineage"),
                "daily",
                probe_id="probe.fred.series",
                field_validator_id="validate.official_series.v1",
            ),
            _capability(
                "fed_funds",
                "FEDFUNDS",
                ("value", "unit", "data_as_of", "content_valid_until", "lineage"),
                "monthly",
                probe_id="probe.fred.series",
                field_validator_id="validate.official_series.v1",
                request_group="fred.fred_series.fed_funds.batch",
            ),
            _capability(
                "target_range",
                "DFEDTARL,DFEDTARU",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "daily",
                probe_id="probe.fred.series",
                field_validator_id="validate.official_series.v1",
            ),
            _capability(
                "jobless_claims",
                "ICSA",
                ("value", "unit", "data_as_of", "released_at", "lineage"),
                "weekly",
                probe_id="probe.fred.series",
                field_validator_id="validate.official_series.v1",
            ),
            _capability(
                "macro_calendar",
                "HSN1F",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "monthly",
                probe_id="probe.fred.series",
                field_validator_id="validate.official_series.v1",
                canonical_metric_ids=("new_home_sales",),
            ),
        ),
        ("PRIMARY", "FALLBACK"),
        credentials=("fred_api_key",),
        timeout="fred_timeout_seconds",
        max_attempts=3,
        retry_policy="BOUNDED_HTTP_RETRY",
        probe_id="probe.fred.series",
        enable_setting="fred_enabled",
        runtime_adapter=True,
        publisher="Federal Reserve Bank of St. Louis",
        distributor="FRED",
        acquisition_provider="FRED",
        source_domains=("fred.stlouisfed.org",),
    ),
    _provider(
        "CBOE",
        "app.providers.cboe_risk_indices_provider:CboeRiskIndicesProvider",
        "OFFICIAL_EXCHANGE",
        (
            _capability(
                "vix",
                "VIX",
                ("value", "provider_timestamp", "data_as_of", "lineage"),
                "daily",
                probe_id="probe.cboe.risk_indices",
                field_validator_id="validate.corporate_index.v1",
                request_group="cboe.risk_indices.batch",
                delivery_capability_id="vix",
                delivery_field_map=(
                    ("value", "value"),
                    ("data_as_of", "data_as_of"),
                    ("lineage", "lineage"),
                ),
            ),
            _capability(
                "vvix",
                "VVIX",
                ("value", "provider_timestamp", "data_as_of", "lineage"),
                "intraday",
                probe_id="probe.cboe.risk_indices",
                field_validator_id="validate.corporate_index.v1",
                request_group="cboe.risk_indices.batch",
            ),
            _capability(
                "risk",
                "SKEW,risk_sentiment,risk_score",
                (
                    "skew",
                    "vvix",
                    "risk_sentiment",
                    "risk_score",
                    "data_as_of",
                    "lineage",
                ),
                "intraday",
                transformation="deterministic_risk_composite",
                probe_id="probe.cboe.risk_indices",
                field_validator_id="validate.risk_context.v1",
                request_group="cboe.risk_indices.batch",
            ),
            _capability(
                "options_positioning",
                "put_call",
                (
                    "put_call_ratio",
                    "data_as_of",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.cboe.put_call",
                field_validator_id="validate.cboe_positioning.v1",
                probe_adapter_path=(
                    "app.providers.cboe_put_call_provider:"
                    "CboePutCallProvider"
                ),
            ),
            _capability(
                "options_positioning",
                "vix_futures",
                (
                    "term_structure",
                    "settlement",
                    "data_as_of",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.cboe.vix_futures",
                field_validator_id="validate.cboe_positioning.v1",
                probe_adapter_path=(
                    "app.providers.cboe_vix_futures_provider:"
                    "CboeVixFuturesProvider"
                ),
            ),
        ),
        ("PRIMARY", "FALLBACK"),
        timeout="http_timeout_seconds",
        probe_id="probe.cboe.delayed_indices",
        enable_setting="enable_cboe_risk_indices",
        runtime_adapter=True,
        additional_adapter_paths=(
            "app.providers.cboe_put_call_provider:CboePutCallProvider",
            "app.providers.cboe_vix_futures_provider:CboeVixFuturesProvider",
        ),
        publisher="Cboe Global Markets",
        distributor="Cboe",
        acquisition_provider="CBOE",
        audit_leaf_request_count=23,
    ),
    _provider(
        "FEDERAL_RESERVE",
        "app.providers.fed_calendar:FederalReserveCalendarProvider",
        "OFFICIAL_GOVERNMENT",
        (
            _capability(
                "macro_calendar",
                "fomc_occurrence",
                ("event_at", "name", "source_url", "lineage"),
                "event",
                probe_id="probe.federal_reserve.calendar",
                field_validator_id="validate.calendar_occurrence.v1",
            ),
        ),
        ("PRIMARY",),
        probe_id="probe.federal_reserve.calendar",
        runtime_adapter=True,
        additional_adapter_paths=(
            "app.providers.federal_reserve:FederalReserveRssProvider",
        ),
        publisher="Board of Governors of the Federal Reserve System",
        distributor="Federal Reserve",
        acquisition_provider="FEDERAL_RESERVE",
    ),
    _provider(
        "INVESTING_FED_RATE_MONITOR",
        "app.providers.investing_fed_rate_monitor_provider:InvestingFedRateMonitorProvider",
        "AGGREGATOR",
        (
            _capability(
                "fomc_expectations",
                "pre_meeting_probabilities",
                (
                    "meeting_date",
                    "target_ranges",
                    "probabilities",
                    "data_as_of",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.investing.fed_rate_monitor",
                field_validator_id="validate.fomc_probabilities.v1",
            ),
        ),
        ("PRIMARY",),
        timeout="investing_fed_rate_monitor_timeout_seconds",
        probe_id="probe.investing.fed_rate_monitor",
        enable_setting="enable_investing_fed_rate_monitor",
        runtime_adapter=True,
        publisher="CME FedWatch inputs / market participants",
        distributor="Investing.com",
        acquisition_provider="INVESTING_FED_RATE_MONITOR",
    ),
    _provider(
        "BLS",
        "app.providers.bls:BlsProvider",
        "OFFICIAL_GOVERNMENT",
        (
            _capability(
                "cpi",
                "CUSR0000SA0",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "monthly",
                probe_id="probe.bls.series",
                field_validator_id="validate.official_series.v1",
                request_group="bls.series.batch",
                canonical_metric_ids=("headline_cpi_mom",),
            ),
            _capability(
                "cpi",
                "CUUR0000SA0",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "monthly",
                probe_id="probe.bls.series",
                field_validator_id="validate.official_series.v1",
                request_group="bls.series.batch",
                canonical_metric_ids=("headline_cpi_yoy",),
            ),
            _capability(
                "cpi",
                "CUSR0000SA0L1E",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "monthly",
                probe_id="probe.bls.series",
                field_validator_id="validate.official_series.v1",
                request_group="bls.series.batch",
                canonical_metric_ids=("core_cpi_mom",),
            ),
            _capability(
                "cpi",
                "CUUR0000SA0L1E",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "monthly",
                probe_id="probe.bls.series",
                field_validator_id="validate.official_series.v1",
                request_group="bls.series.batch",
                canonical_metric_ids=("core_cpi_yoy",),
            ),
            _capability(
                "ppi",
                "WPSFD4",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "monthly",
                probe_id="probe.bls.series",
                field_validator_id="validate.official_series.v1",
                request_group="bls.series.batch",
                canonical_metric_ids=("headline_ppi_mom",),
            ),
            _capability(
                "ppi",
                "WPUFD4",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "monthly",
                probe_id="probe.bls.series",
                field_validator_id="validate.official_series.v1",
                request_group="bls.series.batch",
                canonical_metric_ids=("headline_ppi_yoy",),
            ),
            _capability(
                "nfp",
                "CES0000000001",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "monthly",
                probe_id="probe.bls.series",
                field_validator_id="validate.official_series.v1",
                request_group="bls.series.batch",
                canonical_metric_ids=("nonfarm_payrolls_change",),
            ),
            _capability(
                "employment",
                "LNS14000000",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "monthly",
                probe_id="probe.bls.series",
                field_validator_id="validate.official_series.v1",
                request_group="bls.series.batch",
                canonical_metric_ids=("unemployment_rate",),
            ),
            _capability(
                "wages",
                "CES0500000003",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "monthly",
                probe_id="probe.bls.series",
                field_validator_id="validate.official_series.v1",
                request_group="bls.series.batch",
                canonical_metric_ids=(
                    "average_hourly_earnings_mom",
                    "average_hourly_earnings_yoy",
                ),
            ),
            _capability(
                "wages",
                "CIU1010000000000A",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "quarterly",
                probe_id="probe.bls.series",
                field_validator_id="validate.official_series.v1",
                request_group="bls.series.batch",
                canonical_metric_ids=("employment_cost_index_qoq",),
            ),
            _capability(
                "macro_calendar",
                "bls_release_occurrence",
                (
                    "event_at",
                    "actual",
                    "reference_period",
                    "released_at",
                    "lineage",
                ),
                "event",
                probe_id="probe.bls.release_calendar",
                field_validator_id="validate.calendar_occurrence.v1",
                probe_adapter_path=(
                    "app.providers.bls_calendar:BlsReleaseCalendarProvider"
                ),
            ),
        ),
        ("PRIMARY",),
        credentials=("bls_api_key (optional public-limit registration)",),
        timeout="bls_timeout_seconds",
        max_attempts=3,
        retry_policy="BOUNDED_HTTP_RETRY; FRED transport fallback declared separately",
        probe_id="probe.bls.series",
        enable_setting="bls_enabled",
        runtime_adapter=True,
        additional_adapter_paths=(
            "app.providers.bls_calendar:BlsReleaseCalendarProvider",
        ),
        publisher="U.S. Bureau of Labor Statistics",
        distributor="BLS",
        acquisition_provider="BLS",
    ),
    _provider(
        "BEA",
        "app.providers.bea:BeaProvider",
        "OFFICIAL_GOVERNMENT",
        (
            _capability(
                "gdp",
                "BEA:GDP",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "quarterly",
                probe_id="probe.bea.nipa",
                field_validator_id="validate.official_series.v1",
                request_group="bea.bea_nipa.gdp.batch",
                canonical_metric_ids=("real_gdp_annualized_qoq",),
            ),
            _capability(
                "gdp",
                "BEA:REAL_GDP",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "quarterly",
                probe_id="probe.bea.nipa",
                field_validator_id="validate.official_series.v1",
                request_group="bea.bea_nipa.gdp.batch",
                canonical_metric_ids=("real_gdp_yoy",),
            ),
            _capability(
                "pce",
                "BEA:PCE_PRICE_INDEX",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "monthly",
                probe_id="probe.bea.nipa",
                field_validator_id="validate.official_series.v1",
                request_group="bea.bea_nipa.pce.batch",
                canonical_metric_ids=(
                    "headline_pce_mom",
                    "headline_pce_yoy",
                ),
            ),
            _capability(
                "pce",
                "BEA:CORE_PCE",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "monthly",
                probe_id="probe.bea.nipa",
                field_validator_id="validate.official_series.v1",
                request_group="bea.bea_nipa.pce.batch",
                canonical_metric_ids=("core_pce_mom", "core_pce_yoy"),
            ),
            _capability(
                "pce",
                "BEA:PERSONAL_INCOME",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "monthly",
                probe_id="probe.bea.nipa",
                field_validator_id="validate.official_series.v1",
                request_group="bea.bea_nipa.pce.batch",
                canonical_metric_ids=("personal_income_mom",),
            ),
            _capability(
                "pce",
                "BEA:PERSONAL_SPENDING",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                ),
                "monthly",
                probe_id="probe.bea.nipa",
                field_validator_id="validate.official_series.v1",
                request_group="bea.bea_nipa.pce.batch",
                canonical_metric_ids=("personal_spending_mom",),
            ),
            _capability(
                "macro_calendar",
                "bea_release_occurrence",
                (
                    "event_at",
                    "actual",
                    "reference_period",
                    "released_at",
                    "lineage",
                ),
                "event",
                probe_id="probe.bea.release_schedule",
                field_validator_id="validate.calendar_occurrence.v1",
                probe_adapter_path=(
                    "app.providers.bea_calendar:BeaReleaseScheduleProvider"
                ),
            ),
        ),
        ("PRIMARY",),
        credentials=("bea_api_key",),
        timeout="bea_timeout_seconds",
        max_attempts=3,
        retry_policy="BOUNDED_HTTP_RETRY",
        probe_id="probe.bea.nipa",
        enable_setting="bea_enabled",
        runtime_adapter=True,
        additional_adapter_paths=(
            "app.providers.bea_calendar:BeaReleaseScheduleProvider",
        ),
        publisher="U.S. Bureau of Economic Analysis",
        distributor="BEA",
        acquisition_provider="BEA",
        source_domains=("bea.gov",),
    ),
    _provider(
        "CENSUS",
        "app.providers.census:CensusProvider",
        "OFFICIAL_GOVERNMENT",
        (
            _capability(
                "macro_calendar",
                "CENSUS:MARTS:RETAIL_SALES",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                    "lineage",
                ),
                "monthly",
                probe_id="probe.census.eits",
                field_validator_id="validate.official_series.v1",
                canonical_metric_ids=("advance_retail_sales",),
                probe_query_id="MARTS",
            ),
            _capability(
                "macro_calendar",
                "CENSUS:ADVM3:DURABLE_GOODS",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                    "lineage",
                ),
                "monthly",
                probe_id="probe.census.eits",
                field_validator_id="validate.official_series.v1",
                canonical_metric_ids=("advance_durable_goods_orders",),
                probe_query_id="ADVM3",
            ),
            _capability(
                "macro_calendar",
                "CENSUS:RESCONST:HOUSING_STARTS",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                    "lineage",
                ),
                "monthly",
                probe_id="probe.census.eits",
                field_validator_id="validate.official_series.v1",
                request_group="census.census_eits.resconst.batch",
                canonical_metric_ids=("housing_starts",),
                probe_query_id="RESCONST",
            ),
            _capability(
                "macro_calendar",
                "CENSUS:RESCONST:BUILDING_PERMITS",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                    "lineage",
                ),
                "monthly",
                probe_id="probe.census.eits",
                field_validator_id="validate.official_series.v1",
                request_group="census.census_eits.resconst.batch",
                canonical_metric_ids=("building_permits",),
                probe_query_id="RESCONST",
            ),
            _capability(
                "macro_calendar",
                "CENSUS:FTD:TRADE_BALANCE",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                    "lineage",
                ),
                "monthly",
                probe_id="probe.census.eits",
                field_validator_id="validate.official_series.v1",
                canonical_metric_ids=("international_trade_balance",),
                probe_query_id="FTD",
            ),
        ),
        ("OFFICIAL_ACTUAL",),
        credentials=("census_api_key (optional)",),
        timeout="census_timeout_seconds",
        max_attempts=3,
        retry_policy="BOUNDED_HTTP_RETRY",
        probe_id="probe.census.eits",
        enable_setting="census_enabled",
        runtime_adapter=True,
        publisher="U.S. Census Bureau",
        distributor="Census EITS",
        acquisition_provider="CENSUS",
    ),
    _provider(
        "CANONICAL_EVENT_REPOSITORY",
        "app.services.event_value_candidate_repository:EventValueCandidateRepository",
        "REPOSITORY",
        (
            _capability(
                "macro_calendar",
                "occurrence_fields",
                (
                    "actual",
                    "consensus",
                    "previous",
                    "previous_revised",
                    "occurrence_id",
                    "reference_period",
                    "lineage",
                ),
                "event",
                probe_id="probe.repository.event_values",
                field_validator_id="validate.calendar_occurrence.v1",
                request_group="repository.event_values.occurrence_fields",
                delivery_capability_ids=MACRO_CALENDAR_DELIVERY_METRIC_IDS,
                delivery_field_map=(
                    ("actual", "actual"),
                    ("consensus", "consensus"),
                    ("previous", "previous"),
                    ("previous_revised", "previous_revised"),
                    ("occurrence_id", "occurrence_id"),
                    ("reference_period", "reference_period"),
                    ("lineage", "lineage"),
                ),
            ),
            _capability(
                "flash_services_pmi",
                "flash_services_pmi",
                (
                    "actual",
                    "consensus",
                    "previous",
                    "previous_revised",
                    "occurrence_id",
                    "reference_period",
                    "lineage",
                ),
                "monthly",
                probe_id="probe.repository.event_values",
                field_validator_id="validate.calendar_occurrence.v1",
                request_group="repository.event_values.occurrence_fields",
            ),
        ),
        ("PRIMARY", "CANONICAL_REPOSITORY"),
        probe_id="probe.repository.event_values",
    ),
    _provider(
        "INVESTING_ECONOMIC_CALENDAR",
        "app.providers.investing_economic_calendar_provider:InvestingEconomicCalendarProvider",
        "AGGREGATOR",
        (
            _capability(
                "macro_calendar",
                "occurrence_fields",
                (
                    "event_at",
                    "actual",
                    "consensus",
                    "previous",
                    "reference_period",
                    "lineage",
                ),
                "event",
                probe_id="probe.investing.economic_calendar",
                field_validator_id="validate.calendar_occurrence.v1",
                delivery_capability_ids=MACRO_CALENDAR_DELIVERY_METRIC_IDS,
                delivery_field_map=(
                    ("actual", "actual"),
                    ("consensus", "consensus"),
                    ("previous", "previous"),
                    ("reference_period", "reference_period"),
                    ("lineage", "lineage"),
                ),
            ),
        ),
        ("FALLBACK", "ENRICHMENT"),
        probe_id="probe.investing.economic_calendar",
        enable_setting="enable_investing_calendar",
        runtime_adapter=True,
        additional_adapter_paths=(
            "app.providers.event_enrichment:InvestingEnrichmentProvider",
            "app.providers.event_enrichment:PlaywrightInvestingProvider",
        ),
        publisher="Economic-event publishers",
        distributor="Investing.com",
        acquisition_provider="INVESTING_ECONOMIC_CALENDAR",
    ),
    _provider(
        "XTB",
        "app.providers.xtb_economic_calendar_provider:XtbEconomicCalendarProvider",
        "AGGREGATOR",
        (
            _capability(
                "macro_calendar",
                "occurrence_fields",
                (
                    "event_at",
                    "actual",
                    "consensus",
                    "previous",
                    "reference_period",
                    "lineage",
                ),
                "event",
                probe_id="probe.xtb.economic_calendar",
                field_validator_id="validate.calendar_occurrence.v1",
                delivery_capability_ids=MACRO_CALENDAR_DELIVERY_METRIC_IDS,
                delivery_field_map=(
                    ("actual", "actual"),
                    ("consensus", "consensus"),
                    ("previous", "previous"),
                    ("reference_period", "reference_period"),
                    ("lineage", "lineage"),
                ),
            ),
        ),
        ("FALLBACK",),
        timeout="xtb_calendar_timeout_seconds",
        probe_id="probe.xtb.economic_calendar",
        enable_setting="enable_xtb_calendar",
        runtime_adapter=True,
        publisher="Economic-event publishers",
        distributor="XTB",
        acquisition_provider="XTB",
    ),
    _provider(
        "SPGLOBAL",
        "app.providers.sp_global_pmi:SpGlobalPmiProvider",
        "OFFICIAL_CORPORATE_PUBLISHER",
        (
            _capability(
                "flash_services_pmi",
                "SPGLOBAL:US:FLASH_SERVICES_PMI",
                (
                    "series_id",
                    "value",
                    "observations",
                    "units",
                    "frequency",
                    "data_as_of",
                    "source",
                    "source_url",
                    "lineage",
                ),
                "monthly",
                probe_id="probe.spglobal.flash_services_pmi",
                field_validator_id="validate.official_series.v1",
                canonical_metric_ids=("flash_services_pmi",),
                delivery_field_map=(
                    ("value", "actual"),
                    ("lineage", "lineage"),
                ),
            ),
        ),
        ("PRIMARY",),
        timeout="sp_global_pmi_timeout_seconds",
        probe_id="probe.spglobal.flash_services_pmi",
        enable_setting="sp_global_pmi_enabled",
        runtime_adapter=True,
        publisher="S&P Global Market Intelligence",
        distributor="S&P Global",
        acquisition_provider="SPGLOBAL",
        source_domains=("pmi.spglobal.com",),
    ),
    _provider(
        "INVESTING_EVENT_1062",
        "app.providers.investing_flash_services_pmi:InvestingFlashServicesPmiProvider",
        "AGGREGATOR",
        (
            _capability(
                "flash_services_pmi",
                "flash_services_pmi",
                (
                    "actual",
                    "consensus",
                    "previous",
                    "occurrence_id",
                    "reference_period",
                    "released_at",
                    "lineage",
                ),
                "monthly",
                probe_id="probe.investing.event_1062",
                field_validator_id="validate.flash_services_pmi.v1",
                delivery_field_map=(
                    ("actual", "actual"),
                    ("lineage", "lineage"),
                ),
            ),
        ),
        ("FALLBACK",),
        timeout="investing_flash_services_pmi_timeout_seconds",
        probe_id="probe.investing.event_1062",
        enable_setting="investing_flash_services_pmi_enabled",
        runtime_adapter=True,
        publisher="S&P Global / economic-event publisher",
        distributor="Investing.com",
        acquisition_provider="INVESTING_EVENT_1062",
        source_domains=("endpoints.investing.com",),
    ),
    _provider(
        "FMP_EARNINGS_CALENDAR",
        "app.providers.fmp_earnings_calendar_provider:FmpEarningsCalendarProvider",
        "STRUCTURED_VENDOR",
        (
            _capability(
                "earnings",
                "earnings_event",
                (
                    "symbol",
                    "event_date",
                    "event_at",
                    "timing",
                    "eps_estimate",
                    "revenue_estimate",
                    "temporal_precision",
                    "lineage",
                ),
                "event",
                probe_id="probe.fmp.earnings",
                field_validator_id="validate.earnings_event.v1",
            ),
        ),
        ("FALLBACK",),
        credentials=("fmp_api_key",),
        timeout="timeout_earnings_seconds",
        probe_id="probe.fmp.earnings",
        enable_setting="enable_fmp_earnings",
        runtime_adapter=True,
        publisher="Issuer and market estimates",
        distributor="Financial Modeling Prep",
        acquisition_provider="FMP_EARNINGS_CALENDAR",
        source_domains=("financialmodelingprep.com",),
    ),
    _provider(
        "CFTC",
        "app.providers.cftc_cot_provider:CftcCotProvider",
        "OFFICIAL_GOVERNMENT",
        (
            _capability(
                "positioning",
                "CFTC_MNQ_COT",
                (
                    "report_date",
                    "contract_code",
                    "long",
                    "short",
                    "net_position",
                    "lineage",
                ),
                "weekly",
                probe_id="probe.cftc.cot",
                field_validator_id="validate.cftc_positioning.v1",
            ),
        ),
        ("PRIMARY",),
        timeout="timeout_cot_seconds",
        probe_id="probe.cftc.cot",
        runtime_adapter=True,
        publisher="U.S. Commodity Futures Trading Commission",
        distributor="CFTC",
        acquisition_provider="CFTC",
    ),
    _provider(
        "ALPHA_VANTAGE_NEWS_SENTIMENT",
        "app.providers.news_provider:NewsProvider",
        "STRUCTURED_VENDOR",
        (
            _capability(
                "current_news",
                "current_article",
                (
                    "title",
                    "canonical_url",
                    "publisher",
                    "published_at",
                    "symbols",
                    "topics",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.news.alpha_vantage",
                field_validator_id="validate.current_news.v1",
            ),
        ),
        ("PRIMARY",),
        credentials=("alpha_vantage_api_key",),
        timeout="timeout_news_seconds",
        probe_id="probe.news.alpha_vantage",
        enable_setting="alpha_vantage_api_key",
        runtime_adapter=True,
        publisher="Original article publisher",
        distributor="Alpha Vantage",
        acquisition_provider="ALPHA_VANTAGE_NEWS_SENTIMENT",
    ),
    *tuple(
        _provider(
            provider_id,
            "app.providers.news_provider:NewsProvider",
            provider_type,
            (
                _capability(
                    "current_news",
                    "current_article",
                    (
                        "title",
                        "canonical_url",
                        "publisher",
                        "published_at",
                        "symbols",
                        "topics",
                        "lineage",
                    ),
                    "intraday",
                    probe_id=probe_id,
                    field_validator_id="validate.current_news.v1",
                ),
            ),
            ("FALLBACK",),
            timeout=timeout,
            max_attempts=max_attempts,
            retry_policy=retry_policy,
            probe_id=probe_id,
            enable_setting=enable_setting,
            runtime_adapter=True,
            publisher=publisher,
            distributor=distributor,
            acquisition_provider=provider_id,
        )
        for (
            provider_id,
            provider_type,
            probe_id,
            timeout,
            max_attempts,
            retry_policy,
            publisher,
            distributor,
            enable_setting,
        ) in (
            (
                "GDELT_DOC_API",
                "AGGREGATOR",
                "probe.news.gdelt",
                "news_gdelt_timeout_seconds",
                3,
                "BOUNDED_HTTP_RETRY",
                "Original article publisher",
                "GDELT",
                "news_gdelt_enabled",
            ),
            (
                "FEDERAL_RESERVE_RSS",
                "OFFICIAL_RSS",
                "probe.news.federal_reserve_rss",
                "timeout_news_seconds",
                1,
                "NONE",
                "Federal Reserve",
                "Federal Reserve RSS",
                "news_rss_enabled",
            ),
            (
                "BLS_RSS",
                "OFFICIAL_RSS",
                "probe.news.bls_rss",
                "timeout_news_seconds",
                1,
                "NONE",
                "U.S. Bureau of Labor Statistics",
                "BLS RSS",
                "news_rss_enabled",
            ),
            (
                "BEA_RSS",
                "OFFICIAL_RSS",
                "probe.news.bea_rss",
                "timeout_news_seconds",
                1,
                "NONE",
                "U.S. Bureau of Economic Analysis",
                "BEA RSS",
                "news_rss_enabled",
            ),
            (
                "YAHOO_FINANCE_RSS",
                "DISTRIBUTOR",
                "probe.news.yahoo_rss",
                "timeout_news_seconds",
                1,
                "NONE",
                "Original article publisher",
                "Yahoo Finance",
                "news_rss_enabled",
            ),
            (
                "MARKETWATCH_RSS",
                "PUBLISHER_RSS",
                "probe.news.marketwatch_rss",
                "timeout_news_seconds",
                1,
                "NONE",
                "MarketWatch / Dow Jones",
                "MarketWatch RSS",
                "news_rss_enabled",
            ),
            (
                "GOOGLE_NEWS_RSS",
                "AGGREGATOR",
                "probe.news.google_rss",
                "timeout_news_seconds",
                1,
                "NONE",
                "Original article publisher",
                "Google News",
                "news_rss_enabled",
            ),
        )
    ),
    _provider(
        "NASDAQ_MARKET_INFO",
        "app.providers.nasdaq_market_info_provider:NasdaqMarketInfoProvider",
        "OFFICIAL_EXCHANGE",
        (
            _capability(
                "market_schedule",
                "nasdaq_cash_session",
                (
                    "session_date",
                    "open_at",
                    "close_at",
                    "holiday",
                    "data_as_of",
                    "lineage",
                ),
                "event",
                probe_id="probe.nasdaq.market_info",
                field_validator_id="validate.market_schedule.v1",
            ),
        ),
        ("PRIMARY",),
        timeout="timeout_nasdaq_seconds",
        probe_id="probe.nasdaq.market_info",
        enable_setting="enable_nasdaq_market_info",
        runtime_adapter=True,
        publisher="Nasdaq",
        distributor="Nasdaq",
        acquisition_provider="NASDAQ_MARKET_INFO",
    ),
    _provider(
        "CME",
        "app.providers.cme_market_schedule_provider:CmeMarketScheduleProvider",
        "OFFICIAL_EXCHANGE",
        (
            _capability(
                "market_schedule",
                "mnq_futures_session",
                (
                    "session_date",
                    "open_at",
                    "close_at",
                    "maintenance_break",
                    "data_as_of",
                    "lineage",
                ),
                "event",
                probe_id="probe.cme.market_schedule",
                field_validator_id="validate.market_schedule.v1",
            ),
        ),
        ("FALLBACK",),
        timeout="cme_market_schedule_timeout_seconds",
        probe_id="probe.cme.market_schedule",
        enable_setting="enable_cme_market_schedule",
        runtime_adapter=True,
        publisher="CME Group",
        distributor="CME Group",
        acquisition_provider="CME",
    ),
    _provider(
        "INVESTING_HOLIDAYS",
        "app.providers.investing_holiday_calendar_provider:InvestingHolidayCalendarProvider",
        "AGGREGATOR",
        (
            _capability(
                "market_schedule",
                "market_holidays",
                ("holiday_date", "name", "market", "lineage"),
                "event",
                probe_id="probe.investing.holidays",
                field_validator_id="validate.market_schedule.v1",
            ),
        ),
        ("FALLBACK",),
        probe_id="probe.investing.holidays",
        enable_setting="enable_investing_holidays",
        runtime_adapter=True,
        publisher="Exchange holiday calendars",
        distributor="Investing.com",
        acquisition_provider="INVESTING_HOLIDAYS",
    ),
    _provider(
        "MARKETBEAT",
        "app.providers.marketbeat_holidays_provider:MarketBeatHolidaysProvider",
        "DISTRIBUTOR",
        (
            _capability(
                "market_schedule",
                "market_holidays",
                ("holiday_date", "name", "market", "lineage"),
                "event",
                probe_id="probe.marketbeat.holidays",
                field_validator_id="validate.market_schedule.v1",
            ),
        ),
        ("FALLBACK",),
        timeout="marketbeat_timeout_seconds",
        probe_id="probe.marketbeat.holidays",
        enable_setting="enable_marketbeat_holidays",
        runtime_adapter=True,
        publisher="Exchange holiday calendars",
        distributor="MarketBeat",
        acquisition_provider="MARKETBEAT",
    ),
    _provider(
        "NASDAQ_QQQ_OPTIONS",
        "app.providers.nasdaq_qqq_option_chain_provider:NasdaqQQQOptionChainProvider",
        "OFFICIAL_EXCHANGE",
        (
            _capability(
                "options_positioning",
                "qqq_option_chain",
                (
                    "expiration",
                    "strike",
                    "option_type",
                    "volume",
                    "open_interest",
                    "implied_volatility",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.nasdaq.qqq_options",
                field_validator_id="validate.option_chain.v1",
            ),
        ),
        ("SUPPORTING",),
        timeout="timeout_nasdaq_seconds",
        probe_id="probe.nasdaq.qqq_options",
        enable_setting="enable_nasdaq_qqq_options",
        runtime_adapter=True,
        publisher="Nasdaq",
        distributor="Nasdaq",
        acquisition_provider="NASDAQ_QQQ_OPTIONS",
    ),
    _provider(
        "FINNHUB",
        "app.providers.finnhub:FinnhubProvider",
        "STRUCTURED_VENDOR",
        (
            _capability(
                "earnings",
                "earnings_event_candidate",
                (
                    "symbol",
                    "event_date",
                    "timing",
                    "eps_estimate",
                    "revenue_estimate",
                    "lineage",
                ),
                "event",
                probe_id="probe.finnhub.earnings",
                field_validator_id="validate.earnings_event.v1",
            ),
            _capability(
                "current_news",
                "news_candidate",
                (
                    "headline",
                    "url",
                    "published_at",
                    "source",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.finnhub.news",
                field_validator_id="validate.news_candidate.v1",
            ),
        ),
        ("DISCOVERY", "FALLBACK"),
        credentials=("finnhub_api_key",),
        timeout="finnhub_timeout_seconds",
        max_attempts=3,
        retry_policy="BOUNDED_HTTP_RETRY",
        probe_id="probe.finnhub.earnings",
        enable_setting="finnhub_enabled",
        runtime_adapter=True,
        publisher="Issuer / original article publisher",
        distributor="Finnhub",
        acquisition_provider="FINNHUB",
    ),
    _provider(
        "LEGACY_EARNINGS_AGGREGATOR",
        "app.providers.earnings_provider:EarningsProvider",
        "RECONCILIATION",
        (
            _capability(
                "earnings",
                "earnings_event",
                (
                    "symbol",
                    "event_date",
                    "timing",
                    "eps_estimate",
                    "revenue_estimate",
                    "lineage",
                ),
                "event",
                probe_id="probe.legacy_earnings.chain",
                field_validator_id="validate.earnings_event.v1",
                probe_adapter_path=(
                    "app.services.provider_capability_probe_hooks:"
                    "LocalCapabilityProbeHook"
                ),
            ),
        ),
        ("LEGACY_RUNTIME",),
        probe_id="probe.legacy_earnings.chain",
        runtime_adapter=True,
        acquisition_provider="LEGACY_EARNINGS_AGGREGATOR",
    ),
    _provider(
        "AAII",
        "app.providers.aaii_sentiment_provider:AaiiSentimentProvider",
        "PUBLISHER_WEB",
        (
            _capability(
                "sentiment",
                "aaii_sentiment",
                (
                    "bullish",
                    "neutral",
                    "bearish",
                    "survey_date",
                    "lineage",
                ),
                "weekly",
                probe_id="probe.aaii.sentiment",
                field_validator_id="validate.sentiment_survey.v1",
            ),
        ),
        ("SUPPORTING",),
        timeout="timeout_sentiment_seconds",
        probe_id="probe.aaii.sentiment",
        enable_setting="enable_aaii_sentiment",
        runtime_adapter=True,
        publisher="AAII",
        distributor="AAII",
        acquisition_provider="AAII",
    ),
    _provider(
        "MACROMICRO",
        "app.providers.macromicro_aaii_crosscheck_provider:MacroMicroAaiiCrosscheckProvider",
        "DISTRIBUTOR",
        (
            _capability(
                "sentiment",
                "aaii_sentiment_crosscheck",
                ("survey_date", "bullish", "neutral", "bearish", "lineage"),
                "weekly",
                probe_id="probe.macromicro.aaii",
                field_validator_id="validate.sentiment_survey.v1",
            ),
        ),
        ("CROSSCHECK",),
        timeout="timeout_sentiment_seconds",
        probe_id="probe.macromicro.aaii",
        enable_setting="enable_macromicro_aaii_crosscheck",
        runtime_adapter=True,
        publisher="AAII",
        distributor="MacroMicro",
        acquisition_provider="MACROMICRO",
    ),
    _provider(
        "HACKER_NEWS",
        "app.providers.hacker_news_social_sentiment_provider:HackerNewsSocialSentimentProvider",
        "PUBLIC_API",
        (
            _capability(
                "social_sentiment",
                "mnq_social_mentions",
                ("title", "url", "published_at", "score", "symbols", "lineage"),
                "intraday",
                transformation="deterministic_keyword_sentiment",
                probe_id="probe.hacker_news.social",
                field_validator_id="validate.social_sentiment.v1",
            ),
        ),
        ("SUPPORTING",),
        timeout="social_sentiment_timeout_seconds",
        probe_id="probe.hacker_news.social",
        enable_setting="enable_social_sentiment",
        runtime_adapter=True,
        publisher="Hacker News submitters",
        distributor="Algolia",
        acquisition_provider="HACKER_NEWS",
    ),
    _provider(
        "POLYMARKET",
        "app.providers.polymarket_prediction_provider:PolymarketPredictionProvider",
        "PUBLIC_MARKET_API",
        (
            _capability(
                "prediction_markets",
                "mnq_relevant_markets",
                (
                    "market_id",
                    "question",
                    "probability",
                    "volume",
                    "data_as_of",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.polymarket.markets",
                field_validator_id="validate.prediction_market.v1",
            ),
        ),
        ("SUPPORTING",),
        timeout="polymarket_timeout_seconds",
        probe_id="probe.polymarket.markets",
        enable_setting="enable_polymarket",
        runtime_adapter=True,
        publisher="Polymarket participants",
        distributor="Polymarket",
        acquisition_provider="POLYMARKET",
    ),
    _provider(
        "DAILYFX",
        "app.providers.event_enrichment:DailyFxEnrichmentProvider",
        "AGGREGATOR",
        (
            _capability(
                "macro_calendar",
                "occurrence_enrichment",
                (
                    "event_at",
                    "consensus",
                    "previous",
                    "reference_period",
                    "lineage",
                ),
                "event",
                probe_id="probe.dailyfx.calendar",
                field_validator_id="validate.calendar_occurrence.v1",
            ),
        ),
        ("ENRICHMENT",),
        probe_id="probe.dailyfx.calendar",
        runtime_adapter=True,
        additional_adapter_paths=(
            "app.providers.event_enrichment:PlaywrightDailyFXProvider",
        ),
        publisher="Economic-event publishers",
        distributor="DailyFX",
        acquisition_provider="DAILYFX",
    ),
    _provider(
        "FOREX_FACTORY",
        "app.providers.event_enrichment:ForexFactoryEnrichmentProvider",
        "AGGREGATOR",
        (
            _capability(
                "macro_calendar",
                "occurrence_enrichment",
                (
                    "event_at",
                    "consensus",
                    "previous",
                    "reference_period",
                    "lineage",
                ),
                "event",
                probe_id="probe.forex_factory.calendar",
                field_validator_id="validate.calendar_occurrence.v1",
            ),
        ),
        ("ENRICHMENT",),
        probe_id="probe.forex_factory.calendar",
        runtime_adapter=True,
        additional_adapter_paths=(
            "app.providers.event_enrichment:PlaywrightForexFactoryProvider",
        ),
        publisher="Economic-event publishers",
        distributor="Forex Factory",
        acquisition_provider="FOREX_FACTORY",
    ),
    *tuple(
        _provider(
            provider_id,
            adapter_path,
            provider_type,
            (
                _capability(
                    "macro_calendar",
                    "occurrence_enrichment",
                    (
                        "event_at",
                        "consensus",
                        "previous",
                        "reference_period",
                        "lineage",
                    ),
                    "event",
                    probe_id=probe_id,
                    field_validator_id="validate.calendar_occurrence.v1",
                    ai_eligible=(
                        provider_type == "AI"
                        and provider_id != "OPENAI_EVENT_ENRICHMENT"
                    ),
                ),
            ),
            roles,
            probe_id=probe_id,
            enable_setting=enable_setting,
            runtime_adapter=True,
            publisher=publisher,
            distributor=distributor,
            acquisition_provider=provider_id,
        )
        for (
            provider_id,
            adapter_path,
            provider_type,
            probe_id,
            enable_setting,
            roles,
            publisher,
            distributor,
        ) in (
            (
                "FXSTREET",
                "app.providers.event_enrichment:FXStreetEconomicCalendarProvider",
                "AGGREGATOR",
                "probe.fxstreet.calendar",
                "enable_event_enrichment_scrapers",
                ("ENRICHMENT",),
                "Economic-event publishers",
                "FXStreet",
            ),
            (
                "MARKETWATCH_CALENDAR",
                "app.providers.event_enrichment:MarketWatchEconomicCalendarProvider",
                "PUBLISHER",
                "probe.marketwatch.calendar",
                "enable_event_enrichment_scrapers",
                ("ENRICHMENT",),
                "MarketWatch",
                "MarketWatch",
            ),
            (
                "YAHOO_ECONOMIC_CALENDAR",
                "app.providers.event_enrichment:YahooEconomicCalendarProvider",
                "AGGREGATOR",
                "probe.yahoo.economic_calendar",
                "enable_event_enrichment_scrapers",
                ("ENRICHMENT",),
                "Economic-event publishers",
                "Yahoo Finance",
            ),
            (
                "GENERIC_SEARCH_CALENDAR",
                "app.providers.event_enrichment:GenericSearchSnippetCalendarProvider",
                "SEARCH_SNIPPET",
                "probe.generic_search.calendar",
                "enable_aggressive_scraping",
                ("ENRICHMENT",),
                "Search result publishers",
                "Configured search endpoint",
            ),
            (
                "TARGETED_SEARCH_EVENT",
                "app.providers.event_enrichment:TargetedSearchEventEnrichmentProvider",
                "SEARCH_SNIPPET",
                "probe.targeted_search.event",
                "enable_targeted_search_enrichment",
                ("ENRICHMENT",),
                "Search result publishers",
                "Google News RSS",
            ),
            (
                "MANUAL_EVENT_ENRICHMENT",
                "app.providers.event_enrichment:ManualEventEnrichmentProvider",
                "MANUAL_FILE",
                "probe.manual_event.file",
                None,
                ("ENRICHMENT",),
                "Operator-supplied evidence",
                "Local versioned file",
            ),
            (
                "OPENAI_EVENT_ENRICHMENT",
                "app.providers.event_enrichment:OpenAIEventEnrichmentProvider",
                "AI",
                "probe.ai.openai_event_enrichment",
                "enable_openai_event_enrichment",
                ("AUDIT_ONLY",),
                "Evidence publisher cited by model",
                "OpenAI",
            ),
        )
    ),
    _provider(
        "ECONOMIC_CALENDAR_SCRAPER",
        "app.providers.scraper_calendar:EconomicCalendarScraperProvider",
        "SCRAPER",
        (
            _capability(
                "macro_calendar",
                "occurrence_enrichment",
                ("event_at", "name", "importance", "lineage"),
                "event",
                probe_id="probe.scraper.calendar",
                field_validator_id="validate.calendar_occurrence.v1",
            ),
        ),
        ("LEGACY_FALLBACK",),
        probe_id="probe.scraper.calendar",
        enable_setting="enable_scraper_fallbacks",
        runtime_adapter=True,
        acquisition_provider="ECONOMIC_CALENDAR_SCRAPER",
    ),
    _provider(
        "AI_RESEARCHER",
        "app.providers.ai_researcher_provider:AIResearcherProvider",
        "AI",
        (
            *tuple(
                _capability(
                    (
                        "flash_services_pmi"
                        if specification.canonical_metric_id
                        == "flash_services_pmi"
                        else "macro_calendar"
                    ),
                    specification.canonical_metric_id,
                    (
                        "consensus",
                        "consensus_lineage",
                        "previous",
                        "previous_lineage",
                        "occurrence_id",
                        "reference_period",
                        "source",
                        "source_url",
                        "lineage",
                    ),
                    specification.frequency,
                    transformation=specification.transformation,
                    probe_id="probe.ai.researcher.macro_calendar",
                    field_validator_id="validate.ai_event_evidence.v1",
                    ai_eligible=False,
                    request_group="ai_researcher.concrete_capabilities.batch",
                    canonical_metric_ids=(
                        specification.canonical_metric_id,
                    ),
                    delivery_field_map=(
                            ("consensus", "consensus"),
                            ("consensus_lineage", "consensus_lineage"),
                            ("previous", "previous"),
                            ("previous_lineage", "previous_lineage"),
                            ("occurrence_id", "occurrence_id"),
                            ("reference_period", "reference_period"),
                            ("lineage", "lineage"),
                    ),
                    audit_only_fields=(
                        "actual",
                        "previous_revised",
                        "previous_revised_lineage",
                    ),
                )
                for specification in OFFICIAL_METRIC_REGISTRATIONS
            ),
            _capability(
                "earnings",
                "earnings_schedule_context",
                (
                    "event_date",
                    "timing",
                    "source_url",
                    "publisher",
                    "lineage",
                ),
                "event",
                probe_id="probe.ai.researcher.earnings",
                field_validator_id="validate.ai_earnings_evidence.v1",
                ai_eligible=False,
                delivery_capability_id="earnings_event",
                delivery_field_map=(
                    ("event_date", "event_date"),
                    ("timing", "timing"),
                    ("lineage", "lineage"),
                ),
            ),
            _capability(
                "current_news",
                "current_article",
                (
                    "title",
                    "canonical_url",
                    "publisher",
                    "published_at",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.ai.researcher.current_news",
                field_validator_id="validate.ai_news_evidence.v1",
                ai_eligible=False,
            ),
        ),
        ("AUDIT_ONLY",),
        credentials=("codex_cli_command|openai_api_key",),
        timeout="timeout_ai_research_seconds",
        probe_id="probe.ai.researcher.macro_calendar",
        enable_setting="enable_ai_researcher",
        runtime_adapter=True,
        publisher="Evidence publisher cited by model",
        distributor="Codex CLI or OpenAI API",
        acquisition_provider="AI_RESEARCHER",
        audit_source_url_budget=12,
        source_domains=RESEARCH_RUNTIME_SOURCE_DOMAINS,
    ),
    _provider(
        "CODEX_CLI_RESEARCH_BACKEND",
        (
            "app.services.ai_research_job_executor:"
            "PersistentAIJobExecutor"
        ),
        "AI",
        _research_backend_capabilities(
            probe_id="probe.ai.codex_cli_research.profiles",
        ),
        ("AI_BACKEND",),
        credentials=("codex_cli_command",),
        timeout="ai_job_max_runtime_seconds",
        probe_id="probe.ai.codex_cli_research.profiles",
        enable_setting="enable_ai_researcher",
        runtime_adapter=True,
        publisher="Evidence publisher cited by model",
        distributor="Codex CLI",
        acquisition_provider="CODEX_CLI_RESEARCH_BACKEND",
        capture_mode="SUBPROCESS",
        audit_source_url_budget=12,
        configuration_setting="research_backend",
        configuration_value="codex_cli",
        additional_adapter_paths=RESEARCH_RUNTIME_COMPONENT_PATHS,
    ),
    _provider(
        "OPENAI_RESPONSES_RESEARCH",
        "app.services.research_backend:OpenAIResponsesResearchBackend",
        "AI",
        _research_backend_capabilities(
            probe_id="probe.ai.openai_responses.profiles",
        ),
        ("AI_BACKEND",),
        credentials=("openai_api_key",),
        timeout="openai_research_timeout_seconds",
        probe_id="probe.ai.openai_responses.profiles",
        enable_setting="enable_ai_researcher",
        runtime_adapter=True,
        publisher="Evidence publisher cited by model",
        distributor="OpenAI",
        acquisition_provider="OPENAI_RESPONSES_RESEARCH",
        audit_source_url_budget=12,
        configuration_setting="research_backend",
        configuration_value="openai_api",
        additional_adapter_paths=RESEARCH_RUNTIME_COMPONENT_PATHS,
    ),
    _provider(
        "PROVIDER_CACHE_REPOSITORY",
        "app.infrastructure.persistence.provider_cache_repository:ProviderCacheRepository",
        "REPOSITORY",
        (
            _capability(
                "provider_cache",
                "provider_cache_entry",
                (
                    "payload",
                    "created_at",
                    "updated_at",
                    "valid_until",
                    "stale_until",
                    "status",
                    "checksum",
                ),
                "request",
                probe_id="probe.repository.provider_cache",
                field_validator_id="validate.repository_cache_entry.v1",
            ),
        ),
        ("CANONICAL_REPOSITORY", "TECHNICAL_CACHE"),
        probe_id="probe.repository.provider_cache",
    ),
    _provider(
        "MARKET_FACT_REPOSITORY",
        "app.services.market_fact_repository:MarketFactRepository",
        "REPOSITORY",
        tuple(
            _capability(
                dataset_id,
                f"canonical_{dataset_id}_record",
                (
                    "fact_key",
                    "fact_type",
                    "value",
                    "data_as_of",
                    "release_at",
                    "valid_until",
                    "next_refresh_at",
                    "lineage",
                ),
                query.frequency,
                probe_id="probe.repository.market_facts",
                field_validator_id="validate.repository_market_fact.v1",
                request_group="repository.market_facts.dataset_lookups",
            )
            for dataset_id, query in (
                MARKET_FACT_REPOSITORY_DATASET_QUERIES.items()
            )
        ),
        ("CANONICAL_REPOSITORY",),
        probe_id="probe.repository.market_facts",
    ),
    _provider(
        "MARKET_NEWS_REPOSITORY",
        "app.services.market_news_repository:MarketNewsRepository",
        "REPOSITORY",
        (
            _capability(
                "current_news",
                "current_article",
                (
                    "canonical_url",
                    "publisher",
                    "published_at",
                    "valid_until",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.repository.market_news",
                field_validator_id="validate.repository_news.v1",
            ),
        ),
        ("CANONICAL_REPOSITORY",),
        probe_id="probe.repository.market_news",
    ),
    _provider(
        "FED_EXPECTATIONS_REPOSITORY",
        "app.services.fed_expectations_repository:FedExpectationsRepository",
        "REPOSITORY",
        (
            _capability(
                "fomc_expectations",
                "pre_meeting_probabilities",
                (
                    "meeting_date",
                    "probabilities",
                    "data_as_of",
                    "valid_until",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.repository.fed_expectations",
                field_validator_id="validate.repository_fed_expectations.v1",
            ),
        ),
        ("CANONICAL_REPOSITORY",),
        probe_id="probe.repository.fed_expectations",
    ),
    _provider(
        "RISK_CONTEXT_REPOSITORY",
        "app.services.risk_context_repository:RiskContextHistoryRepository",
        "REPOSITORY",
        (
            _capability(
                "risk",
                "risk_context",
                (
                    "vvix",
                    "skew",
                    "risk_score",
                    "data_as_of",
                    "valid_until",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.repository.risk_context",
                field_validator_id="validate.repository_risk_context.v1",
                request_group="repository.risk_context.latest",
            ),
            _capability(
                "vvix",
                "vvix",
                (
                    "vvix",
                    "data_as_of",
                    "valid_until",
                    "lineage",
                ),
                "intraday",
                probe_id="probe.repository.risk_context",
                field_validator_id="validate.repository_risk_context.v1",
                request_group="repository.risk_context.latest",
            ),
        ),
        ("CANONICAL_REPOSITORY",),
        probe_id="probe.repository.risk_context",
    ),
    _provider(
        "EVENT_CALENDAR_COVERAGE_REPOSITORY",
        "app.services.event_calendar_coverage_repository:EventCalendarCoverageRepository",
        "REPOSITORY",
        (
            _capability(
                "macro_calendar",
                "coverage_window",
                (
                    "country",
                    "window_start",
                    "window_end",
                    "provider",
                    "status",
                    "checked_at",
                ),
                "event",
                probe_id="probe.repository.event_calendar_coverage",
                field_validator_id="validate.repository_calendar_coverage.v1",
            ),
        ),
        ("CANONICAL_REPOSITORY",),
        probe_id="probe.repository.event_calendar_coverage",
    ),
    _provider(
        "MARKET_CONTEXT_SNAPSHOT_REPOSITORY",
        "app.services.market_context_snapshot_repository:MarketContextSnapshotRepository",
        "REPOSITORY",
        (
            _capability(
                "market_context_snapshot",
                "immutable_market_context_snapshot",
                (
                    "snapshot_id",
                    "snapshot_revision",
                    "generated_at",
                    "checksum",
                ),
                "request",
                probe_id="probe.repository.market_context_snapshot",
                field_validator_id="validate.repository_snapshot.v1",
            ),
        ),
        ("CANONICAL_REPOSITORY",),
        probe_id="probe.repository.market_context_snapshot",
    ),
    _provider(
        "OFFICIAL_ACTUAL_TRANSFORMATION",
        "app.services.official_actual_semantics:derive_official_actual",
        "TRANSFORMATION",
        tuple(
            _capability(
                specification.dataset_id,
                specification.canonical_metric_id,
                (
                    "actual",
                    "metric_id",
                    "reference_period",
                    "frequency",
                    "transformation",
                    "lineage",
                ),
                specification.frequency,
                transformation=specification.transformation,
                probe_id="probe.transform.official_actual",
                field_validator_id="validate.official_actual.v1",
            )
            for specification in OFFICIAL_METRIC_REGISTRATIONS
        ),
        ("TRANSFORMATION",),
        probe_id="probe.transform.official_actual",
    ),
    _provider(
        "MACRO_CONSENSUS_RECONCILIATION",
        "app.services.macro_consensus_service:merge_consensus_provider_payloads",
        "RECONCILIATION",
        (
            _capability(
                "macro_calendar",
                "macro_consensus",
                (
                    "consensus",
                    "occurrence_id",
                    "reference_period",
                    "lineage",
                ),
                "event",
                transformation="ranked_exact_occurrence_reconciliation",
                probe_id="probe.transform.macro_consensus",
                field_validator_id="validate.calendar_occurrence.v1",
                request_group=(
                    "macro_consensus_reconciliation."
                    "transform_macro_consensus.macro_calendar.batch"
                ),
            ),
            _capability(
                "macro_calendar",
                "macro_previous",
                (
                    "previous",
                    "occurrence_id",
                    "reference_period",
                    "lineage",
                ),
                "event",
                transformation="ranked_exact_occurrence_reconciliation",
                probe_id="probe.transform.macro_consensus",
                field_validator_id="validate.calendar_occurrence.v1",
                request_group=(
                    "macro_consensus_reconciliation."
                    "transform_macro_consensus.macro_calendar.batch"
                ),
            ),
        ),
        ("RECONCILIATION",),
        probe_id="probe.transform.macro_consensus",
    ),
    _provider(
        "PROVIDER_FORCE_ACTUAL_RECONCILIATION",
        (
            "app.services.provider_force_actual_reconciliation_service:"
            "ProviderForceActualReconciliationService"
        ),
        "RECONCILIATION",
        (
            _capability(
                "flash_services_pmi",
                "flash_services_pmi",
                (
                    "actual",
                    "consensus",
                    "previous",
                    "occurrence_id",
                    "reference_period",
                    "lineage",
                ),
                "monthly",
                transformation="db_first_provider_force_reconciliation",
                probe_id="probe.transform.provider_force_actual",
                field_validator_id="validate.flash_services_pmi.v1",
            ),
        ),
        ("RECONCILIATION",),
        probe_id="probe.transform.provider_force_actual",
    ),
    _provider(
        "REQUEST_PROVIDER_ACCOUNTING",
        "app.services.request_provider_accounting:RequestProviderAccountingCollector",
        "TRANSFORMATION",
        (
            _capability(
                "request_accounting",
                "request_scoped_provider_accounting",
                (
                    "request_id",
                    "correlation_id",
                    "database_lookup",
                    "provider_attempts",
                    "selected_source",
                    "reason_code",
                ),
                "request",
                transformation="request_scoped_evidence_collection",
                probe_id="probe.transform.request_accounting",
                field_validator_id="validate.request_accounting.v1",
            ),
        ),
        ("ACCOUNTING",),
        probe_id="probe.transform.request_accounting",
    ),
    _provider(
        "SENIOR_ANALYST_PROJECTION",
        "app.services.senior_analyst_projection_v1:build_senior_analyst_payload_v1",
        "TRANSFORMATION",
        (
            _capability(
                "senior_analyst_payload",
                "senior_analyst_projection",
                ("analytics", "readiness", "missing_data", "provider_accounting"),
                "request",
                transformation="fail_closed_consumer_projection",
                probe_id="probe.transform.senior_analyst_projection",
                field_validator_id="validate.senior_analyst_payload.v1",
            ),
        ),
        ("PROJECTION",),
        probe_id="probe.transform.senior_analyst_projection",
    ),
)


DATASET_SOURCE_POLICIES: tuple[DatasetSourcePolicy, ...] = (
    DatasetSourcePolicy(
        "nasdaq_100",
        "nasdaq",
        "intraday",
        12 * 60 * 60,
        "INVESCO",
        ("ALPHA_VANTAGE", "NASDAQ", "SEC"),
        "MARKET_FACT_REPOSITORY",
        provider_strategy="CASCADE",
    ),
    DatasetSourcePolicy(
        "mega_cap_quotes",
        "nasdaq",
        "intraday",
        12 * 60 * 60,
        "YAHOO_FINANCE_CHART",
        ("STOOQ", "ALPHA_VANTAGE", "YAHOO_FINANCE_QUOTE"),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "market_internals",
        "market_internals",
        "intraday",
        2 * 60 * 60,
        "TRADIER",
        (),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "vix",
        "vix",
        "daily",
        2 * 24 * 60 * 60,
        "FRED",
        ("CBOE",),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "vvix",
        "vix",
        "intraday",
        2 * 60 * 60,
        "CBOE",
        (),
        "RISK_CONTEXT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "risk",
        "risk",
        "intraday",
        2 * 60 * 60,
        "CBOE",
        (),
        "RISK_CONTEXT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "treasury_rates",
        "rates",
        "daily",
        2 * 24 * 60 * 60,
        "FRED",
        (),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "fed_funds",
        "rates",
        "daily",
        2 * 24 * 60 * 60,
        "FRED",
        (),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "target_range",
        "rates",
        "event",
        45 * 24 * 60 * 60,
        "FRED",
        (),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "fomc_expectations",
        "fomc",
        "intraday",
        2 * 60 * 60,
        "INVESTING_FED_RATE_MONITOR",
        (),
        "FED_EXPECTATIONS_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "cpi",
        "macro",
        "monthly",
        45 * 24 * 60 * 60,
        "BLS",
        (),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "ppi",
        "macro",
        "monthly",
        45 * 24 * 60 * 60,
        "BLS",
        (),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "pce",
        "macro",
        "monthly",
        45 * 24 * 60 * 60,
        "BEA",
        (),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "gdp",
        "macro",
        "quarterly",
        120 * 24 * 60 * 60,
        "BEA",
        (),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "employment",
        "macro",
        "monthly",
        45 * 24 * 60 * 60,
        "BLS",
        (),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "wages",
        "macro",
        "monthly",
        45 * 24 * 60 * 60,
        "BLS",
        (),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "nfp",
        "macro",
        "monthly",
        45 * 24 * 60 * 60,
        "BLS",
        (),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "jobless_claims",
        "macro",
        "weekly",
        14 * 24 * 60 * 60,
        "FRED",
        (),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "macro_calendar",
        "calendar",
        "event",
        7 * 24 * 60 * 60,
        "CANONICAL_EVENT_REPOSITORY",
        ("INVESTING_ECONOMIC_CALENDAR", "XTB"),
        "CANONICAL_EVENT_REPOSITORY",
        (),
    ),
    DatasetSourcePolicy(
        "flash_services_pmi",
        "calendar",
        "monthly",
        45 * 24 * 60 * 60,
        "SPGLOBAL",
        ("INVESTING_EVENT_1062",),
        "CANONICAL_EVENT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "earnings",
        "earnings",
        "event",
        14 * 24 * 60 * 60,
        "NASDAQ",
        ("FMP_EARNINGS_CALENDAR",),
        "MARKET_FACT_REPOSITORY",
        (),
    ),
    DatasetSourcePolicy(
        "options_positioning",
        "options_positioning",
        "intraday",
        2 * 60 * 60,
        "TRADIER",
        (),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "positioning",
        "positioning",
        "weekly",
        10 * 24 * 60 * 60,
        "CFTC",
        (),
        "MARKET_FACT_REPOSITORY",
    ),
    DatasetSourcePolicy(
        "current_news",
        "news",
        "intraday",
        24 * 60 * 60,
        "ALPHA_VANTAGE_NEWS_SENTIMENT",
        (
            "GDELT_DOC_API",
            "FEDERAL_RESERVE_RSS",
            "BLS_RSS",
            "BEA_RSS",
            "YAHOO_FINANCE_RSS",
            "MARKETWATCH_RSS",
            "GOOGLE_NEWS_RSS",
        ),
        "MARKET_NEWS_REPOSITORY",
        (),
        provider_strategy="FAN_IN",
    ),
    DatasetSourcePolicy(
        "market_schedule",
        "market_schedule",
        "event",
        370 * 24 * 60 * 60,
        "NASDAQ_MARKET_INFO",
        ("CME", "INVESTING_HOLIDAYS", "MARKETBEAT"),
        "MARKET_FACT_REPOSITORY",
        provider_strategy="FAN_IN",
    ),
)


_AUTHORITATIVE_CAPTURE = {
    "snapshot_revision": 99,
    "generated_at": "2026-07-30T11:57:24.569801+00:00",
    "run_id": "20260730T115633Z",
    "path": (
        "data/senior-analyst-live-validation/20260730T115633Z/"
        "response-body.json"
    ),
    "http_status": 200,
    "body_size_bytes": 116469,
    "body_sha256": (
        "9a768dd051d5375177723d67ce3c60630b892505b12bb535e883100b7f184835"
    ),
    "body_sha256_scope": "EXACT_HTTP_RESPONSE_BODY_BYTES",
    "gate_status": "FAIL",
    "provider_accounting_valid": False,
}


def provider_by_id(provider_id: str) -> ProviderRegistration:
    normalized = str(provider_id or "").strip().upper()
    for provider in PROVIDER_REGISTRY:
        if provider.provider_id == normalized:
            return provider
    raise KeyError(normalized)


def automatic_ai_delivery_authorized(
    *,
    dataset_id: str | None = None,
    metric_id: str | None = None,
    fields: Iterable[str] = (),
) -> bool:
    """Authorize automatic AI delivery only through an exact policy binding.

    Generic enable flags, research profiles, and provider availability are not
    capability certification. Callers that cannot supply the concrete
    dataset/metric/field identity therefore fail closed.
    """

    normalized_dataset = str(dataset_id or "").strip()
    normalized_metric = str(metric_id or "").strip()
    required_fields = frozenset(
        str(field_name).strip()
        for field_name in fields
        if str(field_name).strip()
    )
    if not normalized_dataset or not normalized_metric or not required_fields:
        return False
    policies = tuple(
        policy
        for policy in DATASET_SOURCE_POLICIES
        if policy.dataset_id == normalized_dataset
    )
    for policy in policies:
        for provider_id in policy.ai_fallback_providers:
            try:
                provider = provider_by_id(provider_id)
            except KeyError:
                continue
            if "AI_FALLBACK" not in provider.allowed_roles:
                continue
            for capability in provider.capabilities:
                delivered_ids = capability_delivery_ids(capability)
                delivered_fields = {
                    delivered
                    for _source, delivered in capability_delivery_field_pairs(
                        capability
                    )
                }
                if (
                    capability.ai_eligible
                    and capability.dataset_id == normalized_dataset
                    and normalized_metric in delivered_ids
                    and required_fields.issubset(delivered_fields)
                ):
                    return True
    return False


def effective_capture_mode(
    provider: ProviderRegistration | Any,
    settings: Any = None,
) -> str:
    """Resolve the registered acquisition mechanism for this audit request."""

    provider_id = str(
        getattr(provider, "provider_id", None)
        or (
            provider.get("provider_id")
            if isinstance(provider, dict)
            else ""
        )
        or ""
    ).upper()
    declared = str(
        getattr(provider, "capture_mode", None)
        or (
            provider.get("capture_mode")
            if isinstance(provider, dict)
            else ""
        )
        or ""
    ).upper()
    if provider_id != "AI_RESEARCHER":
        return declared or "HTTPX"
    mode = str(
        (
            settings.get("ai_researcher_mode")
            if isinstance(settings, dict)
            else getattr(settings, "ai_researcher_mode", None)
        )
        or "codex_cli"
    ).strip().lower()
    return {
        "codex_cli": "SUBPROCESS",
        "openai_api": "HTTPX",
    }.get(mode, "UNKNOWN")


def capability_delivery_ids(
    capability: CapabilityRegistration,
) -> frozenset[str]:
    """Return the explicit delivered-capability relation for policy checks."""

    return frozenset(
        (
            (capability.delivery_capability_id,)
            if capability.delivery_capability_id
            else capability.delivery_capability_ids
            or capability.canonical_metric_ids
            or (capability.metric_id,)
        )
    )


def capability_delivery_field_pairs(
    capability: CapabilityRegistration,
) -> tuple[tuple[str, str], ...]:
    """Return explicit raw-to-delivery fields, defaulting only by identity."""

    return capability.delivery_field_map or tuple(
        (field_name, field_name) for field_name in capability.supported_fields
    )


def capability_delivery_keys(
    capability: CapabilityRegistration,
) -> frozenset[tuple[str, str]]:
    """Return exact delivery metric+field identities for policy relations."""

    return frozenset(
        (delivery_id, delivery_field)
        for delivery_id, (_, delivery_field) in product(
            capability_delivery_ids(capability),
            capability_delivery_field_pairs(capability),
        )
    )


def provider_default_runtime_metric_ids(provider_id: str) -> tuple[str, ...]:
    """Return metrics emitted by the provider's default/main runtime adapter.

    Capabilities with an explicit probe adapter belong to that leaf adapter and
    must not leak into the main provider's no-argument acquisition.
    """

    provider = provider_by_id(provider_id)
    return tuple(
        metric_id
        for capability in provider.capabilities
        if (capability.probe_adapter_path or provider.adapter_path)
        == provider.adapter_path
        for metric_id in (
            item.strip()
            for item in capability.metric_id.split(",")
            if item.strip()
        )
    )


def dataset_policy_by_id(dataset_id: str) -> DatasetSourcePolicy:
    normalized = str(dataset_id or "").strip()
    for policy in DATASET_SOURCE_POLICIES:
        if policy.dataset_id == normalized:
            return policy
    raise KeyError(normalized)


def dataset_runtime_provider_order(
    dataset_id: str,
    mapped_provider_ids: Iterable[str],
) -> tuple[str, ...]:
    """Return the policy order after proving the runtime mapping is registry-bound."""

    policy = dataset_policy_by_id(dataset_id)
    provider_ids = (
        policy.primary_provider,
        *policy.fallback_providers,
    )
    mapped = tuple(str(provider_id).strip().upper() for provider_id in mapped_provider_ids)
    if (
        len(provider_ids) != len(set(provider_ids))
        or len(mapped) != len(set(mapped))
        or set(provider_ids) != set(mapped)
    ):
        raise RuntimeError(
            f"RUNTIME_POLICY_MAPPING_MISMATCH:{dataset_id}:{','.join(provider_ids)}"
        )
    for provider_id in provider_ids:
        registration = provider_by_id(provider_id)
        if not any(
            capability.dataset_id == dataset_id
            for capability in registration.capabilities
        ):
            raise RuntimeError(
                f"RUNTIME_PROVIDER_CAPABILITY_MISSING:{dataset_id}:{provider_id}"
            )
    return provider_ids


def macro_runtime_provider_ids() -> tuple[str, ...]:
    """Derive MacroService providers from macro dataset policies and registry order."""

    policies = tuple(
        policy
        for policy in DATASET_SOURCE_POLICIES
        if policy.section == "macro"
    )
    dataset_ids = {policy.dataset_id for policy in policies}
    expected = {policy.primary_provider for policy in policies}
    ordered = tuple(
        provider.provider_id
        for provider in PROVIDER_REGISTRY
        if provider.provider_id in expected
        and any(
            capability.dataset_id in dataset_ids
            for capability in provider.capabilities
        )
    )
    if len(ordered) != len(expected) or set(ordered) != expected:
        raise RuntimeError(
            "MACRO_RUNTIME_REGISTRY_MAPPING_MISMATCH:"
            f"{','.join(sorted(expected))}"
        )
    return ordered


AUTHORITATIVE_CALENDAR_METRIC_IDS = frozenset(
    {
        "fomc_occurrence",
        "bls_release_occurrence",
        "bea_release_occurrence",
    }
)


def event_runtime_adapter_bindings() -> tuple[tuple[str, str | None], ...]:
    """Derive EventService adapter bindings from occurrence capabilities."""

    bindings: list[tuple[str, str | None]] = []
    for provider in PROVIDER_REGISTRY:
        capabilities = tuple(
            capability
            for capability in provider.capabilities
            if capability.dataset_id == "macro_calendar"
            and (
                capability.metric_id in AUTHORITATIVE_CALENDAR_METRIC_IDS
                or (
                    capability.metric_id == "occurrence_enrichment"
                    and "LEGACY_FALLBACK" in provider.allowed_roles
                )
            )
        )
        if not capabilities:
            continue
        explicit_paths = tuple(
            dict.fromkeys(
                capability.probe_adapter_path
                for capability in capabilities
                if capability.probe_adapter_path
            )
        )
        paths = explicit_paths or (
            provider.adapter_path,
            *provider.additional_adapter_paths,
        )
        bindings.extend(
            _runtime_adapter_binding(provider, path)
            for path in paths
        )
    return tuple(dict.fromkeys(bindings))


def authoritative_calendar_adapter_identities() -> MappingProxyType:
    """Return canonical calendar source identities from runtime bindings.

    The authoritative provider set comes from the registered macro-schedule
    capabilities. Adapter names and source labels come from those same runtime
    classes, so consumers cannot maintain a second BEA/BLS/etc. identity list.
    """

    identities: dict[str, MappingProxyType] = {}
    for provider in PROVIDER_REGISTRY:
        for capability in provider.capabilities:
            if (
                capability.dataset_id != "macro_calendar"
                or capability.metric_id
                not in AUTHORITATIVE_CALENDAR_METRIC_IDS
            ):
                continue
            adapter_path = (
                capability.probe_adapter_path or provider.adapter_path
            )
            adapter = _resolve_adapter_path(adapter_path)
            if getattr(
                adapter,
                "authoritative_calendar_coverage",
                False,
            ) is not True:
                raise RuntimeError(
                    "AUTHORITATIVE_CALENDAR_RUNTIME_BINDING_INCOMPLETE:"
                    f"{provider.provider_id}"
                )
            canonical_source = str(
                getattr(adapter, "canonical_source", "") or ""
            ).strip()
            query_scope = str(
                getattr(adapter, "query_scope", "") or ""
            ).strip()
            if (
                not canonical_source
                or query_scope != capability.metric_id
                or canonical_source in identities
            ):
                raise RuntimeError(
                    "AUTHORITATIVE_CALENDAR_IDENTITY_INVALID:"
                    f"{provider.provider_id}:{capability.metric_id}"
                )
            identities[canonical_source] = MappingProxyType(
                {
                    "provider_id": provider.provider_id,
                    "adapter_path": adapter_path,
                    "adapter_name": adapter.__name__,
                    "source": str(
                        getattr(adapter, "source", "") or ""
                    ),
                    "canonical_source": canonical_source,
                    "query_scope": query_scope,
                    "publisher": str(provider.publisher or ""),
                }
            )
    if set(identities) != {"FED", "BLS", "BEA"}:
        raise RuntimeError(
            "AUTHORITATIVE_CALENDAR_IDENTITY_SET_INCOMPLETE:"
            f"{','.join(sorted(identities))}"
        )
    return MappingProxyType(identities)


def event_enrichment_runtime_adapter_bindings(
) -> tuple[tuple[str, str | None], ...]:
    """Derive the enrichment fallback order from registered roles/adapters."""

    candidates = tuple(
        provider
        for provider in PROVIDER_REGISTRY
        if any(
            capability.dataset_id == "macro_calendar"
            and (
                capability.metric_id == "occurrence_enrichment"
                or (
                    capability.metric_id == "occurrence_fields"
                    and "ENRICHMENT" in provider.allowed_roles
                )
            )
            for capability in provider.capabilities
        )
        and (
            "ENRICHMENT" in provider.allowed_roles
            or "AI_FALLBACK" in provider.allowed_roles
        )
    )
    base_paths: dict[str, str] = {}
    browser_paths: dict[str, tuple[str, ...]] = {}
    for provider in candidates:
        paths = (
            provider.adapter_path,
            *provider.additional_adapter_paths,
        )
        enrichment_paths = tuple(
            path
            for path in paths
            if path.startswith("app.providers.event_enrichment:")
        )
        non_browser = tuple(
            path
            for path in enrichment_paths
            if not _runtime_adapter_name(path).startswith("Playwright")
        )
        if not non_browser:
            raise RuntimeError(
                "ENRICHMENT_RUNTIME_ADAPTER_MISSING:"
                f"{provider.provider_id}"
            )
        base_paths[provider.provider_id] = non_browser[0]
        browser_paths[provider.provider_id] = tuple(
            path
            for path in enrichment_paths
            if _runtime_adapter_name(path).startswith("Playwright")
        )

    tail = tuple(
        provider
        for provider in candidates
        if provider.provider_type in {"MANUAL_FILE", "AI"}
        or provider.enable_setting == "enable_targeted_search_enrichment"
    )
    tail_ids = {provider.provider_id for provider in tail}
    browser_capable = tuple(
        sorted(
            (
                provider
                for provider in candidates
                if browser_paths[provider.provider_id]
                and provider.provider_id not in tail_ids
            ),
            key=lambda provider: (
                "FALLBACK" in provider.allowed_roles,
                _registry_provider_index(provider.provider_id),
            ),
        )
    )
    browser_capable_ids = {
        provider.provider_id for provider in browser_capable
    }
    standard = tuple(
        provider
        for provider in candidates
        if provider.provider_id not in tail_ids
        and provider.provider_id not in browser_capable_ids
    )
    ordered_bindings = [
        _runtime_adapter_binding(
            provider,
            base_paths[provider.provider_id],
        )
        for provider in (*browser_capable, *standard)
    ]
    ordered_bindings.extend(
        _runtime_adapter_binding(provider, path)
        for provider in browser_capable
        for path in browser_paths[provider.provider_id]
    )
    ordered_bindings.extend(
        _runtime_adapter_binding(
            provider,
            base_paths[provider.provider_id],
        )
        for provider in tail
    )
    return tuple(ordered_bindings)


def _runtime_adapter_binding(
    provider: ProviderRegistration,
    path: str,
) -> tuple[str, str | None]:
    if path not in {
        provider.adapter_path,
        *provider.additional_adapter_paths,
    }:
        raise RuntimeError(
            f"RUNTIME_ADAPTER_NOT_REGISTERED:{provider.provider_id}:{path}"
        )
    adapter_name = (
        None
        if path == provider.adapter_path
        else _runtime_adapter_name(path)
    )
    return provider.provider_id, adapter_name


def _runtime_adapter_name(path: str) -> str:
    _, separator, qualname = str(path).partition(":")
    if not separator or not qualname:
        raise RuntimeError(f"INVALID_RUNTIME_ADAPTER_PATH:{path}")
    return qualname.rsplit(".", 1)[-1]


def _registry_provider_index(provider_id: str) -> int:
    return next(
        index
        for index, provider in enumerate(PROVIDER_REGISTRY)
        if provider.provider_id == provider_id
    )


def runtime_adapter_paths() -> tuple[str, ...]:
    paths = {
        path
        for provider in PROVIDER_REGISTRY
        if provider.runtime_adapter
        for path in (
            provider.adapter_path,
            *provider.additional_adapter_paths,
        )
    }
    return tuple(sorted(paths))


def capability_source_adapter_paths() -> tuple[str, ...]:
    from app.services.provider_capability_contracts import (
        LOCAL_PROBE_ADAPTER_PATH,
        PROBE_OUTPUT_CONTRACTS,
    )

    paths = {
        path
        for provider in PROVIDER_REGISTRY
        for path in (
            provider.adapter_path,
            *provider.additional_adapter_paths,
            *(
                capability.probe_adapter_path
                for capability in provider.capabilities
                if capability.probe_adapter_path
                and capability.probe_adapter_path
                != LOCAL_PROBE_ADAPTER_PATH
            ),
        )
    }
    paths.update(
        path
        for contract in PROBE_OUTPUT_CONTRACTS.values()
        for path in contract.source_adapter_paths
    )
    return tuple(sorted(paths))


def discover_capability_source_adapter_paths() -> tuple[str, ...]:
    from app.services.provider_capability_contracts import (
        discover_capability_source_adapter_paths as discover,
    )

    return discover()


def discover_runtime_adapter_paths() -> tuple[str, ...]:
    import app.providers as provider_package

    excluded = {
        "app.providers.base:BaseProvider",
        (
            "app.providers.event_enrichment:"
            "CalendarEnrichmentProvider"
        ),
        (
            "app.providers.event_enrichment:"
            "BrowserCalendarEnrichmentProvider"
        ),
    }
    discovered: set[str] = set()
    prefix = f"{provider_package.__name__}."
    for module_info in pkgutil.walk_packages(
        provider_package.__path__,
        prefix=prefix,
    ):
        module = importlib.import_module(module_info.name)
        for name, value in inspect.getmembers(module, inspect.isclass):
            path = f"{module.__name__}:{value.__qualname__}"
            if (
                name.endswith("Provider")
                and value.__module__ == module.__name__
                and path not in excluded
            ):
                discovered.add(path)
    discovered.update(RESEARCH_BACKEND_RUNTIME_ADAPTER_PATHS.values())
    discovered.update(RESEARCH_RUNTIME_COMPONENT_PATHS)
    return tuple(sorted(discovered))


def _runtime_series_alignment_errors(
    provider: ProviderRegistration,
    adapter_type: Any,
) -> tuple[str, ...]:
    default_ids_resolver = getattr(
        adapter_type,
        "runtime_default_series_ids",
        None,
    )
    if not callable(default_ids_resolver):
        return ()
    try:
        runtime_defaults = tuple(default_ids_resolver())
    except Exception as exc:
        return (
            "runtime_default_series_resolution_failed:"
            f"{provider.provider_id}:{type(exc).__name__}",
        )
    if any(not isinstance(item, str) or not item.strip() for item in runtime_defaults):
        return (f"runtime_default_series_invalid:{provider.provider_id}",)

    registered_defaults = {
        capability.metric_id
        for capability in provider.capabilities
        if (capability.probe_adapter_path or provider.adapter_path)
        == provider.adapter_path
    }
    errors: list[str] = []
    runtime_minus_registry = sorted(set(runtime_defaults) - registered_defaults)
    if runtime_minus_registry:
        errors.append(
            "runtime_default_series_not_registered:"
            f"{provider.provider_id}:{','.join(runtime_minus_registry)}"
        )
    if len(runtime_defaults) != len(set(runtime_defaults)):
        errors.append(
            f"runtime_default_series_duplicated:{provider.provider_id}"
        )

    supported_ids_resolver = getattr(
        adapter_type,
        "runtime_supported_series_ids",
        None,
    )
    if callable(supported_ids_resolver):
        try:
            runtime_supported = set(supported_ids_resolver())
        except Exception as exc:
            errors.append(
                "runtime_supported_series_resolution_failed:"
                f"{provider.provider_id}:{type(exc).__name__}"
            )
        else:
            unsupported_registered = sorted(
                registered_defaults - runtime_supported
            )
            if unsupported_registered:
                errors.append(
                    "registered_default_series_not_supported:"
                    f"{provider.provider_id}:"
                    f"{','.join(unsupported_registered)}"
                )
    return tuple(errors)


def validate_registry(*, raise_on_error: bool = True) -> tuple[str, ...]:
    errors: list[str] = []
    _, baseline_anchor_error = _last_live_baseline_bytes()
    if baseline_anchor_error is not None:
        errors.append(baseline_anchor_error)
    provider_ids = [provider.provider_id for provider in PROVIDER_REGISTRY]
    policy_ids = [policy.dataset_id for policy in DATASET_SOURCE_POLICIES]
    duplicates = sorted(
        provider_id
        for provider_id, count in Counter(provider_ids).items()
        if count > 1
    )
    if duplicates:
        errors.append(f"duplicate_provider_ids:{','.join(duplicates)}")
    duplicate_policies = sorted(
        dataset_id
        for dataset_id, count in Counter(policy_ids).items()
        if count > 1
    )
    if duplicate_policies:
        errors.append(
            f"duplicate_dataset_policy_ids:{','.join(duplicate_policies)}"
        )
    if len(DATASET_SOURCE_POLICIES) != 25:
        errors.append(
            "senior_analyst_dataset_policy_count:"
            f"{len(DATASET_SOURCE_POLICIES)}"
        )

    by_id = {provider.provider_id: provider for provider in PROVIDER_REGISTRY}
    registered_runtime = set(runtime_adapter_paths())
    discovered_runtime = set(discover_runtime_adapter_paths())
    registered_sources = set(capability_source_adapter_paths())
    discovered_sources = set(discover_capability_source_adapter_paths())
    request_groups: Counter[tuple[str, str, str]] = Counter()
    provider_dataset_metric_fields: Counter[
        tuple[str, str, str, str]
    ] = Counter()
    for path in sorted(discovered_runtime - registered_runtime):
        errors.append(f"unregistered_runtime_provider:{path}")
    for path in sorted(registered_runtime - discovered_runtime):
        errors.append(f"registered_runtime_provider_not_discovered:{path}")
    for path in sorted(discovered_sources - registered_sources):
        errors.append(f"unregistered_capability_source_adapter:{path}")
    for path in sorted(registered_sources - discovered_sources):
        errors.append(f"registered_capability_source_not_discovered:{path}")

    from app.services.provider_capability_contracts import (
        validate_capability_output_contracts,
    )

    errors.extend(validate_capability_output_contracts(PROVIDER_REGISTRY))
    for provider in PROVIDER_REGISTRY:
        if not provider.provider_id or provider.provider_id != provider.provider_id.upper():
            errors.append(f"invalid_provider_id:{provider.provider_id}")
        expected_capture_mode = (
            "SUBPROCESS"
            if provider.provider_id
            in {"AI_RESEARCHER", "CODEX_CLI_RESEARCH_BACKEND"}
            else (
                "LOCAL_SANDBOX"
                if provider.provider_type
                in {
                    "MANUAL_FILE",
                    "RECONCILIATION",
                    "REPOSITORY",
                    "TRANSFORMATION",
                }
                else "HTTPX"
            )
        )
        if provider.capture_mode != expected_capture_mode:
            errors.append(
                "provider_capture_mode_mismatch:"
                f"{provider.provider_id}:{provider.capture_mode}:"
                f"{expected_capture_mode}"
            )
        if not provider.probe_id:
            errors.append(f"provider_without_probe:{provider.provider_id}")
        if not provider.probe_enabled:
            errors.append(f"provider_probe_disabled:{provider.provider_id}")
        if provider.max_attempts < 1:
            errors.append(f"provider_invalid_attempts:{provider.provider_id}")
        if provider.audit_leaf_request_count is not None and (
            isinstance(provider.audit_leaf_request_count, bool)
            or not isinstance(provider.audit_leaf_request_count, int)
            or provider.audit_leaf_request_count < 1
        ):
            errors.append(
                "provider_invalid_audit_leaf_request_count:"
                f"{provider.provider_id}:"
                f"{provider.audit_leaf_request_count}"
            )
        if (
            isinstance(provider.audit_source_url_budget, bool)
            or not isinstance(provider.audit_source_url_budget, int)
            or provider.audit_source_url_budget < 0
        ):
            errors.append(
                "provider_invalid_audit_source_url_budget:"
                f"{provider.provider_id}:"
                f"{provider.audit_source_url_budget}"
            )
        if bool(provider.configuration_setting) != bool(
            provider.configuration_value
        ):
            errors.append(
                "provider_incomplete_configuration_selector:"
                f"{provider.provider_id}"
            )
        if not provider.allowed_roles:
            errors.append(f"provider_without_role:{provider.provider_id}")
        if provider.terminal_audit_reason:
            if provider.allowed_roles != ("AUDIT_ONLY",):
                errors.append(
                    "terminal_provider_runtime_role_authorized:"
                    f"{provider.provider_id}"
                )
            if any(
                capability.ai_eligible
                for capability in provider.capabilities
            ):
                errors.append(
                    "terminal_provider_ai_eligible:"
                    f"{provider.provider_id}"
                )
        if not provider.capabilities:
            errors.append(f"provider_without_capability:{provider.provider_id}")
        main_adapter_type: Any | None = None
        for path in (
            provider.adapter_path,
            *provider.additional_adapter_paths,
        ):
            try:
                adapter_type = _resolve_adapter_path(path)
                if path == provider.adapter_path:
                    main_adapter_type = adapter_type
            except (AttributeError, ImportError, ValueError) as exc:
                errors.append(
                    "provider_adapter_not_importable:"
                    f"{provider.provider_id}:{path}:{type(exc).__name__}"
                )
        for leaf_path in provider.uncertified_runtime_leaves:
            try:
                leaf = _resolve_adapter_path(leaf_path)
                if not callable(leaf):
                    raise TypeError("runtime leaf is not callable")
                errors.append(
                    "provider_uncertified_runtime_leaf_callable:"
                    f"{provider.provider_id}:{leaf_path}"
                )
            except (AttributeError, ImportError, TypeError, ValueError) as exc:
                errors.append(
                    "provider_uncertified_runtime_leaf_not_importable:"
                    f"{provider.provider_id}:{leaf_path}:"
                    f"{type(exc).__name__}"
                )
            if leaf_path in {
                provider.adapter_path,
                *provider.additional_adapter_paths,
            }:
                errors.append(
                    "provider_uncertified_runtime_leaf_duplicates_adapter:"
                    f"{provider.provider_id}:{leaf_path}"
                )
        if main_adapter_type is not None:
            errors.extend(
                _runtime_series_alignment_errors(
                    provider,
                    main_adapter_type,
                )
            )
        seen_capabilities: set[
            tuple[str, str, tuple[str, ...], str, str]
        ] = set()
        for capability in provider.capabilities:
            invalid_degradable_checks = sorted(
                set(capability.degradable_quality_checks)
                - {"completeness_valid", "freshness_valid"}
            )
            if invalid_degradable_checks:
                errors.append(
                    "capability_invalid_degradable_quality_checks:"
                    f"{provider.provider_id}:{capability.dataset_id}:"
                    f"{capability.metric_id}:"
                    f"{','.join(invalid_degradable_checks)}"
                )
            if capability.runtime_profile_id is not None:
                profile = PROFILES.get(capability.runtime_profile_id)
                if profile is None:
                    errors.append(
                        "capability_unknown_runtime_profile:"
                        f"{provider.provider_id}:"
                        f"{capability.runtime_profile_id}"
                    )
                elif (
                    capability.metric_id
                    != capability.runtime_profile_id.casefold()
                    or capability.runtime_required_fields
                    != profile.required_fields
                    or capability.runtime_source_domains
                    != profile.priority_domains
                    or capability.supported_fields
                    != (profile.required_fields or ("claims",))
                ):
                    errors.append(
                        "capability_runtime_profile_contract_drift:"
                        f"{provider.provider_id}:"
                        f"{capability.runtime_profile_id}"
                    )
                if not capability.runtime_enable_setting:
                    errors.append(
                        "capability_runtime_profile_enablement_missing:"
                        f"{provider.provider_id}:"
                        f"{capability.runtime_profile_id}"
                    )
                if (
                    not capability.runtime_job_type
                    or JOB_PROFILE.get(capability.runtime_job_type)
                    != capability.runtime_profile_id
                ):
                    errors.append(
                        "capability_runtime_job_type_invalid:"
                        f"{provider.provider_id}:"
                        f"{capability.runtime_profile_id}:"
                        f"{capability.runtime_job_type}"
                    )
            elif any(
                (
                    capability.runtime_topic,
                    capability.runtime_job_type,
                    capability.runtime_enable_setting,
                    capability.runtime_required_fields,
                    capability.runtime_source_domains,
                )
            ):
                errors.append(
                    "capability_runtime_profile_metadata_without_profile:"
                    f"{provider.provider_id}:"
                    f"{capability.dataset_id}:{capability.metric_id}"
                )
            if capability.dataset_id == "*":
                errors.append(
                    "capability_wildcard_dataset_not_atomic:"
                    f"{provider.provider_id}:{capability.metric_id}"
                )
            provider_dataset_metric_fields.update(
                (
                    provider.provider_id,
                    capability.dataset_id,
                    capability.metric_id,
                    field_name,
                )
                for field_name in capability.supported_fields
            )
            if capability.probe_adapter_path:
                try:
                    _resolve_adapter_path(capability.probe_adapter_path)
                except (AttributeError, ImportError, ValueError) as exc:
                    errors.append(
                        "capability_probe_adapter_not_importable:"
                        f"{provider.provider_id}:{capability.probe_id}:"
                        f"{type(exc).__name__}"
                    )
            identity = (
                capability.dataset_id,
                capability.metric_id,
                capability.supported_fields,
                capability.frequency,
                capability.transformation,
            )
            if identity in seen_capabilities:
                errors.append(
                    "duplicate_provider_capability:"
                    f"{provider.provider_id}:{capability.dataset_id}:"
                    f"{capability.metric_id}"
                )
            seen_capabilities.add(identity)
            if not capability.probe_id:
                errors.append(
                    "capability_without_probe:"
                    f"{provider.provider_id}:{capability.dataset_id}:"
                    f"{capability.metric_id}"
                )
            if not capability.field_validator_id:
                errors.append(
                    "capability_without_field_validator:"
                    f"{provider.provider_id}:{capability.dataset_id}:"
                    f"{capability.metric_id}"
                )
            else:
                validator_fields = FIELD_VALIDATOR_SCHEMAS.get(
                    capability.field_validator_id
                )
                if validator_fields is None:
                    errors.append(
                        "capability_unknown_field_validator:"
                        f"{provider.provider_id}:{capability.dataset_id}:"
                        f"{capability.metric_id}:"
                        f"{capability.field_validator_id}"
                    )
                else:
                    unsupported_fields = sorted(
                        {
                            *capability.supported_fields,
                            *capability.audit_only_fields,
                        }
                        - validator_fields
                    )
                    if unsupported_fields:
                        errors.append(
                            "capability_fields_not_validated:"
                            f"{provider.provider_id}:{capability.dataset_id}:"
                            f"{capability.metric_id}:"
                            f"{','.join(unsupported_fields)}"
                        )
                    missing_type_contracts = sorted(
                        field_name
                        for field_name in {
                            *capability.supported_fields,
                            *capability.audit_only_fields,
                        }
                        if field_value_type_contract(
                            capability.field_validator_id,
                            field_name,
                        )
                        is None
                    )
                    if missing_type_contracts:
                        errors.append(
                            "capability_fields_without_type_contract:"
                            f"{provider.provider_id}:{capability.dataset_id}:"
                            f"{capability.metric_id}:"
                            f"{','.join(missing_type_contracts)}"
                        )
                    measurement_contract = capability_measurement_contract(
                        capability
                    )
                    if measurement_contract not in {
                        *KNOWN_NUMERIC_MEASUREMENT_UNITS,
                        "categorical",
                        "mixed_numeric",
                        "mixed_structured",
                        "temporal",
                    }:
                        errors.append(
                            "capability_invalid_measurement_contract:"
                            f"{provider.provider_id}:{capability.dataset_id}:"
                            f"{capability.metric_id}:"
                            f"{measurement_contract}"
                        )
            if not capability.supported_fields:
                errors.append(
                    "capability_without_supported_fields:"
                    f"{provider.provider_id}:{capability.dataset_id}:"
                    f"{capability.metric_id}"
                )
            delivery_id = capability.delivery_capability_id
            if delivery_id is not None and (
                not isinstance(delivery_id, str)
                or not delivery_id.strip()
                or delivery_id.strip().casefold() == "none"
            ):
                errors.append(
                    "capability_invalid_delivery_capability_id:"
                    f"{provider.provider_id}:{capability.dataset_id}:"
                    f"{capability.metric_id}"
                )
            if (
                len(capability.delivery_capability_ids)
                != len(set(capability.delivery_capability_ids))
                or any(
                    not isinstance(item, str)
                    or not item.strip()
                    or item.strip().casefold() == "none"
                    for item in capability.delivery_capability_ids
                )
                or (
                    delivery_id is not None
                    and capability.delivery_capability_ids
                )
            ):
                errors.append(
                    "capability_invalid_delivery_capability_ids:"
                    f"{provider.provider_id}:{capability.dataset_id}:"
                    f"{capability.metric_id}"
                )
            delivery_sources: set[str] = set()
            delivery_targets: set[str] = set()
            for mapping in capability.delivery_field_map:
                if not isinstance(mapping, (tuple, list)) or len(mapping) != 2:
                    errors.append(
                        "capability_invalid_delivery_field_mapping:"
                        f"{provider.provider_id}:{capability.dataset_id}:"
                        f"{capability.metric_id}"
                    )
                    continue
                source_field, delivery_field = mapping
                if (
                    not isinstance(source_field, str)
                    or not source_field.strip()
                    or source_field not in capability.supported_fields
                    or not isinstance(delivery_field, str)
                    or not delivery_field.strip()
                ):
                    errors.append(
                        "capability_invalid_delivery_field_mapping:"
                        f"{provider.provider_id}:{capability.dataset_id}:"
                        f"{capability.metric_id}:"
                        f"{source_field}:{delivery_field}"
                    )
                    continue
                if source_field in delivery_sources:
                    errors.append(
                        "capability_duplicate_delivery_source_field:"
                        f"{provider.provider_id}:{capability.dataset_id}:"
                        f"{capability.metric_id}:{source_field}"
                    )
                if delivery_field in delivery_targets:
                    errors.append(
                        "capability_duplicate_delivery_target_field:"
                        f"{provider.provider_id}:{capability.dataset_id}:"
                        f"{capability.metric_id}:{delivery_field}"
                    )
                delivery_sources.add(source_field)
                delivery_targets.add(delivery_field)
            if not capability.frequency or not capability.transformation:
                errors.append(
                    "capability_without_semantics:"
                    f"{provider.provider_id}:{capability.dataset_id}:"
                    f"{capability.metric_id}"
                )
            if (
                provider.provider_type == "AI"
                and capability.ai_eligible
                and capability.metric_id == "occurrence_enrichment"
                and not capability.canonical_metric_ids
            ):
                errors.append(
                    "ai_generic_capability_marked_certifiable:"
                    f"{provider.provider_id}:{capability.dataset_id}:"
                    f"{capability.metric_id}"
                )
            if provider.provider_type != "AI" and capability.ai_eligible:
                errors.append(
                    "non_ai_capability_marked_ai_eligible:"
                    f"{provider.provider_id}:{capability.dataset_id}:"
                    f"{capability.metric_id}"
                )
            if capability.ai_eligible and (
                "AI_FALLBACK" not in provider.allowed_roles
                or capability.dataset_id == "ai_research_runtime"
                or not any(
                    policy.dataset_id == capability.dataset_id
                    and provider.provider_id
                    in policy.ai_fallback_providers
                    for policy in DATASET_SOURCE_POLICIES
                )
            ):
                errors.append(
                    "ai_capability_not_concrete_policy_bound:"
                    f"{provider.provider_id}:{capability.dataset_id}:"
                    f"{capability.metric_id}"
                )
            request_group = capability.request_group or provider.request_group
            if request_group:
                request_groups[
                    (provider.provider_id, capability.probe_id, request_group)
                ] += 1

    for identity, count in sorted(provider_dataset_metric_fields.items()):
        if count > 1:
            errors.append(
                "duplicate_provider_dataset_metric_field:"
                f"{':'.join(identity)}:{count}"
            )

    official_by_metric = {
        specification.canonical_metric_id: specification
        for specification in OFFICIAL_METRIC_REGISTRATIONS
    }
    duplicate_official_metrics = sorted(
        metric_id
        for metric_id, count in Counter(
            specification.canonical_metric_id
            for specification in OFFICIAL_METRIC_REGISTRATIONS
        ).items()
        if count > 1
    )
    if duplicate_official_metrics:
        errors.append(
            "duplicate_official_metric_registrations:"
            f"{','.join(duplicate_official_metrics)}"
        )

    source_provider_ids = {
        specification.provider_id
        for specification in OFFICIAL_METRIC_REGISTRATIONS
    }
    source_relations: Counter[str] = Counter()
    ai_target_relations: Counter[str] = Counter()
    ai_field_relations: Counter[tuple[str, str]] = Counter()
    for provider in PROVIDER_REGISTRY:
        for capability in provider.capabilities:
            if capability.probe_query_id is not None and not (
                provider.provider_id == "CENSUS"
                and capability.metric_id
                in OFFICIAL_SOURCE_PROBE_QUERY_IDS
            ):
                errors.append(
                    "capability_probe_query_not_runtime_argument:"
                    f"{provider.provider_id}:{capability.dataset_id}:"
                    f"{capability.metric_id}:{capability.probe_query_id}"
                )
            for canonical_metric_id in capability.canonical_metric_ids:
                specification = official_by_metric.get(canonical_metric_id)
                if specification is None:
                    errors.append(
                        "canonical_capability_relation_unknown_metric:"
                        f"{provider.provider_id}:{capability.metric_id}:"
                        f"{canonical_metric_id}"
                    )
                    continue
                if provider.provider_id in source_provider_ids:
                    source_relations[canonical_metric_id] += 1
                    observed_identity = (
                        provider.provider_id,
                        capability.dataset_id,
                        capability.metric_id,
                        capability.frequency,
                        capability.transformation,
                        capability.probe_query_id,
                    )
                    expected_identity = (
                        specification.provider_id,
                        specification.dataset_id,
                        specification.source_series_id,
                        specification.frequency,
                        "identity",
                        OFFICIAL_SOURCE_PROBE_QUERY_IDS.get(
                            specification.source_series_id
                        ),
                    )
                    if observed_identity != expected_identity:
                        errors.append(
                            "official_source_relation_drift:"
                            f"{canonical_metric_id}:"
                            f"{'|'.join(str(item) for item in observed_identity)}"
                        )
                    derived_fields = {
                        "actual",
                        "reference_period",
                        "released_at",
                        "transformation",
                    }.intersection(capability.supported_fields)
                    if derived_fields:
                        errors.append(
                            "official_source_capability_claims_derived_fields:"
                            f"{provider.provider_id}:{capability.metric_id}:"
                            f"{','.join(sorted(derived_fields))}"
                        )
                    continue
                if provider.provider_id != "AI_RESEARCHER":
                    errors.append(
                        "canonical_capability_relation_unsupported_provider:"
                        f"{provider.provider_id}:{canonical_metric_id}"
                    )
                    continue

                ai_target_relations[canonical_metric_id] += 1
                ai_value_fields = {
                    "actual",
                    "consensus",
                    "forecast",
                    "previous",
                    "previous_revised",
                }.intersection(
                    {
                        *capability.supported_fields,
                        *capability.audit_only_fields,
                    }
                )
                ai_field_relations.update(
                    (canonical_metric_id, field_name)
                    for field_name in ai_value_fields
                )
                expected_dataset = (
                    "flash_services_pmi"
                    if canonical_metric_id == "flash_services_pmi"
                    else "macro_calendar"
                )
                observed_ai_identity = (
                    capability.dataset_id,
                    capability.metric_id,
                    capability.frequency,
                    capability.transformation,
                    capability.canonical_metric_ids,
                )
                expected_ai_identity = (
                    expected_dataset,
                    canonical_metric_id,
                    specification.frequency,
                    specification.transformation,
                    (canonical_metric_id,),
                )
                if observed_ai_identity != expected_ai_identity:
                    errors.append(
                        "ai_canonical_capability_drift:"
                        f"{canonical_metric_id}:"
                        f"{capability.dataset_id}|{capability.metric_id}|"
                        f"{capability.frequency}|"
                        f"{capability.transformation}"
                    )
                required_ai_fields = {
                    "consensus",
                    "previous",
                    "occurrence_id",
                    "reference_period",
                    "source",
                    "source_url",
                    "lineage",
                }
                if not required_ai_fields.issubset(
                    capability.supported_fields
                ):
                    errors.append(
                        "ai_canonical_capability_fields_incomplete:"
                        f"{canonical_metric_id}"
                    )
                if "forecast" in ai_value_fields:
                    errors.append(
                        "ai_forecast_without_distinct_canonical_field:"
                        f"{canonical_metric_id}"
                    )
                if (
                    "actual" in capability.supported_fields
                    or "actual" not in capability.audit_only_fields
                ):
                    errors.append(
                        "ai_actual_capability_scope_drift:"
                        f"{canonical_metric_id}"
                    )

    for canonical_metric_id in sorted(official_by_metric):
        relation_count = source_relations[canonical_metric_id]
        if relation_count != 1:
            errors.append(
                "official_source_relation_count:"
                f"{canonical_metric_id}:{relation_count}"
            )
        ai_target_count = ai_target_relations[canonical_metric_id]
        if ai_target_count != 1:
            errors.append(
                "ai_canonical_capability_count:"
                f"{canonical_metric_id}:{ai_target_count}"
            )

    expected_ai_field_relations = {
        (canonical_metric_id, field_name)
        for canonical_metric_id in official_by_metric
        for field_name in (
            "actual",
            "consensus",
            "previous",
            "previous_revised",
        )
    }
    observed_ai_field_relations = set(ai_field_relations)
    for relation in sorted(
        expected_ai_field_relations - observed_ai_field_relations
    ):
        errors.append(
            "ai_canonical_field_capability_missing:"
            f"{relation[0]}:{relation[1]}"
        )
    for relation in sorted(
        observed_ai_field_relations - expected_ai_field_relations
    ):
        errors.append(
            "ai_canonical_field_capability_unbounded:"
            f"{relation[0]}:{relation[1]}"
        )
    for relation, count in sorted(ai_field_relations.items()):
        if count != 1:
            errors.append(
                "ai_canonical_field_capability_count:"
                f"{relation[0]}:{relation[1]}:{count}"
            )

    transformation_provider = by_id.get("OFFICIAL_ACTUAL_TRANSFORMATION")
    transformation_capabilities = (
        transformation_provider.capabilities
        if transformation_provider is not None
        else ()
    )
    transformation_counts = Counter(
        capability.metric_id
        for capability in transformation_capabilities
    )
    for canonical_metric_id, specification in sorted(
        official_by_metric.items()
    ):
        count = transformation_counts[canonical_metric_id]
        if count != 1:
            errors.append(
                "official_transformation_capability_count:"
                f"{canonical_metric_id}:{count}"
            )
            continue
        capability = next(
            item
            for item in transformation_capabilities
            if item.metric_id == canonical_metric_id
        )
        observed_semantics = (
            capability.dataset_id,
            capability.frequency,
            capability.transformation,
        )
        expected_semantics = (
            specification.dataset_id,
            specification.frequency,
            specification.transformation,
        )
        if observed_semantics != expected_semantics:
            errors.append(
                "official_transformation_capability_drift:"
                f"{canonical_metric_id}:"
                f"{'|'.join(observed_semantics)}"
            )
    for metric_id in sorted(
        set(transformation_counts) - set(official_by_metric)
    ):
        errors.append(
            f"official_transformation_unknown_metric:{metric_id}"
        )

    for (provider_id, probe_id, request_group), count in sorted(
        request_groups.items()
    ):
        if count < 2:
            errors.append(
                "request_group_without_shared_acquisition:"
                f"{provider_id}:{probe_id}:{request_group}"
            )

    market_fact_policy_datasets = {
        policy.dataset_id
        for policy in DATASET_SOURCE_POLICIES
        if policy.canonical_repository == "MARKET_FACT_REPOSITORY"
    }
    market_fact_query_datasets = set(
        MARKET_FACT_REPOSITORY_DATASET_QUERIES
    )
    market_fact_provider = by_id.get("MARKET_FACT_REPOSITORY")
    market_fact_capabilities = (
        market_fact_provider.capabilities
        if market_fact_provider is not None
        else ()
    )
    market_fact_capability_datasets = {
        capability.dataset_id
        for capability in market_fact_capabilities
    }
    for label, observed in (
        ("query", market_fact_query_datasets),
        ("capability", market_fact_capability_datasets),
    ):
        for dataset_id in sorted(market_fact_policy_datasets - observed):
            errors.append(
                "market_fact_repository_dataset_"
                f"{label}_missing:{dataset_id}"
            )
        for dataset_id in sorted(observed - market_fact_policy_datasets):
            errors.append(
                "market_fact_repository_dataset_"
                f"{label}_unbounded:{dataset_id}"
            )
    if any(
        capability.dataset_id == "*"
        for capability in market_fact_capabilities
    ):
        errors.append("market_fact_repository_wildcard_capability")
    for capability in market_fact_capabilities:
        query = MARKET_FACT_REPOSITORY_DATASET_QUERIES.get(
            capability.dataset_id
        )
        if query is None:
            continue
        if capability.frequency != query.frequency:
            errors.append(
                "market_fact_repository_frequency_drift:"
                f"{capability.dataset_id}:{capability.frequency}"
            )

    for policy in DATASET_SOURCE_POLICIES:
        if policy.sla_seconds <= 0:
            errors.append(f"dataset_policy_invalid_sla:{policy.dataset_id}")
        if policy.provider_strategy not in {
            "FALLBACK",
            "CASCADE",
            "FAN_IN",
        }:
            errors.append(
                "dataset_policy_invalid_provider_strategy:"
                f"{policy.dataset_id}:{policy.provider_strategy}"
            )
        chain = (policy.primary_provider, *policy.fallback_providers)
        if len(chain) != len(set(chain)):
            errors.append(f"dataset_policy_duplicate_chain:{policy.dataset_id}")
        for role, provider_id in (
            ("primary", policy.primary_provider),
            *(("fallback", item) for item in policy.fallback_providers),
            *(("ai_fallback", item) for item in policy.ai_fallback_providers),
            ("repository", policy.canonical_repository),
        ):
            provider = by_id.get(provider_id)
            if provider is None:
                errors.append(
                    f"dataset_policy_unregistered_{role}:"
                    f"{policy.dataset_id}:{provider_id}"
                )
                continue
            if provider.terminal_audit_reason:
                errors.append(
                    "dataset_policy_terminal_provider_authorized:"
                    f"{policy.dataset_id}:{provider_id}:{role}"
                )
            if role == "repository" and provider.provider_type != "REPOSITORY":
                errors.append(
                    "dataset_policy_non_repository:"
                    f"{policy.dataset_id}:{provider_id}"
                )
            if role == "ai_fallback" and provider.provider_type != "AI":
                errors.append(
                    "dataset_policy_non_ai_fallback:"
                    f"{policy.dataset_id}:{provider_id}"
                )
            if role in {
                "primary",
                "fallback",
                "ai_fallback",
                "repository",
            } and not any(
                capability.dataset_id == policy.dataset_id
                for capability in provider.capabilities
            ):
                missing_role = (
                    "repository_capability"
                    if role == "repository"
                    else "provider_capability"
                )
                errors.append(
                    f"dataset_policy_{missing_role}_missing:"
                    f"{policy.dataset_id}:{provider_id}"
                )
            if role == "ai_fallback" and not any(
                capability.dataset_id == policy.dataset_id
                and capability.ai_eligible
                for capability in provider.capabilities
            ):
                errors.append(
                    "dataset_policy_ai_capability_not_certifiable:"
                    f"{policy.dataset_id}:{provider_id}"
                )
        if (
            policy.provider_strategy == "FALLBACK"
            and (
                policy.fallback_providers
                or policy.ai_fallback_providers
            )
            and policy.primary_provider in by_id
        ):
            primary_delivery_keys = {
                delivery_key
                for capability in by_id[
                    policy.primary_provider
                ].capabilities
                if capability.dataset_id == policy.dataset_id
                for delivery_key in capability_delivery_keys(
                    capability
                )
            }
            for fallback_id in (
                *policy.fallback_providers,
                *policy.ai_fallback_providers,
            ):
                fallback = by_id.get(fallback_id)
                if fallback is None:
                    continue
                fallback_delivery_keys = {
                    delivery_key
                    for capability in fallback.capabilities
                    if capability.dataset_id
                    == policy.dataset_id
                    for delivery_key in capability_delivery_keys(
                        capability
                    )
                }
                if not primary_delivery_keys.intersection(
                    fallback_delivery_keys
                ):
                    errors.append(
                        "dataset_policy_fallback_capability_disjoint:"
                        f"{policy.dataset_id}:"
                        f"{policy.primary_provider}:{fallback_id}"
                    )

    expected_backend_providers = {
        "CODEX_CLI_RESEARCH_BACKEND": "codex_cli",
        "OPENAI_RESPONSES_RESEARCH": "openai_api",
    }
    expected_profile_ids = set(PROFILES)
    for provider_id, backend_name in expected_backend_providers.items():
        provider = by_id.get(provider_id)
        if provider is None:
            errors.append(f"research_backend_provider_missing:{provider_id}")
            continue
        if (
            provider.adapter_path
            != RESEARCH_BACKEND_RUNTIME_ADAPTER_PATHS[backend_name]
            or provider.configuration_setting != "research_backend"
            or provider.configuration_value != backend_name
        ):
            errors.append(
                f"research_backend_provider_wiring_drift:{provider_id}"
            )
        observed_profile_ids = [
            capability.runtime_profile_id
            for capability in provider.capabilities
            if capability.runtime_profile_id is not None
        ]
        if (
            len(observed_profile_ids) != len(expected_profile_ids)
            or set(observed_profile_ids) != expected_profile_ids
        ):
            errors.append(
                f"research_backend_profile_coverage_drift:{provider_id}"
            )
    registered_agent_profiles = {
        registration.profile_id
        for registration in RESEARCH_AGENT_REGISTRY
    }
    if not registered_agent_profiles.issubset(expected_profile_ids):
        errors.append("research_agent_registry_profile_drift")

    result = tuple(sorted(set(errors)))
    if result and raise_on_error:
        raise RuntimeError("invalid_provider_registry:" + "|".join(result))
    return result


def registry_summary() -> dict[str, Any]:
    provider_types = Counter(
        provider.provider_type for provider in PROVIDER_REGISTRY
    )
    capture_modes = Counter(
        provider.capture_mode for provider in PROVIDER_REGISTRY
    )
    capabilities = [
        capability
        for provider in PROVIDER_REGISTRY
        for capability in provider.capabilities
    ]
    registered_runtime = set(runtime_adapter_paths())
    discovered_runtime = set(discover_runtime_adapter_paths())
    registered_sources = set(capability_source_adapter_paths())
    discovered_sources = set(discover_capability_source_adapter_paths())
    unregistered_runtime = sorted(discovered_runtime - registered_runtime)
    undiscovered_registrations = sorted(
        registered_runtime - discovered_runtime
    )
    unregistered_sources = sorted(discovered_sources - registered_sources)
    undiscovered_sources = sorted(registered_sources - discovered_sources)
    from app.services.provider_capability_contracts import (
        PROBE_OUTPUT_CONTRACTS,
        validate_capability_output_contracts,
    )

    output_contract_errors = validate_capability_output_contracts(
        PROVIDER_REGISTRY
    )
    capabilities_without_probe = [
        (
            provider.provider_id,
            capability.dataset_id,
            capability.metric_id,
        )
        for provider in PROVIDER_REGISTRY
        for capability in provider.capabilities
        if not capability.probe_id
    ]
    provider_probe_ids = {
        provider.probe_id
        for provider in PROVIDER_REGISTRY
        if provider.probe_id
    }
    capability_probe_ids = {
        capability.probe_id
        for capability in capabilities
        if capability.probe_id
    }
    request_groups = sorted(
        {
            (
                provider.provider_id,
                capability.probe_id,
                capability.request_group or provider.request_group,
            )
            for provider in PROVIDER_REGISTRY
            for capability in provider.capabilities
            if capability.request_group or provider.request_group
        }
    )
    validation_errors = validate_registry(raise_on_error=False)
    return {
        "providers_registered": len(PROVIDER_REGISTRY),
        "runtime_adapters_registered": len(registered_runtime),
        "runtime_adapters_discovered": len(discovered_runtime),
        "unregistered_runtime_providers": len(unregistered_runtime),
        "unregistered_runtime_provider_paths": unregistered_runtime,
        "registered_runtime_providers_not_discovered": len(
            undiscovered_registrations
        ),
        "registered_runtime_provider_paths_not_discovered": (
            undiscovered_registrations
        ),
        "source_adapters_registered": len(registered_sources),
        "source_adapters_discovered": len(discovered_sources),
        "unregistered_source_adapters": len(unregistered_sources),
        "unregistered_source_adapter_paths": unregistered_sources,
        "registered_source_adapters_not_discovered": len(
            undiscovered_sources
        ),
        "registered_source_adapter_paths_not_discovered": (
            undiscovered_sources
        ),
        "dataset_policies_registered": len(DATASET_SOURCE_POLICIES),
        "official_metrics_registered": len(
            OFFICIAL_METRIC_REGISTRATIONS
        ),
        "capabilities_registered": len(capabilities),
        "capabilities_without_probe": len(capabilities_without_probe),
        "uncertified_runtime_leaves_registered": sum(
            len(provider.uncertified_runtime_leaves)
            for provider in PROVIDER_REGISTRY
        ),
        "uncertified_runtime_leaf_ids": sorted(
            f"{provider.provider_id}:{leaf}"
            for provider in PROVIDER_REGISTRY
            for leaf in provider.uncertified_runtime_leaves
        ),
        "capabilities_without_probe_ids": [
            ":".join(identity)
            for identity in capabilities_without_probe
        ],
        "provider_probe_ids_registered": len(provider_probe_ids),
        "capability_probe_ids_registered": len(capability_probe_ids),
        "field_validator_schemas_registered": len(FIELD_VALIDATOR_SCHEMAS),
        "probe_output_contracts_registered": len(
            PROBE_OUTPUT_CONTRACTS
        ),
        "capability_output_contract_coverage": (
            100 if not output_contract_errors else 0
        ),
        "request_groups_registered": len(request_groups),
        "request_group_ids": [
            ":".join((provider_id, probe_id, request_group))
            for provider_id, probe_id, request_group in request_groups
        ],
        "probe_coverage": (
            100
            if all(provider.probe_id for provider in PROVIDER_REGISTRY)
            and not capabilities_without_probe
            else 0
        ),
        "providers_by_type": dict(sorted(provider_types.items())),
        "providers_by_capture_mode": dict(
            sorted(capture_modes.items())
        ),
        "ai_providers_registered": sum(
            provider.provider_type == "AI"
            for provider in PROVIDER_REGISTRY
        ),
        "repositories_registered": sum(
            provider.provider_type == "REPOSITORY"
            for provider in PROVIDER_REGISTRY
        ),
        "transformations_registered": sum(
            provider.provider_type in {"TRANSFORMATION", "RECONCILIATION"}
            for provider in PROVIDER_REGISTRY
        ),
        "provider_registry_coverage": (
            100 if not validation_errors else 0
        ),
    }


def _last_live_baseline_bytes() -> tuple[bytes | None, str | None]:
    """Read an exact-byte baseline only when its external code pin matches."""

    pinned_sha256 = LAST_LIVE_BASELINE_FILE_SHA256
    try:
        raw = LAST_LIVE_BASELINE_PATH.read_bytes()
    except FileNotFoundError:
        if pinned_sha256 is None:
            return None, None
        return None, "last_live_baseline_pinned_file_missing"
    except OSError:
        return None, "last_live_baseline_unreadable"
    if pinned_sha256 is None:
        return None, "last_live_baseline_unpinned"
    if (
        not isinstance(pinned_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", pinned_sha256) is None
    ):
        return None, "last_live_baseline_pin_invalid"
    if hashlib.sha256(raw).hexdigest() != pinned_sha256:
        return None, "last_live_baseline_sha256_mismatch"
    return raw, None


def _load_last_live_baseline() -> dict[str, Any] | None:
    raw, anchor_error = _last_live_baseline_bytes()
    if raw is None or anchor_error is not None:
        return None
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "contract",
            "schema_version",
            "source",
            "attestation",
            "run_id",
            "audit_status",
            "system_health",
            "registry_sha256",
            "providers_tested",
            "capabilities_tested",
            "capabilities",
        }
        or value.get("contract")
        != "ProviderCapabilityLastLiveBaseline"
        or value.get("schema_version") != "1.2"
        or value.get("audit_status") != "COMPLETED"
        or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_.-]*",
            str(value.get("run_id") or ""),
        )
        or value.get("system_health")
        not in {"HEALTHY", "DEGRADED", "CRITICAL", "UNKNOWN"}
        or not isinstance(value.get("capabilities"), list)
    ):
        return None
    from app.services.provider_capability_audit import (
        AuditStatus,
        FALLBACK_ROLES,
        PRIMARY_ROLES,
        QUALITY_COMPONENTS,
        HealthStatus,
        ProbeOutcome,
        _health_for_field,
        _health_severity,
        _transport_valid,
        determine_system_health,
        recommendations_for_result,
        stable_sha256,
    )

    outcome_scenarios = (
        ProbeOutcome(configured=True, transport_status="OK"),
        ProbeOutcome(
            configured=True,
            transport_status="OK",
            http_status=500,
        ),
        ProbeOutcome(configured=True, transport_status="UNKNOWN"),
        ProbeOutcome(configured=True, transport_status="UNUSABLE"),
        ProbeOutcome(configured=True, transport_status="AUTH_FAILED"),
        ProbeOutcome(configured=True, transport_status="RATE_LIMITED"),
        ProbeOutcome(configured=True, transport_status="DOWN"),
        ProbeOutcome(configured=False, transport_status="NOT_CONFIGURED"),
    )
    non_transport_checks = tuple(
        check
        for _, check, _ in QUALITY_COMPONENTS
        if check != "transport_valid"
    )
    achievable_by_degradable: dict[
        tuple[str, ...],
        dict[HealthStatus, set[int]],
    ] = {}

    def achievable_scores_for(
        degradable_checks: tuple[str, ...],
    ) -> dict[HealthStatus, set[int]]:
        normalized = tuple(sorted(set(degradable_checks)))
        cached = achievable_by_degradable.get(normalized)
        if cached is not None:
            return cached
        scores = {health: set() for health in HealthStatus}
        for outcome in outcome_scenarios:
            for observations in product(
                (True, False, None),
                repeat=len(non_transport_checks),
            ):
                checks = dict(
                    zip(
                        non_transport_checks,
                        observations,
                        strict=True,
                    )
                )
                checks["transport_valid"] = _transport_valid(outcome)
                score = sum(
                    weight
                    for _, check, weight in QUALITY_COMPONENTS
                    if checks.get(check) is True
                )
                health, _ = _health_for_field(
                    outcome,
                    checks,
                    score,
                    degradable_checks=normalized,
                )
                scores[health].add(score)
        achievable_by_degradable[normalized] = scores
        return scores

    if achievable_scores_for(())[HealthStatus.HEALTHY] != {
        sum(weight for _, _, weight in QUALITY_COMPONENTS)
    }:
        return None

    registry_digest = stable_sha256(
        [asdict(provider) for provider in PROVIDER_REGISTRY]
    )
    if value.get("registry_sha256") != registry_digest:
        return None
    source = value.get("source")
    attestation = value.get("attestation")
    unsigned = dict(value)
    unsigned.pop("attestation", None)
    if (
        not isinstance(source, dict)
        or set(source)
        != {
            "pointer_file",
            "audit_report_sha256",
            "audit_report_size_bytes",
            "extracted_capabilities_sha256",
        }
        or source.get("pointer_file")
        != "provider-capability-audit-latest.json"
        or not re.fullmatch(
            r"[0-9a-f]{64}",
            str(source.get("audit_report_sha256") or ""),
        )
        or type(source.get("audit_report_size_bytes")) is not int
        or source["audit_report_size_bytes"] <= 0
        or source.get("extracted_capabilities_sha256")
        != stable_sha256(value["capabilities"])
        or not isinstance(attestation, dict)
        or set(attestation)
        != {
            "algorithm",
            "content_sha256",
            "capability_rows",
            "field_rows",
        }
        or attestation.get("algorithm") != "SHA-256"
        or attestation.get("content_sha256") != stable_sha256(unsigned)
        or attestation.get("capability_rows") != len(value["capabilities"])
        or type(attestation.get("field_rows")) is not int
        or type(value.get("providers_tested")) is not int
        or value["providers_tested"] != len(PROVIDER_REGISTRY)
        or type(value.get("capabilities_tested")) is not int
    ):
        return None
    expected = {
        (
            provider.provider_id,
            capability.dataset_id,
            capability.metric_id,
        ): (provider, capability)
        for provider in PROVIDER_REGISTRY
        for capability in provider.capabilities
    }
    observed: dict[tuple[str, str, str], set[str]] = {}
    observed_field_rows = 0
    derived_capability_results: list[dict[str, Any]] = []
    for capability in value["capabilities"]:
        if not isinstance(capability, dict):
            return None
        identity = (
            str(capability.get("provider_id") or ""),
            str(capability.get("dataset_id") or ""),
            str(capability.get("metric_id") or ""),
        )
        field_results = capability.get("field_results")
        registration = expected.get(identity)
        expected_capability_keys = {
            "capability_id",
            "provider_id",
            "dataset_id",
            "metric_id",
            "health_status",
            "eligible_as_primary",
            "eligible_as_fallback",
            "recommendations",
            "reason_codes",
            "field_results",
        }
        terminal_attestation_present = (
            "terminal_attestation" in capability
        )
        if terminal_attestation_present:
            expected_capability_keys.add("terminal_attestation")
        if (
            identity in observed
            or registration is None
            or set(capability) != expected_capability_keys
            or capability.get("capability_id") != "|".join(identity)
            or not isinstance(field_results, dict)
            or any(
                not isinstance(field_result, dict)
                for field_result in field_results.values()
            )
        ):
            return None
        provider, registered_capability = registration
        terminal_reason = str(
            provider.terminal_audit_reason or ""
        ).strip()
        achievable_scores = achievable_scores_for(
            tuple(registered_capability.degradable_quality_checks)
        )
        observed_fields = {
            str(field_name) for field_name in field_results
        }
        if observed_fields != {
            *registered_capability.supported_fields,
            *registered_capability.audit_only_fields,
        }:
            return None
        roles = {role.upper() for role in provider.allowed_roles}
        expected_primary_role = not roles or bool(roles & PRIMARY_ROLES)
        expected_fallback_role = not roles or bool(roles & FALLBACK_ROLES)
        field_health: list[HealthStatus] = []
        field_primary: list[bool] = []
        field_fallback: list[bool] = []
        field_reasons: set[str] = set()
        for field_result in field_results.values():
            try:
                health = HealthStatus(str(field_result["health_status"]))
            except (KeyError, ValueError):
                return None
            score = field_result.get("quality_score")
            primary = field_result.get("eligible_as_primary")
            fallback = field_result.get("eligible_as_fallback")
            recommendations = field_result.get("recommendations")
            reason_codes = field_result.get("reason_codes")
            checked_at = field_result.get("checked_at")
            if (
                set(field_result)
                != {
                    "health_status",
                    "quality_score",
                    "eligible_as_primary",
                    "eligible_as_fallback",
                    "recommended_role",
                    "recommendations",
                    "reason_codes",
                    "checked_at",
                }
                or
                type(score) is not int
                or not 0 <= score <= 100
                or score not in achievable_scores[health]
                or type(primary) is not bool
                or type(fallback) is not bool
                or not isinstance(recommendations, list)
                or not recommendations
                or any(type(item) is not str or not item for item in recommendations)
                or field_result.get("recommended_role") != recommendations[0]
                or not isinstance(reason_codes, list)
                or reason_codes != sorted(set(reason_codes))
                or any(type(item) is not str or not item for item in reason_codes)
                or not isinstance(checked_at, str)
            ):
                return None
            try:
                parsed_checked_at = datetime.fromisoformat(
                    checked_at.replace("Z", "+00:00")
                )
            except ValueError:
                return None
            if parsed_checked_at.tzinfo is None:
                return None
            expected_primary = (
                health is HealthStatus.HEALTHY and expected_primary_role
            )
            expected_fallback = (
                health in {HealthStatus.HEALTHY, HealthStatus.DEGRADED}
                and expected_fallback_role
            )
            expected_recommendations = recommendations_for_result(
                health,
                roles=roles,
                eligible_as_primary=expected_primary,
                eligible_as_fallback=expected_fallback,
            )
            if (
                primary is not expected_primary
                or fallback is not expected_fallback
                or recommendations != expected_recommendations
            ):
                return None
            field_health.append(health)
            field_primary.append(primary)
            field_fallback.append(fallback)
            field_reasons.update(reason_codes)
        configured_terminal = bool(
            terminal_reason and terminal_attestation_present
        )
        not_configured_terminal = bool(
            terminal_reason
            and not terminal_attestation_present
            and capability.get("health_status") == "NOT_CONFIGURED"
        )
        if configured_terminal:
            if (
                capability.get("terminal_attestation")
                != {
                    "attempts": 0,
                    "real_adapter_invoked": False,
                    "probe_dispatch_status": "TERMINAL_UNSUPPORTED",
                    "reason_code": terminal_reason,
                }
                or any(
                    field_result.get("health_status") != "UNUSABLE"
                    or field_result.get("quality_score") != 0
                    or field_result.get("eligible_as_primary") is not False
                    or field_result.get("eligible_as_fallback") is not False
                    or field_result.get("reason_codes")
                    != [terminal_reason]
                    for field_result in field_results.values()
                )
            ):
                return None
        elif terminal_reason and not_configured_terminal:
            if any(
                field_result.get("health_status") != "NOT_CONFIGURED"
                or field_result.get("quality_score") != 0
                or field_result.get("eligible_as_primary") is not False
                or field_result.get("eligible_as_fallback") is not False
                or "PROVIDER_NOT_CONFIGURED"
                not in field_result.get("reason_codes", [])
                for field_result in field_results.values()
            ):
                return None
        elif terminal_reason:
            return None
        aggregate_health = max(field_health, key=_health_severity)
        aggregate_primary = all(field_primary)
        aggregate_fallback = all(field_fallback)
        aggregate_recommendations = recommendations_for_result(
            aggregate_health,
            roles=roles,
            eligible_as_primary=aggregate_primary,
            eligible_as_fallback=aggregate_fallback,
        )
        if (
            capability.get("health_status") != aggregate_health.value
            or capability.get("eligible_as_primary") is not aggregate_primary
            or capability.get("eligible_as_fallback") is not aggregate_fallback
            or capability.get("recommendations") != aggregate_recommendations
            or capability.get("reason_codes") != sorted(field_reasons)
            or (
                configured_terminal
                and (
                    capability.get("health_status") != "UNUSABLE"
                    or capability.get("eligible_as_primary") is not False
                    or capability.get("eligible_as_fallback") is not False
                    or capability.get("reason_codes") != [terminal_reason]
                )
            )
        ):
            return None
        observed[identity] = observed_fields
        observed_field_rows += len(field_results)
        derived_capability_results.append(
            {
                "health_status": aggregate_health.value,
                "eligible_as_primary": aggregate_primary,
                "eligible_as_fallback": aggregate_fallback,
            }
        )
    if (
        set(observed) != set(expected)
        or value["capabilities_tested"] != len(expected)
        or attestation["field_rows"] != observed_field_rows
        or value["system_health"]
        != determine_system_health(
            derived_capability_results,
            audit_status=AuditStatus.COMPLETED,
        ).value
    ):
        return None
    return value


def _last_live_field_index(
    baseline: dict[str, Any] | None,
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    indexed: dict[
        tuple[str, str, str, str],
        dict[str, Any],
    ] = {}
    for capability in (baseline or {}).get("capabilities") or []:
        if not isinstance(capability, dict):
            continue
        field_results = capability.get("field_results")
        if not isinstance(field_results, dict):
            continue
        identity = (
            str(capability.get("provider_id") or ""),
            str(capability.get("dataset_id") or ""),
            str(capability.get("metric_id") or ""),
        )
        for field_name, field_result in field_results.items():
            if isinstance(field_result, dict):
                indexed[(*identity, str(field_name))] = field_result
    return indexed


def _field_live_record(
    *,
    provider_id: str,
    capability: CapabilityRegistration,
    field_name: str,
    baseline: dict[str, Any] | None,
    live_fields: dict[
        tuple[str, str, str, str],
        dict[str, Any],
    ],
) -> dict[str, Any]:
    run_id = (
        str((baseline or {}).get("run_id") or "").strip()
        or None
    )
    observed = live_fields.get(
        (
            provider_id,
            capability.dataset_id,
            capability.metric_id,
            field_name,
        )
    )
    if observed is None:
        return {
            "field_name": field_name,
            "last_live_result": (
                f"RUN_{run_id}: FIELD_NOT_AUDITED"
                if run_id
                else "NO_VERIFIED_LIVE_BASELINE"
            ),
            "health_status": "NOT_AUDITED",
            "recommended_role": (
                "REQUIRES_FIX"
                if run_id
                else "PENDING_LIVE_AUDIT"
            ),
            "quality_score": None,
            "eligible_as_primary": False,
            "eligible_as_fallback": False,
            "reason_codes": [],
            "checked_at": None,
        }
    health_status = str(
        observed.get("health_status") or "UNKNOWN"
    )
    return {
        "field_name": field_name,
        "last_live_result": f"RUN_{run_id}: {health_status}",
        "health_status": health_status,
        "recommended_role": str(
            observed.get("recommended_role") or "REQUIRES_FIX"
        ),
        "quality_score": observed.get("quality_score"),
        "eligible_as_primary": (
            observed.get("eligible_as_primary") is True
        ),
        "eligible_as_fallback": (
            observed.get("eligible_as_fallback") is True
        ),
        "reason_codes": [
            str(item) for item in observed.get("reason_codes") or []
        ],
        "checked_at": observed.get("checked_at"),
    }


def _capability_matrix_row(
    provider_id: str,
    capability: CapabilityRegistration,
    *,
    baseline: dict[str, Any] | None,
    live_fields: dict[
        tuple[str, str, str, str],
        dict[str, Any],
    ],
) -> dict[str, Any]:
    return {
        **asdict(capability),
        "field_capabilities": [
            _field_live_record(
                provider_id=provider_id,
                capability=capability,
                field_name=field_name,
                baseline=baseline,
                live_fields=live_fields,
            )
            for field_name in (
                *capability.supported_fields,
                *capability.audit_only_fields,
            )
        ],
    }


def render_matrix_json() -> str:
    baseline = _load_last_live_baseline()
    live_fields = _last_live_field_index(baseline)
    providers = [
        {
            **asdict(provider),
            "capabilities": [
                _capability_matrix_row(
                    provider.provider_id,
                    capability,
                    baseline=baseline,
                    live_fields=live_fields,
                )
                for capability in provider.capabilities
            ],
        }
        for provider in PROVIDER_REGISTRY
    ]
    datasets = [
        _dataset_matrix_row(
            policy,
            baseline=baseline,
            live_fields=live_fields,
        )
        for policy in DATASET_SOURCE_POLICIES
    ]
    payload = {
        "contract": "SeniorAnalystDataSourceMatrix",
        "schema_version": "2.0",
        "registry_module": (
            "app.services.provider_capability_registry"
        ),
        "authoritative_capture": _AUTHORITATIVE_CAPTURE,
        "failure_policy": "RETURN_NULL",
        "known_rate_limit_policy": (
            "Use UNKNOWN unless proved by configuration or captured headers."
        ),
        "last_live_capability_audit": (
            {
                "available": True,
                "run_id": baseline["run_id"],
                "audit_status": baseline.get("audit_status"),
                "system_health": baseline.get("system_health"),
                "registry_sha256": baseline.get("registry_sha256"),
                "baseline_path": (
                    "docs/baselines/"
                    "provider-capability-last-live.json"
                ),
            }
            if baseline is not None
            else {
                "available": False,
                "run_id": None,
                "audit_status": None,
                "system_health": None,
                "registry_sha256": None,
                "baseline_path": (
                    "docs/baselines/"
                    "provider-capability-last-live.json"
                ),
            }
        ),
        "registry_summary": registry_summary(),
        "official_metrics": [
            asdict(specification)
            for specification in OFFICIAL_METRIC_REGISTRATIONS
        ],
        "datasets": datasets,
        "providers": providers,
    }
    return json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"


def render_matrix_markdown() -> str:
    summary = registry_summary()
    baseline = _load_last_live_baseline()
    live_fields = _last_live_field_index(baseline)
    live_run_id = (
        str((baseline or {}).get("run_id") or "").strip()
        or "none"
    )
    lines = [
        "# Senior Analyst data-source matrix",
        "",
        (
            "Generated from `app.services.provider_capability_registry`; "
            "do not edit this file manually."
        ),
        "",
        (
            f"Registered providers: **{summary['providers_registered']}**; "
            f"runtime adapters: **{summary['runtime_adapters_registered']}**; "
            f"capabilities: **{summary['capabilities_registered']}**; "
            f"dataset policies: **{summary['dataset_policies_registered']}**."
        ),
        "",
        f"Last verified LIVE capability baseline: **{live_run_id}**.",
        "",
        "## Dataset policy",
        "",
        (
            "| Dataset | Section | Frequency | SLA seconds | Primary | "
            "Ordered fallbacks | AI fallback candidates | Repository | "
            "Strategy | Probes |"
        ),
        "|---|---|---:|---:|---|---|---|---|---|---|",
    ]
    for policy in DATASET_SOURCE_POLICIES:
        provider_ids = (
            policy.primary_provider,
            *policy.fallback_providers,
            *policy.ai_fallback_providers,
        )
        probes = sorted(
            {
                capability.probe_id
                for provider_id in provider_ids
                for capability in provider_by_id(provider_id).capabilities
                if capability.dataset_id == policy.dataset_id
            }
        )
        lines.append(
            "| "
            + " | ".join(
                (
                    policy.dataset_id,
                    policy.section,
                    policy.frequency,
                    str(policy.sla_seconds),
                    policy.primary_provider,
                    ", ".join(policy.fallback_providers) or "none",
                    ", ".join(policy.ai_fallback_providers) or "none",
                    policy.canonical_repository,
                    policy.provider_strategy,
                    "<br>".join(probes),
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "## Official raw-to-canonical mappings",
            "",
            (
                "| Canonical metric | Dataset | Provider | Raw series | "
                "Transformation | Frequency | Unit |"
            ),
            "|---|---|---|---|---|---|---|",
        )
    )
    for specification in OFFICIAL_METRIC_REGISTRATIONS:
        lines.append(
            "| "
            + " | ".join(
                (
                    specification.canonical_metric_id,
                    specification.dataset_id,
                    specification.provider_id,
                    specification.source_series_id,
                    specification.transformation,
                    specification.frequency,
                    specification.unit,
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "## Provider registry",
            "",
            (
                "| Provider | Type | Adapter | Roles | Credentials | Timeout | "
                "Attempts | Rate limit | Probe | Capabilities |"
            ),
            "|---|---|---|---|---|---|---:|---|---|---:|",
        )
    )
    for provider in PROVIDER_REGISTRY:
        lines.append(
            "| "
            + " | ".join(
                (
                    provider.provider_id,
                    provider.provider_type,
                    provider.adapter_path,
                    ", ".join(provider.allowed_roles),
                    (
                        ", ".join(provider.credential_requirements)
                        or "none"
                    ).replace("|", "&#124;"),
                    str(provider.timeout),
                    str(provider.max_attempts),
                    provider.known_rate_limit,
                    provider.probe_id,
                    str(len(provider.capabilities)),
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "## Capability matrix",
            "",
            (
                "| Provider | Dataset | Metric | Field | Frequency | "
                "Transformation | Probe | Field validator | AI eligible | "
                "Request group | Last LIVE | Health | Recommended role |"
            ),
            "|---|---|---|---|---|---|---|---|---:|---|---|---|---|",
        )
    )
    for provider in PROVIDER_REGISTRY:
        for capability in provider.capabilities:
            for field_name in dict.fromkeys(
                (
                    *capability.supported_fields,
                    *capability.audit_only_fields,
                )
            ):
                live = _field_live_record(
                    provider_id=provider.provider_id,
                    capability=capability,
                    field_name=field_name,
                    baseline=baseline,
                    live_fields=live_fields,
                )
                lines.append(
                    "| "
                    + " | ".join(
                        (
                            provider.provider_id,
                            capability.dataset_id,
                            capability.metric_id,
                            field_name,
                            capability.frequency,
                            capability.transformation,
                            capability.probe_id,
                            capability.field_validator_id,
                            "yes" if capability.ai_eligible else "no",
                            capability.request_group or "none",
                            str(live["last_live_result"]),
                            str(live["health_status"]),
                            str(live["recommended_role"]),
                        )
                    )
                    + " |"
                )
    lines.extend(
        (
            "",
            "## Invariants",
            "",
            "- DB-first remains mandatory for every Senior Analyst dataset.",
            (
                "- A capability is auditable only when its probe and field "
                "validator are registered."
            ),
            (
                "- AI eligibility is field-capability specific; a global AI "
                "health result cannot certify a capability."
            ),
            (
                "- `retrieved_at` never substitutes `data_as_of`, "
                "`content_valid_until`, or `refresh_due_at`."
            ),
            (
                "- Unknown rate limits remain `UNKNOWN`; the audit must not "
                "discover them through repeated calls."
            ),
            "",
        )
    )
    return "\n".join(lines)


def _dataset_matrix_row(
    policy: DatasetSourcePolicy,
    *,
    baseline: dict[str, Any] | None,
    live_fields: dict[
        tuple[str, str, str, str],
        dict[str, Any],
    ],
) -> dict[str, Any]:
    provider_ids = (
        policy.primary_provider,
        *policy.fallback_providers,
        *policy.ai_fallback_providers,
    )
    capabilities = [
        {
            "provider_id": provider_id,
            **_capability_matrix_row(
                provider_id,
                capability,
                baseline=baseline,
                live_fields=live_fields,
            ),
        }
        for provider_id in provider_ids
        for capability in provider_by_id(provider_id).capabilities
        if capability.dataset_id == policy.dataset_id
    ]
    primary = provider_by_id(policy.primary_provider)
    return {
        **asdict(policy),
        # Preserve the legacy dataset-level capture marker consumed by the
        # Senior Analyst regression suite. Per-field capability health lives
        # alongside it and is sourced only from a verified audit baseline.
        "last_live_result": (
            f"RUN_{_AUTHORITATIVE_CAPTURE['run_id']}: "
            f"{_AUTHORITATIVE_CAPTURE['gate_status']}"
        ),
        "database_table_or_repository": policy.canonical_repository,
        "failure_policy": "RETURN_NULL",
        "known_rate_limit": primary.known_rate_limit,
        "freshness_sla_seconds": policy.sla_seconds,
        "expiry_rule": (
            "Fail closed when content_valid_until or refresh_due_at is past; "
            "otherwise require data_as_of and release lifecycle to satisfy "
            f"the dataset cadence and {policy.sla_seconds}-second SLA."
        ),
        "max_attempts": primary.max_attempts,
        "provider_timeout": primary.timeout,
        "retry_policy": primary.retry_policy,
        "capabilities": capabilities,
        "probe_ids": sorted(
            {
                capability["probe_id"]
                for capability in capabilities
            }
        ),
    }


def _resolve_adapter_path(path: str) -> Any:
    if ":" not in path:
        raise ValueError("adapter_path_must_use_module_colon_qualname")
    module_name, qualname = path.split(":", 1)
    if not module_name or not qualname:
        raise ValueError("adapter_path_incomplete")
    value: Any = importlib.import_module(module_name)
    for part in qualname.split("."):
        value = getattr(value, part)
    return value
