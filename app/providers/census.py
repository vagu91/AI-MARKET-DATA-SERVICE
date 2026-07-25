from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

import httpx

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheProtocol
from app.models.common import Freshness, ProviderResult, ProviderType
from app.providers.base import BaseProvider, ProviderDisabled, ProviderError, metadata
from app.providers.deterministic import (
    CacheStatus,
    DeterministicHttpClient,
    DeterministicProviderError,
    NormalizedObservation,
    ProviderEnvelope,
    ProviderKind,
    safe_payload_hash,
)


@dataclass(frozen=True)
class CensusSeriesMapping:
    series_id: str
    semantic_field: str
    dataset: str
    program_code: str
    source_program: str
    category_code: str
    data_type_code: str
    seasonally_adjusted: bool
    time_slot_epoch: str
    unit: str
    frequency: str
    seasonal_adjustment: str


# Exact identities come from the official EITS program dictionaries. Unknown or
# ambiguous rows are represented by redacted hashes in audit lineage only and
# are never projected into the operational consumer.
CENSUS_SERIES: tuple[CensusSeriesMapping, ...] = (
    CensusSeriesMapping(
        "CENSUS:MARTS:RETAIL_SALES",
        "advance_retail_sales",
        "marts",
        "MARTS",
        "Advance Monthly Retail Trade and Food Services",
        "44X72",
        "SM",
        True,
        "1992-01",
        "millions_usd",
        "monthly",
        "SA",
    ),
    CensusSeriesMapping(
        "CENSUS:ADVM3:DURABLE_GOODS",
        "advance_durable_goods_orders",
        "advm3",
        "M3ADV",
        "Advance Report on Durable Goods",
        "MDM",
        "NO",
        True,
        "1992-01",
        "millions_usd",
        "monthly",
        "SA",
    ),
    CensusSeriesMapping(
        "CENSUS:RESCONST:HOUSING_STARTS",
        "housing_starts",
        "resconst",
        "RESCONST",
        "New Residential Construction",
        "ASTARTS",
        "TOTAL",
        True,
        "1959-01",
        "thousands_annual_rate",
        "monthly",
        "SAAR",
    ),
    CensusSeriesMapping(
        "CENSUS:RESCONST:BUILDING_PERMITS",
        "building_permits",
        "resconst",
        "RESCONST",
        "New Residential Construction",
        "APERMITS",
        "TOTAL",
        True,
        "1959-01",
        "thousands_annual_rate",
        "monthly",
        "SAAR",
    ),
    CensusSeriesMapping(
        "CENSUS:FTD:TRADE_BALANCE",
        "international_trade_balance",
        "ftd",
        "FTD",
        "International Trade in Goods and Services",
        "BOPGS",
        "BAL",
        True,
        "1992-01",
        "millions_usd",
        "monthly",
        "SA",
    ),
)
CENSUS_DATASETS = {
    "MARTS": "marts",
    "ADVM3": "advm3",
    "RESCONST": "resconst",
    "FTD": "ftd",
}
EITS_OUTPUT_FIELDS = (
    "category_code",
    "cell_value",
    "data_type_code",
    "error_data",
    "program_code",
    "seasonally_adj",
    "time_slot_id",
)
EITS_PREDICATE_ONLY_FIELDS = frozenset({"time", "for", "in", "ucgid"})
CENSUS_QUERY_PREDICATES = {
    # ADVM3's sole official API example requires the national geography
    # predicate. Predicate-only fields remain outside ``get``.
    "ADVM3": {"for": "us:*"},
}
_PERIOD_RE = re.compile(r"^\d{4}-(?:\d{2}|Q[1-4])$")


class CensusProvider(BaseProvider):
    source = "CENSUS"
    provider_type = ProviderType.API
    reliability = 0.98
    cache_key = "provider:census:economic_indicators"

    def __init__(
        self,
        cache: ProviderCacheProtocol,
        settings: Settings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        super().__init__(cache)
        self.settings = settings
        self.http = DeterministicHttpClient(
            allowed_hosts={"api.census.gov"},
            timeout_seconds=settings.census_timeout_seconds,
            retry_attempts=settings.census_retry_attempts,
            allowed_methods={"GET"},
            transport=transport,
        )

    async def fetch(
        self,
        *,
        period: str | None = None,
        datasets: Iterable[str] | None = None,
    ) -> ProviderResult:
        if not self.settings.census_enabled:
            raise ProviderDisabled("Census provider is disabled")
        if not self.settings.census_api_key:
            raise ProviderError("Census API key is not configured")
        if period is None:
            raise ProviderError("Census exact lifecycle period is required")
        _validate_period(period)
        requested = _dataset_names(datasets)
        normalized: dict[str, dict[str, Any]] = {}
        errors: list[str] = []
        latest_as_of: datetime | None = None
        for dataset in requested:
            try:
                envelope = await self.fetch_dataset(dataset, period=period)
            except DeterministicProviderError as exc:
                errors.append(f"Census {dataset} failed: {exc}")
                continue
            errors.extend(
                f"Census {dataset} rejected: {reason}"
                for reason in envelope.rejection_reasons
            )
            for observation in envelope.observations:
                item = {
                    **observation.model_dump(mode="json"),
                    "period": observation.reference_period,
                    "release_vintage": observation.revision,
                }
                normalized[observation.observation_id] = {
                    "series_id": observation.observation_id,
                    "name": observation.semantic_field,
                    "value": observation.value,
                    "units": observation.unit,
                    "frequency": observation.frequency,
                    "seasonal_adjustment": observation.seasonal_adjustment,
                    "data_as_of": observation.reference_period,
                    "reference_period": observation.reference_period,
                    "release_occurrence": observation.occurrence_id,
                    "revision": observation.revision,
                    "source_program": observation.source_program,
                    "observations": [item],
                    "source": self.source,
                    "source_url": envelope.source_url,
                    "canonical_url": "https://www.census.gov/economic-indicators/",
                    "source_domain": "census.gov",
                    "provider_adapter": "CENSUS_OFFICIAL_API",
                    "official_adapter": True,
                    "lineage": envelope.lineage,
                    "telemetry": envelope.telemetry,
                }
                if observation.observed_at:
                    latest_as_of = max(latest_as_of, observation.observed_at) if latest_as_of else observation.observed_at
        if not normalized and not errors:
            errors.append("Census returned no mapped observations")
        return ProviderResult(
            metadata=metadata(
                source=self.source,
                provider_type=self.provider_type,
                reliability=self.reliability if normalized else 0.0,
                data_as_of=latest_as_of,
                freshness=Freshness.RECENT if normalized else Freshness.UNKNOWN,
                errors=errors,
            ),
            data=normalized,
        )

    async def fetch_dataset(self, dataset: str, *, period: str) -> ProviderEnvelope:
        normalized_dataset = str(dataset).upper()
        if normalized_dataset not in CENSUS_DATASETS:
            raise DeterministicProviderError("unknown Census dataset mapping")
        _validate_period(period)
        dataset_path = CENSUS_DATASETS[normalized_dataset]
        base = self.settings.census_base_url.rstrip("/")
        expected_suffix = f"/data/timeseries/eits/{dataset_path}"
        url = (
            base
            if base.lower().endswith(expected_suffix)
            else f"{base}{expected_suffix}"
        )
        specs = [item for item in CENSUS_SERIES if item.dataset == dataset_path]
        if any(item.frequency == "monthly" for item in specs) and "-Q" in period:
            raise ProviderError(
                f"Census {normalized_dataset} requires an exact YYYY-MM occurrence"
            )
        fields = EITS_OUTPUT_FIELDS
        if EITS_PREDICATE_ONLY_FIELDS.intersection(fields):
            raise RuntimeError("predicate-only Census variable present in get")
        params = {
            "get": ",".join(fields),
            "time": period,
            **CENSUS_QUERY_PREDICATES.get(normalized_dataset, {}),
            "key": self.settings.census_api_key,
        }
        payload, telemetry, request_meta = await self.http.request(
            "GET",
            url,
            endpoint_category="economic_indicators",
            provider=self.source,
            params=params,
        )
        telemetry.dataset_or_series = normalized_dataset
        rows = _tabular_rows(payload)
        retrieved_at = datetime.now(UTC)
        raw_payload_hash = safe_payload_hash(payload)
        refresh_at = retrieved_at + timedelta(
            seconds=max(int(self.settings.census_cache_ttl_seconds), 1)
        )
        observations: list[NormalizedObservation] = []
        rejection_reasons: list[str] = []
        rejected_row_hashes: list[str] = []
        for spec in specs:
            expected_time_slot_id = _expected_time_slot_id(
                period,
                epoch=spec.time_slot_epoch,
                frequency=spec.frequency,
            )
            matches = [
                row
                for row in rows
                if _matches_mapping(
                    row,
                    spec=spec,
                    period=period,
                    expected_time_slot_id=expected_time_slot_id,
                )
            ]
            if not matches:
                rejection_reasons.append(
                    f"no_exact_census_mapping:{spec.series_id}"
                )
                rejected_row_hashes.extend(
                    safe_payload_hash(row) for row in rows
                )
                continue
            if len(matches) > 1:
                telemetry.anomalies.append("ambiguous_census_mapping")
                rejection_reasons.append(
                    f"ambiguous_census_mapping:{spec.series_id}"
                )
                rejected_row_hashes.extend(
                    safe_payload_hash(row) for row in matches
                )
                continue
            row = matches[0]
            raw_value = row.get("cell_value")
            value = _decimal_value(raw_value)
            if value is None:
                rejection_reasons.append(
                    f"missing_census_value:{spec.series_id}"
                )
                continue
            observed_at = _period_start(period)
            revision = str(
                row.get("revision")
                or row.get("is_revised")
                or row.get("status")
                or "published"
            )
            occurrence_id = (
                f"{spec.series_id}:{period}:slot-{expected_time_slot_id}"
            )
            observations.append(
                NormalizedObservation(
                    observation_id=spec.series_id,
                    semantic_field=spec.semantic_field,
                    value=value,
                    unit=spec.unit,
                    frequency=spec.frequency,
                    seasonal_adjustment=spec.seasonal_adjustment,
                    reference_period=period,
                    occurrence_id=occurrence_id,
                    observed_at=observed_at,
                    retrieved_at=retrieved_at,
                    provider_timestamp=_parse_timestamp(row.get("updated_at")),
                    valid_until=refresh_at,
                    next_refresh_at=refresh_at,
                    freshness_state="CURRENT_RELEASE",
                    lifecycle_state="PUBLISHED",
                    source_program=spec.source_program,
                    revision=revision,
                    metadata={
                        "dataset": normalized_dataset,
                        "program_code": spec.program_code,
                        "category_code": spec.category_code,
                        "data_type_code": spec.data_type_code,
                        "seasonally_adjusted": spec.seasonally_adjusted,
                        "seasonally_adj_raw": row.get("seasonally_adj"),
                        "time_slot_id": expected_time_slot_id,
                        "reference_period": period,
                        "frequency": spec.frequency,
                        "unit": spec.unit,
                        "semantic_field": spec.semantic_field,
                        "original_value": str(raw_value),
                        "precision": _precision(raw_value),
                        "raw_payload_hash": raw_payload_hash,
                        "request_fingerprint": request_meta[
                            "request_fingerprint"
                        ],
                        "source_url": request_meta["source_url"],
                    },
                )
            )
        telemetry.accepted = len(observations)
        telemetry.rejected = len(rejection_reasons)
        if rows and not observations:
            telemetry.anomalies.append("raw_normalized_count_mismatch")
        warnings = list(dict.fromkeys(
            reason.split(":", 1)[0] for reason in rejection_reasons
        ))
        return ProviderEnvelope(
            provider_id="census",
            provider_kind=ProviderKind.OFFICIAL_GOVERNMENT,
            authority_tier=1,
            requested_domain="macro_actual",
            retrieved_at=retrieved_at,
            source_url=request_meta["source_url"],
            request_fingerprint=request_meta["request_fingerprint"],
            cache_status=CacheStatus.MISS,
            freshness="CURRENT_RELEASE" if observations else "NO_DATA",
            exact_occurrence_identity=(
                f"CENSUS:{normalized_dataset}:{period}:"
                f"slot-{_expected_time_slot_id(period, epoch=specs[0].time_slot_epoch, frequency=specs[0].frequency)}"
            ),
            observations=observations,
            warnings=warnings,
            rejection_reasons=rejection_reasons,
            retry_classification="NONE",
            raw_payload_hash=raw_payload_hash,
            lineage={
                "provider": "CENSUS",
                "dataset": normalized_dataset,
                "period": period,
                "trigger_class": "TRIGGER",
                "normalization": "exact_semantic_tuple",
                "output_fields": list(fields),
                "predicate_fields": sorted(
                    {"time", *CENSUS_QUERY_PREDICATES.get(normalized_dataset, {})}
                ),
                "rejected_row_hashes": sorted(set(rejected_row_hashes)),
            },
            telemetry=telemetry.as_dict(),
        )


def _dataset_names(values: Iterable[str] | None) -> list[str]:
    output = [str(item).upper() for item in (values or CENSUS_DATASETS)]
    unknown = [item for item in output if item not in CENSUS_DATASETS]
    if unknown:
        raise ProviderError(f"unknown Census dataset mapping: {','.join(unknown)}")
    return list(dict.fromkeys(output))


def _validate_period(period: str) -> None:
    if not _PERIOD_RE.fullmatch(str(period)):
        raise ProviderError("Census period must be an exact YYYY-MM or YYYY-Qn occurrence")


def _tabular_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        raw = payload.get("data") or payload.get("results") or payload.get("Data") or []
        if isinstance(raw, dict):
            return [raw]
        return [item for item in raw if isinstance(item, dict)]
    if not isinstance(payload, list) or not payload:
        return []
    if all(isinstance(item, dict) for item in payload):
        return list(payload)
    header = payload[0]
    if not isinstance(header, list):
        raise DeterministicProviderError("Census payload shape changed")
    rows: list[dict[str, Any]] = []
    for values in payload[1:]:
        if isinstance(values, list):
            rows.append(dict(zip((str(item) for item in header), values, strict=False)))
    return rows


def _decimal_value(value: Any) -> int | float | None:
    if value in (None, "", "null", "NA", "(X)"):
        return None
    try:
        number = Decimal(str(value).replace(",", ""))
    except InvalidOperation as exc:
        raise DeterministicProviderError("Census impossible numeric value") from exc
    if not number.is_finite():
        raise DeterministicProviderError("Census non-finite numeric value")
    return int(number) if number == number.to_integral_value() else float(number)


def _matches_mapping(
    row: dict[str, Any],
    *,
    spec: CensusSeriesMapping,
    period: str,
    expected_time_slot_id: str,
) -> bool:
    observed_program = str(row.get("program_code") or "").strip().upper()
    return bool(
        str(row.get("category_code") or "").strip().upper()
        == spec.category_code
        and str(row.get("data_type_code") or "").strip().upper()
        == spec.data_type_code
        and _seasonally_adjusted(row.get("seasonally_adj"))
        is spec.seasonally_adjusted
        and str(row.get("time") or row.get("period") or "").strip() == period
        and str(row.get("time_slot_id") or "").strip()
        == expected_time_slot_id
        and not _error_data_row(row.get("error_data"))
        and observed_program
        in {
            spec.program_code,
            spec.dataset.upper(),
        }
    )


def _seasonally_adjusted(value: Any) -> bool | None:
    normalized = str(value or "").strip().lower()
    if normalized in {
        "1",
        "true",
        "yes",
        "y",
        "sa",
        "saar",
        "seasonally adjusted",
    }:
        return True
    if normalized in {
        "0",
        "0.0",
        "false",
        "no",
        "n",
        "nsa",
        "not seasonally adjusted",
    }:
        return False
    return None


def _error_data_row(value: Any) -> bool:
    return str(value or "").strip().lower() not in {
        "",
        "0",
        "0.0",
        "false",
        "no",
        "n",
        "none",
        "null",
    }


def _expected_time_slot_id(
    period: str,
    *,
    epoch: str,
    frequency: str,
) -> str:
    if frequency != "monthly" or "-Q" in period:
        raise ProviderError("unsupported Census time-slot frequency")
    period_year, period_month = (int(item) for item in period.split("-", 1))
    epoch_year, epoch_month = (int(item) for item in epoch.split("-", 1))
    offset = (period_year - epoch_year) * 12 + period_month - epoch_month
    if offset < 0:
        raise ProviderError("Census period predates mapped time-slot epoch")
    return str(offset + 1)


def _precision(value: Any) -> int:
    raw = str(value or "")
    return len(raw.rsplit(".", 1)[1]) if "." in raw else 0


def _period_start(period: str) -> datetime:
    year = int(period[:4])
    suffix = period[5:]
    month = int(suffix) if suffix.isdigit() else (int(suffix[-1]) - 1) * 3 + 1
    return datetime(year, month, 1, tzinfo=UTC)


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
