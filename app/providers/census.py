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
    program: str
    value_field: str
    code_field: str
    code: str
    unit: str
    frequency: str
    seasonal_adjustment: str


# Codes are explicit service-owned identities. Unknown rows are retained only in audit,
# never projected into the operational consumer.
CENSUS_SERIES: tuple[CensusSeriesMapping, ...] = (
    CensusSeriesMapping(
        "CENSUS:MARTS:RETAIL_SALES",
        "advance_retail_sales",
        "marts",
        "Advance Monthly Retail Trade and Food Services",
        "cell_value",
        "category_code",
        "44X72",
        "millions_usd",
        "monthly",
        "SA",
    ),
    CensusSeriesMapping(
        "CENSUS:ADVM3:DURABLE_GOODS",
        "advance_durable_goods_orders",
        "advm3",
        "Advance Report on Durable Goods",
        "cell_value",
        "category_code",
        "00",
        "millions_usd",
        "monthly",
        "SA",
    ),
    CensusSeriesMapping(
        "CENSUS:RESCONST:HOUSING_STARTS",
        "housing_starts",
        "resconst",
        "New Residential Construction",
        "cell_value",
        "category_code",
        "APERMITS",
        "thousands_annual_rate",
        "monthly",
        "SAAR",
    ),
    CensusSeriesMapping(
        "CENSUS:RESCONST:BUILDING_PERMITS",
        "building_permits",
        "resconst",
        "New Residential Construction",
        "cell_value",
        "category_code",
        "PERMITS",
        "thousands_annual_rate",
        "monthly",
        "SAAR",
    ),
    CensusSeriesMapping(
        "CENSUS:FTD:TRADE_BALANCE",
        "international_trade_balance",
        "ftd",
        "International Trade in Goods and Services",
        "cell_value",
        "category_code",
        "BOPGS",
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
        fields = sorted(
            {
                "time",
                "seasonally_adj",
                "data_type_code",
                *(item.value_field for item in specs),
                *(item.code_field for item in specs),
            }
        )
        payload, telemetry, request_meta = await self.http.request(
            "GET",
            url,
            endpoint_category="economic_indicators",
            provider=self.source,
            params={
                "get": ",".join(fields),
                "time": period,
                "key": self.settings.census_api_key,
            },
        )
        rows = _tabular_rows(payload)
        retrieved_at = datetime.now(UTC)
        observations: list[NormalizedObservation] = []
        rejected = 0
        for spec in specs:
            matches = [
                row
                for row in rows
                if str(row.get(spec.code_field) or row.get("series_code") or "") == spec.code
                and str(row.get("time") or row.get("period") or "") == period
            ]
            if not matches:
                rejected += 1
                continue
            row = matches[-1]
            value = _decimal_value(row.get(spec.value_field) or row.get("value"))
            if value is None:
                rejected += 1
                continue
            observed_at = _period_start(period)
            revision = str(
                row.get("revision")
                or row.get("is_revised")
                or row.get("status")
                or "published"
            )
            occurrence_id = f"CENSUS:{normalized_dataset}:{period}"
            observations.append(
                NormalizedObservation(
                    observation_id=spec.series_id,
                    semantic_field=spec.semantic_field,
                    value=value,
                    unit=spec.unit,
                    frequency=spec.frequency,
                    seasonal_adjustment=str(
                        row.get("seasonally_adj") or spec.seasonal_adjustment
                    ),
                    reference_period=period,
                    occurrence_id=occurrence_id,
                    observed_at=observed_at,
                    retrieved_at=retrieved_at,
                    provider_timestamp=_parse_timestamp(row.get("updated_at")),
                    valid_until=observed_at + timedelta(days=62),
                    next_refresh_at=observed_at + timedelta(days=31),
                    freshness_state="CURRENT_RELEASE",
                    lifecycle_state="PUBLISHED",
                    source_program=spec.program,
                    revision=revision,
                    metadata={
                        "dataset": normalized_dataset,
                        "code": spec.code,
                        "precision": _precision(row.get(spec.value_field) or row.get("value")),
                    },
                )
            )
        telemetry.accepted = len(observations)
        telemetry.rejected = rejected
        if rows and not observations:
            telemetry.anomalies.append("raw_normalized_count_mismatch")
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
            exact_occurrence_identity=f"CENSUS:{normalized_dataset}:{period}",
            observations=observations,
            warnings=[] if observations else ["no_mapped_observation"],
            rejection_reasons=[] if observations else ["mapping_or_period_mismatch"],
            retry_classification="NONE",
            raw_payload_hash=safe_payload_hash(payload),
            lineage={
                "provider": "CENSUS",
                "dataset": normalized_dataset,
                "period": period,
                "trigger_class": "TRIGGER",
                "normalization": "explicit_code_mapping",
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
