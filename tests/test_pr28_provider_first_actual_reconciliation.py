from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.models.common import Freshness, ProviderResult, ProviderType
from app.providers.base import ProviderError, metadata
from app.providers.sp_global_pmi import SERIES_ID, SpGlobalPmiProvider
from app.services.deterministic_actual_resolver import DeterministicActualResolver
from app.services.lifecycle_due_resolver import MacroActualLifecycleProviderAdapter
from app.services.official_actual_semantics import normalize_reference_period


RELEASED = datetime(2026, 7, 24, 14, 0, tzinfo=UTC)
NOW = datetime(2026, 7, 28, 8, 0, tzinfo=UTC)


def settings(tmp_path: Path, **overrides) -> Settings:
    values = {
        "environment": "test",
        "database_path": tmp_path / "pr28.sqlite",
        "source_policy_path": Path(__file__).resolve().parents[1] / "config" / "source_policy.json",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


class OfficialFred:
    source = "FRED"

    async def fetch(self, *, series_ids):
        assert series_ids == ["HSN1F"]
        return ProviderResult(
            metadata=metadata(
                source="FRED",
                provider_type=ProviderType.API,
                reliability=0.98,
                freshness=Freshness.RECENT,
            ),
            data={
                "HSN1F": {
                    "official_adapter": True,
                    "provider_adapter": "FRED_OFFICIAL_API",
                    "source": "FRED",
                    "source_url": "https://fred.stlouisfed.org/series/HSN1F",
                    "canonical_url": "https://fred.stlouisfed.org/series/HSN1F",
                    "source_domain": "fred.stlouisfed.org",
                    "seasonal_adjustment": "SAAR",
                    "observations": [
                        {"period": "2026-05", "value": "618"},
                        {"period": "2026-06", "value": "628"},
                    ],
                }
            },
        )


def test_new_home_sales_exact_occurrence_uses_official_series_and_localized_period(
    tmp_path: Path,
) -> None:
    resolver = DeterministicActualResolver(
        settings(tmp_path), providers={"FRED": OfficialFred()}
    )
    result = resolver.resolve_event(
        event_key="xtb:146392:2026-07-24",
        event={
            "event_id": "xtb:146392:2026-07-24",
            "name": "Vendita case nuove",
            "reference_period": "Giugno",
        },
        temporal_state={"release_at": RELEASED.isoformat()},
    )
    assert result["status"] == "SUCCEEDED"
    candidate = result["results"][0]
    assert (candidate["value"], candidate["previous"], candidate["reference_period"]) == (
        "628",
        "618",
        "2026-06",
    )
    assert candidate["source_series_id"] == "HSN1F"
    assert candidate["provider_adapter"] == "FRED_OFFICIAL_API"


@pytest.mark.parametrize(
    ("localized", "expected"),
    [("Giugno", "2026-06"), ("Luglio", "2026-07")],
)
def test_localized_month_normalization_is_host_locale_independent(
    localized: str, expected: str
) -> None:
    assert normalize_reference_period(
        localized, frequency="monthly", release_date=RELEASED
    ) == expected


@pytest.mark.asyncio
async def test_sp_global_release_parser_preserves_actual_previous_and_lineage(
    tmp_path: Path,
) -> None:
    html = """
    <html><body>
    Flash US Services PMI Business Activity Index: 53.6 (June: 51.2)
    </body></html>
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=html, request=request)

    cfg = settings(
        tmp_path,
        sp_global_pmi_release_url=(
            "https://www.pmi.spglobal.com/Public/Home/PressRelease/fixture-id"
        ),
    )
    provider = SpGlobalPmiProvider(
        ProviderCacheRepository(cfg.database_path),
        cfg,
        transport=httpx.MockTransport(handler),
    )
    result = await provider.fetch(
        expected_period="2026-07", release_date="2026-07-24"
    )
    series = result.data[SERIES_ID]
    assert [row["value"] for row in series["observations"]] == ["51.2", "53.6"]
    assert [row["period"] for row in series["observations"]] == ["2026-06", "2026-07"]
    assert series["raw_lineage_redacted"]["content_sha256"]
    assert series["source_url"].startswith("https://www.pmi.spglobal.com/")


@pytest.mark.asyncio
async def test_sp_global_access_restriction_and_period_mismatch_fail_closed(
    tmp_path: Path,
) -> None:
    async def forbidden(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, request=request)

    cfg = settings(
        tmp_path,
        sp_global_pmi_release_url=(
            "https://www.pmi.spglobal.com/Public/Home/PressRelease/restricted"
        ),
    )
    provider = SpGlobalPmiProvider(
        ProviderCacheRepository(cfg.database_path),
        cfg,
        transport=httpx.MockTransport(forbidden),
    )
    with pytest.raises(ProviderError, match="access_restricted"):
        await provider.fetch(expected_period="2026-07", release_date="2026-07-24")


def test_flash_services_exact_occurrence_accepts_only_official_sp_global_candidate(
    tmp_path: Path,
) -> None:
    html = (
        "Flash US Services PMI Business Activity Index: "
        "53.6 (June: 51.2)"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=html, request=request)

    cfg = settings(
        tmp_path,
        sp_global_pmi_release_url=(
            "https://www.pmi.spglobal.com/Public/Home/PressRelease/official"
        ),
    )
    provider = SpGlobalPmiProvider(
        ProviderCacheRepository(cfg.database_path),
        cfg,
        transport=httpx.MockTransport(handler),
    )
    result = DeterministicActualResolver(
        cfg, providers={"SPGLOBAL": provider}
    ).resolve_event(
        event_key="xtb:146945:2026-07-24",
        event={
            "event_id": "xtb:146945:2026-07-24",
            "reference_period": "Luglio",
        },
        temporal_state={"release_at": "2026-07-24T13:45:00+00:00"},
    )
    assert result["status"] == "SUCCEEDED"
    candidate = result["results"][0]
    assert (
        candidate["value"],
        candidate["previous"],
        candidate["reference_period"],
        candidate["validation_status"],
    ) == ("53.6", "51.2", "2026-07", "accepted")
    assert candidate["raw_lineage_redacted"]["content_sha256"]


def test_stale_calendar_can_resolve_by_stable_occurrence_without_reappearing(
    tmp_path: Path,
) -> None:
    class EmptyCalendar:
        last_provider_results = []

        async def list_events(self, **kwargs):
            del kwargs
            return []

    class OfficialResolver:
        def resolve_event(self, **kwargs):
            assert kwargs["event_key"] == "xtb:146945:2026-07-24"
            return {
                "status": "SUCCEEDED",
                "results": [
                    {
                        "field": "actual",
                        "value": "53.6",
                        "previous": "51.2",
                        "event_metric_id": "flash_services_pmi",
                        "reference_period": "2026-07",
                        "frequency": "monthly",
                        "unit": "index_points",
                        "source": "SPGLOBAL",
                        "publisher": "S&P Global Market Intelligence",
                        "source_url": "https://www.pmi.spglobal.com/Public/Home/PressRelease/id",
                        "canonical_url": "https://www.pmi.spglobal.com/Public/Home/PressRelease/id",
                        "provider_adapter": "SPGLOBAL_OFFICIAL_API",
                        "retrieved_at": NOW.isoformat(),
                        "validation_timestamp": NOW.isoformat(),
                    }
                ],
            }

    adapter = MacroActualLifecycleProviderAdapter(
        settings=settings(tmp_path, event_calendar_catchup_enabled=True),
        event_service=EmptyCalendar(),
        actual_resolver=OfficialResolver(),
        clock=lambda: NOW,
    )
    result = adapter.resolve(
        {
            "entity_key": "xtb:146945:2026-07-24",
            "event_at": "2026-07-24T13:45:00+00:00",
            "payload": {
                "event_id": "xtb:146945:2026-07-24",
                "occurrence_id": "xtb:146945:2026-07-24",
                "name": "Indice PMI dei servizi",
                "reference_period": "Luglio",
                "frequency": "monthly",
                "forecast": 51.5,
                "previous": 51.2,
                "release_at": "2026-07-24T13:45:00+00:00",
            },
            "fields_attempted": ["actual"],
        }
    )
    assert result["status"] == "RESOLVED"
    assert result["datum"]["canonical_event_key"] == "xtb:146945:2026-07-24"
    assert result["datum"]["actual"] == "53.6"
    assert result["datum"]["previous"] == "51.2"
    assert result["datum"]["forecast"] == 51.5
    assert result["datum"]["reference_period"] == "2026-07"
    assert result["datum"]["actual_is_official"] is True
