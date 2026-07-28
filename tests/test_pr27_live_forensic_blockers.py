from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.services.market_fact_repository import MarketFactRepository
from app.services.news_intelligence_service import build_news_context
from app.services.research_scheduler_service import ResearchSchedulerService


NOW = datetime(2026, 7, 27, 13, 28, tzinfo=UTC)
NY = ZoneInfo("America/New_York")


def settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "pr27-live-forensic.sqlite",
        event_calendar_timezone="America/New_York",
    )


@pytest.mark.parametrize(
    ("occurrence_id", "name", "actual", "forecast", "previous", "period"),
    (
        (
            "xtb:146392:2026-07-24",
            "New Home Sales",
            628.0,
            610.0,
            618.0,
            "2026-06",
        ),
        (
            "xtb:146945:2026-07-24",
            "Flash Services PMI",
            53.6,
            52.0,
            51.2,
            "2026-07",
        ),
    ),
)
def test_exact_occurrence_actual_survives_provider_force_absence(
    tmp_path: Path,
    occurrence_id: str,
    name: str,
    actual: float,
    forecast: float,
    previous: float,
    period: str,
) -> None:
    repository = MarketFactRepository(settings(tmp_path), clock=lambda: NOW)
    canonical = {
        "event_id": occurrence_id,
        "occurrence_id": occurrence_id,
        "name": name,
        "country": "US",
        "category": "Economic",
        "reference_period": period,
        "frequency": "MONTHLY",
        "date": "2026-07-24",
        "time_utc": "2026-07-24T14:00:00+00:00",
        "release_at": "2026-07-24T14:00:00+00:00",
        "impact": "HIGH",
        "actual": actual,
        "source": "XTB Economic Calendar",
        "source_url": "https://www.xtb.com/en/market-analysis/economic-calendar",
        "reliability": 0.9,
        "enrichment": {
            "forecast": forecast,
            "previous": previous,
            "actual": actual,
        },
    }
    assert repository.upsert_economic_event(
        canonical,
        occurrence_id,
    )

    provider_force_projection = {
        **canonical,
        "occurrence_id": None,
        "reference_period": None,
        "actual": None,
        "enrichment": {
            "forecast": None,
            "previous": None,
            "actual": None,
        },
    }
    repository.upsert_economic_event(
        provider_force_projection,
        occurrence_id,
    )

    rows = repository.economic_event_payloads(
        country="US",
        start_date="2026-07-24",
        end_date="2026-07-24",
    )
    matching = [
        row
        for row in rows
        if row.get("event_id") == occurrence_id
    ]
    assert len(matching) == 1
    delivered = matching[0]
    assert delivered["occurrence_id"] == occurrence_id
    assert float(delivered["actual"]) == actual
    assert float(delivered["enrichment"]["forecast"]) == forecast
    assert float(delivered["enrichment"]["previous"]) == previous
    assert delivered["reference_period"] == period


class _ProviderScopedCalendar:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.last_provider_results: list[Any] = []
        self.last_provider_coverage_proofs: list[dict[str, Any]] = []

    def coverage_targets(self, **_: Any) -> list[dict[str, str]]:
        return [
            {
                "provider_name": "BLS Release Calendar",
                "query_scope": "country=US",
            },
            {
                "provider_name": "BEA Release Schedule",
                "query_scope": "country=US",
            },
        ]

    def list_events(
        self,
        *,
        start: datetime,
        end: datetime,
        provider_names: list[str],
        **_: Any,
    ) -> list[dict[str, Any]]:
        provider = provider_names[0]
        self.calls.append(provider)
        first = start.astimezone(NY).date()
        last = (end.astimezone(NY) - datetime.resolution).date()
        requested = [
            first + timedelta(days=offset)
            for offset in range((last - first).days + 1)
        ]
        covered = (
            requested
            if provider == "BLS Release Calendar"
            else [day for day in requested if day <= NOW.astimezone(NY).date()]
        )
        event_day = date(2026, 7, 24)
        rows = (
            [
                {
                    "event_id": f"{provider}:release",
                    "occurrence_id": f"{provider}:release:2026-07-24",
                    "name": f"{provider} release",
                    "country": "US",
                    "category": "Economic",
                    "date": event_day.isoformat(),
                    "time_utc": "2026-07-24T12:30:00+00:00",
                    "release_at": "2026-07-24T12:30:00+00:00",
                    "impact": "HIGH",
                    "source": provider,
                    "source_url": (
                        "https://www.bls.gov/news.release/"
                        if provider == "BLS Release Calendar"
                        else "https://www.bea.gov/news/schedule"
                    ),
                    "reliability": 0.9,
                }
            ]
            if event_day in covered
            else []
        )
        empty_dates = [
            day.isoformat() for day in covered if day != event_day
        ]
        self.last_provider_results = [SimpleNamespace(errors=[])]
        self.last_provider_coverage_proofs = [
            {
                "provider_name": provider,
                "query_scope": "country=US",
                "request_succeeded": True,
                "scope_match": True,
                "pagination_complete": True,
                "parsing_succeeded": True,
                "records_valid": True,
                "expected_sources_complete": True,
                "covered_dates": [day.isoformat() for day in covered],
                "authentic_empty_dates": empty_dates,
            }
        ]
        return rows


def test_rollover_coverage_is_provider_scoped_and_future_proof_is_not_fabricated(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path)
    calendar = _ProviderScopedCalendar()
    result = ResearchSchedulerService(
        cfg,
        clock=lambda: NOW,
    )._seed_canonical_schedule_gaps(
        schedule_acquire=calendar.list_events,
        now=NOW,
    )

    assert sorted(calendar.calls) == [
        "BEA Release Schedule",
        "BLS Release Calendar",
    ]
    assert len(result["requested_dates"]) == 21
    with connect_sqlite(cfg.database_path) as conn:
        grouped = conn.execute(
            """
            SELECT provider_name,COUNT(DISTINCT coverage_date) AS days
            FROM event_calendar_coverage
            GROUP BY provider_name
            ORDER BY provider_name
            """
        ).fetchall()
        future_bea = conn.execute(
            """
            SELECT status FROM event_calendar_coverage
            WHERE provider_name='BEA Release Schedule'
              AND coverage_date='2026-07-28'
            """
        ).fetchone()
    assert [(row["provider_name"], row["days"]) for row in grouped] == [
        ("BEA Release Schedule", 21),
        ("BLS Release Calendar", 21),
    ]
    assert future_bea["status"] == "PARTIAL"
    assert "2026-07-28" in result["partial_coverage_days"]


def test_news_contract_is_lossless_and_does_not_infer_distributor() -> None:
    common = {
        "title": "Apple earnings and guidance update",
        "publisher": "Investor's Business Daily",
        "source": "Investor's Business Daily",
        "source_url": "https://www.investors.com/research/apple-update/",
        "canonical_url": "https://www.investors.com/research/apple-update/",
        "provider": "licensed-news-feed",
        "provider_type": "API",
        "retrieved_at": "2026-07-27T12:01:00+00:00",
        "reliability": 0.9,
    }
    context = build_news_context(
        [
            {
                **common,
                "published_at": "2026-07-27T12:00:00+00:00",
                "aggregator_url": (
                    "https://finance.yahoo.com/news/apple-update"
                ),
            },
            {
                **common,
                "title": "Apple earnings and guidance update: second edition",
                "published_at": "2026-07-27T12:05:00+00:00",
                "retrieved_at": "2026-07-27T12:06:00+00:00",
                "content": "Provider-supplied second-edition body.",
                "distribution_source": "Yahoo Finance",
            },
        ],
        now=NOW,
    )
    delivered = context["latest"]
    assert len(delivered) == 2
    first = next(
        item for item in delivered if item["published_at"].startswith("2026-07-27T12:00")
    )
    second = next(
        item for item in delivered if item["published_at"].startswith("2026-07-27T12:05")
    )
    assert first["publisher"] == "Investor's Business Daily"
    assert first["distribution_source"] is None
    assert first["content"] is None
    assert first["content_availability"] == "SOURCE_NOT_PROVIDED"
    assert first["provenance"]["distributor"] is None
    assert first["lineage"]["article_id"] == first["article_id"]
    assert second["distribution_source"] == "Yahoo Finance"
    assert second["content_availability"] == "AVAILABLE"
    assert second["content"] == "Provider-supplied second-edition body."
