from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.models.common import (
    Freshness,
    ProviderMetadata,
    ProviderResult,
    ProviderType,
)
from app.services.deterministic_actual_resolver import (
    DeterministicActualResolver,
)
from app.services.event_driven_lifecycle_service import (
    LifecycleRepository,
    compute_datum_lifecycle,
)
from app.services.event_service import EventService
from app.services.lifecycle_due_resolver import (
    DeterministicLifecycleDueResolver,
    existing_lifecycle_provider_adapters,
)
from app.services.market_context_outbox_service import (
    MarketContextOutboxRepository,
)
from app.services.market_context_snapshot_repository import (
    MarketContextSnapshotRepository,
)
from app.services.research_scheduler_service import ResearchSchedulerService
from app.services.temporal_domain_service import canonical_event_key


ROOT = Path(__file__).resolve().parents[1]
START = datetime(2026, 7, 24, 10, tzinfo=UTC)
RELEASE = datetime(2026, 7, 24, 9, tzinfo=UTC)


def cfg(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_path": tmp_path / "market.sqlite",
        "source_policy_path": ROOT / "config" / "source_policy.json",
        "model_pricing_path": ROOT / "config" / "model_pricing.json",
        "ai_job_workspace_root": tmp_path / "jobs",
        "codex_workspace_dir": tmp_path / "codex",
        "environment": "test",
        "enable_scheduler": True,
        "research_scheduler_enabled": True,
        "lifecycle_due_scanner_enabled": True,
        "lifecycle_startup_catchup_hours": 24,
        "lifecycle_retry_deadline_hours": 24,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def occurrence(
    *,
    release: datetime = RELEASE,
    metric_id: str = "headline_cpi_mom",
    provider_event_id: str = "bls-cpi",
    actual: Any = None,
) -> dict[str, Any]:
    return {
        "event_id": f"{provider_event_id}:{release.date().isoformat()}",
        "provider": "BLS Release Calendar",
        "provider_event_id": provider_event_id,
        "name": "Consumer Price Index",
        "country": "US",
        "category": "CPI",
        "metric_id": metric_id,
        "reference_period": "2026-07",
        "frequency": "monthly",
        "date": release.date().isoformat(),
        "time_utc": release.isoformat(),
        "release_at": release.isoformat(),
        "impact": "HIGH",
        "actual": actual,
        "source": "BLS Release Calendar",
        "source_url": "https://www.bls.gov/schedule/",
        "reliability": 0.99,
    }


class _ControlledCalendarProvider:
    def __init__(
        self,
        events: list[dict[str, Any]],
        *,
        errors: list[str] | None = None,
    ) -> None:
        self.events = events
        self.errors = list(errors or [])
        self.calls = 0

    async def fetch_safe(self) -> ProviderResult:
        self.calls += 1
        return ProviderResult(
            metadata=ProviderMetadata(
                source="BLS Release Calendar",
                provider_type=ProviderType.API,
                retrieved_at=START,
                data_as_of=START,
                freshness=Freshness.RECENT,
                reliability=0.99 if not self.errors else 0,
                errors=self.errors,
            ),
            data=self.events,
        )


class _RecordingEventService(EventService):
    def __init__(self, provider: _ControlledCalendarProvider) -> None:
        super().__init__(providers=[provider])
        self.list_calls: list[dict[str, Any]] = []
        self.upcoming_calls = 0

    async def list_events(self, *args: Any, **kwargs: Any):
        self.list_calls.append(dict(kwargs))
        return await super().list_events(*args, **kwargs)

    async def upcoming(self, *args: Any, **kwargs: Any):
        self.upcoming_calls += 1
        return await super().upcoming(*args, **kwargs)


class _ControlledBlsProvider:
    source = "BLS"

    def __init__(
        self,
        *,
        published: bool = True,
        temporary_failure: bool = False,
    ) -> None:
        self.published = published
        self.temporary_failure = temporary_failure
        self.calls = 0

    async def fetch(self) -> ProviderResult:
        self.calls += 1
        if self.temporary_failure:
            raise TimeoutError("controlled BLS timeout")
        observations = [
            {
                "period": "2026-06",
                "value": "300.0",
                "release_vintage": "initial",
            }
        ]
        if self.published:
            observations.append(
                {
                    "period": "2026-07",
                    "value": "303.0",
                    "release_vintage": "initial",
                }
            )
        return ProviderResult(
            metadata=ProviderMetadata(
                source="BLS",
                provider_type=ProviderType.API,
                retrieved_at=START,
                data_as_of=START,
                freshness=Freshness.RECENT,
                reliability=0.99,
            ),
            data={
                "CUSR0000SA0": {
                    "series_id": "CUSR0000SA0",
                    "observations": observations,
                    "frequency": "monthly",
                    "seasonal_adjustment": "SA",
                    "units": "index",
                    "source": "BLS",
                    "source_url": "https://www.bls.gov/cpi/",
                    "canonical_url": "https://www.bls.gov/cpi/",
                    "source_domain": "bls.gov",
                    "provider_adapter": "BLS_OFFICIAL_API",
                    "official_adapter": True,
                }
            },
        )


class _ControlledBeaProvider:
    source = "BEA"

    def __init__(self) -> None:
        self.calls = 0

    async def fetch(self) -> ProviderResult:
        self.calls += 1
        return ProviderResult(
            metadata=ProviderMetadata(
                source="BEA",
                provider_type=ProviderType.API,
                retrieved_at=START,
                data_as_of=START,
                freshness=Freshness.RECENT,
                reliability=0.99,
            ),
            data={
                "BEA:PCE_PRICE_INDEX": {
                    "series_id": "BEA:PCE_PRICE_INDEX",
                    "observations": [
                        {
                            "period": "2026-06",
                            "value": "100.0",
                            "release_vintage": "initial",
                        },
                        {
                            "period": "2026-07",
                            "value": "101.0",
                            "release_vintage": "initial",
                        },
                    ],
                    "frequency": "monthly",
                    "seasonal_adjustment": "SA",
                    "units": "index",
                    "source": "BEA",
                    "source_url": "https://www.bea.gov/data/",
                    "canonical_url": "https://www.bea.gov/data/",
                    "source_domain": "bea.gov",
                    "provider_adapter": "BEA_OFFICIAL_API",
                    "official_adapter": True,
                }
            },
        )


class _NoOtherProviders:
    def __getattr__(self, name: str):
        async def fail(*_args: Any, **_kwargs: Any):
            raise AssertionError(f"unexpected provider path: {name}")

        return fail


def wired_resolver(
    settings: Settings,
    *,
    calendar: _ControlledCalendarProvider,
    official: Any,
    now: datetime = START,
) -> tuple[DeterministicLifecycleDueResolver, _RecordingEventService]:
    event_service = _RecordingEventService(calendar)
    actual_resolver = DeterministicActualResolver(
        settings,
        providers={str(official.source): official},
    )
    adapters = existing_lifecycle_provider_adapters(
        macro_service=_NoOtherProviders(),
        event_service=event_service,
        nasdaq_data_service=_NoOtherProviders(),
        settings=settings,
        official_actual_resolver=actual_resolver,
        clock=lambda: now,
    )
    return (
        DeterministicLifecycleDueResolver(
            settings,
            clock=lambda: now,
            adapters=adapters,
        ),
        event_service,
    )


def seed_due(
    settings: Settings,
    event: dict[str, Any],
    *,
    now: datetime = START,
    fields: list[str] | None = None,
) -> dict[str, Any]:
    key = canonical_event_key(event)
    payload = {
        **event,
        "canonical_event_key": key,
        "valid_until": (
            datetime.fromisoformat(str(event["release_at"]))
            + timedelta(minutes=30)
        ).isoformat(),
    }
    lifecycle = compute_datum_lifecycle(
        "macro_actual",
        key,
        payload,
        settings=settings,
        now=now,
        fields_attempted=fields or ["actual"],
    )
    return LifecycleRepository(settings, clock=lambda: now).upsert(
        lifecycle,
        payload=payload,
    )


def baseline_snapshot(
    settings: Settings,
    event: dict[str, Any],
) -> MarketContextSnapshotRepository:
    snapshots = MarketContextSnapshotRepository(settings)
    snapshots.save_next(
        symbol="MNQ",
        refresh_mode="macro_actual_wiring_baseline",
        debug_payload={
            "symbol": "MNQ",
            "generated_at_utc": START.isoformat(),
            "data_as_of": START.isoformat(),
            "event_calendar": {
                "critical_macro_events": [event],
                "fed_communications": [],
                "other_economic_events": [],
            },
            "events_today": [event],
            "research": {"status": "NOT_REQUIRED"},
            "ai_enrichment": {"status": "NOT_REQUIRED"},
            "readiness": {"section_status": {}},
        },
        ai_enrichment={"status": "NOT_REQUIRED"},
    )
    return snapshots


def test_real_wiring_recovers_same_past_occurrence_provider_first(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    past = occurrence()
    future = occurrence(release=RELEASE + timedelta(days=1))
    calendar = _ControlledCalendarProvider([future, past])
    official = _ControlledBlsProvider()
    resolver, event_service = wired_resolver(
        settings,
        calendar=calendar,
        official=official,
    )
    snapshots = baseline_snapshot(settings, past)
    seed_due(settings, past)
    ai_calls: list[list[dict[str, Any]]] = []
    scheduler = ResearchSchedulerService(settings, clock=lambda: START)

    first = scheduler.startup_catch_up(
        resolver=resolver.resolve,
        ai_enqueue=ai_calls.append,
    )

    assert first["resolved"]
    assert first["ai_invocations"] == 0
    assert ai_calls == []
    assert official.calls == 1
    assert event_service.upcoming_calls == 0
    assert len(event_service.list_calls) == 1
    assert event_service.list_calls[0]["start"] >= RELEASE - timedelta(
        minutes=1
    )
    assert event_service.list_calls[0]["end"] <= RELEASE + timedelta(
        minutes=1
    )
    latest = snapshots.latest("MNQ")
    assert latest is not None and latest["revision"] == 2
    recent = latest["consumer_payload"]["event_risk"][
        "recently_released_events"
    ]
    assert len(recent) == 1
    assert recent[0]["canonical_event_key"] == canonical_event_key(past)
    assert recent[0]["actual"] == "1.0"
    assert all(
        item.get("release_at") != future["release_at"]
        for item in recent
    )
    assert len(
        MarketContextOutboxRepository(settings).list_events(status=None)
    ) == 1

    second = scheduler.startup_catch_up(
        resolver=resolver.resolve,
        ai_enqueue=ai_calls.append,
    )
    assert second["claimed"] == 0
    assert second["ai_invocations"] == 0
    assert official.calls == 1
    assert snapshots.latest("MNQ")["revision"] == 2
    assert len(
        MarketContextOutboxRepository(settings).list_events(status=None)
    ) == 1


def test_calendar_without_published_actual_enters_backoff(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    event = occurrence()
    official = _ControlledBlsProvider(published=False)
    resolver, _ = wired_resolver(
        settings,
        calendar=_ControlledCalendarProvider([event]),
        official=official,
    )
    item = seed_due(settings, event)

    first = resolver.resolve(item)

    assert first["status"] == "DEFERRED"
    assert first["ai_eligible"] is False
    assert first["datum"].get("actual") is None
    assert first["lifecycle"].freshness_state == "NO_DATA_BACKOFF"
    assert (
        first["lifecycle"].next_retry_at
        == first["lifecycle"].negative_cache_expires_at
    )


def test_real_wiring_routes_bea_metric_to_official_bea_provider(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    event = occurrence(metric_id="headline_pce_mom")
    event.update(
        {
            "provider": "BEA Release Calendar",
            "name": "PCE M/M",
            "category": "PCE",
            "source": "BEA Release Calendar",
            "source_url": "https://www.bea.gov/news/schedule",
        }
    )
    official = _ControlledBeaProvider()
    resolver, _ = wired_resolver(
        settings,
        calendar=_ControlledCalendarProvider([event]),
        official=official,
    )
    result = resolver.resolve(seed_due(settings, event))

    assert result["status"] == "RESOLVED"
    assert result["datum"]["actual"] == "1.0"
    assert result["datum"]["source"] == "BEA"
    assert official.calls == 1


def test_calendar_row_never_promotes_its_unverified_actual(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    event = occurrence(
        metric_id="unsupported_calendar_metric",
        actual=999,
    )
    resolver, _ = wired_resolver(
        settings,
        calendar=_ControlledCalendarProvider([event]),
        official=_ControlledBlsProvider(),
    )
    result = resolver.resolve(seed_due(settings, event))

    assert result["status"] == "NO_DATA"
    assert result["ai_eligible"] is False
    assert result.get("datum") is None


def test_partial_official_resolution_exposes_only_residual_fields(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    event = occurrence()
    resolver, _ = wired_resolver(
        settings,
        calendar=_ControlledCalendarProvider([event]),
        official=_ControlledBlsProvider(),
    )
    result = resolver.resolve(
        seed_due(
            settings,
            event,
            fields=["actual", "consensus"],
        )
    )

    assert result["status"] == "PARTIAL"
    assert result["datum"]["actual"] == "1.0"
    assert result["missing_fields"] == ["consensus"]
    assert result["ai_eligible"] is False


def test_occurrence_beyond_retry_deadline_is_no_data_without_ai(
    tmp_path: Path,
) -> None:
    now = START
    release = now - timedelta(hours=25)
    settings = cfg(
        tmp_path,
        lifecycle_startup_catchup_hours=24,
        lifecycle_retry_deadline_hours=24,
    )
    event = occurrence(
        release=release,
        metric_id="unsupported_calendar_metric",
    )
    official = _ControlledBlsProvider()
    resolver, event_service = wired_resolver(
        settings,
        calendar=_ControlledCalendarProvider([event]),
        official=official,
        now=now,
    )
    result = resolver.resolve(seed_due(settings, event, now=now))

    assert result["status"] == "NO_DATA"
    assert result["data_outcome"] == "NO_DATA"
    assert result["retry_deadline_exhausted"] is True
    assert result["ai_eligible"] is False
    assert official.calls == 0
    assert event_service.list_calls == []


def test_temporary_official_provider_failure_is_negative_cached(
    tmp_path: Path,
) -> None:
    settings = cfg(tmp_path)
    event = occurrence()
    official = _ControlledBlsProvider(temporary_failure=True)
    resolver, _ = wired_resolver(
        settings,
        calendar=_ControlledCalendarProvider([event]),
        official=official,
    )
    item = seed_due(settings, event)

    first = resolver.resolve(item)
    cached_item = {
        **item,
        **first["lifecycle"].as_dict(),
        "freshness_state": "NO_DATA_BACKOFF",
    }
    second = resolver.resolve(cached_item)

    assert first["status"] == second["status"] == "DEFERRED"
    assert second["reason"] == "provider_negative_cache_active"
    assert official.calls == 1
