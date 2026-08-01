from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

import app.services.risk_context_runtime_service as risk_runtime_module
from app.core.config import Settings
from app.models.common import Freshness, ProviderMetadata, ProviderType
from app.models.macro import MacroLatestResponse
from app.providers.cboe_risk_indices_provider import _aggregate_data_as_of
from app.services.data_freshness_service import DataFreshnessService
from app.services.diagnostics_service import (
    DiagnosticsService,
    _attempt_from_runtime_block,
    _calendar_database_evidence,
    _dataset_sla,
    _macro_repository_queries,
    _required_macro_dataset_series,
)
from app.services.multi_source_runtime_service import (
    FACT_TYPES,
    MultiSourceRuntimeService,
)
from app.services.positioning_runtime_service import PositioningRuntimeService
from app.services.risk_context_runtime_service import (
    RiskContextRuntimeService,
    empty_risk_context,
)


NOW = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)

_CALENDAR_PROVIDERS = (
    ("BEA", "BEA Release Schedule"),
    ("BLS", "BLS Release Calendar"),
    ("FEDERAL_RESERVE", "Federal Reserve Calendar"),
)


def _calendar_coverage(
    *,
    requested_dates: list[str],
    by_provider: dict[str, dict],
) -> dict:
    provider_ids = [
        provider_id
        for provider_id, _provider_name in _CALENDAR_PROVIDERS
    ]
    provider_names = sorted(
        provider_name
        for _provider_id, provider_name in _CALENDAR_PROVIDERS
    )
    provider_id_by_name = {
        provider_name: provider_id
        for provider_id, provider_name in _CALENDAR_PROVIDERS
    }
    return {
        "requested_dates": requested_dates,
        "expected_provider_ids": provider_ids,
        "expected_provider_names": provider_names,
        "provider_id_by_name": provider_id_by_name,
        "provider_attempts": [
            {
                "provider_id": provider_id,
                "provider_name": provider_name,
                "query_scope": "country=US",
                "called": True,
                "attempts": 1,
                "successful_attempts": 1,
                "failed_attempts": 0,
                "result": "SUCCESS",
                "not_called_reason": None,
            }
            for provider_id, provider_name in _CALENDAR_PROVIDERS
        ],
        "database_lookup_daily_matrix": {
            "requested_dates": requested_dates,
            "expected_provider_ids": provider_ids,
            "expected_provider_names": provider_names,
            "provider_id_by_name": provider_id_by_name,
            "by_provider": by_provider,
        },
    }


def _settings(tmp_path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "force-db-first.sqlite",
        source_policy_path="config/source_policy.json",
        ai_job_workspace_root=tmp_path / "jobs",
        temp_dir=tmp_path / "temp",
        diagnostics_dir=tmp_path / "diagnostics",
        backups_dir=tmp_path / "backups",
        logs_dir=tmp_path / "logs",
        enable_scheduler=False,
        ai_worker_enabled=False,
        enable_ai_researcher=False,
    )


def test_cboe_aggregate_data_as_of_prefers_earliest_last_trade_time() -> None:
    assert _aggregate_data_as_of(
        {
            "vvix": {
                "last_trade_time": "2026-07-30T16:15:01Z",
                "provider_timestamp": "2026-07-31T05:26:00Z",
            },
            "skew": {
                "last_trade_time": "2026-07-30T17:00:21Z",
                "provider_timestamp": "2026-07-31T05:26:00Z",
            },
        }
    ) == "2026-07-30T16:15:01Z"


def _cot_payload(
    *,
    valid_until: datetime,
    next_refresh_at: datetime,
) -> dict:
    return {
        "status": "found",
        "report_date": "2026-07-24",
        "publication_date": "2026-07-24T19:30:00+00:00",
        "data_as_of": "2026-07-24T19:30:00+00:00",
        "market_name": "MICRO E-MINI NASDAQ-100",
        "source": "CFTC",
        "source_url": (
            "https://www.cftc.gov/dea/newcot/FinFutWk.txt"
        ),
        "retrieved_at": NOW.isoformat(),
        "valid_until": valid_until.isoformat(),
        "next_refresh_at": next_refresh_at.isoformat(),
        "reliability": 0.99,
        "warnings": [],
        "errors": [],
    }


class _ForbiddenCotProvider:
    calls = 0

    async def fetch_nasdaq(self):
        self.calls += 1
        raise AssertionError("valid canonical COT must avoid provider")


class _FreshCotProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def fetch_nasdaq(self):
        self.calls += 1
        return _cot_payload(
            valid_until=NOW + timedelta(days=7),
            next_refresh_at=NOW + timedelta(days=7),
        )


@pytest.mark.asyncio
async def test_cot_force_reads_valid_database_before_provider(
    tmp_path,
) -> None:
    service = PositioningRuntimeService(
        _settings(tmp_path),
        clock=lambda: NOW,
    )
    service._save(
        "cot:nasdaq_100",
        "cot_positioning",
        _cot_payload(
            valid_until=NOW + timedelta(days=1),
            next_refresh_at=NOW + timedelta(days=1),
        ),
        source="CFTC",
    )
    provider = _ForbiddenCotProvider()
    service.cot_provider = provider

    result = await service.cot(refresh="force")

    assert provider.calls == 0
    assert result["cache_used"] is True
    assert result["provider_calls"] == 0
    assert result["database_lookup"] == {
        "performed": True,
        "found": True,
        "data_as_of": "2026-07-24T19:30:00+00:00",
        "content_valid_until": (
            NOW + timedelta(days=1)
        ).isoformat(),
        "refresh_due_at": (
            NOW + timedelta(days=1)
        ).isoformat(),
        "lifecycle_status": "ACTIVE",
        "expired": False,
        "freshness": "VALID",
        "reason_code": "CANONICAL_RECORD_WITHIN_SLA",
    }


@pytest.mark.asyncio
async def test_cot_force_refreshes_expired_database_record(
    tmp_path,
) -> None:
    service = PositioningRuntimeService(
        _settings(tmp_path),
        clock=lambda: NOW,
    )
    service._save(
        "cot:nasdaq_100",
        "cot_positioning",
        _cot_payload(
            valid_until=NOW - timedelta(seconds=1),
            next_refresh_at=NOW - timedelta(seconds=1),
        ),
        source="CFTC",
    )
    provider = _FreshCotProvider()
    service.cot_provider = provider

    result = await service.cot(refresh="force")

    assert provider.calls == 1
    assert result["provider_calls"] == 1
    assert result["database_lookup"]["performed"] is True
    assert result["database_lookup"]["found"] is True
    assert result["database_lookup"]["expired"] is True
    assert result["database_lookup"]["freshness"] != "VALID"
    assert (
        result["database_lookup"]["reason_code"]
        == "CANONICAL_CONTENT_VALID_UNTIL_EXPIRED"
    )


def _risk_payload(
    *,
    valid_until: datetime,
    next_refresh_at: datetime,
) -> dict:
    payload = empty_risk_context(refresh="auto")
    payload.update(
        {
            "status": "available",
            "data_as_of": (NOW - timedelta(minutes=15)).isoformat(),
            "retrieved_at": (NOW - timedelta(minutes=10)).isoformat(),
            "valid_until": valid_until.isoformat(),
            "next_refresh_at": next_refresh_at.isoformat(),
            "vix": {
                "status": "found",
                "value": 16.2,
                "source": "CBOE",
            },
            "vvix": {
                "status": "found",
                "value": 88.4,
                "source": "CBOE",
            },
            "derived_context": {
                "composite_status": "AVAILABLE",
            },
            "source_summary": {
                "selected_sources": {
                    "vix": "CBOE",
                    "vvix": "CBOE",
                },
                "last_known_good_used": False,
            },
            "quality": {
                "quality_score": 0.9,
            },
        }
    )
    return payload


class _RiskRepository:
    def __init__(self, latest: dict) -> None:
        self.value = deepcopy(latest)

    def latest(self):
        return deepcopy(self.value)

    def history(self):
        return [deepcopy(self.value)]

    def count(self):
        return 1

    def append(self, payload):
        self.value = deepcopy(payload)
        return deepcopy(payload)


class _ForbiddenRiskProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def fetch(self):
        self.calls += 1
        raise AssertionError("valid canonical risk record must avoid provider")


class _ObservedRiskProvider:
    def __init__(self, source: str) -> None:
        self.source = source
        self.calls = 0

    async def fetch(self):
        self.calls += 1
        return {
            "status": "found",
            "source": self.source,
            "retrieved_at": NOW.isoformat(),
            "valid_until": (NOW + timedelta(hours=2)).isoformat(),
            "next_refresh_at": (
                NOW + timedelta(hours=2)
            ).isoformat(),
        }


class _NotFoundRiskNormalizer:
    def build(self, **_kwargs):
        payload = empty_risk_context(refresh="force")
        payload["warnings"] = ["controlled_no_data"]
        return payload


class _MacroFacts:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = deepcopy(rows)

    def get_valid_facts_by_type(
        self,
        _fact_type: str,
        *,
        allow_stale: bool = False,
    ) -> list[dict]:
        return deepcopy(self.rows) if allow_stale else []


class _FailedFredVixService:
    def __init__(self, sequence: list[str]) -> None:
        self.sequence = sequence
        self.requested_series: dict[str, tuple[str, ...]] = {}

    async def latest(
        self,
        *,
        force: bool = False,
        requested_series: (
            dict[str, tuple[str, ...]] | None
        ) = None,
    ) -> MacroLatestResponse:
        assert force is True
        self.requested_series = requested_series or {}
        assert self.requested_series == {"FRED": ("VIXCLS",)}
        self.sequence.append("FRED:VIXCLS")
        return MacroLatestResponse(
            provider_results=[
                ProviderMetadata(
                    source="FRED",
                    provider_type=ProviderType.API,
                    freshness=Freshness.UNKNOWN,
                    reliability=0.99,
                    errors=["controlled_vix_primary_failure"],
                )
            ]
        )


class _SequencedRiskProvider:
    def __init__(self, sequence: list[str], name: str) -> None:
        self.sequence = sequence
        self.name = name
        self.calls = 0

    async def fetch(self) -> dict:
        self.calls += 1
        self.sequence.append(self.name)
        return {
            "status": "provider_failed",
            "source": "CBOE",
            "retrieved_at": NOW.isoformat(),
            "valid_until": (
                NOW + timedelta(minutes=15)
            ).isoformat(),
            "errors": ["controlled_cboe_failure"],
        }


def _macro_fact(
    series_id: str,
    *,
    valid_until: datetime,
) -> dict:
    return {
        "fact_key": (
            f"US:{series_id}:latest:official_macro_latest"
        ),
        "fact_type": "official_macro_latest",
        "category": series_id,
        "source": "FRED",
        "provider_type": "API",
        "value": "1",
        "retrieved_at": (NOW - timedelta(minutes=5)).isoformat(),
        "release_at": (NOW - timedelta(minutes=5)).isoformat(),
        "valid_until": valid_until.isoformat(),
        "next_refresh_at": valid_until.isoformat(),
        "raw_payload": {
            "series_id": series_id,
            "data_as_of": (
                NOW - timedelta(minutes=5)
            ).isoformat(),
            "content_valid_until": valid_until.isoformat(),
            "refresh_due_at": valid_until.isoformat(),
        },
    }


@pytest.mark.asyncio
async def test_risk_force_reads_valid_database_before_providers(
    tmp_path,
) -> None:
    repository = _RiskRepository(
        _risk_payload(
            valid_until=NOW + timedelta(hours=1),
            next_refresh_at=NOW + timedelta(hours=1),
        )
    )
    service = RiskContextRuntimeService(
        _settings(tmp_path),
        repository=repository,
        clock=lambda: NOW,
    )
    forbidden = _ForbiddenRiskProvider()
    service.risk_indices_provider = forbidden
    service.vix_futures_provider = forbidden
    service.put_call_provider = forbidden
    service.qqq_options_provider = forbidden

    result, _legacy = await service.snapshot(
        refresh="force",
        macro_snapshot={},
    )

    assert forbidden.calls == 0
    assert result["diagnostics"]["cache_used"] is True
    assert service.last_database_lookup == {
        "performed": True,
        "found": True,
        "data_as_of": (NOW - timedelta(minutes=15)).isoformat(),
        "content_valid_until": (
            NOW + timedelta(hours=1)
        ).isoformat(),
        "refresh_due_at": (
            NOW + timedelta(hours=1)
        ).isoformat(),
        "lifecycle_status": "AVAILABLE",
        "expired": False,
        "freshness": "VALID",
        "reason_code": "CANONICAL_RECORD_WITHIN_SLA",
    }


def test_risk_lookup_does_not_promote_retrieval_to_data_as_of(
    tmp_path,
) -> None:
    payload = _risk_payload(
        valid_until=NOW + timedelta(hours=1),
        next_refresh_at=NOW + timedelta(hours=1),
    )
    payload.pop("data_as_of")
    service = RiskContextRuntimeService(
        _settings(tmp_path),
        repository=_RiskRepository(payload),
        clock=lambda: NOW,
    )

    _latest, freshness = service.lookup_canonical()

    assert freshness.found is True
    assert freshness.usable is False
    assert freshness.evaluation == "MISSING_DATA_AS_OF"
    assert freshness.reason_code == (
        "CANONICAL_RECORD_TIME_NOT_PROVED"
    )


@pytest.mark.asyncio
async def test_expired_vix_attempts_fred_before_cboe_fallback(
    tmp_path,
    monkeypatch,
) -> None:
    cfg = _settings(tmp_path)
    sequence: list[str] = []
    valid_rows = [
        _macro_fact(
            series_id,
            valid_until=NOW + timedelta(hours=1),
        )
        for dataset_id, query in _macro_repository_queries().items()
        for series_id in _required_macro_dataset_series(
            dataset_id,
            query=query,
        )
    ]
    expired_vix = _macro_fact(
        "VIXCLS",
        valid_until=NOW - timedelta(seconds=1),
    )
    diagnostics = object.__new__(DiagnosticsService)
    diagnostics.settings = cfg
    diagnostics.freshness = DataFreshnessService(
        cfg,
        clock=lambda: NOW,
    )
    diagnostics.facts = _MacroFacts(
        [*valid_rows, expired_vix]
    )
    fred = _FailedFredVixService(sequence)
    diagnostics.macro_service = fred
    diagnostics._save_macro = lambda _macro: 0
    vix_freshness = diagnostics.freshness.evaluate_canonical(
        expired_vix,
        max_age=_dataset_sla("vix"),
        data_reference_mode="point_in_time",
    )

    macro, quality = await diagnostics._macro_db_first(
        force=True,
        vix_preflight=(expired_vix, vix_freshness),
    )

    assert fred.requested_series == {"FRED": ("VIXCLS",)}
    assert quality["vix_provider_evidence"] == {
        "provider": "FRED",
        "called": True,
        "attempts": 1,
        "result": "FAILED",
    }
    assert all(
        series.series_id != "VIXCLS"
        for series in macro.series
    )

    risk = RiskContextRuntimeService(
        cfg,
        repository=_RiskRepository(
            _risk_payload(
                valid_until=NOW - timedelta(seconds=1),
                next_refresh_at=NOW - timedelta(seconds=1),
            )
        ),
        clock=lambda: NOW,
    )
    futures = _SequencedRiskProvider(
        sequence,
        "CBOE_VIX_FUTURES",
    )
    put_call = _SequencedRiskProvider(
        sequence,
        "CBOE_PUT_CALL",
    )
    indices = _SequencedRiskProvider(
        sequence,
        "CBOE_RISK_INDICES",
    )
    risk.vix_futures_provider = futures
    risk.put_call_provider = put_call
    risk.risk_indices_provider = indices
    risk.normalizer = _NotFoundRiskNormalizer()
    monkeypatch.setattr(
        risk_runtime_module,
        "build_legacy_risk_sentiment",
        lambda canonical, _legacy: {"status": canonical["status"]},
    )
    fresh_preload = {
        "status": "found",
        "data_as_of": NOW.isoformat(),
        "retrieved_at": NOW.isoformat(),
        "valid_until": (NOW + timedelta(hours=1)).isoformat(),
        "next_refresh_at": (
            NOW + timedelta(hours=1)
        ).isoformat(),
    }

    result, _legacy = await risk.snapshot(
        refresh="force",
        macro_snapshot={},
        preloaded_qqq_options=fresh_preload,
    )

    assert sequence[0] == "FRED:VIXCLS"
    assert sequence.index("FRED:VIXCLS") < sequence.index(
        "CBOE_RISK_INDICES"
    )
    assert indices.calls == 1
    assert risk.last_database_lookup["performed"] is True
    assert risk.last_database_lookup["expired"] is True
    assert result["status"] != "available"


@pytest.mark.asyncio
async def test_vix_recent_retrieval_does_not_revalidate_old_close(
    tmp_path,
) -> None:
    cfg = _settings(tmp_path)
    sequence: list[str] = []
    valid_rows = [
        _macro_fact(
            series_id,
            valid_until=NOW + timedelta(hours=1),
        )
        for dataset_id, query in _macro_repository_queries().items()
        for series_id in _required_macro_dataset_series(
            dataset_id,
            query=query,
        )
    ]
    old_vix = _macro_fact(
        "VIXCLS",
        valid_until=NOW + timedelta(hours=1),
    )
    old_observation = NOW - timedelta(hours=25)
    old_vix["release_at"] = old_observation.isoformat()
    old_vix["retrieved_at"] = NOW.isoformat()
    old_vix["raw_payload"]["data_as_of"] = (
        old_observation.isoformat()
    )
    diagnostics = object.__new__(DiagnosticsService)
    diagnostics.settings = cfg
    diagnostics.freshness = DataFreshnessService(
        cfg,
        clock=lambda: NOW,
    )
    diagnostics.facts = _MacroFacts([*valid_rows, old_vix])
    fred = _FailedFredVixService(sequence)
    diagnostics.macro_service = fred
    diagnostics._save_macro = lambda _macro: 0

    fact, freshness = diagnostics._vix_database_lookup()
    _macro, quality = await diagnostics._macro_db_first(
        force=True,
        vix_preflight=(fact, freshness),
    )

    assert fact == old_vix
    assert freshness.found is True
    assert freshness.usable is False
    assert freshness.expired is True
    assert freshness.evaluation == "INVALID_LIFECYCLE"
    assert freshness.reason_code == "CANONICAL_LIFECYCLE_STALE"
    assert freshness.lifecycle == "STALE"
    assert fred.requested_series == {"FRED": ("VIXCLS",)}
    assert sequence == ["FRED:VIXCLS"]
    assert quality["vix_provider_evidence"]["called"] is True


@pytest.mark.asyncio
async def test_risk_force_refreshes_expired_database_record(
    tmp_path,
    monkeypatch,
) -> None:
    repository = _RiskRepository(
        _risk_payload(
            valid_until=NOW - timedelta(seconds=1),
            next_refresh_at=NOW - timedelta(seconds=1),
        )
    )
    service = RiskContextRuntimeService(
        _settings(tmp_path),
        repository=repository,
        clock=lambda: NOW,
    )
    futures = _ObservedRiskProvider("CBOE Futures")
    put_call = _ObservedRiskProvider("CBOE Put/Call")
    service.vix_futures_provider = futures
    service.put_call_provider = put_call
    service.normalizer = _NotFoundRiskNormalizer()
    monkeypatch.setattr(
        risk_runtime_module,
        "build_legacy_risk_sentiment",
        lambda canonical, _legacy: {"status": canonical["status"]},
    )
    preloaded = {
        "status": "found",
        "data_as_of": (NOW - timedelta(minutes=5)).isoformat(),
        "retrieved_at": NOW.isoformat(),
        "valid_until": (NOW + timedelta(hours=1)).isoformat(),
        "next_refresh_at": (NOW + timedelta(hours=1)).isoformat(),
    }

    result, _legacy = await service.snapshot(
        refresh="force",
        macro_snapshot={},
        preloaded_risk_indices=preloaded,
        preloaded_qqq_options=preloaded,
    )

    assert futures.calls == 1
    assert put_call.calls == 1
    assert service.last_database_lookup["performed"] is True
    assert service.last_database_lookup["found"] is True
    assert service.last_database_lookup["expired"] is True
    assert service.last_database_lookup["freshness"] != "VALID"
    assert (
        service.last_database_lookup["reason_code"]
        == "CANONICAL_CONTENT_VALID_UNTIL_EXPIRED"
    )
    assert result["status"] == "not_found"
    assert result["vix"]["value"] is None
    assert result["vvix"]["value"] is None
    assert result["source_summary"]["last_known_good_used"] is False
    assert result["diagnostics"].get("last_known_good_used") is not True


@pytest.mark.asyncio
async def test_multi_source_honors_canonical_fomc_and_risk_preflight(
    tmp_path,
    monkeypatch,
) -> None:
    service = MultiSourceRuntimeService(_settings(tmp_path))
    calls: list[str] = []

    def block(name: str) -> dict:
        return {
            "status": "not_found",
            "source": name,
            "attempted": False,
            "provider_calls": 0,
            "cache_used": False,
            "fetched_count": 0,
            "materialized_count": 0,
            "warnings": [],
            "errors": [],
        }

    async def run_provider(name, *_args, **_kwargs):
        calls.append(name)
        return block(name)

    async def schedule_chain(*, refresh):
        return {
            name: block(name)
            for name in (
                "investing_holidays",
                "marketbeat_holidays",
                "cme_market_schedule",
                "nasdaq_market_info",
            )
        }

    async def earnings_chain(*, refresh, **_kwargs):
        return {
            "nasdaq_earnings": block("nasdaq_earnings"),
            "fmp_earnings": block("fmp_earnings"),
        }

    async def run_aaii(*, refresh):
        return block("aaii_sentiment")

    monkeypatch.setattr(service, "_run_provider", run_provider)
    monkeypatch.setattr(
        service,
        "_market_schedule_chain",
        schedule_chain,
    )
    monkeypatch.setattr(
        service,
        "_earnings_chain",
        earnings_chain,
    )
    monkeypatch.setattr(service, "_run_aaii", run_aaii)
    monkeypatch.setattr(
        service,
        "_quikstrike_review",
        lambda **_kwargs: block("quikstrike_review"),
    )
    canonical_lookup = {
        "performed": True,
        "found": True,
        "data_as_of": NOW.isoformat(),
        "content_valid_until": (
            NOW + timedelta(hours=1)
        ).isoformat(),
        "refresh_due_at": (
            NOW + timedelta(hours=1)
        ).isoformat(),
        "expired": False,
        "freshness": "VALID",
        "reason_code": "CANONICAL_RECORD_WITHIN_SLA",
    }

    def preloaded(source: str) -> dict:
        return {
            "status": "available",
            "source": source,
            "attempted": False,
            "provider_calls": 0,
            "cache_used": True,
            "fetched_count": 0,
            "materialized_count": 1,
            "database_lookup": dict(canonical_lookup),
            "warnings": [],
            "errors": [],
        }

    result = await service.snapshot(
        refresh="force",
        preloaded_blocks={
            "investing_fed_rate_monitor": preloaded(
                "Investing.com Fed Rate Monitor"
            ),
            "cboe_risk_indices": preloaded("CBOE"),
        },
    )

    assert "investing_fed_rate_monitor" not in calls
    assert "cboe_risk_indices" not in calls
    assert "nasdaq_qqq_options" in calls
    assert result["blocks"]["investing_fed_rate_monitor"][
        "database_lookup"
    ] == canonical_lookup
    assert result["blocks"]["cboe_risk_indices"][
        "database_lookup"
    ] == canonical_lookup


@pytest.mark.asyncio
async def test_multi_source_propagates_observed_database_lifecycle(
    tmp_path,
) -> None:
    service = MultiSourceRuntimeService(_settings(tmp_path))
    observed_at = datetime.now(UTC).replace(microsecond=0)
    valid_until = observed_at + timedelta(hours=1)
    service._save_fact(
        "cboe_risk_indices",
        FACT_TYPES["cboe_risk_indices"],
        {
            "status": "found",
            "source": "CBOE",
            "data_as_of": observed_at.isoformat(),
            "retrieved_at": observed_at.isoformat(),
            "valid_until": valid_until.isoformat(),
            "next_refresh_at": valid_until.isoformat(),
            "indices": {"VIX": {"value": 16.2}},
            "warnings": [],
            "errors": [],
        },
        source="CBOE",
    )
    calls = 0

    async def forbidden_fetch() -> dict:
        nonlocal calls
        calls += 1
        raise AssertionError("valid canonical row must skip provider")

    result = await service._run_provider(
        "cboe_risk_indices",
        FACT_TYPES["cboe_risk_indices"],
        forbidden_fetch,
        item_count=lambda payload: len(payload.get("indices") or {}),
        enabled=True,
        source="CBOE",
        refresh="force",
    )

    assert calls == 0
    assert result["database_lookup"]["lifecycle_status"] == "ACTIVE"


@pytest.mark.asyncio
async def test_cboe_cache_uses_observation_time_not_recent_retrieval(
    tmp_path,
) -> None:
    service = MultiSourceRuntimeService(_settings(tmp_path))
    now = datetime.now(UTC).replace(microsecond=0)
    stale_observation = now - timedelta(hours=3)
    service._save_fact(
        "cboe_risk_indices",
        FACT_TYPES["cboe_risk_indices"],
        {
            "status": "found",
            "source": "CBOE",
            "retrieved_at": now.isoformat(),
            "data_as_of": now.isoformat(),
            "valid_until": (now + timedelta(hours=1)).isoformat(),
            "next_refresh_at": (
                now + timedelta(hours=1)
            ).isoformat(),
            "indices": {
                "vvix": {
                    "current_price": 90.0,
                    "provider_timestamp": now.isoformat(),
                    "last_trade_time": stale_observation.isoformat(),
                },
                "skew": {
                    "current_price": 150.0,
                    "provider_timestamp": now.isoformat(),
                    "last_trade_time": stale_observation.isoformat(),
                },
            },
            "warnings": [],
            "errors": [],
        },
        source="CBOE",
    )
    calls = 0

    async def current_fetch() -> dict:
        nonlocal calls
        calls += 1
        return {
            "status": "found",
            "source": "CBOE",
            "retrieved_at": now.isoformat(),
            "valid_until": (now + timedelta(minutes=15)).isoformat(),
            "indices": {
                "vvix": {
                    "current_price": 91.0,
                    "provider_timestamp": now.isoformat(),
                    "last_trade_time": now.isoformat(),
                }
            },
            "warnings": [],
            "errors": [],
        }

    result = await service._run_provider(
        "cboe_risk_indices",
        FACT_TYPES["cboe_risk_indices"],
        current_fetch,
        item_count=lambda payload: len(payload.get("indices") or {}),
        enabled=True,
        source="CBOE",
        refresh="force",
    )

    assert calls == 1
    assert result["attempted"] is True
    assert result["provider_calls"] == 1
    assert result["cache_used"] is False
    assert result["database_lookup"]["expired"] is True
    assert result["database_lookup"]["freshness"] == "SLA_EXPIRED"
    assert result["database_lookup"]["data_as_of"] == (
        stale_observation.isoformat()
    )


def test_expired_runtime_cache_hit_is_not_a_valid_database_selection() -> None:
    attempt = _attempt_from_runtime_block(
        "CONTROLLED_PROVIDER",
        {
            "status": "found",
            "cache_used": True,
            "attempted": False,
            "provider_calls": 0,
            "database_lookup": {
                "performed": True,
                "found": True,
                "data_as_of": "2026-07-29T12:00:00+00:00",
                "content_valid_until": "2026-07-30T10:00:00+00:00",
                "refresh_due_at": "2026-07-30T10:00:00+00:00",
                "expired": True,
                "freshness": "EXPIRED_CONTENT_VALID_UNTIL",
                "reason_code": "CANONICAL_CONTENT_VALID_UNTIL_EXPIRED",
            },
        },
    )

    assert attempt["called"] is False
    assert attempt["execution_origin"] == "OBSERVED_SKIP"
    assert attempt["not_called_reason"] == (
        "CANONICAL_CONTENT_VALID_UNTIL_EXPIRED"
    )
    assert attempt["not_called_reason"] != (
        "VALID_DATABASE_RECORD_SELECTED"
    )


def test_calendar_provider_call_preserves_expired_database_lookup() -> None:
    now = datetime.now(UTC)
    day = now.date().isoformat()
    evidence = _calendar_database_evidence(
        _calendar_coverage(
            requested_dates=[day],
            by_provider={
                provider_name: {
                    "by_date": {
                        day: {
                            "status": "VERIFIED_COMPLETE",
                            "valid_until": (
                                now - timedelta(minutes=1)
                            ).isoformat(),
                            "next_revision_check_at": (
                                now - timedelta(minutes=1)
                            ).isoformat(),
                        }
                    }
                }
                for _provider_id, provider_name
                in _CALENDAR_PROVIDERS
            },
        ),
        events=[],
        provider_called=True,
    )

    assert evidence["found"] is True
    assert evidence["expired"] is True
    assert evidence["freshness"] == (
        "EXPIRED_CONTENT_VALID_UNTIL"
    )


def test_calendar_database_evidence_aggregates_every_provider_day() -> None:
    now = datetime.now(UTC)
    today = now.date().isoformat()
    tomorrow = (now + timedelta(days=1)).date().isoformat()
    evidence = _calendar_database_evidence(
        _calendar_coverage(
            requested_dates=[today, tomorrow],
            by_provider={
                "BEA Release Schedule": {
                    "by_date": {
                        today: {
                            "status": "VERIFIED_COMPLETE",
                            "valid_until": (
                                now + timedelta(hours=1)
                            ).isoformat(),
                            "next_revision_check_at": (
                                now + timedelta(hours=1)
                            ).isoformat(),
                        }
                    }
                },
                "BLS Release Calendar": {
                    "by_date": {
                        tomorrow: {
                            "status": "PARTIAL",
                            "valid_until": (
                                now + timedelta(hours=1)
                            ).isoformat(),
                            "next_revision_check_at": (
                                now + timedelta(hours=1)
                            ).isoformat(),
                        }
                    }
                },
            },
        ),
        events=[],
        provider_called=True,
    )

    assert evidence["found"] is False
    assert evidence["expired"] is False
    assert evidence["freshness"] == "NOT_FOUND"
    assert evidence["complete"] is False
    summary = evidence["summary"]
    assert summary["providers_expected"] == 3
    assert summary["providers_evaluated"] == 2
    assert summary["provider_ids_expected"] == [
        "BEA",
        "BLS",
        "FEDERAL_RESERVE",
    ]
    assert summary["observations_expected"] == 6
    assert summary["observations_evaluated"] == 2
    assert summary["observations_missing"] == 4
    assert summary["records_terminal"] == 1
    assert summary["records_missing_or_incomplete"] == 1
    assert summary["records_expired"] == 0
    assert summary["window_start"] == today
    assert summary["window_end"] == tomorrow


def test_calendar_database_evidence_uses_weakest_deadline() -> None:
    now = datetime.now(UTC)
    today = now.date().isoformat()
    tomorrow = (now + timedelta(days=1)).date().isoformat()
    evidence = _calendar_database_evidence(
        _calendar_coverage(
            requested_dates=[today, tomorrow],
            by_provider={
                provider_name: {
                    "by_date": {
                        today: {
                            "status": "VERIFIED_COMPLETE",
                            "valid_until": (
                                now + timedelta(hours=1)
                            ).isoformat(),
                            "next_revision_check_at": (
                                now + timedelta(hours=1)
                            ).isoformat(),
                        },
                        tomorrow: {
                            "status": "VERIFIED_EMPTY",
                            "valid_until": (
                                now - timedelta(minutes=1)
                            ).isoformat(),
                            "next_revision_check_at": (
                                now + timedelta(hours=1)
                            ).isoformat(),
                        }
                    }
                }
                for _provider_id, provider_name
                in _CALENDAR_PROVIDERS
            },
        ),
        events=[],
        provider_called=True,
    )

    assert evidence["found"] is True
    assert evidence["expired"] is True
    assert evidence["freshness"] == (
        "EXPIRED_CONTENT_VALID_UNTIL"
    )
    assert evidence["summary"]["records_expired"] == 3
