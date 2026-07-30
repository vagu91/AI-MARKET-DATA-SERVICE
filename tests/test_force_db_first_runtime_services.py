from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

import app.services.risk_context_runtime_service as risk_runtime_module
from app.core.config import Settings
from app.models.common import Freshness, ProviderMetadata, ProviderType
from app.models.macro import MacroLatestResponse
from app.services.data_freshness_service import DataFreshnessService
from app.services.diagnostics_service import (
    DiagnosticsService,
    MACRO_ACCOUNTING_SERIES,
    VIX_MAX_AGE,
    _attempt_from_runtime_block,
    _calendar_database_evidence,
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
        return {
            "status": "not_found",
            "diagnostics": {"provider_calls": 0},
            "history": {},
            "quality": {"quality_score": 0.0},
            "warnings": ["controlled_no_data"],
        }


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


@pytest.mark.asyncio
async def test_expired_vix_attempts_fred_before_cboe_fallback(
    tmp_path,
    monkeypatch,
) -> None:
    cfg = _settings(tmp_path)
    sequence: list[str] = []
    valid_rows = [
        _macro_fact(
            series_ids[0],
            valid_until=NOW + timedelta(hours=1),
        )
        for series_ids in MACRO_ACCOUNTING_SERIES.values()
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
        max_age=VIX_MAX_AGE,
        data_reference_mode="point_in_time",
    )

    macro, quality = await diagnostics._macro_db_first(
        force=True,
        vix_preflight=(expired_vix, vix_freshness),
    )

    assert fred.requested_series == {"FRED": ("VIXCLS",)}
    assert quality["vix_provider_evidence"] == {
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

    await service.snapshot(
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
        {
            "database_lookup_daily_matrix": {
                "by_provider": {
                    "controlled": {
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
                }
            }
        },
        events=[],
        provider_called=True,
    )

    assert evidence["found"] is True
    assert evidence["expired"] is True
    assert evidence["freshness"] == "REFRESH_DUE"
