from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from app.core.config import Settings
from app.models.macro import MacroLatestResponse
from app.services.data_freshness_service import DataFreshnessService
from app.services.diagnostics_service import (
    DiagnosticsService,
    _calendar_database_evidence,
    _macro_repository_queries,
    _required_macro_dataset_series,
)
from app.services.request_provider_accounting import (
    RequestProviderAccountingCollector,
    _calendar_database_lookup_summary_valid,
    _provider_flow_valid,
    provider_attempt,
)
from app.services.senior_analyst_projection_v1 import DATASET_POLICIES


NOW = datetime(2026, 7, 31, 12, 0, tzinfo=UTC)

_CALENDAR_PROVIDERS = (
    ("BEA", "BEA Release Schedule"),
    ("BLS", "BLS Release Calendar"),
    ("FEDERAL_RESERVE", "Federal Reserve Calendar"),
)


def _calendar_coverage(
    *,
    requested_dates: list[str],
    by_provider: dict[str, dict],
    request_id: str,
    observed_at: datetime,
    called_provider_ids: set[str] | None = None,
) -> dict:
    called_provider_ids = (
        set(called_provider_ids)
        if called_provider_ids is not None
        else {
            provider_id
            for provider_id, _provider_name
            in _CALENDAR_PROVIDERS
        }
    )
    provider_ids = [
        provider_id
        for provider_id, _provider_name
        in _CALENDAR_PROVIDERS
    ]
    provider_names = [
        provider_name
        for _provider_id, provider_name
        in _CALENDAR_PROVIDERS
    ]
    provider_id_by_name = {
        provider_name: provider_id
        for provider_id, provider_name
        in _CALENDAR_PROVIDERS
    }
    return {
        "requested_dates": requested_dates,
        "expected_provider_ids": provider_ids,
        "expected_provider_names": sorted(provider_names),
        "provider_id_by_name": provider_id_by_name,
        "provider_attempts": [
            {
                "provider_id": provider_id,
                "provider_name": provider_name,
                "query_scope": "country=US",
                "request_id": request_id,
                "correlation_id": request_id,
                "started_at": observed_at.isoformat(),
                "observed_at": observed_at.isoformat(),
                "called": provider_id in called_provider_ids,
                "attempts": int(
                    provider_id in called_provider_ids
                ),
                "successful_attempts": int(
                    provider_id in called_provider_ids
                ),
                "failed_attempts": 0,
                "result": (
                    "SUCCESS"
                    if provider_id in called_provider_ids
                    else "NOT_CALLED"
                ),
                "not_called_reason": (
                    None
                    if provider_id in called_provider_ids
                    else "VALID_DATABASE_COVERAGE"
                ),
            }
            for provider_id, provider_name
            in _CALENDAR_PROVIDERS
        ],
        "provider_calls_executed": len(called_provider_ids),
        "provider_success_count": len(called_provider_ids),
        "quarantined_occurrence_count": 0,
        "status": "VERIFIED_COMPLETE",
        "daily_matrix": {
            "status": "VERIFIED_COMPLETE",
            "unknown_coverage_days": [],
            "partial_coverage_days": [],
        },
        "database_lookup_daily_matrix": {
            "requested_dates": requested_dates,
            "expected_provider_ids": provider_ids,
            "expected_provider_names": sorted(provider_names),
            "provider_id_by_name": provider_id_by_name,
            "by_provider": by_provider,
        },
    }


def _settings(tmp_path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "diagnostics-composite.sqlite",
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


def _fact(
    *,
    fact_type: str,
    key: str,
    raw: dict,
    source: str,
) -> dict:
    valid_until = NOW + timedelta(hours=1)
    return {
        "fact_key": key,
        "fact_type": fact_type,
        "category": raw.get("series_id") or fact_type,
        "source": source,
        "value": raw.get("value", "1"),
        "retrieved_at": (NOW - timedelta(minutes=5)).isoformat(),
        "release_at": raw.get("data_as_of")
        or (NOW - timedelta(minutes=5)).isoformat(),
        "valid_until": valid_until.isoformat(),
        "next_refresh_at": valid_until.isoformat(),
        "lifecycle_status": "ACTIVE",
        "raw_payload": {
            **raw,
            "data_as_of": raw.get("data_as_of")
            or (NOW - timedelta(minutes=5)).isoformat(),
            "content_valid_until": valid_until.isoformat(),
            "refresh_due_at": valid_until.isoformat(),
        },
    }


class _Facts:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = deepcopy(rows)

    def get_valid_facts_by_type(
        self,
        fact_type: str,
        *,
        allow_stale: bool = False,
    ) -> list[dict]:
        assert allow_stale is True
        return [
            deepcopy(row)
            for row in self.rows
            if row.get("fact_type") == fact_type
        ]


class _ObservedMacroService:
    providers: list[object] = []

    def __init__(self) -> None:
        self.calls = 0
        self.requested_series: dict[str, tuple[str, ...]] = {}

    async def latest(
        self,
        *,
        force: bool = False,
        requested_series: dict[str, tuple[str, ...]] | None = None,
    ) -> MacroLatestResponse:
        assert force is True
        self.calls += 1
        self.requested_series = requested_series or {}
        return MacroLatestResponse()


def _complete_macro_rows() -> list[dict]:
    rows: list[dict] = []
    for dataset_id, query in _macro_repository_queries().items():
        policy = next(
            item
            for item in DATASET_POLICIES
            if item.dataset_id == dataset_id
        )
        for series_id in _required_macro_dataset_series(
            dataset_id,
            query=query,
        ):
            rows.append(
                _fact(
                    fact_type=query.fact_types[0],
                    key=f"US:{series_id}:latest:{dataset_id}",
                    raw={"series_id": series_id, "value": 1.0},
                    source=policy.primary_provider,
                )
            )
    return rows


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dataset_id",
    ["target_range", "treasury_rates", "fed_funds", "pce"],
)
async def test_partial_macro_composite_cache_does_not_skip_provider(
    tmp_path,
    dataset_id: str,
) -> None:
    queries = _macro_repository_queries()
    missing_series = _required_macro_dataset_series(
        dataset_id,
        query=queries[dataset_id],
    )[-1]
    rows = [
        row
        for row in _complete_macro_rows()
        if str((row.get("raw_payload") or {}).get("series_id"))
        != missing_series
    ]
    service = object.__new__(DiagnosticsService)
    service.settings = _settings(tmp_path)
    service.freshness = DataFreshnessService(
        service.settings,
        clock=lambda: NOW,
    )
    service.facts = _Facts(rows)
    service.macro_service = _ObservedMacroService()
    service._save_macro = lambda _macro: 0

    macro, _quality = await service._macro_db_first(force=True)

    assert service.macro_service.calls == 1
    assert missing_series in {
        series_id
        for series_ids in service.macro_service.requested_series.values()
        for series_id in series_ids
    }
    assert not {
        str(item.series_id).upper() for item in macro.series
    } & {
        series_id.upper()
        for series_id in _required_macro_dataset_series(
            dataset_id,
            query=queries[dataset_id],
        )
    }


class _ObservedNasdaqService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def context(
        self,
        *,
        force: bool,
        fetch_news: bool,
        fetch_holdings: bool,
        fetch_mega_cap: bool,
        fetch_earnings: bool,
        preloaded_holdings: dict | None = None,
    ):
        self.calls.append(
            {
                "force": force,
                "fetch_news": fetch_news,
                "fetch_holdings": fetch_holdings,
                "fetch_mega_cap": fetch_mega_cap,
                "fetch_earnings": fetch_earnings,
                "preloaded_holdings": preloaded_holdings,
            }
        )
        raise RuntimeError("controlled_provider_stop")


def _qqq_fact() -> dict:
    return _fact(
        fact_type="qqq_holdings",
        key="nasdaq_context:qqq_holdings",
        source="INVESCO",
        raw={
            "status": "found",
            "holdings_count": 2,
            "weight_data_available": True,
            "holdings": [
                {"symbol": "MSFT", "weight_pct": 9.0},
                {"symbol": "NVDA", "weight_pct": 8.0},
            ],
        },
    )


def _mega_fact(*, tracked: int, resolved: int) -> dict:
    return _fact(
        fact_type="mega_cap_snapshot",
        key="nasdaq_context:mega_cap_snapshot",
        source="YAHOO_FINANCE_CHART",
        raw={
            "source": "YAHOO_FINANCE_CHART",
            "retrieved_at": NOW.isoformat(),
            "stocks": [
                {
                    "symbol": "MSFT",
                    "last_price": 500.0,
                    "change_pct": 1.0,
                }
            ],
            "data_quality": {
                "tracked_count": tracked,
                "resolved_count": resolved,
            },
        },
    )


def _constituents_only_fact() -> dict:
    return _fact(
        fact_type="nasdaq_100_constituents",
        key="nasdaq:constituents",
        source="NASDAQ",
        raw={
            "status": "valid",
            "constituents": [
                {"symbol": "MSFT", "market_cap": 3_000_000_000_000}
            ],
        },
    )


async def _run_nasdaq(
    tmp_path,
    rows: list[dict],
) -> _ObservedNasdaqService:
    service = object.__new__(DiagnosticsService)
    service.settings = _settings(tmp_path)
    service.freshness = DataFreshnessService(
        service.settings,
        clock=lambda: NOW,
    )
    service.facts = _Facts(rows)
    service.nasdaq_data_service = _ObservedNasdaqService()
    await service._nasdaq_db_first(
        symbol="MNQ",
        force=True,
        fetch_news=False,
    )
    return service.nasdaq_data_service


@pytest.mark.asyncio
async def test_constituents_row_without_materializable_holdings_calls_provider(
    tmp_path,
) -> None:
    provider = await _run_nasdaq(
        tmp_path,
        [_constituents_only_fact(), _mega_fact(tracked=1, resolved=1)],
    )

    assert len(provider.calls) == 1
    assert provider.calls[0]["fetch_holdings"] is True
    assert provider.calls[0]["fetch_mega_cap"] is False


@pytest.mark.asyncio
async def test_partial_mega_cap_snapshot_calls_provider(tmp_path) -> None:
    provider = await _run_nasdaq(
        tmp_path,
        [_qqq_fact(), _mega_fact(tracked=2, resolved=1)],
    )

    assert len(provider.calls) == 1
    assert provider.calls[0]["fetch_holdings"] is False
    assert provider.calls[0]["fetch_mega_cap"] is True


class _CaptureCollector:
    def __init__(self) -> None:
        self.policies = {
            policy.dataset_id: policy for policy in DATASET_POLICIES
        }
        self.rows: dict[str, dict] = {}

    def record(self, dataset_id: str, **values) -> None:
        self.rows[dataset_id] = values


def _risk_lookup() -> dict:
    return {
        "performed": True,
        "found": False,
        "data_as_of": None,
        "content_valid_until": None,
        "refresh_due_at": None,
        "lifecycle_status": None,
        "expired": False,
        "freshness": "NOT_FOUND",
    }


def _called_cboe_without_vix() -> dict:
    return {
        "status": "found",
        "source": "CBOE",
        "attempted": True,
        "provider_calls": 1,
        "indices": {
            "vvix": {"current_price": 90.0},
            "skew": {"current_price": 145.0},
        },
    }


def _record_risk_accounting(
    *,
    vix_provider_result: str,
    risk_context: dict,
) -> dict:
    service = object.__new__(DiagnosticsService)
    collector = _CaptureCollector()
    service._record_multi_source_accounting(
        collector,
        blocks={"cboe_risk_indices": _called_cboe_without_vix()},
        risk_context=risk_context,
        vix_database_lookup=_risk_lookup(),
        vix_provider_evidence={
            "provider": "FRED",
            "called": True,
            "attempts": 1,
            "result": vix_provider_result,
        },
        fed_database_lookup=_risk_lookup(),
        risk_database_lookup=_risk_lookup(),
    )
    return collector.rows["vix"]


def test_fred_failure_and_cboe_without_vix_is_not_selected() -> None:
    row = _record_risk_accounting(
        vix_provider_result="FAILED",
        risk_context={
            "vix": {
                "status": "not_found",
                "value": None,
                "source": "FRED",
            }
        },
    )

    assert row["primary_provider"]["result"] == "FAILED"
    assert row["fallbacks"][0]["called"] is True
    assert row["fallbacks"][0]["result"] == "NO_DATA_FOR_VIX"
    assert row["acquisition_selected_source"] is None
    assert row["acquisition_reason_code"] == "RISK_CONTEXT_NOT_AVAILABLE"


def test_fred_success_preserves_real_shared_cboe_call() -> None:
    row = _record_risk_accounting(
        vix_provider_result="FOUND",
        risk_context={
            "vix": {
                "status": "found",
                "value": 16.2,
                "source": "FRED",
            }
        },
    )

    assert row["primary_provider"]["result"] == "FOUND"
    assert row["fallbacks"][0]["called"] is True
    assert row["fallbacks"][0]["attempts"] == 1
    assert row["fallbacks"][0]["result"] == "NO_DATA_FOR_VIX"
    assert row["acquisition_selected_source"] == "FRED"
    assert row["shared_dataset_ids"] == ("vix", "vvix", "risk")


def _shared_vix_flow_row(*, shared: list[str]) -> dict:
    return {
        "dataset_id": "vix",
        "acquisition_id": "risk_context_shared_acquisition",
        "shared_acquisition_dataset_ids": shared,
        "database_record_found": False,
        "database_record_expired": False,
        "database_freshness_evaluation": "NOT_FOUND",
        "primary_provider": provider_attempt(
            "FRED",
            called=True,
            attempts=1,
            result="FOUND",
            execution_origin="PROVIDER_CALL",
        ),
        "fallbacks": [
            provider_attempt(
                "CBOE",
                called=True,
                attempts=1,
                result="NO_DATA_FOR_VIX",
                execution_origin="PROVIDER_CALL",
            )
        ],
        "acquisition_selected_source": "FRED",
    }


def test_post_success_call_requires_explicit_shared_acquisition() -> None:
    policy = SimpleNamespace(
        dataset_id="vix",
        primary_provider="FRED",
        fallback_providers=("CBOE",),
        provider_strategy="FALLBACK",
    )
    governed = {"vix", "vvix", "risk"}

    assert _provider_flow_valid(
        _shared_vix_flow_row(shared=["vix", "vvix", "risk"]),
        policy=policy,
        governed_dataset_ids=governed,
    )
    assert not _provider_flow_valid(
        _shared_vix_flow_row(shared=["vix"]),
        policy=policy,
        governed_dataset_ids=governed,
    )


def test_calendar_without_lookup_stays_incomplete() -> None:
    policy = next(
        policy
        for policy in DATASET_POLICIES
        if policy.dataset_id == "macro_calendar"
    )
    collector = RequestProviderAccountingCollector(
        request_id="calendar-no-lookup",
        correlation_id="calendar-no-lookup",
        request_started_at=NOW - timedelta(seconds=1),
        policies=[policy],
        clock=lambda: NOW,
    )
    service = object.__new__(DiagnosticsService)

    service._record_calendar_accounting(
        collector,
        events=[],
        database_lookup_performed=False,
        provider_called=True,
        provider_result="SUCCESS",
        provider_not_called_reason=None,
        acquisition_reason_code="PROVIDER_ONLY_PATH",
        coverage={},
    )

    row = collector.manifest(request_completed_at=NOW)["datasets"][0]
    assert row["database_lookup_performed"] is False
    assert row["database_record_found"] is None
    assert row["database_freshness_evaluation"] == "NOT_LOOKED_UP"
    assert row["evidence_status"] == "INCOMPLETE"
    assert _calendar_database_evidence(
        {},
        events=[],
        provider_called=True,
        database_lookup_performed=False,
    )["performed"] is False


def test_calendar_accounting_keeps_complete_matrix_summary() -> None:
    observed_at = datetime.now(UTC).replace(microsecond=0)
    today = observed_at.date().isoformat()
    tomorrow = (
        observed_at + timedelta(days=1)
    ).date().isoformat()
    policy = next(
        policy
        for policy in DATASET_POLICIES
        if policy.dataset_id == "macro_calendar"
    )
    collector = RequestProviderAccountingCollector(
        request_id="calendar-matrix",
        correlation_id="calendar-matrix",
        request_started_at=observed_at - timedelta(seconds=1),
        policies=[policy],
        clock=lambda: observed_at,
    )
    service = object.__new__(DiagnosticsService)
    deadline = observed_at + timedelta(hours=1)

    service._record_calendar_accounting(
        collector,
        events=[{"source": "Canonical Economic Calendar"}],
        database_lookup_performed=True,
        provider_called=True,
        provider_result="SCHEDULE_CATCH_UP_COMPLETED",
        provider_not_called_reason=None,
        acquisition_reason_code="CALENDAR_PROVIDER_ACQUIRED",
        coverage=_calendar_coverage(
            requested_dates=[today, tomorrow],
            request_id="calendar-matrix",
            observed_at=observed_at,
            called_provider_ids={"BLS"},
            by_provider={
                    "Federal Reserve Calendar": {
                        "by_date": {
                            today: {
                                "status": "VERIFIED_COMPLETE",
                                "valid_until": deadline.isoformat(),
                                "next_revision_check_at": (
                                    deadline.isoformat()
                                ),
                            },
                            tomorrow: {
                                "status": "VERIFIED_EMPTY",
                                "valid_until": deadline.isoformat(),
                                "next_revision_check_at": (
                                    deadline.isoformat()
                                ),
                            },
                        }
                    },
                    "BLS Release Calendar": {
                        "by_date": {
                            today: {
                                "status": "VERIFIED_COMPLETE",
                                "valid_until": deadline.isoformat(),
                                "next_revision_check_at": (
                                    deadline.isoformat()
                                ),
                            },
                            tomorrow: {
                                "status": "PARTIAL",
                                "valid_until": deadline.isoformat(),
                                "next_revision_check_at": (
                                    deadline.isoformat()
                                ),
                            }
                        }
                    },
                    "BEA Release Schedule": {
                        "by_date": {
                            today: {
                                "status": "VERIFIED_COMPLETE",
                                "valid_until": deadline.isoformat(),
                                "next_revision_check_at": (
                                    deadline.isoformat()
                                ),
                            },
                            tomorrow: {
                                "status": "VERIFIED_EMPTY",
                                "valid_until": deadline.isoformat(),
                                "next_revision_check_at": (
                                    deadline.isoformat()
                                ),
                            },
                        }
                    },
            },
        ),
    )

    manifest = collector.manifest(
        request_completed_at=observed_at,
    )
    row = manifest["datasets"][0]
    assert manifest["evidence_status"] == "ACQUISITION_COMPLETE"
    assert row["database_record_found"] is False
    assert row["database_freshness_evaluation"] == "NOT_FOUND"
    assert row["database_lookup_summary"] == {
        "providers_expected": 3,
        "providers_evaluated": 3,
        "provider_ids_expected": [
            "BEA",
            "BLS",
            "FEDERAL_RESERVE",
        ],
        "provider_ids_evaluated": [
            "BEA",
            "BLS",
            "FEDERAL_RESERVE",
        ],
        "provider_names_expected": [
            "BEA Release Schedule",
            "BLS Release Calendar",
            "Federal Reserve Calendar",
        ],
        "provider_names_evaluated": [
            "BEA Release Schedule",
            "BLS Release Calendar",
            "Federal Reserve Calendar",
        ],
        "unexpected_provider_names": [],
        "provider_refresh_required_ids": ["BLS"],
        "provider_refresh_required_names": [
            "BLS Release Calendar"
        ],
        "provider_attempts": _calendar_coverage(
            requested_dates=[today, tomorrow],
            by_provider={},
            request_id="calendar-matrix",
            observed_at=observed_at,
            called_provider_ids={"BLS"},
        )["provider_attempts"],
        "observations_expected": 6,
        "observations_evaluated": 6,
        "observations_missing": 0,
        "records_terminal": 5,
        "records_missing_or_incomplete": 1,
        "records_expired": 0,
        "window_start": today,
        "window_end": tomorrow,
    }


def test_calendar_accounting_rejects_missing_requested_matrix_row() -> None:
    observed_at = datetime.now(UTC).replace(microsecond=0)
    today = observed_at.date().isoformat()
    tomorrow = (
        observed_at + timedelta(days=1)
    ).date().isoformat()
    policy = next(
        policy
        for policy in DATASET_POLICIES
        if policy.dataset_id == "macro_calendar"
    )
    collector = RequestProviderAccountingCollector(
        request_id="calendar-matrix-gap",
        correlation_id="calendar-matrix-gap",
        request_started_at=observed_at - timedelta(seconds=1),
        policies=[policy],
        clock=lambda: observed_at,
    )
    service = object.__new__(DiagnosticsService)
    deadline = observed_at + timedelta(hours=1)

    service._record_calendar_accounting(
        collector,
        events=[{"source": "Canonical Economic Calendar"}],
        database_lookup_performed=True,
        provider_called=True,
        provider_result="SCHEDULE_CATCH_UP_COMPLETED",
        provider_not_called_reason=None,
        acquisition_reason_code="CALENDAR_PROVIDER_ACQUIRED",
        coverage=_calendar_coverage(
            requested_dates=[today, tomorrow],
            request_id="calendar-matrix-gap",
            observed_at=observed_at,
            by_provider={
                    "Federal Reserve Calendar": {
                        "by_date": {
                            today: {
                                "status": "VERIFIED_COMPLETE",
                                "valid_until": deadline.isoformat(),
                                "next_revision_check_at": (
                                    deadline.isoformat()
                                ),
                            }
                        }
                    },
            },
        ),
    )

    manifest = collector.manifest(
        request_completed_at=observed_at,
    )
    row = manifest["datasets"][0]
    assert manifest["evidence_status"] == "INCOMPLETE"
    assert row["evidence_status"] == "INCOMPLETE"
    assert row["database_lookup_summary"][
        "observations_expected"
    ] == 6
    assert row["database_lookup_summary"][
        "observations_evaluated"
    ] == 1
    assert row["database_lookup_summary"][
        "observations_missing"
    ] == 5


def test_calendar_due_leaf_cannot_be_masked_by_aggregate_success() -> None:
    observed_at = datetime.now(UTC).replace(microsecond=0)
    day = observed_at.date().isoformat()
    deadline = observed_at + timedelta(hours=1)
    policy = next(
        policy
        for policy in DATASET_POLICIES
        if policy.dataset_id == "macro_calendar"
    )
    collector = RequestProviderAccountingCollector(
        request_id="calendar-due-leaf-not-called",
        correlation_id="calendar-due-leaf-not-called",
        request_started_at=observed_at - timedelta(seconds=1),
        policies=[policy],
        clock=lambda: observed_at,
    )
    coverage = _calendar_coverage(
        requested_dates=[day],
        request_id="calendar-due-leaf-not-called",
        observed_at=observed_at,
        called_provider_ids=set(),
        by_provider={
            provider_name: {
                "by_date": {
                    day: {
                        "status": (
                            "PARTIAL"
                            if provider_id == "BLS"
                            else "VERIFIED_COMPLETE"
                        ),
                        "valid_until": deadline.isoformat(),
                        "next_revision_check_at": (
                            deadline.isoformat()
                        ),
                    }
                }
            }
            for provider_id, provider_name
            in _CALENDAR_PROVIDERS
        },
    )

    service = object.__new__(DiagnosticsService)
    service._record_calendar_accounting(
        collector,
        events=[{"source": "Canonical Economic Calendar"}],
        database_lookup_performed=True,
        provider_called=False,
        provider_result="NOT_CALLED",
        provider_not_called_reason=(
            "VALID_DATABASE_RECORD_SELECTED"
        ),
        acquisition_reason_code="CALENDAR_AGGREGATE_CLAIMED_SUCCESS",
        coverage=coverage,
    )

    manifest = collector.manifest(
        request_completed_at=observed_at,
    )
    row = manifest["datasets"][0]
    assert row["database_lookup_summary"][
        "provider_refresh_required_ids"
    ] == ["BLS"]
    assert row["primary_provider"]["called"] is False
    assert row["evidence_status"] == "INCOMPLETE"
    assert manifest["evidence_status"] == "INCOMPLETE"


def test_calendar_called_leaf_timestamp_must_be_in_request_window() -> None:
    observed_at = datetime.now(UTC).replace(microsecond=0)
    day = observed_at.date().isoformat()
    deadline = observed_at + timedelta(hours=1)
    policy = next(
        policy
        for policy in DATASET_POLICIES
        if policy.dataset_id == "macro_calendar"
    )
    collector = RequestProviderAccountingCollector(
        request_id="calendar-leaf-time",
        correlation_id="calendar-leaf-time",
        request_started_at=observed_at - timedelta(seconds=1),
        policies=[policy],
        clock=lambda: observed_at,
    )
    coverage = _calendar_coverage(
        requested_dates=[day],
        request_id="calendar-leaf-time",
        observed_at=observed_at,
        called_provider_ids={"BLS"},
        by_provider={
            provider_name: {
                "by_date": {
                    day: {
                        "status": (
                            "PARTIAL"
                            if provider_id == "BLS"
                            else "VERIFIED_COMPLETE"
                        ),
                        "valid_until": deadline.isoformat(),
                        "next_revision_check_at": (
                            deadline.isoformat()
                        ),
                    }
                }
            }
            for provider_id, provider_name
            in _CALENDAR_PROVIDERS
        },
    )
    bls_attempt = next(
        item
        for item in coverage["provider_attempts"]
        if item["provider_id"] == "BLS"
    )
    bls_attempt["started_at"] = (
        observed_at - timedelta(minutes=1)
    ).isoformat()

    service = object.__new__(DiagnosticsService)
    service._record_calendar_accounting(
        collector,
        events=[{"source": "Canonical Economic Calendar"}],
        database_lookup_performed=True,
        provider_called=True,
        provider_result="SCHEDULE_CATCH_UP_COMPLETED",
        provider_not_called_reason=None,
        acquisition_reason_code="CALENDAR_PROVIDER_ACQUIRED",
        coverage=coverage,
    )

    manifest = collector.manifest(
        request_completed_at=observed_at,
    )
    assert manifest["datasets"][0]["evidence_status"] == "INCOMPLETE"
    assert manifest["evidence_status"] == "INCOMPLETE"


def test_calendar_leaf_identity_is_bound_to_registry_adapter_source() -> None:
    observed_at = datetime.now(UTC).replace(microsecond=0)
    day = observed_at.date().isoformat()
    deadline = observed_at + timedelta(hours=1)
    coverage = _calendar_coverage(
        requested_dates=[day],
        request_id="calendar-identity",
        observed_at=observed_at,
        by_provider={
            provider_name: {
                "by_date": {
                    day: {
                        "status": "PARTIAL",
                        "valid_until": deadline.isoformat(),
                        "next_revision_check_at": (
                            deadline.isoformat()
                        ),
                    }
                }
            }
            for _provider_id, provider_name
            in _CALENDAR_PROVIDERS
        },
    )
    summary = _calendar_database_evidence(
        coverage,
        events=[],
        provider_called=True,
        database_lookup_performed=True,
    )["summary"]

    def valid(value: dict) -> bool:
        return _calendar_database_lookup_summary_valid(
            value,
            request_id="calendar-identity",
            correlation_id="calendar-identity",
            request_started_at=(
                observed_at - timedelta(seconds=1)
            ),
            request_observed_at=observed_at,
        )

    assert valid(summary) is True

    swapped = deepcopy(summary)
    bea = next(
        item
        for item in swapped["provider_attempts"]
        if item["provider_id"] == "BEA"
    )
    bls = next(
        item
        for item in swapped["provider_attempts"]
        if item["provider_id"] == "BLS"
    )
    bea["provider_id"], bls["provider_id"] = (
        bls["provider_id"],
        bea["provider_id"],
    )
    assert valid(swapped) is False

    spoofed = deepcopy(summary)
    spoofed_names = ["spoof-1", "spoof-2", "spoof-3"]
    spoofed["provider_names_expected"] = spoofed_names
    spoofed["provider_names_evaluated"] = spoofed_names
    spoofed["provider_refresh_required_names"] = spoofed_names
    for attempt, spoofed_name in zip(
        spoofed["provider_attempts"],
        spoofed_names,
        strict=True,
    ):
        attempt["provider_name"] = spoofed_name
    assert valid(spoofed) is False

    wrong_scope = deepcopy(summary)
    wrong_scope["provider_attempts"][0][
        "query_scope"
    ] = "country=XX"
    assert valid(wrong_scope) is False
