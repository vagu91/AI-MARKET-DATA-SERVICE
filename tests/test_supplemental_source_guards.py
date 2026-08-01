from __future__ import annotations

from typing import Any

import pytest

from app.core.config import Settings
from app.services.multi_source_runtime_service import MultiSourceRuntimeService
from app.services.risk_context_runtime_service import RiskContextRuntimeService


_SUPPLEMENTAL_BLOCKS = {
    "nasdaq_100",
    "nasdaq_qqq_options",
    "aaii_sentiment",
    "macromicro_aaii_crosscheck",
    "polymarket_prediction_markets",
    "quikstrike_review",
}


def _settings(tmp_path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "supplemental-guards.sqlite",
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


def _runtime_block(source: str) -> dict[str, Any]:
    return {
        "status": "not_called",
        "source": source,
        "attempted": False,
        "provider_calls": 0,
        "actual_network_calls": 0,
        "cache_used": False,
        "fetched_count": 0,
        "validated_count": 0,
        "persisted_count": 0,
        "read_back_count": 0,
        "materialized_count": 0,
        "warnings": [],
        "errors": [],
    }


class _ForbiddenAsyncCall:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls = 0

    async def __call__(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        self.calls += 1
        raise AssertionError(f"supplemental source called: {self.name}")


class _ForbiddenSyncCall:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls = 0

    def __call__(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        self.calls += 1
        raise AssertionError(f"supplemental source called: {self.name}")


@pytest.mark.asyncio
async def test_multi_source_senior_guard_skips_every_supplemental_source(
    tmp_path,
    monkeypatch,
) -> None:
    service = MultiSourceRuntimeService(_settings(tmp_path))
    spies = {
        "nasdaq_100": _ForbiddenAsyncCall("nasdaq_100"),
        "nasdaq_qqq_options": _ForbiddenAsyncCall("nasdaq_qqq_options"),
        "aaii_sentiment": _ForbiddenAsyncCall("aaii_sentiment"),
        "macromicro_aaii_crosscheck": _ForbiddenAsyncCall("macromicro_aaii_crosscheck"),
        "polymarket_prediction_markets": _ForbiddenAsyncCall("polymarket_prediction_markets"),
    }
    monkeypatch.setattr(service.nasdaq_100, "fetch", spies["nasdaq_100"])
    monkeypatch.setattr(
        service.nasdaq_options,
        "fetch",
        spies["nasdaq_qqq_options"],
    )
    monkeypatch.setattr(
        service.positioning_runtime,
        "aaii",
        spies["aaii_sentiment"],
    )
    monkeypatch.setattr(
        service.macromicro,
        "fetch",
        spies["macromicro_aaii_crosscheck"],
    )
    monkeypatch.setattr(
        service.polymarket,
        "fetch",
        spies["polymarket_prediction_markets"],
    )
    quikstrike = _ForbiddenSyncCall("quikstrike_review")
    monkeypatch.setattr(service, "_quikstrike_review", quikstrike)

    async def schedule_chain(*, refresh: str) -> dict[str, dict[str, Any]]:
        assert refresh == "force"
        return {
            name: _runtime_block(name)
            for name in (
                "investing_holidays",
                "marketbeat_holidays",
                "cme_market_schedule",
                "nasdaq_market_info",
            )
        }

    async def earnings_chain(
        *,
        refresh: str,
        **_kwargs: Any,
    ) -> dict[str, dict[str, Any]]:
        assert refresh == "force"
        return {
            "nasdaq_earnings": _runtime_block("nasdaq_earnings"),
            "fmp_earnings": _runtime_block("fmp_earnings"),
        }

    monkeypatch.setattr(service, "_market_schedule_chain", schedule_chain)
    monkeypatch.setattr(service, "_earnings_chain", earnings_chain)
    preloaded_blocks = {
        name: _runtime_block(name)
        for name in (
            "investing_economic_calendar",
            "xtb_economic_calendar",
            "investing_fed_rate_monitor",
            "cboe_risk_indices",
        )
    }
    preloaded_blocks["nasdaq_qqq_options"] = {
        "status": "found",
        "contracts": [{"symbol": "QQQ"}],
        "attempted": True,
        "provider_calls": 1,
    }

    result = await service.snapshot(
        refresh="force",
        preloaded_blocks=preloaded_blocks,
        include_supplemental_context=False,
    )

    assert result["supplemental_context_enabled"] is False
    assert set(result["blocks"]) >= _SUPPLEMENTAL_BLOCKS
    assert all(spy.calls == 0 for spy in spies.values())
    assert quikstrike.calls == 0
    for name in _SUPPLEMENTAL_BLOCKS:
        block = result["blocks"][name]
        assert block["status"] == "not_called"
        assert block["attempted"] is False
        assert block["provider_calls"] == 0
        assert block["actual_network_calls"] == 0
        assert block["materialized_count"] == 0
        assert block["reason_code"] == ("SUPPLEMENTAL_CONTEXT_DISABLED_FOR_REQUEST_ACCOUNTING")
        assert block["database_lookup"]["performed"] is False


@pytest.mark.asyncio
async def test_risk_senior_guard_ignores_preload_and_skips_internal_qqq_fetch(
    tmp_path,
    monkeypatch,
) -> None:
    service = RiskContextRuntimeService(_settings(tmp_path))
    qqq_fetch = _ForbiddenAsyncCall("internal_qqq_options")
    monkeypatch.setattr(service.qqq_options_provider, "fetch", qqq_fetch)

    async def missing_provider() -> dict[str, Any]:
        return {
            "status": "not_found",
            "warnings": [],
            "errors": [],
            "diagnostics": {"actual_network_calls": 0},
        }

    monkeypatch.setattr(service.vix_futures_provider, "fetch", missing_provider)
    monkeypatch.setattr(service.put_call_provider, "fetch", missing_provider)

    captured: dict[str, Any] = {}
    real_build = service.normalizer.build

    def capture_build(**kwargs: Any) -> dict[str, Any]:
        captured["qqq_options"] = kwargs["qqq_options"]
        return real_build(**kwargs)

    monkeypatch.setattr(service.normalizer, "build", capture_build)
    observed_risk_indices = {
        "status": "found",
        "attempted": True,
        "provider_calls": 1,
        "cache_used": False,
        "indices": {},
        "warnings": [],
        "errors": [],
        "diagnostics": {"actual_network_calls": 0},
    }
    qqq_preload = {
        "status": "found",
        "attempted": True,
        "provider_calls": 1,
        "cache_used": False,
        "global_aggregates": {
            "scope": {"full_chain_complete": True},
            "call_volume": 100,
            "put_volume": 70,
            "call_open_interest": 200,
            "put_open_interest": 180,
        },
        "contracts": [{"symbol": "QQQ"}],
    }

    result, _legacy = await service.snapshot(
        refresh="force",
        macro_snapshot={},
        preloaded_risk_indices=observed_risk_indices,
        preloaded_qqq_options=qqq_preload,
        include_supplemental_options=False,
    )

    assert qqq_fetch.calls == 0
    assert captured["qqq_options"] == {}
    assert not any(
        str(ratio.get("ratio_id") or "").startswith("qqq_")
        for ratio in (result.get("put_call") or {}).get("ratios") or []
    )
    assert result["diagnostics"]["source_attempt_count"] == 3
    assert result["diagnostics"]["supplemental_qqq_options"] == {
        "status": "not_called",
        "attempted": False,
        "provider_calls": 0,
        "actual_network_calls": 0,
        "selected": False,
        "reason_code": ("SUPPLEMENTAL_QQQ_OPTIONS_DISABLED_FOR_REQUEST_ACCOUNTING"),
    }
