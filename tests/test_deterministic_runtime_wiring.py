from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from app.api import routes
from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import ProviderCacheRepository
from app.providers.deterministic import (
    AsyncSlidingWindowRateLimiter,
    DeterministicHttpClient,
    DeterministicProviderError,
    RetryClassification,
)
from app.providers.parametric_cache import ParametricProviderCache
from app.services.market_context_snapshot_repository import MarketContextSnapshotRepository
from scripts.replay_deterministic_runtime_e2e import replay


NOW = datetime(2026, 7, 25, 14, tzinfo=UTC)


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "runtime.db",
        enable_ai_researcher=False,
    )


@pytest.mark.asyncio
async def test_production_force_path_invokes_deterministic_runtime(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _settings(tmp_path)
    calls: list[tuple[str, str | None]] = []

    class Diagnostics:
        def __init__(self, *_args, **_kwargs):
            pass

        async def full_model(self, **_kwargs):
            return {"symbol": "MNQ", "generated_at_utc": NOW.isoformat()}

    class Runtime:
        async def enrich_market_context(self, contract, *, refresh, trigger_type=None):
            calls.append((refresh, trigger_type))
            return {**contract, "runtime_wired": True}

    monkeypatch.setattr(routes, "DiagnosticsService", Diagnostics)
    monkeypatch.setattr(
        routes,
        "_materialize_market_context",
        lambda contract, **_kwargs: contract,
    )
    result = await routes.market_context_mnq(
        refresh="force",
        view="consumer",
        macro_service=object(),
        event_service=object(),
        event_window_service=object(),
        nasdaq_service=object(),
        enrichment_orchestrator=SimpleNamespace(settings=settings),
        deterministic_runtime=Runtime(),
    )
    assert result["runtime_wired"] is True
    assert calls == [("force", None)]


def test_real_bootstrap_runtime_snapshot_consumer_lifecycle_and_outbox(
    tmp_path: Path,
) -> None:
    result = replay(tmp_path / "replay.db")
    consumer = result["consumer"]
    for section in (
        "macro_actuals",
        "rates_context",
        "options_positioning",
        "market_internals",
        "cross_asset_context",
        "earnings_intelligence",
        "current_company_news",
    ):
        assert consumer[section]["status"] == "AVAILABLE"
        assert consumer[section]["data_coverage_status"] in {"COMPLETE", "PARTIAL"}
    assert consumer["options_positioning"]["underlying"] == "QQQ"
    assert consumer["options_positioning"]["target_context"] == "MNQ"
    assert consumer["options_positioning"]["proxy_used"] is True
    assert result["first_network_calls"] == 8
    assert result["second_network_calls"] == 0
    assert result["ai_invocations"] == 0
    assert result["outbox_count"] == 1
    assert result["persistence_counts"]["snapshots"] == 1
    assert result["persistence_counts"]["lifecycle_rows"] == 2
    assert result["persistence_counts"]["outbox"] == 1


def test_refresh_false_is_read_only_on_real_snapshot_repository(
    tmp_path: Path,
) -> None:
    result = replay(tmp_path / "read-only.db")
    settings = Settings(
        _env_file=None,
        environment="test",
        database_path=tmp_path / "read-only.db",
    )
    before = settings.database_path.read_bytes()
    returned = asyncio.run(
        routes.market_context_mnq(
            refresh="false",
            view="consumer",
            macro_service=None,
            event_service=None,
            event_window_service=None,
            nasdaq_service=None,
            enrichment_orchestrator=SimpleNamespace(settings=settings),
        )
    )
    after = settings.database_path.read_bytes()
    assert returned["snapshot_id"] == result["snapshot_id"]
    assert before == after


@pytest.mark.asyncio
async def test_parametric_cache_expires_only_requested_key(tmp_path: Path) -> None:
    current = [NOW]
    cache = ParametricProviderCache(
        ProviderCacheRepository(tmp_path / "cache.db"),
        clock=lambda: current[0],
    )
    calls = {"A": 0, "B": 0}

    async def load(name: str):
        calls[name] += 1
        return [{"name": name, "version": calls[name]}]

    async def resolve(name: str):
        return await cache.resolve(
            provider="TEST",
            endpoint="series",
            environment="test",
            parameters={"series": name},
            ttl_seconds=10,
            loader=lambda: load(name),
        )

    await resolve("A")
    await resolve("B")
    current[0] += timedelta(seconds=11)
    refreshed = await resolve("A")
    assert refreshed.cache_status == "REFRESHED"
    assert calls == {"A": 2, "B": 1}


@pytest.mark.asyncio
async def test_stale_grace_and_negative_cache_are_explicit(tmp_path: Path) -> None:
    current = [NOW]
    cache = ParametricProviderCache(
        ProviderCacheRepository(tmp_path / "policy.db"),
        clock=lambda: current[0],
    )
    calls = 0

    async def good():
        nonlocal calls
        calls += 1
        return [{"value": 1}]

    kwargs = {
        "provider": "TEST",
        "endpoint": "dataset",
        "environment": "test",
        "parameters": {"period": "2026-06"},
        "ttl_seconds": 10,
    }
    await cache.resolve(**kwargs, loader=good)
    current[0] += timedelta(seconds=11)

    async def temporary_failure():
        raise DeterministicProviderError(
            "temporary",
            classification=RetryClassification.RETRYABLE,
        )

    stale = await cache.resolve(**kwargs, loader=temporary_failure)
    assert stale.cache_status == "STALE_GRACE"
    assert stale.telemetry["stale_grace"] is True

    terminal_calls = 0

    async def terminal_failure():
        nonlocal terminal_calls
        terminal_calls += 1
        raise DeterministicProviderError(
            "terminal",
            classification=RetryClassification.TERMINAL,
        )

    terminal_kwargs = {
        **kwargs,
        "parameters": {"period": "invalid-terminal"},
    }
    with pytest.raises(DeterministicProviderError):
        await cache.resolve(**terminal_kwargs, loader=terminal_failure)
    negative = await cache.resolve(**terminal_kwargs, loader=terminal_failure)
    assert negative.cache_status == "NEGATIVE_HIT"
    assert terminal_calls == 1


@pytest.mark.asyncio
async def test_tradier_sliding_window_rate_limiter_waits() -> None:
    current = [NOW]
    sleeps: list[float] = []

    async def sleeper(seconds: float) -> None:
        sleeps.append(seconds)
        current[0] += timedelta(seconds=seconds)

    limiter = AsyncSlidingWindowRateLimiter(
        2,
        clock=lambda: current[0],
        sleeper=sleeper,
    )
    await limiter.acquire()
    await limiter.acquire()
    waited = await limiter.acquire()
    assert waited == 60
    assert sleeps == [60]


@pytest.mark.asyncio
async def test_retry_after_header_controls_429_backoff() -> None:
    calls = 0
    sleeps: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "3"})
        return httpx.Response(200, json={"ok": True})

    async def sleeper(seconds: float) -> None:
        sleeps.append(seconds)

    client = DeterministicHttpClient(
        allowed_hosts={"api.tradier.com"},
        timeout_seconds=1,
        retry_attempts=2,
        transport=httpx.MockTransport(handler),
        sleeper=sleeper,
    )
    payload, telemetry, _ = await client.request(
        "GET",
        "https://api.tradier.com/v1/markets/quotes",
        endpoint_category="quotes",
        provider="TRADIER",
    )
    assert payload == {"ok": True}
    assert telemetry.actual_provider_requests == 2
    assert telemetry.retries == 1
    assert sleeps == [3]


def test_redacted_consumer_fixture_is_bounded_and_safe() -> None:
    path = Path(__file__).parent / "fixtures" / "deterministic_consumer_v21_redacted.json"
    raw = path.read_bytes()
    payload = json.loads(raw)
    assert len(raw) < 90_000
    encoded = raw.decode("utf-8").lower()
    for forbidden in (
        "configured-for-offline-replay",
        "authorization",
        '"api_key"',
        '"token"',
        '"chains"',
        "raw_chain",
        "order_request",
        "account_id",
    ):
        assert forbidden not in encoded
    assert payload["current_company_news"]["provider"] == "SOURCE_GATEWAY"
    assert payload["earnings_intelligence"]["candidate_news_policy"].startswith(
        "DISCOVERY_ONLY"
    )
    assert (
        payload["deterministic_domains"]["_runtime"]["numeric_gaps_sent_to_ai"]
        == 0
    )


def test_fixture_consumer_is_restored_from_real_snapshot(tmp_path: Path) -> None:
    result = replay(tmp_path / "fixture.db")
    restored = MarketContextSnapshotRepository(
        Settings(
            _env_file=None,
            environment="test",
            database_path=tmp_path / "fixture.db",
        )
    ).latest("MNQ")
    assert restored is not None
    assert restored["consumer_payload"] == result["consumer"]
