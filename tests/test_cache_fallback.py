from datetime import UTC, datetime

import pytest

from app.core.cache import SQLiteCache
from app.models.common import Freshness, ProviderMetadata, ProviderResult, ProviderType
from app.providers.base import BaseProvider, redact_sensitive
from app.services.market_fact_repository import encode


class FlakyProvider(BaseProvider):
    source = "Flaky"
    provider_type = ProviderType.API
    reliability = 0.7
    cache_key = "test:flaky"

    def __init__(self, cache: SQLiteCache) -> None:
        super().__init__(cache)
        self.fail = False

    async def fetch(self) -> ProviderResult:
        if self.fail:
            raise RuntimeError("upstream is down")
        return ProviderResult(
            metadata=ProviderMetadata(
                source=self.source,
                provider_type=self.provider_type,
                retrieved_at=datetime.now(UTC),
                data_as_of=datetime.now(UTC),
                freshness=Freshness.LIVE,
                reliability=self.reliability,
            ),
            data={"ok": {"value": 1}},
        )


@pytest.mark.asyncio
async def test_provider_uses_last_valid_cache_on_failure(tmp_path) -> None:
    cache = SQLiteCache(tmp_path / "cache.sqlite3")
    provider = FlakyProvider(cache)

    first = await provider.fetch_safe()
    provider.fail = True
    second = await provider.fetch_safe()

    assert first.data == second.data
    assert second.metadata.provider_type == ProviderType.CACHE
    assert second.metadata.is_fallback is True
    assert second.metadata.errors == ["Flaky failed: upstream is down"]


@pytest.mark.asyncio
async def test_provider_does_not_report_cache_fallback_when_cache_is_empty(tmp_path) -> None:
    cache = SQLiteCache(tmp_path / "cache.sqlite3")
    provider = FlakyProvider(cache)
    provider.fail = True

    result = await provider.fetch_safe()

    assert result.data == {}
    assert result.metadata.provider_type == ProviderType.API
    assert result.metadata.is_fallback is True
    assert result.metadata.errors == ["Flaky failed: upstream is down"]


def test_provider_error_redaction_removes_api_keys() -> None:
    message = (
        "https://api.stlouisfed.org/fred/series/observations?"
        "series_id=VIXCLS&api_key=secret&UserID=bea-secret&registrationkey=bls-secret"
    )

    redacted = redact_sensitive(message)

    assert "secret" not in redacted
    assert "api_key=<redacted>" in redacted
    assert "apikey=<redacted>" in redact_sensitive("apikey=alpha-secret")
    assert "ALPHA_VANTAGE_API_KEY=<redacted>" in redact_sensitive(
        "ALPHA_VANTAGE_API_KEY=alpha-secret"
    )
    assert "3CMXPNC9UY3Y20BD" not in redact_sensitive(
        "We have detected your API key as 3CMXPNC9UY3Y20BD and rate limited it"
    )
    assert "API key as <redacted>" in redact_sensitive(
        "We have detected your API key as 3CMXPNC9UY3Y20BD and rate limited it"
    )
    assert "UserID=<redacted>" in redacted
    assert "registrationkey=<redacted>" in redacted


def test_json_payload_encoding_redacts_nested_provider_secrets() -> None:
    encoded = encode(
        {
            "errors": [
                "Alpha Vantage Information: We have detected your API key as 3CMXPNC9UY3Y20BD"
            ]
        }
    )

    assert "3CMXPNC9UY3Y20BD" not in encoded
    assert "<redacted>" in encoded
