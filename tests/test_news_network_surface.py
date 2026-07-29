from __future__ import annotations

from pathlib import Path
import socket
import urllib.request

from app.services.news_intelligence_service import canonicalize_url
from scripts.validate_news_network_surface import (
    analyze_news_network_surface,
    analyze_source,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_production_news_network_surface_is_fully_observed() -> None:
    report = analyze_news_network_surface(REPO_ROOT)

    assert report["pass"] is True, report["violations"]
    assert report["violations"] == []
    assert set(report["observed_provider_functions"]) >= {
        "_fetch_alpha_vantage",
        "_fetch_gdelt",
        "_fetch_one_rss_feed",
        "_enrich_missing_metadata",
    }
    assert "_enrich_missing_metadata.enrich" in report["network_callsite_functions"]
    assert {
        item["callable"]
        for item in report["network_paths"]
    } == {"httpx.AsyncClient.get"}
    assert set(report["intercepted_surfaces"]) == {
        "httpx.AsyncClient.send",
        "httpx.AsyncHTTPTransport.handle_async_request",
    }


def test_urllib_parse_is_proven_local_without_socket_or_urlopen(
    monkeypatch,
) -> None:
    network_attempts: list[str] = []

    def reject_network(*args, **kwargs):
        del args, kwargs
        network_attempts.append("network")
        raise AssertionError("URL canonicalization attempted network I/O")

    monkeypatch.setattr(socket, "create_connection", reject_network)
    monkeypatch.setattr(socket.socket, "connect", reject_network)
    monkeypatch.setattr(urllib.request, "urlopen", reject_network)

    result = canonicalize_url(
        "https://Example.com/story?utm_source=test&id=42#fragment"
    )

    assert result == "https://example.com/story?id=42"
    assert network_attempts == []

    report = analyze_news_network_surface(REPO_ROOT)
    urllib_entries = [
        item
        for item in report["local_only_paths"]
        if item["module"] == "urllib.parse"
    ]
    assert urllib_entries
    assert {
        item["callable"]
        for item in urllib_entries
    } >= {
        "urllib.parse.parse_qsl",
        "urllib.parse.urlencode",
        "urllib.parse.urlparse",
        "urllib.parse.urlunparse",
    }
    assert all(item["reason_code"] == "NO_NETWORK_CAPABILITY" for item in urllib_entries)


def test_new_unobserved_transport_fails_with_precise_diagnostics() -> None:
    report = analyze_source(
        source=(
            "import requests\n"
            "\n"
            "def acquire_news():\n"
            "    return requests.get('https://example.com/news')\n"
        ),
        relative_path="app/providers/future_news_provider.py",
    )

    assert report["network_paths"] == []
    assert report["local_only_paths"] == []
    assert report["violations"] == [
        {
            "library": "requests",
            "module": "requests",
            "callable": "requests.get",
            "file": "app/providers/future_news_provider.py",
            "function": "acquire_news",
            "line": 4,
            "classification": "UNOBSERVED_NETWORK_SURFACE",
            "reason_code": "NETWORK_CALLABLE_NOT_INTERCEPTED",
            "reason": (
                "requests.get can perform network I/O but is not covered by "
                "the validation harness allowlist and audit."
            ),
            "observed_by": (),
        }
    ]


def test_urllib_request_is_not_confused_with_urllib_parse() -> None:
    report = analyze_source(
        source=(
            "from urllib.request import urlopen\n"
            "\n"
            "def acquire_news():\n"
            "    return urlopen('https://example.com/news')\n"
        ),
        relative_path="app/providers/future_urllib_provider.py",
    )

    assert report["local_only_paths"] == []
    assert report["violations"][0]["library"] == "urllib"
    assert report["violations"][0]["module"] == "urllib.request"
    assert report["violations"][0]["callable"] == "urllib.request.urlopen"
    assert report["violations"][0]["function"] == "acquire_news"
    assert report["violations"][0]["reason_code"] == "NETWORK_CALLABLE_NOT_INTERCEPTED"
