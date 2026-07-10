from fastapi.testclient import TestClient

from app.api.deps import get_event_service, get_event_window_service, get_macro_service
from app.main import app
from app.models.events import EconomicEvent
from app.models.macro import EventWindowsResponse, MacroLatestResponse

LEGACY_TERMS = {
    "_".join(("no", "trade")),
    "_".join(("blocks", "trading")),
    "_".join(("blocking", "events")),
    "_".join(("risk", "window")),
    "/" + "/".join(("risk", "-".join(("no", "trade", "now")))),
}


def assert_no_legacy_terms(value) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in LEGACY_TERMS
            assert_no_legacy_terms(item)
    elif isinstance(value, list):
        for item in value:
            assert_no_legacy_terms(item)
    elif isinstance(value, str):
        assert all(term not in value for term in LEGACY_TERMS)


def test_health() -> None:
    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "AI-MARKET-DATA-SERVICE"}


def test_upcoming_days_validation() -> None:
    with TestClient(app) as client:
        response = client.get("/events/upcoming?days=0")

    assert response.status_code == 422


def test_active_windows_endpoint_exists() -> None:
    class FakeEventWindowService:
        async def event_windows(self, symbol: str):
            return EventWindowsResponse(
                symbol=symbol,
                checked_at_utc="2099-07-14T12:00:00+00:00",
            )

    app.dependency_overrides[get_event_window_service] = lambda: FakeEventWindowService()
    try:
        with TestClient(app) as client:
            response = client.get("/events/active-windows?symbol=MNQ")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert "active_event_windows" in payload
    assert "upcoming_event_windows" in payload


def test_public_event_payloads_do_not_include_legacy_operational_fields() -> None:
    class FakeEventService:
        last_enrichment_metadata = {"enriched_count": 1, "missing_enrichment_count": 0}

        async def today(self, country: str = "US"):
            return [EconomicEvent.model_validate(event_payload())]

        async def upcoming(self, country: str = "US", days: int = 7):
            return [EconomicEvent.model_validate(event_payload())]

    class FakeMacroService:
        async def latest(self):
            return MacroLatestResponse()

    class FakeEventWindowService:
        async def event_windows(self, symbol: str):
            return EventWindowsResponse(
                symbol=symbol,
                checked_at_utc="2099-07-14T12:00:00+00:00",
            )

    paths = [
        "/events/today?country=US",
        "/events/upcoming?country=US&days=7",
        "/events/active-windows?symbol=MNQ",
        "/market-context/mnq",
    ]
    app.dependency_overrides[get_event_service] = lambda: FakeEventService()
    app.dependency_overrides[get_macro_service] = lambda: FakeMacroService()
    app.dependency_overrides[get_event_window_service] = lambda: FakeEventWindowService()
    try:
        with TestClient(app) as client:
            for path in paths:
                response = client.get(path)
                assert response.status_code == 200
                assert_no_legacy_terms(response.json())
    finally:
        app.dependency_overrides.clear()


def test_nasdaq_data_endpoints_are_registered() -> None:
    schema = app.openapi()
    paths = schema["paths"]

    for path in [
        "/nasdaq/qqq/holdings",
        "/nasdaq/mega-cap/snapshot",
        "/nasdaq/mega-cap/breadth",
        "/nasdaq/earnings/upcoming",
        "/news/latest",
        "/nasdaq/context",
    ]:
        assert path in paths


def test_events_and_market_context_include_enrichment() -> None:
    class FakeEventService:
        last_enrichment_metadata = {"enriched_count": 1, "missing_enrichment_count": 0}

        async def today(self, country: str = "US"):
            return [EconomicEvent.model_validate(event_payload())]

        async def upcoming(self, country: str = "US", days: int = 7):
            return [EconomicEvent.model_validate(event_payload())]

    class FakeMacroService:
        async def latest(self):
            return MacroLatestResponse()

    class FakeEventWindowService:
        async def event_windows(self, symbol: str):
            return EventWindowsResponse(
                symbol=symbol,
                checked_at_utc="2099-07-14T12:00:00+00:00",
            )

    app.dependency_overrides[get_event_service] = lambda: FakeEventService()
    app.dependency_overrides[get_macro_service] = lambda: FakeMacroService()
    app.dependency_overrides[get_event_window_service] = lambda: FakeEventWindowService()
    try:
        with TestClient(app) as client:
            upcoming = client.get("/events/upcoming?country=US&days=7")
            context = client.get("/market-context/mnq")
    finally:
        app.dependency_overrides.clear()

    assert upcoming.status_code == 200
    assert upcoming.json()[0]["enrichment"]["forecast"] == "0.3%"
    assert context.status_code == 200
    assert context.json()["events_today"][0]["enrichment"]["previous"] == "0.2%"
    assert context.json()["metadata"]["event_enrichment"]["enriched_count"] == 1


def event_payload() -> dict:
    return {
        "event_id": "evt-cpi",
        "name": "Consumer Price Index",
        "country": "US",
        "category": "CPI",
        "date": "2099-07-14",
        "time_utc": "2099-07-14T12:30:00+00:00",
        "time_local": "2099-07-14T14:30:00+02:00",
        "impact": "HIGH",
        "source": "BLS",
        "source_url": "https://bls.test",
        "reliability": 0.9,
        "event_risk_level": "HIGH",
        "default_risk_window_before_minutes": 30,
        "default_risk_window_after_minutes": 30,
        "enrichment": {
            "forecast": "0.3%",
            "previous": "0.2%",
            "consensus": "0.3%",
            "actual": None,
            "source": "DailyFX Economic Calendar",
            "source_url": "https://dailyfx.test/calendar",
            "provider_type": "SCRAPER",
            "retrieved_at": "2099-07-14T11:00:00+00:00",
            "reliability": 0.56,
            "matched_by": "country_date_time_category_keywords",
            "warnings": [],
            "errors": [],
        },
    }
