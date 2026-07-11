from datetime import UTC, datetime, timedelta

from app.providers.aaii_sentiment_provider import parse_aaii_sentiment
from app.providers.cftc_cot_provider import find_nasdaq_row, parse_cftc_financial_row
from app.services.credential_audit_service import credential_audit
from app.core.config import Settings
from app.api.routes import router
from app.services.acquisition_status_service import AcquisitionStatusService, _pipeline_gaps
from app.services.market_fact_repository import MarketFactRepository
from app.services.market_news_repository import MarketNewsRepository
from app.services.positioning_runtime_service import PositioningRuntimeService


BLS_IDS = ("CUSR0000SA0", "WPUFD4", "CES0000000001", "LNS14000000")


def test_cftc_nasdaq_row_parses_net_values():
    text = '\n'.join(
        [
            '"OTHER",260630,2026-06-30,000000,CME ,00,000 ,  1,  1,  1',
            '"NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE",260630,2026-06-30,209742,CME ,00,209 ,  278558,   77804,   80900,    2486,  102755,   35445,    3554,   36796,  105413,   10251,    6991,    7940,       0,  240637,  245989,   37921,   32569,   13475,    8614,   -7097,    -801,    3986,    -416,     486,   -5256,  100.0,"(NASDAQ 100 STOCK INDEX X $20)","209742","CME ","209 ","F20","FutOnly"',
        ]
    )

    row = find_nasdaq_row(text)
    parsed = parse_cftc_financial_row(row)

    assert parsed["market_name"].startswith("NASDAQ MINI")
    assert parsed["cftc_contract_market_code"] == "209742"
    assert parsed["asset_managers"]["net"] == 77804 - 80900
    assert parsed["leveraged_funds"]["net"] == 102755 - 35445


def test_aaii_parser_requires_three_percentages():
    html = "<html>Bullish 35.2% Neutral 31.1% Bearish 33.7% July 9, 2026</html>"
    parsed = parse_aaii_sentiment(html)

    assert parsed["bullish_pct"] == 35.2
    assert round(parsed["bullish_pct"] + parsed["neutral_pct"] + parsed["bearish_pct"], 1) == 100.0


def test_credential_audit_redacts_and_allows_proceed(tmp_path):
    cfg = Settings(_env_file=None, database_path=tmp_path / "market.sqlite")
    audit = credential_audit(cfg)

    assert audit["secrets_redacted"] is True
    assert audit["can_proceed"] is True
    assert "provider_action_required" in audit
    assert not any("secret" in str(row).lower() for row in audit["providers"])


def test_acquisition_routes_registered():
    paths = {route.path for route in router.routes}

    assert "/diagnostics/credential-audit" in paths
    assert "/diagnostics/acquisition-status" in paths


def test_acquisition_status_counts_official_news_from_market_news(tmp_path):
    cfg = Settings(_env_file=None, database_path=tmp_path / "market.sqlite")
    retrieved_at = datetime.now(UTC).replace(microsecond=0)
    MarketNewsRepository(cfg).upsert_news(
        {
            "title": "Federal Reserve official release",
            "source": "Federal Reserve",
            "source_url": "https://www.federalreserve.gov/newsevents/pressreleases/test.htm",
            "published_at": (retrieved_at - timedelta(minutes=1)).isoformat(),
            "retrieved_at": retrieved_at.isoformat(),
            "provider_type": "RSS",
            "is_official": True,
            "reliability": 0.9,
        }
    )

    status = AcquisitionStatusService(cfg).status()

    assert status["blocks"]["official_news"]["status"] == "available"
    assert status["blocks"]["official_news"]["items_found"] == 1
    assert status["pipeline_integrity"]["snapshot_built_from_db"] is True
    assert status["news_pipeline"]["official_news"]["read_back"] == 1
    assert status["news_pipeline"]["official_news"]["eligible"] == 1


def test_acquisition_bls_required_series_present_has_no_snapshot_gap(tmp_path):
    cfg = Settings(_env_file=None, database_path=tmp_path / "market.sqlite")
    repo = MarketFactRepository(cfg)
    for category, value in {
        "CPI": "320.0",
        "PPI": "260.0",
        "Nonfarm Payrolls": "159000",
        "Unemployment Rate": "4.1",
    }.items():
        repo.upsert_fact(
            {
                "fact_key": f"BLS:{category}:latest:official_macro_latest",
                "fact_type": "official_macro_latest",
                "country": "US",
                "category": category,
                "event_name": category,
                "value": value,
                "source": "BLS",
                "provider_type": "API",
                "reliability": 0.95,
                "retrieved_at": "2026-07-10T10:00:00Z",
                "valid_until": "2099-07-10T10:00:00Z",
                "raw_payload_json": {"series_id": category, "value": value},
            }
        )

    status = AcquisitionStatusService(cfg).status()

    assert status["macro_pipeline"]["bls_required_series"]["present"] == list(BLS_IDS)
    assert status["macro_pipeline"]["bls_required_series"]["missing"] == []
    assert status["pipeline_integrity"]["required_macro_saved_but_missing_from_snapshot"] is False
    assert "required_macro_saved_but_missing_from_snapshot" not in status["pipeline_integrity"]["critical"]


def test_acquisition_bls_materialized_from_event_fact_metrics(tmp_path):
    cfg = Settings(_env_file=None, database_path=tmp_path / "market.sqlite")
    MarketFactRepository(cfg).upsert_fact(
        {
            "fact_key": "US:NFP:2026-08-07:macro_event_enrichment",
            "fact_type": "ai_research_result",
            "country": "US",
            "category": "NFP / Nonfarm Payrolls",
            "event_name": "Employment Situation",
            "source": "BLS Employment Situation Summary",
            "provider_type": "AI_RESEARCHER_CODEX_CLI",
            "reliability": 0.95,
            "retrieved_at": "2026-07-10T10:00:00Z",
            "valid_until": "2099-08-07T12:30:00Z",
            "raw_payload_json": {
                "metrics": [
                    {"metric_id": "headline_cpi_mom", "previous": 0.5},
                    {"metric_id": "headline_ppi_final_demand_mom", "previous": 1.1},
                    {"metric_id": "nonfarm_payrolls_change", "previous": 57},
                    {"metric_id": "unemployment_rate", "previous": 4.2},
                ]
            },
        }
    )

    status = AcquisitionStatusService(cfg).status()

    assert status["macro_pipeline"]["bls_required_series"]["present"] == list(BLS_IDS)
    assert status["macro_pipeline"]["bls_required_series"]["missing"] == []
    assert status["macro_pipeline"]["bls_required_series"]["invalid"] == []
    assert status["macro_pipeline"]["official_bls_required_series"]["missing"] == list(BLS_IDS)
    assert status["pipeline_integrity"]["required_macro_saved_but_missing_from_snapshot"] is False


def test_pipeline_gap_only_when_db_saved_series_missing_from_snapshot():
    blocks = {
        "macro": {
            "fetched_count": 0,
            "persisted_count": 0,
            "read_back_count": 0,
            "materialized_count": 0,
            "eligible_count": 0,
        }
    }
    news_pipeline = {"market_news": {"eligible_count": 0, "materialized_count": 0}}

    no_gap = _pipeline_gaps(blocks, news_pipeline, {"required_macro_saved_but_missing_from_snapshot": []})
    gap = _pipeline_gaps(blocks, news_pipeline, {"required_macro_saved_but_missing_from_snapshot": ["WPUFD4"]})

    assert no_gap["required_macro_saved_but_missing_from_snapshot"] is False
    assert gap["required_macro_saved_but_missing_from_snapshot"] is True
    assert gap["required_macro_saved_but_missing_from_snapshot_items"] == ["WPUFD4"]
    assert "required_macro_saved_but_missing_from_snapshot" in gap["critical"]


async def test_cot_refresh_force_persists_and_refresh_false_reads_db_without_network(tmp_path):
    cfg = Settings(_env_file=None, database_path=tmp_path / "market.sqlite")
    service = PositioningRuntimeService(cfg)
    fake = FakeCotProvider()
    service.cot_provider = fake

    missing = await service.cot(refresh="false")
    forced = await service.cot(refresh="force")
    cached = await service.cot(refresh="false")
    acquisition = AcquisitionStatusService(cfg).status()

    assert missing["status"] == "not_found"
    assert missing["provider_calls"] == 0
    assert fake.calls == 1
    assert forced["status"] == "found"
    assert forced["provider_calls"] == 1
    assert forced["persisted_count"] == 1
    assert forced["read_back_count"] == 1
    assert forced["materialized_count"] == 1
    assert cached["status"] == "found"
    assert cached["cache_used"] is True
    assert cached["provider_calls"] == 0
    assert cached["AI_called"] is False
    assert cached["report_date"] == forced["report_date"]
    assert acquisition["blocks"]["cot"]["attempted"] is True
    assert acquisition["blocks"]["cot"]["provider_calls"] == 1
    assert acquisition["blocks"]["cot"]["persisted_count"] == 1
    assert acquisition["blocks"]["cot"]["read_back_count"] == 1
    assert acquisition["blocks"]["cot"]["materialized_count"] == 1


class FakeCotProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def fetch_nasdaq(self):
        self.calls += 1
        return {
            "status": "found",
            "report_date": "2026-07-07",
            "publication_date": None,
            "market_name": "NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE",
            "cftc_contract_market_code": "209742",
            "report_type": "FutOnly",
            "asset_managers": {"long": 10, "short": 4, "spreading": 1, "net": 6, "net_change_week": 2},
            "leveraged_funds": {"long": 20, "short": 15, "spreading": 2, "net": 5, "net_change_week": -1},
            "dealers": {"long": 3, "short": 8, "net": -5},
            "open_interest": 100,
            "source": "CFTC",
            "source_url": "https://www.cftc.gov/dea/newcot/FinFutWk.txt",
            "retrieved_at": "2026-07-10T10:00:00Z",
            "valid_until": "2099-07-17T10:00:00Z",
            "reliability": 0.95,
            "attempted_sources": ["https://www.cftc.gov/dea/newcot/FinFutWk.txt"],
            "duration_ms": 1,
            "warnings": [],
            "errors": [],
        }
