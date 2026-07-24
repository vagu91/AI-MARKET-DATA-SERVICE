from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import HTTPException

from app.core.config import Settings
from app.infrastructure.persistence import migrations
from app.infrastructure.persistence.database import connect_sqlite
from app.infrastructure.persistence.database_safety import assert_test_database_isolated
from app.services.data_integrity_service import classify_source
from app.services.market_context_snapshot_repository import MarketContextSnapshotRepository
from app.services.market_fact_repository import MarketFactRepository
from app.services.source_policy_service import SourcePolicyService
from app.services.research_gap_manifest import ResearchGapManifestBuilder
from app.services.ai_research_job_service import AIResearchJobService
from app.api.routes import market_context_mnq


@pytest.mark.parametrize(
    ("url", "reason"),
    [
        ("https://bls.test", "SOURCE_HOST_RESERVED"),
        ("https://dailyfx.test/calendar", "SOURCE_HOST_RESERVED"),
        ("https://source.invalid/x", "SOURCE_HOST_RESERVED"),
        ("https://source.example/x", "SOURCE_HOST_RESERVED"),
        ("https://example.com/x", "SOURCE_HOST_RESERVED"),
        ("https://localhost/x", "SOURCE_HOST_LOCALHOST"),
        ("https://127.0.0.1/x", "SOURCE_HOST_NON_PUBLIC_IP"),
        ("http://bls.gov/x", "SOURCE_URL_HTTPS_REQUIRED"),
    ],
)
def test_reserved_or_unsafe_source_urls_are_rejected(url: str, reason: str) -> None:
    decision = SourcePolicyService().validate_url(url)
    assert decision.accepted is False
    assert decision.reason_code == reason


@pytest.mark.parametrize(
    ("url", "accepted"),
    [
        ("https://bls.gov", True),
        ("https://www.bls.gov/schedule", True),
        ("https://evilbls.gov", False),
        ("https://bls.gov.evil.com", False),
    ],
)
def test_official_source_dns_boundary(url: str, accepted: bool) -> None:
    policy = SourcePolicyService()
    provenance = policy.provenance(source="BLS", source_url=url)
    assert provenance["is_official_source"] is accepted
    assert bool(policy.rule_for(url)) is accepted


def test_invalid_domain_overrides_bls_label_and_claimed_flags() -> None:
    result = classify_source("BLS", "https://bls.test")
    assert result["is_official_source"] is False
    assert result["source_is_primary_originator"] is False
    assert result["data_origin_is_official"] is False
    assert result["distribution_source_is_official"] is False
    assert result["reliability"] == 0
    assert result["validation"]["reason_code"] == "SOURCE_HOST_RESERVED"


def test_trusted_deterministic_official_resolver_requires_valid_canonical_url() -> None:
    result = SourcePolicyService().provenance(
        source="BLS",
        source_url="https://www.bls.gov/schedule",
        trusted_resolver="bls_release_calendar",
        server_owned_lineage=True,
    )
    assert result["is_official_source"] is True
    assert result["source_is_primary_originator"] is True
    rejected = SourcePolicyService().provenance(
        source="BLS",
        source_url="https://bls.test",
        trusted_resolver="bls_release_calendar",
        server_owned_lineage=True,
    )
    assert rejected["is_official_source"] is False


def test_schema_17_to_18_quarantines_without_deleting_and_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "migration.sqlite"
    full_migrations = migrations.MIGRATIONS
    monkeypatch.setattr(migrations, "MIGRATIONS", full_migrations[:17])
    assert migrations.migrate_database(database)["schema_version"] == 17
    with connect_sqlite(database) as conn:
        conn.execute(
            """
            INSERT INTO economic_events_history(
              event_key,source,source_url,official_reliability,raw_payload_json,
              status,created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                "event-contaminated",
                "BLS",
                "https://bls.test",
                0.9,
                json.dumps({"source_url": "https://dailyfx.test/calendar"}),
                "SCHEDULED",
                "2026-07-23T00:00:00+00:00",
                "2026-07-23T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO market_context_snapshots(
              snapshot_id,symbol,revision,generated_at,data_as_of,refresh_mode,
              debug_payload_json,consumer_payload_json,ai_status,checksum,created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "snapshot-contaminated",
                "MNQ",
                1,
                "2026-07-23T00:00:00+00:00",
                "2026-07-23T00:00:00+00:00",
                "auto",
                json.dumps({"source_url": "https://bls.test"}),
                json.dumps({"source_url": "https://dailyfx.test/calendar"}),
                "NOT_REQUIRED",
                "checksum",
                "2026-07-23T00:00:00+00:00",
            ),
        )
        conn.commit()

    monkeypatch.setattr(migrations, "MIGRATIONS", full_migrations)
    first = migrations.migrate_database(database)
    second = migrations.migrate_database(database)
    assert first["schema_version"] == 19
    assert first["source_reconciliation"]["quarantined_count"] == 2
    assert second["applied"] == []
    with connect_sqlite(database) as conn:
        event = conn.execute(
            "SELECT source_url,official_reliability,source_audit_status,"
            "source_invalid_reason FROM economic_events_history"
        ).fetchone()
        snapshot = conn.execute(
            "SELECT source_audit_status FROM market_context_snapshots"
        ).fetchone()
        quarantine_count = conn.execute(
            "SELECT COUNT(*) FROM source_quarantine"
        ).fetchone()[0]
    assert event["source_url"] == "https://bls.test"
    assert event["official_reliability"] == 0
    assert event["source_audit_status"] == "QUARANTINED"
    assert event["source_invalid_reason"] == "SOURCE_HOST_RESERVED"
    assert snapshot["source_audit_status"] == "QUARANTINED"
    assert quarantine_count == 4
    assert MarketContextSnapshotRepository(
        Settings(_env_file=None, environment="test", database_path=database)
    ).latest("MNQ") is None


def test_invalid_event_upsert_is_sticky_and_excluded(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, environment="local", database_path=tmp_path / "market.sqlite")
    repository = MarketFactRepository(settings)
    invalid = {
        "event_id": "evt",
        "name": "CPI",
        "country": "US",
        "category": "CPI",
        "date": "2026-07-23",
        "time_utc": "2026-07-23T12:30:00+00:00",
        "source": "BLS",
        "source_url": "https://bls.test",
        "reliability": 0.9,
    }
    repository.upsert_economic_event(invalid, "event-key")
    repository.upsert_economic_event(
        {**invalid, "source_url": "https://bls.gov", "reliability": 0.99},
        "event-key",
    )
    with connect_sqlite(settings.database_path) as conn:
        row = conn.execute(
            "SELECT source_audit_status,source_invalid_reason FROM economic_events_history"
        ).fetchone()
    assert row["source_audit_status"] == "QUARANTINED"
    assert row["source_invalid_reason"] == "SOURCE_HOST_RESERVED"
    assert repository.economic_event_payloads(
        country="US",
        start_date="2026-07-01",
        end_date="2026-07-31",
    ) == []


def test_snapshot_materialization_removes_invalid_sources_from_both_views(
    tmp_path: Path,
) -> None:
    settings = Settings(_env_file=None, environment="local", database_path=tmp_path / "market.sqlite")
    repository = MarketContextSnapshotRepository(settings)
    contaminated = {
        "generated_at": "2026-07-23T00:00:00+00:00",
        "event_calendar": {
            "critical_macro_events": [
                {"name": "CPI", "source": "BLS", "source_url": "https://bls.test"}
            ]
        },
        "event_risk": {
            "critical_events": [
                {
                    "metrics": [
                        {"value": "0.3%", "source_url": "https://dailyfx.test/calendar"}
                    ]
                }
            ]
        },
    }
    stored = repository.save(
        snapshot_id="clean-projection",
        revision=1,
        symbol="MNQ",
        refresh_mode="auto",
        debug_payload=contaminated,
        consumer_payload=contaminated,
        ai_status="NOT_REQUIRED",
    )
    assert ".test" not in json.dumps(stored["debug_payload"])
    assert ".test" not in json.dumps(stored["consumer_payload"])
    assert len(json.dumps(stored["consumer_payload"]).encode("utf-8")) < 90_000


def test_source_quarantine_diagnostics_are_compact(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, environment="local", database_path=tmp_path / "market.sqlite")
    repository = MarketFactRepository(settings)
    repository.upsert_fact(
        {
            "fact_key": "invalid-source-fact",
            "fact_type": "macro_event_enrichment",
            "source_url": "https://dailyfx.test/calendar",
            "raw_payload_json": {"source_url": "https://dailyfx.test/calendar"},
        }
    )
    diagnostics = MarketContextSnapshotRepository(settings).source_quarantine_read_model()
    assert diagnostics["invalid_source_count"] == 1
    assert diagnostics["by_reason_code"] == {"SOURCE_HOST_RESERVED": 1}
    assert diagnostics["domains"] == ["dailyfx.test"]
    assert "source_url" not in diagnostics


def test_test_bootstrap_rejects_operational_database() -> None:
    with pytest.raises(RuntimeError, match="test_database_not_isolated"):
        assert_test_database_isolated(
            Path("data/market_data_service.sqlite"),
            environment="test",
        )


def test_source_policy_v3_is_preserved() -> None:
    assert SourcePolicyService().policy_version == "source-policy-v3"
    assert SourcePolicyService().validate_url(
        "https://fixture.test",
        allow_test_reserved=True,
    ).accepted


@pytest.mark.asyncio
async def test_refresh_false_ignores_quarantined_snapshot_without_calls_or_writes(
    tmp_path: Path,
) -> None:
    settings = Settings(
        _env_file=None,
        environment="local",
        database_path=tmp_path / "market.sqlite",
    )
    repository = MarketContextSnapshotRepository(settings)
    repository.save(
        snapshot_id="unusable",
        revision=1,
        symbol="MNQ",
        refresh_mode="seed",
        debug_payload={"generated_at": "2026-07-23T00:00:00+00:00"},
        consumer_payload={"data_as_of": "2026-07-23T00:00:00+00:00"},
        ai_status="NOT_REQUIRED",
    )
    with connect_sqlite(settings.database_path) as conn:
        conn.execute(
            "UPDATE market_context_snapshots SET source_audit_status='QUARANTINED',"
            "source_invalid_reason='SOURCE_HOST_RESERVED'"
        )
        conn.commit()
        count_before = conn.execute(
            "SELECT COUNT(*) FROM market_context_snapshots"
        ).fetchone()[0]

    class NoCalls:
        def __init__(self, configured_settings):
            self.settings = configured_settings

        def __getattr__(self, name):
            raise AssertionError(f"unexpected call:{name}")

    no_calls = NoCalls(settings)
    with pytest.raises(HTTPException) as raised:
        await market_context_mnq(
            refresh="false",
            view="consumer",
            macro_service=no_calls,
            event_service=no_calls,
            event_window_service=no_calls,
            nasdaq_service=no_calls,
            enrichment_orchestrator=no_calls,
        )
    assert raised.value.status_code == 404
    with connect_sqlite(settings.database_path) as conn:
        assert conn.total_changes == 0
        assert (
            conn.execute("SELECT COUNT(*) FROM market_context_snapshots").fetchone()[0]
            == count_before
        )


def test_invalid_source_reopens_gap_and_is_removed_from_agent_input(
    tmp_path: Path,
) -> None:
    settings = Settings(
        _env_file=None,
        environment="local",
        database_path=tmp_path / "market.sqlite",
    )
    event = {
        "event_id": "cpi",
        "name": "CPI",
        "country": "US",
        "category": "CPI",
        "date": "2026-07-24",
        "time_utc": "2026-07-24T12:30:00+00:00",
        "source": "BLS",
        "source_url": "https://bls.test",
        "retrieved_at": "2026-07-23T12:00:00+00:00",
    }
    manifest = ResearchGapManifestBuilder(settings).build(
        snapshot=None,
        components={"event_calendar": {"critical_macro_events": [event]}},
        persist=False,
    )
    macro = next(item for item in manifest["items"] if item["topic"] == "macro_events")
    assert macro["deterministic_status"] in {"MISSING", "NEEDS_AGENT_RESEARCH"}
    assert macro["deterministic_status"] != "SATISFIED_FRESH_DB"

    prompt_payload = AIResearchJobService(settings)._payload(
        job_type="MISSING_EVENT_RESEARCH",
        symbol="MNQ",
        event=event,
        pending_fields=["forecast"],
    )
    assert "https://bls.test" not in json.dumps(prompt_payload)
    assert prompt_payload["event"]["event_id"] == "cpi"
