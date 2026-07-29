from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.core.config import Settings
from app.services.market_context_sync_service import (
    MarketContextSyncService,
    SyncContractError,
    canonical_json,
    news_record_delta,
    section_status,
)
from app.services.market_news_repository import MarketNewsRepository
from app.services.news_intelligence_service import (
    build_news_context,
    normalize_news_article,
)
from app.services.source_policy_service import SourcePolicyService


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "lossless_news_forensic_redacted.json"
NOW = datetime(2026, 7, 28, 16, 0, tzinfo=UTC)


def fixture_records() -> list[dict]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))["records"]


def row(
    title: str,
    *,
    source: str = "Reuters",
    url: str = "https://www.reuters.com/world/us/redacted",
    summary: str | None = (
        "A sufficiently detailed redacted summary preserves the offline record shape."
    ),
    published_at: str = "2026-07-28T12:00:00Z",
    valid_until: str | None = "2026-07-28T18:00:00Z",
    **extra,
) -> dict:
    return {
        "provider_record_id": extra.pop(
            "provider_record_id",
            f"record-{abs(hash((title, url, published_at)))}",
        ),
        "title": title,
        "summary": summary,
        "source": source,
        "original_publisher": source,
        "source_url": url,
        "published_at": published_at,
        "retrieved_at": "2026-07-28T12:01:00Z",
        "valid_until": valid_until,
        "provider_type": "RSS",
        **extra,
    }


def test_01_reuters_distributed_by_yahoo_preserves_lineage_and_is_delivered():
    article = build_news_context([fixture_records()[0]], now=NOW)["articles"][0]
    assert article["original_publisher"] == "Reuters"
    assert article["distributor"] == "Yahoo Finance"
    assert article["acquisition_provider"] == "RSS"
    assert article["publisher_status"] == "VERIFIED"


def test_02_direct_reuters_is_not_subject_to_distributor_gate():
    article = normalize_news_article(row("Direct Reuters report"), now=NOW)
    assert article["accepted"] is True
    assert article["distributor"] is None
    assert article["distributor_status"] == "NOT_APPLICABLE"


def test_03_valid_ibd_record_is_delivered_even_when_historical():
    context = build_news_context([fixture_records()[2]], now=NOW)
    assert len(context["articles"]) == 1
    assert len(context["historical_articles"]) == 1
    assert context["articles"][0]["original_publisher"] == (
        "Investor's Business Daily"
    )


def test_04_official_federal_reserve_rss_has_verified_lineage():
    article = build_news_context([fixture_records()[3]], now=NOW)["articles"][0]
    assert article["is_official_source"] is True
    assert article["publisher_status"] == "VERIFIED"
    assert article["provenance"]["source_url"].startswith(
        "https://www.federalreserve.gov/"
    )


def test_05_unknown_originator_through_yahoo_is_delivered_degraded():
    context = build_news_context([fixture_records()[4]], now=NOW)
    article = context["articles"][0]
    assert context["diagnostics"]["quarantined"] == 0
    assert article["source_verification_status"] == "UNKNOWN"
    assert article["analysis_usability"] == "DEGRADED"
    assert "ORIGINAL_PUBLISHER_UNVERIFIED" in article["warning_codes"]


def test_06_low_relevance_is_delivered_as_descriptive_metadata():
    article = build_news_context(
        [row("Small regional business opens a new office")],
        now=NOW,
    )["articles"][0]
    assert article["relevance"] == "LOW"
    assert article["accepted"] is True


def test_07_ambiguous_topic_is_explicit_and_delivered():
    article = build_news_context(
        [row("Small regional business opens a new office")],
        now=NOW,
    )["articles"][0]
    assert article["topics"] == []
    assert article["topic_status"] == "AMBIGUOUS"


def test_08_record_without_symbols_is_delivered():
    article = build_news_context(
        [row("Treasury market conditions changed during the session")],
        now=NOW,
    )["articles"][0]
    assert article["symbols"] == []


def test_09_headline_only_does_not_invent_content():
    article = build_news_context(
        [row("Headline only dispatch", summary=None)],
        now=NOW,
    )["articles"][0]
    assert article["content_status"] == "HEADLINE_ONLY"
    assert article["summary"] is None
    assert article["content"] is None


def test_10_summary_only_has_explicit_content_status():
    article = build_news_context([row("Summary-only dispatch")], now=NOW)[
        "articles"
    ][0]
    assert article["content_status"] == "SUMMARY_ONLY"
    assert article["content"] is None


def test_11_expired_record_inside_scope_is_delivered_as_historical():
    article = row(
        "Historical Reuters dispatch",
        published_at="2026-07-10T12:00:00Z",
        valid_until="2026-07-10T18:00:00Z",
    )
    context = build_news_context([article], now=NOW, scope_days=30)
    assert len(context["articles"]) == 1
    assert context["historical_articles"][0]["lifecycle_status"] == "EXPIRED"


def test_12_record_outside_declared_scope_is_counted_explicitly():
    article = row(
        "Out-of-scope Reuters dispatch",
        published_at="2026-05-01T12:00:00Z",
        valid_until="2026-05-01T18:00:00Z",
    )
    context = build_news_context([article], now=NOW, scope_days=30)
    assert context["articles"] == []
    assert context["diagnostics"]["outside_scope"] == 1
    assert context["excluded"][0]["reason"] == "outside_declared_scope"


def test_13_temporally_distinct_reuters_updates_are_both_delivered():
    context = build_news_context(fixture_records()[:2], now=NOW)
    assert len(context["articles"]) == 2
    assert len({item["published_at"] for item in context["articles"]}) == 2


def test_14_two_publishers_on_same_event_are_both_preserved():
    rows = [
        row("Export controls updated for a chip maker"),
        row(
            "Export controls updated for a chip maker",
            source="Associated Press",
            url="https://apnews.com/article/redacted-chip-update",
            provider_record_id="ap-redacted",
        ),
    ]
    context = build_news_context(rows, now=NOW)
    assert len(context["articles"]) == 2
    assert {item["original_publisher"] for item in context["articles"]} == {
        "Reuters",
        "Associated Press",
    }


def test_15_no_top_n_cap_or_order_dependent_selection():
    rows = [
        row(
            f"Reuters record {index}",
            url=f"https://www.reuters.com/world/us/redacted-{index}",
            provider_record_id=f"record-{index}",
        )
        for index in range(401)
    ]
    forward = build_news_context(rows, now=NOW)
    reverse = build_news_context(list(reversed(rows)), now=NOW)
    assert len(forward["articles"]) == 401
    assert canonical_json(forward["articles"]) == canonical_json(
        reverse["articles"]
    )


def test_16_unicode_multimegabyte_content_is_lossless():
    original = "Mercati € 東京 🚀 " * 150_000
    article = build_news_context(
        [row("Unicode full-text record", content=original)],
        now=NOW,
    )["articles"][0]
    assert len(original.encode("utf-8")) > 2_000_000
    assert article["content"] == original
    assert article["content_status"] == "FULL_TEXT_AVAILABLE"


def test_17_record_delta_contains_only_new_updated_and_lifecycle_changes():
    base_rows = [
        row("Unchanged", provider_record_id="same"),
        row("Updated", provider_record_id="updated"),
        row("Lifecycle", provider_record_id="life"),
    ]
    target_rows = [
        base_rows[0],
        {**base_rows[1], "summary": "A materially updated redacted summary with new facts."},
        {**base_rows[2], "valid_until": "2026-07-28T11:00:00Z"},
        row("New", provider_record_id="new"),
    ]
    base = {"context": build_news_context(base_rows, now=NOW)}
    target = {"context": build_news_context(target_rows, now=NOW)}
    delta = news_record_delta(
        base,
        target,
        base_snapshot_revision=1,
        target_snapshot_revision=2,
    )
    assert delta["incremental"]["mode"] == "RECORD_DELTA"
    assert delta["incremental"]["new_count"] == 1
    assert delta["incremental"]["updated_count"] == 1
    assert delta["incremental"]["lifecycle_change_count"] == 1
    assert delta["incremental"]["unchanged_count"] == 1
    assert len(delta["context"]["articles"]) == 3


def test_18_invalid_consumer_ack_is_rejected_offline(tmp_path: Path):
    settings = Settings(_env_file=None, database_path=tmp_path / "ack.sqlite")
    service = MarketContextSyncService(settings)
    with pytest.raises(SyncContractError, match="delivery_not_found"):
        service.acknowledge(
            {
                "consumer_id": "consumer",
                "delivery_id": "stale-or-wrong",
                "snapshot_revision": 1,
                "status": "PERSISTED",
                "section_revisions": {"news": 1},
                "checksum": "0" * 64,
                "acknowledged_at": NOW.isoformat(),
            }
        )


def test_19_replay_is_byte_identical_and_fixed_point():
    records = fixture_records()
    first = canonical_json(build_news_context(records, now=NOW)).encode("utf-8")
    second = canonical_json(build_news_context(records, now=NOW)).encode("utf-8")
    assert first == second


@pytest.mark.parametrize(
    ("records", "expected"),
    [
        ([fixture_records()[0]], "DEGRADED"),
        ([fixture_records()[0], fixture_records()[4]], "DEGRADED"),
        ([fixture_records()[4]], "DEGRADED"),
        ([], "UNAVAILABLE"),
    ],
)
def test_20_readiness_status_matches_delivered_content(records, expected):
    context = build_news_context(records, now=NOW)
    status, _ = section_status(
        {"context": context},
        section_name="news",
    )
    assert status == expected


def test_21_pipeline_has_zero_ai_browser_or_backend_invocations():
    context = build_news_context(fixture_records(), now=NOW)
    assert context["pipeline_version"] == "lossless_news_intelligence_v4"
    encoded = canonical_json(context)
    assert '"AI_called":true' not in encoded
    assert '"provider_calls":1' not in encoded
    assert '"browser_calls":1' not in encoded


def test_22_accounting_equation_has_no_unexplained_loss():
    records = fixture_records()
    context = build_news_context(records, now=NOW)
    diagnostics = context["diagnostics"]
    assert diagnostics["accounting_balanced"] is True
    assert diagnostics["raw_acquired"] == (
        diagnostics["persisted_valid"]
        + diagnostics["technically_rejected"]
    )
    assert diagnostics["persisted_valid_in_scope"] == (
        diagnostics["delivered"]
        + diagnostics["quarantined"]
        + diagnostics["withheld"]
    )
    assert diagnostics["delivered"] == (
        diagnostics["current_delivered"]
        + diagnostics["historical_delivered"]
    )


def test_23_technical_duplicate_classification_is_nondestructive():
    duplicate = row("Exact technical retry", provider_record_id="retry")
    context = build_news_context([duplicate, dict(duplicate)], now=NOW)
    assert len(context["articles"]) == 1
    assert context["diagnostics"]["delivered"] == 2
    assert context["diagnostics"]["duplicate_count"] == 1
    assert len(context["articles"][0]["source_occurrences"]) == 2
    assert (
        context["duplicates"][0]["disposition"]
        == "CONSOLIDATED_WITH_LINEAGE"
    )


def test_24_absence_does_not_create_an_unconfirmed_removal():
    base = {"context": build_news_context([row("Base record")], now=NOW)}
    target = {"context": build_news_context([], now=NOW)}
    delta = news_record_delta(
        base,
        target,
        base_snapshot_revision=1,
        target_snapshot_revision=2,
    )
    assert delta["incremental"]["confirmed_removal_count"] == 0
    assert delta["incremental"]["absence_not_confirmed_count"] == 1


def test_25_policy_marks_unknown_yahoo_originator_as_degraded_not_invalid():
    policy = SourcePolicyService()
    decision = policy.validate(
        fixture_records()[4],
        field_semantics="news",
    )
    assert decision.accepted is True
    assert decision.reasons == ()


def test_26_null_publisher_is_delivered_with_explicit_unknown_metadata():
    article = build_news_context(
        [
            {
                "provider_record_id": "publisher-null",
                "title": "Valid publisher-null market dispatch",
                "summary": "A valid source record can preserve uncertainty without losing the news.",
                "source": None,
                "original_publisher": None,
                "source_url": "https://independent-wire.example.net/item/1",
                "published_at": "2026-07-28T12:00:00Z",
                "retrieved_at": "2026-07-28T12:01:00Z",
                "provider_type": "RSS",
            }
        ],
        now=NOW,
    )["articles"][0]
    assert article["original_publisher"] is None
    assert article["original_publisher_status"] == "UNKNOWN"
    assert article["source_verification_status"] == "UNKNOWN"
    assert article["analysis_usability"] == "DEGRADED"


def test_26b_null_publisher_on_known_direct_host_is_not_a_contradiction():
    context = build_news_context(
        [
            {
                "provider_record_id": "publisher-null-direct",
                "title": "Valid publisher-null direct-host dispatch",
                "summary": "The URL is safe evidence, but it does not justify inventing a publisher.",
                "source": None,
                "publisher": None,
                "original_publisher": None,
                "source_url": "https://www.reuters.com/world/us/redacted-direct-host",
                "published_at": "2026-07-28T12:00:00Z",
                "retrieved_at": "2026-07-28T12:01:00Z",
                "provider_type": "RSS",
            }
        ],
        now=NOW,
    )
    assert context["quarantined_records"] == []
    article = context["articles"][0]
    assert article["original_publisher"] is None
    assert article["source_verification_status"] == "UNKNOWN"
    assert article["analysis_usability"] == "DEGRADED"


def test_26c_editorial_timestamps_survive_repository_round_trip(
    tmp_path: Path,
):
    repository = MarketNewsRepository(
        Settings(
            _env_file=None,
            database_path=tmp_path / "news-timestamps.sqlite",
        ),
        clock=lambda: NOW,
    )
    repository.upsert_news(
        row(
            "Timestamp-preserving dispatch",
            first_seen_at="2026-07-28T12:01:00Z",
            last_seen_at="2026-07-28T12:04:00Z",
            updated_at="2026-07-28T12:03:00Z",
        )
    )
    article = repository.stored(days=30)[0]
    assert article["first_seen_at"] == "2026-07-28T12:01:00+00:00"
    assert article["last_seen_at"] == "2026-07-28T12:04:00+00:00"
    assert article["updated_at"] == "2026-07-28T12:03:00+00:00"
    assert article["persistence_updated_at"] == NOW.isoformat()


def test_27_authorized_acquisition_provider_does_not_promote_publisher():
    article = build_news_context(
        [
            row(
                "Provider-authorized unverified publisher",
                source="Declared Wire",
                url="https://finance.yahoo.com/news/provider-authorized",
                provider="FINNHUB",
                provider_type="API",
            )
        ],
        now=NOW,
    )["articles"][0]
    assert article["acquisition_provider"] == "FINNHUB"
    assert article["publisher_status"] == "DECLARED_UNVERIFIED"
    assert article["source_verification_status"] == "UNKNOWN"
    assert article["accepted"] is True


def test_28_missing_url_with_stable_source_identity_is_delivered():
    article = build_news_context(
        [
            row(
                "URL-less but source-identified dispatch",
                url="",
                provider_record_id="stable-source-id-without-url",
            )
        ],
        now=NOW,
    )["articles"][0]
    assert article["source_url"] is None
    assert article["raw_source_identity"]["provider_record_id"] == (
        "stable-source-id-without-url"
    )
    assert "canonical_unresolved" in article["warnings"]


def test_29_full_content_is_preserved_under_both_contract_names():
    full_text = "Full editorial content with exact Unicode: € 東京 🚀."
    article = build_news_context(
        [row("Full-content dispatch", content=full_text)],
        now=NOW,
    )["articles"][0]
    assert article["content"] == full_text
    assert article["full_content"] == full_text
    assert article["content_availability_status"] == "FULL_TEXT_AVAILABLE"


def test_30_exact_syndication_consolidates_content_and_preserves_lineage():
    common = {
        "title": "Exact Reuters syndicated occurrence",
        "summary": "The exact same Reuters copy is distributed through two audited channels.",
        "source": "Reuters",
        "original_publisher": "Reuters",
        "published_at": "2026-07-28T12:00:00Z",
        "retrieved_at": "2026-07-28T12:01:00Z",
        "provider_type": "RSS",
    }
    context = build_news_context(
        [
            {
                **common,
                "provider_record_id": "yahoo-copy",
                "distribution_source": "Yahoo Finance",
                "source_url": "https://finance.yahoo.com/news/exact-copy",
            },
            {
                **common,
                "provider_record_id": "msn-copy",
                "distribution_source": "MSN",
                "source_url": "https://www.msn.com/en-us/money/exact-copy",
            },
        ],
        now=NOW,
    )
    assert len(context["articles"]) == 1
    assert context["diagnostics"]["delivered"] == 2
    occurrences = context["articles"][0]["source_occurrences"]
    assert {item["distribution_source"] for item in occurrences} == {
        "Yahoo Finance",
        "MSN",
    }


def test_31_identical_timestamp_with_different_content_stays_distinct():
    context = build_news_context(
        [
            row(
                "Same-timestamp Reuters update",
                provider_record_id="content-a",
                content="First materially distinct content body.",
            ),
            row(
                "Same-timestamp Reuters update",
                provider_record_id="content-b",
                content="Second materially distinct content body.",
            ),
        ],
        now=NOW,
    )
    assert len(context["articles"]) == 2
    assert {item["full_content"] for item in context["articles"]} == {
        "First materially distinct content body.",
        "Second materially distinct content body.",
    }


def test_32_authentic_empty_and_concrete_quarantine_have_distinct_readiness():
    empty = build_news_context([], now=NOW, authentic_empty=True)
    assert section_status(
        {"context": empty},
        section_name="news",
    )[0] == "NO_DATA"
    unsafe = build_news_context(
        [
            row(
                "Unsafe credential-bearing URL",
                url="https://redacted-user@unsafe.example.org/item",
            )
        ],
        now=NOW,
    )
    assert unsafe["articles"] == []
    assert unsafe["diagnostics"]["quarantined"] == 1
    assert unsafe["diagnostics"]["non_delivered_records"][0][
        "reason_code"
    ] == "SOURCE_URL_CREDENTIALS_FORBIDDEN"
