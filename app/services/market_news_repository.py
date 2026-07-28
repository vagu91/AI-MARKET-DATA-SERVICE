from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.core.text_normalization import normalize_payload_text
from app.core.redaction import redact_payload
from app.core.config import Settings
from app.services.data_freshness_service import DataFreshnessService, parse_datetime
from app.services.data_integrity_service import news_content_status
from app.services.fact_key_service import FactKeyService
from app.services.market_fact_repository import (
    _record_source_quarantine,
    connect_market_db,
    decode,
    encode,
    init_market_db,
)
from app.services.news_intelligence_service import (
    DEFAULT_CURRENT_WINDOW_HOURS,
    normalize_news_article,
)
from app.services.source_policy_service import (
    SourcePolicyService,
    SourceUrlValidation,
)


class MarketNewsRepository:
    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.clock = clock or (lambda: datetime.now(UTC))
        self.keys = FactKeyService()
        self.freshness = DataFreshnessService(settings)
        self.source_policy = SourcePolicyService(settings.source_policy_path)
        init_market_db(settings)

    def upsert_news(self, article: dict[str, Any]) -> dict[str, Any]:
        article = normalize_news_article(normalize_payload_text(redact_payload(dict(article))))
        content_status = news_content_status(article)
        if content_status == "invalid_content":
            article["content_status"] = "invalid_content"
            warnings = list(article.get("warnings") or [])
            if "invalid_content" not in warnings:
                warnings.append("invalid_content")
            article["warnings"] = warnings
        source_url = str(article.get("source_url") or article.get("url") or "")
        identity_only = not source_url and any(
            article.get(key) not in (None, "")
            for key in (
                "news_key",
                "provider_record_id",
                "occurrence_id",
                "article_id",
                "record_id",
            )
        )
        if not source_url and not identity_only:
            raise ValueError("news source identity is required")
        current = self.clock()
        timestamp = current.replace(microsecond=0).isoformat()
        published_at = parse_datetime(article.get("published_at"))
        if published_at and published_at > current + timedelta(minutes=5):
            raise ValueError("future_news_timestamp")
        policy = self.source_policy.validate(article, field_semantics="news")
        source_validation = self.source_policy.validate_url(
            source_url,
            allow_test_reserved=self.settings.environment.lower() == "test",
        )
        reserved_fixture_source = (
            self.settings.environment.lower() == "test"
            and source_validation.accepted
            and not policy.accepted
            and bool(policy.reasons)
            and set(policy.reasons).issubset(
                {
                    "SOURCE_HOST_RESERVED",
                    "SOURCE_HOST_LOCALHOST",
                    "SOURCE_HOST_NON_PUBLIC_IP",
                }
            )
        )
        source_admitted = policy.accepted and (
            source_validation.accepted or identity_only
        ) or reserved_fixture_source
        topics = list(article.get("topics") or [])
        source_identity_seed = source_url or (
            f"identity:{article.get('acquisition_provider') or article.get('provider_type') or 'UNKNOWN'}:"
            f"{article.get('provider_record_id') or article.get('occurrence_id') or article.get('article_id') or article.get('record_id')}"
        )
        payload = {
            "news_key": article.get("news_key") or self.keys.news_key(title=str(article.get("title") or ""), source_url=source_identity_seed),
            "title": article.get("title") or "",
            "summary": article.get("summary"),
            "content_snippet": article.get("content_snippet"),
            "source": article.get("source"),
            "source_url": source_url,
            "published_at": article.get("published_at"),
            "retrieved_at": article.get("retrieved_at") or timestamp,
            "valid_from": article.get("valid_from") or article.get("published_at") or timestamp,
            "symbols_json": encode(article.get("symbols") or []),
            "topics_json": encode(topics),
            "country": article.get("country"),
            "category": article.get("category"),
            "relevance": article.get("relevance"),
            "reliability": article.get("reliability") or 0,
            "confidence": article.get("confidence") or article.get("reliability") or 0,
            "provider_type": article.get("provider_type"),
            "is_official": 1 if article.get("is_official_source") or article.get("is_official") else 0,
            "is_duplicate": 1 if article.get("is_duplicate") else 0,
            "raw_payload_json": encode(article),
            "created_at": article.get("created_at") or timestamp,
            "updated_at": timestamp,
            "canonical_url": article.get("canonical_url") or source_url or None,
            "aggregator_url": article.get("aggregator_url"),
            "original_publisher": article.get("original_publisher") or article.get("publisher") or article.get("source"),
            "source_tier": article.get("source_tier") or policy.tier,
            "source_classification": article.get("source_classification") or policy.classification,
            "source_audit_status": (
                "ACTIVE"
                if source_admitted
                else "QUARANTINED"
            ),
            "source_invalid_reason": (
                (
                    source_validation.reason_code
                    if not source_validation.accepted and not identity_only
                    else None
                )
                or (",".join(policy.reasons) if not policy.accepted else None)
            ),
        }
        article["validation"] = {
            "status": (
                "accepted"
                if source_admitted
                and article.get("source_verification_status") == "VERIFIED"
                else "accepted_degraded"
                if source_admitted
                else "unverified"
                if reserved_fixture_source
                else "rejected"
            ),
            "reason_code": payload["source_invalid_reason"],
            "policy_version": policy.policy_version,
        }
        if not source_admitted:
            payload["reliability"] = 0.0
            payload["confidence"] = 0.0
            payload["is_official"] = 0
        payload["valid_until"] = article.get("valid_until") or self.freshness.news_valid_until(
            published_at=payload["published_at"],
            retrieved_at=payload["retrieved_at"],
            topics=topics,
        )
        payload["next_refresh_at"] = article.get("next_refresh_at") or self.freshness.next_refresh_at(payload["valid_until"])
        parsed_valid_until = parse_datetime(payload["valid_until"])
        payload["lifecycle_status"] = (
            "CURRENT"
            if parsed_valid_until and parsed_valid_until > current
            else "EXPIRED"
            if parsed_valid_until
            else "UNCLASSIFIED"
        )
        article.update(
            {
                "valid_until": payload["valid_until"],
                "next_refresh_at": payload["next_refresh_at"],
                "lifecycle_status": payload["lifecycle_status"],
                "category": payload["category"],
            }
        )
        payload["raw_payload_json"] = encode(article)
        columns = list(payload)
        updates = ", ".join(
            (
                f"{column}=CASE WHEN market_news.source_audit_status='QUARANTINED' "
                "AND excluded.source_audit_status='QUARANTINED' "
                f"THEN market_news.{column} ELSE excluded.{column} END"
                if column in {
                    "source_url",
                    "canonical_url",
                    "reliability",
                    "confidence",
                    "is_official",
                    "source_audit_status",
                    "source_invalid_reason",
                    "raw_payload_json",
                }
                else f"{column}=excluded.{column}"
            )
            for column in columns
            if column not in {"news_key", "created_at"}
        )
        with connect_market_db(self.settings) as conn:
            conn.execute(
                f"""
                INSERT INTO market_news ({", ".join(columns)}) VALUES ({", ".join("?" for _ in columns)})
                ON CONFLICT(news_key) DO UPDATE SET {updates}
                """,
                [payload[column] for column in columns],
            )
            if not source_admitted:
                _record_source_quarantine(
                    conn,
                    entity_table="market_news",
                    entity_key=str(payload["news_key"]),
                    invalid=(
                        source_validation
                        if not source_validation.accepted
                        else SourceUrlValidation(
                            accepted=False,
                            url=source_url,
                            domain=policy.domain,
                            reason_code=payload["source_invalid_reason"],
                        )
                    ),
                    previous_status="ACTIVE",
                    lineage={"canonical_url": payload["canonical_url"]},
                )
            conn.commit()
        return payload

    def stored(
        self,
        *,
        symbols: list[str] | None = None,
        days: int = 7,
        limit: int | None = None,
        current_only: bool = False,
        include_quarantined: bool = False,
    ) -> list[dict[str, Any]]:
        cutoff = (self.clock() - timedelta(days=max(days, 1))).replace(microsecond=0).isoformat()
        audit_filter = (
            ""
            if include_quarantined
            else "AND source_audit_status='ACTIVE'"
        )
        query = f"""
                SELECT * FROM market_news
                WHERE COALESCE(published_at, retrieved_at) >= ?
                  {audit_filter}
                ORDER BY COALESCE(published_at, retrieved_at) DESC,
                         news_key ASC
        """
        parameters: tuple[Any, ...] = (cutoff,)
        if limit is not None:
            query += " LIMIT ?"
            parameters = (cutoff, max(int(limit), 1))
        with connect_market_db(self.settings) as conn:
            rows = conn.execute(query, parameters).fetchall()
        items = [self._row(row) for row in rows]
        now = self.clock()
        for item in items:
            valid_until = parse_datetime(item.get("valid_until"))
            published_at = parse_datetime(item.get("published_at"))
            declared = str(item.get("lifecycle_status") or "").upper()
            item["lifecycle_status"] = (
                "CURRENT"
                if valid_until and valid_until > now
                else "EXPIRED"
                if valid_until
                else declared
                if declared in {"CURRENT", "EXPIRED"}
                else "CURRENT"
                if published_at
                and published_at
                + timedelta(hours=DEFAULT_CURRENT_WINDOW_HOURS)
                > now
                else "EXPIRED"
                if published_at
                else "UNCLASSIFIED"
            )
            item["historical"] = item["lifecycle_status"] == "EXPIRED"
        if current_only:
            items = [item for item in items if item["lifecycle_status"] == "CURRENT"]
        if symbols:
            wanted = {symbol.upper() for symbol in symbols}
            items = [item for item in items if wanted.intersection({symbol.upper() for symbol in item.get("symbols", [])})]
        return items

    def current(self, *, symbols: list[str] | None = None, days: int = 7, limit: int | None = None) -> list[dict[str, Any]]:
        return self.stored(symbols=symbols, days=days, limit=limit, current_only=True)

    def _row(self, row) -> dict[str, Any]:
        data = dict(row)
        persistence_updated_at = data.get("updated_at")
        data["symbols"] = decode(data.pop("symbols_json", None), [])
        data["topics"] = decode(data.pop("topics_json", None), [])
        data["raw_payload"] = decode(data.pop("raw_payload_json", None), None)
        if isinstance(data["raw_payload"], dict) and data["raw_payload"].get("content_status"):
            data["content_status"] = data["raw_payload"]["content_status"]
        if isinstance(data["raw_payload"], dict):
            for key in (
                "accepted", "article_id", "author", "canonical_url", "aggregator_url", "canonical_status",
                "redirect_chain", "summary_source_type", "summary_source_url", "summary_quality",
                "summary_is_generated", "summary_reliability", "source_text_available", "original_publisher",
                "source_classification", "is_official", "is_official_source", "is_primary_source", "entities",
                "matched_entities", "topic_classifications", "relevance_score", "relevance_reasons",
                "relevance_tier", "exclusion_reason", "duplicate_group_id", "duplicate_of", "syndication_group",
                "independent_source_count", "pipeline_version", "warnings", "content_status",
                "distribution_source", "distributor", "publisher", "validation", "lineage", "content",
                "headline", "content_availability", "provenance", "provider", "provider_name",
                "content_availability_status",
                "canonical_news_id", "acquisition_provider", "publisher_status",
                "original_publisher_status", "source_verification_status",
                "distributor_status", "lineage_status", "category_status",
                "topic_status", "lifecycle", "full_content", "first_seen_at",
                "last_seen_at", "analysis_usability", "warning_codes",
                "reason_codes", "raw_source_identity", "source_occurrences",
                "distribution_lineage",
            ):
                if data.get(key) in (None, "") and key in data["raw_payload"]:
                    data[key] = data["raw_payload"][key]
            if data["raw_payload"].get("updated_at") not in (None, ""):
                data["persistence_updated_at"] = persistence_updated_at
                data["updated_at"] = data["raw_payload"]["updated_at"]
        return data
