from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

from app.core.config import Settings
from app.infrastructure.persistence.database import connect_sqlite
from app.services.data_freshness_service import parse_datetime
from app.services.source_policy_service import SourcePolicyService


class NewsResearchPolicy:
    """Separates article acceptance, claim verification and cluster confirmation."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.source_policy = SourcePolicyService(settings.source_policy_path)

    def decide(
        self,
        *,
        run_id: str,
        article: dict[str, Any],
        claim_verified: bool,
        independent_domains: set[str],
        now: datetime | None = None,
        persist: bool = True,
    ) -> dict[str, Any]:
        now = _aware(now or datetime.now(UTC))
        url = str(article.get("canonical_url") or article.get("source_url") or "")
        domain = (urlsplit(url).hostname or "").lower().removeprefix("www.")
        reasons: list[str] = []
        if not url.startswith("https://"):
            reasons.append("CANONICAL_URL_REQUIRED")
        if self.source_policy.rule_for(url, article.get("publisher")) is None:
            reasons.append("SOURCE_NOT_ALLOWED")
        published = parse_datetime(article.get("published_at"))
        if published is None:
            reasons.append("PUBLISHED_AT_REQUIRED")
        elif _aware(published) > now + timedelta(
            seconds=self.settings.research_clock_skew_seconds
        ):
            reasons.append("PUBLISHED_AT_FUTURE")
        elif _aware(published) < now - timedelta(hours=24):
            reasons.append("ARTICLE_STALE")
        if float(article.get("mnq_relevance") or 0) <= 0:
            reasons.append("MNQ_RELEVANCE_REQUIRED")
        if not article.get("content_acquired"):
            reasons.append("CONTENT_NOT_ACQUIRED")
        article_status = "ACCEPTED" if not reasons else "REJECTED"
        if article_status == "ACCEPTED" and claim_verified:
            confirmation = (
                "CONFIRMED"
                if len({item.lower() for item in independent_domains if item}) >= 2
                else "SINGLE_SOURCE_REPORT"
            )
        else:
            confirmation = "UNVERIFIED"
        decision = {
            "candidate_id": "nrc-" + hashlib.sha256(
                f"{run_id}|{url}".encode("utf-8")
            ).hexdigest()[:24],
            "run_id": run_id,
            "canonical_url": url or None,
            "source_domain": domain or None,
            "article_status": article_status,
            "claim_verification_status": (
                "VERIFIED" if claim_verified else "UNVERIFIED"
            ),
            "confirmation_status": confirmation,
            "rejection_reason": reasons[0] if reasons else None,
            "rejection_reasons": reasons,
            "created_at": now.replace(microsecond=0).isoformat(),
        }
        if persist:
            self._persist(decision)
        return decision

    def _persist(self, decision: dict[str, Any]) -> None:
        with connect_sqlite(self.settings.database_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO news_research_candidate_decisions(
                  candidate_id,run_id,canonical_url,source_domain,article_status,
                  claim_verification_status,confirmation_status,rejection_reason,
                  decision_json,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    decision["candidate_id"],
                    decision["run_id"],
                    decision["canonical_url"],
                    decision["source_domain"],
                    decision["article_status"],
                    decision["claim_verification_status"],
                    decision["confirmation_status"],
                    decision["rejection_reason"],
                    json.dumps(
                        decision,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    decision["created_at"],
                ),
            )
            conn.commit()


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
