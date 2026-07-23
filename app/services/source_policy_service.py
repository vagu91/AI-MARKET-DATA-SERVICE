from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


DEFAULT_POLICY_PATH = Path(__file__).resolve().parents[2] / "config" / "source_policy.json"


@dataclass(frozen=True)
class SourceDecision:
    accepted: bool
    domain: str
    tier: int
    classification: str
    reliability: float
    policy_version: str
    reasons: tuple[str, ...] = ()


class SourcePolicyService:
    """Versioned, executable source policy; prompts receive a projection of this policy."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = Path(path or DEFAULT_POLICY_PATH)
        self.policy = self._load(self.path)
        self.policy_version = str(self.policy["policy_version"])
        self._rules = {str(item["domain"]).lower(): item for item in self.policy["rules"]}

    @staticmethod
    def _load(path: Path) -> dict[str, Any]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or not payload.get("policy_version"):
            raise ValueError("source policy requires policy_version")
        rules = payload.get("rules")
        if not isinstance(rules, list) or not rules:
            raise ValueError("source policy requires non-empty rules")
        required = {
            "domain",
            "publisher",
            "tier",
            "data_types",
            "official_actual",
            "consensus",
            "news",
            "base_reliability",
            "multiple_confirmation",
            "aggregator",
        }
        for rule in rules:
            if not isinstance(rule, dict) or required.difference(rule):
                raise ValueError(f"invalid source policy rule: {rule!r}")
            if int(rule["tier"]) not in range(1, 6):
                raise ValueError("source tier must be between 1 and 5")
        return payload

    def domain(self, url: str | None) -> str:
        host = urlparse(str(url or "")).hostname or ""
        return host.lower().removeprefix("www.")

    def rule_for(self, url: str | None, publisher: str | None = None) -> dict[str, Any] | None:
        domain = self.domain(url)
        candidates = [
            rule for key, rule in self._rules.items() if _domain_matches(domain, key)
        ]
        if candidates:
            return min(candidates, key=lambda item: int(item["tier"]))
        for issuer, raw_config in (self.policy.get("issuer_official_sources") or {}).items():
            config = raw_config if isinstance(raw_config, dict) else {}
            channel_domains = {
                "NEWSROOM": config.get("newsroom_domains") or [],
                "INVESTOR_RELATIONS": config.get("investor_relations_domains") or [],
                "CORPORATE": config.get("domains") or [],
            }
            channel = next(
                (
                    name
                    for name, domains in channel_domains.items()
                    if any(_domain_matches(domain, str(item)) for item in domains)
                ),
                None,
            )
            if channel and str(url or "").lower().startswith("https://"):
                return {
                    "domain": domain,
                    "publisher": issuer,
                    "issuer": issuer,
                    "issuer_official": True,
                    "issuer_channel": channel,
                    "tier": 1,
                    "data_types": [
                        "actual",
                        "event",
                        "earnings",
                        "guidance",
                        "issuer_announcement",
                        "news",
                    ],
                    "official_actual": True,
                    "consensus": False,
                    "news": True,
                    "base_reliability": 0.97,
                    "multiple_confirmation": False,
                    "aggregator": False,
                }
        publisher_text = str(publisher or "").lower()
        issuer_match = next(
            (
                (issuer, str(allowed_domain).lower())
                for issuer, domains in (self.policy.get("issuer_domain_allowlist") or {}).items()
                for allowed_domain in domains
                if str(issuer).lower() in publisher_text
                and _domain_matches(domain, str(allowed_domain))
            ),
            None,
        )
        if (
            issuer_match
            and str(url or "").lower().startswith("https://")
            and ("investor relations" in publisher_text or publisher_text.endswith(" ir"))
        ):
            return {
                "domain": domain,
                "publisher": publisher,
                "issuer": issuer_match[0],
                "issuer_official": True,
                "issuer_channel": "LEGACY_ALLOWLIST",
                "tier": 1,
                "data_types": [
                    "actual",
                    "event",
                    "earnings",
                    "guidance",
                    "issuer_announcement",
                    "news",
                ],
                "official_actual": True,
                "consensus": False,
                "news": True,
                "base_reliability": 0.97,
                "multiple_confirmation": False,
                "aggregator": False,
            }
        return None

    def validate(
        self,
        candidate: dict[str, Any],
        *,
        field_semantics: str,
        numerical: bool = False,
    ) -> SourceDecision:
        url = candidate.get("canonical_url") or candidate.get("source_url")
        publisher = candidate.get("publisher") or candidate.get("source")
        domain = self.domain(url)
        forbidden = {str(item).lower() for item in self.policy.get("forbidden_domains") or []}
        if any(_domain_matches(domain, item) for item in forbidden):
            return self._decision(False, domain, 5, "FORBIDDEN", 0.0, "forbidden_domain")
        rule = self.rule_for(url, publisher)
        if rule is None:
            if field_semantics in {"sentiment", "exploratory_context"}:
                return self._decision(True, domain, 5, "SECONDARY_CONTEXT", 0.35)
            return self._decision(False, domain, 5, "UNKNOWN", 0.0, "unknown_source")
        semantics = field_semantics.lower()
        reasons: list[str] = []
        if semantics in {"actual", "official_actual"} and not bool(rule["official_actual"]):
            reasons.append("actual_requires_official_source")
        if semantics in {"actual", "official_actual"} and int(rule["tier"]) != 1:
            reasons.append("actual_requires_tier_1")
        rule_data_types = {str(item).lower() for item in rule["data_types"]}
        authorized_semantics = self.authorized_data_types(semantics)
        if not authorized_semantics.intersection(rule_data_types):
            reasons.append("field_semantics_not_allowed_for_source")
        semantic_policy = self.semantic_policy(semantics)
        allowed_tiers = {int(item) for item in semantic_policy.get("allowed_tiers") or range(1, 6)}
        if int(rule["tier"]) not in allowed_tiers:
            reasons.append("source_tier_not_allowed_for_semantics")
        required_confirmations = self.required_confirmations(semantics)
        independent = (
            candidate.get("verified_independent_domains") or []
            if candidate.get("_service_evidence_verified") is True
            else []
        )
        confirmation_count = len({str(item).lower() for item in independent if item})
        if required_confirmations > 1 and confirmation_count < required_confirmations:
            reasons.append("required_confirmations_not_met")
        if semantics in {"consensus", "forecast"} and not bool(rule["consensus"]):
            reasons.append("source_not_authorized_for_consensus")
        if semantics in {"news", "current_news"} and not bool(rule["news"]):
            reasons.append("source_not_authorized_for_news")
        if numerical:
            for field in ("metric_id", "period", "unit", "source_url", "evidence_text"):
                if candidate.get(field) in (None, ""):
                    reasons.append(f"missing_{field}")
        classification = (
            "OFFICIAL"
            if int(rule["tier"]) == 1
            else "PRIMARY_MARKET"
            if int(rule["tier"]) == 2
            else "FINANCIAL_MEDIA"
            if int(rule["tier"]) == 3
            else "CALENDAR_CONSENSUS"
            if int(rule["tier"]) == 4
            else "SECONDARY_CONTEXT"
        )
        return self._decision(
            not reasons,
            domain,
            int(rule["tier"]),
            classification,
            float(rule["base_reliability"]),
            *reasons,
        )

    def prompt_projection(self) -> dict[str, Any]:
        return {
            "policy_version": self.policy_version,
            "rules": [
                {
                    key: item[key]
                    for key in (
                        "domain",
                        "publisher",
                        "tier",
                        "data_types",
                        "official_actual",
                        "consensus",
                        "news",
                        "multiple_confirmation",
                        "aggregator",
                    )
                }
                for item in self.policy["rules"]
            ],
            "forbidden_domains": self.policy.get("forbidden_domains") or [],
            "issuer_official_domains": self.policy.get("issuer_official_domains") or [],
            "issuer_domain_allowlist": self.policy.get("issuer_domain_allowlist") or {},
            "issuer_official_sources": self.policy.get("issuer_official_sources") or {},
            "semantic_policies": self.policy.get("semantic_policies") or {},
        }

    def semantic_policy(self, field_semantics: str) -> dict[str, Any]:
        return dict((self.policy.get("semantic_policies") or {}).get(field_semantics.lower()) or {})

    def required_confirmations(self, field_semantics: str) -> int:
        return max(
            int(self.semantic_policy(field_semantics).get("required_confirmations") or 1),
            1,
        )

    def authorized_data_types(self, field_semantics: str) -> set[str]:
        semantics = field_semantics.lower()
        if semantics == "transcript_url":
            return {"speech"}
        if semantics == "forecast":
            return {"consensus"}
        if semantics == "official_actual":
            return {"actual"}
        if semantics in {"scheduled_event", "official_calendar_event"}:
            return {"event"}
        if semantics == "earnings_schedule":
            return {"earnings", "event"}
        if semantics == "issuer_announcement":
            return {"issuer_announcement", "earnings", "news"}
        if semantics in {"news", "current_news"}:
            return {"news"}
        if semantics == "current_market_context":
            return {"market", "positioning"}
        if semantics in {"outcome", "exploratory_context"}:
            return {
                "actual",
                "event",
                "market",
                "positioning",
                "speech",
                "earnings",
                "news",
            }
        return {semantics}

    def rule_supports(self, rule: dict[str, Any], field_semantics: str) -> bool:
        rule_types = {str(item).lower() for item in rule.get("data_types") or []}
        return bool(self.authorized_data_types(field_semantics).intersection(rule_types))

    def enrich_lineage(self, candidate: dict[str, Any], *, field_semantics: str) -> dict[str, Any]:
        decision = self.validate(candidate, field_semantics=field_semantics, numerical=False)
        return {
            **candidate,
            "source_domain": decision.domain,
            "source_tier": decision.tier,
            "source_classification": decision.classification,
            "policy_version": decision.policy_version,
            "validation": {
                "status": "accepted" if decision.accepted else "rejected",
                "reasons": list(decision.reasons),
            },
        }

    def _decision(
        self,
        accepted: bool,
        domain: str,
        tier: int,
        classification: str,
        reliability: float,
        *reasons: str,
    ) -> SourceDecision:
        return SourceDecision(
            accepted=accepted,
            domain=domain,
            tier=tier,
            classification=classification,
            reliability=reliability,
            policy_version=self.policy_version,
            reasons=tuple(reasons),
        )


def _domain_matches(host: str, allowed_domain: str) -> bool:
    host = str(host or "").lower().rstrip(".")
    allowed = str(allowed_domain or "").lower().strip().rstrip(".")
    return bool(host and allowed and (host == allowed or host.endswith(f".{allowed}")))
