from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
import contextvars
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time
from typing import Any, Iterator
import uuid

import httpx

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.config import Settings
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)
from app.providers.news_provider import NewsProvider, _aggregate_provider_accounting
from app.services.market_news_repository import MarketNewsRepository


_LOGICAL_CALL_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "news_pipeline_logical_call_id",
    default=None,
)
_SECRET_PATTERNS = (
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    re.compile(r"(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})"),
    re.compile(r"\bsk-[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{24,}"),
    re.compile(
        r"(?i)(?:apikey|api_key|access_token|refresh_token|"
        r"client_secret|password)=[^&\s]{6,}"
    ),
)


class ControlledNetworkAudit:
    def __init__(self, baseline: dict[str, Any]) -> None:
        self.endpoint_provider = {
            _canonical_url(item["endpoint"]): item["provider"]
            for item in baseline["providers"]
        }
        self.yahoo_redirect_hosts = set(
            baseline["network"]["metadata_redirects"][
                "yahoo_allowed_hosts"
            ]
        )
        self.article_provider: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.active = False

    def register_provider_batches(
        self,
        batches: list[dict[str, Any]],
    ) -> None:
        for batch in batches:
            provider = str(batch.get("provider") or "UNKNOWN")
            for article in batch.get("_articles") or []:
                url = article.get("source_url") or article.get("url")
                record_id = article.get("raw_record_id")
                if not url or not record_id:
                    continue
                canonical = _canonical_url(str(url))
                existing = self.article_provider.get(canonical)
                if existing is not None:
                    if existing["provider"] != provider:
                        raise RuntimeError(
                            "ARTICLE_URL_HAS_AMBIGUOUS_PROVIDER_LINEAGE"
                        )
                    existing["record_ids"] = sorted(
                        {
                            *existing["record_ids"],
                            str(record_id),
                        }
                    )
                    continue
                self.article_provider[canonical] = {
                    "provider": provider,
                    "record_ids": [str(record_id)],
                    "origin": "CURRENT_ACQUISITION_RAW_RECORD",
                }

    def register_metadata_redirect(
        self,
        *,
        provider: str,
        record_id: str,
        original_url: str,
        redirect_url: str,
    ) -> None:
        original = self.article_provider.get(_canonical_url(original_url))
        original_parsed = httpx.URL(original_url)
        redirect_parsed = httpx.URL(redirect_url)
        lineage_valid = (
            original is not None
            and provider == "Yahoo Finance RSS"
            and str(record_id) in original["record_ids"]
        )
        host_policy_valid = (
            original_parsed.scheme == "https"
            and redirect_parsed.scheme == "https"
            and original_parsed.host in self.yahoo_redirect_hosts
            and redirect_parsed.host in self.yahoo_redirect_hosts
        )
        if not lineage_valid or not host_policy_valid:
            raise RuntimeError("REDIRECT_WITHOUT_CURRENT_ACQUISITION_LINEAGE")
        self.article_provider[_canonical_url(redirect_url)] = {
            "provider": provider,
            "record_ids": [record_id],
            "origin": "ALLOWLISTED_YAHOO_REDIRECT",
        }

    def decision(self, request: httpx.Request) -> dict[str, Any]:
        method = request.method.upper()
        exact = _canonical_url(str(request.url))
        parsed = httpx.URL(exact)
        base = _canonical_url(str(parsed.copy_with(query=None)))
        decision = {
            "allowed": False,
            "reason_code": "ENDPOINT_NOT_ALLOWLISTED",
            "method": method,
            "endpoint_logical": f"{parsed.host}{parsed.path}",
            "provider": "UNKNOWN",
            "call_kind": "UNKNOWN",
            "article_lineage_ids": [],
        }
        if method != "GET":
            decision["reason_code"] = "METHOD_NOT_ALLOWLISTED"
            return decision
        if not self.active:
            decision["reason_code"] = "CALL_OUTSIDE_ACQUISITION"
            return decision
        if base in self.endpoint_provider:
            provider = self.endpoint_provider[base]
            return {
                **decision,
                "allowed": True,
                "reason_code": None,
                "endpoint_logical": provider.upper().replace(" ", "_"),
                "provider": provider,
                "call_kind": "MAIN_PROVIDER",
            }
        article = self.article_provider.get(exact)
        if article:
            return {
                **decision,
                "allowed": True,
                "reason_code": None,
                "endpoint_logical": (
                    f"ARTICLE_METADATA:{parsed.host}{parsed.path}"
                ),
                "provider": article["provider"],
                "call_kind": "METADATA_ENRICHMENT",
                "article_lineage_ids": article["record_ids"],
                "allowlist_origin": article["origin"],
            }
        return decision

    def emit(self, event: str, **payload: Any) -> None:
        self.events.append(
            {
                "event": event,
                "timestamp_utc": datetime.now(UTC).isoformat(),
                **payload,
            }
        )

    @contextmanager
    def instrument(self) -> Iterator[None]:
        original_send = httpx.AsyncClient.send
        original_transport = httpx.AsyncHTTPTransport.handle_async_request
        audit = self

        async def audited_send(
            client: httpx.AsyncClient,
            request: httpx.Request,
            *args: Any,
            **kwargs: Any,
        ) -> httpx.Response:
            logical_id = str(uuid.uuid4())
            decision = audit.decision(request)
            if not decision["allowed"]:
                audit.emit(
                    "network_blocked",
                    logical_call_id=logical_id,
                    **decision,
                )
                raise RuntimeError(str(decision["reason_code"]))
            token = _LOGICAL_CALL_ID.set(logical_id)
            started = time.perf_counter()
            response: httpx.Response | None = None
            error: Exception | None = None
            try:
                response = await original_send(
                    client,
                    request,
                    *args,
                    **kwargs,
                )
                return response
            except Exception as exc:
                error = exc
                raise
            finally:
                _LOGICAL_CALL_ID.reset(token)
                audit.emit(
                    "network_call",
                    logical_call_id=logical_id,
                    duration_ms=round(
                        (time.perf_counter() - started) * 1000,
                        3,
                    ),
                    http_status=(
                        response.status_code if response is not None else None
                    ),
                    error_type=(
                        type(error).__name__ if error is not None else None
                    ),
                    error_reason=(
                        _redact(str(error))[:240]
                        if error is not None
                        else None
                    ),
                    **decision,
                )

        async def audited_transport(
            transport: httpx.AsyncHTTPTransport,
            request: httpx.Request,
        ) -> httpx.Response:
            logical_id = _LOGICAL_CALL_ID.get()
            decision = audit.decision(request)
            if not logical_id or not decision["allowed"]:
                audit.emit(
                    "transport_blocked",
                    logical_call_id=logical_id,
                    transport="HTTPX_ASYNC_TRANSPORT",
                    **decision,
                )
                raise RuntimeError(
                    str(
                        decision.get("reason_code")
                        or "UNOBSERVED_ASYNC_TRANSPORT"
                    )
                )
            audit.emit(
                "transport_call",
                logical_call_id=logical_id,
                transport="HTTPX_ASYNC_TRANSPORT",
                **decision,
            )
            return await original_transport(transport, request)

        httpx.AsyncClient.send = audited_send
        httpx.AsyncHTTPTransport.handle_async_request = audited_transport
        try:
            yield
        finally:
            httpx.AsyncClient.send = original_send
            httpx.AsyncHTTPTransport.handle_async_request = original_transport

    def verify(self) -> dict[str, Any]:
        calls = [
            item for item in self.events if item["event"] == "network_call"
        ]
        transports = [
            item for item in self.events if item["event"] == "transport_call"
        ]
        blocked = [
            item
            for item in self.events
            if item["event"] in {"network_blocked", "transport_blocked"}
        ]
        call_ids = {item["logical_call_id"] for item in calls}
        transport_ids = {item["logical_call_id"] for item in transports}
        return {
            "pass": not blocked and call_ids == transport_ids,
            "network_call_count": len(calls),
            "transport_call_count": len(transports),
            "blocked_call_count": len(blocked),
            "unobserved_call_ids": sorted(call_ids - transport_ids),
            "transport_without_send_ids": sorted(transport_ids - call_ids),
            "events": self.events,
        }


async def controlled_live(
    *,
    repo_root: Path,
    output_root: Path,
    baseline_path: Path,
) -> dict[str, Any]:
    baseline_bytes = baseline_path.read_bytes()
    baseline = json.loads(baseline_bytes)
    baseline_sha = hashlib.sha256(baseline_bytes).hexdigest().upper()
    operational = repo_root / "data" / "market_data_service.sqlite"
    before = _database_state(operational)
    if not before or not operational.is_file():
        raise RuntimeError("OPERATIONAL_DATABASE_COMPONENTS_NOT_FOUND")

    sandbox_root = output_root / "sandbox"
    sandbox_root.mkdir(parents=True, exist_ok=True)
    sandbox = sandbox_root / "market_data_service.sqlite"
    sandbox_preflight = _create_consistent_sandbox(
        operational,
        sandbox,
        expected_source_state=before,
    )

    guard = (
        repo_root
        / "data"
        / f"news-pipeline-controlled-live-{baseline_sha}.guard"
    )
    settings = Settings(
        _env_file=None,
        environment="controlled_validation",
        database_path=sandbox,
        source_policy_path=repo_root / "config" / "source_policy.json",
        enable_scheduler=False,
        event_calendar_catchup_enabled=False,
        research_scheduler_enabled=False,
        lifecycle_due_scanner_enabled=False,
        ai_worker_enabled=False,
        enable_ai_researcher=False,
        research_agents_enabled=False,
        ai_research_web_access_enabled=False,
        enable_openai_fallback=False,
        enable_browser_scraping=False,
        tradier_enabled=False,
        tradier_trading_enabled=False,
        bls_rss_url="https://www.bls.gov/feed/bls_latest.rss",
        bea_rss_url="https://apps.bea.gov/rss/rss.xml",
    )
    audit = ControlledNetworkAudit(baseline)
    repository = MarketNewsRepository(settings)
    provider = NewsProvider(
        ProviderCacheRepository(sandbox_root / "provider-cache.sqlite"),
        settings,
        market_news_repository=repository,
        network_observer=audit,
    )

    try:
        file_descriptor = os.open(
            guard,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        )
    except FileExistsError as exc:
        raise RuntimeError("CONTROLLED_LIVE_SINGLE_USE_GUARD_CONSUMED") from exc
    with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
        handle.write(datetime.now(UTC).isoformat())

    print("LIVE_PROVIDER_ACQUISITION_START", flush=True)
    audit.active = True
    try:
        with audit.instrument():
            result = await provider.fetch_for_symbols(
                ["QQQ", "NVDA", "AAPL", "MSFT"],
                limit=250,
                recency_days=30,
            )
    finally:
        audit.active = False
        print("LIVE_PROVIDER_ACQUISITION_END", flush=True)

    payload = result.model_dump(mode="json")
    network = audit.verify()
    after = _database_state(operational)
    database_invariants = _database_invariants(before, after)
    acceptance = _evaluate_controlled_live_acceptance(
        baseline=baseline,
        payload=payload,
        network=network,
        database_invariants=database_invariants,
    )
    report = {
        "mode": "CONTROLLED_LIVE",
        "result": acceptance["status"],
        "baseline": baseline["contract_id"],
        "baseline_sha256": baseline_sha,
        "guard_path": str(guard),
        "guard_consumed": True,
        "sandbox_preflight": sandbox_preflight,
        "operational_database_before": before,
        "operational_database_after": after,
        "operational_database_unchanged": database_invariants["pass"],
        "database_invariants": database_invariants,
        "provider_accounting_valid": acceptance["accounting_valid"],
        "coverage_has_no_blocking_failure": acceptance[
            "coverage_has_no_blocking_failure"
        ],
        "provider_availability": acceptance["provider_availability"],
        "acceptance": acceptance,
        "network": network,
        "response": payload,
        "pass": acceptance["pass"],
    }
    _write_json(output_root / "controlled-live-validation.json", report)
    findings = _secret_scan(output_root)
    _write_json(
        output_root / "secret-scan.json",
        {"pass": not findings, "findings": findings},
    )
    if findings:
        raise RuntimeError("CONTROLLED_LIVE_SECRET_SCAN_FAILED")
    if not report["pass"]:
        raise RuntimeError("CONTROLLED_LIVE_VALIDATION_FAILED")
    return report


def _database_state(main: Path) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for role, path in (
        ("MAIN", main),
        ("WAL", Path(str(main) + "-wal")),
        ("SHM", Path(str(main) + "-shm")),
    ):
        if not path.is_file():
            continue
        stat = path.stat()
        output.append(
            {
                "role": role,
                "path": str(path.resolve()),
                "bytes": stat.st_size,
                "last_write_ns": stat.st_mtime_ns,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest().upper(),
            }
        )
    return output


def _database_invariants(
    before: list[dict[str, Any]],
    after: list[dict[str, Any]],
) -> dict[str, Any]:
    before_by_role = {
        str(item["role"]): item for item in before if item.get("role")
    }
    after_by_role = {
        str(item["role"]): item for item in after if item.get("role")
    }
    roles_unchanged = (
        bool(before_by_role)
        and set(before_by_role) == set(after_by_role)
        and "MAIN" in before_by_role
    )
    changed_content_roles = sorted(
        role
        for role in set(before_by_role) | set(after_by_role)
        if role not in before_by_role
        or role not in after_by_role
        or before_by_role[role].get("bytes")
        != after_by_role[role].get("bytes")
        or before_by_role[role].get("sha256")
        != after_by_role[role].get("sha256")
    )
    changed_metadata_roles = sorted(
        role
        for role in set(before_by_role) | set(after_by_role)
        if role not in before_by_role
        or role not in after_by_role
        or before_by_role[role].get("last_write_ns")
        != after_by_role[role].get("last_write_ns")
    )
    content_unchanged = roles_unchanged and not changed_content_roles
    metadata_unchanged = roles_unchanged and not changed_metadata_roles
    semantic_database_unchanged = content_unchanged
    metadata_only_shm_timestamp_change = (
        content_unchanged
        and changed_metadata_roles == ["SHM"]
    )
    if content_unchanged and metadata_unchanged:
        classification = "UNCHANGED"
    elif metadata_only_shm_timestamp_change:
        classification = "METADATA_ONLY_SHM_TIMESTAMP_CHANGE"
    elif content_unchanged:
        classification = "UNEXPECTED_METADATA_CHANGE"
    else:
        classification = "CONTENT_CHANGED"
    return {
        "classification": classification,
        "content_unchanged": content_unchanged,
        "metadata_unchanged": metadata_unchanged,
        "semantic_database_unchanged": semantic_database_unchanged,
        "metadata_only_shm_timestamp_change": (
            metadata_only_shm_timestamp_change
        ),
        "changed_content_roles": changed_content_roles,
        "changed_metadata_roles": changed_metadata_roles,
        "pass": (
            semantic_database_unchanged
            and (
                metadata_unchanged
                or metadata_only_shm_timestamp_change
            )
        ),
    }


def _create_consistent_sandbox(
    source: Path,
    target: Path,
    *,
    expected_source_state: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    target_paths = [
        target,
        Path(str(target) + "-wal"),
        Path(str(target) + "-shm"),
    ]
    if any(path.exists() for path in target_paths):
        raise RuntimeError("SANDBOX_DATABASE_ALREADY_EXISTS")
    target.parent.mkdir(parents=True, exist_ok=True)
    expected = expected_source_state or _database_state(source)
    expected_by_role = {
        str(item["role"]): item for item in expected if item.get("role")
    }
    if "MAIN" not in expected_by_role:
        raise RuntimeError("OPERATIONAL_DATABASE_MAIN_NOT_FOUND")
    for role, suffix in (("MAIN", ""), ("WAL", "-wal"), ("SHM", "-shm")):
        if role not in expected_by_role:
            continue
        source_component = Path(str(source) + suffix)
        target_component = Path(str(target) + suffix)
        shutil.copy2(source_component, target_component)

    source_after = _database_state(source)
    source_invariants = _database_invariants(expected, source_after)
    if source_invariants["classification"] != "UNCHANGED":
        raise RuntimeError(
            "OPERATIONAL_DATABASE_CHANGED_DURING_SANDBOX_COPY "
            f"classification={source_invariants['classification']}"
        )
    sandbox_state = _database_state(target)
    sandbox_invariants = _database_invariants(expected, sandbox_state)
    if not sandbox_invariants["content_unchanged"]:
        raise RuntimeError("SANDBOX_DATABASE_BUNDLE_COPY_MISMATCH")
    return {
        "method": "FILESYSTEM_BUNDLE_COPY_WITHOUT_SQLITE_SOURCE_OPEN",
        "operational_sqlite_opened": False,
        "source_invariants": source_invariants,
        "sandbox_content_matches_source": True,
    }


def _evaluate_controlled_live_acceptance(
    *,
    baseline: dict[str, Any],
    payload: dict[str, Any],
    network: dict[str, Any],
    database_invariants: dict[str, Any],
) -> dict[str, Any]:
    data = payload.get("data") or {}
    quality = data.get("data_quality") or {}
    accounts = list(data.get("provider_accounting") or [])
    account_by_provider = {
        str(account.get("provider")): account
        for account in accounts
        if account.get("provider")
    }
    provider_policies = {
        str(item["provider"]): item
        for item in baseline.get("providers") or []
    }
    availability = baseline.get("availability") or {}
    optional_providers = set(availability.get("optional_providers") or [])
    required_groups = list(
        availability.get("required_provider_groups") or []
    )

    hard_failures: list[dict[str, Any]] = []
    degraded_reasons: list[dict[str, Any]] = []
    unknown_providers = sorted(
        set(account_by_provider) - set(provider_policies)
    )
    if unknown_providers:
        hard_failures.append(
            {
                "reason_code": "PROVIDER_WITHOUT_BASELINE_POLICY",
                "providers": unknown_providers,
            }
        )
    if not network.get("pass"):
        hard_failures.append(
            {"reason_code": "NETWORK_INSTRUMENTATION_FAILED"}
        )
    accounting_valid = bool(accounts) and all(
        account.get("accounting_valid") is True for account in accounts
    )
    aggregate_accounting = _aggregate_provider_accounting(accounts)
    accounting_valid = accounting_valid and aggregate_accounting["valid"]
    if not accounting_valid:
        hard_failures.append(
            {"reason_code": "PROVIDER_ACCOUNTING_INVALID"}
        )

    falsely_complete: list[str] = []
    unreasoned_unavailable: list[str] = []
    non_temporary_optional_failures: list[str] = []
    for provider, account in account_by_provider.items():
        status = str(account.get("status") or "UNKNOWN")
        coverage_status = str(
            account.get("coverage_status") or "UNKNOWN"
        )
        error_text = " ".join(
            [
                str(account.get("reason_code") or ""),
                *[str(item) for item in account.get("errors") or []],
            ]
        ).upper()
        if status == "COMPLETE" and (
            coverage_status != "COMPLETE"
            or account.get("pagination_complete") is False
            or bool(account.get("errors"))
        ):
            falsely_complete.append(provider)
        if status in {"FAILED", "TEMPORARILY_UNAVAILABLE"}:
            if not error_text.strip():
                unreasoned_unavailable.append(provider)
            temporary = (
                status == "TEMPORARILY_UNAVAILABLE"
                or account.get("temporary") is True
                or "TIMEOUT" in error_text
                or "TEMPORAR" in error_text
            )
            if provider in optional_providers and not temporary:
                non_temporary_optional_failures.append(provider)
        if status != "COMPLETE" or coverage_status != "COMPLETE":
            degraded_reasons.append(
                {
                    "reason_code": "PROVIDER_COVERAGE_DEGRADED",
                    "provider": provider,
                    "status": status,
                    "coverage_status": coverage_status,
                }
            )
        if account.get("metadata_enrichment_status") == "PARTIAL":
            degraded_reasons.append(
                {
                    "reason_code": "OPTIONAL_METADATA_ENRICHMENT_PARTIAL",
                    "provider": provider,
                }
            )
    if falsely_complete:
        hard_failures.append(
            {
                "reason_code": "FAILED_OR_PARTIAL_SOURCE_DECLARED_COMPLETE",
                "providers": sorted(falsely_complete),
            }
        )
    if unreasoned_unavailable:
        hard_failures.append(
            {
                "reason_code": "UNAVAILABLE_PROVIDER_WITHOUT_REASON",
                "providers": sorted(unreasoned_unavailable),
            }
        )
    if non_temporary_optional_failures:
        hard_failures.append(
            {
                "reason_code": "OPTIONAL_PROVIDER_NON_TEMPORARY_FAILURE",
                "providers": sorted(non_temporary_optional_failures),
            }
        )

    group_results: list[dict[str, Any]] = []
    for group in required_groups:
        members = set(group.get("providers") or [])
        usable = sorted(
            provider
            for provider in members
            if provider in account_by_provider
            and account_by_provider[provider].get("accounting_valid") is True
            and account_by_provider[provider].get("status")
            in {"COMPLETE", "PARTIAL"}
            and account_by_provider[provider].get("coverage_status")
            in {"COMPLETE", "PARTIAL"}
        )
        persisted_count = sum(
            int(
                account_by_provider[provider].get("persisted_count")
                or len(
                    account_by_provider[provider].get(
                        "persisted_record_ids"
                    )
                    or []
                )
            )
            for provider in usable
        )
        group_pass = (
            len(usable)
            >= int(group.get("minimum_usable_providers") or 1)
            and persisted_count
            >= int(group.get("minimum_persisted_records") or 1)
        )
        group_result = {
            "group": group.get("group"),
            "members": sorted(members),
            "usable_providers": usable,
            "persisted_count": persisted_count,
            "pass": group_pass,
        }
        group_results.append(group_result)
        if not group_pass:
            hard_failures.append(
                {
                    "reason_code": "REQUIRED_PROVIDER_GROUP_UNAVAILABLE",
                    "group": group.get("group"),
                }
            )

    response_articles = list(data.get("articles") or [])
    response_ids = {
        str(article.get("raw_record_id"))
        for article in response_articles
        if article.get("raw_record_id")
    }
    response_missing_identity_count = sum(
        not article.get("raw_record_id")
        for article in response_articles
    )
    persisted_ids = {
        str(record_id)
        for account in accounts
        for record_id in account.get("persisted_record_ids") or []
    }
    response_without_persisted_identity = sorted(
        response_ids - persisted_ids
    )
    if response_missing_identity_count:
        hard_failures.append(
            {
                "reason_code": "RESPONSE_ARTICLE_WITHOUT_RAW_IDENTITY",
                "count": response_missing_identity_count,
            }
        )
    if response_without_persisted_identity:
        hard_failures.append(
            {
                "reason_code": "RESPONSE_ARTICLE_WITHOUT_PERSISTED_IDENTITY",
                "record_ids": response_without_persisted_identity,
            }
        )
    if (
        not quality.get("final_data_available")
        or not response_articles
    ):
        hard_failures.append(
            {"reason_code": "NO_USABLE_NEWS_PAYLOAD"}
        )
    if not database_invariants.get("pass"):
        hard_failures.append(
            {
                "reason_code": "OPERATIONAL_DATABASE_INVARIANT_FAILED",
                "classification": database_invariants.get(
                    "classification"
                ),
            }
        )
    elif database_invariants.get(
        "metadata_only_shm_timestamp_change"
    ):
        degraded_reasons.append(
            {
                "reason_code": "SHM_TIMESTAMP_METADATA_ONLY_CHANGE",
            }
        )

    if hard_failures:
        status = "FAIL"
    elif degraded_reasons:
        status = "PASS_DEGRADED"
    else:
        status = "PASS"
    return {
        "status": status,
        "pass": status in {"PASS", "PASS_DEGRADED"},
        "usable": status in {"PASS", "PASS_DEGRADED"},
        "accounting_valid": accounting_valid,
        "aggregate_accounting": aggregate_accounting,
        "coverage_has_no_blocking_failure": not any(
            item["reason_code"]
            in {
                "REQUIRED_PROVIDER_GROUP_UNAVAILABLE",
                "FAILED_OR_PARTIAL_SOURCE_DECLARED_COMPLETE",
                "UNAVAILABLE_PROVIDER_WITHOUT_REASON",
                "OPTIONAL_PROVIDER_NON_TEMPORARY_FAILURE",
            }
            for item in hard_failures
        ),
        "provider_availability": {
            "required_groups": group_results,
            "optional_providers": sorted(optional_providers),
        },
        "response_article_count": len(response_articles),
        "response_without_persisted_identity_ids": (
            response_without_persisted_identity
        ),
        "hard_failures": hard_failures,
        "degraded_reasons": degraded_reasons,
    }


def _canonical_url(value: str) -> str:
    return str(httpx.URL(value).copy_with(fragment=None))


def _redact(value: str) -> str:
    value = re.sub(
        r"(?i)(apikey|api_key|access_token|token|key)=([^&\s]+)",
        r"\1=REDACTED",
        value,
    )
    return re.sub(
        r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+",
        "Bearer REDACTED",
        value,
    )


def _secret_scan(root: Path) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for pattern in _SECRET_PATTERNS:
            if pattern.search(text):
                findings.append(
                    {
                        "file": str(path),
                        "pattern": pattern.pattern,
                    }
                )
    return findings


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Permanent single-use controlled-live news validation."
    )
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    args = parser.parse_args()
    report = asyncio.run(
        controlled_live(
            repo_root=args.repo_root.resolve(),
            output_root=args.output_root.resolve(),
            baseline_path=args.baseline.resolve(),
        )
    )
    print(
        f"CONTROLLED_LIVE_NEWS_PIPELINE_{report['result']} "
        f"network_calls={report['network']['network_call_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
