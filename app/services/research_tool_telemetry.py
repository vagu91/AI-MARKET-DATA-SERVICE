from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, deque
from datetime import UTC, datetime
from typing import Any

from app.core.config import Settings
from app.services.codex_runtime_contract import sanitize_diagnostic


TERMINAL_LIFECYCLES = {"completed", "failed"}
COUNTED_SOURCE_ACTIONS = {"open_source", "fetch", "verify_source"}
_URL_RE = re.compile(r"^https://[^\s]+$", re.IGNORECASE)


def normalize_codex_event(
    event: dict[str, Any],
    *,
    step_name: str,
) -> list[dict[str, Any]]:
    raw_event_type = str(event.get("type") or "")[:120]
    item = event.get("item") if isinstance(event.get("item"), dict) else event
    item_type = str(item.get("type") or raw_event_type).lower()[:120]
    lifecycle = _lifecycle(raw_event_type, item)
    item_id = str(item.get("id") or event.get("item_id") or "")[:200] or None
    query = _bounded_text(item.get("query") or item.get("search_query"), 1000)
    urls = _event_urls(item)
    url_query = query if query and _URL_RE.fullmatch(query.strip()) else None
    primary_url = url_query or (urls[0] if urls else None)
    semantic_action = _semantic_action(
        item_type=item_type,
        raw_event_type=raw_event_type,
        step_name=step_name,
        query=query,
        primary_url=primary_url,
    )
    provider_tool_type = item_type if semantic_action else None
    operational_identity = bool(item_id or query or primary_url)
    if not operational_identity:
        semantic_action = None
        provider_tool_type = item_type
    fingerprint = action_fingerprint(
        item_id=item_id,
        item_type=item_type,
        phase=step_name,
        semantic_action=semantic_action or "non_operational",
        query=query,
        source_url=primary_url,
    )
    usage = _usage(event, item)
    envelope = {
        "raw_event_type": raw_event_type,
        "raw_event_digest": hashlib.sha256(
            json.dumps(
                event,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        ).hexdigest(),
        "raw_shape": {
            "event_keys": sorted(str(key)[:80] for key in event)[:40],
            "item_keys": sorted(str(key)[:80] for key in item)[:40],
        },
        "provider_payload": _provider_summary(item),
        "lifecycle": lifecycle,
        "item_id": item_id,
        "item_type": item_type,
        "phase": step_name,
        "provider_tool_type": provider_tool_type,
        "semantic_action": semantic_action or "non_operational",
        "event_type": _legacy_event_type(semantic_action),
        "observed_at": str(
            item.get("observed_at")
            or item.get("timestamp")
            or event.get("timestamp")
            or datetime.now(UTC).replace(microsecond=0).isoformat()
        )[:80],
        "query": query,
        "source_url": primary_url,
        "canonical_url": _bounded_text(
            item.get("canonical_url") or primary_url,
            2048,
        ),
        "redirect_url": _bounded_text(item.get("redirect_url"), 2048),
        "tool_action_fingerprint": fingerprint,
        "status": _bounded_text(
            item.get("status") or event.get("status") or lifecycle,
            80,
        ),
        "usage": usage,
        "counts_usage": bool(operational_identity and semantic_action and lifecycle == "completed"),
        "discovered_urls": urls[:20] if semantic_action == "search" else [],
        "content_hash": _bounded_text(
            item.get("content_hash") or item.get("content_checksum"),
            128,
        ),
        "http_status": _bounded_status(item.get("http_status") or item.get("status_code")),
    }
    return [envelope]


def normalize_usage(candidate: Any) -> dict[str, Any]:
    if not isinstance(candidate, dict):
        return {}
    input_details = (
        candidate.get("input_tokens_details")
        if isinstance(candidate.get("input_tokens_details"), dict)
        else {}
    )
    output_details = (
        candidate.get("output_tokens_details")
        if isinstance(candidate.get("output_tokens_details"), dict)
        else {}
    )
    input_tokens = max(int(candidate.get("input_tokens") or 0), 0)
    output_tokens = max(int(candidate.get("output_tokens") or 0), 0)
    reported_total = max(int(candidate.get("total_tokens") or 0), 0)
    normalized: dict[str, Any] = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cached_tokens": max(
            int(
                candidate.get("cached_tokens")
                or candidate.get("cached_input_tokens")
                or input_details.get("cached_tokens")
                or 0
            ),
            0,
        ),
        "reasoning_tokens": max(
            int(candidate.get("reasoning_tokens") or output_details.get("reasoning_tokens") or 0),
            0,
        ),
        "total_tokens": reported_total or input_tokens + output_tokens,
    }
    for key in ("cost", "cost_usd", "total_cost_usd"):
        if candidate.get(key) is not None:
            normalized[key] = max(float(candidate[key]), 0.0)
    return normalized


def action_fingerprint(
    *,
    item_id: str | None,
    item_type: str,
    phase: str,
    semantic_action: str,
    query: str | None,
    source_url: str | None,
) -> str:
    if item_id:
        seed = f"id|{phase.upper()}|{item_type.lower()}|{item_id}"
    else:
        seed = "|".join(
            (
                "fallback",
                phase.upper(),
                item_type.lower(),
                semantic_action,
                _normalize_text(query),
                _normalize_url(source_url),
            )
        )
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


class ResearchLoopDetected(RuntimeError):
    def __init__(
        self,
        *,
        step: str,
        run_id: str,
        job_id: str,
        reason: str,
        evidence: list[dict[str, Any]],
    ) -> None:
        self.category = "LOOP_DETECTED"
        self.retryable = False
        self.retry_classification = "NON_RETRYABLE"
        self.code = f"research_loop_detected:{reason}"
        self.diagnostic = sanitize_diagnostic(
            {
                "category": self.category,
                "reason": reason[:120],
                "step": step,
                "run_id": run_id,
                "job_id": job_id,
                "retryable": False,
                "retry_classification": self.retry_classification,
                "fingerprints": [
                    str(item.get("tool_action_fingerprint") or "")[:64] for item in evidence[-12:]
                ],
                "loop_evidence": [_compact_event(item) for item in evidence[-12:]],
                "timestamp": datetime.now(UTC).replace(microsecond=0).isoformat(),
            }
        )
        super().__init__(self.code)


class ProgressLoopGuard:
    def __init__(
        self,
        settings: Settings,
        *,
        known_queries: list[str] | None = None,
        known_sources: list[str] | None = None,
    ) -> None:
        self.settings = settings
        self.known_queries = {_normalize_text(value) for value in known_queries or [] if value}
        self.known_sources = {_normalize_url(value) for value in known_sources or [] if value}
        self.recent: deque[dict[str, Any]] = deque(
            maxlen=max(settings.research_loop_cycle_window, 4)
            * max(settings.research_loop_cycle_repetitions, 2)
        )
        self.signature_counts: Counter[str] = Counter()
        self.failure_counts: Counter[str] = Counter()
        self.no_progress_actions = 0
        self.tool_calls = 0
        self.progress_events = 0

    def observe(self, envelope: dict[str, Any]) -> tuple[bool, str | None]:
        if envelope.get("lifecycle") == "failed":
            signature = _semantic_signature(envelope)
            self.failure_counts[signature] += 1
            self.recent.append(dict(envelope))
            if (
                self.failure_counts[signature]
                >= self.settings.research_loop_repeat_action_threshold
            ):
                return False, "repeated_tool_error"
            return False, None
        if not envelope.get("counts_usage"):
            return False, None
        self.tool_calls += 1
        signature = _semantic_signature(envelope)
        self.signature_counts[signature] += 1
        progress = self._is_progress(envelope)
        if progress:
            self.no_progress_actions = 0
            self.progress_events += 1
        else:
            self.no_progress_actions += 1
        self.recent.append(dict(envelope))
        if self.tool_calls > self.settings.research_emergency_max_tool_actions:
            return progress, "emergency_tool_action_limit"
        if (
            self.signature_counts[signature] >= self.settings.research_loop_repeat_action_threshold
            and not progress
        ):
            return progress, "repeated_action_without_progress"
        if self.no_progress_actions >= self.settings.research_loop_no_progress_action_threshold:
            return progress, "no_progress_action_window"
        if self._cyclic():
            return progress, "cyclic_action_sequence"
        return progress, None

    def mark_phase_progress(self) -> None:
        self.no_progress_actions = 0
        self.progress_events += 1

    def snapshot(self) -> dict[str, Any]:
        return {
            "tool_calls": self.tool_calls,
            "progress_events": self.progress_events,
            "no_progress_actions": self.no_progress_actions,
            "recent_fingerprints": [
                str(item.get("tool_action_fingerprint") or "")[:64]
                for item in list(self.recent)[-12:]
            ],
        }

    def evidence(self) -> list[dict[str, Any]]:
        return list(self.recent)

    def _is_progress(self, envelope: dict[str, Any]) -> bool:
        action = str(envelope.get("semantic_action") or "")
        query = _normalize_text(envelope.get("query"))
        source = _normalize_url(envelope.get("canonical_url") or envelope.get("source_url"))
        discovered = {
            _normalize_url(value) for value in envelope.get("discovered_urls") or [] if value
        }
        new_discovered = discovered - self.known_sources
        if new_discovered:
            self.known_sources.update(new_discovered)
            return True
        if action in COUNTED_SOURCE_ACTIONS and source:
            if source not in self.known_sources:
                self.known_sources.add(source)
                return True
            return False
        if action == "search" and query:
            if query not in self.known_queries:
                self.known_queries.add(query)
                return True
            return False
        return False

    def _cyclic(self) -> bool:
        window = self.settings.research_loop_cycle_window
        repetitions = self.settings.research_loop_cycle_repetitions
        required = window * repetitions
        if len(self.recent) < required:
            return False
        signatures = [_semantic_signature(item) for item in self.recent]
        tail = signatures[-window:]
        return all(
            signatures[-window * (index + 1) : -window * index or None] == tail
            for index in range(repetitions)
        )


def _semantic_action(
    *,
    item_type: str,
    raw_event_type: str,
    step_name: str,
    query: str | None,
    primary_url: str | None,
) -> str | None:
    combined = f"{item_type} {raw_event_type}".lower()
    phase = step_name.upper()
    url_only = bool(query and _URL_RE.fullmatch(query.strip()))
    if any(token in combined for token in ("web_fetch", "fetch_url", "http_fetch", "browser.open")):
        return "fetch"
    if any(token in combined for token in ("web_open", "open_source", "source.open", "open_url")):
        return "open_source"
    if any(
        token in combined for token in ("verify_source", "source.verify", "server_source_verified")
    ):
        return "verify_source"
    if any(token in combined for token in ("web_search", "search_query", "source.search")):
        if phase == "CROSS_CHECK" and url_only:
            return "verify_source"
        if url_only or (primary_url and phase == "OPEN_SOURCE"):
            return "open_source"
        return "search"
    return None


def _lifecycle(raw_event_type: str, item: dict[str, Any]) -> str:
    lowered = raw_event_type.lower()
    status = str(item.get("status") or "").lower()
    for value in ("failed", "completed", "started"):
        if lowered.endswith(f".{value}") or status == value:
            return value
    return "observed"


def _usage(event: dict[str, Any], item: dict[str, Any]) -> dict[str, int]:
    candidate = event.get("usage") or item.get("usage")
    return {
        key: int(value)
        for key, value in normalize_usage(candidate).items()
        if key
        in {
            "input_tokens",
            "output_tokens",
            "cached_tokens",
            "reasoning_tokens",
            "total_tokens",
        }
    }


def _event_urls(value: Any) -> list[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if (
                key.lower()
                in {
                    "url",
                    "source_url",
                    "canonical_url",
                    "redirect_url",
                }
                and isinstance(item, str)
                and _URL_RE.fullmatch(item.strip())
            ):
                found.add(item.strip())
            else:
                found.update(_event_urls(item))
    elif isinstance(value, list):
        for item in value:
            found.update(_event_urls(item))
    return sorted(found)


def _provider_summary(item: dict[str, Any]) -> dict[str, Any]:
    """Retain bounded operational fields while excluding messages and page content."""

    allowed_scalar = {
        "action",
        "id",
        "name",
        "query",
        "search_query",
        "status",
        "status_code",
        "http_status",
        "title",
        "type",
        "url",
        "source_url",
        "canonical_url",
        "redirect_url",
        "content_hash",
        "content_checksum",
    }
    summary: dict[str, Any] = {}
    for key in allowed_scalar:
        value = item.get(key)
        if isinstance(value, (str, int, float, bool)):
            summary[key] = str(value)[:1000] if isinstance(value, str) else value
        elif isinstance(value, dict):
            summary[key] = {
                str(child_key)[:80]: (
                    str(child_value)[:1000] if isinstance(child_value, str) else child_value
                )
                for child_key, child_value in list(value.items())[:20]
                if isinstance(child_value, (str, int, float, bool))
                and str(child_key).lower() not in {"content", "text", "message", "prompt", "body"}
            }
    urls = _event_urls(item)
    if urls:
        summary["urls"] = urls[:20]
    return summary


def _legacy_event_type(semantic_action: str | None) -> str:
    if semantic_action == "search":
        return "search"
    if semantic_action in COUNTED_SOURCE_ACTIONS:
        return "open_source"
    return "observed"


def _semantic_signature(envelope: dict[str, Any]) -> str:
    return "|".join(
        (
            str(envelope.get("phase") or ""),
            str(envelope.get("semantic_action") or ""),
            _normalize_text(envelope.get("query")),
            _normalize_url(envelope.get("canonical_url") or envelope.get("source_url")),
            str(envelope.get("status") or ""),
        )
    )


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").lower().split())[:1000]


def _normalize_url(value: Any) -> str:
    return str(value or "").strip().lower().rstrip("/")[:2048]


def _bounded_text(value: Any, limit: int) -> str | None:
    text = str(value or "").strip()
    return text[:limit] if text else None


def _bounded_status(value: Any) -> int | None:
    try:
        status = int(value)
    except (TypeError, ValueError):
        return None
    return status if 100 <= status <= 599 else None


def _compact_event(event: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "raw_event_type",
        "lifecycle",
        "item_id",
        "item_type",
        "phase",
        "semantic_action",
        "query",
        "source_url",
        "tool_action_fingerprint",
        "status",
    )
    compact = {key: event.get(key) for key in keys}
    return json.loads(json.dumps(compact, ensure_ascii=False, default=str))
