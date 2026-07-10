from __future__ import annotations

import os
from typing import Any

from app.core.config import Settings


def credential_audit(settings: Settings) -> dict[str, Any]:
    rows = [
        _row("FRED macro", "FRED", True, bool(settings.fred_api_key), "free", False, "DB cache", True),
        _row("BLS macro/calendar", "BLS", False, bool(settings.bls_api_key), "free public limited", False, "public API", True),
        _row("BEA macro", "BEA", True, bool(settings.bea_api_key), "free", False, "DB cache", True),
        _row("QQQ holdings/news/earnings", "Alpha Vantage", True, bool(settings.alpha_vantage_api_key), "free limited", False, "Invesco/Yahoo/RSS", True),
        _row("Yahoo quote/news", "Yahoo Finance", False, True, "public unofficial", False, "Stooq/RSS/cache", False),
        _row("COT", "CFTC", False, True, "public official", False, "CFTC public text", True),
        _row("Fed official news", "Federal Reserve RSS", False, True, "public official", False, "RSS cache", False),
        _row("AAII sentiment", "AAII", False, False, "web access dependent", False, "public page if visible", True),
        _row("Canonical URL", "Publisher HTTP/OpenGraph", False, True, "public web", False, "aggregator URL + AI fallback", True),
        _row("Forecast/consensus", "Event enrichment/web sources", False, True, "public web", False, "AI Researcher", True),
        _row("Put/call", "public web", False, False, "provider-dependent", False, "AI Researcher", True),
        _row("VIX term structure", "public web", False, False, "provider-dependent", False, "AI Researcher", True),
        _row("AI Researcher", "Codex CLI/OpenAI", False, bool(settings.openai_api_key) or settings.ai_researcher_mode == "codex_cli", "local login or API", False, "negative cache", False),
    ]
    return {
        "providers": rows,
        "secrets_redacted": True,
        "environment_keys_present": sorted(name for name in os.environ if name.startswith("AI_MARKET_") and "KEY" in name),
        "provider_action_required": [],
        "can_proceed": True,
        "reason": "No missing key is strictly required: unavailable optional data has public-source or AI fallback with negative cache.",
        "service_role": "data provider only",
    }


def _row(data: str, provider: str, key_required: bool, configured: bool, plan: str, manual: bool, alternative: str, ai_fallback: bool) -> dict[str, Any]:
    if key_required and configured:
        status = "KEY_ALREADY_CONFIGURED"
    elif key_required:
        status = "KEY_MISSING_FREE_REGISTRATION" if "free" in plan else "KEY_MISSING_PAID_ONLY"
    elif "restricted" in plan.lower():
        status = "ACCESS_RESTRICTED"
    elif not key_required:
        status = "NO_KEY_REQUIRED"
    else:
        status = "AI_FALLBACK_AVAILABLE"
    return {
        "data": data,
        "provider": provider,
        "api_key_required": key_required,
        "already_configured": configured,
        "free_plan_sufficient": "free" in plan or not key_required,
        "manual_registration_required": manual,
        "alternative_without_key": alternative,
        "ai_fallback": ai_fallback,
        "classification": status if configured or not ai_fallback else "AI_FALLBACK_AVAILABLE",
    }
