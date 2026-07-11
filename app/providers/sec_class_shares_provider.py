from __future__ import annotations

import html
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from app.core.config import Settings


CLASS_MEMBER_MAP = {
    "CommonClassAMember": "A",
    "CommonClassBMember": "B",
    "CommonClassCMember": "C",
    "CapitalClassCMember": "C",
}


class SecClassSharesProvider:
    source = "SEC inline XBRL filing"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fetch(self, *, cik: str, listed_class_symbols: dict[str, str]) -> dict[str, Any]:
        now = datetime.now(UTC)
        normalized_cik = str(cik).zfill(10)
        headers = {
            "User-Agent": self.settings.sec_user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json,text/html,application/xhtml+xml",
        }
        network_calls = 0
        try:
            async with httpx.AsyncClient(timeout=self.settings.sec_request_timeout_seconds) as client:
                submissions_url = f"{self.settings.sec_submissions_base_url}/CIK{normalized_cik}.json"
                network_calls += 1
                response = await client.get(submissions_url, headers=headers)
                response.raise_for_status()
                filing = latest_periodic_filing(response.json())
                if not filing:
                    return _failure("periodic_filing_not_found", cik, now, network_calls)
                accession = str(filing["accession"]).replace("-", "")
                filing_url = (
                    f"{self.settings.sec_archives_base_url}/{int(normalized_cik)}/"
                    f"{accession}/{filing['document']}"
                )
                network_calls += 1
                response = await client.get(filing_url, headers=headers)
                response.raise_for_status()
        except Exception as exc:
            return _failure(str(exc) or type(exc).__name__, cik, now, network_calls)

        parsed = parse_inline_xbrl_class_shares(response.text, listed_class_symbols=listed_class_symbols)
        valid_until = now + timedelta(hours=self.settings.sec_class_shares_ttl_hours)
        verified = bool(parsed["listed_shares"]) and not parsed["errors"]
        return {
            "status": "found" if verified else "partial" if parsed["class_shares"] else "not_found",
            "issuer_id": f"CIK{normalized_cik}",
            "cik": normalized_cik,
            "source": self.source,
            "source_url": filing_url,
            "filing_form": filing["form"],
            "filing_date": filing["filed"],
            "accession": filing["accession"],
            "class_shares": parsed["class_shares"],
            "listed_shares": parsed["listed_shares"],
            "unlisted_classes": parsed["unlisted_classes"],
            "shares_as_of": parsed["shares_as_of"],
            "retrieved_at": _iso(now),
            "valid_until": _iso(valid_until),
            "verified": verified,
            "network_calls": network_calls,
            "warnings": parsed["warnings"],
            "errors": parsed["errors"],
        }


def latest_periodic_filing(payload: dict[str, Any]) -> dict[str, Any] | None:
    recent = ((payload.get("filings") or {}).get("recent") or {})
    forms = recent.get("form") or []
    accessions = recent.get("accessionNumber") or []
    documents = recent.get("primaryDocument") or []
    filed_dates = recent.get("filingDate") or []
    candidates = []
    for index, form in enumerate(forms):
        if form not in {"10-Q", "10-K"}:
            continue
        if index >= len(accessions) or index >= len(documents):
            continue
        candidates.append(
            {
                "form": form,
                "accession": accessions[index],
                "document": documents[index],
                "filed": filed_dates[index] if index < len(filed_dates) else None,
            }
        )
    return max(candidates, key=lambda item: str(item.get("filed") or ""), default=None)


def parse_inline_xbrl_class_shares(
    text: str,
    *,
    listed_class_symbols: dict[str, str],
) -> dict[str, Any]:
    contexts: dict[str, dict[str, str | None]] = {}
    for match in re.finditer(
        r'<xbrli:context\s+id="([^"]+)"[^>]*>(.*?)</xbrli:context>',
        text,
        flags=re.I | re.S,
    ):
        context_id, block = match.groups()
        member_match = re.search(r'<xbrldi:explicitMember[^>]*>(.*?)</xbrldi:explicitMember>', block, flags=re.I | re.S)
        instant_match = re.search(r'<xbrli:instant>(.*?)</xbrli:instant>', block, flags=re.I | re.S)
        member = _clean(member_match.group(1)) if member_match else None
        class_code = _class_code(member)
        contexts[context_id] = {
            "member": member,
            "class_code": class_code,
            "instant": _clean(instant_match.group(1)) if instant_match else None,
        }

    facts: list[dict[str, Any]] = []
    pattern = r'<ix:nonFraction\b([^>]*)>(.*?)</ix:nonFraction>'
    for match in re.finditer(pattern, text, flags=re.I | re.S):
        attributes = _attributes(match.group(1))
        if str(attributes.get("name") or "").lower() != "dei:entitycommonstocksharesoutstanding":
            continue
        context = contexts.get(str(attributes.get("contextref") or ""), {})
        class_code = context.get("class_code")
        value = _number(match.group(2))
        if not class_code or value is None:
            continue
        scale = int(str(attributes.get("scale") or "0"))
        facts.append(
            {
                "class_code": class_code,
                "shares": value * (10**scale),
                "as_of": context.get("instant"),
                "member": context.get("member"),
            }
        )
    if not facts:
        return {
            "class_shares": {},
            "listed_shares": {},
            "unlisted_classes": [],
            "shares_as_of": None,
            "warnings": [],
            "errors": ["class_share_facts_not_found"],
        }
    latest_as_of = max(str(fact.get("as_of") or "") for fact in facts)
    latest = [fact for fact in facts if str(fact.get("as_of") or "") == latest_as_of]
    class_shares = {str(fact["class_code"]): float(fact["shares"]) for fact in latest}
    listed_shares = {
        symbol: class_shares[class_code]
        for class_code, symbol in listed_class_symbols.items()
        if class_code in class_shares
    }
    unlisted = sorted(class_code for class_code in class_shares if class_code not in listed_class_symbols)
    missing = sorted(set(listed_class_symbols) - set(class_shares))
    return {
        "class_shares": class_shares,
        "listed_shares": listed_shares,
        "unlisted_classes": unlisted,
        "shares_as_of": latest_as_of or None,
        "warnings": [f"unlisted_class_excluded:{item}" for item in unlisted],
        "errors": [f"listed_class_missing:{item}" for item in missing],
    }


def _attributes(value: str) -> dict[str, str]:
    return {
        key.lower(): html.unescape(raw)
        for key, raw in re.findall(r'([\w:-]+)\s*=\s*"([^"]*)"', value)
    }


def _class_code(member: str | None) -> str | None:
    if not member:
        return None
    for marker, class_code in CLASS_MEMBER_MAP.items():
        if marker.lower() in member.lower():
            return class_code
    match = re.search(r"Class([A-Z])", member, flags=re.I)
    return match.group(1).upper() if match else None


def _number(value: str) -> float | None:
    cleaned = re.sub(r"[^0-9.()\-]", "", html.unescape(re.sub(r"<[^>]+>", "", value)))
    if not cleaned:
        return None
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    try:
        parsed = float(cleaned.strip("()"))
    except ValueError:
        return None
    return -parsed if negative else parsed


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", html.unescape(str(value or ""))).strip()


def _failure(reason: str, cik: str, now: datetime, network_calls: int) -> dict[str, Any]:
    return {
        "status": "not_found",
        "issuer_id": f"CIK{str(cik).zfill(10)}",
        "cik": str(cik).zfill(10),
        "source": "SEC inline XBRL filing",
        "source_url": None,
        "class_shares": {},
        "listed_shares": {},
        "unlisted_classes": [],
        "shares_as_of": None,
        "retrieved_at": _iso(now),
        "valid_until": _iso(now + timedelta(hours=1)),
        "verified": False,
        "network_calls": network_calls,
        "warnings": [],
        "errors": [reason],
    }


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
