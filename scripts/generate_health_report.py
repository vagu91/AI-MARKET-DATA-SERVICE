from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.health_report_service import HealthReportService


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate AI-MARKET-DATA-SERVICE health report.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8011")
    parser.add_argument("--refresh", choices=["false", "auto", "force"], default="false")
    parser.add_argument("--output-directory", default=str(ROOT / "data" / "diagnostics" / "health_reports"))
    parser.add_argument("--fail-on-warning", action="store_true")
    parser.add_argument("--compact", action="store_true")
    parser.add_argument("--no-history", action="store_true")
    parser.add_argument("--open-latest", action="store_true")
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()

    try:
        report = collect_report(args)
        report = HealthReportService().write_report(
            report,
            output_directory=args.output_directory,
            no_history=args.no_history,
        )
        print_report(report, compact=args.compact)
        if args.open_latest and report["files"]["latest_report"]:
            open_file(report["files"]["latest_report"])
        return int(report.get("exit_code", 3))
    except Exception as exc:
        print(f"Technical error while generating health report: {exc}", file=sys.stderr)
        return 3


def collect_report(args: argparse.Namespace) -> dict[str, Any]:
    base_url = args.base_url.rstrip("/")
    responses: dict[str, Any] = {}
    errors: dict[str, str] = {}
    endpoints = {
        "db_health": "/db/health",
        "market_context": market_context_path(args.refresh),
        "temporal_integrity": "/diagnostics/temporal-integrity",
        "release_refresh": "/diagnostics/release-refresh-status",
        "news_freshness": "/diagnostics/news-freshness",
        "source_classification": "/diagnostics/source-classification",
        "db_summary": "/diagnostics/db-summary",
    }
    for key, path in endpoints.items():
        try:
            responses[key] = fetch_json(f"{base_url}{path}", timeout=args.timeout)
        except Exception as exc:
            errors[key] = str(exc)
    service_status = "ok" if not errors else ("unreachable" if "db_health" in errors and "market_context" in errors else "error")
    db_health = responses.get("db_health") or {"status": "error", "errors": errors}
    db_summary = responses.get("db_summary") or (db_health.get("db_summary") if isinstance(db_health, dict) else {}) or {}
    report = HealthReportService().build_report(
        base_url=base_url,
        refresh_mode=args.refresh,
        service_status=service_status,
        db_health=db_health,
        market_context=responses.get("market_context") or {},
        temporal_integrity=responses.get("temporal_integrity") or {},
        release_refresh=responses.get("release_refresh") or {},
        news_freshness=responses.get("news_freshness") or {},
        source_classification=responses.get("source_classification") or {},
        db_summary=db_summary,
        ai_researcher_enabled=db_health.get("ai_researcher_enabled") if isinstance(db_health, dict) else None,
        ai_researcher_mode=db_health.get("ai_researcher_mode") if isinstance(db_health, dict) else None,
        fail_on_warning=args.fail_on_warning,
    )
    if errors:
        report["infos"].append({"endpoint_errors": errors})
    return report


def market_context_path(refresh: str) -> str:
    return f"/market-context/mnq/debug?refresh={urllib.parse.quote(refresh)}"


def fetch_json(url: str, *, timeout: float) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        if response.status >= 400:
            raise urllib.error.HTTPError(url, response.status, response.reason, response.headers, None)
        return json.loads(response.read().decode("utf-8"))


def print_report(report: dict[str, Any], *, compact: bool) -> None:
    status = str(report.get("status") or "FAIL")
    summary = report.get("summary") or {}
    quality = report.get("quality_scores") or {}
    counts = report.get("model_counts") or {}
    temporal = report.get("temporal_integrity") or {}
    release = report.get("release_refresh") or {}
    news = report.get("news_health") or {}
    nasdaq = report.get("nasdaq_health") or {}
    if compact:
        print(
            f"{status} | ready={str(summary.get('is_ready_for_market_analysis')).lower()} "
            f"| critical={summary.get('critical_error_count', 0)} "
            f"| warnings={summary.get('warning_count', 0)} "
            f"| future_actual={temporal.get('future_actual_count', 0)} "
            f"| stale_as_recent={temporal.get('stale_as_recent_count', 0)} "
            f"| news={counts.get('latest_news', 0)} "
            f"| qqq={counts.get('qqq_holdings', 0)}"
        )
        return
    line = "=" * 60
    print(line)
    print("AI-MARKET-DATA-SERVICE HEALTH REPORT")
    print(line)
    print(f"Status: {status}")
    print(f"Generated: {report.get('generated_at_utc')}")
    print(f"Base URL: {(report.get('service') or {}).get('base_url')}")
    print(f"Ready: {str(summary.get('is_ready_for_market_analysis')).lower()}")
    print("")
    print("Quality")
    print(f"- Completeness: {float(quality.get('completeness_score') or 0):.3f}")
    print(f"- Freshness: {float(quality.get('freshness_score') or 0):.3f}")
    print(f"- Reliability: {float(quality.get('reliability_score') or 0):.3f}")
    print(f"- Temporal consistency: {float(quality.get('temporal_consistency_score') or 0):.3f}")
    print(f"- Source integrity: {float(quality.get('source_integrity_score') or 0):.3f}")
    print("")
    print("Anomalies")
    print(f"- Future actual: {temporal.get('future_actual_count', 0)}")
    print(f"- Stale as recent: {temporal.get('stale_as_recent_count', 0)}")
    print(f"- Invalid periods: {temporal.get('invalid_period_mapping_count', 0)}")
    print(f"- Blocking errors: {temporal.get('blocking_errors_count', 0)}")
    print(f"- FRED in release queue: {release.get('fred_in_release_queue', 0)}")
    print(f"- Placeholder news: {news.get('placeholder_news_count', 0)}")
    print("")
    print("Coverage")
    print(f"- Critical macro events: {counts.get('critical_macro_events', 0)}")
    print(f"- Fed communications: {counts.get('fed_communications', 0)}")
    print(f"- Latest news: {counts.get('latest_news', 0)}")
    print(f"- QQQ holdings: {counts.get('qqq_holdings', 0)}")
    print(f"- Mega-cap resolved: {counts.get('mega_cap_resolved', 0)}")
    print(f"- Sector unknown: {float(nasdaq.get('sector_unknown_weight_pct') or 0):.2f}%")
    print("")
    print("Report:")
    print((report.get("files") or {}).get("latest_report"))
    print(line)


def open_file(path: str) -> None:
    if os.name == "nt":
        os.startfile(path)  # type: ignore[attr-defined]
    else:
        print(f"Report written to {path}")


def _dig(value: Any, *keys: str) -> Any:
    current = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


if __name__ == "__main__":
    raise SystemExit(main())
