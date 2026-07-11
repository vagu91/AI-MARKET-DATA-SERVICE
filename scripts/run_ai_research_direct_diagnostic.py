from __future__ import annotations

import argparse
import inspect
import json
from pathlib import Path

from app.core.config import Settings
from app.providers.ai_researcher_provider import AIResearcherProvider
from app.services.ai_research_validation_service import validate_ai_research_result
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
from app.services.market_fact_repository import MarketFactRepository


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the production Codex CLI path for one captured event.")
    parser.add_argument("--input", type=Path, required=True, help="Existing production research_input.json.")
    parser.add_argument("--artifact-dir", type=Path, required=True)
    args = parser.parse_args()

    payload = json.loads(args.input.read_text(encoding="utf-8"))
    events = list(payload.get("events") or [])[:1]
    if not events:
        raise SystemExit("input does not contain an event")
    settings = Settings(
        _env_file=None,
        enable_ai_researcher=True,
        ai_diagnostics=True,
        ai_diagnostics_dir=args.artifact_dir,
    )
    _write_pipeline_map(args.artifact_dir)
    facts, status = AIResearcherProvider(settings)._codex_cli(events)
    (args.artifact_dir / "direct_cli_summary.json").write_text(
        json.dumps({"facts_returned": len(facts), "status": status}, indent=2, default=str),
        encoding="utf-8",
    )
    print(json.dumps({"artifact_dir": str(args.artifact_dir), "facts_returned": len(facts), "status": status}, indent=2, default=str))
    return 0


def _write_pipeline_map(artifact_dir: Path) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    targets = [
        ("AI eligibility and event mapping", EnrichmentOrchestrator._ai_candidates),
        ("AI input payload", EnrichmentOrchestrator._event_payload),
        ("Codex CLI invocation and stdout parser", AIResearcherProvider._codex_cli),
        ("JSON validation and fact mapping", AIResearcherProvider.load_payload),
        ("Field validator", validate_ai_research_result),
        ("Persistence", MarketFactRepository.upsert_fact),
        ("Read-back", MarketFactRepository.get_fact),
    ]
    lines = ["# AI Enrichment Pipeline Map", "", "This map is generated from the production code used by this diagnostic run.", ""]
    for label, target in targets:
        source_file = inspect.getsourcefile(target) or "unknown"
        _, line_no = inspect.getsourcelines(target)
        lines.append(f"- **{label}**: `{source_file}:{line_no}` - `{target.__qualname__}`")
    lines.extend(
        [
            "",
            "## Timeout hierarchy",
            "",
            "- Endpoint client timeout is configured by the diagnostic harness.",
            "- `DiagnosticsService.full_model` wraps event enrichment with `timeout_events_seconds`.",
            "- `AIResearcherProvider.research` wraps the worker with `timeout_ai_research_seconds`.",
            "- `AIResearcherProvider._codex_cli` passes `codex_research_timeout_seconds` to `subprocess.run`.",
        ]
    )
    (artifact_dir / "pipeline_map.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
