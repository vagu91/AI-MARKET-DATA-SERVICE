from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services import provider_capability_registry as registry  # noqa: E402
from app.services.provider_capability_registry import (  # noqa: E402
    render_matrix_json,
    render_matrix_markdown,
    validate_registry,
)
from scripts.provider_capability_live_baseline import (  # noqa: E402
    ProviderCapabilityBaselineImportError,
    baseline_file_sha256,
    import_verified_live_audit,
)


JSON_PATH = ROOT / "docs" / "baselines" / "senior-analyst-data-source-matrix.json"
MARKDOWN_PATH = ROOT / "docs" / "senior-analyst-data-source-matrix.md"
LAST_LIVE_BASELINE_PATH = (
    ROOT / "docs" / "baselines" / "provider-capability-last-live.json"
)


def _expected_documents() -> tuple[tuple[Path, str], ...]:
    validate_registry()
    baseline_expected = (
        registry.LAST_LIVE_BASELINE_FILE_SHA256 is not None
        or registry.LAST_LIVE_BASELINE_PATH.exists()
    )
    if baseline_expected and registry._load_last_live_baseline() is None:  # noqa: SLF001
        raise RuntimeError(
            "invalid_or_stale_provider_capability_live_baseline"
        )
    return (
        (JSON_PATH, render_matrix_json()),
        (MARKDOWN_PATH, render_matrix_markdown()),
    )


def _check() -> int:
    stale: list[str] = []
    for path, expected in _expected_documents():
        if not path.is_file():
            stale.append(f"missing:{path.relative_to(ROOT)}")
            continue
        actual = path.read_text(encoding="utf-8")
        if actual != expected:
            stale.append(f"out_of_date:{path.relative_to(ROOT)}")
    if stale:
        print("\n".join(stale), file=sys.stderr)
        return 1
    print("provider capability matrix is deterministic and up to date")
    return 0


def _write() -> int:
    for path, content in _expected_documents():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8", newline="\n")
        print(f"wrote {path.relative_to(ROOT)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate the Senior Analyst provider capability matrix."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail when committed JSON or Markdown differs byte-for-byte.",
    )
    parser.add_argument(
        "--import-live-pointer",
        type=Path,
        help=(
            "Import a fully verified COMPLETED LIVE audit pointer into "
            "the compact versioned last-LIVE baseline."
        ),
    )
    parser.add_argument(
        "--last-live-output",
        type=Path,
        default=LAST_LIVE_BASELINE_PATH,
        help=(
            "Destination for --import-live-pointer "
            "(default: docs/baselines/provider-capability-last-live.json)."
        ),
    )
    args = parser.parse_args()
    if args.import_live_pointer is not None:
        if args.check:
            parser.error(
                "--check cannot be combined with --import-live-pointer"
            )
        try:
            baseline = import_verified_live_audit(
                args.import_live_pointer,
                destination=args.last_live_output,
            )
        except ProviderCapabilityBaselineImportError as exc:
            parser.error(str(exc))
        print(
            "imported verified provider capability LIVE audit "
            f"{baseline['run_id']} to "
            f"{args.last_live_output.resolve()}"
        )
        baseline_sha256 = baseline_file_sha256(baseline)
        print(
            "exact baseline file SHA-256: "
            f"{baseline_sha256}; update "
            "LAST_LIVE_BASELINE_FILE_SHA256 explicitly before regenerating "
            "the versioned matrix"
        )
        return 0
    return _check() if args.check else _write()


if __name__ == "__main__":
    raise SystemExit(main())
