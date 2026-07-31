from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from app.services.provider_audit_artifacts import (
    ProviderAuditPublicationRejected,
    publish_verified_audit_candidate,
)


def _candidate_tree(
    tmp_path: Path,
) -> tuple[Path, Path, Path, dict[str, Any]]:
    data = tmp_path / "data"
    run_directory = data / "provider-capability-audit" / "RUN-1"
    run_directory.mkdir(parents=True)
    report_path = run_directory / "audit-report.json"
    report_path.write_text(
        '{"run_id":"RUN-1","audit_status":"COMPLETED"}\n',
        encoding="utf-8",
    )
    pointer = {
        "schema_version": "provider-capability-audit-latest-v1",
        "run_id": "RUN-1",
        "audit_status": "COMPLETED",
        "full_audit_scope": True,
        "run_directory": str(run_directory),
    }
    candidate = data / ".provider-capability-audit-latest.RUN-1.candidate.json"
    candidate.write_text(
        json.dumps(pointer, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    latest = data / "provider-capability-audit-latest.json"
    latest.write_bytes(b'{"run_id":"OLD"}\n')
    return candidate, latest, report_path, pointer


def test_publication_rejects_artifact_tree_changed_by_verifier(
    tmp_path: Path,
) -> None:
    candidate, latest, report_path, pointer = _candidate_tree(tmp_path)
    old_latest = latest.read_bytes()

    def mutating_verifier(
        _candidate: Path,
        _latest: Path,
    ) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
        report_path.write_text(
            '{"run_id":"FORGED","audit_status":"COMPLETED"}\n',
            encoding="utf-8",
        )
        return pointer, {
            "run_id": "RUN-1",
            "audit_status": "COMPLETED",
        }, []

    with pytest.raises(ProviderAuditPublicationRejected) as raised:
        publish_verified_audit_candidate(
            candidate,
            latest,
            mutating_verifier,
        )
    assert (
        "CAPABILITY_AUDIT_ARTIFACT_TREE_CHANGED_DURING_ACCEPTANCE"
        in raised.value.errors
    )
    assert latest.read_bytes() == old_latest
    assert not candidate.exists()
