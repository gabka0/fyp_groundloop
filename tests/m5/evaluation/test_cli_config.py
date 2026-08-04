from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

from groundloop.m5.evaluation.config import (
    SOURCE_VERIFICATION_STATEMENT,
    ControlledEvaluationConfig,
)
from groundloop.m5.evaluation.records import WiceSplit
from m5.evaluation.helpers import Rows, parent_row, subclaim_row


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _environment() -> dict[str, str]:
    return {
        **os.environ,
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": str(_repository_root() / "src"),
    }


def test_cli_routes_source_manifest_only_through_consumed_config(
    tmp_path: Path,
    wice_fixture_factory: Callable[[Rows], tuple[Path, Path]],
    controlled_config_factory: Callable[[Path], ControlledEvaluationConfig],
) -> None:
    rows: Rows = {
        WiceSplit.TRAIN: (
            [parent_row("cli-config", ["evidence"])],
            [subclaim_row("cli-config-0", ["evidence"], [[0]])],
        )
    }
    source_root, manifest_path = wice_fixture_factory(rows)
    config = controlled_config_factory(manifest_path)
    output = tmp_path / "audit.json"
    result = subprocess.run(
        (
            sys.executable,
            "scripts/m5/run_controlled_evaluation.py",
            "--source-root",
            str(source_root),
            "--config",
            str(config.consumed_path),
            "--output",
            str(output),
            "--audit-only",
        ),
        cwd=_repository_root(),
        env=_environment(),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    summary = json.loads(result.stdout)
    report = json.loads(output.read_text(encoding="utf-8"))
    assert summary["config_path"] == str(config.consumed_path)
    assert summary["config_canonical_sha256"] == config.canonical_sha256
    assert summary["source_manifest_path"] == str(config.source_manifest_path)
    assert summary["source_verification"] == SOURCE_VERIFICATION_STATEMENT
    assert summary["result_classification"]["classification"] == (
        "non_primary_config_bundle"
    )
    assert summary["result_classification"]["eligible"] is False
    assert report["evaluation_config"]["canonical_sha256"] == (config.canonical_sha256)
    assert report["adapter_audit"]["evaluation_config"] == (report["evaluation_config"])
    assert report["result_classification"] == summary["result_classification"]


def test_cli_rejects_removed_free_manifest_argument(tmp_path: Path) -> None:
    result = subprocess.run(
        (
            sys.executable,
            "scripts/m5/run_controlled_evaluation.py",
            "--source-root",
            str(tmp_path),
            "--output",
            str(tmp_path / "report.json"),
            "--manifest",
            str(tmp_path / "manifest.json"),
        ),
        cwd=_repository_root(),
        env=_environment(),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "unrecognized arguments: --manifest" in result.stderr
