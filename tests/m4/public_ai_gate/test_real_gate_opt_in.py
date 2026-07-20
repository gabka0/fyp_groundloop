from __future__ import annotations

import os
from pathlib import Path

import pytest

from groundloop.m4.public_ai_gate import run_public_ai_gate


@pytest.mark.skipif(
    os.environ.get("GROUNDLOOP_RUN_M4_PUBLIC_AI_REAL") != "1",
    reason="real M4.12 model gate is explicit and local-only",
)
def test_real_vitaminc_gate(tmp_path: Path) -> None:
    required = {
        "official_repository": "GROUNDLOOP_VITAMINC_REPOSITORY",
        "vitaminc_archive": "GROUNDLOOP_VITAMINC_ARCHIVE",
        "vitaminc_root": "GROUNDLOOP_VITAMINC_ROOT",
        "m3_artifact_root": "GROUNDLOOP_M3_VERIFIER_ARTIFACT_ROOT",
        "embedding_cache": "GROUNDLOOP_M3_EMBEDDING_CACHE",
    }
    values = {name: os.environ.get(variable) for name, variable in required.items()}
    missing = [required[name] for name, value in values.items() if value is None]
    assert not missing, f"missing explicit real-gate environment variables: {missing}"
    result = run_public_ai_gate(
        official_repository=Path(str(values["official_repository"])),
        vitaminc_archive=Path(str(values["vitaminc_archive"])),
        vitaminc_root=Path(str(values["vitaminc_root"])),
        m3_artifact_root=Path(str(values["m3_artifact_root"])),
        embedding_cache=Path(str(values["embedding_cache"])),
        config_path=Path("configs/m4/public_ai/vitaminc_revision_gate_v1.json"),
        output_directory=tmp_path,
    )
    report = result["report"]
    assert report["gate_verdict"]["provenance"] == "PASSED"
    assert report["gate_verdict"]["real_model_execution"] == "PASSED"
    assert report["dataset"]["selection"]["pages"] == 128
    assert result["semantic_result_sha256"] == report["semantic_result"]["sha256"]
    assert "report_sha256_run_specific" in result
    assert "manifest_sha256_run_specific" in result
    assert (tmp_path / "semantic_result.json").is_file()
    assert (tmp_path / "manifest.json").is_file()
