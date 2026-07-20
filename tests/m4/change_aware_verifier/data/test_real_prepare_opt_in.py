from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from m4_13_verifier.prepare import prepare


@pytest.mark.skipif(
    os.environ.get("GROUNDLOOP_RUN_M4_13_REAL_PREPARE") != "1",
    reason="real M4.13 source preparation is explicit and local-only",
)
def test_real_preparation(tmp_path: Path) -> None:
    required = {
        "official_repository": "GROUNDLOOP_VITAMINC_REPOSITORY",
        "vitaminc_archive": "GROUNDLOOP_VITAMINC_ARCHIVE",
        "vitaminc_root": "GROUNDLOOP_VITAMINC_ROOT",
        "m3_artifact_root": "GROUNDLOOP_M3_VERIFIER_ARTIFACT_ROOT",
        "m4_12_artifact_root": "GROUNDLOOP_M4_12_ARTIFACT_ROOT",
    }
    values = {name: os.environ.get(variable) for name, variable in required.items()}
    missing = [required[name] for name, value in values.items() if value is None]
    assert not missing, f"missing real preparation inputs: {missing}"
    output = tmp_path / "m4-13"
    result = prepare(
        official_repository=Path(str(values["official_repository"])),
        vitaminc_archive=Path(str(values["vitaminc_archive"])),
        vitaminc_root=Path(str(values["vitaminc_root"])),
        m3_artifact_root=Path(str(values["m3_artifact_root"])),
        m4_12_config_path=Path(
            "configs/m4/public_ai/vitaminc_revision_gate_v1.json"
        ),
        m4_12_artifact_root=Path(str(values["m4_12_artifact_root"])),
        output_root=output,
        config_path=Path("configs/m4/verifier/change_aware_v1.json"),
    )
    assert result["terminal_reserve_manifest_sha256"] == (
        "3dcfcba0b809e3bcfcf2f9c036c4946f484d706faa61eec8c3fa489fbcb11dc5"
    )
    manifest = json.loads(
        (output / "prepared" / "dataset_manifest.json").read_text()
    )
    encoded = json.dumps(manifest["training_surface"], sort_keys=True)
    assert "terminal_reserve.jsonl" not in encoded
    assert manifest["sealed_terminal_reference"][
        "contains_terminal_row_ids_text_or_labels"
    ] is False
