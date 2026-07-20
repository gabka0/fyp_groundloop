from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path

import pytest
from m4_13_verifier.calibrate import calibrate_selected_checkpoint
from m4_13_verifier.train import (
    BASE_LOGIT_ORDER,
    STORED_LABEL_ORDER,
    _validate_dataset_manifest,
    load_vitaminc_training_rows,
    train_variant,
)

from groundloop.ai.verification.artifacts import file_sha256
from groundloop.errors import ValidationError


def _write_training_surface(prepared: Path, config_path: Path) -> dict[str, object]:
    names = (
        "train_vitaminc.jsonl",
        "train_vitaminc_manifest.json",
        "train_m3_replay.jsonl",
        "development_vitaminc.jsonl",
        "development_vitaminc_manifest.json",
        "development_m3.jsonl",
    )
    for name in names:
        (prepared / name).write_text(f"fixture:{name}\n", encoding="utf-8")
    return {
        "schema_version": "groundloop-m4-13-dataset-manifest-v1",
        "config_sha256": file_sha256(config_path),
        "label_mapping": {
            "SUPPORTS": {"stored": "support", "base_logit_index": 1},
            "REFUTES": {"stored": "refute", "base_logit_index": 0},
            "NOT ENOUGH INFO": {"stored": "neutral", "base_logit_index": 2},
            "base_logit_order": list(BASE_LOGIT_ORDER),
            "stored_probability_order": list(STORED_LABEL_ORDER),
        },
        "sealed_terminal_reference": {
            "rows": 512,
            "manifest_sha256": "a" * 64,
            "normalized_page_exclusion_digest_sha256": "b" * 64,
        },
        "training_surface": {
            "accepts_test_path": False,
            "terminal_paths_exposed": False,
            "vitaminc_row_schema": "groundloop-m4-13-vitaminc-row-v1",
            "artifacts": {name: file_sha256(prepared / name) for name in names},
        },
    }


def test_dataset_manifest_rejects_hash_drift_and_terminal_content(
    tmp_path: Path,
) -> None:
    prepared = tmp_path / "prepared"
    prepared.mkdir()
    config = tmp_path / "config.json"
    config.write_text("{}\n", encoding="utf-8")
    manifest = _write_training_surface(prepared, config)
    _validate_dataset_manifest(manifest, config_path=config, prepared=prepared)

    (prepared / "development_m3.jsonl").write_text("drift\n", encoding="utf-8")
    with pytest.raises(ValidationError, match="artifact hash drifted"):
        _validate_dataset_manifest(manifest, config_path=config, prepared=prepared)
    (prepared / "development_m3.jsonl").write_text(
        "fixture:development_m3.jsonl\n", encoding="utf-8"
    )
    terminal = dict(manifest)
    terminal["sealed_terminal_reference"] = {
        "rows": 512,
        "path": "/secret/reserve.jsonl",
    }
    with pytest.raises(ValidationError, match="terminal reserve content"):
        _validate_dataset_manifest(terminal, config_path=config, prepared=prepared)


def test_vitaminc_loader_rejects_source_label_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "train_vitaminc.jsonl"
    rows = []
    layout = (
        (1, "claim a", "evidence a", "SUPPORTS", "refute"),
        (2, "claim a", "evidence b", "REFUTES", "refute"),
        (3, "claim b", "evidence a", "REFUTES", "refute"),
        (4, "claim b", "evidence b", "SUPPORTS", "support"),
    )
    for suffix, claim, evidence, source_label, stored_label in layout:
        rows.append(
            {
                "schema_version": "groundloop-m4-13-vitaminc-row-v1",
                "split": "train",
                "stratum": "support_refute",
                "selection_key": "s" * 64,
                "unique_id": f"case_1_{suffix}",
                "case_id": "case_1",
                "page": "page",
                "normalized_page_sha256": "n" * 64,
                "wiki_revision_id": "1",
                "revision_type": "real",
                "source_label": source_label,
                "label": stored_label,
                "claim": claim,
                "evidence": evidence,
                "claim_sha256": hashlib.sha256(claim.encode()).hexdigest(),
                "evidence_sha256": hashlib.sha256(evidence.encode()).hexdigest(),
            }
        )
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    with pytest.raises(ValidationError, match="label mapping drifted"):
        load_vitaminc_training_rows(path)


def test_training_and_calibration_interfaces_have_no_terminal_input() -> None:
    for callable_value in (train_variant, calibrate_selected_checkpoint):
        parameter_names = {
            name.casefold() for name in inspect.signature(callable_value).parameters
        }
        assert not any("test" in name or "terminal" in name for name in parameter_names)
