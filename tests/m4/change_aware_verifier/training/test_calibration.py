from __future__ import annotations

import hashlib
import json
import math
import shutil
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import pytest
from m4_13_verifier.calibrate import (
    CALIBRATION_SCHEMA,
    CALIBRATOR_IMPLEMENTATION_SCHEMA,
    CalibrationFailureStage,
    DevelopmentLogit,
    _development_source_alignment_sha256,
    _selection_allows,
    _validate_development_identities,
    _write_calibration_atomically,
    calibrate_selected_checkpoint,
    fit_group_balanced_temperature,
    group_balanced_nll,
)
from m4_13_verifier.calibrate import _canonical_sha256 as _calibration_sha256
from m4_13_verifier.train import seal_training_run

from groundloop.ai.verification.artifacts import file_sha256
from groundloop.errors import ValidationError

CALIBRATOR_IMPLEMENTATION_PATHS = (
    "training/m4_13_verifier/calibrate.py",
    "training/m4_13_verifier/losses.py",
    "training/m4_13_verifier/train.py",
    "training/m4_13_verifier/__init__.py",
    "src/groundloop/ai/verification/artifacts.py",
    "src/groundloop/errors.py",
)
SOURCE_ROOT = Path(__file__).resolve().parents[4]


def _clean_calibrator_repository(root: Path) -> Path:
    root.mkdir()
    for relative in CALIBRATOR_IMPLEMENTATION_PATHS:
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(SOURCE_ROOT / relative, destination)
    (root / ".gitignore").write_text("*.ignored\n", encoding="utf-8")
    for arguments in (
        ("init", "--quiet"),
        ("config", "user.email", "fixture@example.invalid"),
        ("config", "user.name", "GroundLoop Fixture"),
        ("add", "."),
        ("commit", "--quiet", "-m", "fixture"),
    ):
        subprocess.run(
            ("git", "-C", str(root), *arguments),
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    return root


def _row(
    domain: str,
    row_id: str,
    group_id: str,
    label: str,
    logits: tuple[float, float, float],
) -> DevelopmentLogit:
    assert domain in {"m3", "vitaminc"}
    return DevelopmentLogit(  # type: ignore[arg-type]
        domain,
        row_id,
        group_id,
        label,
        "a" * 64,
        "b" * 64,
        logits,
        "c" * 64,
        "V3-margin-mix",
        20260720,
    )


def _nll(logits: tuple[float, float, float], base_label: int) -> float:
    maximum = max(logits)
    return (
        maximum
        + math.log(sum(math.exp(item - maximum) for item in logits))
        - logits[base_label]
    )


def test_nll_is_equal_domain_and_group_balanced_not_row_pooled() -> None:
    rows = (
        _row("m3", "m1", "large", "support", (0.0, 2.0, 0.0)),
        _row("m3", "m2", "large", "support", (0.0, 1.0, 0.0)),
        _row("m3", "m3", "small", "refute", (0.5, 0.0, 0.0)),
        _row("vitaminc", "v1", "case", "neutral", (0.0, 0.0, 1.5)),
        _row("vitaminc", "v2", "case", "support", (0.0, 1.5, 0.0)),
    )
    measured = group_balanced_nll(rows, 1.0)
    expected_m3 = 0.5 * (
        0.5 * (_nll((0.0, 2.0, 0.0), 1) + _nll((0.0, 1.0, 0.0), 1))
        + _nll((0.5, 0.0, 0.0), 0)
    )
    expected_vitamin = 0.5 * (_nll((0.0, 0.0, 1.5), 2) + _nll((0.0, 1.5, 0.0), 1))
    assert measured.m3 == pytest.approx(expected_m3)
    assert measured.vitaminc == pytest.approx(expected_vitamin)
    assert measured.combined == pytest.approx(0.5 * (expected_m3 + expected_vitamin))


def test_golden_temperature_is_deterministic_and_obeys_domain_gate() -> None:
    rows = (
        _row("m3", "m1", "g1", "support", (0.0, 1.0, 0.0)),
        _row("m3", "m2", "g2", "refute", (1.0, 0.0, 0.0)),
        _row("vitaminc", "v1", "c1", "neutral", (0.0, 0.0, 1.0)),
        _row("vitaminc", "v2", "c2", "support", (0.0, 1.0, 0.0)),
    )
    first = fit_group_balanced_temperature(rows)
    second = fit_group_balanced_temperature(rows)
    assert first == second
    assert first.accepted
    assert first.deployed_temperature == first.candidate_temperature
    assert first.nll_candidate.combined < first.nll_before.combined
    assert first.nll_candidate.m3 <= first.nll_before.m3 + 0.01
    assert first.nll_candidate.vitaminc <= first.nll_before.vitaminc + 0.01


def test_selection_must_uniquely_allow_exact_selected_checkpoint(
    tmp_path: Path,
) -> None:
    logits = tmp_path / "development" / "candidate.jsonl"
    logits.parent.mkdir()
    logits.write_text("fixture\n", encoding="utf-8")
    selection = {
        "schema_version": "groundloop-m4-13-selection-v1",
        "sealed": True,
        "selected_variant": "V3-margin-mix",
        "primary_seed": 20260720,
        "candidate_checkpoints": [
            {
                "variant": "V3-margin-mix",
                "seed": 20260720,
                "checkpoint_tree_sha256": "c" * 64,
                "checkpoint_identity_sha256": "b" * 64,
                "development_logits_path": "development/candidate.jsonl",
                "development_logits_sha256": "d" * 64,
                "source_alignment_sha256": "e" * 64,
            }
        ],
    }
    _selection_allows(
        selection,
        variant="V3-margin-mix",
        seed=20260720,
        checkpoint_tree_sha256="c" * 64,
        checkpoint_identity_sha256="b" * 64,
        selection_directory=tmp_path,
        development_logits_path=logits,
        development_logits_sha256="d" * 64,
        source_alignment_sha256="e" * 64,
    )
    selection["selected_variant"] = "V2-ce-mix"
    with pytest.raises(ValidationError, match="not selected"):
        _selection_allows(
            selection,
            variant="V3-margin-mix",
            seed=20260720,
            checkpoint_tree_sha256="c" * 64,
            checkpoint_identity_sha256="b" * 64,
            selection_directory=tmp_path,
            development_logits_path=logits,
            development_logits_sha256="d" * 64,
            source_alignment_sha256="e" * 64,
        )


@pytest.mark.parametrize(
    ("actual_logits", "actual_alignment"),
    (("f" * 64, "e" * 64), ("d" * 64, "f" * 64)),
)
def test_selection_rejects_post_selection_logit_or_source_substitution(
    tmp_path: Path, actual_logits: str, actual_alignment: str
) -> None:
    logits = tmp_path / "development" / "candidate.jsonl"
    logits.parent.mkdir()
    logits.write_text("fixture\n", encoding="utf-8")
    selection = {
        "schema_version": "groundloop-m4-13-selection-v1",
        "sealed": True,
        "selected_variant": "V3-margin-mix",
        "primary_seed": 20260720,
        "candidate_checkpoints": [
            {
                "variant": "V3-margin-mix",
                "seed": 20260720,
                "checkpoint_tree_sha256": "c" * 64,
                "checkpoint_identity_sha256": "b" * 64,
                "development_logits_path": "development/candidate.jsonl",
                "development_logits_sha256": "d" * 64,
                "source_alignment_sha256": "e" * 64,
            }
        ],
    }
    with pytest.raises(ValidationError, match="not uniquely allow-listed"):
        _selection_allows(
            selection,
            variant="V3-margin-mix",
            seed=20260720,
            checkpoint_tree_sha256="c" * 64,
            checkpoint_identity_sha256="b" * 64,
            selection_directory=tmp_path,
            development_logits_path=logits,
            development_logits_sha256=actual_logits,
            source_alignment_sha256=actual_alignment,
        )


@pytest.mark.parametrize("stage", list(CalibrationFailureStage))
def test_calibration_publish_is_failure_atomic(
    tmp_path: Path, stage: CalibrationFailureStage
) -> None:
    path = tmp_path / "calibration.json"
    with pytest.raises(RuntimeError, match="injected failure"):
        _write_calibration_atomically(
            path,
            {
                "schema_version": CALIBRATION_SCHEMA,
                "status": "complete",
                "invocation_sha256": "a" * 64,
            },
            expected_invocation_sha256="a" * 64,
            failure_stage=stage,
        )
    assert not path.exists()
    assert not list(tmp_path.glob(".calibration.json.partial-*"))


def test_existing_partial_calibration_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "calibration.json"
    path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValidationError, match="partial"):
        _write_calibration_atomically(
            path,
            {
                "schema_version": CALIBRATION_SCHEMA,
                "status": "complete",
                "invocation_sha256": "a" * 64,
            },
            expected_invocation_sha256="a" * 64,
            failure_stage=None,
        )


def test_calibration_replay_requires_the_full_canonical_payload(
    tmp_path: Path,
) -> None:
    path = tmp_path / "calibration.json"
    payload = {
        "schema_version": CALIBRATION_SCHEMA,
        "status": "complete",
        "invocation_sha256": "a" * 64,
        "candidate_temperature": 1.25,
        "calibration_version": "temperature-m4-13-v1:fixture",
    }
    first = _write_calibration_atomically(
        path,
        payload,
        expected_invocation_sha256="a" * 64,
        failure_stage=None,
    )
    assert _write_calibration_atomically(
        path,
        payload,
        expected_invocation_sha256="a" * 64,
        failure_stage=None,
    ) == first

    drifted = dict(payload)
    drifted["candidate_temperature"] = 3.0
    with pytest.raises(ValidationError, match="partial or belongs to another run"):
        _write_calibration_atomically(
            path,
            drifted,
            expected_invocation_sha256="a" * 64,
            failure_stage=None,
        )


def test_calibration_writer_rejects_inconsistent_requested_payload(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValidationError, match="not self-consistent"):
        _write_calibration_atomically(
            tmp_path / "calibration.json",
            {
                "schema_version": CALIBRATION_SCHEMA,
                "status": "complete",
                "invocation_sha256": "b" * 64,
            },
            expected_invocation_sha256="a" * 64,
            failure_stage=None,
        )


def test_development_identity_check_rejects_same_count_substitution(
    tmp_path: Path,
) -> None:
    prepared = tmp_path / "prepared"
    prepared.mkdir()
    vitamin_claim = "vitamin claim"
    vitamin_evidence = "vitamin evidence"
    m3_claim = "m3 claim"
    m3_evidence = "m3 evidence"
    (prepared / "development_vitaminc.jsonl").write_text(
        json.dumps(
            {
                "split": "development",
                "unique_id": "v1",
                "case_id": "case-1",
                "label": "support",
                "claim_sha256": hashlib.sha256(vitamin_claim.encode()).hexdigest(),
                "evidence_sha256": hashlib.sha256(
                    vitamin_evidence.encode()
                ).hexdigest(),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (prepared / "development_m3.jsonl").write_text(
        json.dumps(
            {
                "split": "development",
                "example_id": "m1",
                "claim_group_id": "group-1",
                "label": "neutral",
                "claim": m3_claim,
                "evidence": m3_evidence,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    rows = (
        DevelopmentLogit(
            "vitaminc",
            "v1",
            "case-1",
            "support",
            hashlib.sha256(vitamin_claim.encode()).hexdigest(),
            hashlib.sha256(vitamin_evidence.encode()).hexdigest(),
            (0.0, 1.0, 0.0),
            "c" * 64,
            "V3-margin-mix",
            20260720,
        ),
        DevelopmentLogit(
            "m3",
            "m1",
            "group-1",
            "neutral",
            hashlib.sha256(m3_claim.encode()).hexdigest(),
            hashlib.sha256(m3_evidence.encode()).hexdigest(),
            (0.0, 0.0, 1.0),
            "c" * 64,
            "V3-margin-mix",
            20260720,
        ),
    )
    _validate_development_identities(rows, prepared)
    substituted = (
        DevelopmentLogit(
            "vitaminc",
            "same-count-substitute",
            rows[0].group_id,
            rows[0].stored_label,
            rows[0].claim_sha256,
            rows[0].evidence_sha256,
            rows[0].logits,
            rows[0].checkpoint_tree_sha256,
            rows[0].variant,
            rows[0].seed,
        ),
        rows[1],
    )
    with pytest.raises(ValidationError, match="exactly match"):
        _validate_development_identities(substituted, prepared)


def test_calibrate_selected_checkpoint_seals_real_schema_end_to_end(
    tmp_path: Path,
) -> None:
    repository_root = _clean_calibrator_repository(tmp_path / "repository")
    artifact_root = tmp_path / "artifacts"
    prepared = artifact_root / "prepared"
    prepared.mkdir(parents=True)
    config_path = tmp_path / "config.json"

    vitamin_claim = "Vitamin claim"
    vitamin_evidence = "Vitamin evidence"
    vitamin_row = {
        "schema_version": "groundloop-m4-13-vitaminc-row-v1",
        "split": "development",
        "stratum": "support_refute",
        "unique_id": "case-1_1",
        "case_id": "case-1",
        "normalized_page_sha256": "1" * 64,
        "label": "support",
        "claim": vitamin_claim,
        "evidence": vitamin_evidence,
        "claim_sha256": hashlib.sha256(vitamin_claim.encode()).hexdigest(),
        "evidence_sha256": hashlib.sha256(vitamin_evidence.encode()).hexdigest(),
    }
    m3_claim = "M3 claim"
    m3_evidence = "M3 evidence"
    m3_row = {
        "split": "development",
        "example_id": "m3-1",
        "claim_group_id": "group-1",
        "label": "neutral",
        "claim": m3_claim,
        "evidence": m3_evidence,
    }
    (prepared / "development_vitaminc.jsonl").write_text(
        json.dumps(vitamin_row) + "\n", encoding="utf-8"
    )
    (prepared / "development_m3.jsonl").write_text(
        json.dumps(m3_row) + "\n", encoding="utf-8"
    )
    for name in (
        "train_vitaminc.jsonl",
        "train_vitaminc_manifest.json",
        "train_m3_replay.jsonl",
        "development_vitaminc_manifest.json",
    ):
        (prepared / name).write_text(f"fixture:{name}\n", encoding="utf-8")

    config = {
        "schema_version": "groundloop-m4-change-aware-data-config-v1",
        "dataset": {
            "samples": {
                "development": {
                    "rows": 1,
                    "cases": 1,
                    "manifest_sha256": file_sha256(
                        prepared / "development_vitaminc_manifest.json"
                    ),
                }
            }
        },
        "m3": {
            "development": {
                "rows": 1,
                "claim_groups": 1,
                "sha256": file_sha256(prepared / "development_m3.jsonl"),
            }
        },
    }
    config_path.write_text(json.dumps(config) + "\n", encoding="utf-8")
    artifact_names = (
        "train_vitaminc.jsonl",
        "train_vitaminc_manifest.json",
        "train_m3_replay.jsonl",
        "development_vitaminc.jsonl",
        "development_vitaminc_manifest.json",
        "development_m3.jsonl",
    )
    dataset_manifest = {
        "schema_version": "groundloop-m4-13-dataset-manifest-v1",
        "config_sha256": file_sha256(config_path),
        "label_mapping": {
            "SUPPORTS": {"stored": "support", "base_logit_index": 1},
            "REFUTES": {"stored": "refute", "base_logit_index": 0},
            "NOT ENOUGH INFO": {"stored": "neutral", "base_logit_index": 2},
            "base_logit_order": ["contradiction", "entailment", "neutral"],
            "stored_probability_order": ["support", "refute", "neutral"],
        },
        "sealed_terminal_reference": {"rows": 2},
        "training_surface": {
            "accepts_test_path": False,
            "terminal_paths_exposed": False,
            "vitaminc_row_schema": "groundloop-m4-13-vitaminc-row-v1",
            "artifacts": {
                name: file_sha256(prepared / name) for name in artifact_names
            },
        },
    }
    dataset_manifest_path = prepared / "dataset_manifest.json"
    dataset_manifest_path.write_text(
        json.dumps(dataset_manifest) + "\n", encoding="utf-8"
    )

    run_directory = artifact_root / "runs" / "V3-margin-mix" / "20260720"

    def checkpoint_writer(path: Path) -> None:
        path.mkdir()
        (path / "model.safetensors").write_bytes(b"candidate")

    completed = seal_training_run(
        run_directory=run_directory,
        invocation_sha256="a" * 64,
        training_manifest={"variant": "V3-margin-mix", "seed": 20260720},
        schedule_manifest={"schema_version": "fixture-schedule"},
        runtime_manifest={"schema_version": "fixture-runtime"},
        checkpoint_writer=checkpoint_writer,
    )
    checkpoint_tree = str(completed.checkpoint_identity["checkpoint_tree_sha256"])
    checkpoint_identity_sha256 = file_sha256(run_directory / "checkpoint_identity.json")

    development = artifact_root / "development" / "candidate.jsonl"
    development.parent.mkdir()
    development_rows = (
        {
            "schema_version": "groundloop-m4-13-development-logit-v1",
            "split": "development",
            "domain": "vitaminc",
            "row_id": "case-1_1",
            "group_id": "case-1",
            "label": "support",
            "claim_sha256": vitamin_row["claim_sha256"],
            "evidence_sha256": vitamin_row["evidence_sha256"],
            "base_logit_order": ["contradiction", "entailment", "neutral"],
            "logits": [0.0, 2.0, 0.0],
            "checkpoint_tree_sha256": checkpoint_tree,
            "variant": "V3-margin-mix",
            "seed": 20260720,
        },
        {
            "schema_version": "groundloop-m4-13-development-logit-v1",
            "split": "development",
            "domain": "m3",
            "row_id": "m3-1",
            "group_id": "group-1",
            "label": "neutral",
            "claim_sha256": hashlib.sha256(m3_claim.encode()).hexdigest(),
            "evidence_sha256": hashlib.sha256(m3_evidence.encode()).hexdigest(),
            "base_logit_order": ["contradiction", "entailment", "neutral"],
            "logits": [0.0, 0.0, 2.0],
            "checkpoint_tree_sha256": checkpoint_tree,
            "variant": "V3-margin-mix",
            "seed": 20260720,
        },
    )
    development.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in development_rows),
        encoding="utf-8",
    )
    alignment = _development_source_alignment_sha256(
        prepared=prepared, dataset_manifest_path=dataset_manifest_path
    )
    selection_path = artifact_root / "selection.json"
    selection_path.write_text(
        json.dumps(
            {
                "schema_version": "groundloop-m4-13-selection-v1",
                "sealed": True,
                "selected_variant": "V3-margin-mix",
                "primary_seed": 20260720,
                "candidate_checkpoints": [
                    {
                        "variant": "V3-margin-mix",
                        "seed": 20260720,
                        "checkpoint_tree_sha256": checkpoint_tree,
                        "checkpoint_identity_sha256": checkpoint_identity_sha256,
                        "development_logits_path": "development/candidate.jsonl",
                        "development_logits_sha256": file_sha256(development),
                        "source_alignment_sha256": alignment,
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    result = calibrate_selected_checkpoint(
        artifact_root=artifact_root,
        config_path=config_path,
        run_directory=run_directory,
        selection_path=selection_path,
        development_logits_path=development,
        repository_root=repository_root,
    )
    assert result["schema_version"] == CALIBRATION_SCHEMA
    assert result["status"] == "complete"
    assert result["development_logits_sha256"] == file_sha256(development)
    assert result["source_alignment_sha256"] == alignment
    expected_files = {
        relative: file_sha256(repository_root / relative)
        for relative in CALIBRATOR_IMPLEMENTATION_PATHS
    }
    expected_implementation_sha256 = _calibration_sha256(
        {
            "schema_version": CALIBRATOR_IMPLEMENTATION_SCHEMA,
            "files_sha256": expected_files,
        }
    )
    assert result["repository"] == {
        "git_head": subprocess.run(
            ("git", "-C", str(repository_root), "rev-parse", "HEAD"),
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        ).stdout.strip(),
        "dirty": False,
    }
    assert result["calibrator_implementation"] == {
        "schema_version": CALIBRATOR_IMPLEMENTATION_SCHEMA,
        "files_sha256": expected_files,
        "sha256": expected_implementation_sha256,
    }
    invocation_keys = (
        "selection_sha256",
        "selected",
        "variant",
        "seed",
        "checkpoint_tree_sha256",
        "checkpoint_identity_sha256",
        "dataset_manifest_sha256",
        "m3_development_jsonl_sha256",
        "vitaminc_development_manifest_sha256",
        "development_logits_sha256",
        "source_alignment_sha256",
        "method",
        "group_weighting",
        "search",
        "repository",
        "calibrator_implementation",
    )
    invocation = {
        "schema_version": "groundloop-m4-13-calibration-invocation-v1",
        **{key: result[key] for key in invocation_keys},
    }
    assert result["invocation_sha256"] == _calibration_sha256(invocation)
    drifted_implementation = dict(
        cast(Mapping[str, object], result["calibrator_implementation"])
    )
    drifted_implementation["sha256"] = "f" * 64
    drifted_invocation = dict(invocation)
    drifted_invocation["calibrator_implementation"] = {
        **drifted_implementation,
    }
    assert _calibration_sha256(drifted_invocation) != result["invocation_sha256"]
