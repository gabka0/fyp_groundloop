from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from experiments.m4_change_aware_verifier import artifacts
from experiments.m4_change_aware_verifier.artifacts import (
    _FROZEN_HYPERPARAMETERS,
    _FROZEN_OPTIMIZER_SCHEDULE,
    _FROZEN_TRAINING_INPUTS,
    _validate_calibration,
    build_development_models_manifest,
    load_development_models_manifest,
)
from experiments.m4_change_aware_verifier.common import (
    canonical_sha256,
    file_sha256,
    write_canonical_json,
)
from experiments.m4_change_aware_verifier.selection import FROZEN_M3_TREE_SHA256
from groundloop.ai.verification.artifacts import tree_digest
from groundloop.errors import ValidationError

from .helpers import selection, trainer_provenance

RUNS = tuple(
    (variant, seed)
    for variant, seeds in (
        ("V1-replay-only", (20260720,)),
        ("V2-ce-mix", (20260720, 20260721, 20260722)),
        ("V3-margin-mix", (20260720, 20260721, 20260722)),
        ("A1-margin-no-replay", (20260720,)),
    )
    for seed in seeds
)


def _make_run(
    root: Path,
    *,
    variant: str,
    seed: int,
    checkpoint_token: str,
    schedule_token: str,
) -> None:
    run = root / "runs" / variant / str(seed)
    checkpoint = run / "checkpoint"
    checkpoint.mkdir(parents=True)
    (checkpoint / "model.safetensors").write_bytes(checkpoint_token.encode())
    tree = tree_digest(checkpoint)
    weights = file_sha256(checkpoint / "model.safetensors")
    identity_path = run / "checkpoint_identity.json"
    write_canonical_json(
        identity_path,
        {
            "schema_version": "groundloop-m4-13-checkpoint-identity-v1",
            "checkpoint_tree_sha256": tree,
            "weights_sha256": weights,
            "files": [],
        },
    )
    family = (
        "mixed"
        if variant in {"V2-ce-mix", "V3-margin-mix"}
        else "m3"
        if variant == "V1-replay-only"
        else "vitaminc"
    )
    domains = ["vitaminc"] * 512 + ["m3"] * 378 if family == "mixed" else [family] * 890
    batches = []
    batch_payload = []
    boundaries = []
    label_counts = [0, 0, 0]
    for index, domain in enumerate(domains):
        row_count = 8
        rows = []
        hashed_rows = []
        for row_index in range(row_count):
            base_label = (index * row_count + row_index) % 3
            label_counts[base_label] += 1
            row_id = f"{schedule_token}-{domain}-{index}-{row_index}"
            group_id = f"{schedule_token}-{domain}-group-{index}-{row_index // 4}"
            rows.append(
                {
                    "row_id": row_id,
                    "group_id": group_id,
                    "base_label": base_label,
                    "repeat_index": 0,
                }
            )
            hashed_rows.append([row_id, group_id, 0])
        transitions = [[0, 1], [2, 3], [4, 5], [6, 7]] if domain == "vitaminc" else []
        optimizer_step = index // 4
        divisor = 2 if optimizer_step == 222 else 4
        closes = (index + 1) % 4 == 0 or index == 889
        batches.append(
            {
                "batch_index": index,
                "domain": domain,
                "optimizer_step": optimizer_step,
                "accumulation_divisor": divisor,
                "closes_optimizer_step": closes,
                "transitions": transitions,
                "rows": rows,
            }
        )
        batch_payload.append(
            {"domain": domain, "transitions": transitions, "rows": hashed_rows}
        )
        boundaries.append([index, optimizer_step, divisor, closes])
    class_weights = [
        sum(label_counts) / (3.0 * label_counts[index]) for index in range(3)
    ]
    batch_hash = canonical_sha256(batch_payload)
    optimizer_hash = canonical_sha256(
        {
            "gradient_accumulation_steps": 4,
            "optimizer_steps": 223,
            "warmup_steps": 13,
            "boundaries": boundaries,
        }
    )
    schedule_hash = canonical_sha256(
        {
            "seed": seed,
            "family": family,
            "batch_order_sha256": batch_hash,
            "optimizer_schedule_sha256": optimizer_hash,
            "class_weights": class_weights,
        }
    )
    schedule = {
        "schema_version": "groundloop-m4-13-batch-schedule-v1",
        "seed": seed,
        "schedule_family": (
            "mixed-vitaminc-m3-v1" if family == "mixed" else f"cycled-{family}-v1"
        ),
        "microbatches": 890,
        "optimizer_steps": 223,
        "gradient_accumulation_steps": 4,
        "base_logit_order": ["contradiction", "entailment", "neutral"],
        "stored_label_order": ["support", "refute", "neutral"],
        "batch_order_sha256": batch_hash,
        "optimizer_schedule_sha256": optimizer_hash,
        "schedule_sha256": schedule_hash,
        "base_order_class_weights": class_weights,
        "batches": batches,
    }
    schedule_path = run / "batch_schedule.json"
    write_canonical_json(schedule_path, schedule)
    objective = {
        "endpoint": "weighted-endpoint-ce-arithmetic-mean-v1",
        "paired": (
            "symmetric-log-probability-margin-v1"
            if variant in {"V3-margin-mix", "A1-margin-no-replay"}
            else None
        ),
        "margin": 0.5,
        "paired_weight": 0.25,
    }
    hyperparameters = _FROZEN_HYPERPARAMETERS
    trainer_sha256, trainer_files = trainer_provenance()
    repository = {"git_head": "1" * 40, "dirty": False}
    trainer = {
        "schema_version": "groundloop-m4-13-trainer-implementation-v1",
        "files_sha256": trainer_files,
        "sha256": trainer_sha256,
    }
    invocation_payload = {
        "schema_version": "groundloop-m4-13-training-invocation-v1",
        "variant": variant,
        "seed": seed,
        **_FROZEN_TRAINING_INPUTS,
        "schedule_sha256": schedule["schedule_sha256"],
        "objective": objective,
        "hyperparameters": hyperparameters,
        "optimizer_schedule": _FROZEN_OPTIMIZER_SCHEDULE,
        "repository": repository,
        "trainer_implementation": trainer,
    }
    invocation = canonical_sha256(invocation_payload)
    manifest_path = run / "training_manifest.json"
    write_canonical_json(
        manifest_path,
        {
            **invocation_payload,
            "schema_version": "groundloop-m4-13-training-run-v1",
            "status": "complete",
            "invocation_sha256": invocation,
            "checkpoint_tree_sha256": tree,
            "weights_sha256": weights,
            "microbatches": 890,
            "optimizer_steps": 223,
            "batch_order_sha256": schedule["batch_order_sha256"],
            "optimizer_schedule_sha256": schedule["optimizer_schedule_sha256"],
            "base_order_class_weights": schedule["base_order_class_weights"],
        },
    )
    runtime_path = run / "runtime.json"
    write_canonical_json(
        runtime_path,
        {
            "schema_version": "groundloop-m4-13-training-runtime-v1",
            "dependencies": {
                "python": "3.12-test",
                "torch": "test",
                "transformers": "test",
                "tokenizers": "test",
                "safetensors": "test",
                "numpy": "test",
            },
            "hardware": {
                "platform": "test",
                "processor": "test",
                "logical_cpus": 8,
                "torch_threads": 8,
                "torch_interop_threads": 1,
                "cuda_available": False,
            },
        },
    )
    write_canonical_json(
        run / "run_complete.json",
        {
            "schema_version": "groundloop-m4-13-training-complete-v1",
            "status": "complete",
            "invocation_sha256": invocation,
            "training_manifest_sha256": file_sha256(manifest_path),
            "checkpoint_identity_sha256": file_sha256(identity_path),
            "batch_schedule_file_sha256": file_sha256(schedule_path),
            "runtime_file_sha256": file_sha256(runtime_path),
        },
    )


def _make_artifact_root(
    root: Path, *, duplicate_checkpoint: bool = False, mismatched_pair: bool = False
) -> None:
    for index, (variant, seed) in enumerate(RUNS):
        schedule_token = (
            str(seed) if variant in {"V2-ce-mix", "V3-margin-mix"} else variant
        )
        if mismatched_pair and variant == "V3-margin-mix" and seed == 20260720:
            schedule_token = "mismatch"
        checkpoint_token = str(index)
        if duplicate_checkpoint and index == 1:
            checkpoint_token = "0"
        _make_run(
            root,
            variant=variant,
            seed=seed,
            checkpoint_token=checkpoint_token,
            schedule_token=schedule_token,
        )


def _manifest_with_fake_v0(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **root_options: bool
) -> Path:
    artifact_root = tmp_path / "artifacts"
    _make_artifact_root(artifact_root, **root_options)
    v0 = tmp_path / "v0"
    v0.mkdir()
    (v0 / "model.safetensors").write_bytes(b"v0")
    actual_tree_digest = tree_digest
    monkeypatch.setattr(
        artifacts,
        "tree_digest",
        lambda path: (
            FROZEN_M3_TREE_SHA256
            if Path(path) == v0
            else actual_tree_digest(Path(path))
        ),
    )
    manifest = tmp_path / "development_models.json"
    build_development_models_manifest(
        manifest, artifact_root=artifact_root, v0_checkpoint=v0
    )
    return manifest


def test_development_models_are_derived_from_all_completed_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    models = load_development_models_manifest(
        _manifest_with_fake_v0(tmp_path, monkeypatch)
    )
    assert len(models) == 9
    assert {(model.variant, model.seed) for model in models[1:]} == set(RUNS)


def test_checkpoint_substitution_across_runs_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = _manifest_with_fake_v0(tmp_path, monkeypatch, duplicate_checkpoint=True)
    with pytest.raises(ValidationError, match="substituted"):
        load_development_models_manifest(manifest)


def test_v2_v3_schedule_mismatch_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = _manifest_with_fake_v0(tmp_path, monkeypatch, mismatched_pair=True)
    with pytest.raises(ValidationError, match="paired schedule"):
        load_development_models_manifest(manifest)


def test_calibration_copied_from_another_seed_is_rejected(tmp_path: Path) -> None:
    selection_path = tmp_path / "selection.json"
    write_canonical_json(selection_path, {"sealed": True})
    frozen = replace(
        selection(),
        path=selection_path,
        file_sha256=file_sha256(selection_path),
    )
    checkpoint = frozen.selected_checkpoints[0]
    semantic = {
        "selection_sha256": frozen.file_sha256,
        "selected": True,
        "variant": checkpoint.variant,
        "seed": checkpoint.seed + 1,
        "checkpoint_tree_sha256": checkpoint.checkpoint_tree_sha256,
        "checkpoint_identity_sha256": checkpoint.checkpoint_identity_sha256,
        "dataset_manifest_sha256": "1" * 64,
        "m3_development_jsonl_sha256": "2" * 64,
        "vitaminc_development_manifest_sha256": "3" * 64,
        "development_logits_sha256": checkpoint.development_logits_sha256,
        "source_alignment_sha256": checkpoint.source_alignment_sha256,
        "method": "test",
        "group_weighting": {},
        "search": {},
        "candidate_temperature": 2.0,
        "deployed_temperature": 1.0,
        "accepted": False,
        "rejection_reasons": ["test rejection"],
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
    )
    payload = {
        **semantic,
        "schema_version": "groundloop-m4-13-group-balanced-temperature-v1",
        "status": "complete",
        "invocation_sha256": canonical_sha256(
            {
                "schema_version": "groundloop-m4-13-calibration-invocation-v1",
                **{key: semantic[key] for key in invocation_keys},
            }
        ),
        "calibration_version": (
            "temperature-m4-13-v1:"
            + canonical_sha256(
                {
                    "schema_version": ("groundloop-m4-13-calibration-invocation-v1"),
                    **semantic,
                }
            )
        ),
    }
    path = tmp_path / "calibration.json"
    write_canonical_json(path, payload)
    with pytest.raises(ValidationError, match="calibration seed"):
        _validate_calibration(
            path=path,
            selection=frozen,
            checkpoint=checkpoint,
        )
