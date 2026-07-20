from __future__ import annotations

import json
from pathlib import Path

import pytest

from experiments.m4_change_aware_verifier.provenance import (
    M4_12_FROZEN_IDENTITIES,
    TERMINAL_RESERVE_SHA256,
    verify_terminal_reserve,
)
from experiments.m4_change_aware_verifier.selection import (
    FROZEN_M3_TREE_SHA256,
    load_sealed_selection,
)
from groundloop.errors import ValidationError

from .helpers import digest, selection, trainer_provenance


def _selection_payload() -> dict[str, object]:
    checkpoints = []
    index = 0
    trainer_sha256, trainer_files = trainer_provenance()
    for variant in ("V2-ce-mix", "V3-margin-mix"):
        for seed in (20260720, 20260721, 20260722):
            checkpoints.append(
                {
                    "variant": variant,
                    "seed": seed,
                    "checkpoint_relative_path": f"runs/{variant}/{seed}/checkpoint",
                    "checkpoint_tree_sha256": digest(str(index + 1)),
                    "checkpoint_identity_sha256": digest(chr(ord("a") + index)),
                    "development_logits_path": (
                        f"development/{variant}:seed-{seed}/development_logits.jsonl"
                    ),
                    "development_logits_sha256": digest("d"),
                    "source_alignment_sha256": digest("e"),
                    "run_complete_sha256": digest("a"),
                    "training_manifest_sha256": digest("b"),
                    "batch_schedule_sha256": digest("c"),
                    "training_git_head": "1" * 40,
                    "training_repository_dirty": False,
                    "trainer_implementation_sha256": trainer_sha256,
                    "trainer_implementation_files_sha256": trainer_files,
                }
            )
            index += 1
    return {
        "schema_version": "groundloop-m4-13-selection-v1",
        "sealed": True,
        "terminal_data_accessed": False,
        "development_only": True,
        "selected_variant": "V3-margin-mix",
        "terminal_evaluation_authorized": True,
        "primary_seed": 20260720,
        "development_report": {
            "path": "development_report.json",
            "sha256": digest("a"),
        },
        "diagnostic_checkpoints": [
            {
                "variant": variant,
                "seed": 20260720,
                "checkpoint_tree_sha256": digest("a"),
                "checkpoint_identity_sha256": digest("b"),
                "development_logits_path": f"development/{variant}/logits.jsonl",
                "development_logits_sha256": digest("c"),
                "source_alignment_sha256": digest("e"),
                "run_complete_sha256": digest("a"),
                "training_manifest_sha256": digest("b"),
                "batch_schedule_sha256": digest("c"),
                "training_git_head": "1" * 40,
                "training_repository_dirty": False,
                "trainer_implementation_sha256": trainer_sha256,
                "trainer_implementation_files_sha256": trainer_files,
            }
            for variant in ("V1-replay-only", "A1-margin-no-replay")
        ],
        "candidate_checkpoints": checkpoints,
        "objective_design_verdict": {"verdict": "PAIRED_MARGIN_USEFUL"},
        "v0_m3_development": {
            "development_logits_path": (
                "development/V0-frozen-m3/development_logits.jsonl"
            ),
            "development_logits_sha256": digest("d"),
            "source_alignment_sha256": digest("e"),
        },
    }


def test_terminal_checkpoint_allow_list_is_sealed_and_exact(tmp_path: Path) -> None:
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(_selection_payload()), encoding="utf-8")
    frozen = load_sealed_selection(path)
    selected = frozen.selected_checkpoints[0]
    frozen.authorize(
        variant=selected.variant,
        seed=selected.seed,
        checkpoint_tree_sha256=selected.checkpoint_tree_sha256,
    )
    frozen.authorize(
        variant="V0-frozen-m3",
        seed=None,
        checkpoint_tree_sha256=FROZEN_M3_TREE_SHA256,
    )
    with pytest.raises(ValidationError, match="absent"):
        frozen.authorize(
            variant="V2-ce-mix",
            seed=20260720,
            checkpoint_tree_sha256=digest("e"),
        )


def test_unsealed_selection_is_rejected(tmp_path: Path) -> None:
    payload = _selection_payload()
    payload["sealed"] = False
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="not sealed"):
        load_sealed_selection(path)


def test_m4_12_consumed_manifest_cannot_unlock_terminal(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="pre-frozen"):
        verify_terminal_reserve(
            selection=selection(),
            final_test_manifest_sha256=M4_12_FROZEN_IDENTITIES[
                "sample_manifest_sha256"
            ],
            reserve_path=tmp_path / "not-read.jsonl",
            manifest_path=tmp_path / "not-read.json",
            identity_path=tmp_path / "not-read-identity.json",
        )


def test_arbitrary_terminal_hash_is_rejected_before_files_are_read(
    tmp_path: Path,
) -> None:
    assert TERMINAL_RESERVE_SHA256 != digest("0")
    with pytest.raises(ValidationError, match="pre-frozen"):
        verify_terminal_reserve(
            selection=selection(),
            final_test_manifest_sha256=digest("0"),
            reserve_path=tmp_path / "not-read.jsonl",
            manifest_path=tmp_path / "not-read.json",
            identity_path=tmp_path / "not-read-identity.json",
        )
