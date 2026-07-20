from __future__ import annotations

import json
from pathlib import Path

import pytest
from m4_13_verifier.train import (
    FailureStage,
    TrainingExample,
    TrainingVariant,
    assert_v2_v3_schedule_identity,
    build_training_schedule,
    load_completed_training_run,
    seal_training_run,
    validate_continuation_checkpoint,
)

from groundloop.ai.verification.artifacts import file_sha256, tree_digest
from groundloop.errors import ValidationError


def _vitaminc_case(case_index: int) -> tuple[TrainingExample, ...]:
    case_id = f"case-{case_index}"
    first_other = "refute" if case_index % 2 == 0 else "neutral"
    return (
        TrainingExample(
            f"{case_id}_1",
            "vitaminc",
            case_id,
            case_id,
            1,
            f"claim-{case_index}-a",
            f"evidence-{case_index}-a",
            "support",
        ),
        TrainingExample(
            f"{case_id}_2",
            "vitaminc",
            case_id,
            case_id,
            2,
            f"claim-{case_index}-a",
            f"evidence-{case_index}-b",
            first_other,
        ),
        TrainingExample(
            f"{case_id}_3",
            "vitaminc",
            case_id,
            case_id,
            3,
            f"claim-{case_index}-b",
            f"evidence-{case_index}-a",
            first_other,
        ),
        TrainingExample(
            f"{case_id}_4",
            "vitaminc",
            case_id,
            case_id,
            4,
            f"claim-{case_index}-b",
            f"evidence-{case_index}-b",
            "support",
        ),
    )


def _fixture_rows() -> tuple[tuple[TrainingExample, ...], tuple[TrainingExample, ...]]:
    vitamin = tuple(row for index in range(4) for row in _vitaminc_case(index))
    labels = ("support", "refute", "neutral", "support", "neutral")
    m3 = tuple(
        TrainingExample(
            f"m3-{index}",
            "m3",
            f"group-{index // 3}",
            None,
            None,
            f"m3 claim {index}",
            f"m3 evidence {index}",
            labels[index % len(labels)],
        )
        for index in range(11)
    )
    return vitamin, m3


def test_v2_v3_have_identical_rows_batches_weights_and_optimizer_schedule() -> None:
    vitamin, m3 = _fixture_rows()
    v2 = build_training_schedule(
        vitaminc=vitamin,
        m3=m3,
        variant=TrainingVariant.V2_CE_MIX,
        seed=20260720,
        target_microbatches=4,
    )
    v3 = build_training_schedule(
        vitaminc=vitamin,
        m3=m3,
        variant=TrainingVariant.V3_MARGIN_MIX,
        seed=20260720,
        target_microbatches=4,
    )
    assert_v2_v3_schedule_identity(v2, v3)
    assert v2.batch_order_sha256 == v3.batch_order_sha256
    assert v2.optimizer_schedule_sha256 == v3.optimizer_schedule_sha256
    assert v2.schedule_sha256 == v3.schedule_sha256
    assert v2.base_order_class_weights == v3.base_order_class_weights
    assert {batch.domain for batch in v2.batches} == {"vitaminc", "m3"}
    assert {
        row.example.row_id
        for batch in v2.batches
        for row in batch.rows
        if row.example.domain == "m3"
    } == {row.row_id for row in m3}


def test_v1_and_a1_cycle_only_at_prebuilt_source_batch_boundaries() -> None:
    vitamin, m3 = _fixture_rows()
    replay = build_training_schedule(
        vitaminc=vitamin,
        m3=m3,
        variant=TrainingVariant.V1_REPLAY_ONLY,
        seed=20260720,
        target_microbatches=5,
    )
    no_replay = build_training_schedule(
        vitaminc=vitamin,
        m3=m3,
        variant=TrainingVariant.A1_MARGIN_NO_REPLAY,
        seed=20260720,
        target_microbatches=5,
    )
    assert {batch.domain for batch in replay.batches} == {"m3"}
    assert {batch.domain for batch in no_replay.batches} == {"vitaminc"}
    assert any(row.repeat_index > 0 for batch in replay.batches for row in batch.rows)
    assert any(
        row.repeat_index > 0 for batch in no_replay.batches for row in batch.rows
    )
    assert all(
        len(batch.rows) == 8 and len(batch.transitions) == 4
        for batch in no_replay.batches
    )


def test_frozen_checkpoint_requires_exact_tree_weights_and_label_order(
    tmp_path: Path,
) -> None:
    checkpoint = tmp_path / "m3"
    checkpoint.mkdir()
    (checkpoint / "model.safetensors").write_bytes(b"frozen weights")
    (checkpoint / "config.json").write_text(
        json.dumps(
            {
                "id2label": {
                    "0": "contradiction",
                    "1": "entailment",
                    "2": "neutral",
                }
            }
        ),
        encoding="utf-8",
    )
    expected_tree = tree_digest(checkpoint)
    expected_weights = file_sha256(checkpoint / "model.safetensors")
    identity = validate_continuation_checkpoint(
        checkpoint,
        expected_tree_sha256=expected_tree,
        expected_weights_sha256=expected_weights,
    )
    assert identity["tree_sha256"] == expected_tree

    (checkpoint / "model.safetensors").write_bytes(b"base or altered weights")
    with pytest.raises(ValidationError, match="frozen M3"):
        validate_continuation_checkpoint(
            checkpoint,
            expected_tree_sha256=expected_tree,
            expected_weights_sha256=expected_weights,
        )


def _fake_checkpoint(path: Path) -> None:
    path.mkdir()
    (path / "model.safetensors").write_bytes(b"candidate weights")
    (path / "config.json").write_text("{}\n", encoding="utf-8")


@pytest.mark.parametrize("stage", list(FailureStage))
def test_training_run_publish_is_failure_atomic(
    tmp_path: Path, stage: FailureStage
) -> None:
    final = tmp_path / "run"
    with pytest.raises(RuntimeError, match="injected failure"):
        seal_training_run(
            run_directory=final,
            invocation_sha256="a" * 64,
            training_manifest={"variant": "V2-ce-mix", "seed": 20260720},
            schedule_manifest={"schema_version": "fixture"},
            runtime_manifest={"schema_version": "fixture-runtime"},
            checkpoint_writer=_fake_checkpoint,
            failure_stage=stage,
        )
    assert not final.exists()
    assert not list(tmp_path.glob(".run.partial-*"))


def test_training_seal_replay_tamper_and_partial_run_rejection(tmp_path: Path) -> None:
    final = tmp_path / "run"
    first = seal_training_run(
        run_directory=final,
        invocation_sha256="b" * 64,
        training_manifest={"variant": "V3-margin-mix", "seed": 20260720},
        schedule_manifest={"schema_version": "fixture"},
        runtime_manifest={"schema_version": "fixture-runtime"},
        checkpoint_writer=_fake_checkpoint,
    )
    replay = seal_training_run(
        run_directory=final,
        invocation_sha256="b" * 64,
        training_manifest={"ignored": "on exact replay"},
        schedule_manifest={"ignored": "on exact replay"},
        runtime_manifest={"ignored": "on exact replay"},
        checkpoint_writer=lambda _: pytest.fail("replay rewrote checkpoint"),
    )
    assert first.invocation_sha256 == replay.invocation_sha256
    (final / "checkpoint" / "model.safetensors").write_bytes(b"tampered")
    with pytest.raises(ValidationError, match="tree has drifted"):
        load_completed_training_run(final)

    partial = tmp_path / "partial"
    partial.mkdir()
    (partial / "training_manifest.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ValidationError, match="partial"):
        load_completed_training_run(partial)
