from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from experiments.m4_change_aware_verifier.common import canonical_sha256
from experiments.m4_change_aware_verifier.contracts import (
    EvaluationRow,
    ModelSpec,
    RawLogitRow,
)
from experiments.m4_change_aware_verifier.scoring import (
    ScoringResult,
    operational_label,
    stored_probabilities,
)
from experiments.m4_change_aware_verifier.selection import (
    FROZEN_M3_TREE_SHA256,
    REPLICATION_SEEDS,
    CandidateCheckpoint,
    DiagnosticCheckpoint,
    SealedSelection,
)


def digest(character: str) -> str:
    return character * 64


def trainer_provenance() -> tuple[str, dict[str, str]]:
    files = {
        "training/m4_13_verifier/train.py": digest("a"),
        "training/m4_13_verifier/losses.py": digest("b"),
        "training/m4_13_verifier/__init__.py": digest("c"),
        "src/groundloop/ai/verification/artifacts.py": digest("d"),
        "src/groundloop/errors.py": digest("e"),
    }
    return (
        canonical_sha256(
            {
                "schema_version": "groundloop-m4-13-trainer-implementation-v1",
                "files_sha256": files,
            }
        ),
        files,
    )


def selection() -> SealedSelection:
    trainer_sha256, trainer_files = trainer_provenance()
    checkpoints = tuple(
        CandidateCheckpoint(
            variant=variant,
            seed=seed,
            checkpoint_relative_path=f"runs/{variant}/{seed}/checkpoint",
            checkpoint_tree_sha256=digest(str(index + 1)),
            checkpoint_identity_sha256=digest(chr(ord("a") + index)),
            development_logits_path=(
                f"development/{variant}:seed-{seed}/development_logits.jsonl"
            ),
            development_logits_sha256=digest("d"),
            source_alignment_sha256=digest("e"),
            run_complete_sha256=digest("a"),
            training_manifest_sha256=digest("b"),
            batch_schedule_sha256=digest("c"),
            training_git_head="1" * 40,
            training_repository_dirty=False,
            trainer_implementation_sha256=trainer_sha256,
            trainer_implementation_files_sha256=trainer_files,
        )
        for index, (variant, seed) in enumerate(
            (variant, seed)
            for variant in ("V2-ce-mix", "V3-margin-mix")
            for seed in REPLICATION_SEEDS
        )
    )
    return SealedSelection(
        path=Path("selection.json"),
        file_sha256=digest("f"),
        selected_variant="V3-margin-mix",
        primary_seed=20260720,
        checkpoints=checkpoints,
        objective_design_verdict="PAIRED_MARGIN_USEFUL",
        v0_development_logits_path=(
            "development/V0-frozen-m3/development_logits.jsonl"
        ),
        v0_development_logits_sha256=digest("d"),
        source_alignment_sha256=digest("e"),
        development_report_path="development_report.json",
        development_report_sha256=digest("a"),
        diagnostic_checkpoints=tuple(
            DiagnosticCheckpoint(
                variant=variant,
                seed=20260720,
                checkpoint_tree_sha256=digest("a"),
                checkpoint_identity_sha256=digest("b"),
                development_logits_path=f"development/{variant}/logits.jsonl",
                development_logits_sha256=digest("c"),
                source_alignment_sha256=digest("e"),
                run_complete_sha256=digest("a"),
                training_manifest_sha256=digest("b"),
                batch_schedule_sha256=digest("c"),
                training_git_head="1" * 40,
                training_repository_dirty=False,
                trainer_implementation_sha256=trainer_sha256,
                trainer_implementation_files_sha256=trainer_files,
            )
            for variant in ("V1-replay-only", "A1-margin-no-replay")
        ),
    )


def models(frozen: SealedSelection) -> tuple[ModelSpec, ...]:
    baseline = ModelSpec(
        variant="V0-frozen-m3",
        seed=None,
        checkpoint_path="v0",
        checkpoint_tree_sha256=FROZEN_M3_TREE_SHA256,
        model_identity=digest("b"),
        calibration_identity=digest("c"),
        checkpoint_identity_path="",
        calibration_path="v0-calibration.json",
        run_complete_sha256=None,
        training_manifest_sha256=None,
        batch_schedule_sha256=None,
        training_git_head=None,
        training_repository_dirty=None,
        trainer_implementation_sha256=None,
        trainer_implementation_files_sha256=None,
        temperature=1.0,
    )
    candidates = tuple(
        ModelSpec(
            variant=frozen.selected_variant,
            seed=item.seed,
            checkpoint_path=item.checkpoint_relative_path,
            checkpoint_tree_sha256=item.checkpoint_tree_sha256,
            model_identity=item.checkpoint_identity_sha256,
            calibration_identity=digest("d"),
            checkpoint_identity_path=(
                f"runs/{frozen.selected_variant}/{item.seed}/checkpoint_identity.json"
            ),
            calibration_path=(
                f"runs/{frozen.selected_variant}/{item.seed}/calibration.json"
            ),
            run_complete_sha256=item.run_complete_sha256,
            training_manifest_sha256=item.training_manifest_sha256,
            batch_schedule_sha256=item.batch_schedule_sha256,
            training_git_head=item.training_git_head,
            training_repository_dirty=item.training_repository_dirty,
            trainer_implementation_sha256=item.trainer_implementation_sha256,
            trainer_implementation_files_sha256=(
                item.trainer_implementation_files_sha256
            ),
            temperature=1.0,
        )
        for item in frozen.selected_checkpoints
    )
    return (baseline, *candidates)


def evaluation_rows() -> tuple[
    tuple[EvaluationRow, ...],
    tuple[EvaluationRow, ...],
    tuple[EvaluationRow, ...],
]:
    vitamin: list[EvaluationRow] = []
    for case_index in range(128):
        stratum = "support_refute" if case_index < 64 else "support_neutral"
        alternative = "refute" if stratum == "support_refute" else "neutral"
        labels = ("support", alternative, alternative, "support")
        for suffix, label in enumerate(labels, 1):
            case_id = f"case-{case_index}"
            vitamin.append(
                EvaluationRow(
                    fixture="vitaminc_terminal_reserve",
                    split="terminal",
                    row_id=f"{case_id}_{suffix}",
                    claim=f"claim {case_index}-{1 if suffix <= 2 else 2}",
                    evidence=f"evidence {case_index}-{1 if suffix in {1, 3} else 2}",
                    label=label,
                    page_id=f"page-{case_index}",
                    case_id=case_id,
                    transition_id=(f"{case_id}:transition-{1 if suffix <= 2 else 2}"),
                    stratum=stratum,
                )
            )
    m3 = tuple(
        EvaluationRow(
            fixture="original_m3_public_test",
            split="test",
            row_id=f"m3-{index}",
            claim=f"m3 claim {index}",
            evidence=f"m3 evidence {index}",
            label="support" if index < 111 else "neutral",
            claim_group_id=f"group-{index}",
        )
        for index in range(358)
    )
    git = tuple(
        EvaluationRow(
            fixture="corrected_m4_10_git_pilot",
            split="transfer",
            row_id=f"git-{index}",
            claim=f"git claim {index}",
            evidence=f"git evidence {index}",
            label=None,
            claim_group_id=f"git-group-{index}",
        )
        for index in range(14)
    )
    return tuple(vitamin), m3, git


def _logits(label: str | None, *, correct: bool) -> tuple[float, float, float]:
    if not correct or label is None:
        return 0.0, 0.0, 3.0
    if label == "support":
        return 0.0, 3.0, 0.0
    if label == "refute":
        return 3.0, 0.0, 0.0
    return 0.0, 0.0, 3.0


def raw_row(
    source: EvaluationRow,
    *,
    model_key: str,
    correct: bool,
    model_identity: str = "a" * 64,
    calibration_identity: str = "b" * 64,
) -> RawLogitRow:
    logits = _logits(source.label, correct=correct)
    probabilities = stored_probabilities(logits, temperature=1.0)
    return RawLogitRow(
        fixture=source.fixture,
        split=source.split,
        stratum=source.stratum,
        page_id=source.page_id,
        case_id=source.case_id,
        claim_group_id=source.claim_group_id,
        transition_id=source.transition_id,
        row_id=source.row_id,
        claim_sha256=source.claim_sha256,
        evidence_sha256=source.evidence_sha256,
        mapped_label=source.label,
        input_sha256=source.input_sha256,
        base_logits=logits,
        uncalibrated_probabilities=probabilities,
        old_m3_temperature_probabilities=stored_probabilities(
            logits, temperature=1.1037657679769346
        ),
        calibrated_probabilities=probabilities,
        operational_label=operational_label(probabilities),
        truncated=False,
        model_key=model_key,
        model_identity=model_identity,
        calibration_identity=calibration_identity,
        temperature=1.0,
    )


class FakeScorer:
    def __init__(self) -> None:
        self.calls = 0

    def score(self, model: ModelSpec, rows: Sequence[EvaluationRow]) -> ScoringResult:
        self.calls += 1
        correct = model.variant != "V0-frozen-m3" or rows[0].fixture != (
            "vitaminc_terminal_reserve"
        )
        return ScoringResult(
            rows=tuple(
                raw_row(
                    row,
                    model_key=model.key,
                    correct=correct,
                    model_identity=model.model_identity,
                    calibration_identity=model.calibration_identity,
                )
                for row in rows
            ),
            batches=1,
            wall_seconds_including_model_load=0.1,
            peak_rss_kib=100,
        )
