from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path

import pytest

from experiments.m4_change_aware_verifier.contracts import EvaluationRow, ModelSpec
from experiments.m4_change_aware_verifier.development import (
    DevelopmentCandidate,
    FrozenM3DevelopmentBaseline,
    build_selection_payload,
    seal_selection,
    validate_development_bundle,
    validate_selection_evidence,
)
from experiments.m4_change_aware_verifier.development_runner import (
    DevelopmentInputs,
    run_development_evaluation,
)
from experiments.m4_change_aware_verifier.scoring import ScoringResult
from experiments.m4_change_aware_verifier.selection import (
    FROZEN_M3_TREE_SHA256,
    DiagnosticCheckpoint,
    load_sealed_selection,
)
from groundloop.errors import ValidationError

from .helpers import FakeScorer, digest, raw_row, trainer_provenance


def _candidates(*, v3_gain: float = 0.02) -> tuple[DevelopmentCandidate, ...]:
    result = []
    index = 0
    trainer_sha256, trainer_files = trainer_provenance()
    for variant in ("V2-ce-mix", "V3-margin-mix"):
        for seed in (20260720, 20260721, 20260722):
            result.append(
                DevelopmentCandidate(
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
                    vitaminc_joint_correct=(
                        0.30 + (v3_gain if variant == "V3-margin-mix" else 0.0)
                    ),
                    m3_macro_f1=0.70,
                    m3_per_class_f1={
                        "support": 0.70,
                        "refute": 0.70,
                        "neutral": 0.70,
                    },
                )
            )
            index += 1
    return tuple(result)


def _baseline() -> FrozenM3DevelopmentBaseline:
    return FrozenM3DevelopmentBaseline(
        macro_f1=0.70,
        per_class_f1={"support": 0.70, "refute": 0.70, "neutral": 0.70},
        checkpoint_tree_sha256=digest("f"),
        development_logits_sha256=digest("a"),
        source_alignment_sha256=digest("e"),
        development_logits_path=("development/V0-frozen-m3/development_logits.jsonl"),
    )


def _diagnostics() -> tuple[DiagnosticCheckpoint, ...]:
    trainer_sha256, trainer_files = trainer_provenance()
    return tuple(
        DiagnosticCheckpoint(
            variant=variant,
            seed=20260720,
            checkpoint_tree_sha256=_real_digest(f"{variant}-tree"),
            checkpoint_identity_sha256=_real_digest(f"{variant}-identity"),
            development_logits_path=f"development/{variant}/logits.jsonl",
            development_logits_sha256=_real_digest(f"{variant}-logits"),
            source_alignment_sha256=digest("e"),
            run_complete_sha256=_real_digest(f"{variant}-complete"),
            training_manifest_sha256=_real_digest(f"{variant}-manifest"),
            batch_schedule_sha256=_real_digest(f"{variant}-schedule"),
            training_git_head="1" * 40,
            training_repository_dirty=False,
            trainer_implementation_sha256=trainer_sha256,
            trainer_implementation_files_sha256=trainer_files,
        )
        for variant in ("V1-replay-only", "A1-margin-no-replay")
    )


def _build_payload(
    candidates: tuple[DevelopmentCandidate, ...],
) -> dict[str, object]:
    return dict(
        build_selection_payload(
            candidates=candidates,
            baseline=_baseline(),
            development_report_path="development_report.json",
            development_report_sha256=_real_digest("report"),
            diagnostic_checkpoints=_diagnostics(),
        )
    )


def test_paired_margin_selected_only_when_preregistered_rule_passes() -> None:
    payload = _build_payload(_candidates())
    assert payload["selected_variant"] == "V3-margin-mix"
    assert payload["objective_design_verdict"]["verdict"] == ("PAIRED_MARGIN_USEFUL")
    tie = _build_payload(_candidates(v3_gain=0.009))
    assert tie["selected_variant"] == "V2-ce-mix"


def test_both_unsafe_variants_produce_sealed_negative_decision() -> None:
    failed = tuple(
        replace(
            candidate,
            m3_macro_f1=0.1,
            m3_per_class_f1={
                "support": 0.1,
                "refute": 0.1,
                "neutral": 0.1,
            },
        )
        for candidate in _candidates(v3_gain=0.0)
    )
    payload = _build_payload(failed)
    assert payload["selected_variant"] is None
    assert payload["terminal_evaluation_authorized"] is False
    assert payload["objective_design_verdict"]["verdict"] == ("NO_ELIGIBLE_SELECTION")


def test_unsafe_v2_without_useful_v3_does_not_fall_back_to_v2(
    tmp_path: Path,
) -> None:
    candidates = tuple(
        replace(
            candidate,
            m3_macro_f1=0.1,
            m3_per_class_f1={
                "support": 0.1,
                "refute": 0.1,
                "neutral": 0.1,
            },
        )
        if candidate.variant == "V2-ce-mix"
        else candidate
        for candidate in _candidates(v3_gain=0.0)
    )
    path = tmp_path / "negative-selection.json"
    decision = seal_selection(
        path,
        candidates=candidates,
        baseline=_baseline(),
        development_report_path="development_report.json",
        development_report_sha256=_real_digest("report"),
        diagnostic_checkpoints=_diagnostics(),
    )
    assert decision.selected_variant is None
    assert path.is_file()
    with pytest.raises(ValidationError, match="NO_ELIGIBLE_SELECTION"):
        load_sealed_selection(path)


def test_selection_seal_is_idempotent_but_not_replaceable(tmp_path: Path) -> None:
    path = tmp_path / "selection.json"
    kwargs = {
        "candidates": _candidates(),
        "baseline": _baseline(),
        "development_report_path": "development_report.json",
        "development_report_sha256": _real_digest("report"),
        "diagnostic_checkpoints": _diagnostics(),
    }
    first = seal_selection(path, **kwargs)
    second = seal_selection(path, **kwargs)
    assert first.file_sha256 == second.file_sha256
    with pytest.raises(ValidationError, match="another result"):
        seal_selection(
            path,
            candidates=_candidates(v3_gain=0.009),
            baseline=_baseline(),
            development_report_path="development_report.json",
            development_report_sha256=_real_digest("report"),
            diagnostic_checkpoints=_diagnostics(),
        )


def _real_digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _development_inputs() -> DevelopmentInputs:
    vitamin: list[EvaluationRow] = []
    for case_index in range(256):
        alternative = "refute" if case_index < 128 else "neutral"
        stratum = "support_refute" if alternative == "refute" else "support_neutral"
        for suffix, label in enumerate(
            ("support", alternative, alternative, "support"), 1
        ):
            case_id = f"dev-case-{case_index}"
            vitamin.append(
                EvaluationRow(
                    fixture="vitaminc_development",
                    split="development",
                    row_id=f"{case_id}_{suffix}",
                    claim=f"claim-{case_index}-{suffix <= 2}",
                    evidence=f"evidence-{case_index}-{suffix in {1, 3}}",
                    label=label,
                    page_id=f"page-{case_index}",
                    case_id=case_id,
                    transition_id=f"{case_id}:transition-{1 if suffix <= 2 else 2}",
                    stratum=stratum,
                )
            )
    labels = ("support", "refute", "neutral")
    m3 = tuple(
        EvaluationRow(
            fixture="m3_development",
            split="development",
            row_id=f"m3-dev-{index}",
            claim=f"m3-claim-{index}",
            evidence=f"m3-evidence-{index}",
            label=labels[index % 3],
            claim_group_id=f"group-{min(index, 648)}",
        )
        for index in range(987)
    )
    models = [
        ModelSpec(
            variant="V0-frozen-m3",
            seed=None,
            checkpoint_path="v0",
            checkpoint_tree_sha256=FROZEN_M3_TREE_SHA256,
            model_identity=FROZEN_M3_TREE_SHA256,
            calibration_identity=_real_digest("uncalibrated"),
            checkpoint_identity_path="",
            calibration_path="development-only-uncalibrated",
            run_complete_sha256=None,
            training_manifest_sha256=None,
            batch_schedule_sha256=None,
            training_git_head=None,
            training_repository_dirty=None,
            trainer_implementation_sha256=None,
            trainer_implementation_files_sha256=None,
            temperature=1.0,
        )
    ]
    for variant, seeds in (
        ("V1-replay-only", (20260720,)),
        ("V2-ce-mix", (20260720, 20260721, 20260722)),
        ("V3-margin-mix", (20260720, 20260721, 20260722)),
        ("A1-margin-no-replay", (20260720,)),
    ):
        for seed in seeds:
            key = f"{variant}-{seed}"
            models.append(
                ModelSpec(
                    variant=variant,
                    seed=seed,
                    checkpoint_path=f"runs/{variant}/{seed}/checkpoint",
                    checkpoint_tree_sha256=_real_digest(f"tree-{key}"),
                    model_identity=_real_digest(f"identity-{key}"),
                    calibration_identity=_real_digest("uncalibrated"),
                    checkpoint_identity_path=f"runs/{variant}/{seed}/identity.json",
                    calibration_path="development-only-uncalibrated",
                    run_complete_sha256=_real_digest(f"complete-{key}"),
                    training_manifest_sha256=_real_digest(f"manifest-{key}"),
                    batch_schedule_sha256=_real_digest(f"schedule-{key}"),
                    training_git_head="1" * 40,
                    training_repository_dirty=False,
                    trainer_implementation_sha256=trainer_provenance()[0],
                    trainer_implementation_files_sha256=trainer_provenance()[1],
                    temperature=1.0,
                )
            )
    return DevelopmentInputs(
        models=tuple(models),
        vitaminc_rows=tuple(vitamin),
        m3_rows=m3,
        dataset_manifest_sha256=_real_digest("dataset"),
        vitaminc_manifest_sha256=_real_digest("vitamin"),
        m3_development_sha256=_real_digest("m3"),
    )


class _UnsafeCandidateScorer(FakeScorer):
    def score(
        self, model: ModelSpec, rows: Sequence[EvaluationRow]
    ) -> ScoringResult:
        result = super().score(model, rows)
        if (
            model.variant in {"V2-ce-mix", "V3-margin-mix"}
            and rows[0].fixture == "m3_development"
        ):
            return replace(
                result,
                rows=tuple(
                    raw_row(
                        row,
                        model_key=model.key,
                        correct=False,
                        model_identity=model.model_identity,
                        calibration_identity=model.calibration_identity,
                    )
                    for row in rows
                ),
            )
        return result


def test_terminal_recomputes_selection_from_raw_development_logits(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "development-bundle"
    selection, _ = run_development_evaluation(
        inputs=_development_inputs(),
        scorer=FakeScorer(),
        output_directory=bundle,
        synthetic_test_mode=True,
    )
    validate_selection_evidence(selection, allow_synthetic=True)
    report_path = bundle / selection.development_report_path
    original_report = report_path.read_bytes()
    report_path.write_bytes(original_report + b"\n")
    with pytest.raises(ValidationError, match="development report drifted"):
        validate_selection_evidence(selection, allow_synthetic=True)
    report_path.write_bytes(original_report)
    payload = json.loads(selection.path.read_text(encoding="utf-8"))
    payload["development_metrics"][0]["m3_macro_f1"] = 0.123
    selection.path.write_text(json.dumps(payload), encoding="utf-8")
    tampered = load_sealed_selection(selection.path)
    with pytest.raises(ValidationError, match="not derivable"):
        validate_selection_evidence(tampered, allow_synthetic=True)


@pytest.mark.parametrize("failure_stage", ("after_raw_logits", "before_bundle_seal"))
def test_development_failure_is_atomic(tmp_path: Path, failure_stage: str) -> None:
    output = tmp_path / "development-failure"
    with pytest.raises(RuntimeError, match="injected"):
        run_development_evaluation(
            inputs=_development_inputs(),
            scorer=FakeScorer(),
            output_directory=output,
            synthetic_test_mode=True,
            failure_stage=failure_stage,
        )
    assert not output.exists()
    assert not tuple(tmp_path.glob(".development-failure.partial-*"))


def test_development_exact_replay_and_collision_precede_scoring(
    tmp_path: Path,
) -> None:
    output = tmp_path / "development-replay"
    inputs = _development_inputs()
    first = FakeScorer()
    run_development_evaluation(
        inputs=inputs,
        scorer=first,
        output_directory=output,
        synthetic_test_mode=True,
    )
    assert first.calls == 18
    replay = FakeScorer()
    run_development_evaluation(
        inputs=inputs,
        scorer=replay,
        output_directory=output,
        synthetic_test_mode=True,
    )
    assert replay.calls == 0
    collision = FakeScorer()
    with pytest.raises(ValidationError, match="invocation collision"):
        run_development_evaluation(
            inputs=replace(inputs, dataset_manifest_sha256=_real_digest("changed")),
            scorer=collision,
            output_directory=output,
            synthetic_test_mode=True,
        )
    assert collision.calls == 0


def test_development_exact_replay_rejects_expanded_or_incomplete_seal(
    tmp_path: Path,
) -> None:
    output = tmp_path / "development-replay-surface"
    inputs = _development_inputs()
    run_development_evaluation(
        inputs=inputs,
        scorer=FakeScorer(),
        output_directory=output,
        synthetic_test_mode=True,
    )
    unexpected = output / "unexpected.json"
    unexpected.write_text("{}", encoding="utf-8")
    scorer = FakeScorer()
    with pytest.raises(ValidationError, match="extra or missing files"):
        run_development_evaluation(
            inputs=inputs,
            scorer=scorer,
            output_directory=output,
            synthetic_test_mode=True,
        )
    assert scorer.calls == 0
    unexpected.unlink()

    seal_path = output / "DEVELOPMENT_COMPLETED.json"
    seal = json.loads(seal_path.read_text(encoding="utf-8"))
    seal["files"] = seal["files"][:-1]
    seal_path.write_text(json.dumps(seal), encoding="utf-8")
    with pytest.raises(ValidationError, match="sealed file surface drifted"):
        run_development_evaluation(
            inputs=inputs,
            scorer=scorer,
            output_directory=output,
            synthetic_test_mode=True,
        )
    assert scorer.calls == 0


def test_negative_development_decision_semantically_replays_without_scoring(
    tmp_path: Path,
) -> None:
    output = tmp_path / "negative-development-replay"
    inputs = _development_inputs()
    first = _UnsafeCandidateScorer()
    decision, _ = run_development_evaluation(
        inputs=inputs,
        scorer=first,
        output_directory=output,
        synthetic_test_mode=True,
    )
    assert decision.selected_variant is None
    assert first.calls == 18
    replay = _UnsafeCandidateScorer()
    replayed, _ = run_development_evaluation(
        inputs=inputs,
        scorer=replay,
        output_directory=output,
        synthetic_test_mode=True,
    )
    assert replayed.selected_variant is None
    assert replay.calls == 0


def test_resealed_fabricated_negative_fails_semantic_replay_before_scoring(
    tmp_path: Path,
) -> None:
    output = tmp_path / "fabricated-negative"
    inputs = _development_inputs()
    decision, _ = run_development_evaluation(
        inputs=inputs,
        scorer=FakeScorer(),
        output_directory=output,
        synthetic_test_mode=True,
    )
    assert decision.selected_variant == "V2-ce-mix"
    selection_path = output / "selection.json"
    payload = json.loads(selection_path.read_text(encoding="utf-8"))
    payload["selected_variant"] = None
    payload["terminal_evaluation_authorized"] = False
    payload["objective_design_verdict"]["verdict"] = "NO_ELIGIBLE_SELECTION"
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    selection_path.write_bytes(encoded)
    selection_sha256 = hashlib.sha256(encoded).hexdigest()
    seal_path = output / "DEVELOPMENT_COMPLETED.json"
    seal = json.loads(seal_path.read_text(encoding="utf-8"))
    seal["selection_sha256"] = selection_sha256
    seal["terminal_evaluation_authorized"] = False
    for item in seal["files"]:
        if item["path"] == "selection.json":
            item["sha256"] = selection_sha256
    seal_path.write_text(json.dumps(seal), encoding="utf-8")

    scorer = FakeScorer()
    with pytest.raises(ValidationError, match="not derivable"):
        run_development_evaluation(
            inputs=inputs,
            scorer=scorer,
            output_directory=output,
            synthetic_test_mode=True,
        )
    assert scorer.calls == 0


def test_development_bundle_rejects_missing_tampered_and_extra_files(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source-bundle"
    run_development_evaluation(
        inputs=_development_inputs(),
        scorer=FakeScorer(),
        output_directory=source,
        synthetic_test_mode=True,
    )
    missing = tmp_path / "missing-seal"
    tampered = tmp_path / "tampered-runtime"
    expanded = tmp_path / "expanded-bundle"
    for target in (missing, tampered, expanded):
        shutil.copytree(source, target)
    (missing / "DEVELOPMENT_COMPLETED.json").unlink()
    (tampered / "development_runtime.json").write_text("{}", encoding="utf-8")
    (expanded / "unexpected.json").write_text("{}", encoding="utf-8")
    for target, message in (
        (missing, "extra or missing files"),
        (tampered, "sealed development artifact drifted"),
        (expanded, "extra or missing files"),
    ):
        frozen = load_sealed_selection(target / "selection.json")
        with pytest.raises(ValidationError, match=message):
            validate_development_bundle(frozen, allow_synthetic=True)
