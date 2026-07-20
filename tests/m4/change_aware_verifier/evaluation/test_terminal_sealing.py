from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from experiments.m4_change_aware_verifier import terminal
from experiments.m4_change_aware_verifier.artifacts import (
    load_development_models_manifest,
)
from experiments.m4_change_aware_verifier.common import canonical_sha256
from experiments.m4_change_aware_verifier.development import DEVELOPMENT_BUNDLE_FILES
from experiments.m4_change_aware_verifier.provenance import (
    evaluation_rows_identity_sha256,
)
from experiments.m4_change_aware_verifier.selection import (
    CandidateCheckpoint,
    DiagnosticCheckpoint,
)
from experiments.m4_change_aware_verifier.terminal import (
    TerminalArtifactPaths,
    TerminalInputs,
    TerminalResult,
    _assert_prepublication_provenance,
    _git_diagnostic,
    run_terminal_evaluation,
    run_terminal_from_artifacts,
)
from groundloop.errors import ValidationError

from .helpers import (
    EvaluationRow,
    FakeScorer,
    evaluation_rows,
    models,
    raw_row,
    selection,
)
from .test_artifacts import _manifest_with_fake_v0


class MetadataTamperingScorer(FakeScorer):
    def score(self, model, rows):  # type: ignore[no-untyped-def]
        result = super().score(model, rows)
        return replace(
            result,
            rows=(replace(result.rows[0], claim_sha256="0" * 64), *result.rows[1:]),
        )


class DerivedFieldTamperingScorer(FakeScorer):
    def score(self, model, rows):  # type: ignore[no-untyped-def]
        result = super().score(model, rows)
        return replace(
            result,
            rows=(
                replace(
                    result.rows[0],
                    calibrated_probabilities=(0.01, 0.01, 0.98),
                    operational_label="neutral",
                ),
                *result.rows[1:],
            ),
        )


class InterruptingScorer(FakeScorer):
    def score(self, model, rows):  # type: ignore[no-untyped-def]
        raise KeyboardInterrupt


def _inputs() -> TerminalInputs:
    frozen = selection()
    vitamin, m3, git = evaluation_rows()
    return TerminalInputs(
        selection=frozen,
        models=models(frozen),
        vitaminc_rows=vitamin,
        m3_rows=m3,
        git_rows=git,
        m3_test_proof={
            "ordered_row_identity_sha256": evaluation_rows_identity_sha256(m3),
        },
        m4_12_proof={
            "terminal_baseline": False,
            "adapted_checkpoint_evaluated": False,
        },
        reserve_proof={
            "manifest_sha256": (
                "3dcfcba0b809e3bcfcf2f9c036c4946f484d706faa61eec8c3fa489fbcb11dc5"
            ),
            "consumed_m4_12_used": False,
            "ordered_row_identity_sha256": evaluation_rows_identity_sha256(vitamin),
        },
        m4_10_proof={
            "independently_adjudicated_labels": False,
            "ordered_row_identity_sha256": evaluation_rows_identity_sha256(git),
        },
    )


@pytest.mark.parametrize(
    "failure_stage",
    ("after_raw_logits", "before_result_manifest", "before_result_seal"),
)
def test_terminal_failure_is_atomic(tmp_path: Path, failure_stage: str) -> None:
    output = tmp_path / "result"
    with pytest.raises(RuntimeError, match="injected"):
        run_terminal_evaluation(
            inputs=_inputs(),
            scorer=FakeScorer(),
            output_directory=output,
            bootstrap_resamples=5,
            failure_stage=failure_stage,
            synthetic_test_mode=True,
        )
    assert not output.exists()
    assert not tuple(tmp_path.glob(".result.partial-*"))
    run_terminal_evaluation(
        inputs=_inputs(),
        scorer=FakeScorer(),
        output_directory=output,
        bootstrap_resamples=5,
        synthetic_test_mode=True,
    )
    assert (output / "terminal" / "COMPLETED.json").is_file()


def test_terminal_interrupt_is_atomic(tmp_path: Path) -> None:
    output = tmp_path / "interrupted"
    with pytest.raises(KeyboardInterrupt):
        run_terminal_evaluation(
            inputs=_inputs(),
            scorer=InterruptingScorer(),
            output_directory=output,
            bootstrap_resamples=5,
            synthetic_test_mode=True,
        )
    assert not output.exists()
    assert not tuple(tmp_path.glob(".interrupted.partial-*"))


@pytest.mark.parametrize("orphan", ["RUNNING.json", "vitaminc_logits.jsonl"])
def test_orphan_terminal_output_requires_new_path(tmp_path: Path, orphan: str) -> None:
    terminal = tmp_path / "orphan" / "terminal"
    terminal.mkdir(parents=True)
    (terminal / orphan).write_text("{}", encoding="utf-8")
    with pytest.raises(ValidationError, match="orphaned/partial"):
        run_terminal_evaluation(
            inputs=_inputs(),
            scorer=FakeScorer(),
            output_directory=tmp_path / "orphan",
            bootstrap_resamples=5,
            synthetic_test_mode=True,
        )


def test_complete_run_exact_replays_without_scoring(tmp_path: Path) -> None:
    output = tmp_path / "result"
    scorer = FakeScorer()
    created = run_terminal_evaluation(
        inputs=_inputs(),
        scorer=scorer,
        output_directory=output,
        bootstrap_resamples=5,
        synthetic_test_mode=True,
    )
    assert created.disposition == "CREATED"
    assert created.result_manifest["evaluation_mode"] == "synthetic-fixture"
    assert created.result_manifest["scientific_result"] is False
    metrics = json.loads(
        (output / "terminal" / "metrics.json").read_text(encoding="utf-8")
    )
    assert metrics["stop_go"]["verdict"] == "NON_SCIENTIFIC_TEST_ONLY"
    assert metrics["stop_go"]["promotion_authorized"] is False
    bootstrap = json.loads(
        (output / "terminal" / "bootstrap.json").read_text(encoding="utf-8")
    )

    def seeds(value: object) -> list[object]:
        if isinstance(value, dict):
            return [
                child
                for key, item in value.items()
                for child in ([item] if key == "seed" else seeds(item))
            ]
        if isinstance(value, list):
            return [child for item in value for child in seeds(item)]
        return []

    assert seeds(bootstrap)
    assert set(seeds(bootstrap)) == {20260720}
    assert scorer.calls == 10
    replay_scorer = FakeScorer()
    replayed = run_terminal_evaluation(
        inputs=_inputs(),
        scorer=replay_scorer,
        output_directory=output,
        bootstrap_resamples=5,
        synthetic_test_mode=True,
    )
    assert replayed.disposition == "REPLAYED"
    assert replay_scorer.calls == 0


def test_terminal_scorer_cannot_substitute_row_metadata(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="provenance drifted"):
        run_terminal_evaluation(
            inputs=_inputs(),
            scorer=MetadataTamperingScorer(),
            output_directory=tmp_path / "tampered",
            bootstrap_resamples=5,
            synthetic_test_mode=True,
        )


def test_terminal_scorer_cannot_substitute_derived_probabilities(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValidationError, match="provenance drifted"):
        run_terminal_evaluation(
            inputs=_inputs(),
            scorer=DerivedFieldTamperingScorer(),
            output_directory=tmp_path / "derived-tamper",
            bootstrap_resamples=5,
            synthetic_test_mode=True,
        )


@pytest.mark.parametrize(("seed", "resamples"), [(1, 1000), (20260720, 1)])
def test_production_rejects_bootstrap_protocol_overrides(
    tmp_path: Path, seed: int, resamples: int
) -> None:
    with pytest.raises(ValidationError, match="bootstrap is frozen"):
        run_terminal_evaluation(
            inputs=_inputs(),
            scorer=FakeScorer(),
            output_directory=tmp_path / "production-override",
            bootstrap_seed=seed,
            bootstrap_resamples=resamples,
            synthetic_test_mode=False,
        )


def test_git_diagnostic_uses_distinct_evidence_truth_table() -> None:
    specifications = (
        ("claim-bustub-development-ubuntu-22", "refute", "ubuntu-dev"),
        ("claim-bustub-development-ubuntu-22", "refute", "ubuntu-dev"),
        ("claim-bustub-grading-ubuntu-22", "refute", "ubuntu-grade"),
        ("claim-bustub-wsl-unsupported", "neutral", "wsl"),
        ("claim-dynagox-is-hardware-tee", "support", "tee"),
        ("claim-dynagox-buckets-protected", "support", "buckets"),
        ("claim-dynagox-buckets-protected", "support", "buckets"),
        ("claim-dynagox-direct-maps-visible", "support", "maps"),
        ("claim-mp-spdz-fixed-16-16", "refute", "fixed"),
        ("claim-mp-spdz-fixed-16-16", "refute", "fixed"),
        ("claim-mp-spdz-sbitint-type", "refute", "sbitint"),
        ("claim-mp-spdz-boost-1-81", "support", "boost-support"),
        ("claim-mp-spdz-boost-1-81", "refute", "boost-refute"),
        ("claim-mp-spdz-set-precision", "refute", "precision"),
    )
    probabilities = {
        "support": (0.9, 0.05, 0.05),
        "refute": (0.05, 0.9, 0.05),
        "neutral": (0.05, 0.05, 0.9),
    }
    rows = []
    for index, (claim_id, label, evidence) in enumerate(specifications):
        source = EvaluationRow(
            "corrected_m4_10_git_pilot",
            "transfer",
            f"git-{index}",
            claim_id,
            evidence,
            None,
            claim_group_id=claim_id,
        )
        rows.append(
            replace(
                raw_row(source, model_key="candidate", correct=False),
                calibrated_probabilities=probabilities[label],
                operational_label=label,
            )
        )
    report = _git_diagnostic(tuple(rows), require_frozen_ids=True)
    model = report["models"]["candidate"]
    assert model["claim_states"]["claim-mp-spdz-boost-1-81"] == "CONFLICTED"
    assert model["claim_states"]["claim-bustub-wsl-unsupported"] == "UNSUPPORTED"
    assert model["obsolete_concept_groups"]["ubuntu_22_04"]["state"] == "REFUTED"
    assert model["claim_witness_counts"]["claim-bustub-development-ubuntu-22"] == {
        "pairs": 2,
        "distinct_evidence": 1,
    }
    assert model["stable_negative_warnings"] == ["claim-mp-spdz-set-precision"]
    assert model["stable_unsupported"] == ["claim-bustub-wsl-unsupported"]
    assert model["inserted_positive_states"] == {
        "claim-dynagox-buckets-protected": "SUPPORTED",
        "claim-dynagox-direct-maps-visible": "SUPPORTED",
    }
    assert report["go_no_go_effect"] is None
    with pytest.raises(ValidationError, match="14 new-version pairs"):
        _git_diagnostic(tuple(rows[:-1]), require_frozen_ids=True)


def _production_paths(tmp_path: Path) -> TerminalArtifactPaths:
    artifact_root = tmp_path / "artifacts"
    development_bundle = artifact_root / "development_bundle"
    for relative in DEVELOPMENT_BUNDLE_FILES:
        path = development_bundle / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}\n", encoding="utf-8")
    data_root = tmp_path / "data"
    (data_root / "prepared").mkdir(parents=True)
    (data_root / "prepared" / "test_m3.jsonl").write_text("\n", encoding="utf-8")
    (data_root / "sealed").mkdir(parents=True)
    (data_root / "sealed" / "terminal_reserve.jsonl").write_text(
        "sealed sentinel\n", encoding="utf-8"
    )
    for directory in (tmp_path / "m4-12", tmp_path / "m4-10", tmp_path / "v0"):
        directory.mkdir(exist_ok=True)
    (tmp_path / "v0-calibration.json").write_text("{}\n", encoding="utf-8")
    return TerminalArtifactPaths(
        selection=development_bundle / "selection.json",
        data_root=data_root,
        candidate_artifact_root=artifact_root,
        m4_12_artifact_root=tmp_path / "m4-12",
        corrected_m4_10_root=tmp_path / "m4-10",
        v0_checkpoint=tmp_path / "v0",
        v0_calibration=tmp_path / "v0-calibration.json",
        final_test_manifest_sha256=(
            "3dcfcba0b809e3bcfcf2f9c036c4946f484d706faa61eec8c3fa489fbcb11dc5"
        ),
    )


@pytest.mark.parametrize(
    "relative",
    (
        "selection.json",
        "development/V2-ce-mix:seed-20260720/development_logits.jsonl",
    ),
)
def test_preflight_rejects_sealed_symlink_without_opening_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative: str,
) -> None:
    paths = _production_paths(tmp_path)
    target = paths.data_root / "sealed" / "terminal_reserve.jsonl"
    alias = paths.selection.parent / relative
    alias.unlink()
    alias.symlink_to(target)
    original_read_text = Path.read_text

    def guarded_read_text(path: Path, *args: object, **kwargs: object) -> str:
        if path.resolve() == target.resolve():
            pytest.fail("sealed reserve was opened through a preflight symlink")
        return original_read_text(path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, "read_text", guarded_read_text)
    with pytest.raises(ValidationError, match="symlink"):
        run_terminal_from_artifacts(
            paths=paths,
            scorer=FakeScorer(),
            output_directory=tmp_path / "result",
        )


@pytest.mark.parametrize("root_name", ("candidate", "m4-12"))
def test_preflight_rejects_descendant_sealed_alias_without_opening_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    root_name: str,
) -> None:
    paths = _production_paths(tmp_path)
    target = paths.data_root / "sealed" / "terminal_reserve.jsonl"
    root = (
        paths.candidate_artifact_root
        if root_name == "candidate"
        else paths.m4_12_artifact_root
    )
    alias = root / "nested" / "run_complete.json"
    alias.parent.mkdir(parents=True)
    alias.symlink_to(target)
    original_read_text = Path.read_text

    def guarded_read_text(path: Path, *args: object, **kwargs: object) -> str:
        if path.resolve() == target.resolve():
            pytest.fail("sealed reserve was opened through a prerequisite alias")
        return original_read_text(path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, "read_text", guarded_read_text)
    with pytest.raises(ValidationError, match="tree contains a symlink"):
        run_terminal_from_artifacts(
            paths=paths,
            scorer=FakeScorer(),
            output_directory=tmp_path / "result",
        )


def test_preflight_rejects_output_under_sealed_before_any_input_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _production_paths(tmp_path)

    def forbidden_read_text(
        _path: Path, *_args: object, **_kwargs: object
    ) -> str:
        pytest.fail("an input was opened before unsafe output rejection")

    monkeypatch.setattr(Path, "read_text", forbidden_read_text)
    with pytest.raises(ValidationError, match="output directory resolves inside"):
        run_terminal_from_artifacts(
            paths=paths,
            scorer=FakeScorer(),
            output_directory=paths.data_root / "sealed" / "terminal-result",
        )


def test_production_calibration_failure_precedes_reserve_unlock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(terminal, "load_sealed_selection", lambda _path: selection())
    monkeypatch.setattr(
        terminal, "validate_development_bundle", lambda _value: {"proof": "dev"}
    )
    monkeypatch.setattr(terminal, "_validate_all_selection_runs", lambda *_args: None)

    def reject_models(**_kwargs: object) -> tuple[()]:
        raise ValidationError("calibration semantic replay differs")

    monkeypatch.setattr(terminal, "load_terminal_models", reject_models)
    monkeypatch.setattr(
        terminal,
        "verify_terminal_reserve",
        lambda **_kwargs: pytest.fail("reserve was opened before calibration failed"),
    )
    with pytest.raises(ValidationError, match="calibration semantic replay"):
        run_terminal_from_artifacts(
            paths=_production_paths(tmp_path),
            scorer=FakeScorer(),
            output_directory=tmp_path / "result",
        )


def test_incomplete_development_bundle_precedes_reserve_unlock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(terminal, "load_sealed_selection", lambda _path: selection())

    def reject_bundle(_selection: object) -> dict[str, object]:
        raise ValidationError("development completion seal is missing")

    monkeypatch.setattr(terminal, "validate_development_bundle", reject_bundle)
    monkeypatch.setattr(
        terminal,
        "verify_terminal_reserve",
        lambda **_kwargs: pytest.fail("incomplete development opened the reserve"),
    )
    with pytest.raises(ValidationError, match="completion seal"):
        run_terminal_from_artifacts(
            paths=_production_paths(tmp_path),
            scorer=FakeScorer(),
            output_directory=tmp_path / "result",
        )


def test_unselected_and_diagnostic_run_tampering_precedes_reserve_unlock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = _manifest_with_fake_v0(tmp_path, monkeypatch)
    loaded = load_development_models_manifest(manifest)
    by_run = {
        (model.variant, model.seed): model for model in loaded if model.seed is not None
    }

    def candidate(variant: str, seed: int) -> CandidateCheckpoint:
        model = by_run[(variant, seed)]
        return CandidateCheckpoint(
            variant=variant,
            seed=seed,
            checkpoint_relative_path=f"runs/{variant}/{seed}/checkpoint",
            checkpoint_tree_sha256=model.checkpoint_tree_sha256,
            checkpoint_identity_sha256=model.model_identity,
            development_logits_path=f"development/{variant}:seed-{seed}/logits.jsonl",
            development_logits_sha256="d" * 64,
            source_alignment_sha256="e" * 64,
            run_complete_sha256=str(model.run_complete_sha256),
            training_manifest_sha256=str(model.training_manifest_sha256),
            batch_schedule_sha256=str(model.batch_schedule_sha256),
            training_git_head=str(model.training_git_head),
            training_repository_dirty=False,
            trainer_implementation_sha256=str(model.trainer_implementation_sha256),
            trainer_implementation_files_sha256=dict(
                model.trainer_implementation_files_sha256 or {}
            ),
        )

    def diagnostic(variant: str) -> DiagnosticCheckpoint:
        model = by_run[(variant, 20260720)]
        return DiagnosticCheckpoint(
            variant=variant,
            seed=20260720,
            checkpoint_tree_sha256=model.checkpoint_tree_sha256,
            checkpoint_identity_sha256=model.model_identity,
            development_logits_path=f"development/{variant}:seed-20260720/logits.jsonl",
            development_logits_sha256="d" * 64,
            source_alignment_sha256="e" * 64,
            run_complete_sha256=str(model.run_complete_sha256),
            training_manifest_sha256=str(model.training_manifest_sha256),
            batch_schedule_sha256=str(model.batch_schedule_sha256),
            training_git_head=str(model.training_git_head),
            training_repository_dirty=False,
            trainer_implementation_sha256=str(model.trainer_implementation_sha256),
            trainer_implementation_files_sha256=dict(
                model.trainer_implementation_files_sha256 or {}
            ),
        )

    frozen = replace(
        selection(),
        checkpoints=tuple(
            candidate(variant, seed)
            for variant in ("V2-ce-mix", "V3-margin-mix")
            for seed in (20260720, 20260721, 20260722)
        ),
        diagnostic_checkpoints=(
            diagnostic("V1-replay-only"),
            diagnostic("A1-margin-no-replay"),
        ),
    )
    monkeypatch.setattr(terminal, "load_sealed_selection", lambda _path: frozen)
    monkeypatch.setattr(
        terminal, "validate_development_bundle", lambda _value: {"proof": "dev"}
    )
    monkeypatch.setattr(
        terminal,
        "verify_terminal_reserve",
        lambda **_kwargs: pytest.fail("tampered run opened the reserve"),
    )
    paths = replace(
        _production_paths(tmp_path),
        candidate_artifact_root=tmp_path / "artifacts",
    )
    for variant, seed in (
        ("V1-replay-only", 20260720),
        ("A1-margin-no-replay", 20260720),
        ("V2-ce-mix", 20260721),
    ):
        weights = (
            tmp_path
            / "artifacts"
            / "runs"
            / variant
            / str(seed)
            / "checkpoint"
            / "model.safetensors"
        )
        original = weights.read_bytes()
        try:
            weights.write_bytes(b"tampered")
            with pytest.raises(ValidationError, match="checkpoint tree"):
                run_terminal_from_artifacts(
                    paths=paths,
                    scorer=FakeScorer(),
                    output_directory=tmp_path / f"result-{variant}-{seed}",
                )
        finally:
            weights.write_bytes(original)


def test_production_exact_replay_precedes_reserve_unlock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    frozen = selection()
    _vitamin, m3, git = evaluation_rows()
    monkeypatch.setattr(terminal, "load_sealed_selection", lambda _path: frozen)
    monkeypatch.setattr(
        terminal, "validate_development_bundle", lambda _value: {"proof": "dev"}
    )
    monkeypatch.setattr(terminal, "_validate_all_selection_runs", lambda *_args: None)
    monkeypatch.setattr(
        terminal,
        "load_terminal_models",
        lambda **_kwargs: models(frozen),
    )
    monkeypatch.setattr(
        terminal,
        "verify_m4_12_diagnostic",
        lambda _path: {"schema_version": "test-m4-12"},
    )
    monkeypatch.setattr(
        terminal,
        "load_original_m3_test",
        lambda _path: (
            m3,
            {"ordered_row_identity_sha256": evaluation_rows_identity_sha256(m3)},
        ),
    )
    monkeypatch.setattr(
        terminal,
        "verify_corrected_m4_10",
        lambda _path: (
            git,
            {"ordered_row_identity_sha256": evaluation_rows_identity_sha256(git)},
        ),
    )
    monkeypatch.setattr(
        terminal,
        "_execution_identity",
        lambda: {"repository_dirty": False, "evaluation_code_sha256": "a" * 64},
    )
    monkeypatch.setattr(
        terminal,
        "_completed_replay",
        lambda *_args, **_kwargs: TerminalResult("REPLAYED", {"state": "COMPLETED"}),
    )
    monkeypatch.setattr(
        terminal,
        "verify_terminal_reserve",
        lambda **_kwargs: pytest.fail("exact replay reopened the reserve"),
    )
    result = run_terminal_from_artifacts(
        paths=_production_paths(tmp_path),
        scorer=FakeScorer(),
        output_directory=tmp_path / "result",
    )
    assert result.disposition == "REPLAYED"


def test_production_dirty_evaluator_precedes_reserve_unlock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    frozen = selection()
    _vitamin, m3, git = evaluation_rows()
    monkeypatch.setattr(terminal, "load_sealed_selection", lambda _path: frozen)
    monkeypatch.setattr(
        terminal, "validate_development_bundle", lambda _value: {"proof": "dev"}
    )
    monkeypatch.setattr(terminal, "_validate_all_selection_runs", lambda *_args: None)
    monkeypatch.setattr(
        terminal,
        "load_terminal_models",
        lambda **_kwargs: models(frozen),
    )
    monkeypatch.setattr(
        terminal,
        "verify_m4_12_diagnostic",
        lambda _path: {"schema_version": "test-m4-12"},
    )
    monkeypatch.setattr(
        terminal,
        "load_original_m3_test",
        lambda _path: (m3, {"proof": "m3"}),
    )
    monkeypatch.setattr(
        terminal,
        "verify_corrected_m4_10",
        lambda _path: (git, {"proof": "git"}),
    )
    monkeypatch.setattr(
        terminal,
        "_execution_identity",
        lambda: {"repository_dirty": True, "evaluation_code_sha256": "a" * 64},
    )
    monkeypatch.setattr(
        terminal,
        "verify_terminal_reserve",
        lambda **_kwargs: pytest.fail("dirty evaluator opened the reserve"),
    )
    with pytest.raises(ValidationError, match="clean repository"):
        run_terminal_from_artifacts(
            paths=_production_paths(tmp_path),
            scorer=FakeScorer(),
            output_directory=tmp_path / "result",
        )


def test_direct_materialized_production_api_is_forbidden(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="run_terminal_from_artifacts"):
        run_terminal_evaluation(
            inputs=_inputs(),
            scorer=FakeScorer(),
            output_directory=tmp_path / "forbidden",
        )


def test_prepublication_reserve_mutation_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    frozen = selection()
    vitamin, _m3, _git = evaluation_rows()
    preflight = {"schema_version": "test-preflight-v1"}
    proof = {"ordered_row_identity_sha256": evaluation_rows_identity_sha256(vitamin)}
    monkeypatch.setattr(
        terminal,
        "_production_preflight",
        lambda *_args, **_kwargs: (
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            preflight,
        ),
    )
    changed = (replace(vitamin[0], claim="mutated"), *vitamin[1:])
    monkeypatch.setattr(
        terminal,
        "verify_terminal_reserve",
        lambda **_kwargs: (changed, proof),
    )
    with pytest.raises(ValidationError, match="reserve changed"):
        _assert_prepublication_provenance(
            paths=_production_paths(tmp_path),
            selection=frozen,
            expected_preflight_sha256=canonical_sha256(preflight),
            expected_reserve_rows=vitamin,
            expected_reserve_proof=proof,
            bootstrap_seed=20260720,
            bootstrap_resamples=1000,
        )
