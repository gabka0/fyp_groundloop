"""Development scoring, ablation reporting, and immutable selection sealing."""

from __future__ import annotations

import os
import shutil
import tempfile
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from groundloop.ai.verification.artifacts import tree_digest
from groundloop.errors import ValidationError

from .artifacts import _load_completed_candidate
from .common import (
    canonical_sha256,
    file_sha256,
    load_json,
    require_array,
    require_object,
    require_sha256,
    require_text,
    write_canonical_json,
    write_canonical_jsonl,
)
from .contracts import (
    BASE_LOGIT_ORDER,
    STORED_LABELS,
    EvaluationRow,
    ModelSpec,
    RawLogitRow,
)
from .development import (
    DevelopmentCandidate,
    FrozenM3DevelopmentBaseline,
    NoEligibleSelection,
    preflight_development_bundle_paths,
    seal_selection,
    validate_development_bundle,
    validate_development_decision_evidence,
)
from .metrics import classification_metrics, vitaminc_point_metrics
from .scoring import TerminalScorer, operational_label, stored_probabilities
from .selection import (
    FROZEN_M3_TREE_SHA256,
    REPLICATION_SEEDS,
    DiagnosticCheckpoint,
    SealedSelection,
    load_sealed_selection,
)
from .terminal import _execution_identity

_DEVELOPMENT_VARIANTS = {
    ("V0-frozen-m3", None),
    ("V1-replay-only", 20260720),
    ("V2-ce-mix", 20260720),
    ("V2-ce-mix", 20260721),
    ("V2-ce-mix", 20260722),
    ("V3-margin-mix", 20260720),
    ("V3-margin-mix", 20260721),
    ("V3-margin-mix", 20260722),
    ("A1-margin-no-replay", 20260720),
}


@dataclass(frozen=True, slots=True)
class DevelopmentInputs:
    models: tuple[ModelSpec, ...]
    vitaminc_rows: tuple[EvaluationRow, ...]
    m3_rows: tuple[EvaluationRow, ...]
    dataset_manifest_sha256: str
    vitaminc_manifest_sha256: str
    m3_development_sha256: str


def _alignment_sha256(inputs: DevelopmentInputs) -> str:
    return canonical_sha256(
        {
            "dataset_manifest_sha256": inputs.dataset_manifest_sha256,
            "vitaminc_manifest_sha256": inputs.vitaminc_manifest_sha256,
            "m3_development_sha256": inputs.m3_development_sha256,
            "rows": [
                {
                    "fixture": row.fixture,
                    "row_id": row.row_id,
                    "page_id": row.page_id,
                    "case_id": row.case_id,
                    "claim_group_id": row.claim_group_id,
                    "transition_id": row.transition_id,
                    "stratum": row.stratum,
                    "label": row.label,
                    "claim_sha256": row.claim_sha256,
                    "evidence_sha256": row.evidence_sha256,
                }
                for row in (*inputs.vitaminc_rows, *inputs.m3_rows)
            ],
        }
    )


def _validate_models(models: Sequence[ModelSpec], *, synthetic_test_mode: bool) -> None:
    observed = [(model.variant, model.seed) for model in models]
    if len(observed) != len(set(observed)) or set(observed) != _DEVELOPMENT_VARIANTS:
        raise ValidationError("development requires V0, V1, V2, V3 and A1 exactly")
    if synthetic_test_mode:
        return
    for model in models:
        if tree_digest(Path(model.checkpoint_path)) != model.checkpoint_tree_sha256:
            raise ValidationError(f"development checkpoint drifted: {model.key}")
        if model.variant == "V0-frozen-m3":
            if model.checkpoint_tree_sha256 != FROZEN_M3_TREE_SHA256:
                raise ValidationError("development V0 is not frozen M3")
            continue
        identity_path = Path(model.checkpoint_identity_path)
        if file_sha256(identity_path) != model.model_identity:
            raise ValidationError(
                f"development checkpoint identity drifted: {model.key}"
            )
        identity = load_json(identity_path, f"checkpoint identity {model.key}")
        if identity.get("checkpoint_tree_sha256") != model.checkpoint_tree_sha256:
            raise ValidationError("development checkpoint identity/tree differs")
        completed, _schedule = _load_completed_candidate(
            Path(model.checkpoint_path).parent
        )
        if _development_model_identity(completed) != _development_model_identity(model):
            raise ValidationError(
                f"development completed-run provenance drifted: {model.key}"
            )


def _validate_sources(inputs: DevelopmentInputs) -> None:
    if (
        len(inputs.vitaminc_rows) != 1024
        or len({row.case_id for row in inputs.vitaminc_rows}) != 256
        or len({row.page_id for row in inputs.vitaminc_rows}) != 256
        or any(row.fixture != "vitaminc_development" for row in inputs.vitaminc_rows)
    ):
        raise ValidationError("VitaminC development source is incomplete")
    if (
        len(inputs.m3_rows) != 987
        or len({row.claim_group_id for row in inputs.m3_rows}) != 649
        or any(row.fixture != "m3_development" for row in inputs.m3_rows)
    ):
        raise ValidationError("M3 development source is incomplete")


def _verify_rows(
    *,
    model: ModelSpec,
    source: Sequence[EvaluationRow],
    rows: Sequence[RawLogitRow],
) -> None:
    if len(rows) != len(source):
        raise ValidationError("development scorer returned incomplete rows")
    by_id = {row.row_id: row for row in rows}
    if len(by_id) != len(rows) or by_id.keys() != {row.row_id for row in source}:
        raise ValidationError("development scorer changed row identities")
    for expected in source:
        actual = by_id[expected.row_id]
        uncalibrated = stored_probabilities(actual.base_logits, temperature=1.0)
        old_m3 = stored_probabilities(
            actual.base_logits, temperature=1.1037657679769346
        )
        deployed = stored_probabilities(
            actual.base_logits, temperature=model.temperature
        )
        if (
            actual.model_key != model.key
            or actual.model_identity != model.model_identity
            or actual.calibration_identity != model.calibration_identity
            or actual.fixture != expected.fixture
            or actual.split != expected.split
            or actual.stratum != expected.stratum
            or actual.transition_id != expected.transition_id
            or actual.input_sha256 != expected.input_sha256
            or actual.claim_sha256 != expected.claim_sha256
            or actual.evidence_sha256 != expected.evidence_sha256
            or actual.mapped_label != expected.label
            or actual.page_id != expected.page_id
            or actual.case_id != expected.case_id
            or actual.claim_group_id != expected.claim_group_id
            or actual.uncalibrated_probabilities != uncalibrated
            or actual.old_m3_temperature_probabilities != old_m3
            or actual.calibrated_probabilities != deployed
            or actual.operational_label != operational_label(deployed)
            or actual.temperature != model.temperature
        ):
            raise ValidationError(
                f"development row provenance drifted: {expected.row_id}"
            )


def _development_logit_payload(
    row: RawLogitRow, model: ModelSpec
) -> Mapping[str, object]:
    if row.fixture == "vitaminc_development":
        domain = "vitaminc"
        group_id = row.case_id
    elif row.fixture == "m3_development":
        domain = "m3"
        group_id = row.claim_group_id
    else:
        raise ValidationError("unsupported development fixture")
    if (
        group_id is None
        or row.mapped_label is None
        or model.seed is None
        and model.variant != "V0-frozen-m3"
    ):
        raise ValidationError("development logit identity is incomplete")
    return {
        "schema_version": "groundloop-m4-13-development-logit-v1",
        "split": "development",
        "domain": domain,
        "row_id": row.row_id,
        "group_id": group_id,
        "label": row.mapped_label,
        "claim_sha256": row.claim_sha256,
        "evidence_sha256": row.evidence_sha256,
        "input_sha256": row.input_sha256,
        "page_id": row.page_id,
        "case_id": row.case_id,
        "claim_group_id": row.claim_group_id,
        "transition_id": row.transition_id,
        "stratum": row.stratum,
        "base_logit_order": list(BASE_LOGIT_ORDER),
        "stored_probability_order": list(STORED_LABELS),
        "logits": list(row.base_logits),
        "uncalibrated_probabilities": list(row.uncalibrated_probabilities),
        "old_m3_temperature_probabilities": list(row.old_m3_temperature_probabilities),
        "calibrated_probabilities": list(row.calibrated_probabilities),
        "operational_label": row.operational_label,
        "truncated": row.truncated,
        "temperature": row.temperature,
        "checkpoint_tree_sha256": model.checkpoint_tree_sha256,
        "checkpoint_identity_sha256": model.model_identity,
        "calibration_identity": model.calibration_identity,
        "variant": model.variant,
        "seed": model.seed,
        "contains_raw_text": False,
    }


def _per_class_f1(metrics: Mapping[str, object]) -> Mapping[str, float]:
    per_class = cast(Mapping[str, Mapping[str, object]], metrics["per_class"])
    result: dict[str, float] = {}
    for label in ("support", "refute", "neutral"):
        value = per_class[label]["f1"]
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValidationError("development data must contain every class")
        result[label] = float(value)
    return result


def _contained_bundle_path(root: Path, relative: str) -> Path:
    value = Path(relative)
    if value.is_absolute() or ".." in value.parts or value == Path("."):
        raise ValidationError("development bundle contains an unsafe path")
    resolved_root = root.resolve()
    candidate = resolved_root / value
    result = candidate.resolve()
    if candidate.is_symlink() or resolved_root not in result.parents:
        raise ValidationError("development bundle path escapes its root")
    return result


def _completed_development_replay(
    output_directory: Path,
    *,
    invocation_sha256: str,
    allow_synthetic: bool,
) -> tuple[SealedSelection | NoEligibleSelection, Mapping[str, object]] | None:
    if not output_directory.exists():
        return None
    if not output_directory.is_dir():
        raise ValidationError("development output collides with a non-directory")
    preflight_development_bundle_paths(output_directory / "selection.json")
    seal_path = output_directory / "DEVELOPMENT_COMPLETED.json"
    if not seal_path.is_file():
        raise ValidationError(
            "existing development bundle is partial; use a new output path"
        )
    seal = load_json(seal_path, "development completion seal")
    if (
        seal.get("schema_version") != "groundloop-m4-13-development-complete-v1"
        or seal.get("state") != "COMPLETED"
    ):
        raise ValidationError("development completion seal is invalid")
    if (
        require_sha256(seal.get("invocation_sha256"), "development invocation")
        != invocation_sha256
    ):
        raise ValidationError("development output invocation collision")
    expected_files = {
        "development_report.json",
        "development_runtime.json",
        "selection.json",
        *(
            "development/V0-frozen-m3/development_logits.jsonl"
            if variant == "V0-frozen-m3"
            else f"development/{variant}:seed-{seed}/development_logits.jsonl"
            for variant, seed in _DEVELOPMENT_VARIANTS
        ),
    }
    observed: dict[str, str] = {}
    for raw in require_array(seal.get("files"), "development sealed files"):
        item = require_object(raw, "development sealed file")
        relative = require_text(item.get("path"), "development sealed path")
        if relative in observed:
            raise ValidationError("development completion seal repeats a file")
        observed[relative] = require_sha256(
            item.get("sha256"), "development file hash"
        )
    if set(observed) != expected_files:
        raise ValidationError("development sealed file surface drifted")
    for relative, expected in observed.items():
        if file_sha256(_contained_bundle_path(output_directory, relative)) != expected:
            raise ValidationError(f"sealed development artifact drifted: {relative}")
    actual_files = {
        path.relative_to(output_directory).as_posix()
        for path in output_directory.rglob("*")
        if path.is_file() or path.is_symlink()
    }
    if actual_files != expected_files | {"DEVELOPMENT_COMPLETED.json"}:
        raise ValidationError("development bundle contains extra or missing files")
    selection_path = output_directory / "selection.json"
    report_path = output_directory / "development_report.json"
    runtime_path = output_directory / "development_runtime.json"
    selection_payload = load_json(selection_path, "sealed development selection")
    report = load_json(report_path, "development report")
    runtime = load_json(runtime_path, "development runtime")
    if (
        seal.get("selection_sha256") != file_sha256(selection_path)
        or seal.get("development_report_sha256") != file_sha256(report_path)
        or seal.get("runtime_sha256") != file_sha256(runtime_path)
        or report.get("invocation_sha256") != invocation_sha256
        or runtime.get("invocation_sha256") != invocation_sha256
        or runtime.get("schema_version")
        != "groundloop-m4-13-development-runtime-v1"
    ):
        raise ValidationError("development completion hash chain drifted")
    terminal_authorized = selection_payload.get("selected_variant") is not None
    if seal.get("terminal_evaluation_authorized") is not terminal_authorized:
        raise ValidationError("development completion authorization drifted")
    if selection_payload.get("selected_variant") is None:
        objective = require_object(
            selection_payload.get("objective_design_verdict"),
            "negative objective-design verdict",
        )
        if (
            objective.get("verdict") != "NO_ELIGIBLE_SELECTION"
            or selection_payload.get("terminal_evaluation_authorized") is not False
        ):
            raise ValidationError("negative development selection is malformed")
        selection: SealedSelection | NoEligibleSelection = NoEligibleSelection(
            path=selection_path,
            file_sha256=file_sha256(selection_path),
        )
        if (
            validate_development_decision_evidence(
                selection_path, allow_synthetic=allow_synthetic
            )
            is not None
        ):
            raise ValidationError("negative decision replay selected a variant")
    else:
        selection = load_sealed_selection(selection_path)
        validate_development_bundle(selection, allow_synthetic=allow_synthetic)
    return selection, report


def _development_model_identity(model: ModelSpec) -> Mapping[str, object]:
    return {
        "model_key": model.key,
        "variant": model.variant,
        "seed": model.seed,
        "checkpoint_tree_sha256": model.checkpoint_tree_sha256,
        "checkpoint_identity_sha256": model.model_identity,
        "run_complete_sha256": model.run_complete_sha256,
        "training_manifest_sha256": model.training_manifest_sha256,
        "batch_schedule_sha256": model.batch_schedule_sha256,
        "training_git_head": model.training_git_head,
        "training_repository_dirty": model.training_repository_dirty,
        "trainer_implementation_sha256": model.trainer_implementation_sha256,
        "trainer_implementation_files_sha256": (
            None
            if model.trainer_implementation_files_sha256 is None
            else dict(sorted(model.trainer_implementation_files_sha256.items()))
        ),
    }


def _development_candidates(
    *,
    by_key: Mapping[str, ModelSpec],
    metrics_by_model: Mapping[str, Mapping[str, object]],
    raw_hashes: Mapping[str, str],
    alignment_sha256: str,
) -> list[DevelopmentCandidate]:
    candidates: list[DevelopmentCandidate] = []
    for variant in ("V2-ce-mix", "V3-margin-mix"):
        for seed in REPLICATION_SEEDS:
            key = f"{variant}:seed-{seed}"
            model = by_key[key]
            metrics = metrics_by_model[key]
            vitamin = cast(Mapping[str, object], metrics["vitaminc"])
            transition = cast(Mapping[str, object], vitamin["transition"])
            point = cast(Mapping[str, object], transition["point"])
            m3 = cast(Mapping[str, object], metrics["m3"])
            candidates.append(
                DevelopmentCandidate(
                    variant=variant,
                    seed=seed,
                    checkpoint_relative_path=(f"runs/{variant}/{seed}/checkpoint"),
                    checkpoint_tree_sha256=model.checkpoint_tree_sha256,
                    checkpoint_identity_sha256=model.model_identity,
                    development_logits_path=(
                        f"development/{key}/development_logits.jsonl"
                    ),
                    development_logits_sha256=raw_hashes[key],
                    source_alignment_sha256=alignment_sha256,
                    run_complete_sha256=cast(str, model.run_complete_sha256),
                    training_manifest_sha256=cast(str, model.training_manifest_sha256),
                    batch_schedule_sha256=cast(str, model.batch_schedule_sha256),
                    training_git_head=cast(str, model.training_git_head),
                    training_repository_dirty=cast(
                        bool, model.training_repository_dirty
                    ),
                    trainer_implementation_sha256=cast(
                        str, model.trainer_implementation_sha256
                    ),
                    trainer_implementation_files_sha256=cast(
                        Mapping[str, str],
                        model.trainer_implementation_files_sha256,
                    ),
                    vitaminc_joint_correct=float(cast(float, point["joint_correct"])),
                    m3_macro_f1=float(cast(float, m3["macro_f1"])),
                    m3_per_class_f1=_per_class_f1(m3),
                )
            )
    return candidates


def _development_diagnostics(
    *,
    by_key: Mapping[str, ModelSpec],
    raw_hashes: Mapping[str, str],
    alignment_sha256: str,
) -> list[DiagnosticCheckpoint]:
    diagnostics: list[DiagnosticCheckpoint] = []
    for variant in ("V1-replay-only", "A1-margin-no-replay"):
        key = f"{variant}:seed-20260720"
        model = by_key[key]
        diagnostics.append(
            DiagnosticCheckpoint(
                variant=variant,
                seed=20260720,
                checkpoint_tree_sha256=model.checkpoint_tree_sha256,
                checkpoint_identity_sha256=model.model_identity,
                development_logits_path=(f"development/{key}/development_logits.jsonl"),
                development_logits_sha256=raw_hashes[key],
                source_alignment_sha256=alignment_sha256,
                run_complete_sha256=cast(str, model.run_complete_sha256),
                training_manifest_sha256=cast(str, model.training_manifest_sha256),
                batch_schedule_sha256=cast(str, model.batch_schedule_sha256),
                training_git_head=cast(str, model.training_git_head),
                training_repository_dirty=cast(bool, model.training_repository_dirty),
                trainer_implementation_sha256=cast(
                    str, model.trainer_implementation_sha256
                ),
                trainer_implementation_files_sha256=cast(
                    Mapping[str, str], model.trainer_implementation_files_sha256
                ),
            )
        )
    return diagnostics


def run_development_evaluation(
    *,
    inputs: DevelopmentInputs,
    scorer: TerminalScorer,
    output_directory: Path,
    synthetic_test_mode: bool = False,
    failure_stage: str | None = None,
) -> tuple[SealedSelection | NoEligibleSelection, Mapping[str, object]]:
    """Failure-atomically publish nine raw outputs, report, and selection."""
    _validate_models(inputs.models, synthetic_test_mode=synthetic_test_mode)
    _validate_sources(inputs)
    execution_identity = _execution_identity()
    if not synthetic_test_mode and execution_identity["repository_dirty"] is not False:
        raise ValidationError(
            "production development evaluation requires a clean repository"
        )
    alignment_sha256 = _alignment_sha256(inputs)
    invocation = {
        "schema_version": "groundloop-m4-13-development-invocation-v1",
        "evaluation_mode": (
            "synthetic-fixture" if synthetic_test_mode else "production"
        ),
        "scientific_result": not synthetic_test_mode,
        "source_alignment_sha256": alignment_sha256,
        "dataset_manifest_sha256": inputs.dataset_manifest_sha256,
        "vitaminc_manifest_sha256": inputs.vitaminc_manifest_sha256,
        "m3_development_sha256": inputs.m3_development_sha256,
        "execution_identity": execution_identity,
        "models": [
            _development_model_identity(model)
            for model in sorted(inputs.models, key=lambda item: item.key)
        ],
    }
    invocation_sha256 = canonical_sha256(invocation)
    replay = _completed_development_replay(
        output_directory,
        invocation_sha256=invocation_sha256,
        allow_synthetic=synthetic_test_mode,
    )
    if replay is not None:
        return replay

    output_directory.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{output_directory.name}.partial-",
            dir=output_directory.parent,
        )
    )
    started = time.perf_counter()
    try:
        raw_by_model: dict[str, tuple[RawLogitRow, ...]] = {}
        raw_hashes: dict[str, str] = {}
        telemetry: list[Mapping[str, object]] = []
        for model in sorted(inputs.models, key=lambda item: item.key):
            combined: list[RawLogitRow] = []
            for source in (inputs.vitaminc_rows, inputs.m3_rows):
                result = scorer.score(model, source)
                if result.failures or result.timeouts:
                    raise ValidationError(
                        "development scorer returned failures/timeouts"
                    )
                _verify_rows(model=model, source=source, rows=result.rows)
                combined.extend(result.rows)
                telemetry.append(
                    {
                        "model_key": model.key,
                        "fixture": source[0].fixture,
                        "rows": len(result.rows),
                        "batches": result.batches,
                        "wall_seconds_including_model_load": (
                            result.wall_seconds_including_model_load
                        ),
                        "peak_rss_kib": result.peak_rss_kib,
                        "truncated_rows": sum(row.truncated for row in result.rows),
                        "failures": result.failures,
                        "timeouts": result.timeouts,
                    }
                )
            raw_by_model[model.key] = tuple(combined)
            relative = f"development/{model.key}/development_logits.jsonl"
            raw_hashes[model.key] = write_canonical_jsonl(
                staging / relative,
                [_development_logit_payload(row, model) for row in combined],
            )
        if failure_stage == "after_raw_logits":
            raise RuntimeError("injected failure after development logits")

        metrics_by_model: dict[str, Mapping[str, object]] = {}
        for model in sorted(inputs.models, key=lambda item: item.key):
            rows = raw_by_model[model.key]
            vitamin = tuple(
                row for row in rows if row.fixture == "vitaminc_development"
            )
            m3 = tuple(row for row in rows if row.fixture == "m3_development")
            metrics_by_model[model.key] = {
                "vitaminc": vitaminc_point_metrics(vitamin),
                "m3": classification_metrics(m3),
                "development_logits_sha256": raw_hashes[model.key],
                "scoring_counts": {
                    "rows": len(rows),
                    "truncated_rows": sum(row.truncated for row in rows),
                    "failures": 0,
                    "timeouts": 0,
                    "operational_label_counts": dict(
                        sorted(Counter(row.operational_label for row in rows).items())
                    ),
                },
            }
        report = {
            "schema_version": "groundloop-m4-13-development-report-v1",
            "invocation_sha256": invocation_sha256,
            "source_alignment_sha256": alignment_sha256,
            "dataset_manifest_sha256": inputs.dataset_manifest_sha256,
            "vitaminc_manifest_sha256": inputs.vitaminc_manifest_sha256,
            "m3_development_sha256": inputs.m3_development_sha256,
            "models": metrics_by_model,
            "terminal_data_accessed": False,
            "mandatory_diagnostic_ablations": [
                "V1-replay-only",
                "A1-margin-no-replay",
            ],
            "evaluation_mode": (
                "synthetic-fixture" if synthetic_test_mode else "production"
            ),
            "scientific_result": not synthetic_test_mode,
            "execution_identity": execution_identity,
            "model_identities": {
                model.key: _development_model_identity(model)
                for model in sorted(inputs.models, key=lambda item: item.key)
            },
        }
        report_sha256 = write_canonical_json(
            staging / "development_report.json", report
        )
        by_key = {model.key: model for model in inputs.models}
        baseline_metrics = cast(
            Mapping[str, object], metrics_by_model["V0-frozen-m3"]["m3"]
        )
        baseline = FrozenM3DevelopmentBaseline(
            macro_f1=float(cast(float, baseline_metrics["macro_f1"])),
            per_class_f1=_per_class_f1(baseline_metrics),
            checkpoint_tree_sha256=FROZEN_M3_TREE_SHA256,
            development_logits_sha256=raw_hashes["V0-frozen-m3"],
            source_alignment_sha256=alignment_sha256,
            development_logits_path=(
                "development/V0-frozen-m3/development_logits.jsonl"
            ),
        )
        candidates = _development_candidates(
            by_key=by_key,
            metrics_by_model=metrics_by_model,
            raw_hashes=raw_hashes,
            alignment_sha256=alignment_sha256,
        )
        diagnostics = _development_diagnostics(
            by_key=by_key,
            raw_hashes=raw_hashes,
            alignment_sha256=alignment_sha256,
        )
        selection = seal_selection(
            staging / "selection.json",
            candidates=candidates,
            baseline=baseline,
            development_report_path="development_report.json",
            development_report_sha256=report_sha256,
            diagnostic_checkpoints=diagnostics,
        )
        runtime_payload = {
            "schema_version": "groundloop-m4-13-development-runtime-v1",
            "invocation_sha256": invocation_sha256,
            "total_wall_seconds": time.perf_counter() - started,
            "scoring": telemetry,
        }
        runtime_sha256 = write_canonical_json(
            staging / "development_runtime.json", runtime_payload
        )
        files = [
            {
                "path": f"development/{key}/development_logits.jsonl",
                "sha256": digest,
            }
            for key, digest in sorted(raw_hashes.items())
        ] + [
            {"path": "development_report.json", "sha256": report_sha256},
            {"path": "selection.json", "sha256": selection.file_sha256},
            {"path": "development_runtime.json", "sha256": runtime_sha256},
        ]
        if failure_stage == "before_bundle_seal":
            raise RuntimeError("injected failure before development bundle seal")
        write_canonical_json(
            staging / "DEVELOPMENT_COMPLETED.json",
            {
                "schema_version": "groundloop-m4-13-development-complete-v1",
                "state": "COMPLETED",
                "invocation_sha256": invocation_sha256,
                "files": files,
                "selection_sha256": selection.file_sha256,
                "development_report_sha256": report_sha256,
                "runtime_sha256": runtime_sha256,
                "terminal_evaluation_authorized": (
                    selection.selected_variant is not None
                ),
            },
        )
        _validate_models(inputs.models, synthetic_test_mode=synthetic_test_mode)
        if canonical_sha256(_execution_identity()) != canonical_sha256(
            execution_identity
        ):
            raise ValidationError(
                "development evaluator identity changed during evaluation"
            )
        staged_replay = _completed_development_replay(
            staging,
            invocation_sha256=invocation_sha256,
            allow_synthetic=synthetic_test_mode,
        )
        if staged_replay is None:
            raise AssertionError("completed development staging bundle did not replay")
        if isinstance(staged_replay[0], SealedSelection):
            validate_development_bundle(
                staged_replay[0], allow_synthetic=synthetic_test_mode
            )
        if output_directory.exists():
            raise ValidationError("development output appeared before atomic publish")
        os.replace(staging, output_directory)
        replayed = _completed_development_replay(
            output_directory,
            invocation_sha256=invocation_sha256,
            allow_synthetic=synthetic_test_mode,
        )
        if replayed is None:
            raise AssertionError("published development bundle did not replay")
        return replayed
    finally:
        shutil.rmtree(staging, ignore_errors=True)
