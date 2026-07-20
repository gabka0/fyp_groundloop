"""Raw-first, sealed terminal evaluation orchestration for M4.13."""

from __future__ import annotations

import importlib.metadata
import os
import platform
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from groundloop.ai.verification.artifacts import tree_digest
from groundloop.errors import ValidationError

from .artifacts import (
    _load_completed_candidate,
    _validate_calibration,
    load_terminal_models,
)
from .common import (
    canonical_sha256,
    file_sha256,
    load_json,
    load_jsonl,
    require_array,
    require_number,
    require_object,
    require_sha256,
    require_text,
    write_canonical_json,
    write_canonical_jsonl,
)
from .contracts import EvaluationRow, ModelSpec, RawLogitRow
from .development import (
    preflight_development_bundle_paths,
    validate_development_bundle,
    validate_selection_evidence,
)
from .metrics import evaluate_m3, evaluate_vitaminc, summarize_training_seeds
from .provenance import (
    CORRECTED_M4_10_IDENTITIES,
    M4_12_FROZEN_IDENTITIES,
    M4_13_CONFIG_SHA256,
    M4_13_DATASET_MANIFEST_SHA256,
    M4_13_SOURCE_MANIFEST_SHA256,
    TERMINAL_RESERVE_IDENTITY_SHA256,
    TERMINAL_RESERVE_SHA256,
    evaluation_rows_identity_sha256,
    load_original_m3_test,
    verify_corrected_m4_10,
    verify_m4_12_diagnostic,
    verify_terminal_reserve,
)
from .scoring import (
    ScoringResult,
    TerminalScorer,
    operational_label,
    stored_probabilities,
)
from .selection import (
    PRIMARY_SEED,
    REPLICATION_SEEDS,
    CandidateCheckpoint,
    DiagnosticCheckpoint,
    SealedSelection,
    load_sealed_selection,
)
from .verdict import SeedGateInput, evaluate_stop_go

_RAW_PATHS = {
    "vitaminc_terminal_reserve": "vitaminc_logits.jsonl",
    "original_m3_public_test": "m3_test_logits.jsonl",
    "corrected_m4_10_git_pilot": "git_pilot_logits.jsonl",
}
_TERMINAL_RESERVE_JSONL_SHA256 = (
    "b7d3c61f041d7000626c4f1ce248c2c01a3dcde52de7f94701247e5efb124796"
)
_RESULT_FILES = frozenset(
    {
        "terminal/vitaminc_logits.jsonl",
        "terminal/m3_test_logits.jsonl",
        "terminal/git_pilot_logits.jsonl",
        "terminal/metrics.json",
        "terminal/bootstrap.json",
        "terminal/semantic_result.json",
        "runtime/timings.json",
    }
)


@dataclass(frozen=True, slots=True)
class TerminalArtifactPaths:
    """Production-only path boundary; reserve rows are opened inside the core."""

    selection: Path
    data_root: Path
    candidate_artifact_root: Path
    m4_12_artifact_root: Path
    corrected_m4_10_root: Path
    v0_checkpoint: Path
    v0_calibration: Path
    final_test_manifest_sha256: str


@dataclass(frozen=True, slots=True)
class TerminalInputs:
    selection: SealedSelection
    models: tuple[ModelSpec, ...]
    vitaminc_rows: tuple[EvaluationRow, ...]
    m3_rows: tuple[EvaluationRow, ...]
    git_rows: tuple[EvaluationRow, ...]
    m3_test_proof: Mapping[str, object]
    m4_12_proof: Mapping[str, object]
    reserve_proof: Mapping[str, object]
    m4_10_proof: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class TerminalResult:
    disposition: str
    result_manifest: Mapping[str, object]


def _model_identity(model: ModelSpec) -> Mapping[str, object]:
    return {
        "model_key": model.key,
        "variant": model.variant,
        "seed": model.seed,
        "checkpoint_tree_sha256": model.checkpoint_tree_sha256,
        "model_identity": model.model_identity,
        "calibration_identity": model.calibration_identity,
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
        "calibration_git_head": model.calibration_git_head,
        "calibration_repository_dirty": model.calibration_repository_dirty,
        "calibrator_implementation_sha256": (model.calibrator_implementation_sha256),
        "calibrator_implementation_files_sha256": (
            None
            if model.calibrator_implementation_files_sha256 is None
            else dict(sorted(model.calibrator_implementation_files_sha256.items()))
        ),
        "calibration_semantic_replay_verified": (
            model.calibration_semantic_replay_verified
        ),
        "temperature": model.temperature,
        "max_length": model.max_length,
        "batch_size": model.batch_size,
    }


def _validate_inputs(inputs: TerminalInputs) -> None:
    if inputs.reserve_proof.get("manifest_sha256") != TERMINAL_RESERVE_SHA256:
        raise ValidationError("terminal inputs do not bind the frozen reserve")
    if inputs.reserve_proof.get("consumed_m4_12_used") is not False:
        raise ValidationError("M4.12 consumed data cannot be a terminal input")
    if inputs.m4_12_proof.get("terminal_baseline") is not False:
        raise ValidationError("M4.12 diagnostic was promoted to terminal baseline")
    if inputs.m4_12_proof.get("adapted_checkpoint_evaluated") is not False:
        raise ValidationError("adapted checkpoint was run on consumed M4.12 data")
    if inputs.m4_10_proof.get("independently_adjudicated_labels") is not False:
        raise ValidationError("this evaluator expects the unadjudicated M4.10 pilot")
    expected_keys = {"V0-frozen-m3"} | {
        f"{inputs.selection.selected_variant}:seed-{seed}" for seed in REPLICATION_SEEDS
    }
    if {model.key for model in inputs.models} != expected_keys:
        raise ValidationError("terminal models must be V0 plus three selected seeds")
    if len({model.key for model in inputs.models}) != len(inputs.models):
        raise ValidationError("terminal model keys must be unique")
    for model in inputs.models:
        inputs.selection.authorize(
            variant=model.variant,
            seed=model.seed,
            checkpoint_tree_sha256=model.checkpoint_tree_sha256,
            checkpoint_identity_sha256=(
                None if model.variant == "V0-frozen-m3" else model.model_identity
            ),
        )
    for rows, fixture, expected in (
        (inputs.vitaminc_rows, "vitaminc_terminal_reserve", 512),
        (inputs.m3_rows, "original_m3_public_test", 358),
        (inputs.git_rows, "corrected_m4_10_git_pilot", 14),
    ):
        if len(rows) != expected or any(row.fixture != fixture for row in rows):
            raise ValidationError(f"terminal {fixture} rows are incomplete")
    expected_row_hashes = (
        (
            inputs.vitaminc_rows,
            inputs.reserve_proof,
            "terminal reserve",
        ),
        (inputs.m3_rows, inputs.m3_test_proof, "M3 public test"),
        (inputs.git_rows, inputs.m4_10_proof, "corrected Git pilot"),
    )
    for rows, proof, name in expected_row_hashes:
        if proof.get("ordered_row_identity_sha256") != (
            evaluation_rows_identity_sha256(rows)
        ):
            raise ValidationError(f"{name} ordered row identity drifted")


def _execution_identity() -> Mapping[str, object]:
    package_root = Path(__file__).resolve().parent
    repository = package_root.parents[1]
    code_paths = tuple(sorted(package_root.rglob("*.py"))) + (
        repository / "src/groundloop/ai/verification/artifacts.py",
        repository / "src/groundloop/errors.py",
    )
    code_files = {
        str(path.relative_to(repository)): file_sha256(path)
        for path in code_paths
        if "__pycache__" not in path.parts
    }
    try:
        commit = subprocess.run(
            ["git", "-C", str(repository), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        dirty_output = subprocess.run(
            [
                "git",
                "-C",
                str(repository),
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        raise ValidationError("cannot derive evaluator repository identity") from error
    dependencies: dict[str, str] = {}
    for package in ("numpy", "safetensors", "tokenizers", "torch", "transformers"):
        try:
            dependencies[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            dependencies[package] = "absent"
    if len(commit) not in {40, 64} or any(
        character not in "0123456789abcdef" for character in commit
    ):
        raise ValidationError("repository commit is not a Git object ID")
    return {
        "repository_commit": commit,
        "repository_dirty": bool(dirty_output),
        "repository_dirty_state_sha256": canonical_sha256(dirty_output),
        "evaluation_code_schema": "groundloop-m4-13-evaluator-implementation-v1",
        "evaluation_code_sha256": canonical_sha256(
            {
                "schema_version": "groundloop-m4-13-evaluator-implementation-v1",
                "files_sha256": code_files,
            }
        ),
        "evaluation_code_files": code_files,
        "python": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "dependencies": dependencies,
        "python_executable_sha256": file_sha256(Path(sys.executable)),
        "inference_determinism": {
            "torch_threads": 8,
            "torch_interop_threads": 1,
            "deterministic_algorithms": True,
            "max_length": 256,
            "tokenizers_parallelism": False,
        },
    }


def _invocation_payload(
    inputs: TerminalInputs,
    *,
    seed: int,
    resamples: int,
    synthetic_test_mode: bool,
    execution_identity: Mapping[str, object] | None = None,
    preflight_sha256: str | None = None,
) -> Mapping[str, object]:
    execution = (
        _execution_identity()
        if execution_identity is None
        else dict(execution_identity)
    )
    if not synthetic_test_mode and execution["repository_dirty"] is not False:
        raise ValidationError(
            "production terminal evaluation requires a clean repository"
        )
    return {
        "schema_version": "groundloop-m4-13-terminal-invocation-v1",
        "evaluation_mode": (
            "synthetic-fixture" if synthetic_test_mode else "production"
        ),
        "scientific_result": not synthetic_test_mode,
        "execution_identity": execution,
        "preflight_sha256": preflight_sha256,
        "selection_sha256": inputs.selection.file_sha256,
        "development_report_sha256": inputs.selection.development_report_sha256,
        "terminal_reserve_sha256": TERMINAL_RESERVE_SHA256,
        "terminal_reserve_identity_sha256": inputs.reserve_proof.get(
            "reserve_identity_sha256"
        ),
        "terminal_reserve_jsonl_sha256": inputs.reserve_proof.get(
            "reserve_jsonl_sha256"
        ),
        "data_provenance": dict(inputs.reserve_proof),
        "original_m3_public_test": dict(inputs.m3_test_proof),
        "m4_12": dict(inputs.m4_12_proof),
        "m4_10": dict(inputs.m4_10_proof),
        "models": [
            _model_identity(model)
            for model in sorted(inputs.models, key=lambda item: item.key)
        ],
        "bootstrap_seed": seed,
        "bootstrap_resamples": resamples,
        "ece_bins": 10,
        "frozen_policy": {
            "version": "m3-policy-v1",
            "support_threshold": 0.8,
            "refute_threshold": 0.8,
            "tie_rule_version": "v1",
        },
    }


def _validate_model_artifacts(
    inputs: TerminalInputs, *, synthetic_test_mode: bool
) -> Mapping[str, bool]:
    if synthetic_test_mode:
        return {model.key: True for model in inputs.models}
    calibration_valid: dict[str, bool] = {}
    for model in inputs.models:
        if tree_digest(Path(model.checkpoint_path)) != model.checkpoint_tree_sha256:
            raise ValidationError(f"checkpoint tree drifted for {model.key}")
        calibration_path = Path(model.calibration_path)
        if file_sha256(calibration_path) != model.calibration_identity:
            raise ValidationError(f"calibration artifact drifted for {model.key}")
        calibration = load_json(calibration_path, f"calibration for {model.key}")
        if model.variant == "V0-frozen-m3":
            if model.checkpoint_tree_sha256 != (
                "81870b683cec57eff82665103fcff3a35f45b9c9be0e18b53dcd40f485bfa4cf"
            ) or model.calibration_identity != (
                "d0ec9d23ded61fbcfaccc550e0486ce89845685abb4f27a0ca20e22d4934b873"
            ):
                raise ValidationError("V0 model/calibration is not the frozen M3 pair")
            observed_temperature = calibration.get("temperature")
            if (
                not isinstance(observed_temperature, (int, float))
                or isinstance(observed_temperature, bool)
                or float(observed_temperature) != model.temperature
            ):
                raise ValidationError("V0 calibration temperature drifted")
            calibration_valid[model.key] = True
            continue
        identity_path = Path(model.checkpoint_identity_path)
        if file_sha256(identity_path) != model.model_identity:
            raise ValidationError(f"checkpoint identity drifted for {model.key}")
        identity = load_json(identity_path, f"checkpoint identity for {model.key}")
        if (
            require_sha256(
                identity.get("checkpoint_tree_sha256"), "checkpoint tree identity"
            )
            != model.checkpoint_tree_sha256
        ):
            raise ValidationError("checkpoint identity does not bind its tree")
        checkpoint = next(
            (
                item
                for item in inputs.selection.selected_checkpoints
                if item.variant == model.variant and item.seed == model.seed
            ),
            None,
        )
        if checkpoint is None:
            raise ValidationError("terminal candidate is absent from selection")
        completed, _schedule = _load_completed_candidate(
            Path(model.checkpoint_path).parent
        )
        completed_fields = (
            completed.variant,
            completed.seed,
            completed.checkpoint_tree_sha256,
            completed.model_identity,
            completed.run_complete_sha256,
            completed.training_manifest_sha256,
            completed.batch_schedule_sha256,
            completed.training_git_head,
            completed.training_repository_dirty,
            completed.trainer_implementation_sha256,
            tuple(
                sorted((completed.trainer_implementation_files_sha256 or {}).items())
            ),
        )
        model_fields = (
            model.variant,
            model.seed,
            model.checkpoint_tree_sha256,
            model.model_identity,
            model.run_complete_sha256,
            model.training_manifest_sha256,
            model.batch_schedule_sha256,
            model.training_git_head,
            model.training_repository_dirty,
            model.trainer_implementation_sha256,
            tuple(sorted((model.trainer_implementation_files_sha256 or {}).items())),
        )
        if completed_fields != model_fields:
            raise ValidationError("terminal candidate training provenance drifted")
        replay = _validate_calibration(
            path=calibration_path,
            selection=inputs.selection,
            checkpoint=checkpoint,
        )
        if (
            replay.file_sha256 != model.calibration_identity
            or replay.deployed_temperature != model.temperature
            or replay.git_head != model.calibration_git_head
            or replay.repository_dirty != model.calibration_repository_dirty
            or replay.implementation_sha256 != model.calibrator_implementation_sha256
            or dict(replay.implementation_files_sha256)
            != dict(model.calibrator_implementation_files_sha256 or {})
            or model.calibration_semantic_replay_verified is not True
        ):
            raise ValidationError("terminal calibration replay/provenance drifted")
        accepted = calibration.get("accepted")
        reasons = calibration.get("rejection_reasons")
        if accepted is True:
            calibration_valid[model.key] = True
        elif (
            accepted is False
            and model.temperature == 1.0
            and isinstance(reasons, list)
            and reasons
        ):
            calibration_valid[model.key] = True
        else:
            raise ValidationError("calibration neither passed nor explicitly fell back")
    return calibration_valid


def _validate_provenance(inputs: TerminalInputs, *, synthetic_test_mode: bool) -> bool:
    if synthetic_test_mode:
        return True
    if inputs.m4_12_proof.get("schema_version") != (
        "groundloop-m4-13-consumed-diagnostic-proof-v1"
    ):
        raise ValidationError("M4.12 proof schema is absent")
    for key, expected in M4_12_FROZEN_IDENTITIES.items():
        if inputs.m4_12_proof.get(key) != expected:
            raise ValidationError(f"M4.12 proof drifted: {key}")
    reserve_jsonl_sha256 = require_sha256(
        inputs.reserve_proof.get("reserve_jsonl_sha256"),
        "terminal reserve JSONL hash",
    )
    if (
        inputs.reserve_proof.get("schema_version")
        != "groundloop-m4-13-terminal-unlock-proof-v1"
        or inputs.reserve_proof.get("selection_sha256") != inputs.selection.file_sha256
        or inputs.reserve_proof.get("manifest_sha256") != TERMINAL_RESERVE_SHA256
        or inputs.reserve_proof.get("reserve_identity_sha256")
        != TERMINAL_RESERVE_IDENTITY_SHA256
        or inputs.reserve_proof.get("config_sha256") != M4_13_CONFIG_SHA256
        or inputs.reserve_proof.get("dataset_manifest_sha256")
        != M4_13_DATASET_MANIFEST_SHA256
        or inputs.reserve_proof.get("source_manifest_sha256")
        != M4_13_SOURCE_MANIFEST_SHA256
        or reserve_jsonl_sha256 != _TERMINAL_RESERVE_JSONL_SHA256
        or inputs.reserve_proof.get("rows") != 512
        or inputs.reserve_proof.get("cases") != 128
        or inputs.reserve_proof.get("pages") != 128
    ):
        raise ValidationError("terminal reserve proof is incomplete or drifted")
    if inputs.m4_10_proof.get("schema_version") != (
        "groundloop-m4-13-corrected-m4-10-proof-v1"
    ):
        raise ValidationError("corrected M4.10 proof schema is absent")
    for key, expected in CORRECTED_M4_10_IDENTITIES.items():
        if inputs.m4_10_proof.get(key) != expected:
            raise ValidationError(f"corrected M4.10 proof drifted: {key}")
    return True


def _verify_scoring_result(
    result: ScoringResult,
    *,
    model: ModelSpec,
    source: Sequence[EvaluationRow],
) -> None:
    if result.failures or result.timeouts or len(result.rows) != len(source):
        raise ValidationError("terminal scorer returned incomplete output")
    by_id = {row.row_id: row for row in result.rows}
    if len(by_id) != len(result.rows) or by_id.keys() != {row.row_id for row in source}:
        raise ValidationError("terminal scorer changed row identities")
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
            or actual.split != expected.split
            or actual.stratum != expected.stratum
            or actual.page_id != expected.page_id
            or actual.case_id != expected.case_id
            or actual.claim_group_id != expected.claim_group_id
            or actual.transition_id != expected.transition_id
            or actual.claim_sha256 != expected.claim_sha256
            or actual.evidence_sha256 != expected.evidence_sha256
            or actual.input_sha256 != expected.input_sha256
            or actual.mapped_label != expected.label
            or actual.fixture != expected.fixture
            or actual.uncalibrated_probabilities != uncalibrated
            or actual.old_m3_temperature_probabilities != old_m3
            or actual.calibrated_probabilities != deployed
            or actual.operational_label != operational_label(deployed)
            or actual.temperature != model.temperature
        ):
            raise ValidationError(f"scorer provenance drifted for {expected.row_id}")


def _probability_triplet(value: object, name: str) -> tuple[float, float, float]:
    values = require_array(value, name)
    if len(values) != 3:
        raise ValidationError(f"{name} must contain exactly three values")
    return cast(
        tuple[float, float, float],
        tuple(require_number(item, name) for item in values),
    )


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return require_text(value, name)


def _reload_raw_logits(path: Path) -> tuple[RawLogitRow, ...]:
    """Reload the immutable staged JSONL and reject derived-field drift."""
    result: list[RawLogitRow] = []
    for payload in load_jsonl(path, "terminal raw logits"):
        if payload.get("schema_version") != "groundloop-m4-13-raw-logit-v1":
            raise ValidationError("terminal raw-logit schema drifted")
        if payload.get("base_logit_order") != [
            "contradiction",
            "entailment",
            "neutral",
        ] or payload.get("stored_probability_order") != [
            "support",
            "refute",
            "neutral",
        ]:
            raise ValidationError("terminal label/logit order drifted")
        logits = _probability_triplet(payload.get("base_logits"), "base logits")
        temperature = require_number(payload.get("temperature"), "temperature")
        uncalibrated = stored_probabilities(logits, temperature=1.0)
        old_m3 = stored_probabilities(logits, temperature=1.1037657679769346)
        calibrated = stored_probabilities(logits, temperature=temperature)
        if (
            _probability_triplet(
                payload.get("uncalibrated_probabilities"),
                "uncalibrated probabilities",
            )
            != uncalibrated
            or _probability_triplet(
                payload.get("old_m3_temperature_probabilities"),
                "old M3 probabilities",
            )
            != old_m3
            or _probability_triplet(
                payload.get("calibrated_probabilities"),
                "calibrated probabilities",
            )
            != calibrated
            or payload.get("operational_label") != operational_label(calibrated)
            or payload.get("contains_raw_text") is not False
            or not isinstance(payload.get("truncated"), bool)
        ):
            raise ValidationError("terminal raw-logit derived fields drifted")
        mapped = payload.get("mapped_label")
        if mapped is not None and mapped not in {"support", "refute", "neutral"}:
            raise ValidationError("terminal mapped label drifted")
        result.append(
            RawLogitRow(
                fixture=require_text(payload.get("fixture"), "fixture"),
                split=require_text(payload.get("split"), "split"),
                stratum=_optional_text(payload.get("stratum"), "stratum"),
                page_id=_optional_text(payload.get("page_id"), "page ID"),
                case_id=_optional_text(payload.get("case_id"), "case ID"),
                claim_group_id=_optional_text(
                    payload.get("claim_group_id"), "claim group ID"
                ),
                transition_id=_optional_text(
                    payload.get("transition_id"), "transition ID"
                ),
                row_id=require_text(payload.get("row_id"), "row ID"),
                claim_sha256=require_sha256(payload.get("claim_sha256"), "claim hash"),
                evidence_sha256=require_sha256(
                    payload.get("evidence_sha256"), "evidence hash"
                ),
                mapped_label=mapped,
                input_sha256=require_sha256(payload.get("input_sha256"), "input hash"),
                base_logits=logits,
                uncalibrated_probabilities=uncalibrated,
                old_m3_temperature_probabilities=old_m3,
                calibrated_probabilities=calibrated,
                operational_label=operational_label(calibrated),
                truncated=bool(payload.get("truncated")),
                model_key=require_text(payload.get("model_key"), "model key"),
                model_identity=require_sha256(
                    payload.get("model_identity"), "model identity"
                ),
                calibration_identity=require_sha256(
                    payload.get("calibration_identity"), "calibration identity"
                ),
                temperature=temperature,
            )
        )
    return tuple(result)


def _verify_reloaded_raw_order(
    *,
    rows: Sequence[RawLogitRow],
    fixture: str,
    models: Sequence[ModelSpec],
    source: Sequence[EvaluationRow],
) -> None:
    eligible = tuple(
        model
        for model in sorted(models, key=lambda item: item.key)
        if fixture != "corrected_m4_10_git_pilot" or model.seed in {None, PRIMARY_SEED}
    )
    expected = tuple((model, row) for model in eligible for row in source)
    if len(rows) != len(expected):
        raise ValidationError(f"reloaded {fixture} raw-logit count drifted")
    for actual, (model, original) in zip(rows, expected, strict=True):
        if (
            actual.fixture != fixture
            or actual.model_key != model.key
            or actual.model_identity != model.model_identity
            or actual.calibration_identity != model.calibration_identity
            or actual.row_id != original.row_id
            or actual.claim_sha256 != original.claim_sha256
            or actual.evidence_sha256 != original.evidence_sha256
            or actual.input_sha256 != original.input_sha256
            or actual.mapped_label != original.label
        ):
            raise ValidationError(
                f"reloaded raw-logit order/provenance drifted: {actual.row_id}"
            )


def _raw_by_model_fixture(
    rows: Sequence[RawLogitRow], model_key: str, fixture: str
) -> tuple[RawLogitRow, ...]:
    result = tuple(
        row for row in rows if row.model_key == model_key and row.fixture == fixture
    )
    if not result:
        raise ValidationError(f"raw terminal rows absent for {model_key}/{fixture}")
    return result


def _delta_lower(bootstrap: Mapping[str, object], surface: str, metric: str) -> float:
    parent = require_object(bootstrap.get(surface), surface)
    intervals = require_object(parent.get("intervals"), f"{surface} intervals")
    metric_row = require_object(intervals.get(metric), f"{metric} interval")
    delta = require_object(metric_row.get("delta"), f"{metric} delta interval")
    value = delta.get("lower")
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValidationError("bootstrap lower bound must be numeric")
    return float(value)


def _delta_upper(bootstrap: Mapping[str, object], surface: str, metric: str) -> float:
    parent = require_object(bootstrap.get(surface), surface)
    intervals = require_object(parent.get("intervals"), f"{surface} intervals")
    metric_row = require_object(intervals.get(metric), f"{metric} interval")
    delta = require_object(metric_row.get("delta"), f"{metric} delta interval")
    value = delta.get("upper")
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValidationError("bootstrap upper bound must be numeric")
    return float(value)


def _point_delta(
    candidate: Mapping[str, object], baseline: Mapping[str, object], name: str
) -> float:
    left = baseline.get(name)
    right = candidate.get(name)
    if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
        raise ValidationError("point metrics must be numeric")
    return float(right) - float(left)


_GIT_CLAIM_IDS = frozenset(
    {
        "claim-bustub-development-ubuntu-22",
        "claim-bustub-grading-ubuntu-22",
        "claim-bustub-wsl-unsupported",
        "claim-dynagox-is-hardware-tee",
        "claim-dynagox-buckets-protected",
        "claim-dynagox-direct-maps-visible",
        "claim-mp-spdz-fixed-16-16",
        "claim-mp-spdz-sbitint-type",
        "claim-mp-spdz-boost-1-81",
        "claim-mp-spdz-set-precision",
    }
)
_OBSOLETE_CONCEPTS = {
    "ubuntu_22_04": (
        "claim-bustub-development-ubuntu-22",
        "claim-bustub-grading-ubuntu-22",
    ),
    "fixed_16_16_precision": ("claim-mp-spdz-fixed-16-16",),
    "boost_1_81": ("claim-mp-spdz-boost-1-81",),
    "sbitint_get_type": ("claim-mp-spdz-sbitint-type",),
    "hardware_tee": ("claim-dynagox-is-hardware-tee",),
}
_STABLE_WARNING_IDS = (
    "claim-bustub-wsl-unsupported",
    "claim-mp-spdz-set-precision",
)
_INSERTED_POSITIVE_IDS = (
    "claim-dynagox-buckets-protected",
    "claim-dynagox-direct-maps-visible",
)


def _claim_state(rows: Sequence[RawLogitRow]) -> str:
    by_evidence: dict[str, str] = {}
    for row in rows:
        previous = by_evidence.setdefault(row.evidence_sha256, row.operational_label)
        if previous != row.operational_label:
            raise ValidationError(
                "one Git evidence identity has contradictory pair decisions"
            )
    support = any(label == "support" for label in by_evidence.values())
    refute = any(label == "refute" for label in by_evidence.values())
    if support and refute:
        return "CONFLICTED"
    if support:
        return "SUPPORTED"
    if refute:
        return "REFUTED"
    return "UNSUPPORTED"


def _git_diagnostic(
    rows: Sequence[RawLogitRow], *, require_frozen_ids: bool
) -> Mapping[str, object]:
    by_model: dict[str, Mapping[str, object]] = {}
    for model_key in dict.fromkeys(row.model_key for row in rows):
        selected = [row for row in rows if row.model_key == model_key]
        grouped = {
            claim_id: tuple(row for row in selected if row.claim_group_id == claim_id)
            for claim_id in dict.fromkeys(
                row.claim_group_id for row in selected if row.claim_group_id is not None
            )
        }
        if require_frozen_ids and (
            len(selected) != 14 or set(grouped) != _GIT_CLAIM_IDS
        ):
            raise ValidationError(
                "Git diagnostic must contain 14 new-version pairs over 10 "
                "frozen claim IDs"
            )
        states = {
            claim_id: _claim_state(claim_rows)
            for claim_id, claim_rows in sorted(grouped.items())
        }
        concepts = {
            concept: {
                "member_claim_ids": list(members),
                "member_states": {
                    claim_id: states.get(claim_id) for claim_id in members
                },
                "state": (
                    "REFUTED"
                    if all(states.get(claim_id) == "REFUTED" for claim_id in members)
                    else "NOT_REFUTED"
                ),
            }
            for concept, members in _OBSOLETE_CONCEPTS.items()
        }
        obsolete_states = {
            claim_id: states.get(claim_id)
            for members in _OBSOLETE_CONCEPTS.values()
            for claim_id in members
        }
        by_model[model_key] = {
            "pairs": len(selected),
            "operational_label_counts": dict(
                sorted(Counter(row.operational_label for row in selected).items())
            ),
            "pair_decisions": [
                {
                    "row_id": row.row_id,
                    "claim_id": row.claim_group_id,
                    "evidence_sha256": row.evidence_sha256,
                    "operational_label": row.operational_label,
                    "probabilities": list(row.calibrated_probabilities),
                }
                for row in selected
            ],
            "claim_states": states,
            "claim_witness_counts": {
                claim_id: {
                    "pairs": len(claim_rows),
                    "distinct_evidence": len(
                        {row.evidence_sha256 for row in claim_rows}
                    ),
                }
                for claim_id, claim_rows in sorted(grouped.items())
            },
            "obsolete_concept_groups": concepts,
            "obsolete_concept_groups_refuted": sum(
                value["state"] == "REFUTED" for value in concepts.values()
            ),
            "obsolete_supported_or_conflicted": sorted(
                claim_id
                for claim_id, state in obsolete_states.items()
                if state in {"SUPPORTED", "CONFLICTED"}
            ),
            "obsolete_supported_or_conflicted_count": sum(
                state in {"SUPPORTED", "CONFLICTED"}
                for state in obsolete_states.values()
            ),
            "stable_negative_warnings": sorted(
                claim_id
                for claim_id in _STABLE_WARNING_IDS
                if states.get(claim_id) in {"REFUTED", "CONFLICTED"}
            ),
            "stable_negative_warning_count": sum(
                states.get(claim_id) in {"REFUTED", "CONFLICTED"}
                for claim_id in _STABLE_WARNING_IDS
            ),
            "stable_unsupported": sorted(
                claim_id
                for claim_id in _STABLE_WARNING_IDS
                if states.get(claim_id) == "UNSUPPORTED"
            ),
            "stable_unsupported_count": sum(
                states.get(claim_id) == "UNSUPPORTED"
                for claim_id in _STABLE_WARNING_IDS
            ),
            "inserted_positive_states": {
                claim_id: states.get(claim_id) for claim_id in _INSERTED_POSITIVE_IDS
            },
        }
    return {
        "models": by_model,
        "aggregation": "distinct-evidence-groundloop-truth-table-v1",
        "new_version_pairs_only": True,
        "temporal_regression_measured": False,
        "independently_adjudicated_labels": False,
        "go_no_go_effect": None,
        "reason": (
            "Fixture-author construction notes are not objective gold; exact "
            "decisions are reported as an unlabelled transfer diagnostic only."
        ),
        "bootstrap": None,
        "bootstrap_reason": "Three Git histories are too small for useful uncertainty.",
    }


def _flatten_classification_points(
    prefix: str, metrics: Mapping[str, object]
) -> dict[str, float]:
    result = {
        f"{prefix}.{name}": float(cast(float, metrics[name]))
        for name in ("accuracy", "macro_f1", "nll", "multiclass_brier", "ece")
    }
    per_class = require_object(metrics.get("per_class"), f"{prefix} per class")
    for label in ("support", "refute", "neutral"):
        values = require_object(per_class.get(label), f"{prefix} {label}")
        support = values.get("support")
        if not isinstance(support, int) or isinstance(support, bool):
            raise ValidationError(f"{prefix} {label} support must be an integer")
        if support == 0:
            continue
        for name in ("precision", "recall", "f1"):
            value = values.get(name)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise ValidationError(f"{prefix} present class {label} has null {name}")
            result[f"{prefix}.{label}.{name}"] = float(value)
    return result


def _flatten_calibration_points(
    prefix: str, ablation: Mapping[str, object]
) -> dict[str, float]:
    result: dict[str, float] = {}
    for surface in (
        "uncalibrated_T1",
        "old_m3_temperature",
        "deployed_temperature",
    ):
        values = require_object(ablation.get(surface), f"{prefix} {surface}")
        result[f"{prefix}.{surface}.nll"] = float(cast(float, values["nll"]))
        rows = int(cast(int, values["rows"]))
        counts = require_object(
            values.get("operational_label_counts"),
            f"{prefix} {surface} operational counts",
        )
        for label in ("support", "refute", "neutral"):
            result[f"{prefix}.{surface}.operational_{label}_proportion"] = (
                int(cast(int, counts.get(label, 0))) / rows
            )
    return result


def _flatten_seed_points(
    *,
    vitamin_candidate: Mapping[str, object],
    m3_candidate: Mapping[str, object],
    vitamin_calibration: Mapping[str, object],
    m3_calibration: Mapping[str, object],
) -> Mapping[str, float]:
    result = _flatten_classification_points(
        "vitaminc.endpoint",
        require_object(vitamin_candidate.get("endpoint"), "VitaminC endpoint"),
    )
    result.update(_flatten_classification_points("m3.endpoint", m3_candidate))
    transition = require_object(
        vitamin_candidate.get("transition"), "VitaminC transition"
    )
    point = require_object(transition.get("point"), "VitaminC transition point")
    for name in ("flip_detected", "joint_correct", "bidirectional_margin"):
        result[f"vitaminc.transition.overall.{name}"] = float(cast(float, point[name]))
    by_stratum = require_object(
        transition.get("by_stratum"), "VitaminC transition strata"
    )
    for stratum in ("support_refute", "support_neutral"):
        values = require_object(by_stratum.get(stratum), stratum)
        for name in ("flip_detected", "joint_correct", "bidirectional_margin"):
            result[f"vitaminc.transition.{stratum}.{name}"] = float(
                cast(float, values[name])
            )
    case = require_object(vitamin_candidate.get("case"), "VitaminC case")
    result["vitaminc.case.overall.case_complete"] = float(
        cast(float, case["case_complete"])
    )
    case_strata = require_object(case.get("by_stratum"), "VitaminC case strata")
    for stratum in ("support_refute", "support_neutral"):
        result[f"vitaminc.case.{stratum}.case_complete"] = float(
            cast(float, case_strata[stratum])
        )
    result.update(
        _flatten_calibration_points("vitaminc.calibration", vitamin_calibration)
    )
    result.update(_flatten_calibration_points("m3.calibration", m3_calibration))
    return result


def _contained_result_path(root: Path, relative: str) -> Path:
    value = Path(relative)
    if value.is_absolute() or value == Path(".") or ".." in value.parts:
        raise ValidationError("terminal result contains an unsafe path")
    resolved_root = root.resolve()
    result = (resolved_root / value).resolve()
    if resolved_root not in result.parents or result.is_symlink():
        raise ValidationError("terminal result path escapes its bundle")
    return result


def _completed_replay(
    output: Path,
    *,
    invocation_sha256: str | None,
    preflight_sha256: str | None,
) -> TerminalResult | None:
    if not output.exists():
        return None
    if not output.is_dir():
        raise ValidationError("terminal output collides with a non-directory")
    seal_path = output / "terminal" / "COMPLETED.json"
    result_path = output / "terminal" / "result_manifest.json"
    if not seal_path.exists() and not result_path.exists():
        raise ValidationError(
            "existing terminal bundle is orphaned/partial; use a new output path"
        )
    if not seal_path.exists() or not result_path.exists():
        raise ValidationError("partial terminal output cannot be treated as complete")
    seal = load_json(seal_path, "terminal completion seal")
    if (
        seal.get("schema_version") != "groundloop-m4-13-terminal-state-v1"
        or require_text(seal.get("state"), "terminal state") != "COMPLETED"
    ):
        raise ValidationError("terminal result is not complete")
    sealed_invocation = require_sha256(
        seal.get("invocation_sha256"), "terminal invocation"
    )
    if invocation_sha256 is not None and sealed_invocation != invocation_sha256:
        raise ValidationError("terminal exact replay invocation differs")
    sealed_preflight = seal.get("preflight_sha256")
    if preflight_sha256 is not None and (
        require_sha256(sealed_preflight, "terminal preflight") != preflight_sha256
    ):
        raise ValidationError("terminal exact replay preflight differs")
    if require_sha256(
        seal.get("result_manifest_sha256"), "result manifest"
    ) != file_sha256(result_path):
        raise ValidationError("sealed terminal result manifest drifted")
    result = load_json(result_path, "terminal result manifest")
    if (
        result.get("schema_version") != "groundloop-m4-13-terminal-result-v1"
        or result.get("state") != "COMPLETED"
        or result.get("invocation_sha256") != sealed_invocation
    ):
        raise ValidationError("terminal result manifest is malformed")
    files = require_array(result.get("files"), "terminal result files")
    observed_paths: set[str] = set()
    for entry in files:
        item = require_object(entry, "terminal result file")
        relative = require_text(item.get("path"), "terminal result path")
        if relative in observed_paths:
            raise ValidationError("terminal result repeats a sealed file")
        observed_paths.add(relative)
        expected = require_sha256(item.get("sha256"), "terminal result file hash")
        if file_sha256(_contained_result_path(output, relative)) != expected:
            raise ValidationError(f"sealed terminal artifact drifted: {relative}")
    if observed_paths != _RESULT_FILES:
        raise ValidationError("terminal result file surface is incomplete or expanded")
    semantic_path = output / "terminal" / "semantic_result.json"
    semantic = load_json(semantic_path, "terminal semantic result")
    semantic_invocation = require_object(
        semantic.get("invocation"), "terminal semantic invocation"
    )
    if (
        canonical_sha256(semantic) != file_sha256(semantic_path)
        or canonical_sha256(semantic_invocation) != sealed_invocation
        or result.get("semantic_result_sha256") != file_sha256(semantic_path)
        or semantic.get("metrics_sha256")
        != file_sha256(output / "terminal" / "metrics.json")
        or semantic.get("bootstrap_sha256")
        != file_sha256(output / "terminal" / "bootstrap.json")
    ):
        raise ValidationError("terminal semantic hash chain drifted")
    if preflight_sha256 is not None and (
        semantic_invocation.get("preflight_sha256") != preflight_sha256
        or result.get("preflight_sha256") != preflight_sha256
        or semantic_invocation.get("evaluation_mode") != "production"
        or semantic_invocation.get("scientific_result") is not True
        or semantic_invocation.get("terminal_reserve_sha256") != TERMINAL_RESERVE_SHA256
        or semantic_invocation.get("terminal_reserve_identity_sha256")
        != TERMINAL_RESERVE_IDENTITY_SHA256
        or semantic_invocation.get("terminal_reserve_jsonl_sha256")
        != _TERMINAL_RESERVE_JSONL_SHA256
        or semantic_invocation.get("bootstrap_seed") != 20260720
        or semantic_invocation.get("bootstrap_resamples") != 1000
    ):
        raise ValidationError("terminal production invocation drifted")
    semantic_raw_hashes = require_object(
        semantic.get("raw_logit_sha256"), "terminal raw-logit hashes"
    )
    reloaded_by_fixture: dict[str, tuple[RawLogitRow, ...]] = {}
    for fixture, raw_name in _RAW_PATHS.items():
        path = output / "terminal" / raw_name
        if semantic_raw_hashes.get(raw_name) != file_sha256(path):
            raise ValidationError("terminal semantic raw-logit hash drifted")
        reloaded_by_fixture[fixture] = _reload_raw_logits(path)
    model_entries = tuple(
        require_object(item, "terminal invocation model")
        for item in require_array(semantic_invocation.get("models"), "models")
    )
    model_keys = tuple(
        require_text(item.get("model_key"), "terminal model key")
        for item in model_entries
    )
    if len(model_keys) != 4 or model_keys != tuple(sorted(set(model_keys))):
        raise ValidationError("terminal invocation model surface drifted")
    primary_git_models = tuple(
        key
        for key in model_keys
        if key == "V0-frozen-m3" or key.endswith(":seed-20260720")
    )
    expected_dimensions = {
        "vitaminc_terminal_reserve": (model_keys, 512),
        "original_m3_public_test": (model_keys, 358),
        "corrected_m4_10_git_pilot": (primary_git_models, 14),
    }
    identity_by_key = {
        require_text(item.get("model_key"), "terminal model key"): (
            require_sha256(item.get("model_identity"), "terminal model identity"),
            require_sha256(
                item.get("calibration_identity"), "terminal calibration identity"
            ),
        )
        for item in model_entries
    }
    for fixture, (expected_models, rows_per_model) in expected_dimensions.items():
        rows = reloaded_by_fixture[fixture]
        if len(rows) != len(expected_models) * rows_per_model:
            raise ValidationError(f"terminal replay dimensions drifted: {fixture}")
        reference_ids: tuple[str, ...] | None = None
        for index, model_key in enumerate(expected_models):
            block = rows[index * rows_per_model : (index + 1) * rows_per_model]
            row_ids = tuple(row.row_id for row in block)
            if reference_ids is None:
                reference_ids = row_ids
            if (
                row_ids != reference_ids
                or len(set(row_ids)) != rows_per_model
                or any(
                    row.model_key != model_key
                    or (row.model_identity, row.calibration_identity)
                    != identity_by_key[model_key]
                    or row.fixture != fixture
                    for row in block
                )
            ):
                raise ValidationError(
                    f"terminal replay raw alignment drifted: {fixture}/{model_key}"
                )
    return TerminalResult("REPLAYED", result)


def _run_terminal_materialized(
    *,
    inputs: TerminalInputs,
    scorer: TerminalScorer,
    output_directory: Path,
    bootstrap_seed: int = 20260720,
    bootstrap_resamples: int = 1000,
    failure_stage: str | None = None,
    synthetic_test_mode: bool = False,
    execution_identity: Mapping[str, object] | None = None,
    preflight_sha256: str | None = None,
    prepublication_revalidator: Callable[[], None] | None = None,
    replay_prechecked: bool = False,
) -> TerminalResult:
    """Private row-materialized implementation after the production unlock."""
    if not synthetic_test_mode and (
        bootstrap_seed != 20260720 or bootstrap_resamples != 1000
    ):
        raise ValidationError(
            "production terminal bootstrap is frozen at seed 20260720 and "
            "1000 resamples"
        )
    _validate_inputs(inputs)
    if not synthetic_test_mode and preflight_sha256 is None:
        raise ValidationError("production terminal execution requires sealed preflight")
    if not synthetic_test_mode:
        validate_selection_evidence(inputs.selection)
    provenance_complete = _validate_provenance(
        inputs, synthetic_test_mode=synthetic_test_mode
    )
    calibration_valid = (
        _validate_model_artifacts(inputs, synthetic_test_mode=True)
        if synthetic_test_mode
        else {model.key: True for model in inputs.models}
    )
    if bootstrap_resamples <= 0:
        raise ValidationError("bootstrap resamples must be positive")
    invocation = _invocation_payload(
        inputs,
        seed=bootstrap_seed,
        resamples=bootstrap_resamples,
        synthetic_test_mode=synthetic_test_mode,
        execution_identity=execution_identity,
        preflight_sha256=preflight_sha256,
    )
    invocation_sha256 = canonical_sha256(invocation)
    if replay_prechecked:
        if output_directory.exists():
            raise ValidationError("terminal output appeared after reserve unlock")
    else:
        replay = _completed_replay(
            output_directory,
            invocation_sha256=invocation_sha256,
            preflight_sha256=preflight_sha256,
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
    terminal = staging / "terminal"
    runtime = staging / "runtime"
    terminal.mkdir(parents=True, exist_ok=True)
    runtime.mkdir(parents=True, exist_ok=True)
    write_canonical_json(
        terminal / "RUNNING.json",
        {
            "schema_version": "groundloop-m4-13-terminal-state-v1",
            "state": "RUNNING",
            "invocation_sha256": invocation_sha256,
        },
    )
    started = time.perf_counter()
    scoring_results: list[tuple[str, str, ScoringResult]] = []
    all_raw: list[RawLogitRow] = []
    try:
        sources = (
            inputs.vitaminc_rows,
            inputs.m3_rows,
            inputs.git_rows,
        )
        for model in sorted(inputs.models, key=lambda item: item.key):
            for source in sources:
                if source[
                    0
                ].fixture == "corrected_m4_10_git_pilot" and model.seed not in {
                    None,
                    PRIMARY_SEED,
                }:
                    continue
                result = scorer.score(model, source)
                _verify_scoring_result(result, model=model, source=source)
                scoring_results.append((model.key, source[0].fixture, result))
                all_raw.extend(result.rows)

        source_by_fixture = {source[0].fixture: source for source in sources}
        raw_hashes: dict[str, str] = {}
        persisted_raw: list[RawLogitRow] = []
        for fixture, filename in _RAW_PATHS.items():
            source = source_by_fixture[fixture]
            indexed = {
                (row.model_key, row.row_id): row
                for row in all_raw
                if row.fixture == fixture
            }
            eligible = tuple(
                model
                for model in sorted(inputs.models, key=lambda item: item.key)
                if fixture != "corrected_m4_10_git_pilot"
                or model.seed in {None, PRIMARY_SEED}
            )
            rows = tuple(
                indexed[(model.key, source_row.row_id)]
                for model in eligible
                for source_row in source
            )
            raw_hashes[filename] = write_canonical_jsonl(
                terminal / filename, [row.payload() for row in rows]
            )
            reloaded = _reload_raw_logits(terminal / filename)
            _verify_reloaded_raw_order(
                rows=reloaded,
                fixture=fixture,
                models=inputs.models,
                source=source,
            )
            persisted_raw.extend(reloaded)
        all_raw = persisted_raw
        if failure_stage == "after_raw_logits":
            raise RuntimeError("injected failure after immutable raw logits")

        baseline_key = "V0-frozen-m3"
        selected_variant = inputs.selection.selected_variant
        deterministic_metrics: dict[str, object] = {}
        deterministic_bootstrap: dict[str, object] = {}
        seed_gate: dict[int, SeedGateInput] = {}
        seed_points: dict[int, Mapping[str, float]] = {}
        baseline_vitaminc = _raw_by_model_fixture(
            all_raw, baseline_key, "vitaminc_terminal_reserve"
        )
        baseline_m3 = _raw_by_model_fixture(
            all_raw, baseline_key, "original_m3_public_test"
        )
        for seed in REPLICATION_SEEDS:
            key = f"{selected_variant}:seed-{seed}"
            candidate_vitaminc = _raw_by_model_fixture(
                all_raw, key, "vitaminc_terminal_reserve"
            )
            candidate_m3 = _raw_by_model_fixture(
                all_raw, key, "original_m3_public_test"
            )
            vitamin = evaluate_vitaminc(
                baseline_vitaminc,
                candidate_vitaminc,
                seed=bootstrap_seed,
                resamples=bootstrap_resamples,
            )
            m3 = evaluate_m3(
                baseline_m3,
                candidate_m3,
                seed=bootstrap_seed,
                resamples=bootstrap_resamples,
            )
            vitamin_baseline = require_object(vitamin["baseline"], "VitaminC baseline")
            vitamin_candidate = require_object(
                vitamin["candidate"], "VitaminC candidate"
            )
            transition_baseline = require_object(
                vitamin_baseline.get("transition"), "baseline transition"
            )
            transition_candidate = require_object(
                vitamin_candidate.get("transition"), "candidate transition"
            )
            baseline_transition_point = require_object(
                transition_baseline.get("point"), "baseline transition point"
            )
            transition_point = require_object(
                transition_candidate.get("point"), "candidate transition point"
            )
            m3_candidate = require_object(m3["candidate"], "candidate M3 metrics")
            vitamin_bootstrap = require_object(
                vitamin["paired_bootstrap"], "VitaminC bootstrap"
            )
            m3_bootstrap = require_object(m3["paired_bootstrap"], "M3 bootstrap")
            joint = float(cast(float, transition_point["joint_correct"]))
            flip = float(cast(float, transition_point["flip_detected"]))
            margin = float(cast(float, transition_point["bidirectional_margin"]))
            seed_points[seed] = _flatten_seed_points(
                vitamin_candidate=vitamin_candidate,
                m3_candidate=m3_candidate,
                vitamin_calibration=require_object(
                    require_object(
                        vitamin.get("endpoint_calibration_ablation"),
                        "VitaminC calibration ablation",
                    ).get("candidate"),
                    "VitaminC candidate calibration",
                ),
                m3_calibration=require_object(
                    require_object(
                        m3.get("calibration_ablation"),
                        "M3 calibration ablation",
                    ).get("candidate"),
                    "M3 candidate calibration",
                ),
            )
            seed_gate[seed] = SeedGateInput(
                seed=seed,
                reserve_joint_correct=joint,
                reserve_flip_detected=flip,
                reserve_bidirectional_margin=margin,
                joint_delta_lower=_delta_lower(
                    vitamin_bootstrap, "transition_by_page", "joint_correct"
                ),
                joint_delta_upper=_delta_upper(
                    vitamin_bootstrap, "transition_by_page", "joint_correct"
                ),
                joint_delta_point=_point_delta(
                    transition_point, baseline_transition_point, "joint_correct"
                ),
                flip_delta_lower=_delta_lower(
                    vitamin_bootstrap, "transition_by_page", "flip_detected"
                ),
                flip_delta_upper=_delta_upper(
                    vitamin_bootstrap, "transition_by_page", "flip_detected"
                ),
                margin_delta_lower=_delta_lower(
                    vitamin_bootstrap,
                    "transition_by_page",
                    "bidirectional_margin",
                ),
                m3_macro_f1=float(cast(float, m3_candidate["macro_f1"])),
                m3_accuracy=float(cast(float, m3_candidate["accuracy"])),
                m3_macro_f1_delta_lower=_delta_lower(
                    {"m3": m3_bootstrap}, "m3", "macro_f1"
                ),
                m3_accuracy_delta_lower=_delta_lower(
                    {"m3": m3_bootstrap}, "m3", "accuracy"
                ),
                calibration_valid=calibration_valid[key],
                complete=(
                    len(candidate_vitaminc) == 512
                    and len(candidate_m3) == 358
                    and not any(
                        result.failures or result.timeouts
                        for model_key, _fixture, result in scoring_results
                        if model_key == key
                    )
                ),
            )
            deterministic_metrics[key] = {
                "vitaminc": {
                    "baseline": vitamin["baseline"],
                    "candidate": vitamin["candidate"],
                    "endpoint_calibration_ablation": vitamin[
                        "endpoint_calibration_ablation"
                    ],
                },
                "m3": {
                    "baseline": m3["baseline"],
                    "candidate": m3["candidate"],
                    "calibration_ablation": m3["calibration_ablation"],
                },
            }
            deterministic_bootstrap[key] = {
                "vitaminc": vitamin["paired_bootstrap"],
                "m3": m3["paired_bootstrap"],
            }

        git_rows = [
            row for row in all_raw if row.fixture == "corrected_m4_10_git_pilot"
        ]
        provenance_complete = provenance_complete and all(calibration_valid.values())
        stop_go = (
            {
                "schema_version": "groundloop-m4-13-stop-go-v1",
                "verdict": "NON_SCIENTIFIC_TEST_ONLY",
                "clauses": [],
                "failed_clauses": [],
                "promotion_authorized": False,
                "terminal_tuning_authorized": False,
            }
            if synthetic_test_mode
            else evaluate_stop_go(seed_gate, provenance_complete=provenance_complete)
        )
        metrics_payload = {
            "schema_version": "groundloop-m4-13-terminal-metrics-v1",
            "models": deterministic_metrics,
            "training_seed_summary": summarize_training_seeds(seed_points),
            "git_transfer_diagnostic": _git_diagnostic(
                git_rows, require_frozen_ids=not synthetic_test_mode
            ),
            "stop_go": stop_go,
        }
        bootstrap_payload = {
            "schema_version": "groundloop-m4-13-terminal-bootstrap-v1",
            "models": deterministic_bootstrap,
            "seed_summary_confidence_interval": None,
            "seed_summary_note": "No confidence interval over three training seeds.",
        }
        metrics_hash = write_canonical_json(terminal / "metrics.json", metrics_payload)
        bootstrap_hash = write_canonical_json(
            terminal / "bootstrap.json", bootstrap_payload
        )
        semantic_payload = {
            "schema_version": "groundloop-m4-13-terminal-semantic-result-v1",
            "invocation": invocation,
            "raw_logit_sha256": dict(sorted(raw_hashes.items())),
            "metrics_sha256": metrics_hash,
            "bootstrap_sha256": bootstrap_hash,
            "stop_go": stop_go,
            "evaluation_mode": invocation["evaluation_mode"],
            "scientific_result": invocation["scientific_result"],
            "scientific_boundary": {
                "system_exactness_changed": False,
                "m4_12_role": "consumed provenance regression only",
                "terminal_reserve_role": "pre-frozen page-disjoint diagnostic",
                "git_pilot_role": "small unlabelled transfer diagnostic",
                "objective_truth_claimed": False,
            },
            "transitive_provenance": {
                "data": dict(inputs.reserve_proof),
                "original_m3_public_test": dict(inputs.m3_test_proof),
                "models": [
                    _model_identity(model)
                    for model in sorted(inputs.models, key=lambda item: item.key)
                ],
                "m4_12_consumed_diagnostic": dict(inputs.m4_12_proof),
                "corrected_m4_10": dict(inputs.m4_10_proof),
            },
        }
        semantic_hash = write_canonical_json(
            terminal / "semantic_result.json", semantic_payload
        )
        if failure_stage == "before_result_manifest":
            raise RuntimeError("injected failure before result manifest")

        timing_payload = {
            "schema_version": "groundloop-m4-13-terminal-runtime-v1",
            "total_wall_seconds": time.perf_counter() - started,
            "score_calls": len(scoring_results),
            "scoring": [
                {
                    "model_key": model_key,
                    "fixture": fixture,
                    "rows": len(result.rows),
                    "batches": result.batches,
                    "wall_seconds_including_model_load": (
                        result.wall_seconds_including_model_load
                    ),
                    "peak_rss_kib": result.peak_rss_kib,
                    "truncated_rows": sum(row.truncated for row in result.rows),
                    "operational_label_counts": dict(
                        sorted(
                            Counter(
                                row.operational_label for row in result.rows
                            ).items()
                        )
                    ),
                    "failures": result.failures,
                    "timeouts": result.timeouts,
                }
                for model_key, fixture, result in scoring_results
            ],
            "platform": platform.platform(),
            "logical_cpus": os.cpu_count(),
            "inference_determinism": require_object(
                invocation["execution_identity"], "execution identity"
            )["inference_determinism"],
        }
        timing_hash = write_canonical_json(runtime / "timings.json", timing_payload)
        files = [
            {"path": f"terminal/{name}", "sha256": digest}
            for name, digest in sorted(raw_hashes.items())
        ] + [
            {"path": "terminal/metrics.json", "sha256": metrics_hash},
            {"path": "terminal/bootstrap.json", "sha256": bootstrap_hash},
            {"path": "terminal/semantic_result.json", "sha256": semantic_hash},
            {"path": "runtime/timings.json", "sha256": timing_hash},
        ]
        result_manifest = {
            "schema_version": "groundloop-m4-13-terminal-result-v1",
            "invocation_sha256": invocation_sha256,
            "semantic_result_sha256": semantic_hash,
            "runtime_timing_sha256": timing_hash,
            "hash_separation": (
                "semantic identity excludes wall time, platform, memory and "
                "runtime telemetry"
            ),
            "files": files,
            "state": "COMPLETED",
            "evaluation_mode": invocation["evaluation_mode"],
            "scientific_result": invocation["scientific_result"],
            "preflight_sha256": preflight_sha256,
            "stop_go": stop_go,
            "execution_identity": invocation["execution_identity"],
            "model_provenance": invocation["models"],
            "data_provenance": {
                "terminal_reserve": dict(inputs.reserve_proof),
                "original_m3_public_test": dict(inputs.m3_test_proof),
                "m4_12": dict(inputs.m4_12_proof),
                "corrected_m4_10": dict(inputs.m4_10_proof),
            },
        }
        result_hash = write_canonical_json(
            terminal / "result_manifest.json", result_manifest
        )
        if failure_stage == "before_result_seal":
            raise RuntimeError("injected failure before completion seal")
        write_canonical_json(
            terminal / "COMPLETED.json",
            {
                "schema_version": "groundloop-m4-13-terminal-state-v1",
                "state": "COMPLETED",
                "invocation_sha256": invocation_sha256,
                "preflight_sha256": preflight_sha256,
                "result_manifest_sha256": result_hash,
            },
        )
        (terminal / "RUNNING.json").unlink(missing_ok=True)
        if prepublication_revalidator is not None:
            prepublication_revalidator()
        self_check = _completed_replay(
            staging,
            invocation_sha256=invocation_sha256,
            preflight_sha256=preflight_sha256,
        )
        if self_check is None:
            raise AssertionError("completed terminal staging bundle did not replay")
        if output_directory.exists():
            raise ValidationError("terminal output appeared before atomic publish")
        os.replace(staging, output_directory)
        return TerminalResult("CREATED", result_manifest)
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _sealed_run_identity(
    record: CandidateCheckpoint | DiagnosticCheckpoint,
) -> tuple[object, ...]:
    return (
        record.variant,
        record.seed,
        record.checkpoint_tree_sha256,
        record.checkpoint_identity_sha256,
        record.run_complete_sha256,
        record.training_manifest_sha256,
        record.batch_schedule_sha256,
        record.training_git_head,
        record.training_repository_dirty,
        record.trainer_implementation_sha256,
        tuple(sorted(record.trainer_implementation_files_sha256.items())),
    )


def _completed_run_identity(model: ModelSpec) -> tuple[object, ...]:
    return (
        model.variant,
        model.seed,
        model.checkpoint_tree_sha256,
        model.model_identity,
        model.run_complete_sha256,
        model.training_manifest_sha256,
        model.batch_schedule_sha256,
        model.training_git_head,
        model.training_repository_dirty,
        model.trainer_implementation_sha256,
        tuple(sorted((model.trainer_implementation_files_sha256 or {}).items())),
    )


def _validate_all_selection_runs(
    selection: SealedSelection, candidate_artifact_root: Path
) -> None:
    """Reopen all six candidate and two diagnostic run bundles."""
    records: tuple[CandidateCheckpoint | DiagnosticCheckpoint, ...] = (
        *selection.checkpoints,
        *selection.diagnostic_checkpoints,
    )
    expected = {
        ("V1-replay-only", 20260720),
        ("V2-ce-mix", 20260720),
        ("V2-ce-mix", 20260721),
        ("V2-ce-mix", 20260722),
        ("V3-margin-mix", 20260720),
        ("V3-margin-mix", 20260721),
        ("V3-margin-mix", 20260722),
        ("A1-margin-no-replay", 20260720),
    }
    if (
        len(records) != 8
        or {(record.variant, record.seed) for record in records} != expected
    ):
        raise ValidationError("selection does not bind all eight frozen runs")
    schedules: dict[tuple[str, int], Mapping[str, object]] = {}
    completed_models: list[ModelSpec] = []
    for record in records:
        run_directory = (
            candidate_artifact_root / "runs" / record.variant / str(record.seed)
        )
        model, schedule = _load_completed_candidate(run_directory)
        if _completed_run_identity(model) != _sealed_run_identity(record):
            raise ValidationError(
                f"selection-bound completed run drifted: {record.variant}/{record.seed}"
            )
        schedules[(record.variant, record.seed)] = schedule
        completed_models.append(model)
    if len({model.checkpoint_tree_sha256 for model in completed_models}) != 8:
        raise ValidationError("selection-bound checkpoint substitution detected")
    trainer_surfaces = {
        (
            model.training_git_head,
            model.trainer_implementation_sha256,
            tuple(sorted((model.trainer_implementation_files_sha256 or {}).items())),
        )
        for model in completed_models
    }
    runtime_surfaces = {
        canonical_sha256(schedule["_runtime_comparability"])
        for schedule in schedules.values()
    }
    if len(trainer_surfaces) != 1 or len(runtime_surfaces) != 1:
        raise ValidationError("selection-bound run environment parity drifted")
    for seed in REPLICATION_SEEDS:
        left = schedules[("V2-ce-mix", seed)]
        right = schedules[("V3-margin-mix", seed)]
        for field in (
            "batches",
            "batch_order_sha256",
            "optimizer_schedule_sha256",
            "schedule_sha256",
            "base_order_class_weights",
        ):
            if left.get(field) != right.get(field):
                raise ValidationError(f"V2/V3 paired run schedule drifted: {field}")


def _terminal_plain_path(path: Path, *, expect_directory: bool) -> Path:
    absolute = Path(os.path.abspath(path))
    for component in (*reversed(absolute.parents), absolute):
        try:
            metadata = os.lstat(component)
        except OSError as error:
            raise ValidationError(
                f"terminal preflight path is absent: {path}"
            ) from error
        if stat.S_ISLNK(metadata.st_mode):
            raise ValidationError(f"terminal preflight path contains a symlink: {path}")
    if expect_directory:
        if not stat.S_ISDIR(metadata.st_mode):
            raise ValidationError(f"terminal preflight expected a directory: {path}")
    elif not stat.S_ISREG(metadata.st_mode):
        raise ValidationError(f"terminal preflight expected a regular file: {path}")
    return absolute


def _terminal_plain_tree(path: Path) -> Path:
    """Metadata-walk a prerequisite tree without opening file contents."""
    root = _terminal_plain_path(path, expect_directory=True)
    for current, directories, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        for name in (*directories, *files):
            candidate = current_path / name
            try:
                metadata = os.lstat(candidate)
            except OSError as error:
                raise ValidationError(
                    f"terminal prerequisite disappeared: {candidate}"
                ) from error
            if stat.S_ISLNK(metadata.st_mode):
                raise ValidationError(
                    f"terminal prerequisite tree contains a symlink: {candidate}"
                )
            if name in directories:
                if not stat.S_ISDIR(metadata.st_mode):
                    raise ValidationError(
                        f"terminal prerequisite contains a non-directory: {candidate}"
                    )
            elif not stat.S_ISREG(metadata.st_mode):
                raise ValidationError(
                    f"terminal prerequisite contains a non-regular file: {candidate}"
                )
    return root


def _preflight_output_path(output_directory: Path, data_root: Path) -> None:
    """Reject reserve aliases before result replay or any prerequisite read."""
    root = _terminal_plain_path(data_root, expect_directory=True)
    sealed_root = _terminal_plain_path(root / "sealed", expect_directory=True)
    output = Path(os.path.abspath(output_directory))
    for component in (*reversed(output.parents), output):
        try:
            metadata = os.lstat(component)
        except FileNotFoundError:
            break
        except OSError as error:
            raise ValidationError(
                f"cannot inspect terminal output: {output}"
            ) from error
        if stat.S_ISLNK(metadata.st_mode):
            raise ValidationError("terminal output path contains a symlink")
    if output == sealed_root or sealed_root in output.parents:
        raise ValidationError("terminal output directory resolves inside sealed data")
    if output.exists():
        _terminal_plain_tree(output)


def _preflight_nonreserve_paths(paths: TerminalArtifactPaths) -> None:
    """Reject reserve aliases and unsafe paths before opening any input file."""
    data_root = _terminal_plain_path(paths.data_root, expect_directory=True)
    sealed_root = _terminal_plain_path(data_root / "sealed", expect_directory=True)
    candidate_root = _terminal_plain_tree(paths.candidate_artifact_root)
    selection = _terminal_plain_path(paths.selection, expect_directory=False)
    expected_selection = candidate_root / "development_bundle" / "selection.json"
    if selection != expected_selection:
        raise ValidationError(
            "selection must be the candidate root's canonical development bundle"
        )
    checked = {
        "selection": selection,
        "candidate artifact root": candidate_root,
        "M4.12 artifact root": _terminal_plain_tree(paths.m4_12_artifact_root),
        "corrected M4.10 root": _terminal_plain_tree(paths.corrected_m4_10_root),
        "V0 checkpoint": _terminal_plain_tree(paths.v0_checkpoint),
        "V0 calibration": _terminal_plain_path(
            paths.v0_calibration, expect_directory=False
        ),
        "original M3 test": _terminal_plain_path(
            data_root / "prepared" / "test_m3.jsonl", expect_directory=False
        ),
    }
    for name, path in checked.items():
        if path == sealed_root or sealed_root in path.parents:
            raise ValidationError(f"non-reserve {name} resolves inside sealed data")
    preflight_development_bundle_paths(selection)


def _production_preflight(
    paths: TerminalArtifactPaths,
    *,
    bootstrap_seed: int,
    bootstrap_resamples: int,
) -> tuple[
    SealedSelection,
    tuple[ModelSpec, ...],
    tuple[EvaluationRow, ...],
    Mapping[str, object],
    tuple[EvaluationRow, ...],
    Mapping[str, object],
    Mapping[str, object],
    Mapping[str, object],
    Mapping[str, object],
]:
    """Validate every non-reserve prerequisite and return a row-free seal."""
    if (
        paths.final_test_manifest_sha256 != TERMINAL_RESERVE_SHA256
        or bootstrap_seed != 20260720
        or bootstrap_resamples != 1000
    ):
        raise ValidationError(
            "production terminal bootstrap/reserve identity is not the frozen protocol"
        )
    _preflight_nonreserve_paths(paths)
    selection = load_sealed_selection(paths.selection)
    development_bundle = validate_development_bundle(selection)
    _validate_all_selection_runs(selection, paths.candidate_artifact_root)
    models = load_terminal_models(
        selection=selection,
        candidate_artifact_root=paths.candidate_artifact_root,
        v0_checkpoint=paths.v0_checkpoint,
        v0_calibration=paths.v0_calibration,
    )
    m4_12 = verify_m4_12_diagnostic(paths.m4_12_artifact_root)
    m3_rows, m3_proof = load_original_m3_test(
        paths.data_root / "prepared" / "test_m3.jsonl"
    )
    git_rows, m4_10 = verify_corrected_m4_10(paths.corrected_m4_10_root)
    execution = _execution_identity()
    if execution.get("repository_dirty") is not False:
        raise ValidationError(
            "production terminal evaluation requires a clean repository"
        )
    preflight = {
        "schema_version": "groundloop-m4-13-terminal-preflight-v1",
        "selection_sha256": selection.file_sha256,
        "development_report_sha256": selection.development_report_sha256,
        "development_bundle": dict(development_bundle),
        "models": [
            _model_identity(model)
            for model in sorted(models, key=lambda item: item.key)
        ],
        "original_m3_public_test": dict(m3_proof),
        "m4_12": dict(m4_12),
        "corrected_m4_10": dict(m4_10),
        "execution_identity": dict(execution),
        "terminal_reserve": {
            "manifest_sha256": TERMINAL_RESERVE_SHA256,
            "identity_sha256": TERMINAL_RESERVE_IDENTITY_SHA256,
            "reserve_jsonl_sha256": _TERMINAL_RESERVE_JSONL_SHA256,
            "config_sha256": M4_13_CONFIG_SHA256,
            "dataset_manifest_sha256": M4_13_DATASET_MANIFEST_SHA256,
            "source_manifest_sha256": M4_13_SOURCE_MANIFEST_SHA256,
        },
        "bootstrap_seed": bootstrap_seed,
        "bootstrap_resamples": bootstrap_resamples,
        "ece_bins": 10,
        "policy_version": "m3-policy-v1",
    }
    return (
        selection,
        models,
        m3_rows,
        m3_proof,
        git_rows,
        m4_10,
        m4_12,
        execution,
        preflight,
    )


def run_terminal_from_artifacts(
    *,
    paths: TerminalArtifactPaths,
    scorer: TerminalScorer,
    output_directory: Path,
    bootstrap_seed: int = 20260720,
    bootstrap_resamples: int = 1000,
    failure_stage: str | None = None,
) -> TerminalResult:
    """Production terminal boundary with replay/collision checks before unlock."""
    _preflight_output_path(output_directory, paths.data_root)
    (
        selection,
        models,
        m3_rows,
        m3_proof,
        git_rows,
        m4_10,
        m4_12,
        execution,
        preflight,
    ) = _production_preflight(
        paths,
        bootstrap_seed=bootstrap_seed,
        bootstrap_resamples=bootstrap_resamples,
    )
    preflight_sha256 = canonical_sha256(preflight)
    replay = _completed_replay(
        output_directory,
        invocation_sha256=None,
        preflight_sha256=preflight_sha256,
    )
    if replay is not None:
        return replay

    vitamin_rows, reserve_proof = verify_terminal_reserve(
        selection=selection,
        final_test_manifest_sha256=paths.final_test_manifest_sha256,
        reserve_path=paths.data_root / "sealed" / "terminal_reserve.jsonl",
        manifest_path=paths.data_root / "sealed" / "terminal_reserve_manifest.json",
        identity_path=paths.data_root / "sealed" / "terminal_reserve_identity.json",
    )

    def revalidate() -> None:
        _assert_prepublication_provenance(
            paths=paths,
            selection=selection,
            expected_preflight_sha256=preflight_sha256,
            expected_reserve_rows=vitamin_rows,
            expected_reserve_proof=reserve_proof,
            bootstrap_seed=bootstrap_seed,
            bootstrap_resamples=bootstrap_resamples,
        )

    return _run_terminal_materialized(
        inputs=TerminalInputs(
            selection=selection,
            models=models,
            vitaminc_rows=vitamin_rows,
            m3_rows=m3_rows,
            git_rows=git_rows,
            m3_test_proof=m3_proof,
            m4_12_proof=m4_12,
            reserve_proof=reserve_proof,
            m4_10_proof=m4_10,
        ),
        scorer=scorer,
        output_directory=output_directory,
        bootstrap_seed=bootstrap_seed,
        bootstrap_resamples=bootstrap_resamples,
        failure_stage=failure_stage,
        synthetic_test_mode=False,
        execution_identity=execution,
        preflight_sha256=preflight_sha256,
        prepublication_revalidator=revalidate,
        replay_prechecked=True,
    )


def _assert_prepublication_provenance(
    *,
    paths: TerminalArtifactPaths,
    selection: SealedSelection,
    expected_preflight_sha256: str,
    expected_reserve_rows: Sequence[EvaluationRow],
    expected_reserve_proof: Mapping[str, object],
    bootstrap_seed: int,
    bootstrap_resamples: int,
) -> None:
    """Recheck long-running evaluator inputs immediately before publication."""
    refreshed = _production_preflight(
        paths,
        bootstrap_seed=bootstrap_seed,
        bootstrap_resamples=bootstrap_resamples,
    )
    if canonical_sha256(refreshed[-1]) != expected_preflight_sha256:
        raise ValidationError(
            "terminal prerequisite identity changed during evaluation"
        )
    refreshed_rows, refreshed_reserve = verify_terminal_reserve(
        selection=selection,
        final_test_manifest_sha256=paths.final_test_manifest_sha256,
        reserve_path=paths.data_root / "sealed" / "terminal_reserve.jsonl",
        manifest_path=paths.data_root / "sealed" / "terminal_reserve_manifest.json",
        identity_path=paths.data_root / "sealed" / "terminal_reserve_identity.json",
    )
    if evaluation_rows_identity_sha256(
        tuple(refreshed_rows)
    ) != evaluation_rows_identity_sha256(
        tuple(expected_reserve_rows)
    ) or canonical_sha256(refreshed_reserve) != canonical_sha256(
        expected_reserve_proof
    ):
        raise ValidationError("terminal reserve changed during evaluation")


def run_terminal_evaluation(
    *,
    inputs: TerminalInputs,
    scorer: TerminalScorer,
    output_directory: Path,
    bootstrap_seed: int = 20260720,
    bootstrap_resamples: int = 1000,
    failure_stage: str | None = None,
    synthetic_test_mode: bool = False,
) -> TerminalResult:
    """Synthetic row-injection API; production must use the artifact boundary."""
    if not synthetic_test_mode and (
        bootstrap_seed != 20260720 or bootstrap_resamples != 1000
    ):
        raise ValidationError(
            "production terminal bootstrap is frozen at seed 20260720 and "
            "1000 resamples"
        )
    if not synthetic_test_mode:
        raise ValidationError(
            "production terminal evaluation requires run_terminal_from_artifacts"
        )
    return _run_terminal_materialized(
        inputs=inputs,
        scorer=scorer,
        output_directory=output_directory,
        bootstrap_seed=bootstrap_seed,
        bootstrap_resamples=bootstrap_resamples,
        failure_stage=failure_stage,
        synthetic_test_mode=True,
    )
