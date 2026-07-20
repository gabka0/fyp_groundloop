"""Fail-closed construction of development and terminal model specifications."""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from groundloop.ai.verification.artifacts import tree_digest
from groundloop.errors import ValidationError

from .common import (
    canonical_sha256,
    file_sha256,
    load_json,
    load_jsonl,
    require_array,
    require_integer,
    require_number,
    require_object,
    require_sha256,
    require_text,
    write_canonical_json,
)
from .contracts import ModelSpec
from .selection import FROZEN_M3_TREE_SHA256, CandidateCheckpoint, SealedSelection

_V0_CALIBRATION_SHA256 = (
    "d0ec9d23ded61fbcfaccc550e0486ce89845685abb4f27a0ca20e22d4934b873"
)
_UNCALIBRATED_DEVELOPMENT_IDENTITY = (
    "63141d5325af81f0a77fb072b61c0f46a8ee005a905ba4cacab3622ab953a5f0"
)
_TRAINING_MANIFEST_SCHEMA = "groundloop-m4-13-training-run-v1"
_CHECKPOINT_IDENTITY_SCHEMA = "groundloop-m4-13-checkpoint-identity-v1"
_COMPLETION_SCHEMA = "groundloop-m4-13-training-complete-v1"
_SCHEDULE_SCHEMA = "groundloop-m4-13-batch-schedule-v1"
_CALIBRATION_SCHEMA = "groundloop-m4-13-group-balanced-temperature-v1"
_TRAINER_IMPLEMENTATION_SCHEMA = "groundloop-m4-13-trainer-implementation-v1"
_TRAINER_IMPLEMENTATION_PATHS = {
    "training/m4_13_verifier/train.py",
    "training/m4_13_verifier/losses.py",
    "training/m4_13_verifier/__init__.py",
    "src/groundloop/ai/verification/artifacts.py",
    "src/groundloop/errors.py",
}
_CALIBRATOR_IMPLEMENTATION_SCHEMA = "groundloop-m4-13-calibrator-implementation-v1"
_CALIBRATOR_IMPLEMENTATION_PATHS = {
    "training/m4_13_verifier/calibrate.py",
    "training/m4_13_verifier/losses.py",
    "training/m4_13_verifier/train.py",
    "training/m4_13_verifier/__init__.py",
    "src/groundloop/ai/verification/artifacts.py",
    "src/groundloop/errors.py",
}
_EXPECTED_DEVELOPMENT_RUNS = {
    ("V1-replay-only", 20260720),
    ("V2-ce-mix", 20260720),
    ("V2-ce-mix", 20260721),
    ("V2-ce-mix", 20260722),
    ("V3-margin-mix", 20260720),
    ("V3-margin-mix", 20260721),
    ("V3-margin-mix", 20260722),
    ("A1-margin-no-replay", 20260720),
}
_FROZEN_TRAINING_INPUTS = {
    "config_sha256": "d50c2af5b5461f74e31fc759c3ca5e0aa074556cf0163789a4fe9615c8b7a11d",
    "dataset_manifest_sha256": (
        "1a5ed4b7933c79cba5a09487a24c7dcd576e3518e1a27808d5f4afcbc3007e60"
    ),
    "train_vitaminc_sha256": (
        "01459cc7b4cbe611c0967dec22b4ab53ef93f336dc71706475e8858d07c7a178"
    ),
    "train_vitaminc_manifest_sha256": (
        "d51ca3366328fd623148d3d9f0c01844aca73212831fef563d4839cf43238c2d"
    ),
    "train_m3_sha256": (
        "1b76ed557b43955980e57a0f049666496335b649caa6e406c2e7b0245e7e3f4e"
    ),
    "continuation_checkpoint_tree_sha256": FROZEN_M3_TREE_SHA256,
    "continuation_weights_sha256": (
        "81c49c30048dcbcc9fb2895b56622f702b4aa9894e7d055f28bb520c09f74e3e"
    ),
}
_FROZEN_CALIBRATION_INPUTS = {
    "dataset_manifest_sha256": (
        "1a5ed4b7933c79cba5a09487a24c7dcd576e3518e1a27808d5f4afcbc3007e60"
    ),
    "m3_development_jsonl_sha256": (
        "a9cd74df8df6e6f7a825ee446d222adc87a7bcf61c9394f45efaacc1cb479882"
    ),
    "vitaminc_development_manifest_sha256": (
        "527e727273af7b7721509c625b721a489da88503fc994b7bca05215eb93580df"
    ),
    "method": "equal-domain-group-balanced-log-temperature-golden-v1",
    "group_weighting": {
        "domain_weights": {"m3": 0.5, "vitaminc": 0.5},
        "m3": "mean row NLL within claim_group_id, then mean groups",
        "vitaminc": "mean row NLL within case_id, then mean cases",
    },
    "search": {
        "domain": "positive log-temperature",
        "lower": 0.05,
        "upper": 20.0,
        "iterations": 96,
        "maximum_domain_nll_increase": 0.01,
    },
}
_FROZEN_CALIBRATION_COUNTS = {
    "examples": 2011,
    "m3_examples": 987,
    "m3_claim_groups": 649,
    "vitaminc_examples": 1024,
    "vitaminc_cases": 256,
}
_FROZEN_HYPERPARAMETERS = {
    "max_length": 256,
    "microbatch_size": 8,
    "gradient_accumulation_steps": 4,
    "epochs": 1,
    "learning_rate": 1e-5,
    "weight_decay": 0.01,
    "optimizer_kind": "torch-adamw",
    "adam_beta1": 0.9,
    "adam_beta2": 0.999,
    "adam_epsilon": 1e-8,
    "adam_amsgrad": False,
    "adam_maximize": False,
    "adam_foreach": False,
    "adam_capturable": False,
    "adam_differentiable": False,
    "adam_fused": False,
    "scheduler_kind": "transformers-linear-warmup-v1",
    "warmup_ratio": 0.06,
    "gradient_norm_clip": 1.0,
    "torch_threads": 8,
    "torch_interop_threads": 1,
    "deterministic_algorithms": True,
    "paired_margin": 0.5,
    "paired_weight": 0.25,
}
_FROZEN_OPTIMIZER_SCHEDULE = {
    "optimizer": "torch-adamw",
    "scheduler": "transformers-linear-warmup-v1",
    "warmup_steps": 13,
    "total_steps": 223,
}


@dataclass(frozen=True, slots=True)
class CalibrationValidation:
    file_sha256: str
    deployed_temperature: float
    git_head: str
    repository_dirty: bool
    implementation_sha256: str
    implementation_files_sha256: Mapping[str, str]
    semantic_replay_verified: bool


def _group_balanced_nll(
    rows: tuple[Mapping[str, object], ...], temperature: float
) -> Mapping[str, float]:
    grouped: dict[str, defaultdict[str, list[float]]] = {
        "m3": defaultdict(list),
        "vitaminc": defaultdict(list),
    }
    base_index = {"support": 1, "refute": 0, "neutral": 2}
    for row in rows:
        domain = require_text(row.get("domain"), "calibration domain")
        group = require_text(row.get("group_id"), "calibration group")
        label = require_text(row.get("label"), "calibration label")
        logits_raw = require_array(row.get("logits"), "calibration logits")
        if domain not in grouped or label not in base_index or len(logits_raw) != 3:
            raise ValidationError("calibration development row is invalid")
        logits = tuple(
            require_number(value, "calibration logit") for value in logits_raw
        )
        scaled = tuple(value / temperature for value in logits)
        maximum = max(scaled)
        log_denominator = maximum + math.log(
            sum(math.exp(value - maximum) for value in scaled)
        )
        grouped[domain][group].append(log_denominator - scaled[base_index[label]])
    domain_values: dict[str, float] = {}
    for domain in ("m3", "vitaminc"):
        if not grouped[domain]:
            raise ValidationError("calibration development domain is empty")
        group_means = [sum(values) / len(values) for values in grouped[domain].values()]
        domain_values[domain] = sum(group_means) / len(group_means)
    return {
        "m3": domain_values["m3"],
        "vitaminc": domain_values["vitaminc"],
        "combined": 0.5 * domain_values["m3"] + 0.5 * domain_values["vitaminc"],
    }


def _recompute_calibration(
    rows: tuple[Mapping[str, object], ...],
) -> Mapping[str, object]:
    left, right = math.log(0.05), math.log(20.0)
    ratio = (math.sqrt(5.0) - 1.0) / 2.0
    first = right - ratio * (right - left)
    second = left + ratio * (right - left)
    first_loss = _group_balanced_nll(rows, math.exp(first))["combined"]
    second_loss = _group_balanced_nll(rows, math.exp(second))["combined"]
    for _ in range(96):
        if first_loss <= second_loss:
            right = second
            second = first
            second_loss = first_loss
            first = right - ratio * (right - left)
            first_loss = _group_balanced_nll(rows, math.exp(first))["combined"]
        else:
            left = first
            first = second
            first_loss = second_loss
            second = left + ratio * (right - left)
            second_loss = _group_balanced_nll(rows, math.exp(second))["combined"]
    candidate_temperature = math.exp((left + right) / 2.0)
    before = _group_balanced_nll(rows, 1.0)
    candidate = _group_balanced_nll(rows, candidate_temperature)
    reasons: list[str] = []
    if candidate["combined"] >= before["combined"]:
        reasons.append("combined_development_nll_did_not_decrease")
    if candidate["m3"] > before["m3"] + 0.01:
        reasons.append("m3_development_nll_increased_over_0.01")
    if candidate["vitaminc"] > before["vitaminc"] + 0.01:
        reasons.append("vitaminc_development_nll_increased_over_0.01")
    accepted = not reasons
    deployed = candidate_temperature if accepted else 1.0
    return {
        "candidate_temperature": candidate_temperature,
        "deployed_temperature": deployed,
        "accepted": accepted,
        "rejection_reasons": reasons,
        "counts": {
            "examples": len(rows),
            "m3_examples": sum(row.get("domain") == "m3" for row in rows),
            "m3_claim_groups": len(
                {row.get("group_id") for row in rows if row.get("domain") == "m3"}
            ),
            "vitaminc_examples": sum(row.get("domain") == "vitaminc" for row in rows),
            "vitaminc_cases": len(
                {row.get("group_id") for row in rows if row.get("domain") == "vitaminc"}
            ),
        },
        "nll": {
            "uncalibrated": before,
            "old_m3_temperature": {
                "temperature": 1.1037657679769346,
                **_group_balanced_nll(rows, 1.1037657679769346),
            },
            "candidate": candidate,
            "deployed": _group_balanced_nll(rows, deployed),
        },
    }


def _resolve_within(root: Path, value: str, name: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        raise ValidationError(f"{name} must be relative to its artifact root")
    resolved_root = root.resolve()
    resolved = (resolved_root / path).resolve()
    if resolved != resolved_root and resolved_root not in resolved.parents:
        raise ValidationError(f"{name} escapes its artifact root")
    return resolved


def _expect_equal(payload: object, expected: object, name: str) -> None:
    if payload != expected:
        raise ValidationError(f"{name} drifted")


def _validate_schedule_semantics(
    schedule: Mapping[str, object], *, variant: str, seed: int
) -> None:
    batches_raw = require_array(schedule.get("batches"), "batch schedule batches")
    if len(batches_raw) != 890:
        raise ValidationError("batch schedule must materialize exactly 890 batches")
    batch_payload: list[dict[str, object]] = []
    optimizer_boundaries: list[list[object]] = []
    domains: list[str] = []
    base_label_counts: Counter[int] = Counter()
    for index, raw in enumerate(batches_raw):
        batch = require_object(raw, f"batch schedule batch {index}")
        if require_integer(batch.get("batch_index"), "batch index") != index:
            raise ValidationError("batch schedule indices are not contiguous")
        domain = require_text(batch.get("domain"), "batch domain")
        if domain not in {"vitaminc", "m3"}:
            raise ValidationError("batch schedule contains an unsupported domain")
        domains.append(domain)
        optimizer_step = require_integer(batch.get("optimizer_step"), "optimizer step")
        expected_step = index // 4
        if optimizer_step != expected_step:
            raise ValidationError("batch optimizer-step boundary drifted")
        divisor = require_integer(
            batch.get("accumulation_divisor"), "accumulation divisor"
        )
        expected_divisor = 2 if expected_step == 222 else 4
        closes = batch.get("closes_optimizer_step")
        expected_closes = (index + 1) % 4 == 0 or index == 889
        if divisor != expected_divisor or closes is not expected_closes:
            raise ValidationError("batch accumulation schedule drifted")
        transitions = [
            list(require_array(item, "transition"))
            for item in require_array(batch.get("transitions"), "batch transitions")
        ]
        rows_raw = require_array(batch.get("rows"), "batch rows")
        row_payload: list[list[object]] = []
        for raw_row in rows_raw:
            row = require_object(raw_row, "batch row")
            row_payload.append(
                [
                    require_text(row.get("row_id"), "batch row ID"),
                    require_text(row.get("group_id"), "batch group ID"),
                    require_integer(row.get("repeat_index"), "batch repeat index"),
                ]
            )
            base_label = require_integer(row.get("base_label"), "batch base label")
            if base_label not in {0, 1, 2}:
                raise ValidationError("batch base label is invalid")
            base_label_counts[base_label] += 1
        if domain == "vitaminc" and (
            len(row_payload) != 8 or transitions != [[0, 1], [2, 3], [4, 5], [6, 7]]
        ):
            raise ValidationError("VitaminC batch must contain eight endpoints")
        if domain == "m3" and (not 1 <= len(row_payload) <= 8 or transitions):
            raise ValidationError("M3 batch shape drifted")
        batch_payload.append(
            {
                "domain": domain,
                "transitions": transitions,
                "rows": row_payload,
            }
        )
        optimizer_boundaries.append([index, optimizer_step, divisor, closes])
    class_weights_raw = require_array(
        schedule.get("base_order_class_weights"), "schedule class weights"
    )
    class_weights = [
        require_number(value, "schedule class weight") for value in class_weights_raw
    ]
    if len(class_weights) != 3:
        raise ValidationError("schedule must bind three class weights")
    if set(base_label_counts) != {0, 1, 2}:
        raise ValidationError("schedule must contain every base label")
    total_rows = sum(base_label_counts.values())
    expected_class_weights = [
        total_rows / (3.0 * base_label_counts[index]) for index in range(3)
    ]
    if class_weights != expected_class_weights:
        raise ValidationError("schedule class weights are not derivable from rows")
    batch_hash = canonical_sha256(batch_payload)
    optimizer_hash = canonical_sha256(
        {
            "gradient_accumulation_steps": 4,
            "optimizer_steps": 223,
            "warmup_steps": 13,
            "boundaries": optimizer_boundaries,
        }
    )
    family = "mixed" if variant in {"V2-ce-mix", "V3-margin-mix"} else domains[0]
    expected_domain_counts = (
        Counter({"vitaminc": 512, "m3": 378})
        if family == "mixed"
        else Counter({"m3": 890})
        if variant == "V1-replay-only"
        else Counter({"vitaminc": 890})
    )
    if Counter(domains) != expected_domain_counts:
        raise ValidationError("batch schedule domain budget drifted")
    expected_family_name = (
        "mixed-vitaminc-m3-v1" if family == "mixed" else f"cycled-{family}-v1"
    )
    _expect_equal(
        schedule.get("schedule_family"),
        expected_family_name,
        "batch schedule family",
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
    for field, expected in (
        ("batch_order_sha256", batch_hash),
        ("optimizer_schedule_sha256", optimizer_hash),
        ("schedule_sha256", schedule_hash),
    ):
        _expect_equal(schedule.get(field), expected, f"batch schedule {field}")
    _expect_equal(
        schedule.get("gradient_accumulation_steps"),
        4,
        "batch schedule gradient accumulation",
    )
    _expect_equal(
        schedule.get("base_logit_order"),
        ["contradiction", "entailment", "neutral"],
        "batch schedule base-logit order",
    )
    _expect_equal(
        schedule.get("stored_label_order"),
        ["support", "refute", "neutral"],
        "batch schedule stored-label order",
    )


def _validate_calibration(
    *,
    path: Path,
    selection: SealedSelection,
    checkpoint: CandidateCheckpoint,
) -> CalibrationValidation:
    payload = load_json(path, "candidate calibration")
    _expect_equal(
        payload.get("schema_version"), _CALIBRATION_SCHEMA, "calibration schema"
    )
    _expect_equal(payload.get("status"), "complete", "calibration status")
    _expect_equal(
        payload.get("selection_sha256"), selection.file_sha256, "calibration selection"
    )
    _expect_equal(payload.get("selected"), True, "calibration selected flag")
    for field in (
        "variant",
        "seed",
        "checkpoint_tree_sha256",
        "checkpoint_identity_sha256",
    ):
        _expect_equal(
            payload.get(field), getattr(checkpoint, field), f"calibration {field}"
        )
    _expect_equal(
        payload.get("development_logits_sha256"),
        checkpoint.development_logits_sha256,
        "calibration development logits",
    )
    _expect_equal(
        payload.get("source_alignment_sha256"),
        checkpoint.source_alignment_sha256,
        "calibration development source alignment",
    )
    for field, expected in _FROZEN_CALIBRATION_INPUTS.items():
        _expect_equal(payload.get(field), expected, f"calibration {field}")
    repository = require_object(payload.get("repository"), "calibration repository")
    git_head = require_text(repository.get("git_head"), "calibration Git HEAD")
    if (
        len(git_head) not in {40, 64}
        or any(character not in "0123456789abcdef" for character in git_head)
        or repository.get("dirty") is not False
    ):
        raise ValidationError("calibration repository provenance is not clean")
    calibrator = require_object(
        payload.get("calibrator_implementation"), "calibrator implementation"
    )
    _expect_equal(
        calibrator.get("schema_version"),
        _CALIBRATOR_IMPLEMENTATION_SCHEMA,
        "calibrator implementation schema",
    )
    calibrator_files = {
        require_text(name, "calibrator implementation path"): require_sha256(
            digest, "calibrator implementation file hash"
        )
        for name, digest in require_object(
            calibrator.get("files_sha256"), "calibrator implementation files"
        ).items()
    }
    if set(calibrator_files) != _CALIBRATOR_IMPLEMENTATION_PATHS:
        raise ValidationError("calibrator implementation file surface drifted")
    calibrator_sha256 = require_sha256(
        calibrator.get("sha256"), "calibrator implementation hash"
    )
    _expect_equal(
        calibrator_sha256,
        canonical_sha256(
            {
                "schema_version": _CALIBRATOR_IMPLEMENTATION_SCHEMA,
                "files_sha256": dict(sorted(calibrator_files.items())),
            }
        ),
        "calibrator implementation aggregate",
    )
    invocation_fields = (
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
    invocation_payload = {
        "schema_version": "groundloop-m4-13-calibration-invocation-v1",
        **{field: payload.get(field) for field in invocation_fields},
    }
    _expect_equal(
        payload.get("invocation_sha256"),
        canonical_sha256(invocation_payload),
        "calibration invocation",
    )
    version = require_text(payload.get("calibration_version"), "calibration version")
    semantic = dict(payload)
    for field in (
        "schema_version",
        "status",
        "invocation_sha256",
        "calibration_version",
    ):
        semantic.pop(field, None)
    semantic["schema_version"] = "groundloop-m4-13-calibration-invocation-v1"
    expected_version = f"temperature-m4-13-v1:{canonical_sha256(semantic)}"
    if version != expected_version:
        raise ValidationError("unsupported M4.13 calibration version")
    development_path = _resolve_within(
        selection.path.parent,
        checkpoint.development_logits_path,
        "calibration development-logits path",
    )
    development_rows = load_jsonl(development_path, "calibration development logits")
    _expect_equal(
        file_sha256(development_path),
        checkpoint.development_logits_sha256,
        "calibration development-logits file",
    )
    if len(development_rows) != 2011:
        raise ValidationError("calibration development logits are incomplete")
    if any(
        row.get("variant") != checkpoint.variant
        or row.get("seed") != checkpoint.seed
        or row.get("checkpoint_tree_sha256") != checkpoint.checkpoint_tree_sha256
        for row in development_rows
    ):
        raise ValidationError("calibration development logits belong to another model")
    recomputed = _recompute_calibration(development_rows)
    for field in (
        "candidate_temperature",
        "deployed_temperature",
        "accepted",
        "rejection_reasons",
        "counts",
        "nll",
    ):
        if canonical_sha256(payload.get(field)) != canonical_sha256(
            recomputed.get(field)
        ):
            raise ValidationError(f"calibration semantic replay differs: {field}")
    _expect_equal(
        payload.get("counts"), _FROZEN_CALIBRATION_COUNTS, "calibration counts"
    )
    _expect_equal(
        payload.get("base_logit_order"),
        ["contradiction", "entailment", "neutral"],
        "calibration base-logit order",
    )
    _expect_equal(
        payload.get("stored_probability_order"),
        ["support", "refute", "neutral"],
        "calibration stored-probability order",
    )
    deployed = require_number(
        payload.get("deployed_temperature"), "deployed temperature"
    )
    accepted = payload.get("accepted")
    if not isinstance(accepted, bool):
        raise ValidationError("calibration accepted must be boolean")
    reasons = require_array(
        payload.get("rejection_reasons"), "calibration rejection reasons"
    )
    if any(not isinstance(reason, str) or not reason for reason in reasons):
        raise ValidationError("calibration rejection reasons must be non-empty text")
    if accepted:
        candidate = require_number(
            payload.get("candidate_temperature"), "candidate temperature"
        )
        if deployed != candidate or reasons:
            raise ValidationError("accepted calibration has inconsistent deployment")
    elif deployed != 1.0 or not reasons:
        raise ValidationError("rejected calibration must explicitly deploy T=1")
    return CalibrationValidation(
        file_sha256=file_sha256(path),
        deployed_temperature=deployed,
        git_head=git_head,
        repository_dirty=False,
        implementation_sha256=calibrator_sha256,
        implementation_files_sha256=calibrator_files,
        semantic_replay_verified=True,
    )


def load_terminal_models(
    *,
    selection: SealedSelection,
    candidate_artifact_root: Path,
    v0_checkpoint: Path,
    v0_calibration: Path,
) -> tuple[ModelSpec, ...]:
    """Verify checkpoint/calibration files before constructing scorer inputs."""
    if tree_digest(v0_checkpoint) != FROZEN_M3_TREE_SHA256:
        raise ValidationError("V0 checkpoint tree drifted")
    if file_sha256(v0_calibration) != _V0_CALIBRATION_SHA256:
        raise ValidationError("V0 calibration file drifted")
    v0_payload = load_json(v0_calibration, "V0 calibration")
    v0_temperature = require_number(v0_payload.get("temperature"), "V0 temperature")
    baseline = ModelSpec(
        variant="V0-frozen-m3",
        seed=None,
        checkpoint_path=str(v0_checkpoint),
        checkpoint_tree_sha256=FROZEN_M3_TREE_SHA256,
        model_identity=FROZEN_M3_TREE_SHA256,
        calibration_identity=_V0_CALIBRATION_SHA256,
        checkpoint_identity_path="",
        calibration_path=str(v0_calibration),
        run_complete_sha256=None,
        training_manifest_sha256=None,
        batch_schedule_sha256=None,
        training_git_head=None,
        training_repository_dirty=None,
        trainer_implementation_sha256=None,
        trainer_implementation_files_sha256=None,
        temperature=v0_temperature,
    )
    candidates: list[ModelSpec] = []
    for checkpoint in selection.selected_checkpoints:
        expected_relative_path = (
            f"runs/{checkpoint.variant}/{checkpoint.seed}/checkpoint"
        )
        if checkpoint.checkpoint_relative_path != expected_relative_path:
            raise ValidationError(
                f"candidate checkpoint path is not canonical: {checkpoint.key}"
            )
        checkpoint_path = _resolve_within(
            candidate_artifact_root,
            checkpoint.checkpoint_relative_path,
            "candidate checkpoint path",
        )
        completed_model, _schedule = _load_completed_candidate(checkpoint_path.parent)
        completed_identity = (
            completed_model.variant,
            completed_model.seed,
            completed_model.checkpoint_tree_sha256,
            completed_model.model_identity,
            completed_model.run_complete_sha256,
            completed_model.training_manifest_sha256,
            completed_model.batch_schedule_sha256,
            completed_model.training_git_head,
            completed_model.training_repository_dirty,
            completed_model.trainer_implementation_sha256,
            tuple(
                sorted(
                    (completed_model.trainer_implementation_files_sha256 or {}).items()
                )
            ),
        )
        selected_identity = (
            checkpoint.variant,
            checkpoint.seed,
            checkpoint.checkpoint_tree_sha256,
            checkpoint.checkpoint_identity_sha256,
            checkpoint.run_complete_sha256,
            checkpoint.training_manifest_sha256,
            checkpoint.batch_schedule_sha256,
            checkpoint.training_git_head,
            checkpoint.training_repository_dirty,
            checkpoint.trainer_implementation_sha256,
            tuple(sorted(checkpoint.trainer_implementation_files_sha256.items())),
        )
        if completed_identity != selected_identity:
            raise ValidationError(
                f"selected training run provenance drifted: {checkpoint.key}"
            )
        identity_path = checkpoint_path.parent / "checkpoint_identity.json"
        calibration_path = checkpoint_path.parent / "calibration.json"
        for artifact_name, expected_sha256 in (
            ("run_complete.json", checkpoint.run_complete_sha256),
            ("training_manifest.json", checkpoint.training_manifest_sha256),
            ("batch_schedule.json", checkpoint.batch_schedule_sha256),
        ):
            if file_sha256(checkpoint_path.parent / artifact_name) != expected_sha256:
                raise ValidationError(
                    "candidate sealed training artifact drifted: "
                    f"{checkpoint.key}/{artifact_name}"
                )
        if tree_digest(checkpoint_path) != checkpoint.checkpoint_tree_sha256:
            raise ValidationError(
                f"candidate checkpoint tree drifted: {checkpoint.key}"
            )
        if file_sha256(identity_path) != checkpoint.checkpoint_identity_sha256:
            raise ValidationError(
                f"candidate checkpoint identity drifted: {checkpoint.key}"
            )
        calibration = _validate_calibration(
            path=calibration_path,
            selection=selection,
            checkpoint=checkpoint,
        )
        candidates.append(
            ModelSpec(
                variant=checkpoint.variant,
                seed=checkpoint.seed,
                checkpoint_path=str(checkpoint_path),
                checkpoint_tree_sha256=checkpoint.checkpoint_tree_sha256,
                model_identity=checkpoint.checkpoint_identity_sha256,
                calibration_identity=calibration.file_sha256,
                checkpoint_identity_path=str(identity_path),
                calibration_path=str(calibration_path),
                run_complete_sha256=checkpoint.run_complete_sha256,
                training_manifest_sha256=checkpoint.training_manifest_sha256,
                batch_schedule_sha256=checkpoint.batch_schedule_sha256,
                training_git_head=checkpoint.training_git_head,
                training_repository_dirty=checkpoint.training_repository_dirty,
                trainer_implementation_sha256=(
                    checkpoint.trainer_implementation_sha256
                ),
                trainer_implementation_files_sha256=(
                    checkpoint.trainer_implementation_files_sha256
                ),
                temperature=calibration.deployed_temperature,
                calibration_git_head=calibration.git_head,
                calibration_repository_dirty=calibration.repository_dirty,
                calibrator_implementation_sha256=(calibration.implementation_sha256),
                calibrator_implementation_files_sha256=(
                    calibration.implementation_files_sha256
                ),
                calibration_semantic_replay_verified=(
                    calibration.semantic_replay_verified
                ),
            )
        )
    return (baseline, *candidates)


def build_development_models_manifest(
    path: Path, *, artifact_root: Path, v0_checkpoint: Path
) -> str:
    """Publish paths only; identities are always derived from sealed artifacts."""
    payload = {
        "schema_version": "groundloop-m4-13-development-models-v1",
        "v0_checkpoint_path": str(v0_checkpoint),
        "candidate_run_directories": [
            str(artifact_root / "runs" / variant / str(seed))
            for variant, seed in sorted(_EXPECTED_DEVELOPMENT_RUNS)
        ],
    }
    return write_canonical_json(path, payload)


def _load_completed_candidate(
    run_directory: Path,
) -> tuple[ModelSpec, Mapping[str, object]]:
    completion_path = run_directory / "run_complete.json"
    manifest_path = run_directory / "training_manifest.json"
    identity_path = run_directory / "checkpoint_identity.json"
    schedule_path = run_directory / "batch_schedule.json"
    runtime_path = run_directory / "runtime.json"
    completion = load_json(completion_path, "training completion seal")
    _expect_equal(
        completion.get("schema_version"),
        _COMPLETION_SCHEMA,
        "training completion schema",
    )
    _expect_equal(completion.get("status"), "complete", "training completion status")
    bound = {
        "training_manifest_sha256": manifest_path,
        "checkpoint_identity_sha256": identity_path,
        "batch_schedule_file_sha256": schedule_path,
        "runtime_file_sha256": runtime_path,
    }
    for field, artifact in bound.items():
        _expect_equal(
            completion.get(field), file_sha256(artifact), f"training seal {field}"
        )
    invocation = require_sha256(
        completion.get("invocation_sha256"), "training invocation"
    )
    runtime = load_json(runtime_path, "training runtime")
    _expect_equal(
        runtime.get("schema_version"),
        "groundloop-m4-13-training-runtime-v1",
        "training runtime schema",
    )
    dependencies = {
        require_text(name, "training dependency name"): require_text(
            version, "training dependency version"
        )
        for name, version in require_object(
            runtime.get("dependencies"), "training dependencies"
        ).items()
    }
    expected_dependencies = {
        "python",
        "torch",
        "transformers",
        "tokenizers",
        "safetensors",
        "numpy",
    }
    if set(dependencies) != expected_dependencies or "absent" in dependencies.values():
        raise ValidationError("training runtime dependency surface is incomplete")
    hardware = require_object(runtime.get("hardware"), "training hardware")
    if (
        hardware.get("torch_threads") != 8
        or hardware.get("torch_interop_threads") != 1
        or hardware.get("cuda_available") is not False
    ):
        raise ValidationError("training runtime did not use the frozen CPU contract")
    manifest = load_json(manifest_path, "training manifest")
    _expect_equal(
        manifest.get("schema_version"),
        _TRAINING_MANIFEST_SCHEMA,
        "training manifest schema",
    )
    _expect_equal(manifest.get("status"), "complete", "training manifest status")
    _expect_equal(
        manifest.get("invocation_sha256"), invocation, "training manifest invocation"
    )
    for field, expected in _FROZEN_TRAINING_INPUTS.items():
        _expect_equal(manifest.get(field), expected, f"training manifest {field}")
    variant = require_text(manifest.get("variant"), "training variant")
    seed = require_integer(manifest.get("seed"), "training seed")
    if (variant, seed) not in _EXPECTED_DEVELOPMENT_RUNS:
        raise ValidationError("completed run is not a frozen M4.13 variant/seed")
    _expect_equal(manifest.get("microbatches"), 890, "training microbatch budget")
    _expect_equal(
        manifest.get("optimizer_steps"), 223, "training optimizer-step budget"
    )
    expected_objective = {
        "endpoint": "weighted-endpoint-ce-arithmetic-mean-v1",
        "paired": (
            "symmetric-log-probability-margin-v1"
            if variant in {"V3-margin-mix", "A1-margin-no-replay"}
            else None
        ),
        "margin": 0.5,
        "paired_weight": 0.25,
    }
    _expect_equal(manifest.get("objective"), expected_objective, "training objective")
    _expect_equal(
        manifest.get("hyperparameters"),
        _FROZEN_HYPERPARAMETERS,
        "training hyperparameters",
    )
    _expect_equal(
        manifest.get("optimizer_schedule"),
        _FROZEN_OPTIMIZER_SCHEDULE,
        "training optimizer schedule",
    )
    repository = require_object(manifest.get("repository"), "training repository")
    git_head = require_text(repository.get("git_head"), "training Git HEAD")
    if (
        len(git_head) not in {40, 64}
        or any(character not in "0123456789abcdef" for character in git_head)
        or repository.get("dirty") is not False
    ):
        raise ValidationError("real candidate training repository was not clean")
    trainer = require_object(
        manifest.get("trainer_implementation"), "trainer implementation"
    )
    _expect_equal(
        trainer.get("schema_version"),
        _TRAINER_IMPLEMENTATION_SCHEMA,
        "trainer implementation schema",
    )
    trainer_files = {
        require_text(name, "trainer implementation path"): require_sha256(
            digest, "trainer implementation file hash"
        )
        for name, digest in require_object(
            trainer.get("files_sha256"), "trainer implementation files"
        ).items()
    }
    if set(trainer_files) != _TRAINER_IMPLEMENTATION_PATHS:
        raise ValidationError("trainer implementation file surface drifted")
    trainer_sha256 = require_sha256(
        trainer.get("sha256"), "trainer implementation hash"
    )
    _expect_equal(
        trainer_sha256,
        canonical_sha256(
            {
                "schema_version": _TRAINER_IMPLEMENTATION_SCHEMA,
                "files_sha256": dict(sorted(trainer_files.items())),
            }
        ),
        "trainer implementation aggregate",
    )
    invocation_payload = {
        "schema_version": "groundloop-m4-13-training-invocation-v1",
        "variant": variant,
        "seed": seed,
        **{field: manifest.get(field) for field in _FROZEN_TRAINING_INPUTS},
        "schedule_sha256": manifest.get("schedule_sha256"),
        "objective": manifest.get("objective"),
        "hyperparameters": manifest.get("hyperparameters"),
        "optimizer_schedule": manifest.get("optimizer_schedule"),
        "repository": dict(repository),
        "trainer_implementation": dict(trainer),
    }
    _expect_equal(
        canonical_sha256(invocation_payload),
        invocation,
        "training invocation digest",
    )
    schedule = load_json(schedule_path, "batch schedule")
    _expect_equal(
        schedule.get("schema_version"), _SCHEDULE_SCHEMA, "batch schedule schema"
    )
    _expect_equal(schedule.get("seed"), seed, "batch schedule seed")
    _validate_schedule_semantics(schedule, variant=variant, seed=seed)
    for field in (
        "microbatches",
        "optimizer_steps",
        "batch_order_sha256",
        "optimizer_schedule_sha256",
        "schedule_sha256",
        "base_order_class_weights",
    ):
        _expect_equal(
            schedule.get(field), manifest.get(field), f"batch schedule {field}"
        )
    identity = load_json(identity_path, "checkpoint identity")
    _expect_equal(
        identity.get("schema_version"),
        _CHECKPOINT_IDENTITY_SCHEMA,
        "checkpoint identity schema",
    )
    checkpoint = run_directory / "checkpoint"
    tree = require_sha256(identity.get("checkpoint_tree_sha256"), "checkpoint tree")
    weights = require_sha256(identity.get("weights_sha256"), "checkpoint weights")
    _expect_equal(tree_digest(checkpoint), tree, "candidate checkpoint tree")
    _expect_equal(
        file_sha256(checkpoint / "model.safetensors"),
        weights,
        "candidate checkpoint weights",
    )
    _expect_equal(
        manifest.get("checkpoint_tree_sha256"), tree, "manifest checkpoint tree"
    )
    _expect_equal(
        manifest.get("weights_sha256"), weights, "manifest checkpoint weights"
    )
    schedule_with_runtime = dict(schedule)
    schedule_with_runtime["_runtime_comparability"] = {
        "dependencies": dict(sorted(dependencies.items())),
        "torch_threads": 8,
        "torch_interop_threads": 1,
        "cuda_available": False,
    }
    return (
        ModelSpec(
            variant=variant,
            seed=seed,
            checkpoint_path=str(checkpoint),
            checkpoint_tree_sha256=tree,
            model_identity=file_sha256(identity_path),
            calibration_identity=_UNCALIBRATED_DEVELOPMENT_IDENTITY,
            checkpoint_identity_path=str(identity_path),
            calibration_path="development-only-uncalibrated",
            run_complete_sha256=file_sha256(completion_path),
            training_manifest_sha256=file_sha256(manifest_path),
            batch_schedule_sha256=file_sha256(schedule_path),
            training_git_head=git_head,
            training_repository_dirty=False,
            trainer_implementation_sha256=trainer_sha256,
            trainer_implementation_files_sha256=trainer_files,
            temperature=1.0,
        ),
        schedule_with_runtime,
    )


def load_development_models_manifest(path: Path) -> tuple[ModelSpec, ...]:
    """Derive all candidate identities from eight exact completed training runs."""
    payload = load_json(path, "development model manifest")
    _expect_equal(
        payload.get("schema_version"),
        "groundloop-m4-13-development-models-v1",
        "development model manifest schema",
    )
    v0_checkpoint = Path(
        require_text(payload.get("v0_checkpoint_path"), "V0 checkpoint path")
    )
    if tree_digest(v0_checkpoint) != FROZEN_M3_TREE_SHA256:
        raise ValidationError("development V0 is not the frozen M3 checkpoint")
    raw_runs = require_array(
        payload.get("candidate_run_directories"), "candidate run directories"
    )
    if len(raw_runs) != len(_EXPECTED_DEVELOPMENT_RUNS):
        raise ValidationError(
            "development manifest must bind exactly eight candidate runs"
        )
    candidates: list[ModelSpec] = []
    schedules: dict[tuple[str, int], Mapping[str, object]] = {}
    for raw in raw_runs:
        model, schedule = _load_completed_candidate(
            Path(require_text(raw, "candidate run directory"))
        )
        if (model.variant, model.seed) in schedules:
            raise ValidationError("development manifest repeats a variant/seed run")
        candidates.append(model)
        schedules[(model.variant, require_integer(model.seed, "candidate seed"))] = (
            schedule
        )
    if set(schedules) != _EXPECTED_DEVELOPMENT_RUNS:
        raise ValidationError("development manifest omitted a frozen variant/seed run")
    checkpoint_trees = [model.checkpoint_tree_sha256 for model in candidates]
    if len(set(checkpoint_trees)) != len(checkpoint_trees):
        raise ValidationError("a candidate checkpoint was substituted across runs")
    trainer_identities = {
        (
            model.training_git_head,
            model.training_repository_dirty,
            model.trainer_implementation_sha256,
            tuple(sorted((model.trainer_implementation_files_sha256 or {}).items())),
        )
        for model in candidates
    }
    if len(trainer_identities) != 1:
        raise ValidationError("completed runs do not share one clean trainer identity")
    runtime_identities = {
        canonical_sha256(schedule["_runtime_comparability"])
        for schedule in schedules.values()
    }
    if len(runtime_identities) != 1:
        raise ValidationError(
            "completed runs do not share one runtime dependency surface"
        )
    for seed in (20260720, 20260721, 20260722):
        v2 = schedules[("V2-ce-mix", seed)]
        v3 = schedules[("V3-margin-mix", seed)]
        for field in (
            "batches",
            "batch_order_sha256",
            "optimizer_schedule_sha256",
            "schedule_sha256",
            "base_order_class_weights",
        ):
            _expect_equal(
                v2.get(field), v3.get(field), f"V2/V3 paired schedule {field}"
            )
    baseline = ModelSpec(
        variant="V0-frozen-m3",
        seed=None,
        checkpoint_path=str(v0_checkpoint),
        checkpoint_tree_sha256=FROZEN_M3_TREE_SHA256,
        model_identity=FROZEN_M3_TREE_SHA256,
        calibration_identity=_UNCALIBRATED_DEVELOPMENT_IDENTITY,
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
    return (baseline, *sorted(candidates, key=lambda model: model.key))
