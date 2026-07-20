#!/usr/bin/env python3
"""Deterministic M4.13 continuation training from the frozen M3 checkpoint."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import random
import resource
import shutil
import subprocess
import tempfile
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from typing import Literal, cast

from groundloop.ai.verification.artifacts import file_sha256, tree_digest, tree_manifest
from groundloop.errors import ValidationError

from .losses import (
    BASE_LOGIT_ORDER,
    STORED_LABEL_ORDER,
    change_aware_loss,
    normalize_stored_label,
    stored_label_to_base_index,
    vitaminc_label_to_stored,
)

CONFIG_SCHEMA = "groundloop-m4-change-aware-data-config-v1"
DATASET_MANIFEST_SCHEMA = "groundloop-m4-13-dataset-manifest-v1"
VITAMINC_ROW_SCHEMA = "groundloop-m4-13-vitaminc-row-v1"
TRAINING_MANIFEST_SCHEMA = "groundloop-m4-13-training-run-v1"
CHECKPOINT_IDENTITY_SCHEMA = "groundloop-m4-13-checkpoint-identity-v1"
COMPLETION_SCHEMA = "groundloop-m4-13-training-complete-v1"
SCHEDULE_SCHEMA = "groundloop-m4-13-batch-schedule-v1"
TRAINER_IMPLEMENTATION_SCHEMA = "groundloop-m4-13-trainer-implementation-v1"

_TRAINER_IMPLEMENTATION_PATHS = (
    "training/m4_13_verifier/train.py",
    "training/m4_13_verifier/losses.py",
    "training/m4_13_verifier/__init__.py",
    "src/groundloop/ai/verification/artifacts.py",
    "src/groundloop/errors.py",
)

FROZEN_PRIMARY_SEED = 20260720
FROZEN_REPLICATION_SEEDS = (20260720, 20260721, 20260722)
FROZEN_MICROBATCHES = 890
FROZEN_OPTIMIZER_STEPS = 223
FROZEN_VITAMINC_ROWS = 4096
FROZEN_VITAMINC_CASES = 1024
FROZEN_M3_ROWS = 3022
FROZEN_M3_GROUPS = 2067

VariantName = Literal[
    "V1-replay-only", "V2-ce-mix", "V3-margin-mix", "A1-margin-no-replay"
]


class TrainingVariant(StrEnum):
    V1_REPLAY_ONLY = "V1-replay-only"
    V2_CE_MIX = "V2-ce-mix"
    V3_MARGIN_MIX = "V3-margin-mix"
    A1_MARGIN_NO_REPLAY = "A1-margin-no-replay"

    @property
    def uses_vitaminc(self) -> bool:
        return self is not TrainingVariant.V1_REPLAY_ONLY

    @property
    def uses_m3(self) -> bool:
        return self is not TrainingVariant.A1_MARGIN_NO_REPLAY

    @property
    def uses_paired_margin(self) -> bool:
        return self in {
            TrainingVariant.V3_MARGIN_MIX,
            TrainingVariant.A1_MARGIN_NO_REPLAY,
        }

    @property
    def is_mixed(self) -> bool:
        return self in {
            TrainingVariant.V2_CE_MIX,
            TrainingVariant.V3_MARGIN_MIX,
        }


class FailureStage(StrEnum):
    BEFORE_CHECKPOINT_WRITE = "before_checkpoint_write"
    BEFORE_MANIFEST_WRITE = "before_manifest_write"
    BEFORE_RESULT_SEALING = "before_result_sealing"


@dataclass(frozen=True, slots=True)
class TrainingHyperparameters:
    max_length: int = 256
    microbatch_size: int = 8
    gradient_accumulation_steps: int = 4
    epochs: int = 1
    learning_rate: float = 1e-5
    weight_decay: float = 0.01
    warmup_ratio: float = 0.06
    gradient_norm_clip: float = 1.0
    torch_threads: int = 8
    torch_interop_threads: int = 1
    deterministic_algorithms: bool = True
    paired_margin: float = 0.5
    paired_weight: float = 0.25


FROZEN_HYPERPARAMETERS = TrainingHyperparameters()


@dataclass(frozen=True, slots=True)
class TrainingExample:
    row_id: str
    domain: Literal["vitaminc", "m3"]
    group_id: str
    case_id: str | None
    suffix: int | None
    claim: str
    evidence: str
    stored_label: str

    @property
    def base_label(self) -> int:
        return stored_label_to_base_index(self.stored_label)


@dataclass(frozen=True, slots=True)
class ScheduledRow:
    example: TrainingExample
    repeat_index: int


@dataclass(frozen=True, slots=True)
class ScheduledBatch:
    batch_index: int
    domain: Literal["vitaminc", "m3"]
    rows: tuple[ScheduledRow, ...]
    transitions: tuple[tuple[int, int], ...]
    optimizer_step: int
    accumulation_divisor: int
    closes_optimizer_step: bool


@dataclass(frozen=True, slots=True)
class TrainingSchedule:
    variant: TrainingVariant
    seed: int
    batches: tuple[ScheduledBatch, ...]
    base_order_class_weights: tuple[float, float, float]
    batch_order_sha256: str
    optimizer_schedule_sha256: str
    schedule_sha256: str
    optimizer_steps: int

    def to_manifest(self) -> dict[str, object]:
        return {
            "schema_version": SCHEDULE_SCHEMA,
            "seed": self.seed,
            "schedule_family": (
                "mixed-vitaminc-m3-v1"
                if self.variant.is_mixed
                else f"cycled-{self.batches[0].domain}-v1"
            ),
            "microbatches": len(self.batches),
            "optimizer_steps": self.optimizer_steps,
            "gradient_accumulation_steps": (
                FROZEN_HYPERPARAMETERS.gradient_accumulation_steps
            ),
            "base_logit_order": list(BASE_LOGIT_ORDER),
            "stored_label_order": list(STORED_LABEL_ORDER),
            "base_order_class_weights": list(self.base_order_class_weights),
            "batch_order_sha256": self.batch_order_sha256,
            "optimizer_schedule_sha256": self.optimizer_schedule_sha256,
            "schedule_sha256": self.schedule_sha256,
            "batches": [_batch_identity(batch) for batch in self.batches],
        }


@dataclass(frozen=True, slots=True)
class PreparedTrainingInputs:
    vitaminc: tuple[TrainingExample, ...]
    m3: tuple[TrainingExample, ...]
    config_sha256: str
    dataset_manifest_sha256: str
    train_vitaminc_sha256: str
    train_vitaminc_manifest_sha256: str
    train_m3_sha256: str


@dataclass(frozen=True, slots=True)
class CompletedTrainingRun:
    invocation_sha256: str
    training_manifest: Mapping[str, object]
    checkpoint_identity: Mapping[str, object]
    completion: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class RepositoryProvenance:
    git_head: str
    dirty: bool
    implementation_files_sha256: Mapping[str, str]
    implementation_sha256: str

    def to_manifest(self) -> dict[str, object]:
        return {
            "repository": {
                "git_head": self.git_head,
                "dirty": self.dirty,
            },
            "trainer_implementation": {
                "schema_version": TRAINER_IMPLEMENTATION_SCHEMA,
                "files_sha256": dict(self.implementation_files_sha256),
                "sha256": self.implementation_sha256,
            },
        }


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _run_git(repository_root: Path, *arguments: str) -> str:
    result = subprocess.run(
        ("git", "-C", str(repository_root), *arguments),
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env={**os.environ, "LC_ALL": "C"},
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "unknown git error"
        raise ValidationError(f"cannot validate training repository: {detail}")
    return result.stdout


def validate_repository_provenance(repository_root: Path) -> RepositoryProvenance:
    """Bind one clean Git commit and the exact trainer implementation bytes."""
    supplied_root = repository_root.resolve()
    discovered = Path(
        _run_git(supplied_root, "rev-parse", "--show-toplevel").strip()
    ).resolve()
    if discovered != supplied_root:
        raise ValidationError(
            "training repository root must be the exact Git worktree root"
        )
    git_head = _run_git(discovered, "rev-parse", "HEAD").strip()
    if len(git_head) not in {40, 64} or any(
        character not in "0123456789abcdef" for character in git_head
    ):
        raise ValidationError("training repository HEAD is not a canonical Git digest")
    status = _run_git(
        discovered,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    )
    if status:
        raise ValidationError(
            "real training requires a clean repository with no tracked or "
            "nonignored untracked changes"
        )
    files: dict[str, str] = {}
    for relative in _TRAINER_IMPLEMENTATION_PATHS:
        path = discovered / relative
        if not path.is_file():
            raise ValidationError(
                f"trainer implementation dependency is absent: {relative}"
            )
        files[relative] = file_sha256(path)
    implementation = {
        "schema_version": TRAINER_IMPLEMENTATION_SCHEMA,
        "files_sha256": files,
    }
    return RepositoryProvenance(
        git_head=git_head,
        dirty=False,
        implementation_files_sha256=files,
        implementation_sha256=_canonical_sha256(implementation),
    )


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValidationError(f"{name} must be a JSON object")
    return cast(Mapping[str, object], value)


def _string(value: Mapping[str, object], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result.strip():
        raise ValidationError(f"{key} must be a non-empty string")
    return result


def _integer(value: Mapping[str, object], key: str) -> int:
    result = value.get(key)
    if not isinstance(result, int) or isinstance(result, bool):
        raise ValidationError(f"{key} must be an integer")
    return result


def _load_json(path: Path, name: str) -> Mapping[str, object]:
    if not path.is_file():
        raise FileNotFoundError(f"{name} is absent: {path}")
    try:
        return _mapping(json.loads(path.read_text(encoding="utf-8")), name)
    except json.JSONDecodeError as error:
        raise ValidationError(f"{name} is not valid JSON: {path}") from error


def _load_jsonl(path: Path, name: str) -> tuple[Mapping[str, object], ...]:
    if not path.is_file():
        raise FileNotFoundError(f"{name} is absent: {path}")
    rows: list[Mapping[str, object]] = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        if not line.strip():
            continue
        try:
            rows.append(_mapping(json.loads(line), f"{name} row {line_number}"))
        except json.JSONDecodeError as error:
            raise ValidationError(
                f"{name} row {line_number} is not valid JSON"
            ) from error
    if not rows:
        raise ValidationError(f"{name} is empty")
    return tuple(rows)


def _vitaminc_suffix(unique_id: str) -> int:
    try:
        return int(unique_id.rsplit("_", 1)[1])
    except (IndexError, ValueError) as error:
        raise ValidationError(f"invalid VitaminC unique_id: {unique_id}") from error


def load_vitaminc_training_rows(path: Path) -> tuple[TrainingExample, ...]:
    """Load Lane A rows and revalidate every semantic/hash field."""
    examples: list[TrainingExample] = []
    identities: set[str] = set()
    for row in _load_jsonl(path, "VitaminC training JSONL"):
        if _string(row, "schema_version") != VITAMINC_ROW_SCHEMA:
            raise ValidationError("unsupported VitaminC prepared-row schema")
        if _string(row, "split") != "train":
            raise ValidationError("training received a non-train VitaminC row")
        if _string(row, "revision_type") != "real":
            raise ValidationError("M4.13 training accepts only real revisions")
        unique_id = _string(row, "unique_id")
        if unique_id in identities:
            raise ValidationError(f"duplicate VitaminC unique_id: {unique_id}")
        identities.add(unique_id)
        source_label = _string(row, "source_label")
        stored_label = normalize_stored_label(_string(row, "label"))
        if vitaminc_label_to_stored(source_label) != stored_label:
            raise ValidationError("VitaminC source/stored label mapping drifted")
        claim = _string(row, "claim")
        evidence = _string(row, "evidence")
        if hashlib.sha256(claim.encode()).hexdigest() != _string(row, "claim_sha256"):
            raise ValidationError("VitaminC claim hash drifted")
        if hashlib.sha256(evidence.encode()).hexdigest() != _string(
            row, "evidence_sha256"
        ):
            raise ValidationError("VitaminC evidence hash drifted")
        examples.append(
            TrainingExample(
                row_id=unique_id,
                domain="vitaminc",
                group_id=_string(row, "case_id"),
                case_id=_string(row, "case_id"),
                suffix=_vitaminc_suffix(unique_id),
                claim=claim,
                evidence=evidence,
                stored_label=stored_label,
            )
        )
    _validate_vitaminc_cases(examples)
    return tuple(examples)


def _validate_vitaminc_cases(examples: Sequence[TrainingExample]) -> None:
    groups: defaultdict[str, list[TrainingExample]] = defaultdict(list)
    for example in examples:
        groups[example.group_id].append(example)
    for case_id, rows in groups.items():
        if len(rows) != 4:
            raise ValidationError(f"VitaminC case {case_id} does not have four rows")
        by_suffix = {row.suffix: row for row in rows}
        if set(by_suffix) != {1, 2, 3, 4}:
            raise ValidationError(f"VitaminC case {case_id} has malformed suffixes")
        if not (
            by_suffix[1].claim == by_suffix[2].claim
            and by_suffix[3].claim == by_suffix[4].claim
            and by_suffix[1].evidence == by_suffix[3].evidence
            and by_suffix[2].evidence == by_suffix[4].evidence
            and len({row.claim for row in rows}) == 2
            and len({row.evidence for row in rows}) == 2
            and len({(row.claim, row.evidence) for row in rows}) == 4
        ):
            raise ValidationError(f"VitaminC case {case_id} violates the 2x2 layout")
        for left, right in ((1, 2), (3, 4)):
            labels = {by_suffix[left].stored_label, by_suffix[right].stored_label}
            if "support" not in labels or len(labels) != 2:
                raise ValidationError(
                    f"VitaminC case {case_id} transition labels are invalid"
                )


def load_m3_training_rows(path: Path) -> tuple[TrainingExample, ...]:
    """Load the complete M3 replay split in its existing row schema."""
    examples: list[TrainingExample] = []
    identities: set[str] = set()
    for row in _load_jsonl(path, "M3 replay JSONL"):
        if _string(row, "split") != "train":
            raise ValidationError("training received a non-train M3 replay row")
        row_id = _string(row, "example_id")
        if row_id in identities:
            raise ValidationError(f"duplicate M3 example_id: {row_id}")
        identities.add(row_id)
        examples.append(
            TrainingExample(
                row_id=row_id,
                domain="m3",
                group_id=_string(row, "claim_group_id"),
                case_id=None,
                suffix=None,
                claim=_string(row, "claim"),
                evidence=_string(row, "evidence"),
                stored_label=normalize_stored_label(_string(row, "label")),
            )
        )
    return tuple(examples)


def _vitaminc_identity_rows(path: Path) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for row in _load_jsonl(path, "VitaminC training JSONL"):
        result.append(
            {
                key: _string(row, source_key)
                for key, source_key in (
                    ("stratum", "stratum"),
                    ("selection_key", "selection_key"),
                    ("unique_id", "unique_id"),
                    ("case_id", "case_id"),
                    ("page", "page"),
                    ("wiki_revision_id", "wiki_revision_id"),
                    ("revision_type", "revision_type"),
                    ("label", "source_label"),
                    ("claim_sha256", "claim_sha256"),
                    ("evidence_sha256", "evidence_sha256"),
                )
            }
        )
    return result


def _assert_no_terminal_payload(manifest: Mapping[str, object]) -> None:
    """Permit reserve digests/counts but reject training-visible reserve content."""
    forbidden = {"claim", "evidence", "label", "unique_id", "case_id", "page", "path"}

    def visit(value: object, *, in_terminal: bool) -> None:
        if isinstance(value, dict):
            for raw_key, child in value.items():
                key = str(raw_key).casefold()
                child_terminal = in_terminal or "terminal" in key or "reserve" in key
                if in_terminal and key in forbidden:
                    raise ValidationError(
                        "dataset manifest exposes terminal reserve content to training"
                    )
                visit(child, in_terminal=child_terminal)
        elif isinstance(value, list):
            for child in value:
                visit(child, in_terminal=in_terminal)

    visit(manifest, in_terminal=False)


def _validate_dataset_manifest(
    manifest: Mapping[str, object],
    *,
    config_path: Path,
    prepared: Path,
) -> None:
    _assert_no_terminal_payload(manifest)
    if _string(manifest, "schema_version") != DATASET_MANIFEST_SCHEMA:
        raise ValidationError("unsupported prepared dataset manifest schema")
    if _string(manifest, "config_sha256") != file_sha256(config_path):
        raise ValidationError("prepared dataset was built from a different config")
    surface = _mapping(manifest.get("training_surface"), "training surface")
    if surface.get("accepts_test_path") is not False:
        raise ValidationError("training surface does not fail closed on test paths")
    if surface.get("terminal_paths_exposed") is not False:
        raise ValidationError("training surface exposes terminal paths")
    if _string(surface, "vitaminc_row_schema") != VITAMINC_ROW_SCHEMA:
        raise ValidationError("training surface changed the VitaminC row schema")
    artifacts = _mapping(surface.get("artifacts"), "training artifacts")
    required = (
        "train_vitaminc.jsonl",
        "train_vitaminc_manifest.json",
        "train_m3_replay.jsonl",
        "development_vitaminc.jsonl",
        "development_vitaminc_manifest.json",
        "development_m3.jsonl",
    )
    for name in required:
        if file_sha256(prepared / name) != _string(artifacts, name):
            raise ValidationError(f"prepared training artifact hash drifted: {name}")
    label_mapping = _mapping(manifest.get("label_mapping"), "label mapping")
    base_order = label_mapping.get("base_logit_order")
    if not isinstance(base_order, list) or tuple(base_order) != BASE_LOGIT_ORDER:
        raise ValidationError("dataset manifest base-logit order drifted")
    stored_order = label_mapping.get("stored_probability_order")
    if not isinstance(stored_order, list) or tuple(stored_order) != STORED_LABEL_ORDER:
        raise ValidationError("dataset manifest stored label order drifted")
    for source_label, stored_label in (
        ("SUPPORTS", "support"),
        ("REFUTES", "refute"),
        ("NOT ENOUGH INFO", "neutral"),
    ):
        item = _mapping(label_mapping.get(source_label), source_label)
        if _string(item, "stored") != stored_label or _integer(
            item, "base_logit_index"
        ) != stored_label_to_base_index(stored_label):
            raise ValidationError("dataset manifest label mapping drifted")


def load_prepared_training_inputs(
    *, artifact_root: Path, config_path: Path, require_frozen_counts: bool = True
) -> PreparedTrainingInputs:
    config = _load_json(config_path, "M4.13 configuration")
    if _string(config, "schema_version") != CONFIG_SCHEMA:
        raise ValidationError("unsupported M4.13 configuration schema")
    dataset = _mapping(config.get("dataset"), "dataset configuration")
    m3 = _mapping(config.get("m3"), "M3 configuration")
    samples = _mapping(dataset.get("samples"), "dataset samples")
    train_sample = _mapping(samples.get("train"), "VitaminC train sample")
    m3_train = _mapping(m3.get("train"), "M3 train identity")

    prepared = artifact_root / "prepared"
    dataset_manifest_path = prepared / "dataset_manifest.json"
    dataset_manifest = _load_json(dataset_manifest_path, "prepared dataset manifest")
    _validate_dataset_manifest(
        dataset_manifest, config_path=config_path, prepared=prepared
    )
    vitaminc_path = prepared / "train_vitaminc.jsonl"
    vitaminc_manifest_path = prepared / "train_vitaminc_manifest.json"
    m3_path = prepared / "train_m3_replay.jsonl"

    if file_sha256(vitaminc_manifest_path) != _string(train_sample, "manifest_sha256"):
        raise ValidationError("VitaminC train identity manifest hash drifted")
    try:
        recorded_identity = json.loads(
            vitaminc_manifest_path.read_text(encoding="utf-8")
        )
    except json.JSONDecodeError as error:
        raise ValidationError("VitaminC train identity manifest is invalid") from error
    derived_identity = _vitaminc_identity_rows(vitaminc_path)
    if recorded_identity != derived_identity:
        raise ValidationError("VitaminC rows do not match their identity manifest")
    if file_sha256(m3_path) != _string(m3_train, "sha256"):
        raise ValidationError("M3 replay hash drifted")

    vitaminc = load_vitaminc_training_rows(vitaminc_path)
    m3_examples = load_m3_training_rows(m3_path)
    if require_frozen_counts:
        expected_vitaminc_rows = _integer(train_sample, "rows")
        expected_vitaminc_cases = _integer(train_sample, "cases")
        expected_m3_rows = _integer(m3_train, "rows")
        expected_m3_groups = _integer(m3_train, "claim_groups")
        if (len(vitaminc), len({row.group_id for row in vitaminc})) != (
            expected_vitaminc_rows,
            expected_vitaminc_cases,
        ):
            raise ValidationError("VitaminC frozen train dimensions drifted")
        if (len(m3_examples), len({row.group_id for row in m3_examples})) != (
            expected_m3_rows,
            expected_m3_groups,
        ):
            raise ValidationError("M3 complete replay dimensions drifted")
        if (
            expected_vitaminc_rows != FROZEN_VITAMINC_ROWS
            or expected_vitaminc_cases != FROZEN_VITAMINC_CASES
            or expected_m3_rows != FROZEN_M3_ROWS
            or expected_m3_groups != FROZEN_M3_GROUPS
        ):
            raise ValidationError("configuration changed the frozen training budget")

    return PreparedTrainingInputs(
        vitaminc,
        m3_examples,
        file_sha256(config_path),
        file_sha256(dataset_manifest_path),
        file_sha256(vitaminc_path),
        file_sha256(vitaminc_manifest_path),
        file_sha256(m3_path),
    )


def _derived_seed(seed: int, namespace: str) -> int:
    digest = hashlib.sha256(f"{seed}\0{namespace}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _vitaminc_batches(
    examples: Sequence[TrainingExample], seed: int
) -> tuple[tuple[TrainingExample, ...], ...]:
    groups: defaultdict[str, list[TrainingExample]] = defaultdict(list)
    for example in examples:
        if example.domain != "vitaminc":
            raise ValidationError("VitaminC schedule received a foreign-domain row")
        groups[example.group_id].append(example)
    case_ids = sorted(groups)
    random.Random(_derived_seed(seed, "vitaminc-case-order-v1")).shuffle(case_ids)
    if len(case_ids) % 2:
        raise ValidationError("VitaminC case count must be even for two-case batches")
    batches: list[tuple[TrainingExample, ...]] = []
    for offset in range(0, len(case_ids), 2):
        rows: list[TrainingExample] = []
        for case_id in case_ids[offset : offset + 2]:
            case = sorted(groups[case_id], key=lambda item: int(item.suffix or 0))
            if len(case) != 4:
                raise ValidationError("VitaminC microbatch contains an incomplete case")
            rows.extend(case)
        batches.append(tuple(rows))
    return tuple(batches)


def _m3_batches(
    examples: Sequence[TrainingExample], seed: int, microbatch_size: int
) -> tuple[tuple[TrainingExample, ...], ...]:
    groups: defaultdict[str, list[TrainingExample]] = defaultdict(list)
    for example in examples:
        if example.domain != "m3":
            raise ValidationError("M3 schedule received a foreign-domain row")
        groups[example.group_id].append(example)
    group_ids = sorted(groups)
    random.Random(_derived_seed(seed, "m3-claim-group-order-v1")).shuffle(group_ids)
    ordered = [
        row
        for group_id in group_ids
        for row in sorted(groups[group_id], key=lambda item: item.row_id)
    ]
    if len({row.row_id for row in ordered}) != len(examples):
        raise ValidationError("M3 schedule lost or duplicated replay rows")
    return tuple(
        tuple(ordered[offset : offset + microbatch_size])
        for offset in range(0, len(ordered), microbatch_size)
    )


def _merge_proportionally(
    vitaminc: Sequence[tuple[TrainingExample, ...]],
    m3: Sequence[tuple[TrainingExample, ...]],
) -> tuple[tuple[TrainingExample, ...], ...]:
    """Use an integer fair merge with no RNG or floating-point decisions."""
    total = len(vitaminc) + len(m3)
    vitamin_index = 0
    m3_index = 0
    result: list[tuple[TrainingExample, ...]] = []
    for position in range(total):
        vitamin_target = ((position + 1) * len(vitaminc)) // total
        if vitamin_target > vitamin_index:
            result.append(vitaminc[vitamin_index])
            vitamin_index += 1
        else:
            result.append(m3[m3_index])
            m3_index += 1
    if vitamin_index != len(vitaminc) or m3_index != len(m3):
        raise AssertionError("proportional merge did not consume both sources")
    return tuple(result)


def _cycle_batches(
    batches: Sequence[tuple[TrainingExample, ...]], target: int
) -> tuple[tuple[tuple[TrainingExample, ...], int], ...]:
    if not batches or target <= 0:
        raise ValidationError("cannot cycle an empty schedule")
    return tuple(
        (batches[index % len(batches)], index // len(batches))
        for index in range(target)
    )


def _batch_identity(batch: ScheduledBatch) -> dict[str, object]:
    return {
        "batch_index": batch.batch_index,
        "domain": batch.domain,
        "optimizer_step": batch.optimizer_step,
        "accumulation_divisor": batch.accumulation_divisor,
        "closes_optimizer_step": batch.closes_optimizer_step,
        "transitions": [list(pair) for pair in batch.transitions],
        "rows": [
            {
                "row_id": row.example.row_id,
                "group_id": row.example.group_id,
                "base_label": row.example.base_label,
                "repeat_index": row.repeat_index,
            }
            for row in batch.rows
        ],
    }


def build_training_schedule(
    *,
    vitaminc: Sequence[TrainingExample],
    m3: Sequence[TrainingExample],
    variant: TrainingVariant,
    seed: int,
    target_microbatches: int = FROZEN_MICROBATCHES,
    hyperparameters: TrainingHyperparameters = FROZEN_HYPERPARAMETERS,
) -> TrainingSchedule:
    """Materialize the full deterministic row, batch, and optimizer schedule."""
    if seed < 0 or target_microbatches <= 0:
        raise ValidationError("seed and target microbatch count must be valid")
    vitamin_batches = _vitaminc_batches(vitaminc, seed) if variant.uses_vitaminc else ()
    m3_batches = (
        _m3_batches(m3, seed, hyperparameters.microbatch_size)
        if variant.uses_m3
        else ()
    )
    raw: tuple[tuple[tuple[TrainingExample, ...], int], ...]
    if variant.is_mixed:
        merged = _merge_proportionally(vitamin_batches, m3_batches)
        if len(merged) != target_microbatches:
            raise ValidationError(
                "mixed source dimensions do not match the frozen microbatch budget"
            )
        raw = tuple((batch, 0) for batch in merged)
    elif variant is TrainingVariant.V1_REPLAY_ONLY:
        raw = _cycle_batches(m3_batches, target_microbatches)
    else:
        raw = _cycle_batches(vitamin_batches, target_microbatches)

    batches: list[ScheduledBatch] = []
    accumulation = hyperparameters.gradient_accumulation_steps
    optimizer_steps = math.ceil(len(raw) / accumulation)
    for batch_index, (rows, repeat_index) in enumerate(raw):
        domain = rows[0].domain
        if any(row.domain != domain for row in rows):
            raise AssertionError("one microbatch crossed source domains")
        transitions: tuple[tuple[int, int], ...] = ()
        if domain == "vitaminc":
            if len(rows) != 8:
                raise ValidationError("VitaminC microbatch must contain two full cases")
            transitions = ((0, 1), (2, 3), (4, 5), (6, 7))
        optimizer_step = batch_index // accumulation
        group_start = optimizer_step * accumulation
        accumulation_divisor = min(accumulation, len(raw) - group_start)
        closes = (batch_index + 1) % accumulation == 0 or batch_index + 1 == len(raw)
        batches.append(
            ScheduledBatch(
                batch_index,
                domain,
                tuple(ScheduledRow(row, repeat_index) for row in rows),
                transitions,
                optimizer_step,
                accumulation_divisor,
                closes,
            )
        )
    label_counts = Counter(
        row.example.base_label for batch in batches for row in batch.rows
    )
    total_rows = sum(label_counts.values())
    if set(label_counts) != {0, 1, 2}:
        raise ValidationError("materialized epoch must contain every verifier class")
    class_weights = cast(
        tuple[float, float, float],
        tuple(total_rows / (3.0 * label_counts[index]) for index in range(3)),
    )
    batch_payload = [
        {
            "domain": batch.domain,
            "transitions": [list(pair) for pair in batch.transitions],
            "rows": [
                [row.example.row_id, row.example.group_id, row.repeat_index]
                for row in batch.rows
            ],
        }
        for batch in batches
    ]
    optimizer_payload = [
        [
            batch.batch_index,
            batch.optimizer_step,
            batch.accumulation_divisor,
            batch.closes_optimizer_step,
        ]
        for batch in batches
    ]
    batch_hash = _canonical_sha256(batch_payload)
    optimizer_hash = _canonical_sha256(
        {
            "gradient_accumulation_steps": accumulation,
            "optimizer_steps": optimizer_steps,
            "warmup_steps": math.floor(optimizer_steps * hyperparameters.warmup_ratio),
            "boundaries": optimizer_payload,
        }
    )
    schedule_hash = _canonical_sha256(
        {
            "seed": seed,
            "family": "mixed" if variant.is_mixed else batches[0].domain,
            "batch_order_sha256": batch_hash,
            "optimizer_schedule_sha256": optimizer_hash,
            "class_weights": class_weights,
        }
    )
    result = TrainingSchedule(
        variant,
        seed,
        tuple(batches),
        class_weights,
        batch_hash,
        optimizer_hash,
        schedule_hash,
        optimizer_steps,
    )
    _validate_schedule_coverage(result, vitaminc=vitaminc, m3=m3)
    return result


def _validate_schedule_coverage(
    schedule: TrainingSchedule,
    *,
    vitaminc: Sequence[TrainingExample],
    m3: Sequence[TrainingExample],
) -> None:
    first_cycle = Counter(
        row.example.row_id
        for batch in schedule.batches
        for row in batch.rows
        if row.repeat_index == 0
    )
    if schedule.variant.uses_vitaminc and any(
        first_cycle[row.row_id] != 1 for row in vitaminc
    ):
        raise ValidationError("schedule does not contain one complete VitaminC pass")
    if schedule.variant.uses_m3 and any(first_cycle[row.row_id] != 1 for row in m3):
        raise ValidationError("schedule does not contain one complete M3 replay")


def assert_v2_v3_schedule_identity(v2: TrainingSchedule, v3: TrainingSchedule) -> None:
    """Fail the objective ablation unless all non-objective schedules match."""
    if (
        v2.variant is not TrainingVariant.V2_CE_MIX
        or v3.variant is not TrainingVariant.V3_MARGIN_MIX
        or v2.seed != v3.seed
        or v2.batch_order_sha256 != v3.batch_order_sha256
        or v2.optimizer_schedule_sha256 != v3.optimizer_schedule_sha256
        or v2.schedule_sha256 != v3.schedule_sha256
        or v2.base_order_class_weights != v3.base_order_class_weights
    ):
        raise ValidationError("V2/V3 row, batch, or optimizer schedule differs")


def validate_continuation_checkpoint(
    checkpoint: Path, *, expected_tree_sha256: str, expected_weights_sha256: str
) -> dict[str, object]:
    """Reject a base-model or drifted checkpoint before any optimizer update."""
    if not checkpoint.is_dir():
        raise FileNotFoundError(f"frozen M3 checkpoint is absent: {checkpoint}")
    actual_tree = tree_digest(checkpoint)
    weights = checkpoint / "model.safetensors"
    if not weights.is_file():
        raise ValidationError("frozen M3 checkpoint has no model.safetensors")
    actual_weights = file_sha256(weights)
    if actual_tree != expected_tree_sha256 or actual_weights != expected_weights_sha256:
        raise ValidationError("continuation checkpoint does not match frozen M3")
    config = _load_json(checkpoint / "config.json", "M3 checkpoint config")
    id2label = _mapping(config.get("id2label"), "M3 checkpoint id2label")
    if (
        tuple(str(id2label.get(str(index), "")) for index in range(3))
        != BASE_LOGIT_ORDER
    ):
        raise ValidationError("M3 checkpoint base-logit label order drifted")
    return {
        "tree_sha256": actual_tree,
        "weights_sha256": actual_weights,
        "files": tree_manifest(checkpoint),
    }


def _write_canonical_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(value) + b"\n")


def load_completed_training_run(
    run_directory: Path, *, expected_invocation_sha256: str | None = None
) -> CompletedTrainingRun:
    """Validate the seal and all bound artifacts; partial directories fail closed."""
    if not run_directory.is_dir():
        raise FileNotFoundError(f"completed training run is absent: {run_directory}")
    completion_path = run_directory / "run_complete.json"
    if not completion_path.is_file():
        raise ValidationError("training run is partial: completion seal is absent")
    completion = _load_json(completion_path, "training completion seal")
    if _string(completion, "schema_version") != COMPLETION_SCHEMA:
        raise ValidationError("unsupported training completion schema")
    if _string(completion, "status") != "complete":
        raise ValidationError("training completion seal is not complete")
    invocation = _string(completion, "invocation_sha256")
    if (
        expected_invocation_sha256 is not None
        and invocation != expected_invocation_sha256
    ):
        raise ValidationError("existing run belongs to a different training invocation")
    manifest_path = run_directory / "training_manifest.json"
    identity_path = run_directory / "checkpoint_identity.json"
    schedule_path = run_directory / "batch_schedule.json"
    runtime_path = run_directory / "runtime.json"
    bound_files = {
        "training_manifest_sha256": manifest_path,
        "checkpoint_identity_sha256": identity_path,
        "batch_schedule_file_sha256": schedule_path,
        "runtime_file_sha256": runtime_path,
    }
    for key, path in bound_files.items():
        if file_sha256(path) != _string(completion, key):
            raise ValidationError(f"completed training run has drifted {path.name}")
    manifest = _load_json(manifest_path, "training manifest")
    identity = _load_json(identity_path, "checkpoint identity")
    if _string(manifest, "schema_version") != TRAINING_MANIFEST_SCHEMA:
        raise ValidationError("unsupported training manifest schema")
    if _string(manifest, "status") != "complete":
        raise ValidationError("training manifest is not complete")
    if _string(manifest, "invocation_sha256") != invocation:
        raise ValidationError("training manifest invocation does not match seal")
    if _string(identity, "schema_version") != CHECKPOINT_IDENTITY_SCHEMA:
        raise ValidationError("unsupported checkpoint identity schema")
    checkpoint = run_directory / "checkpoint"
    if tree_digest(checkpoint) != _string(identity, "checkpoint_tree_sha256"):
        raise ValidationError("sealed checkpoint tree has drifted")
    if file_sha256(checkpoint / "model.safetensors") != _string(
        identity, "weights_sha256"
    ):
        raise ValidationError("sealed checkpoint weights have drifted")
    return CompletedTrainingRun(invocation, manifest, identity, completion)


def seal_training_run(
    *,
    run_directory: Path,
    invocation_sha256: str,
    training_manifest: Mapping[str, object],
    schedule_manifest: Mapping[str, object],
    runtime_manifest: Mapping[str, object],
    checkpoint_writer: Callable[[Path], None],
    failure_stage: FailureStage | None = None,
) -> CompletedTrainingRun:
    """Publish checkpoint plus manifests by one same-filesystem directory rename."""
    if run_directory.exists():
        return load_completed_training_run(
            run_directory, expected_invocation_sha256=invocation_sha256
        )
    run_directory.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{run_directory.name}.partial-", dir=run_directory.parent
        )
    )
    try:
        if failure_stage is FailureStage.BEFORE_CHECKPOINT_WRITE:
            raise RuntimeError("injected failure before checkpoint write")
        checkpoint = staging / "checkpoint"
        checkpoint_writer(checkpoint)
        if not (checkpoint / "model.safetensors").is_file():
            raise ValidationError("checkpoint writer did not emit model.safetensors")
        checkpoint_identity: dict[str, object] = {
            "schema_version": CHECKPOINT_IDENTITY_SCHEMA,
            "checkpoint_tree_sha256": tree_digest(checkpoint),
            "weights_sha256": file_sha256(checkpoint / "model.safetensors"),
            "files": tree_manifest(checkpoint),
        }
        _write_canonical_json(staging / "checkpoint_identity.json", checkpoint_identity)
        _write_canonical_json(staging / "batch_schedule.json", schedule_manifest)
        _write_canonical_json(staging / "runtime.json", runtime_manifest)
        if failure_stage is FailureStage.BEFORE_MANIFEST_WRITE:
            raise RuntimeError("injected failure before training manifest write")
        complete_manifest = dict(training_manifest)
        complete_manifest.update(
            {
                "schema_version": TRAINING_MANIFEST_SCHEMA,
                "status": "complete",
                "invocation_sha256": invocation_sha256,
                "checkpoint_tree_sha256": checkpoint_identity["checkpoint_tree_sha256"],
                "weights_sha256": checkpoint_identity["weights_sha256"],
            }
        )
        _write_canonical_json(staging / "training_manifest.json", complete_manifest)
        if failure_stage is FailureStage.BEFORE_RESULT_SEALING:
            raise RuntimeError("injected failure before result sealing")
        completion = {
            "schema_version": COMPLETION_SCHEMA,
            "status": "complete",
            "invocation_sha256": invocation_sha256,
            "training_manifest_sha256": file_sha256(staging / "training_manifest.json"),
            "checkpoint_identity_sha256": file_sha256(
                staging / "checkpoint_identity.json"
            ),
            "batch_schedule_file_sha256": file_sha256(staging / "batch_schedule.json"),
            "runtime_file_sha256": file_sha256(staging / "runtime.json"),
        }
        _write_canonical_json(staging / "run_complete.json", completion)
        load_completed_training_run(
            staging, expected_invocation_sha256=invocation_sha256
        )
        try:
            staging.rename(run_directory)
        except FileExistsError:
            return load_completed_training_run(
                run_directory, expected_invocation_sha256=invocation_sha256
            )
        return load_completed_training_run(
            run_directory, expected_invocation_sha256=invocation_sha256
        )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _dependency_versions() -> dict[str, str]:
    result: dict[str, str] = {}
    for package in ("torch", "transformers", "safetensors"):
        try:
            result[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            result[package] = "absent"
    return result


def _load_frozen_m3_identity(config: Mapping[str, object]) -> tuple[str, str]:
    m3 = _mapping(config.get("m3"), "M3 configuration")
    return _string(m3, "checkpoint_tree_sha256"), _string(m3, "weights_sha256")


def train_variant(
    *,
    artifact_root: Path,
    config_path: Path,
    m3_checkpoint: Path,
    repository_root: Path,
    variant: TrainingVariant,
    seed: int,
    failure_stage: FailureStage | None = None,
) -> CompletedTrainingRun:
    """Execute one frozen CPU run; this is never called by ordinary tests."""
    if variant in {TrainingVariant.V1_REPLAY_ONLY, TrainingVariant.A1_MARGIN_NO_REPLAY}:
        if seed != FROZEN_PRIMARY_SEED:
            raise ValidationError("V1 and A1 run only the predesignated primary seed")
    elif seed not in FROZEN_REPLICATION_SEEDS:
        raise ValidationError("V2/V3 seed is outside the frozen three-seed set")
    config = _load_json(config_path, "M4.13 configuration")
    if _string(config, "schema_version") != CONFIG_SCHEMA:
        raise ValidationError("unsupported M4.13 configuration schema")
    expected_tree, expected_weights = _load_frozen_m3_identity(config)
    continuation = validate_continuation_checkpoint(
        m3_checkpoint,
        expected_tree_sha256=expected_tree,
        expected_weights_sha256=expected_weights,
    )
    prepared = load_prepared_training_inputs(
        artifact_root=artifact_root, config_path=config_path
    )
    schedule = build_training_schedule(
        vitaminc=prepared.vitaminc,
        m3=prepared.m3,
        variant=variant,
        seed=seed,
    )
    if len(schedule.batches) != FROZEN_MICROBATCHES or (
        schedule.optimizer_steps != FROZEN_OPTIMIZER_STEPS
    ):
        raise ValidationError("materialized schedule changed the frozen step budget")
    if variant.is_mixed:
        comparator_variant = (
            TrainingVariant.V3_MARGIN_MIX
            if variant is TrainingVariant.V2_CE_MIX
            else TrainingVariant.V2_CE_MIX
        )
        comparator = build_training_schedule(
            vitaminc=prepared.vitaminc,
            m3=prepared.m3,
            variant=comparator_variant,
            seed=seed,
        )
        if variant is TrainingVariant.V2_CE_MIX:
            assert_v2_v3_schedule_identity(schedule, comparator)
        else:
            assert_v2_v3_schedule_identity(comparator, schedule)

    repository_provenance = validate_repository_provenance(repository_root)

    invocation_payload = {
        "schema_version": "groundloop-m4-13-training-invocation-v1",
        "variant": variant.value,
        "seed": seed,
        "config_sha256": prepared.config_sha256,
        "dataset_manifest_sha256": prepared.dataset_manifest_sha256,
        "train_vitaminc_sha256": prepared.train_vitaminc_sha256,
        "train_vitaminc_manifest_sha256": prepared.train_vitaminc_manifest_sha256,
        "train_m3_sha256": prepared.train_m3_sha256,
        "continuation_checkpoint_tree_sha256": continuation["tree_sha256"],
        "continuation_weights_sha256": continuation["weights_sha256"],
        "schedule_sha256": schedule.schedule_sha256,
        "objective": {
            "endpoint": "weighted-endpoint-ce-arithmetic-mean-v1",
            "paired": (
                "symmetric-log-probability-margin-v1"
                if variant.uses_paired_margin
                else None
            ),
            "margin": FROZEN_HYPERPARAMETERS.paired_margin,
            "paired_weight": FROZEN_HYPERPARAMETERS.paired_weight,
        },
        "hyperparameters": asdict(FROZEN_HYPERPARAMETERS),
        **repository_provenance.to_manifest(),
    }
    invocation_sha256 = _canonical_sha256(invocation_payload)
    run_directory = artifact_root / "runs" / variant.value / str(seed)
    if run_directory.exists():
        return load_completed_training_run(
            run_directory, expected_invocation_sha256=invocation_sha256
        )

    os.environ["OMP_NUM_THREADS"] = str(FROZEN_HYPERPARAMETERS.torch_threads)
    os.environ["MKL_NUM_THREADS"] = str(FROZEN_HYPERPARAMETERS.torch_threads)
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    try:
        import torch
        from transformers import (
            AutoModelForSequenceClassification,
            AutoTokenizer,
            get_linear_schedule_with_warmup,
        )
    except ImportError as error:
        raise RuntimeError(
            "M4.13 training requires the optional ML dependencies"
        ) from error
    random.seed(seed)
    torch.manual_seed(seed)
    torch.set_num_threads(FROZEN_HYPERPARAMETERS.torch_threads)
    torch.set_num_interop_threads(FROZEN_HYPERPARAMETERS.torch_interop_threads)
    torch.use_deterministic_algorithms(True)

    tokenizer = AutoTokenizer.from_pretrained(  # type: ignore[no-untyped-call]
        m3_checkpoint, local_files_only=True
    )
    model = AutoModelForSequenceClassification.from_pretrained(
        m3_checkpoint, local_files_only=True
    )
    model.cpu()
    if int(model.config.num_labels) != 3:
        raise ValidationError("continuation model does not expose three logits")
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=FROZEN_HYPERPARAMETERS.learning_rate,
        weight_decay=FROZEN_HYPERPARAMETERS.weight_decay,
    )
    scheduler = get_linear_schedule_with_warmup(  # type: ignore[no-untyped-call]
        optimizer,
        num_warmup_steps=math.floor(
            schedule.optimizer_steps * FROZEN_HYPERPARAMETERS.warmup_ratio
        ),
        num_training_steps=schedule.optimizer_steps,
    )
    weights = torch.tensor(schedule.base_order_class_weights, dtype=torch.float32)
    model.train()
    optimizer.zero_grad(set_to_none=True)
    started = time.perf_counter()
    loss_sum = 0.0
    endpoint_sum = 0.0
    paired_sum = 0.0
    optimizer_steps = 0
    for batch in schedule.batches:
        encoded = tokenizer(
            [row.example.evidence for row in batch.rows],
            [row.example.claim for row in batch.rows],
            padding=True,
            truncation=True,
            max_length=FROZEN_HYPERPARAMETERS.max_length,
            return_tensors="pt",
        )
        labels = torch.tensor(
            [row.example.base_label for row in batch.rows], dtype=torch.long
        )
        logits = model(**encoded).logits
        loss = change_aware_loss(
            logits,
            labels,
            weights,
            batch.transitions,
            include_paired_margin=(
                variant.uses_paired_margin and batch.domain == "vitaminc"
            ),
            margin=FROZEN_HYPERPARAMETERS.paired_margin,
            paired_weight=FROZEN_HYPERPARAMETERS.paired_weight,
        )
        (loss.total / batch.accumulation_divisor).backward()
        loss_sum += float(loss.total.detach())
        endpoint_sum += float(loss.endpoint.detach())
        paired_sum += float(loss.paired.detach())
        if batch.closes_optimizer_step:
            torch.nn.utils.clip_grad_norm_(
                model.parameters(), FROZEN_HYPERPARAMETERS.gradient_norm_clip
            )
            optimizer.step()
            scheduler.step()
            optimizer.zero_grad(set_to_none=True)
            optimizer_steps += 1
    if optimizer_steps != schedule.optimizer_steps:
        raise AssertionError("runtime optimizer steps differ from frozen schedule")
    wall_seconds = time.perf_counter() - started
    training_manifest: dict[str, object] = {
        **invocation_payload,
        "batch_order_sha256": schedule.batch_order_sha256,
        "optimizer_schedule_sha256": schedule.optimizer_schedule_sha256,
        "base_order_class_weights": list(schedule.base_order_class_weights),
        "microbatches": len(schedule.batches),
        "optimizer_steps": optimizer_steps,
        "mean_total_loss": loss_sum / len(schedule.batches),
        "mean_endpoint_loss": endpoint_sum / len(schedule.batches),
        "mean_paired_loss_over_all_microbatches": paired_sum / len(schedule.batches),
    }
    runtime_manifest: dict[str, object] = {
        "schema_version": "groundloop-m4-13-training-runtime-v1",
        "training_wall_seconds": wall_seconds,
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "dependencies": _dependency_versions(),
        "hardware": {
            "platform": platform.platform(),
            "processor": platform.processor(),
            "logical_cpus": os.cpu_count(),
            "torch_threads": FROZEN_HYPERPARAMETERS.torch_threads,
            "torch_interop_threads": FROZEN_HYPERPARAMETERS.torch_interop_threads,
            "cuda_available": torch.cuda.is_available(),
        },
    }

    def write_checkpoint(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=False)
        model.save_pretrained(path, safe_serialization=True)
        tokenizer.save_pretrained(path)

    return seal_training_run(
        run_directory=run_directory,
        invocation_sha256=invocation_sha256,
        training_manifest=training_manifest,
        schedule_manifest=schedule.to_manifest(),
        runtime_manifest=runtime_manifest,
        checkpoint_writer=write_checkpoint,
        failure_stage=failure_stage,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continue the frozen M3 verifier under one M4.13 variant."
    )
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--m3-checkpoint", type=Path, required=True)
    parser.add_argument(
        "--repository-root",
        type=Path,
        default=Path("."),
        help="exact clean Git worktree root whose trainer bytes are executed",
    )
    parser.add_argument(
        "--variant",
        choices=[variant.value for variant in TrainingVariant],
        required=True,
    )
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument(
        "--failure-stage", choices=[stage.value for stage in FailureStage]
    )
    return parser.parse_args()


def main() -> None:
    arguments = _parse_args()
    result = train_variant(
        artifact_root=arguments.artifact_root,
        config_path=arguments.config,
        m3_checkpoint=arguments.m3_checkpoint,
        repository_root=arguments.repository_root,
        variant=TrainingVariant(arguments.variant),
        seed=arguments.seed,
        failure_stage=(
            None
            if arguments.failure_stage is None
            else FailureStage(arguments.failure_stage)
        ),
    )
    print(json.dumps(dict(result.completion), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
