"""Development-only V2/V3 selection under the frozen M4.13 rule."""

from __future__ import annotations

import math
import os
import stat
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import cast

from groundloop.errors import ValidationError

from .common import (
    canonical_bytes,
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
)
from .contracts import BASE_LOGIT_ORDER, STORED_LABELS, RawLogitRow
from .metrics import classification_metrics, vitaminc_point_metrics
from .scoring import operational_label, stored_probabilities
from .selection import (
    ELIGIBLE_VARIANTS,
    PRIMARY_SEED,
    REPLICATION_SEEDS,
    SELECTION_SCHEMA,
    DiagnosticCheckpoint,
    SealedSelection,
    load_development_decision,
    load_sealed_selection,
)

_DEVELOPMENT_MODEL_KEYS = frozenset(
    {
        "V0-frozen-m3",
        "V1-replay-only:seed-20260720",
        "V2-ce-mix:seed-20260720",
        "V2-ce-mix:seed-20260721",
        "V2-ce-mix:seed-20260722",
        "V3-margin-mix:seed-20260720",
        "V3-margin-mix:seed-20260721",
        "V3-margin-mix:seed-20260722",
        "A1-margin-no-replay:seed-20260720",
    }
)
DEVELOPMENT_BUNDLE_FILES = frozenset(
    {
        *(
            f"development/{key}/development_logits.jsonl"
            for key in _DEVELOPMENT_MODEL_KEYS
        ),
        "development_report.json",
        "development_runtime.json",
        "selection.json",
        "DEVELOPMENT_COMPLETED.json",
    }
)


@dataclass(frozen=True, slots=True)
class DevelopmentCandidate:
    variant: str
    seed: int
    checkpoint_relative_path: str
    checkpoint_tree_sha256: str
    checkpoint_identity_sha256: str
    development_logits_path: str
    development_logits_sha256: str
    source_alignment_sha256: str
    run_complete_sha256: str
    training_manifest_sha256: str
    batch_schedule_sha256: str
    training_git_head: str
    training_repository_dirty: bool
    trainer_implementation_sha256: str
    trainer_implementation_files_sha256: Mapping[str, str]
    vitaminc_joint_correct: float
    m3_macro_f1: float
    m3_per_class_f1: Mapping[str, float]

    def __post_init__(self) -> None:
        values = (
            self.vitaminc_joint_correct,
            self.m3_macro_f1,
            *self.m3_per_class_f1.values(),
        )
        if any(not math.isfinite(value) or not 0.0 <= value <= 1.0 for value in values):
            raise ValidationError("development metrics must be finite probabilities")
        for name, value in (
            ("checkpoint_tree_sha256", self.checkpoint_tree_sha256),
            ("checkpoint_identity_sha256", self.checkpoint_identity_sha256),
            ("development_logits_sha256", self.development_logits_sha256),
            ("source_alignment_sha256", self.source_alignment_sha256),
            ("run_complete_sha256", self.run_complete_sha256),
            ("training_manifest_sha256", self.training_manifest_sha256),
            ("batch_schedule_sha256", self.batch_schedule_sha256),
        ):
            if len(value) != 64 or any(
                character not in "0123456789abcdef" for character in value
            ):
                raise ValidationError(f"{name} must be SHA-256")
        if (
            len(self.training_git_head) not in {40, 64}
            or any(
                character not in "0123456789abcdef"
                for character in self.training_git_head
            )
            or self.training_repository_dirty
        ):
            raise ValidationError(
                "development candidate training repository is invalid"
            )
        expected_trainer = canonical_sha256(
            {
                "schema_version": "groundloop-m4-13-trainer-implementation-v1",
                "files_sha256": dict(
                    sorted(self.trainer_implementation_files_sha256.items())
                ),
            }
        )
        if self.trainer_implementation_sha256 != expected_trainer:
            raise ValidationError("development candidate trainer identity drifted")


@dataclass(frozen=True, slots=True)
class FrozenM3DevelopmentBaseline:
    macro_f1: float
    per_class_f1: Mapping[str, float]
    checkpoint_tree_sha256: str
    development_logits_sha256: str
    source_alignment_sha256: str
    development_logits_path: str

    def __post_init__(self) -> None:
        values = (self.macro_f1, *self.per_class_f1.values())
        if any(not math.isfinite(value) or not 0.0 <= value <= 1.0 for value in values):
            raise ValidationError("V0 development metrics must be finite probabilities")
        for name, value in (
            ("checkpoint_tree_sha256", self.checkpoint_tree_sha256),
            ("development_logits_sha256", self.development_logits_sha256),
            ("source_alignment_sha256", self.source_alignment_sha256),
        ):
            if len(value) != 64 or any(
                character not in "0123456789abcdef" for character in value
            ):
                raise ValidationError(f"baseline {name} must be SHA-256")


@dataclass(frozen=True, slots=True)
class NoEligibleSelection:
    """Sealed negative development result; it can never unlock terminal data."""

    path: Path
    file_sha256: str
    selected_variant: None = None
    objective_design_verdict: str = "NO_ELIGIBLE_SELECTION"


def _guard(
    candidate: DevelopmentCandidate,
    baseline: FrozenM3DevelopmentBaseline,
) -> tuple[bool, tuple[str, ...]]:
    reasons: list[str] = []
    if baseline.macro_f1 - candidate.m3_macro_f1 > 0.03:
        reasons.append("m3_macro_f1_drop_gt_0.03")
    if set(candidate.m3_per_class_f1) != {"support", "refute", "neutral"}:
        raise ValidationError("M3 development per-class F1 surface is incomplete")
    for label, baseline_f1 in baseline.per_class_f1.items():
        if baseline_f1 - candidate.m3_per_class_f1[label] > 0.05:
            reasons.append(f"m3_{label}_f1_drop_gt_0.05")
    return not reasons, tuple(reasons)


def build_selection_payload(
    *,
    candidates: Sequence[DevelopmentCandidate],
    baseline: FrozenM3DevelopmentBaseline,
    development_report_path: str,
    development_report_sha256: str,
    diagnostic_checkpoints: Sequence[DiagnosticCheckpoint],
) -> Mapping[str, object]:
    expected = {
        (variant, seed) for variant in ELIGIBLE_VARIANTS for seed in REPLICATION_SEEDS
    }
    observed = [(candidate.variant, candidate.seed) for candidate in candidates]
    if len(observed) != len(set(observed)) or set(observed) != expected:
        raise ValidationError("development selection requires all six V2/V3 seeds")
    if set(baseline.per_class_f1) != {"support", "refute", "neutral"}:
        raise ValidationError("V0 M3 development per-class F1 surface is incomplete")
    if len(development_report_sha256) != 64 or not development_report_path:
        raise ValidationError("selection requires the immutable development report")
    if {(item.variant, item.seed) for item in diagnostic_checkpoints} != {
        ("V1-replay-only", PRIMARY_SEED),
        ("A1-margin-no-replay", PRIMARY_SEED),
    }:
        raise ValidationError("selection requires exact V1/A1 diagnostics")

    by_key = {
        (candidate.variant, candidate.seed): candidate for candidate in candidates
    }
    guards: dict[tuple[str, int], tuple[bool, tuple[str, ...]]] = {
        key: _guard(candidate, baseline) for key, candidate in by_key.items()
    }
    surviving = {
        variant: all(guards[(variant, seed)][0] for seed in REPLICATION_SEEDS)
        for variant in ELIGIBLE_VARIANTS
    }
    medians = {
        variant: median(
            by_key[(variant, seed)].vitaminc_joint_correct for seed in REPLICATION_SEEDS
        )
        for variant in ELIGIBLE_VARIANTS
    }
    corresponding_wins = sum(
        by_key[("V3-margin-mix", seed)].vitaminc_joint_correct
        >= by_key[("V2-ce-mix", seed)].vitaminc_joint_correct
        for seed in REPLICATION_SEEDS
    )
    margin_improvement = medians["V3-margin-mix"] - medians["V2-ce-mix"]
    paired_useful = (
        margin_improvement >= 0.01
        and surviving["V3-margin-mix"]
        and corresponding_wins >= 2
    )
    if paired_useful:
        selected = "V3-margin-mix"
        objective_verdict = "PAIRED_MARGIN_USEFUL"
    elif surviving["V2-ce-mix"]:
        selected = "V2-ce-mix"
        objective_verdict = "PAIRED_MARGIN_NOT_USEFUL"
    else:
        selected = None
        objective_verdict = "NO_ELIGIBLE_SELECTION"
    return {
        "schema_version": SELECTION_SCHEMA,
        "sealed": True,
        "terminal_data_accessed": False,
        "development_only": True,
        "selected_variant": selected,
        "terminal_evaluation_authorized": selected is not None,
        "primary_seed": PRIMARY_SEED,
        "development_report": {
            "path": development_report_path,
            "sha256": development_report_sha256,
        },
        "diagnostic_checkpoints": [
            {
                "variant": item.variant,
                "seed": item.seed,
                "checkpoint_tree_sha256": item.checkpoint_tree_sha256,
                "checkpoint_identity_sha256": item.checkpoint_identity_sha256,
                "development_logits_path": item.development_logits_path,
                "development_logits_sha256": item.development_logits_sha256,
                "source_alignment_sha256": item.source_alignment_sha256,
                "run_complete_sha256": item.run_complete_sha256,
                "training_manifest_sha256": item.training_manifest_sha256,
                "batch_schedule_sha256": item.batch_schedule_sha256,
                "training_git_head": item.training_git_head,
                "training_repository_dirty": item.training_repository_dirty,
                "trainer_implementation_sha256": item.trainer_implementation_sha256,
                "trainer_implementation_files_sha256": dict(
                    sorted(item.trainer_implementation_files_sha256.items())
                ),
            }
            for item in sorted(
                diagnostic_checkpoints, key=lambda value: (value.variant, value.seed)
            )
        ],
        "selection_rule": {
            "m3_macro_f1_max_drop": 0.03,
            "m3_per_class_f1_max_drop": 0.05,
            "vitaminc_joint_median_min_margin_gain": 0.01,
            "paired_seed_matches_required": 2,
            "tie_break": "V2-ce-mix",
            "terminal_metrics_used": False,
        },
        "v0_m3_development": {
            "macro_f1": baseline.macro_f1,
            "per_class_f1": dict(sorted(baseline.per_class_f1.items())),
            "checkpoint_tree_sha256": baseline.checkpoint_tree_sha256,
            "development_logits_sha256": baseline.development_logits_sha256,
            "development_logits_path": baseline.development_logits_path,
            "source_alignment_sha256": baseline.source_alignment_sha256,
        },
        "development_metrics": [
            {
                "variant": candidate.variant,
                "seed": candidate.seed,
                "vitaminc_joint_correct": candidate.vitaminc_joint_correct,
                "m3_macro_f1": candidate.m3_macro_f1,
                "m3_per_class_f1": dict(sorted(candidate.m3_per_class_f1.items())),
                "development_logits_sha256": candidate.development_logits_sha256,
                "development_logits_path": candidate.development_logits_path,
                "source_alignment_sha256": candidate.source_alignment_sha256,
                "run_complete_sha256": candidate.run_complete_sha256,
                "training_manifest_sha256": candidate.training_manifest_sha256,
                "batch_schedule_sha256": candidate.batch_schedule_sha256,
                "training_git_head": candidate.training_git_head,
                "training_repository_dirty": candidate.training_repository_dirty,
                "trainer_implementation_sha256": (
                    candidate.trainer_implementation_sha256
                ),
                "trainer_implementation_files_sha256": dict(
                    sorted(candidate.trainer_implementation_files_sha256.items())
                ),
                "forgetting_guard_passed": guards[candidate.variant, candidate.seed][0],
                "forgetting_guard_failures": list(
                    guards[candidate.variant, candidate.seed][1]
                ),
            }
            for candidate in sorted(
                candidates, key=lambda item: (item.variant, item.seed)
            )
        ],
        "variant_summary": {
            variant: {
                "survives_forgetting_guard": surviving[variant],
                "median_vitaminc_joint_correct": medians[variant],
            }
            for variant in ELIGIBLE_VARIANTS
        },
        "objective_design_verdict": {
            "verdict": objective_verdict,
            "median_joint_correct_delta_v3_minus_v2": margin_improvement,
            "v3_corresponding_seed_matches_or_wins": corresponding_wins,
        },
        "candidate_checkpoints": [
            {
                "variant": candidate.variant,
                "seed": candidate.seed,
                "checkpoint_relative_path": candidate.checkpoint_relative_path,
                "checkpoint_tree_sha256": candidate.checkpoint_tree_sha256,
                "checkpoint_identity_sha256": candidate.checkpoint_identity_sha256,
                "development_logits_path": candidate.development_logits_path,
                "development_logits_sha256": candidate.development_logits_sha256,
                "source_alignment_sha256": candidate.source_alignment_sha256,
                "run_complete_sha256": candidate.run_complete_sha256,
                "training_manifest_sha256": candidate.training_manifest_sha256,
                "batch_schedule_sha256": candidate.batch_schedule_sha256,
                "training_git_head": candidate.training_git_head,
                "training_repository_dirty": candidate.training_repository_dirty,
                "trainer_implementation_sha256": (
                    candidate.trainer_implementation_sha256
                ),
                "trainer_implementation_files_sha256": dict(
                    sorted(candidate.trainer_implementation_files_sha256.items())
                ),
            }
            for candidate in sorted(
                candidates, key=lambda item: (item.variant, item.seed)
            )
        ],
    }


def seal_selection(
    path: Path,
    *,
    candidates: Sequence[DevelopmentCandidate],
    baseline: FrozenM3DevelopmentBaseline,
    development_report_path: str,
    development_report_sha256: str,
    diagnostic_checkpoints: Sequence[DiagnosticCheckpoint],
) -> SealedSelection | NoEligibleSelection:
    payload = build_selection_payload(
        candidates=candidates,
        baseline=baseline,
        development_report_path=development_report_path,
        development_report_sha256=development_report_sha256,
        diagnostic_checkpoints=diagnostic_checkpoints,
    )
    encoded = canonical_bytes(payload)
    if path.exists():
        existing = load_json(path, "existing M4.13 selection")
        if canonical_bytes(existing) != encoded:
            raise ValidationError("existing sealed selection belongs to another result")
        if existing.get("selected_variant") is None:
            return NoEligibleSelection(path=path, file_sha256=file_sha256(path))
        return load_sealed_selection(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.partial")
    try:
        temporary.write_bytes(encoded)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    if payload.get("selected_variant") is None:
        return NoEligibleSelection(path=path, file_sha256=file_sha256(path))
    sealed = load_sealed_selection(path)
    if sealed.file_sha256 != file_sha256(path):
        raise AssertionError("selection file identity changed during sealing")
    return sealed


def _selection_logit_rows(
    *,
    path: Path,
    expected_sha256: str,
    variant: str,
    seed: int | None,
    checkpoint_tree_sha256: str,
    checkpoint_identity_sha256: str,
) -> tuple[RawLogitRow, ...]:
    if file_sha256(path) != expected_sha256:
        raise ValidationError(f"development logits drifted: {path}")
    result: list[RawLogitRow] = []
    for raw in load_jsonl(path, "development logits"):
        if raw.get("schema_version") != "groundloop-m4-13-development-logit-v1":
            raise ValidationError("unsupported development-logit schema")
        if raw.get("contains_raw_text") is not False:
            raise ValidationError(
                "development-logit artifact has an invalid text boundary"
            )
        if raw.get("split") != "development":
            raise ValidationError("development-logit split drifted")
        if raw.get("variant") != variant or raw.get("seed") != seed:
            raise ValidationError("development logits belong to another variant/seed")
        if raw.get("checkpoint_tree_sha256") != checkpoint_tree_sha256:
            raise ValidationError(
                "development logits belong to another checkpoint tree"
            )
        if raw.get("checkpoint_identity_sha256") != checkpoint_identity_sha256:
            raise ValidationError(
                "development logits belong to another checkpoint identity"
            )
        if (
            tuple(require_array(raw.get("base_logit_order"), "base logit order"))
            != BASE_LOGIT_ORDER
        ):
            raise ValidationError("development base-logit order drifted")
        logits_raw = require_array(raw.get("logits"), "development logits")
        if len(logits_raw) != 3:
            raise ValidationError("development row must contain exactly three logits")
        logits = cast(
            tuple[float, float, float],
            tuple(require_number(value, "development logit") for value in logits_raw),
        )
        label = require_text(raw.get("label"), "development label")
        if label not in STORED_LABELS:
            raise ValidationError("development row has an unsupported label")
        domain = require_text(raw.get("domain"), "development domain")
        if domain == "vitaminc":
            fixture = "vitaminc_development"
            group_field = raw.get("case_id")
        elif domain == "m3":
            fixture = "m3_development"
            group_field = raw.get("claim_group_id")
        else:
            raise ValidationError("development row has an unsupported domain")
        group_id = require_text(raw.get("group_id"), "development group ID")
        if group_field != group_id:
            raise ValidationError("development row group identity drifted")
        probabilities = stored_probabilities(logits, temperature=1.0)
        old_probabilities = stored_probabilities(logits, temperature=1.1037657679769346)
        if (
            tuple(
                require_array(
                    raw.get("stored_probability_order"), "stored probability order"
                )
            )
            != STORED_LABELS
        ):
            raise ValidationError("development stored-probability order drifted")
        for field, expected in (
            ("uncalibrated_probabilities", probabilities),
            ("old_m3_temperature_probabilities", old_probabilities),
            ("calibrated_probabilities", probabilities),
        ):
            observed = tuple(
                require_number(value, field)
                for value in require_array(raw.get(field), field)
            )
            if observed != expected:
                raise ValidationError(f"development {field} drifted")
        if raw.get("operational_label") != operational_label(probabilities):
            raise ValidationError("development operational label drifted")
        if not isinstance(raw.get("truncated"), bool):
            raise ValidationError("development truncation flag is invalid")
        if require_number(raw.get("temperature"), "development temperature") != 1.0:
            raise ValidationError("development selection must use T=1")
        optional_fields = {
            name: value if isinstance(value, str) else None
            for name in (
                "stratum",
                "page_id",
                "case_id",
                "claim_group_id",
                "transition_id",
            )
            for value in (raw.get(name),)
        }
        result.append(
            RawLogitRow(
                fixture=fixture,
                split="development",
                stratum=optional_fields["stratum"],
                page_id=optional_fields["page_id"],
                case_id=optional_fields["case_id"],
                claim_group_id=optional_fields["claim_group_id"],
                transition_id=optional_fields["transition_id"],
                row_id=require_text(raw.get("row_id"), "development row ID"),
                claim_sha256=require_sha256(raw.get("claim_sha256"), "claim hash"),
                evidence_sha256=require_sha256(
                    raw.get("evidence_sha256"), "evidence hash"
                ),
                mapped_label=label,
                input_sha256=require_sha256(raw.get("input_sha256"), "input hash"),
                base_logits=logits,
                uncalibrated_probabilities=probabilities,
                old_m3_temperature_probabilities=old_probabilities,
                calibrated_probabilities=probabilities,
                operational_label=operational_label(probabilities),
                truncated=bool(raw.get("truncated")),
                model_key=(variant if seed is None else f"{variant}:seed-{seed}"),
                model_identity=checkpoint_identity_sha256,
                calibration_identity="63141d5325af81f0a77fb072b61c0f46a8ee005a905ba4cacab3622ab953a5f0",
                temperature=1.0,
            )
        )
    if len(result) != 2011 or len({row.row_id for row in result}) != len(result):
        raise ValidationError("development logits are incomplete or duplicate row IDs")
    if sum(row.fixture == "vitaminc_development" for row in result) != 1024:
        raise ValidationError("development logits omit VitaminC rows")
    if sum(row.fixture == "m3_development" for row in result) != 987:
        raise ValidationError("development logits omit M3 rows")
    return tuple(result)


def _plain_path_lstat(path: Path, *, expect_directory: bool) -> Path:
    absolute = Path(os.path.abspath(path))
    for component in (*reversed(absolute.parents), absolute):
        try:
            metadata = os.lstat(component)
        except OSError as error:
            raise ValidationError(
                f"development bundle path is absent: {path}"
            ) from error
        if stat.S_ISLNK(metadata.st_mode):
            raise ValidationError(
                f"development bundle path contains a symlink: {path}"
            )
    if expect_directory:
        if not stat.S_ISDIR(metadata.st_mode):
            raise ValidationError(f"development bundle path is not a directory: {path}")
    elif not stat.S_ISREG(metadata.st_mode):
        raise ValidationError(f"development bundle path is not a regular file: {path}")
    return absolute


def preflight_development_bundle_paths(selection_path: Path) -> Path:
    """Validate the exact no-symlink bundle surface without opening any file."""
    selection = Path(os.path.abspath(selection_path))
    if selection.name != "selection.json":
        raise ValidationError("development selection path is not canonical")
    root = _plain_path_lstat(selection.parent, expect_directory=True)
    if selection != root / "selection.json":
        raise ValidationError("development selection is outside its bundle root")
    actual: set[str] = set()
    for current, directories, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        for name in (*directories, *files):
            path = current_path / name
            try:
                metadata = os.lstat(path)
            except OSError as error:
                raise ValidationError(
                    f"development bundle path disappeared: {path}"
                ) from error
            if stat.S_ISLNK(metadata.st_mode):
                raise ValidationError(
                    f"development bundle contains a symlink: {path}"
                )
            if name in directories:
                if not stat.S_ISDIR(metadata.st_mode):
                    raise ValidationError(
                        f"development bundle contains a non-directory: {path}"
                    )
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise ValidationError(
                    f"development bundle contains a non-regular file: {path}"
                )
            actual.add(path.relative_to(root).as_posix())
    if actual != DEVELOPMENT_BUNDLE_FILES:
        raise ValidationError("development bundle contains extra or missing files")
    for relative in DEVELOPMENT_BUNDLE_FILES:
        _plain_path_lstat(root / relative, expect_directory=False)
    return root


def validate_development_decision_evidence(
    path: Path, *, allow_synthetic: bool = False
) -> str | None:
    """Recompute a positive or negative decision from raw development logits."""
    preflight_development_bundle_paths(path)
    selection = load_development_decision(path)
    root = selection.path.parent
    report_path = root / selection.development_report_path
    if file_sha256(report_path) != selection.development_report_sha256:
        raise ValidationError("immutable development report drifted")
    report = load_json(report_path, "development report")
    if (
        report.get("schema_version") != "groundloop-m4-13-development-report-v1"
        or report.get("terminal_data_accessed") is not False
    ):
        raise ValidationError("development report boundary drifted")
    if not allow_synthetic and (
        report.get("evaluation_mode") != "production"
        or report.get("scientific_result") is not True
    ):
        raise ValidationError(
            "terminal evaluation requires a scientific development report"
        )
    execution = require_object(
        report.get("execution_identity"), "development execution identity"
    )
    if not allow_synthetic and execution.get("repository_dirty") is not False:
        raise ValidationError("scientific development report used a dirty repository")
    baseline_rows = _selection_logit_rows(
        path=root / selection.v0_development_logits_path,
        expected_sha256=selection.v0_development_logits_sha256,
        variant="V0-frozen-m3",
        seed=None,
        checkpoint_tree_sha256="81870b683cec57eff82665103fcff3a35f45b9c9be0e18b53dcd40f485bfa4cf",
        checkpoint_identity_sha256="81870b683cec57eff82665103fcff3a35f45b9c9be0e18b53dcd40f485bfa4cf",
    )
    report_dataset_sha256 = require_sha256(
        report.get("dataset_manifest_sha256"), "development dataset manifest"
    )
    report_vitamin_sha256 = require_sha256(
        report.get("vitaminc_manifest_sha256"), "development VitaminC manifest"
    )
    report_m3_sha256 = require_sha256(
        report.get("m3_development_sha256"), "development M3 source"
    )
    reconstructed_alignment = canonical_sha256(
        {
            "dataset_manifest_sha256": report_dataset_sha256,
            "vitaminc_manifest_sha256": report_vitamin_sha256,
            "m3_development_sha256": report_m3_sha256,
            "rows": [
                {
                    "fixture": row.fixture,
                    "row_id": row.row_id,
                    "page_id": row.page_id,
                    "case_id": row.case_id,
                    "claim_group_id": row.claim_group_id,
                    "transition_id": row.transition_id,
                    "stratum": row.stratum,
                    "label": row.mapped_label,
                    "claim_sha256": row.claim_sha256,
                    "evidence_sha256": row.evidence_sha256,
                }
                for row in baseline_rows
            ],
        }
    )
    if (
        report.get("source_alignment_sha256") != reconstructed_alignment
        or selection.source_alignment_sha256 != reconstructed_alignment
    ):
        raise ValidationError("development source alignment is not reproducible")

    def identity(row: RawLogitRow) -> tuple[object, ...]:
        return (
            row.fixture,
            row.row_id,
            row.stratum,
            row.page_id,
            row.case_id,
            row.claim_group_id,
            row.transition_id,
            row.mapped_label,
            row.claim_sha256,
            row.evidence_sha256,
            row.input_sha256,
        )

    source_identity = tuple(sorted(identity(row) for row in baseline_rows))
    report_models = require_object(report.get("models"), "development report models")
    for diagnostic in selection.diagnostic_checkpoints:
        rows = _selection_logit_rows(
            path=root / diagnostic.development_logits_path,
            expected_sha256=diagnostic.development_logits_sha256,
            variant=diagnostic.variant,
            seed=diagnostic.seed,
            checkpoint_tree_sha256=diagnostic.checkpoint_tree_sha256,
            checkpoint_identity_sha256=diagnostic.checkpoint_identity_sha256,
        )
        if tuple(sorted(identity(row) for row in rows)) != source_identity:
            raise ValidationError("diagnostic development source differs from V0")
        key = f"{diagnostic.variant}:seed-{diagnostic.seed}"
        recomputed = {
            "vitaminc": vitaminc_point_metrics(
                tuple(row for row in rows if row.fixture == "vitaminc_development")
            ),
            "m3": classification_metrics(
                tuple(row for row in rows if row.fixture == "m3_development")
            ),
            "development_logits_sha256": diagnostic.development_logits_sha256,
            "scoring_counts": _selection_scoring_counts(rows),
        }
        if canonical_bytes(report_models.get(key)) != canonical_bytes(recomputed):
            raise ValidationError(f"development report diagnostic drifted: {key}")
    baseline_m3 = classification_metrics(
        tuple(row for row in baseline_rows if row.fixture == "m3_development")
    )
    baseline_report_entry = {
        "vitaminc": vitaminc_point_metrics(
            tuple(row for row in baseline_rows if row.fixture == "vitaminc_development")
        ),
        "m3": baseline_m3,
        "development_logits_sha256": selection.v0_development_logits_sha256,
        "scoring_counts": _selection_scoring_counts(baseline_rows),
    }
    if canonical_bytes(report_models.get("V0-frozen-m3")) != canonical_bytes(
        baseline_report_entry
    ):
        raise ValidationError("development report V0 metrics drifted")
    baseline = FrozenM3DevelopmentBaseline(
        macro_f1=require_number(baseline_m3.get("macro_f1"), "M3 macro-F1"),
        per_class_f1=_selection_per_class_f1(baseline_m3),
        checkpoint_tree_sha256="81870b683cec57eff82665103fcff3a35f45b9c9be0e18b53dcd40f485bfa4cf",
        development_logits_sha256=selection.v0_development_logits_sha256,
        source_alignment_sha256=selection.source_alignment_sha256,
        development_logits_path=selection.v0_development_logits_path,
    )
    candidates: list[DevelopmentCandidate] = []
    for checkpoint in selection.checkpoints:
        rows = _selection_logit_rows(
            path=root / checkpoint.development_logits_path,
            expected_sha256=checkpoint.development_logits_sha256,
            variant=checkpoint.variant,
            seed=checkpoint.seed,
            checkpoint_tree_sha256=checkpoint.checkpoint_tree_sha256,
            checkpoint_identity_sha256=checkpoint.checkpoint_identity_sha256,
        )
        if tuple(sorted(identity(row) for row in rows)) != source_identity:
            raise ValidationError("candidate development source differs from V0")
        vitamin = vitaminc_point_metrics(
            tuple(row for row in rows if row.fixture == "vitaminc_development")
        )
        transition = require_object(vitamin.get("transition"), "VitaminC transition")
        point = require_object(transition.get("point"), "VitaminC transition point")
        m3 = classification_metrics(
            tuple(row for row in rows if row.fixture == "m3_development")
        )
        report_key = f"{checkpoint.variant}:seed-{checkpoint.seed}"
        report_entry = {
            "vitaminc": vitamin,
            "m3": m3,
            "development_logits_sha256": checkpoint.development_logits_sha256,
            "scoring_counts": _selection_scoring_counts(rows),
        }
        if canonical_bytes(report_models.get(report_key)) != canonical_bytes(
            report_entry
        ):
            raise ValidationError(f"development report candidate drifted: {report_key}")
        candidates.append(
            DevelopmentCandidate(
                variant=checkpoint.variant,
                seed=checkpoint.seed,
                checkpoint_relative_path=checkpoint.checkpoint_relative_path,
                checkpoint_tree_sha256=checkpoint.checkpoint_tree_sha256,
                checkpoint_identity_sha256=checkpoint.checkpoint_identity_sha256,
                development_logits_path=checkpoint.development_logits_path,
                development_logits_sha256=checkpoint.development_logits_sha256,
                source_alignment_sha256=checkpoint.source_alignment_sha256,
                run_complete_sha256=checkpoint.run_complete_sha256,
                training_manifest_sha256=checkpoint.training_manifest_sha256,
                batch_schedule_sha256=checkpoint.batch_schedule_sha256,
                training_git_head=checkpoint.training_git_head,
                training_repository_dirty=checkpoint.training_repository_dirty,
                trainer_implementation_sha256=checkpoint.trainer_implementation_sha256,
                trainer_implementation_files_sha256=(
                    checkpoint.trainer_implementation_files_sha256
                ),
                vitaminc_joint_correct=float(
                    require_number(point.get("joint_correct"), "joint correct")
                ),
                m3_macro_f1=float(require_number(m3.get("macro_f1"), "M3 macro F1")),
                m3_per_class_f1=_selection_per_class_f1(m3),
            )
        )
    expected = build_selection_payload(
        candidates=candidates,
        baseline=baseline,
        development_report_path=selection.development_report_path,
        development_report_sha256=selection.development_report_sha256,
        diagnostic_checkpoints=selection.diagnostic_checkpoints,
    )
    actual = load_json(selection.path, "sealed selection")
    if canonical_bytes(expected) != canonical_bytes(actual):
        raise ValidationError(
            "sealed selection is not derivable from raw development logits"
        )
    return selection.selected_variant


def validate_selection_evidence(
    selection: SealedSelection, *, allow_synthetic: bool = False
) -> None:
    """Recompute an eligible sealed choice from raw development logits."""
    selected = validate_development_decision_evidence(
        selection.path, allow_synthetic=allow_synthetic
    )
    if selected != selection.selected_variant:
        raise ValidationError("loaded selection differs from replayed decision")


def _development_bundle_path(root: Path, relative: str) -> Path:
    value = Path(relative)
    if value.is_absolute() or value == Path(".") or ".." in value.parts:
        raise ValidationError("development completion seal contains an unsafe path")
    candidate = root / value
    resolved_root = root.resolve()
    resolved = candidate.resolve()
    if candidate.is_symlink() or resolved_root not in resolved.parents:
        raise ValidationError("development sealed path escapes its bundle")
    return resolved


def validate_development_bundle(
    selection: SealedSelection, *, allow_synthetic: bool = False
) -> Mapping[str, object]:
    """Validate the exact completed development bundle, including runtime."""
    root = preflight_development_bundle_paths(selection.path)
    validate_selection_evidence(selection, allow_synthetic=allow_synthetic)
    expected_logits = {
        selection.v0_development_logits_path,
        *(item.development_logits_path for item in selection.checkpoints),
        *(item.development_logits_path for item in selection.diagnostic_checkpoints),
    }
    expected_files = expected_logits | {
        "development_report.json",
        "development_runtime.json",
        "selection.json",
    }
    seal_path = root / "DEVELOPMENT_COMPLETED.json"
    seal = load_json(seal_path, "development completion seal")
    if (
        seal.get("schema_version") != "groundloop-m4-13-development-complete-v1"
        or seal.get("state") != "COMPLETED"
        or seal.get("terminal_evaluation_authorized") is not True
    ):
        raise ValidationError("development completion seal is invalid")
    entries = require_array(seal.get("files"), "development sealed files")
    observed: dict[str, str] = {}
    for raw in entries:
        item = require_object(raw, "development sealed file")
        relative = require_text(item.get("path"), "development sealed path")
        if relative in observed:
            raise ValidationError("development completion seal repeats a file")
        observed[relative] = require_sha256(
            item.get("sha256"), "development sealed file hash"
        )
    if set(observed) != expected_files:
        raise ValidationError("development sealed file surface drifted")
    for relative, expected_sha256 in observed.items():
        if file_sha256(_development_bundle_path(root, relative)) != expected_sha256:
            raise ValidationError(f"sealed development artifact drifted: {relative}")
    actual_files = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() or path.is_symlink()
    }
    if actual_files != expected_files | {"DEVELOPMENT_COMPLETED.json"}:
        raise ValidationError("development bundle contains extra or missing files")
    report = load_json(root / "development_report.json", "development report")
    runtime = load_json(root / "development_runtime.json", "development runtime")
    invocation_sha256 = require_sha256(
        seal.get("invocation_sha256"), "development invocation"
    )
    if (
        report.get("invocation_sha256") != invocation_sha256
        or runtime.get("invocation_sha256") != invocation_sha256
        or runtime.get("schema_version") != "groundloop-m4-13-development-runtime-v1"
        or seal.get("selection_sha256") != file_sha256(selection.path)
        or seal.get("development_report_sha256")
        != file_sha256(root / "development_report.json")
        or seal.get("runtime_sha256") != file_sha256(root / "development_runtime.json")
    ):
        raise ValidationError("development completion hash chain drifted")
    model_keys = {
        "V0-frozen-m3",
        *(f"{item.variant}:seed-{item.seed}" for item in selection.checkpoints),
        *(
            f"{item.variant}:seed-{item.seed}"
            for item in selection.diagnostic_checkpoints
        ),
    }
    scoring = require_array(runtime.get("scoring"), "development runtime scoring")
    if len(scoring) != 18:
        raise ValidationError("development runtime scoring surface is incomplete")
    observed_calls: set[tuple[str, str]] = set()
    for raw in scoring:
        item = require_object(raw, "development runtime score call")
        model_key = require_text(item.get("model_key"), "development model key")
        fixture = require_text(item.get("fixture"), "development fixture")
        call = (model_key, fixture)
        if (
            model_key not in model_keys
            or fixture not in {"vitaminc_development", "m3_development"}
            or call in observed_calls
            or require_integer(item.get("rows"), "development scored rows")
            != (1024 if fixture == "vitaminc_development" else 987)
            or require_integer(item.get("failures"), "development failures") != 0
            or require_integer(item.get("timeouts"), "development timeouts") != 0
        ):
            raise ValidationError("development runtime scoring record drifted")
        observed_calls.add(call)
    if observed_calls != {
        (model_key, fixture)
        for model_key in model_keys
        for fixture in ("vitaminc_development", "m3_development")
    }:
        raise ValidationError("development runtime omitted a model/fixture call")
    return {
        "schema_version": "groundloop-m4-13-development-bundle-proof-v1",
        "completion_sha256": file_sha256(seal_path),
        "invocation_sha256": invocation_sha256,
        "selection_sha256": selection.file_sha256,
        "development_report_sha256": selection.development_report_sha256,
        "runtime_sha256": file_sha256(root / "development_runtime.json"),
        "files": len(expected_files) + 1,
    }


def _selection_scoring_counts(
    rows: Sequence[RawLogitRow],
) -> Mapping[str, object]:
    return {
        "rows": len(rows),
        "truncated_rows": sum(row.truncated for row in rows),
        "failures": 0,
        "timeouts": 0,
        "operational_label_counts": dict(
            sorted(Counter(row.operational_label for row in rows).items())
        ),
    }


def _selection_per_class_f1(metrics: Mapping[str, object]) -> Mapping[str, float]:
    per_class = require_object(metrics.get("per_class"), "per-class metrics")
    result: dict[str, float] = {}
    for label in STORED_LABELS:
        values = require_object(per_class.get(label), f"{label} metrics")
        result[label] = require_number(values.get("f1"), f"{label} F1")
    return result
