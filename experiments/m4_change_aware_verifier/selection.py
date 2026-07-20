"""Fail-closed development selection and terminal checkpoint allow-list."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from groundloop.errors import ValidationError

from .common import (
    canonical_sha256,
    file_sha256,
    load_json,
    require_array,
    require_integer,
    require_object,
    require_sha256,
    require_text,
)

SELECTION_SCHEMA = "groundloop-m4-13-selection-v1"
FROZEN_M3_TREE_SHA256 = (
    "81870b683cec57eff82665103fcff3a35f45b9c9be0e18b53dcd40f485bfa4cf"
)
PRIMARY_SEED = 20260720
REPLICATION_SEEDS = (20260720, 20260721, 20260722)
ELIGIBLE_VARIANTS = ("V2-ce-mix", "V3-margin-mix")


def _relative_artifact_path(value: object, name: str) -> str:
    text = require_text(value, name)
    path = Path(text)
    if path.is_absolute() or ".." in path.parts or path == Path("."):
        raise ValidationError(f"{name} must be a contained relative path")
    return path.as_posix()


@dataclass(frozen=True, slots=True)
class CandidateCheckpoint:
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

    @property
    def key(self) -> tuple[str, int]:
        return self.variant, self.seed


@dataclass(frozen=True, slots=True)
class DiagnosticCheckpoint:
    variant: str
    seed: int
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


@dataclass(frozen=True, slots=True)
class SealedSelection:
    path: Path
    file_sha256: str
    selected_variant: str
    primary_seed: int
    checkpoints: tuple[CandidateCheckpoint, ...]
    objective_design_verdict: str
    v0_development_logits_path: str
    v0_development_logits_sha256: str
    source_alignment_sha256: str
    development_report_path: str
    development_report_sha256: str
    diagnostic_checkpoints: tuple[DiagnosticCheckpoint, ...]

    @property
    def selected_checkpoints(self) -> tuple[CandidateCheckpoint, ...]:
        return tuple(
            checkpoint
            for checkpoint in self.checkpoints
            if checkpoint.variant == self.selected_variant
        )

    def authorize(
        self,
        *,
        variant: str,
        seed: int | None,
        checkpoint_tree_sha256: str,
        checkpoint_identity_sha256: str | None = None,
    ) -> None:
        """Authorize only frozen V0 or the three development-selected candidates."""
        if variant == "V0-frozen-m3":
            if seed is not None or checkpoint_tree_sha256 != FROZEN_M3_TREE_SHA256:
                raise ValidationError("V0 terminal identity is not the frozen M3 tree")
            return
        if variant != self.selected_variant or seed not in REPLICATION_SEEDS:
            raise ValidationError("checkpoint is absent from the terminal allow-list")
        matches = [
            checkpoint
            for checkpoint in self.selected_checkpoints
            if checkpoint.seed == seed
            and checkpoint.checkpoint_tree_sha256 == checkpoint_tree_sha256
            and (
                checkpoint_identity_sha256 is None
                or checkpoint.checkpoint_identity_sha256 == checkpoint_identity_sha256
            )
        ]
        if len(matches) != 1:
            raise ValidationError("checkpoint digest is absent from sealed selection")


@dataclass(frozen=True, slots=True)
class NegativeSelectionEvidence:
    """Complete negative decision surface used for semantic replay only."""

    path: Path
    file_sha256: str
    selected_variant: None
    primary_seed: int
    checkpoints: tuple[CandidateCheckpoint, ...]
    objective_design_verdict: str
    v0_development_logits_path: str
    v0_development_logits_sha256: str
    source_alignment_sha256: str
    development_report_path: str
    development_report_sha256: str
    diagnostic_checkpoints: tuple[DiagnosticCheckpoint, ...]


def load_development_decision(
    path: Path,
) -> SealedSelection | NegativeSelectionEvidence:
    """Load the complete positive or negative development-decision surface."""
    payload = load_json(path, "M4.13 selection")
    if (
        require_text(payload.get("schema_version"), "selection schema")
        != SELECTION_SCHEMA
    ):
        raise ValidationError("unsupported M4.13 selection schema")
    if payload.get("sealed") is not True:
        raise ValidationError("selection.json is not sealed")
    if payload.get("terminal_data_accessed") is not False:
        raise ValidationError("selection must assert zero terminal-data access")
    selected_value = payload.get("selected_variant")
    if selected_value is None:
        objective = require_object(
            payload.get("objective_design_verdict"), "objective_design_verdict"
        )
        if (
            objective.get("verdict") != "NO_ELIGIBLE_SELECTION"
            or payload.get("terminal_evaluation_authorized") is not False
        ):
            raise ValidationError("malformed negative selection outcome")
        selected: str | None = None
    else:
        selected = require_text(selected_value, "selected_variant")
        if selected not in ELIGIBLE_VARIANTS:
            raise ValidationError(
                "only V2 or V3 may be selected for terminal evaluation"
            )
        if payload.get("terminal_evaluation_authorized") is not True:
            raise ValidationError(
                "selected development result does not authorize terminal access"
            )
    primary = require_integer(payload.get("primary_seed"), "primary_seed")
    if primary != PRIMARY_SEED:
        raise ValidationError("the deployable primary seed was not predesignated")
    raw_checkpoints = require_array(
        payload.get("candidate_checkpoints"), "candidate_checkpoints"
    )
    checkpoints: list[CandidateCheckpoint] = []
    for index, raw in enumerate(raw_checkpoints):
        item = require_object(raw, f"candidate checkpoint {index}")
        variant = require_text(item.get("variant"), "checkpoint variant")
        seed = require_integer(item.get("seed"), "checkpoint seed")
        if variant not in ELIGIBLE_VARIANTS or seed not in REPLICATION_SEEDS:
            raise ValidationError("candidate checkpoint is not a frozen V2/V3 seed")
        checkpoints.append(
            CandidateCheckpoint(
                variant=variant,
                seed=seed,
                checkpoint_relative_path=_relative_artifact_path(
                    item.get("checkpoint_relative_path"),
                    "checkpoint_relative_path",
                ),
                checkpoint_tree_sha256=require_sha256(
                    item.get("checkpoint_tree_sha256"),
                    "checkpoint_tree_sha256",
                ),
                checkpoint_identity_sha256=require_sha256(
                    item.get("checkpoint_identity_sha256"),
                    "checkpoint_identity_sha256",
                ),
                development_logits_path=_relative_artifact_path(
                    item.get("development_logits_path"),
                    "development_logits_path",
                ),
                development_logits_sha256=require_sha256(
                    item.get("development_logits_sha256"),
                    "development_logits_sha256",
                ),
                source_alignment_sha256=require_sha256(
                    item.get("source_alignment_sha256"),
                    "source_alignment_sha256",
                ),
                run_complete_sha256=require_sha256(
                    item.get("run_complete_sha256"), "run_complete_sha256"
                ),
                training_manifest_sha256=require_sha256(
                    item.get("training_manifest_sha256"),
                    "training_manifest_sha256",
                ),
                batch_schedule_sha256=require_sha256(
                    item.get("batch_schedule_sha256"),
                    "batch_schedule_sha256",
                ),
                training_git_head=require_text(
                    item.get("training_git_head"), "training_git_head"
                ),
                training_repository_dirty=(
                    False if item.get("training_repository_dirty") is False else True
                ),
                trainer_implementation_sha256=require_sha256(
                    item.get("trainer_implementation_sha256"),
                    "trainer_implementation_sha256",
                ),
                trainer_implementation_files_sha256={
                    require_text(key, "trainer implementation path"): require_sha256(
                        value, "trainer implementation file hash"
                    )
                    for key, value in require_object(
                        item.get("trainer_implementation_files_sha256"),
                        "trainer implementation files",
                    ).items()
                },
            )
        )
    keys = [checkpoint.key for checkpoint in checkpoints]
    expected = {
        (variant, seed) for variant in ELIGIBLE_VARIANTS for seed in REPLICATION_SEEDS
    }
    if len(keys) != len(set(keys)) or set(keys) != expected:
        raise ValidationError("selection must bind exactly all six V2/V3 checkpoints")
    if any(checkpoint.training_repository_dirty for checkpoint in checkpoints):
        raise ValidationError("selection contains a dirty training run")
    if any(
        len(checkpoint.training_git_head) not in {40, 64}
        or any(
            character not in "0123456789abcdef"
            for character in checkpoint.training_git_head
        )
        for checkpoint in checkpoints
    ):
        raise ValidationError("selection contains an invalid training Git identity")
    for checkpoint in checkpoints:
        expected_trainer = canonical_sha256(
            {
                "schema_version": "groundloop-m4-13-trainer-implementation-v1",
                "files_sha256": dict(
                    sorted(checkpoint.trainer_implementation_files_sha256.items())
                ),
            }
        )
        if checkpoint.trainer_implementation_sha256 != expected_trainer:
            raise ValidationError("selection trainer implementation digest drifted")
    trainer_identities = {
        (
            checkpoint.training_git_head,
            checkpoint.trainer_implementation_sha256,
            tuple(sorted(checkpoint.trainer_implementation_files_sha256.items())),
        )
        for checkpoint in checkpoints
    }
    if len(trainer_identities) != 1:
        raise ValidationError("selected candidates do not share one trainer identity")
    objective = require_object(
        payload.get("objective_design_verdict"), "objective_design_verdict"
    )
    verdict = require_text(objective.get("verdict"), "objective design verdict")
    if selected is None:
        if verdict != "NO_ELIGIBLE_SELECTION":
            raise ValidationError("negative selection has an invalid verdict")
    elif verdict not in {"PAIRED_MARGIN_USEFUL", "PAIRED_MARGIN_NOT_USEFUL"}:
        raise ValidationError("unsupported objective-design verdict")
    elif (selected == "V3-margin-mix") != (verdict == "PAIRED_MARGIN_USEFUL"):
        raise ValidationError("selected variant contradicts objective-design verdict")
    if payload.get("development_only") is not True:
        raise ValidationError(
            "selection must identify its evidence as development-only"
        )
    v0 = require_object(payload.get("v0_m3_development"), "V0 development")
    v0_logits_path = _relative_artifact_path(
        v0.get("development_logits_path"), "V0 development logits path"
    )
    v0_logits_sha = require_sha256(
        v0.get("development_logits_sha256"), "V0 development logits hash"
    )
    source_alignment = require_sha256(
        v0.get("source_alignment_sha256"), "development source alignment"
    )
    if any(
        checkpoint.source_alignment_sha256 != source_alignment
        for checkpoint in checkpoints
    ):
        raise ValidationError("candidate development source alignments differ")
    report = require_object(payload.get("development_report"), "development report")
    report_path = _relative_artifact_path(report.get("path"), "development report path")
    report_sha256 = require_sha256(report.get("sha256"), "development report hash")
    raw_diagnostics = require_array(
        payload.get("diagnostic_checkpoints"), "diagnostic checkpoints"
    )
    diagnostics: list[DiagnosticCheckpoint] = []
    for raw in raw_diagnostics:
        item = require_object(raw, "diagnostic checkpoint")
        files = {
            require_text(key, "trainer implementation path"): require_sha256(
                value, "trainer implementation file hash"
            )
            for key, value in require_object(
                item.get("trainer_implementation_files_sha256"),
                "trainer implementation files",
            ).items()
        }
        diagnostic = DiagnosticCheckpoint(
            variant=require_text(item.get("variant"), "diagnostic variant"),
            seed=require_integer(item.get("seed"), "diagnostic seed"),
            checkpoint_tree_sha256=require_sha256(
                item.get("checkpoint_tree_sha256"), "diagnostic checkpoint tree"
            ),
            checkpoint_identity_sha256=require_sha256(
                item.get("checkpoint_identity_sha256"),
                "diagnostic checkpoint identity",
            ),
            development_logits_path=_relative_artifact_path(
                item.get("development_logits_path"), "diagnostic logits path"
            ),
            development_logits_sha256=require_sha256(
                item.get("development_logits_sha256"), "diagnostic logits hash"
            ),
            source_alignment_sha256=require_sha256(
                item.get("source_alignment_sha256"), "diagnostic source alignment"
            ),
            run_complete_sha256=require_sha256(
                item.get("run_complete_sha256"), "diagnostic completion hash"
            ),
            training_manifest_sha256=require_sha256(
                item.get("training_manifest_sha256"), "diagnostic training manifest"
            ),
            batch_schedule_sha256=require_sha256(
                item.get("batch_schedule_sha256"), "diagnostic schedule hash"
            ),
            training_git_head=require_text(
                item.get("training_git_head"), "diagnostic Git HEAD"
            ),
            training_repository_dirty=(
                False if item.get("training_repository_dirty") is False else True
            ),
            trainer_implementation_sha256=require_sha256(
                item.get("trainer_implementation_sha256"),
                "diagnostic trainer implementation",
            ),
            trainer_implementation_files_sha256=files,
        )
        diagnostics.append(diagnostic)
    if {(item.variant, item.seed) for item in diagnostics} != {
        ("V1-replay-only", PRIMARY_SEED),
        ("A1-margin-no-replay", PRIMARY_SEED),
    } or len(diagnostics) != 2:
        raise ValidationError("selection must bind exact V1/A1 diagnostic checkpoints")
    common_trainer = next(iter(trainer_identities))
    for diagnostic in diagnostics:
        identity = (
            diagnostic.training_git_head,
            diagnostic.trainer_implementation_sha256,
            tuple(sorted(diagnostic.trainer_implementation_files_sha256.items())),
        )
        if (
            diagnostic.training_repository_dirty
            or diagnostic.source_alignment_sha256 != source_alignment
            or identity != common_trainer
        ):
            raise ValidationError("diagnostic checkpoint provenance differs")
    if selected is None:
        return NegativeSelectionEvidence(
            path=path,
            file_sha256=file_sha256(path),
            selected_variant=None,
            primary_seed=primary,
            checkpoints=tuple(checkpoints),
            objective_design_verdict=verdict,
            v0_development_logits_path=v0_logits_path,
            v0_development_logits_sha256=v0_logits_sha,
            source_alignment_sha256=source_alignment,
            development_report_path=report_path,
            development_report_sha256=report_sha256,
            diagnostic_checkpoints=tuple(diagnostics),
        )
    return SealedSelection(
        path=path,
        file_sha256=file_sha256(path),
        selected_variant=selected,
        primary_seed=primary,
        checkpoints=tuple(checkpoints),
        objective_design_verdict=verdict,
        v0_development_logits_path=v0_logits_path,
        v0_development_logits_sha256=v0_logits_sha,
        source_alignment_sha256=source_alignment,
        development_report_path=report_path,
        development_report_sha256=report_sha256,
        diagnostic_checkpoints=tuple(diagnostics),
    )


def load_sealed_selection(path: Path) -> SealedSelection:
    """Load an eligible decision; negative decisions can never unlock terminal."""
    decision = load_development_decision(path)
    if isinstance(decision, NegativeSelectionEvidence):
        raise ValidationError(
            "sealed decision is NO_ELIGIBLE_SELECTION; terminal evaluation is forbidden"
        )
    return decision
