"""Typed, text-separated contracts for M4.13 evaluation."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from dataclasses import dataclass

from groundloop.errors import ValidationError

STORED_LABELS = ("support", "refute", "neutral")
BASE_LOGIT_ORDER = ("contradiction", "entailment", "neutral")


def text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class EvaluationRow:
    """One evaluator input. Raw text is never copied into raw-logit output."""

    fixture: str
    split: str
    row_id: str
    claim: str
    evidence: str
    label: str | None
    page_id: str | None = None
    case_id: str | None = None
    claim_group_id: str | None = None
    transition_id: str | None = None
    stratum: str | None = None

    def __post_init__(self) -> None:
        for name, value in (
            ("fixture", self.fixture),
            ("split", self.split),
            ("row_id", self.row_id),
            ("claim", self.claim),
            ("evidence", self.evidence),
        ):
            if not value.strip():
                raise ValidationError(f"evaluation {name} must be non-empty")
        if self.label is not None and self.label not in STORED_LABELS:
            raise ValidationError(f"unsupported evaluation label: {self.label}")

    @property
    def claim_sha256(self) -> str:
        return text_sha256(self.claim)

    @property
    def evidence_sha256(self) -> str:
        return text_sha256(self.evidence)

    @property
    def input_sha256(self) -> str:
        encoded = "groundloop-m4-13-input-v1\0" + self.evidence + "\0" + self.claim
        return text_sha256(encoded)


@dataclass(frozen=True, slots=True)
class ModelSpec:
    variant: str
    seed: int | None
    checkpoint_path: str
    checkpoint_tree_sha256: str
    model_identity: str
    calibration_identity: str
    checkpoint_identity_path: str
    calibration_path: str
    run_complete_sha256: str | None
    training_manifest_sha256: str | None
    batch_schedule_sha256: str | None
    training_git_head: str | None
    training_repository_dirty: bool | None
    trainer_implementation_sha256: str | None
    trainer_implementation_files_sha256: Mapping[str, str] | None
    temperature: float
    calibration_git_head: str | None = None
    calibration_repository_dirty: bool | None = None
    calibrator_implementation_sha256: str | None = None
    calibrator_implementation_files_sha256: Mapping[str, str] | None = None
    calibration_semantic_replay_verified: bool | None = None
    max_length: int = 256
    batch_size: int = 8

    def __post_init__(self) -> None:
        if not self.variant.strip() or not self.checkpoint_path.strip():
            raise ValidationError("model variant and checkpoint path are required")
        if not self.calibration_path.strip():
            raise ValidationError("model calibration path is required")
        if self.variant != "V0-frozen-m3" and not self.checkpoint_identity_path.strip():
            raise ValidationError("candidate checkpoint identity path is required")
        if self.variant != "V0-frozen-m3":
            for name, value in (
                ("run_complete_sha256", self.run_complete_sha256),
                ("training_manifest_sha256", self.training_manifest_sha256),
                ("batch_schedule_sha256", self.batch_schedule_sha256),
                ("trainer_implementation_sha256", self.trainer_implementation_sha256),
            ):
                if (
                    value is None
                    or len(value) != 64
                    or any(character not in "0123456789abcdef" for character in value)
                ):
                    raise ValidationError(f"candidate {name} is required")
            if (
                self.training_git_head is None
                or len(self.training_git_head) not in {40, 64}
                or any(
                    character not in "0123456789abcdef"
                    for character in self.training_git_head
                )
                or self.training_repository_dirty is not False
            ):
                raise ValidationError(
                    "candidate training repository provenance is invalid"
                )
            if not self.trainer_implementation_files_sha256:
                raise ValidationError(
                    "candidate trainer implementation file map is required"
                )
            for path, digest in self.trainer_implementation_files_sha256.items():
                if (
                    not path
                    or len(digest) != 64
                    or any(character not in "0123456789abcdef" for character in digest)
                ):
                    raise ValidationError("candidate trainer file identity is invalid")
        for name, value in (
            ("checkpoint_tree_sha256", self.checkpoint_tree_sha256),
            ("model_identity", self.model_identity),
            ("calibration_identity", self.calibration_identity),
        ):
            if len(value) != 64 or any(
                character not in "0123456789abcdef" for character in value
            ):
                raise ValidationError(f"{name} must be a lowercase SHA-256 digest")
        if not math.isfinite(self.temperature) or self.temperature <= 0.0:
            raise ValidationError("temperature must be finite and positive")
        if self.max_length != 256 or self.batch_size <= 0:
            raise ValidationError(
                "terminal scoring requires max_length=256 and positive batch"
            )

    @property
    def key(self) -> str:
        return self.variant if self.seed is None else f"{self.variant}:seed-{self.seed}"


@dataclass(frozen=True, slots=True)
class RawLogitRow:
    fixture: str
    split: str
    stratum: str | None
    page_id: str | None
    case_id: str | None
    claim_group_id: str | None
    transition_id: str | None
    row_id: str
    claim_sha256: str
    evidence_sha256: str
    mapped_label: str | None
    input_sha256: str
    base_logits: tuple[float, float, float]
    uncalibrated_probabilities: tuple[float, float, float]
    old_m3_temperature_probabilities: tuple[float, float, float]
    calibrated_probabilities: tuple[float, float, float]
    operational_label: str
    truncated: bool
    model_key: str
    model_identity: str
    calibration_identity: str
    temperature: float

    def __post_init__(self) -> None:
        if len(self.base_logits) != 3 or any(
            not math.isfinite(value) for value in self.base_logits
        ):
            raise ValidationError("raw logits must be finite")
        if self.operational_label not in STORED_LABELS:
            raise ValidationError("operational label is not a stored GroundLoop label")
        for probabilities in (
            self.uncalibrated_probabilities,
            self.old_m3_temperature_probabilities,
            self.calibrated_probabilities,
        ):
            if len(probabilities) != 3 or any(
                not math.isfinite(value) or value < 0.0 for value in probabilities
            ):
                raise ValidationError("probabilities must be finite and nonnegative")
            if not math.isclose(sum(probabilities), 1.0, abs_tol=1e-8):
                raise ValidationError("probabilities must sum to one")

    def payload(self) -> dict[str, object]:
        return {
            "schema_version": "groundloop-m4-13-raw-logit-v1",
            "fixture": self.fixture,
            "split": self.split,
            "stratum": self.stratum,
            "page_id": self.page_id,
            "case_id": self.case_id,
            "claim_group_id": self.claim_group_id,
            "transition_id": self.transition_id,
            "row_id": self.row_id,
            "claim_sha256": self.claim_sha256,
            "evidence_sha256": self.evidence_sha256,
            "mapped_label": self.mapped_label,
            "input_sha256": self.input_sha256,
            "base_logit_order": list(BASE_LOGIT_ORDER),
            "base_logits": list(self.base_logits),
            "stored_probability_order": list(STORED_LABELS),
            "uncalibrated_probabilities": list(self.uncalibrated_probabilities),
            "old_m3_temperature_probabilities": list(
                self.old_m3_temperature_probabilities
            ),
            "calibrated_probabilities": list(self.calibrated_probabilities),
            "operational_label": self.operational_label,
            "truncated": self.truncated,
            "model_key": self.model_key,
            "model_identity": self.model_identity,
            "calibration_identity": self.calibration_identity,
            "temperature": self.temperature,
            "contains_raw_text": False,
        }
