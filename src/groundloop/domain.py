"""Immutable domain records for the GroundLoop M1 reference semantics.

These types implement the frozen v0.2 design (docs/technical_design.md).
Observations store score vectors, never permanent labels; labels are derived
by a versioned decision policy. Activation state belongs to the repository,
never to the immutable historical objects defined here.
"""

import hashlib
import math
import re
from dataclasses import dataclass, field
from enum import StrEnum

from groundloop.errors import ValidationError

_WHITESPACE_RUN = re.compile(r"\s+")


def normalized_text_hash(text: str) -> str:
    """Normalization v1: strip, collapse whitespace runs, then SHA-256."""
    normalized = _WHITESPACE_RUN.sub(" ", text.strip())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class VerificationLabel(StrEnum):
    SUPPORT = "support"
    REFUTE = "refute"
    NEUTRAL = "neutral"


class SubjectKind(StrEnum):
    CLAIM = "claim"
    REQUIREMENT = "requirement"


class ClaimStatus(StrEnum):
    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"
    REFUTED = "refuted"
    CONFLICTED = "conflicted"


class AnswerStatus(StrEnum):
    VALID = "valid"
    PARTIALLY_SUPPORTED = "partially_supported"
    UNSUPPORTED = "unsupported"
    CONFLICTED = "conflicted"
    CONTRADICTED = "contradicted"


class EvaluationState(StrEnum):
    """Publication confidence for the M2 semantic-epoch coordinator."""

    COMPLETE = "complete"
    PENDING = "pending"
    DEGRADED = "degraded"
    FAILED = "failed"


class UpdateOperation(StrEnum):
    INSERT = "insert"
    DELETE = "delete"
    REPLACE = "replace"
    POLICY_CHANGE = "policy_change"
    OBSERVE = "observe"


@dataclass(frozen=True, slots=True)
class ModelStamp:
    """Identity of the model and prompt that produced a record."""

    model_id: str
    model_version: str
    prompt_version: str


@dataclass(frozen=True, slots=True)
class DocumentVersion:
    document_version_id: str
    document_id: str
    content_hash: str


@dataclass(frozen=True, slots=True)
class ChunkVersion:
    chunk_version_id: str
    document_version_id: str
    chunk_index: int
    text: str
    text_hash: str = field(default="")

    def __post_init__(self) -> None:
        if self.chunk_index < 0:
            raise ValidationError(
                f"chunk {self.chunk_version_id}: chunk_index must be nonnegative"
            )
        expected_hash = normalized_text_hash(self.text)
        if not self.text_hash:
            object.__setattr__(self, "text_hash", expected_hash)
        elif self.text_hash != expected_hash:
            raise ValidationError(
                f"chunk {self.chunk_version_id}: supplied text_hash does not "
                "match normalization v1"
            )


@dataclass(frozen=True, slots=True)
class Question:
    question_id: str
    text: str


@dataclass(frozen=True, slots=True)
class AnswerVersion:
    answer_version_id: str
    question_id: str
    text: str
    producer: ModelStamp


@dataclass(frozen=True, slots=True)
class Claim:
    claim_id: str
    answer_version_id: str
    text: str
    extractor: ModelStamp
    required: bool


ObservationKey = tuple[SubjectKind, str, str, str]


@dataclass(frozen=True, slots=True)
class SemanticObservation:
    """A versioned neural score observation, not an assertion of truth.

    Scores must be finite and lie in [0, 1]. Labels are never stored here;
    they are derived by the current DecisionPolicy (tie rule v1).
    """

    observation_id: str
    subject_kind: SubjectKind
    subject_id: str
    chunk_version_id: str
    task_type: str
    support_score: float
    refute_score: float
    neutral_score: float
    producer: ModelStamp
    input_hash: str

    def __post_init__(self) -> None:
        for name, score in (
            ("support_score", self.support_score),
            ("refute_score", self.refute_score),
            ("neutral_score", self.neutral_score),
        ):
            if not math.isfinite(score) or not 0.0 <= score <= 1.0:
                raise ValidationError(
                    f"observation {self.observation_id}: {name}={score!r} "
                    "must be finite and within [0, 1]"
                )

    @property
    def key(self) -> ObservationKey:
        """The currency key: at most one current observation per key (D-8)."""
        return (
            self.subject_kind,
            self.subject_id,
            self.chunk_version_id,
            self.task_type,
        )


@dataclass(frozen=True, slots=True)
class DecisionPolicy:
    """Versioned score-to-label policy. Thresholds are configuration, not
    universal truth boundaries."""

    policy_version: str
    support_threshold: float
    refute_threshold: float
    tie_rule_version: str = "v1"

    def __post_init__(self) -> None:
        for name, value in (
            ("support_threshold", self.support_threshold),
            ("refute_threshold", self.refute_threshold),
        ):
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValidationError(
                    f"policy {self.policy_version}: {name}={value!r} "
                    "must be finite and within [0, 1]"
                )
        if self.tie_rule_version != "v1":
            raise ValidationError(
                f"policy {self.policy_version}: unknown tie rule "
                f"{self.tie_rule_version!r}; only 'v1' is frozen"
            )


@dataclass(frozen=True, slots=True)
class ClaimState:
    """Complete reference state for one claim.

    Counts are over distinct normalized text hashes (D-11). The contributing
    observation identifiers are recorded so tests and provenance compare
    complete state, not only enum labels.
    """

    claim_id: str
    support_count: int
    refute_count: int
    best_support_score: float | None
    best_refute_score: float | None
    supporting_observation_ids: tuple[str, ...]
    refuting_observation_ids: tuple[str, ...]
    status: ClaimStatus


@dataclass(frozen=True, slots=True)
class AnswerState:
    """Complete reference state for one answer over its required claims."""

    answer_version_id: str
    required_claim_count: int
    supported_count: int
    unsupported_count: int
    refuted_count: int
    conflicted_count: int
    status: AnswerStatus


@dataclass(frozen=True, slots=True)
class StatusDelta:
    """An externally visible state transition attributed to one event."""

    event_id: str
    object_type: str  # "claim" | "answer"
    object_id: str
    old_status: str
    new_status: str
    reason: str
