"""Typed records for the M5 controlled-evaluation boundary.

The four record families in this module are deliberately separate:

* source annotations describe what the external source says;
* controlled projections describe deterministic score-row provenance;
* exact states describe the GroundLoop SDR policy;
* frozen-model diagnostics describe optional model output.

Keeping these as different types makes it difficult for report code to turn a
retrospective source label into semantic-observation currency accidentally.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from groundloop.domain import SemanticObservation
from groundloop.errors import ValidationError
from groundloop.m5.domain import EvidenceGroupVersion


class WiceSplit(StrEnum):
    TRAIN = "train"
    DEV = "dev"
    TEST = "test"


class WiceRowKind(StrEnum):
    CLAIM = "claim"
    SUBCLAIM = "subclaim"


class SourceLabel(StrEnum):
    SUPPORTED = "supported"
    PARTIALLY_SUPPORTED = "partially_supported"
    NOT_SUPPORTED = "not_supported"


class RejectReason(StrEnum):
    BLANK_JSONL_ROW = "blank_jsonl_row"
    INVALID_JSON = "invalid_json"
    DUPLICATE_JSON_KEY = "duplicate_json_key"
    NON_OBJECT_ROW = "non_object_row"
    MALFORMED_PARENT_ROW = "malformed_parent_row"
    MALFORMED_SUBCLAIM_ROW = "malformed_subclaim_row"
    DUPLICATE_PARENT_ID = "duplicate_parent_id"
    DUPLICATE_SUBCLAIM_ID = "duplicate_subclaim_id"
    INVALID_SUBCLAIM_ID = "invalid_subclaim_id"
    MISSING_SAME_SPLIT_PARENT = "missing_same_split_parent"
    EVIDENCE_ARRAY_BYTE_MISMATCH = "evidence_array_byte_mismatch"
    INVALID_SUPPORTING_SENTENCES = "invalid_supporting_sentences"
    MALFORMED_MEMBERSHIP = "malformed_membership"
    EMPTY_SUPPORTING_SET = "empty_supporting_set"
    NONINTEGER_SENTENCE_INDEX = "noninteger_sentence_index"
    NEGATIVE_SENTENCE_INDEX = "negative_sentence_index"
    OUT_OF_RANGE_SENTENCE_INDEX = "out_of_range_sentence_index"
    DUPLICATE_SENTENCE_INDEX = "duplicate_sentence_index"
    EMPTY_NORMALIZED_SENTENCE = "empty_normalized_sentence"
    CONFLICTING_SOURCE_TEXT = "conflicting_source_text"
    EVIDENCE_UNIT_OVERLENGTH = "evidence_unit_overlength"


class PrimaryExclusionReason(StrEnum):
    PARENT_NOT_SUPPORTED = "parent_not_supported"
    NO_FINAL_SUBCLAIMS = "no_final_subclaims"
    TOO_MANY_FINAL_SUBCLAIMS = "too_many_final_subclaims"
    SUBCLAIM_ORDINAL_NOT_DENSE = "subclaim_ordinal_not_dense"
    INVALID_SUBCLAIM_MAPPING = "invalid_subclaim_mapping"
    SUBCLAIM_NOT_SUPPORTED = "subclaim_not_supported"
    DUPLICATE_REQUIREMENT_TEXT = "duplicate_requirement_text"
    UNREPRESENTABLE_POSITIVE_REQUIREMENT = "unrepresentable_positive_requirement"


def _require_nonempty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{name} must be a nonempty string")


def _require_sha256(name: str, value: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _require_nonnegative(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValidationError(f"{name} must be a nonnegative integer")


@dataclass(frozen=True, slots=True)
class SourceFileSpec:
    split: WiceSplit
    kind: WiceRowKind
    relative_path: str
    rows: int
    byte_count: int
    sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.split, WiceSplit):
            raise ValidationError("source file split must be a WiceSplit")
        if not isinstance(self.kind, WiceRowKind):
            raise ValidationError("source file kind must be a WiceRowKind")
        _require_nonempty("source relative_path", self.relative_path)
        _require_nonnegative("source rows", self.rows)
        _require_nonnegative("source byte_count", self.byte_count)
        _require_sha256("source sha256", self.sha256)


@dataclass(frozen=True, slots=True)
class SupplementaryFileSpec:
    role: str
    relative_path: str
    byte_count: int
    sha256: str

    def __post_init__(self) -> None:
        _require_nonempty("supplementary role", self.role)
        _require_nonempty("supplementary relative_path", self.relative_path)
        _require_nonnegative("supplementary byte_count", self.byte_count)
        _require_sha256("supplementary sha256", self.sha256)


@dataclass(frozen=True, slots=True)
class LicenseRecord:
    annotation_license: str
    underlying_text_terms: tuple[str, ...]
    source_url: str

    def __post_init__(self) -> None:
        _require_nonempty("annotation_license", self.annotation_license)
        _require_nonempty("license source_url", self.source_url)
        if not self.underlying_text_terms:
            raise ValidationError("underlying text terms must be recorded")
        for term in self.underlying_text_terms:
            _require_nonempty("underlying text term", term)


@dataclass(frozen=True, slots=True)
class WiceSourceManifest:
    schema: str
    dataset: str
    repository_url: str
    official_commit: str
    adapter_version: str
    source_files: tuple[SourceFileSpec, ...]
    supplementary_files: tuple[SupplementaryFileSpec, ...]
    license: LicenseRecord
    manifest_hash: str

    def __post_init__(self) -> None:
        for name, value in (
            ("manifest schema", self.schema),
            ("dataset", self.dataset),
            ("repository_url", self.repository_url),
            ("official_commit", self.official_commit),
            ("adapter_version", self.adapter_version),
        ):
            _require_nonempty(name, value)
        _require_sha256("manifest_hash", self.manifest_hash)
        expected_pairs = {(split, kind) for split in WiceSplit for kind in WiceRowKind}
        actual_pairs = {(item.split, item.kind) for item in self.source_files}
        if actual_pairs != expected_pairs or len(self.source_files) != len(
            expected_pairs
        ):
            raise ValidationError(
                "manifest must contain exactly one claim and subclaim file per split"
            )


@dataclass(frozen=True, slots=True)
class VerifiedSourceFile:
    relative_path: str
    byte_count: int
    row_count: int | None
    sha256: str


@dataclass(frozen=True, slots=True)
class AdapterReject:
    split: WiceSplit
    kind: WiceRowKind
    row_number: int
    reason: RejectReason
    parent_meta_id: str | None = None
    subclaim_meta_id: str | None = None
    evidence_set_ordinal: int | None = None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class PrimaryCohortExclusion:
    split: WiceSplit
    parent_meta_id: str
    reason: PrimaryExclusionReason
    subclaim_meta_id: str | None = None


@dataclass(frozen=True, slots=True)
class EvidenceUnit:
    evidence_unit_id: str
    text_hash: str
    content_json: str
    normalized_sentences: tuple[str, ...]
    rendered_text: str
    rendered_sha256: str
    rendered_character_count: int

    def __post_init__(self) -> None:
        _require_sha256("evidence_unit_id", self.evidence_unit_id)
        _require_sha256("text_hash", self.text_hash)
        _require_sha256("rendered_sha256", self.rendered_sha256)
        _require_nonempty("content_json", self.content_json)
        if not self.normalized_sentences:
            raise ValidationError("an evidence unit must contain sentences")
        if tuple(sorted(set(self.normalized_sentences))) != self.normalized_sentences:
            raise ValidationError(
                "evidence unit sentences must be sorted and content-distinct"
            )
        if self.rendered_text != "\n\n".join(self.normalized_sentences):
            raise ValidationError("rendered text does not match canonical sentences")
        if self.rendered_character_count != len(self.rendered_text):
            raise ValidationError("rendered character count is inconsistent")


@dataclass(frozen=True, slots=True)
class EvidenceUnitMember:
    evidence_unit_id: str
    source_document_id: str
    sentence_index: int
    normalized_sentence: str

    def __post_init__(self) -> None:
        _require_sha256("member evidence_unit_id", self.evidence_unit_id)
        _require_nonempty("source_document_id", self.source_document_id)
        _require_nonnegative("sentence_index", self.sentence_index)
        _require_nonempty("normalized_sentence", self.normalized_sentence)


@dataclass(frozen=True, slots=True)
class SourceClaim:
    claim_id: str
    split: WiceSplit
    parent_meta_id: str
    source_claim_text: str
    source_label: SourceLabel
    source_document_id: str
    group_family_id: str
    group_version_id: str


@dataclass(frozen=True, slots=True)
class SourceRequirement:
    requirement_version_id: str
    group_version_id: str
    split: WiceSplit
    parent_meta_id: str
    subclaim_meta_id: str
    ordinal: int
    source_requirement_text: str
    normalized_requirement_text: str
    requirement_text_hash: str
    source_label: SourceLabel


@dataclass(frozen=True, slots=True)
class SourceAnnotation:
    source_annotation_id: str
    split: WiceSplit
    parent_meta_id: str
    subclaim_meta_id: str
    evidence_set_ordinal: int
    evidence_unit_id: str
    text_hash: str
    source_label: SourceLabel

    def __post_init__(self) -> None:
        _require_sha256("source_annotation_id", self.source_annotation_id)
        _require_nonnegative("evidence_set_ordinal", self.evidence_set_ordinal)
        _require_sha256("source annotation evidence_unit_id", self.evidence_unit_id)
        _require_sha256("source annotation text_hash", self.text_hash)


@dataclass(frozen=True, slots=True)
class SourceSemanticState:
    claim_id: str
    split: WiceSplit
    parent_meta_id: str
    direct_source_support: bool
    requirement_source_conjunction: bool
    source_semantic_label: bool

    def __post_init__(self) -> None:
        if self.source_semantic_label != (
            self.direct_source_support or self.requirement_source_conjunction
        ):
            raise ValidationError("source semantic label does not match its disjuncts")


@dataclass(frozen=True, slots=True)
class ControlledObservationProjection:
    observation_id: str
    source_annotation_id: str
    projection_version: str
    split: WiceSplit
    manifest_hash: str
    projected_label: str
    event_id: str
    event_payload_hash: str
    observation: SemanticObservation

    def __post_init__(self) -> None:
        _require_sha256("projection observation_id", self.observation_id)
        _require_sha256("projection source_annotation_id", self.source_annotation_id)
        _require_nonempty("projection_version", self.projection_version)
        _require_sha256("projection manifest_hash", self.manifest_hash)
        if self.projected_label not in ("support", "neutral"):
            raise ValidationError(
                "controlled projected label must be support or neutral"
            )
        _require_sha256("projection event_id", self.event_id)
        _require_sha256("projection event_payload_hash", self.event_payload_hash)
        if self.observation.observation_id != self.observation_id:
            raise ValidationError("projection and observation identifiers disagree")


@dataclass(frozen=True, slots=True)
class ControlledGroupProjection:
    split: WiceSplit
    parent_meta_id: str
    manifest_hash: str
    group: EvidenceGroupVersion

    def __post_init__(self) -> None:
        _require_sha256("group projection manifest_hash", self.manifest_hash)


@dataclass(frozen=True, slots=True)
class ExactSystemState:
    claim_id: str
    group_version_id: str
    requirement_count: int
    distinct_content_count: int
    matching_size: int
    groundloop_sdr_complete: bool
    assignment_by_ordinal: tuple[str | None, ...]
    perfect_matching_count: int

    def __post_init__(self) -> None:
        for name, value in (
            ("requirement_count", self.requirement_count),
            ("distinct_content_count", self.distinct_content_count),
            ("matching_size", self.matching_size),
            ("perfect_matching_count", self.perfect_matching_count),
        ):
            _require_nonnegative(name, value)
        if len(self.assignment_by_ordinal) != self.requirement_count:
            raise ValidationError("assignment length must equal requirement count")
        if self.groundloop_sdr_complete != (
            self.matching_size == self.requirement_count
        ):
            raise ValidationError("SDR label must equal exact matching completeness")


@dataclass(frozen=True, slots=True)
class FrozenModelDiagnostic:
    """Optional model result that cannot serve as source or controlled currency."""

    claim_id: str
    requirement_version_id: str
    evidence_unit_id: str
    model_id: str
    model_version: str
    prompt_version: str
    input_hash: str
    support_score: float
    refute_score: float
    neutral_score: float


@dataclass(frozen=True, slots=True)
class SplitAudit:
    split: WiceSplit
    parent_rows: int
    subclaim_rows: int
    mapped_subclaim_rows: int
    source_supported_parents: int
    representable_primary_parents: int
    hall_complete_parents: int
    hall_failing_parents: int
    primary_overlength_annotations: int
    requirement_count_histogram: tuple[tuple[int, int], ...]
    distinct_content_count_histogram: tuple[tuple[int, int], ...]
    perfect_matching_count_histogram: tuple[tuple[int, int], ...]
    overlapping_parent_count: int
    alternative_assignment_parent_count: int


@dataclass(frozen=True, slots=True)
class WiceAuditReport:
    schema: str
    adapter_version: str
    source_revision: str
    manifest_hash: str
    verified_files: tuple[VerifiedSourceFile, ...]
    split_audits: tuple[SplitAudit, ...]
    rejection_counts: tuple[tuple[str, int], ...]
    primary_exclusion_counts: tuple[tuple[str, int], ...]
    direct_claim_projection_count: int
    model_call_count: int
    test_selection_performed: bool


@dataclass(frozen=True, slots=True)
class WiceAdapterResult:
    manifest: WiceSourceManifest
    source_claims: tuple[SourceClaim, ...]
    source_requirements: tuple[SourceRequirement, ...]
    evidence_units: tuple[EvidenceUnit, ...]
    evidence_unit_members: tuple[EvidenceUnitMember, ...]
    source_annotations: tuple[SourceAnnotation, ...]
    source_semantic_states: tuple[SourceSemanticState, ...]
    controlled_groups: tuple[ControlledGroupProjection, ...]
    controlled_projections: tuple[ControlledObservationProjection, ...]
    exact_system_states: tuple[ExactSystemState, ...]
    frozen_model_diagnostics: tuple[FrozenModelDiagnostic, ...]
    rejects: tuple[AdapterReject, ...]
    primary_exclusions: tuple[PrimaryCohortExclusion, ...]
    audit: WiceAuditReport


__all__ = [
    "AdapterReject",
    "ControlledGroupProjection",
    "ControlledObservationProjection",
    "EvidenceUnit",
    "EvidenceUnitMember",
    "ExactSystemState",
    "FrozenModelDiagnostic",
    "LicenseRecord",
    "PrimaryCohortExclusion",
    "PrimaryExclusionReason",
    "RejectReason",
    "SourceAnnotation",
    "SourceClaim",
    "SourceFileSpec",
    "SourceLabel",
    "SourceRequirement",
    "SourceSemanticState",
    "SplitAudit",
    "SupplementaryFileSpec",
    "VerifiedSourceFile",
    "WiceAdapterResult",
    "WiceAuditReport",
    "WiceRowKind",
    "WiceSourceManifest",
    "WiceSplit",
]
