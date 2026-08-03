"""Immutable M5 domain records for bounded evidence-group semantics."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from groundloop.domain import (
    AnswerStatus,
    ClaimStatus,
    ObservationKey,
    SemanticObservation,
    SubjectKind,
)
from groundloop.errors import ValidationError
from groundloop.m5.digests import (
    enum_field,
    hash_field,
    int_field,
    normalize_text_v1,
    normalized_text_hash_v1,
    option_field,
    sequence_field,
    stable_m5_digest,
    text_field,
)


def _require_identifier(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a nonempty identifier")


def _require_nonnegative_integer(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValidationError(f"{name} must be a nonnegative integer")


def _validate_model_triple(
    *,
    model_id: str | None,
    model_version: str | None,
    prompt_version: str | None,
    context: str,
) -> None:
    values = (model_id, model_version, prompt_version)
    present = tuple(value is not None for value in values)
    if any(present) and not all(present):
        raise ValidationError(f"{context}: constructor model triple is partial")
    for value in values:
        if value is not None:
            _require_identifier(f"{context} constructor field", value)


class EvidenceGroupType(StrEnum):
    SUPPORT_CONJUNCTION = "support_conjunction"


class ConstructionKind(StrEnum):
    GOLD = "gold"
    CONTROLLED = "controlled"
    MODEL_PROPOSED = "model_proposed"


class ClaimSupportKind(StrEnum):
    NONE = "none"
    DIRECT = "direct"
    GROUP = "group"


@dataclass(frozen=True, slots=True, order=True)
class SnapshotPoint:
    epoch_id: int
    revision: int

    def __post_init__(self) -> None:
        _require_nonnegative_integer("snapshot epoch", self.epoch_id)
        _require_nonnegative_integer("snapshot revision", self.revision)


@dataclass(frozen=True, slots=True)
class EvidenceGroupFamily:
    group_family_id: str
    claim_id: str
    created_epoch: int

    def __post_init__(self) -> None:
        _require_identifier("group_family_id", self.group_family_id)
        _require_identifier("claim_id", self.claim_id)
        _require_nonnegative_integer("family created_epoch", self.created_epoch)


@dataclass(frozen=True, slots=True)
class EvidenceRequirementVersion:
    requirement_version_id: str
    group_version_id: str
    ordinal: int
    requirement_text: str
    requirement_text_hash: str = field(default="")
    constructor_model_id: str | None = None
    constructor_model_version: str | None = None
    constructor_prompt_version: str | None = None
    supersedes_requirement_version_id: str | None = None

    def __post_init__(self) -> None:
        _require_identifier("requirement_version_id", self.requirement_version_id)
        _require_identifier("group_version_id", self.group_version_id)
        _require_nonnegative_integer("requirement ordinal", self.ordinal)
        normalized = normalize_text_v1(self.requirement_text)
        if not normalized:
            raise ValidationError("normalized requirement text must be nonempty")
        object.__setattr__(self, "requirement_text", normalized)
        expected_hash = normalized_text_hash_v1(normalized)
        if not self.requirement_text_hash:
            object.__setattr__(self, "requirement_text_hash", expected_hash)
        elif self.requirement_text_hash != expected_hash:
            raise ValidationError(
                "requirement_text_hash does not match normalized text"
            )
        _validate_model_triple(
            model_id=self.constructor_model_id,
            model_version=self.constructor_model_version,
            prompt_version=self.constructor_prompt_version,
            context=f"requirement {self.requirement_version_id}",
        )
        predecessor = self.supersedes_requirement_version_id
        if predecessor is not None:
            _require_identifier("supersedes_requirement_version_id", predecessor)
            if predecessor == self.requirement_version_id:
                raise ValidationError("a requirement cannot supersede itself")


def semantic_structure_digest(
    group_type: EvidenceGroupType,
    requirements: tuple[EvidenceRequirementVersion, ...],
) -> str:
    hashes = tuple(
        sorted(requirement.requirement_text_hash for requirement in requirements)
    )
    return stable_m5_digest(
        "m5-semantic-structure-v1",
        enum_field(group_type),
        sequence_field(hash_field(value) for value in hashes),
    )


@dataclass(frozen=True, slots=True)
class EvidenceGroupVersion:
    group_version_id: str
    group_family_id: str
    owner_claim_id: str
    requirements: tuple[EvidenceRequirementVersion, ...]
    group_type: EvidenceGroupType = EvidenceGroupType.SUPPORT_CONJUNCTION
    construction_kind: ConstructionKind = ConstructionKind.CONTROLLED
    construction_source_id: str = ""
    constructor_model_id: str | None = None
    constructor_model_version: str | None = None
    constructor_prompt_version: str | None = None
    supersedes_group_version_id: str | None = None
    semantic_structure_hash: str = ""
    record_payload_hash: str = ""

    def __post_init__(self) -> None:
        _require_identifier("group_version_id", self.group_version_id)
        _require_identifier("group_family_id", self.group_family_id)
        _require_identifier("owner_claim_id", self.owner_claim_id)
        _require_identifier("construction_source_id", self.construction_source_id)
        if not isinstance(self.group_type, EvidenceGroupType):
            raise ValidationError("group_type must be an EvidenceGroupType")
        if not isinstance(self.construction_kind, ConstructionKind):
            raise ValidationError("construction_kind must be a ConstructionKind")
        if not isinstance(self.requirements, tuple):
            raise ValidationError("group requirements must be an immutable tuple")
        if not 1 <= len(self.requirements) <= 8:
            raise ValidationError(
                "an evidence group must have one to eight requirements"
            )
        expected_ordinals = tuple(range(len(self.requirements)))
        actual_ordinals = tuple(
            requirement.ordinal for requirement in self.requirements
        )
        if actual_ordinals != expected_ordinals:
            raise ValidationError(
                "requirement ordinals must be dense and ordered from zero"
            )
        requirement_ids = tuple(
            requirement.requirement_version_id for requirement in self.requirements
        )
        if len(set(requirement_ids)) != len(requirement_ids):
            raise ValidationError("requirement version identifiers must be unique")
        text_hashes = tuple(
            requirement.requirement_text_hash for requirement in self.requirements
        )
        if len(set(text_hashes)) != len(text_hashes):
            raise ValidationError("normalized requirement texts must be unique")
        for requirement in self.requirements:
            if requirement.group_version_id != self.group_version_id:
                raise ValidationError(
                    "every requirement must reference its containing group version"
                )

        _validate_model_triple(
            model_id=self.constructor_model_id,
            model_version=self.constructor_model_version,
            prompt_version=self.constructor_prompt_version,
            context=f"group {self.group_version_id}",
        )
        model_triple = (
            self.constructor_model_id,
            self.constructor_model_version,
            self.constructor_prompt_version,
        )
        if self.construction_kind in (
            ConstructionKind.GOLD,
            ConstructionKind.CONTROLLED,
        ):
            if any(value is not None for value in model_triple):
                raise ValidationError(
                    "gold/controlled groups cannot name a constructor model"
                )
        elif not all(value is not None for value in model_triple):
            raise ValidationError(
                "model_proposed groups require a complete model triple"
            )
        for requirement in self.requirements:
            if (
                requirement.constructor_model_id,
                requirement.constructor_model_version,
                requirement.constructor_prompt_version,
            ) != model_triple:
                raise ValidationError(
                    "requirement constructor provenance must equal its parent group"
                )

        predecessor = self.supersedes_group_version_id
        if predecessor is not None:
            _require_identifier("supersedes_group_version_id", predecessor)
            if predecessor == self.group_version_id:
                raise ValidationError("a group version cannot supersede itself")

        expected_structure = semantic_structure_digest(
            self.group_type, self.requirements
        )
        if not self.semantic_structure_hash:
            object.__setattr__(self, "semantic_structure_hash", expected_structure)
        elif self.semantic_structure_hash != expected_structure:
            raise ValidationError("semantic_structure_hash does not match requirements")

        expected_record = group_record_digest(self)
        if not self.record_payload_hash:
            object.__setattr__(self, "record_payload_hash", expected_record)
        elif self.record_payload_hash != expected_record:
            raise ValidationError("record_payload_hash does not match group record")


def group_record_digest(group: EvidenceGroupVersion) -> str:
    requirement_fields = []
    for requirement in group.requirements:
        requirement_fields.append(
            sequence_field(
                (
                    text_field(requirement.requirement_version_id),
                    int_field(requirement.ordinal),
                    text_field(requirement.requirement_text),
                    hash_field(requirement.requirement_text_hash),
                    option_field(
                        text_field(requirement.constructor_model_id)
                        if requirement.constructor_model_id is not None
                        else None
                    ),
                    option_field(
                        text_field(requirement.constructor_model_version)
                        if requirement.constructor_model_version is not None
                        else None
                    ),
                    option_field(
                        text_field(requirement.constructor_prompt_version)
                        if requirement.constructor_prompt_version is not None
                        else None
                    ),
                    option_field(
                        text_field(requirement.supersedes_requirement_version_id)
                        if requirement.supersedes_requirement_version_id is not None
                        else None
                    ),
                )
            )
        )
    return stable_m5_digest(
        "m5-group-record-v1",
        text_field(group.owner_claim_id),
        text_field(group.group_version_id),
        text_field(group.group_family_id),
        enum_field(group.group_type),
        enum_field(group.construction_kind),
        text_field(group.construction_source_id),
        option_field(
            text_field(group.constructor_model_id)
            if group.constructor_model_id is not None
            else None
        ),
        option_field(
            text_field(group.constructor_model_version)
            if group.constructor_model_version is not None
            else None
        ),
        option_field(
            text_field(group.constructor_prompt_version)
            if group.constructor_prompt_version is not None
            else None
        ),
        option_field(
            text_field(group.supersedes_group_version_id)
            if group.supersedes_group_version_id is not None
            else None
        ),
        hash_field(group.semantic_structure_hash),
        sequence_field(requirement_fields),
    )


@dataclass(frozen=True, slots=True)
class EvidenceGroupValidity:
    group_version_id: str
    valid_from_epoch: int
    valid_to_epoch: int | None = None

    def __post_init__(self) -> None:
        _require_identifier("group_version_id", self.group_version_id)
        _require_nonnegative_integer("valid_from_epoch", self.valid_from_epoch)
        if self.valid_to_epoch is not None:
            _require_nonnegative_integer("valid_to_epoch", self.valid_to_epoch)
        if (
            self.valid_to_epoch is not None
            and self.valid_to_epoch <= self.valid_from_epoch
        ):
            raise ValidationError(
                "group validity must be a nonempty half-open interval"
            )

    def contains(self, epoch_id: int) -> bool:
        return self.valid_from_epoch <= epoch_id and (
            self.valid_to_epoch is None or epoch_id < self.valid_to_epoch
        )


@dataclass(frozen=True, slots=True)
class EvidenceGroupFamilyRetirement:
    group_family_id: str
    retired_epoch_id: int
    event_id: str

    def __post_init__(self) -> None:
        _require_identifier("group_family_id", self.group_family_id)
        _require_identifier("event_id", self.event_id)
        _require_nonnegative_integer("retired_epoch_id", self.retired_epoch_id)


@dataclass(frozen=True, slots=True)
class RequirementObservationRecord:
    observation: SemanticObservation
    eligible_for_currency: bool
    produced_at: SnapshotPoint

    def __post_init__(self) -> None:
        if not isinstance(self.produced_at, SnapshotPoint):
            raise ValidationError("produced_at must be a SnapshotPoint")
        if self.observation.subject_kind is not SubjectKind.REQUIREMENT:
            raise ValidationError(
                "M5 requirement records require a REQUIREMENT subject"
            )


@dataclass(frozen=True, slots=True)
class ObservationCurrencyInterval:
    """Reference-only interval over the total committed ``SnapshotPoint`` order.

    This is behaviorally equivalent for M5.1's monotone successful points to
    epoch-local revision rows plus published-epoch fallback. Migration 014
    deliberately implements that exact SQL representation instead of copying
    this compact in-memory shape.
    """

    key: ObservationKey
    observation_id: str | None
    valid_from: SnapshotPoint
    valid_to: SnapshotPoint | None = None

    def __post_init__(self) -> None:
        if self.key[0] is not SubjectKind.REQUIREMENT:
            raise ValidationError("M5 currency history requires a REQUIREMENT key")
        if self.observation_id is not None:
            _require_identifier("observation_id", self.observation_id)
        if self.valid_to is not None and self.valid_to <= self.valid_from:
            raise ValidationError("currency validity must be a nonempty interval")

    def contains(self, point: SnapshotPoint) -> bool:
        return self.valid_from <= point and (
            self.valid_to is None or point < self.valid_to
        )


@dataclass(frozen=True, slots=True)
class RequirementWitness:
    """One derived requirement-to-distinct-content edge and its provenance."""

    requirement_version_id: str
    requirement_ordinal: int
    text_hash: str
    active_observation_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_identifier("requirement_version_id", self.requirement_version_id)
        _require_nonnegative_integer("requirement ordinal", self.requirement_ordinal)
        hash_field(self.text_hash)
        if not isinstance(self.active_observation_ids, tuple):
            raise ValidationError("active observation IDs must be an immutable tuple")
        if not self.active_observation_ids:
            raise ValidationError("a derived witness must have active observations")
        if (
            tuple(sorted(set(self.active_observation_ids)))
            != self.active_observation_ids
        ):
            raise ValidationError(
                "active witness observation IDs must be unique sorted identifiers"
            )
        for observation_id in self.active_observation_ids:
            _require_identifier("active observation ID", observation_id)


@dataclass(frozen=True, slots=True)
class RequirementState:
    requirement_version_id: str
    witness_hashes: tuple[str, ...]
    supporting_observation_ids: tuple[str, ...]
    witness_count: int
    satisfied: bool


@dataclass(frozen=True, slots=True)
class GroupState:
    group_version_id: str
    requirement_count: int
    satisfied_count: int
    matching_size: int
    complete: bool


@dataclass(frozen=True, slots=True)
class CombinedClaimState:
    claim_id: str
    support_count: int
    refute_count: int
    best_support_score: float | None
    best_refute_score: float | None
    supporting_observation_ids: tuple[str, ...]
    refuting_observation_ids: tuple[str, ...]
    complete_group_count: int
    complete_group_ids: tuple[str, ...]
    status: ClaimStatus


@dataclass(frozen=True, slots=True)
class CombinedAnswerState:
    answer_version_id: str
    required_claim_count: int
    supported_count: int
    unsupported_count: int
    refuted_count: int
    conflicted_count: int
    status: AnswerStatus


@dataclass(frozen=True, slots=True)
class GroupCertificateRow:
    requirement_ordinal: int
    requirement_version_id: str
    text_hash: str
    selected_observation_id: str

    def __post_init__(self) -> None:
        _require_nonnegative_integer(
            "certificate requirement ordinal", self.requirement_ordinal
        )
        _require_identifier("requirement_version_id", self.requirement_version_id)
        hash_field(self.text_hash)
        _require_identifier("selected_observation_id", self.selected_observation_id)


@dataclass(frozen=True, slots=True)
class GroupMatchingCertificateArtifact:
    decision_policy_version: str
    group_version_id: str
    rows: tuple[GroupCertificateRow, ...]
    certificate_version: str = "m5-group-certificate-v1"
    certificate_digest: str = ""

    def __post_init__(self) -> None:
        _require_identifier("decision_policy_version", self.decision_policy_version)
        _require_identifier("group_version_id", self.group_version_id)
        if not isinstance(self.rows, tuple):
            raise ValidationError("group certificate rows must be an immutable tuple")
        if self.certificate_version != "m5-group-certificate-v1":
            raise ValidationError("unsupported M5 group certificate version")
        if not 1 <= len(self.rows) <= 8:
            raise ValidationError("group certificates require one to eight rows")
        ordinals = tuple(row.requirement_ordinal for row in self.rows)
        if ordinals != tuple(range(len(self.rows))):
            raise ValidationError("group certificate rows must be dense ordinal order")
        if len({row.text_hash for row in self.rows}) != len(self.rows):
            raise ValidationError("group certificate text hashes must be unique")
        if len({row.requirement_version_id for row in self.rows}) != len(self.rows):
            raise ValidationError("group certificate requirement IDs must be unique")
        expected = stable_m5_digest(
            "m5-group-certificate-v1",
            text_field(self.decision_policy_version),
            text_field(self.group_version_id),
            int_field(len(self.rows)),
            sequence_field(
                sequence_field(
                    (
                        int_field(row.requirement_ordinal),
                        text_field(row.requirement_version_id),
                        hash_field(row.text_hash),
                        text_field(row.selected_observation_id),
                    )
                )
                for row in self.rows
            ),
        )
        if not self.certificate_digest:
            object.__setattr__(self, "certificate_digest", expected)
        elif self.certificate_digest != expected:
            raise ValidationError("group certificate digest does not match its rows")

    @property
    def requirement_count(self) -> int:
        return len(self.rows)


@dataclass(frozen=True, slots=True)
class ClaimCertificateArtifact:
    claim_id: str
    decision_policy_version: str
    support_kind: ClaimSupportKind
    direct_support_observation_id: str | None = None
    group_version_id: str | None = None
    group_certificate_digest: str | None = None
    direct_refute_observation_id: str | None = None
    certificate_version: str = "m5-claim-certificate-v2"
    certificate_digest: str = ""

    def __post_init__(self) -> None:
        _require_identifier("claim_id", self.claim_id)
        _require_identifier("decision_policy_version", self.decision_policy_version)
        if not isinstance(self.support_kind, ClaimSupportKind):
            raise ValidationError("support_kind must be a ClaimSupportKind")
        if self.certificate_version != "m5-claim-certificate-v2":
            raise ValidationError("unsupported M5 claim certificate version")
        direct = self.direct_support_observation_id
        group_id = self.group_version_id
        group_digest = self.group_certificate_digest
        if self.support_kind is ClaimSupportKind.NONE:
            if direct is not None or group_id is not None or group_digest is not None:
                raise ValidationError("NONE support certificate fields must be NULL")
        elif self.support_kind is ClaimSupportKind.DIRECT:
            if direct is None or group_id is not None or group_digest is not None:
                raise ValidationError("DIRECT certificate has an invalid field shape")
        elif direct is not None or group_id is None or group_digest is None:
            raise ValidationError("GROUP certificate has an invalid field shape")
        for name, value in (
            ("direct_support_observation_id", direct),
            ("group_version_id", group_id),
            ("direct_refute_observation_id", self.direct_refute_observation_id),
        ):
            if value is not None:
                _require_identifier(name, value)
        if group_digest is not None:
            hash_field(group_digest)
        expected = stable_m5_digest(
            "m5-claim-certificate-v2",
            text_field(self.claim_id),
            text_field(self.decision_policy_version),
            enum_field(self.support_kind),
            option_field(text_field(direct) if direct is not None else None),
            option_field(text_field(group_id) if group_id is not None else None),
            option_field(
                hash_field(group_digest) if group_digest is not None else None
            ),
            option_field(
                text_field(self.direct_refute_observation_id)
                if self.direct_refute_observation_id is not None
                else None
            ),
        )
        if not self.certificate_digest:
            object.__setattr__(self, "certificate_digest", expected)
        elif self.certificate_digest != expected:
            raise ValidationError("claim certificate digest does not match its fields")


__all__ = [
    "ClaimCertificateArtifact",
    "ClaimSupportKind",
    "CombinedAnswerState",
    "CombinedClaimState",
    "ConstructionKind",
    "EvidenceGroupFamily",
    "EvidenceGroupFamilyRetirement",
    "EvidenceGroupType",
    "EvidenceGroupValidity",
    "EvidenceGroupVersion",
    "EvidenceRequirementVersion",
    "GroupCertificateRow",
    "GroupMatchingCertificateArtifact",
    "GroupState",
    "ObservationCurrencyInterval",
    "RequirementObservationRecord",
    "RequirementWitness",
    "RequirementState",
    "SnapshotPoint",
    "group_record_digest",
    "semantic_structure_digest",
]
