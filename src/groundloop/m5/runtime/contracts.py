"""Immutable byte-total contracts for the GroundLoop M5 typed runtime.

The records in this module validate semantic shape and immutable identities.
They do not perform retrieval, model inference, persistence, or transaction
coordination.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from groundloop.domain import (
    DecisionPolicy,
    ModelStamp,
    SemanticObservation,
    StatusDelta,
    SubjectKind,
    VerificationLabel,
)
from groundloop.errors import ValidationError
from groundloop.events import (
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
)
from groundloop.events import (
    Event as LegacyEvent,
)
from groundloop.m4.application import (
    DiscoveryResult as M4DiscoveryResult,
)
from groundloop.m4.application import (
    DynamicEventPlan,
    OpenEventReceipt,
    PublicationReceipt,
)
from groundloop.m4.contracts import (
    AdmittedPair as M4AdmittedPair,
)
from groundloop.m4.contracts import (
    ChannelHit as M4ChannelHit,
)
from groundloop.m4.contracts import (
    DiscoveryScope as M4DiscoveryScope,
)
from groundloop.m4.contracts import (
    JobAttempt as M4JobAttempt,
)
from groundloop.m4.contracts import (
    JobCompletion as M4JobCompletion,
)
from groundloop.m4.contracts import (
    JobKind as M4JobKind,
)
from groundloop.m4.contracts import (
    JobState as M4JobState,
)
from groundloop.m4.contracts import (
    LogicalJobSpec as M4LogicalJobSpec,
)
from groundloop.m4.contracts import (
    VectorIndexKind,
    stable_m4_digest,
)
from groundloop.m5.digests import (
    normalize_text_v1,
    normalized_text_hash_v1,
    whitespace_codepoints_v1,
)
from groundloop.m5.events import (
    M5Event,
    ObserveRequirementEvent,
    RegisterGroupEvent,
    ReplaceGroupEvent,
    RetireGroupEvent,
    legacy_event_payload_digest,
    m5_event_payload_digest,
)
from groundloop.m5.runtime import digests
from groundloop.policy import decide

_HEX = frozenset("0123456789abcdef")
M5_REQUIREMENT_TASK = "verify_requirement_v1"
M5_NORMALIZER_PROVENANCE_HASH = (
    "d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb"
)


def _require_text(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a nonempty string")


def _require_hash(name: str, value: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in _HEX for character in value)
    ):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _require_int(name: str, value: int, *, positive: bool = False) -> None:
    minimum = 1 if positive else 0
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        qualifier = "positive" if positive else "nonnegative"
        raise ValidationError(f"{name} must be a {qualifier} integer")


def _require_bool(name: str, value: bool) -> None:
    if not isinstance(value, bool):
        raise ValidationError(f"{name} must be a boolean")


def _require_timestamptz(name: str, value: datetime) -> None:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValidationError(f"{name} must be a timezone-aware datetime")


def _require_finite_number(name: str, value: float) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (float, int))
        or not math.isfinite(float(value))
    ):
        raise ValidationError(f"{name} must be a finite number")


def _require_tuple(name: str, value: object) -> None:
    if not isinstance(value, tuple):
        raise ValidationError(f"{name} must be an immutable tuple")


def _require_sorted_unique_text(name: str, values: tuple[str, ...]) -> None:
    _require_tuple(name, values)
    for value in values:
        _require_text(f"{name} member", value)
    if values != tuple(sorted(set(values))):
        raise ValidationError(f"{name} must be sorted and unique")


def _require_identity(name: str, supplied: str, expected: str) -> None:
    _require_hash(name, supplied)
    if supplied != expected:
        raise ValidationError(f"{name} does not match the frozen digest recipe")


def _decision_policy_hash(policy: DecisionPolicy) -> str:
    """Preserve the frozen M4 policy identity without importing model adapters."""

    return stable_m4_digest(
        "m4-decision-policy-v1",
        policy.policy_version,
        format(policy.support_threshold, ".17g"),
        format(policy.refute_threshold, ".17g"),
        policy.tie_rule_version,
    )


class M5DiscoveryDirection(StrEnum):
    FORWARD_REQUIREMENT = "forward_requirement"
    REVERSE_CHUNK = "reverse_chunk"


class M5RequirementAdmissionChannel(StrEnum):
    LEXICAL = "lexical"
    LINEAGE = "lineage"
    VECTOR = "vector"


class M5RetrievalTermination(StrEnum):
    BUDGET_FILLED = "budget_filled"
    SNAPSHOT_EXHAUSTED = "snapshot_exhausted"


class M5JobKind(StrEnum):
    FORWARD_REQUIREMENT_RETRIEVAL = "forward_requirement_retrieval"
    REVERSE_REQUIREMENT_DISCOVERY = "reverse_requirement_discovery"
    VERIFY_REQUIREMENT_PAIR = "verify_requirement_pair"


class M5JobState(StrEnum):
    DECLARED = "declared"
    RUNNING = "running"
    COMPLETED_ACTIVE = "completed_active"
    COMPLETED_INACTIVE = "completed_inactive"
    RETRYABLE_FAILED = "retryable_failed"
    TERMINAL_FAILED = "terminal_failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in {
            M5JobState.COMPLETED_ACTIVE,
            M5JobState.COMPLETED_INACTIVE,
            M5JobState.TERMINAL_FAILED,
            M5JobState.CANCELLED,
        }


class M5ScopeState(StrEnum):
    OPEN = "open"
    RESULT_STAGED = "result_staged"
    CLOSED_ACTIVE = "closed_active"
    CLOSED_INACTIVE = "closed_inactive"
    TERMINAL_FAILED = "terminal_failed"
    CANCELLED = "cancelled"


class M5TerminalReason(StrEnum):
    CHUNK_INACTIVE = "chunk_inactive"
    SUBJECT_INACTIVE = "subject_inactive"
    EPOCH_FAILED = "epoch_failed"
    SCOPE_RETIRED = "scope_retired"
    RETRY_EXHAUSTED = "retry_exhausted"
    RETRIEVAL_ERROR = "retrieval_error"
    VERIFIER_ERROR = "verifier_error"
    INVALID_ARTIFACT = "invalid_artifact"


class M5AttemptDisposition(StrEnum):
    ROOT_RESULT_STAGED = "root_result_staged"
    VERIFIER_COMPLETED_ACTIVE = "verifier_completed_active"
    VERIFIER_COMPLETED_INACTIVE = "verifier_completed_inactive"
    TERMINAL_AUDIT_ONLY = "terminal_audit_only"


class M5AttemptArchiveReason(StrEnum):
    ATTEMPT_EXPIRED = "attempt_expired"
    EPOCH_FAILED = "epoch_failed"
    SUBJECT_INACTIVE = "subject_inactive"
    CHUNK_INACTIVE = "chunk_inactive"
    JOB_ALREADY_TERMINAL = "job_already_terminal"


class M5AcquisitionDisposition(StrEnum):
    DISPATCH_NEW = "dispatch_new"
    DISPATCH_TAKEOVER = "dispatch_takeover"
    LIVE_LEASE = "live_lease"
    RESULT_RESERVED = "result_reserved"
    TERMINAL = "terminal"


class M5RuntimeSubgraph(StrEnum):
    DIRECT = "direct"
    REQUIREMENT = "requirement"


class M5ExecutionEvidenceDisposition(StrEnum):
    RETURNED = "returned"
    REUSED_ARTIFACT = "reused_artifact"
    RETRYABLE_FAILURE = "retryable_failure"
    TERMINAL_FAILURE = "terminal_failure"


class M5RuntimeWorkContributionKind(StrEnum):
    STRUCTURAL_OPEN = "structural_open"
    M5_ACQUISITION = "m5_acquisition"
    DIRECT_ACQUISITION = "direct_acquisition"
    M5_ATTEMPT_EXECUTION = "m5_attempt_execution"
    DIRECT_ATTEMPT_EXECUTION = "direct_attempt_execution"
    ROOT_RESULT_STAGE = "root_result_stage"
    ROOT_BARRIER = "root_barrier"
    VERIFIER_COMPLETION = "verifier_completion"
    CANCELLATION = "cancellation"
    TERMINAL_JOB_FAILURE = "terminal_job_failure"
    DIRECT_TRANSITION = "direct_transition"
    PRETERMINAL_LATE_RETURN = "preterminal_late_return"
    EPOCH_FAILURE = "epoch_failure"
    SEAL = "seal"


class M5TypedDirectReturnKind(StrEnum):
    DISCOVERY = "discovery"
    VERIFIER = "verifier"


class M5TypedDirectScopeKind(StrEnum):
    ALL_REGISTERED_CLAIMS = "all_registered_claims"
    EXPLICIT_CLAIMS = "explicit_claims"


class M5RunState(StrEnum):
    SEALED = "sealed"
    FAILED = "failed"
    BLOCKED = "blocked"
    REPLAYED = "replayed"


class M5ReplayedOutcome(StrEnum):
    SEALED = "sealed"
    FAILED = "failed"


class M5RunFailureReason(StrEnum):
    WORK_IN_PROGRESS = "work_in_progress"
    RETRIEVAL_UNAVAILABLE = "retrieval_unavailable"
    VERIFIER_UNAVAILABLE = "verifier_unavailable"
    RETRY_EXHAUSTED = "retry_exhausted"
    RETRIEVAL_ERROR = "retrieval_error"
    VERIFIER_ERROR = "verifier_error"
    INVALID_ARTIFACT = "invalid_artifact"
    INVARIANT_FAILURE = "invariant_failure"


class M5StateReferenceKind(StrEnum):
    REQUIREMENT_STATE = "requirement_state"
    GROUP_STATE = "group_state"
    GROUP_CERTIFICATE = "group_certificate"
    CLAIM_STATE = "claim_state"
    CLAIM_CERTIFICATE = "claim_certificate"
    ANSWER_STATE = "answer_state"


@dataclass(frozen=True, slots=True)
class M5TextNormalizerProvenance:
    normalizer_id: str = "m5-normalize-text-v1"
    whitespace_codepoints: tuple[int, ...] = whitespace_codepoints_v1()
    boundary_rule: str = "remove-boundary-runs"
    internal_rule: str = "collapse-internal-runs-to-u+0020"
    other_codepoint_rule: str = "preserve-exactly"
    unicode_normalization_rule: str = "none"
    encoding: str = "utf-8"
    hash_algorithm: str = "sha256"

    def __post_init__(self) -> None:
        if (
            self.normalizer_id != "m5-normalize-text-v1"
            or self.whitespace_codepoints != whitespace_codepoints_v1()
            or self.boundary_rule != "remove-boundary-runs"
            or self.internal_rule != "collapse-internal-runs-to-u+0020"
            or self.other_codepoint_rule != "preserve-exactly"
            or self.unicode_normalization_rule != "none"
            or self.encoding != "utf-8"
            or self.hash_algorithm != "sha256"
        ):
            raise ValidationError("M5 normalizer provenance is a frozen singleton")
        if self.normalizer_provenance_hash != M5_NORMALIZER_PROVENANCE_HASH:
            raise ValidationError("M5 normalizer provenance golden hash drift")

    @property
    def normalizer_provenance_hash(self) -> str:
        return digests.text_normalizer_provenance_digest(
            normalizer_id=self.normalizer_id,
            whitespace_codepoints=self.whitespace_codepoints,
            boundary_rule=self.boundary_rule,
            internal_rule=self.internal_rule,
            other_codepoint_rule=self.other_codepoint_rule,
            unicode_normalization_rule=self.unicode_normalization_rule,
            encoding=self.encoding,
            hash_algorithm=self.hash_algorithm,
        )


@dataclass(frozen=True, slots=True)
class M5CandidatePolicyManifest:
    candidate_policy_id: str
    embedding_model_artifact_id: str
    requirement_role_template_hash: str
    chunk_role_template_hash: str
    vector_method_version: str
    vector_index_kind: VectorIndexKind
    vector_index_build_config_hash: str
    vector_search_config_hash: str
    lexical_method_version: str
    lexical_config_hash: str
    lexical_postgres_version: str
    lexical_regconfig_identity: str
    fusion_version: str
    reverse_budget_per_inserted_chunk: int
    forward_budget_per_requirement: int
    verifier_execution_spec_hash: str
    decision_policy_version: str
    lineage_safety_override: bool

    def __post_init__(self) -> None:
        for name in (
            "embedding_model_artifact_id",
            "vector_method_version",
            "lexical_method_version",
            "lexical_postgres_version",
            "lexical_regconfig_identity",
            "fusion_version",
            "decision_policy_version",
        ):
            _require_text(name, getattr(self, name))
        for name in (
            "requirement_role_template_hash",
            "chunk_role_template_hash",
            "vector_index_build_config_hash",
            "vector_search_config_hash",
            "lexical_config_hash",
            "verifier_execution_spec_hash",
        ):
            _require_hash(name, getattr(self, name))
        if not isinstance(self.vector_index_kind, VectorIndexKind):
            raise ValidationError("vector_index_kind must be a VectorIndexKind")
        if self.fusion_version != "rank-interleave-v1":
            raise ValidationError("fusion_version must be rank-interleave-v1")
        _require_int(
            "reverse_budget_per_inserted_chunk",
            self.reverse_budget_per_inserted_chunk,
            positive=True,
        )
        _require_int(
            "forward_budget_per_requirement",
            self.forward_budget_per_requirement,
            positive=True,
        )
        _require_bool("lineage_safety_override", self.lineage_safety_override)
        _require_identity(
            "candidate_policy_id", self.candidate_policy_id, self.manifest_hash
        )

    @property
    def manifest_hash(self) -> str:
        return digests.candidate_policy_manifest_digest(
            embedding_model_artifact_id=self.embedding_model_artifact_id,
            requirement_role_template_hash=self.requirement_role_template_hash,
            chunk_role_template_hash=self.chunk_role_template_hash,
            vector_method_version=self.vector_method_version,
            vector_index_kind=self.vector_index_kind,
            vector_index_build_config_hash=self.vector_index_build_config_hash,
            vector_search_config_hash=self.vector_search_config_hash,
            lexical_method_version=self.lexical_method_version,
            lexical_config_hash=self.lexical_config_hash,
            lexical_postgres_version=self.lexical_postgres_version,
            lexical_regconfig_identity=self.lexical_regconfig_identity,
            fusion_version=self.fusion_version,
            reverse_budget_per_inserted_chunk=self.reverse_budget_per_inserted_chunk,
            forward_budget_per_requirement=self.forward_budget_per_requirement,
            verifier_execution_spec_hash=self.verifier_execution_spec_hash,
            decision_policy_version=self.decision_policy_version,
            lineage_safety_override=self.lineage_safety_override,
        )

    @classmethod
    def build(
        cls,
        *,
        embedding_model_artifact_id: str,
        requirement_role_template_hash: str,
        chunk_role_template_hash: str,
        vector_method_version: str,
        vector_index_kind: VectorIndexKind,
        vector_index_build_config_hash: str,
        vector_search_config_hash: str,
        lexical_method_version: str,
        lexical_config_hash: str,
        lexical_postgres_version: str,
        lexical_regconfig_identity: str,
        fusion_version: str,
        reverse_budget_per_inserted_chunk: int,
        forward_budget_per_requirement: int,
        verifier_execution_spec_hash: str,
        decision_policy_version: str,
        lineage_safety_override: bool,
    ) -> M5CandidatePolicyManifest:
        policy_id = digests.candidate_policy_manifest_digest(
            embedding_model_artifact_id=embedding_model_artifact_id,
            requirement_role_template_hash=requirement_role_template_hash,
            chunk_role_template_hash=chunk_role_template_hash,
            vector_method_version=vector_method_version,
            vector_index_kind=vector_index_kind,
            vector_index_build_config_hash=vector_index_build_config_hash,
            vector_search_config_hash=vector_search_config_hash,
            lexical_method_version=lexical_method_version,
            lexical_config_hash=lexical_config_hash,
            lexical_postgres_version=lexical_postgres_version,
            lexical_regconfig_identity=lexical_regconfig_identity,
            fusion_version=fusion_version,
            reverse_budget_per_inserted_chunk=reverse_budget_per_inserted_chunk,
            forward_budget_per_requirement=forward_budget_per_requirement,
            verifier_execution_spec_hash=verifier_execution_spec_hash,
            decision_policy_version=decision_policy_version,
            lineage_safety_override=lineage_safety_override,
        )
        return cls(
            candidate_policy_id=policy_id,
            embedding_model_artifact_id=embedding_model_artifact_id,
            requirement_role_template_hash=requirement_role_template_hash,
            chunk_role_template_hash=chunk_role_template_hash,
            vector_method_version=vector_method_version,
            vector_index_kind=vector_index_kind,
            vector_index_build_config_hash=vector_index_build_config_hash,
            vector_search_config_hash=vector_search_config_hash,
            lexical_method_version=lexical_method_version,
            lexical_config_hash=lexical_config_hash,
            lexical_postgres_version=lexical_postgres_version,
            lexical_regconfig_identity=lexical_regconfig_identity,
            fusion_version=fusion_version,
            reverse_budget_per_inserted_chunk=reverse_budget_per_inserted_chunk,
            forward_budget_per_requirement=forward_budget_per_requirement,
            verifier_execution_spec_hash=verifier_execution_spec_hash,
            decision_policy_version=decision_policy_version,
            lineage_safety_override=lineage_safety_override,
        )

    @property
    def forward_retrieval_execution_spec_hash(self) -> str:
        return digests.forward_retrieval_execution_spec_digest(
            self.manifest_hash,
            M5TextNormalizerProvenance().normalizer_provenance_hash,
        )

    @property
    def reverse_retrieval_execution_spec_hash(self) -> str:
        return digests.reverse_retrieval_execution_spec_digest(
            self.manifest_hash,
            M5TextNormalizerProvenance().normalizer_provenance_hash,
        )

    @property
    def requirement_verifier_role_binding_hash(self) -> str:
        return digests.requirement_verifier_role_binding_digest(
            self.requirement_role_template_hash, self.chunk_role_template_hash
        )


@dataclass(frozen=True, slots=True)
class RequirementRegistrySnapshotEntry:
    requirement_version_id: str
    group_version_id: str
    group_family_id: str
    owner_claim_id: str
    normalized_requirement_text: str
    requirement_text_hash: str

    def __post_init__(self) -> None:
        for name in (
            "requirement_version_id",
            "group_version_id",
            "group_family_id",
            "owner_claim_id",
            "normalized_requirement_text",
        ):
            _require_text(name, getattr(self, name))
        if normalize_text_v1(self.normalized_requirement_text) != (
            self.normalized_requirement_text
        ):
            raise ValidationError("requirement snapshot text is not M5-normalized")
        _require_identity(
            "requirement_text_hash",
            self.requirement_text_hash,
            normalized_text_hash_v1(self.normalized_requirement_text),
        )

    @property
    def digest_row(self) -> digests.RequirementRegistryRow:
        return (
            self.requirement_version_id,
            self.group_version_id,
            self.group_family_id,
            self.owner_claim_id,
            self.normalized_requirement_text,
            self.requirement_text_hash,
        )

    @classmethod
    def build(
        cls,
        *,
        requirement_version_id: str,
        group_version_id: str,
        group_family_id: str,
        owner_claim_id: str,
        requirement_text: str,
    ) -> RequirementRegistrySnapshotEntry:
        normalized = normalize_text_v1(requirement_text)
        return cls(
            requirement_version_id=requirement_version_id,
            group_version_id=group_version_id,
            group_family_id=group_family_id,
            owner_claim_id=owner_claim_id,
            normalized_requirement_text=normalized,
            requirement_text_hash=normalized_text_hash_v1(normalized),
        )


@dataclass(frozen=True, slots=True)
class RequirementRegistrySnapshot:
    requirement_count: int
    entries: tuple[RequirementRegistrySnapshotEntry, ...]
    requirement_registry_snapshot_digest: str

    def __post_init__(self) -> None:
        _require_tuple("requirement registry entries", self.entries)
        _require_int("requirement_count", self.requirement_count)
        if self.requirement_count != len(self.entries):
            raise ValidationError("requirement_count must equal entries length")
        ids = tuple(entry.requirement_version_id for entry in self.entries)
        if ids != tuple(sorted(set(ids))):
            raise ValidationError(
                "requirement snapshot entries must be ID-sorted unique"
            )
        _require_identity(
            "requirement_registry_snapshot_digest",
            self.requirement_registry_snapshot_digest,
            digests.requirement_registry_snapshot_digest(
                tuple(entry.digest_row for entry in self.entries)
            ),
        )

    @classmethod
    def build(
        cls, entries: tuple[RequirementRegistrySnapshotEntry, ...]
    ) -> RequirementRegistrySnapshot:
        canonical = tuple(
            sorted(entries, key=lambda entry: entry.requirement_version_id)
        )
        if len({entry.requirement_version_id for entry in canonical}) != len(canonical):
            raise ValidationError("requirement snapshot entries must be unique")
        digest = digests.requirement_registry_snapshot_digest(
            tuple(entry.digest_row for entry in canonical)
        )
        return cls(len(canonical), canonical, digest)

    def contains(self, requirement_version_id: str) -> bool:
        return any(
            entry.requirement_version_id == requirement_version_id
            for entry in self.entries
        )

    def member(self, requirement_version_id: str) -> RequirementRegistrySnapshotEntry:
        for entry in self.entries:
            if entry.requirement_version_id == requirement_version_id:
                return entry
        raise ValidationError("requirement is absent from the frozen registry snapshot")


@dataclass(frozen=True, slots=True)
class ActiveChunkSnapshotEntry:
    chunk_version_id: str
    text_hash: str

    def __post_init__(self) -> None:
        _require_text("chunk_version_id", self.chunk_version_id)
        _require_hash("text_hash", self.text_hash)

    @classmethod
    def build(
        cls, *, chunk_version_id: str, chunk_text: str
    ) -> ActiveChunkSnapshotEntry:
        normalized = normalize_text_v1(chunk_text)
        if not normalized:
            raise ValidationError("M5-normalized chunk text must be nonempty")
        return cls(chunk_version_id, normalized_text_hash_v1(chunk_text))


@dataclass(frozen=True, slots=True)
class ActiveChunkSnapshot:
    chunk_count: int
    entries: tuple[ActiveChunkSnapshotEntry, ...]
    active_chunk_snapshot_digest: str

    def __post_init__(self) -> None:
        _require_tuple("active chunk entries", self.entries)
        _require_int("chunk_count", self.chunk_count)
        if self.chunk_count != len(self.entries):
            raise ValidationError("chunk_count must equal entries length")
        ids = tuple(entry.chunk_version_id for entry in self.entries)
        if ids != tuple(sorted(set(ids))):
            raise ValidationError("active chunk entries must be ID-sorted unique")
        _require_identity(
            "active_chunk_snapshot_digest",
            self.active_chunk_snapshot_digest,
            digests.active_chunk_snapshot_digest(
                tuple(
                    (entry.chunk_version_id, entry.text_hash) for entry in self.entries
                )
            ),
        )

    @classmethod
    def build(
        cls, entries: tuple[ActiveChunkSnapshotEntry, ...]
    ) -> ActiveChunkSnapshot:
        canonical = tuple(sorted(entries, key=lambda entry: entry.chunk_version_id))
        if len({entry.chunk_version_id for entry in canonical}) != len(canonical):
            raise ValidationError("active chunk snapshot entries must be unique")
        digest = digests.active_chunk_snapshot_digest(
            tuple((entry.chunk_version_id, entry.text_hash) for entry in canonical)
        )
        return cls(len(canonical), canonical, digest)

    def contains(self, chunk_version_id: str) -> bool:
        return any(entry.chunk_version_id == chunk_version_id for entry in self.entries)

    def member(self, chunk_version_id: str) -> ActiveChunkSnapshotEntry:
        for entry in self.entries:
            if entry.chunk_version_id == chunk_version_id:
                return entry
        raise ValidationError("chunk is absent from the frozen active-chunk snapshot")


@dataclass(frozen=True, slots=True, order=True)
class SemanticPairKey:
    subject_kind: SubjectKind
    subject_id: str
    chunk_version_id: str

    def __post_init__(self) -> None:
        if self.subject_kind is not SubjectKind.REQUIREMENT:
            raise ValidationError("M5 runtime pairs require subject_kind=requirement")
        _require_text("subject_id", self.subject_id)
        _require_text("chunk_version_id", self.chunk_version_id)

    @property
    def semantic_pair_digest(self) -> str:
        return digests.semantic_pair_digest(
            self.subject_kind, self.subject_id, self.chunk_version_id
        )

    def validate_snapshots(
        self,
        requirement_snapshot: RequirementRegistrySnapshot,
        chunk_snapshot: ActiveChunkSnapshot,
    ) -> None:
        if not requirement_snapshot.contains(self.subject_id):
            raise ValidationError("pair requirement is absent from its snapshot")
        if not chunk_snapshot.contains(self.chunk_version_id):
            raise ValidationError("pair chunk is absent from its snapshot")


@dataclass(frozen=True, slots=True)
class M5DiscoveryScopeContract:
    direction: M5DiscoveryDirection
    requirement_version_id: str | None
    inserted_chunk_version_id: str | None
    candidate_policy_id: str
    requirement_registry_snapshot_digest: str
    active_chunk_snapshot_digest: str
    scope_contract_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.direction, M5DiscoveryDirection):
            raise ValidationError("direction must be an M5DiscoveryDirection")
        _require_text("candidate_policy_id", self.candidate_policy_id)
        _require_hash(
            "requirement_registry_snapshot_digest",
            self.requirement_registry_snapshot_digest,
        )
        _require_hash("active_chunk_snapshot_digest", self.active_chunk_snapshot_digest)
        if self.direction is M5DiscoveryDirection.FORWARD_REQUIREMENT:
            if self.requirement_version_id is None or (
                self.inserted_chunk_version_id is not None
            ):
                raise ValidationError(
                    "forward scope requires only a requirement target"
                )
            _require_text("requirement_version_id", self.requirement_version_id)
        elif self.inserted_chunk_version_id is None or (
            self.requirement_version_id is not None
        ):
            raise ValidationError(
                "reverse scope requires only an inserted chunk target"
            )
        else:
            _require_text("inserted_chunk_version_id", self.inserted_chunk_version_id)
        _require_identity(
            "scope_contract_digest",
            self.scope_contract_digest,
            digests.discovery_scope_contract_digest(
                direction=self.direction,
                requirement_version_id=self.requirement_version_id,
                inserted_chunk_version_id=self.inserted_chunk_version_id,
                candidate_policy_id=self.candidate_policy_id,
                requirement_registry_snapshot_digest_value=(
                    self.requirement_registry_snapshot_digest
                ),
                active_chunk_snapshot_digest_value=self.active_chunk_snapshot_digest,
            ),
        )

    @classmethod
    def build(
        cls,
        *,
        direction: M5DiscoveryDirection,
        requirement_version_id: str | None,
        inserted_chunk_version_id: str | None,
        candidate_policy_id: str,
        requirement_registry_snapshot_digest: str,
        active_chunk_snapshot_digest: str,
    ) -> M5DiscoveryScopeContract:
        digest = digests.discovery_scope_contract_digest(
            direction=direction,
            requirement_version_id=requirement_version_id,
            inserted_chunk_version_id=inserted_chunk_version_id,
            candidate_policy_id=candidate_policy_id,
            requirement_registry_snapshot_digest_value=(
                requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest_value=active_chunk_snapshot_digest,
        )
        return cls(
            direction,
            requirement_version_id,
            inserted_chunk_version_id,
            candidate_policy_id,
            requirement_registry_snapshot_digest,
            active_chunk_snapshot_digest,
            digest,
        )

    def validate_snapshots(
        self,
        requirement_snapshot: RequirementRegistrySnapshot,
        chunk_snapshot: ActiveChunkSnapshot,
    ) -> None:
        if (
            requirement_snapshot.requirement_registry_snapshot_digest
            != self.requirement_registry_snapshot_digest
            or chunk_snapshot.active_chunk_snapshot_digest
            != self.active_chunk_snapshot_digest
        ):
            raise ValidationError("scope snapshot digest binding mismatch")
        if (
            self.requirement_version_id is not None
            and not requirement_snapshot.contains(self.requirement_version_id)
        ):
            raise ValidationError("forward target is absent from requirement snapshot")
        if self.inserted_chunk_version_id is not None and not chunk_snapshot.contains(
            self.inserted_chunk_version_id
        ):
            raise ValidationError("reverse target is absent from chunk snapshot")

    def validate_pair(self, pair: SemanticPairKey) -> None:
        if (
            self.direction is M5DiscoveryDirection.FORWARD_REQUIREMENT
            and pair.subject_id != self.requirement_version_id
        ):
            raise ValidationError("forward scope selected another requirement")
        if (
            self.direction is M5DiscoveryDirection.REVERSE_CHUNK
            and pair.chunk_version_id != self.inserted_chunk_version_id
        ):
            raise ValidationError("reverse scope selected another chunk")


M5RequirementScopeContract = M5DiscoveryScopeContract


@dataclass(frozen=True, slots=True)
class M5RequirementChannelHit:
    epoch_id: int
    root_job_id: str
    scope_contract_digest: str
    pair: SemanticPairKey
    semantic_pair_digest: str
    candidate_policy_id: str
    channel: M5RequirementAdmissionChannel
    rank: int
    score: float | None
    channel_artifact_hash: str
    hit_digest: str

    def __post_init__(self) -> None:
        _require_int("epoch_id", self.epoch_id, positive=True)
        _require_text("root_job_id", self.root_job_id)
        _require_hash("scope_contract_digest", self.scope_contract_digest)
        _require_identity(
            "semantic_pair_digest",
            self.semantic_pair_digest,
            self.pair.semantic_pair_digest,
        )
        _require_text("candidate_policy_id", self.candidate_policy_id)
        if not isinstance(self.channel, M5RequirementAdmissionChannel):
            raise ValidationError("channel must be an M5 admission channel")
        _require_int("channel rank", self.rank, positive=True)
        if self.channel is M5RequirementAdmissionChannel.LINEAGE:
            if self.score is not None:
                raise ValidationError("lineage hits must carry a NULL score")
        elif (
            self.score is None
            or isinstance(self.score, bool)
            or not isinstance(self.score, (int, float))
            or not math.isfinite(self.score)
        ):
            raise ValidationError("vector and lexical hits require a finite score")
        _require_hash("channel_artifact_hash", self.channel_artifact_hash)
        _require_identity(
            "hit_digest",
            self.hit_digest,
            digests.requirement_channel_hit_digest(
                epoch_id=self.epoch_id,
                root_job_id=self.root_job_id,
                scope_contract_digest=self.scope_contract_digest,
                semantic_pair_digest_value=self.semantic_pair_digest,
                candidate_policy_id=self.candidate_policy_id,
                channel=self.channel,
                rank=self.rank,
                score=self.score,
                channel_artifact_hash=self.channel_artifact_hash,
            ),
        )

    @classmethod
    def build(
        cls,
        *,
        epoch_id: int,
        root_job_id: str,
        scope_contract_digest: str,
        pair: SemanticPairKey,
        candidate_policy_id: str,
        channel: M5RequirementAdmissionChannel,
        rank: int,
        score: float | None,
        channel_artifact_hash: str,
    ) -> M5RequirementChannelHit:
        pair_digest = pair.semantic_pair_digest
        hit_digest = digests.requirement_channel_hit_digest(
            epoch_id=epoch_id,
            root_job_id=root_job_id,
            scope_contract_digest=scope_contract_digest,
            semantic_pair_digest_value=pair_digest,
            candidate_policy_id=candidate_policy_id,
            channel=channel,
            rank=rank,
            score=score,
            channel_artifact_hash=channel_artifact_hash,
        )
        return cls(
            epoch_id,
            root_job_id,
            scope_contract_digest,
            pair,
            pair_digest,
            candidate_policy_id,
            channel,
            rank,
            score,
            channel_artifact_hash,
            hit_digest,
        )


def validate_requirement_channel_hits(
    hits: tuple[M5RequirementChannelHit, ...],
) -> None:
    _require_tuple("channel_hits", hits)
    if hits != tuple(
        sorted(
            hits,
            key=lambda hit: (
                hit.channel.value,
                hit.rank,
                hit.semantic_pair_digest,
            ),
        )
    ):
        raise ValidationError("channel hits must use canonical channel/rank/pair order")
    channel_rank_keys = tuple((hit.channel, hit.rank) for hit in hits)
    channel_pair_keys = tuple((hit.channel, hit.semantic_pair_digest) for hit in hits)
    if len(set(channel_rank_keys)) != len(channel_rank_keys):
        raise ValidationError("channel hits repeat a channel rank")
    if len(set(channel_pair_keys)) != len(channel_pair_keys):
        raise ValidationError("channel hits repeat a channel/pair")
    for channel in M5RequirementAdmissionChannel:
        ranks = tuple(hit.rank for hit in hits if hit.channel is channel)
        if ranks and tuple(sorted(ranks)) != tuple(range(1, len(ranks) + 1)):
            raise ValidationError("nonempty channel ranks must be dense from one")


@dataclass(frozen=True, slots=True)
class M5RequirementScopeSelection:
    root_job_id: str
    scope_contract_digest: str
    pair: SemanticPairKey
    semantic_pair_digest: str
    fused_rank: int
    reasons: tuple[M5RequirementAdmissionChannel, ...]
    mandatory_lineage: bool
    selection_digest: str

    def __post_init__(self) -> None:
        _require_text("root_job_id", self.root_job_id)
        _require_hash("scope_contract_digest", self.scope_contract_digest)
        _require_identity(
            "semantic_pair_digest",
            self.semantic_pair_digest,
            self.pair.semantic_pair_digest,
        )
        _require_int("fused_rank", self.fused_rank, positive=True)
        _require_tuple("selection reasons", self.reasons)
        if not self.reasons:
            raise ValidationError("scope selection reasons must be nonempty")
        if not all(
            isinstance(reason, M5RequirementAdmissionChannel) for reason in self.reasons
        ):
            raise ValidationError("selection reasons must be M5 channels")
        expected_reasons = tuple(sorted(set(self.reasons), key=lambda item: item.value))
        if self.reasons != expected_reasons:
            raise ValidationError("selection reasons must be wire-sorted and unique")
        if self.mandatory_lineage != (
            M5RequirementAdmissionChannel.LINEAGE in self.reasons
        ):
            raise ValidationError("mandatory_lineage must equal lineage membership")
        _require_identity(
            "selection_digest",
            self.selection_digest,
            digests.requirement_scope_selection_digest(
                root_job_id=self.root_job_id,
                scope_contract_digest=self.scope_contract_digest,
                semantic_pair_digest_value=self.semantic_pair_digest,
                fused_rank=self.fused_rank,
                reasons=self.reasons,
                mandatory_lineage=self.mandatory_lineage,
            ),
        )

    @classmethod
    def build(
        cls,
        *,
        root_job_id: str,
        scope_contract_digest: str,
        pair: SemanticPairKey,
        fused_rank: int,
        reasons: tuple[M5RequirementAdmissionChannel, ...],
    ) -> M5RequirementScopeSelection:
        if not all(
            isinstance(reason, M5RequirementAdmissionChannel) for reason in reasons
        ):
            raise ValidationError("selection reasons must be M5 channels")
        canonical = tuple(sorted(set(reasons), key=lambda item: item.value))
        pair_digest = pair.semantic_pair_digest
        mandatory = M5RequirementAdmissionChannel.LINEAGE in canonical
        digest = digests.requirement_scope_selection_digest(
            root_job_id=root_job_id,
            scope_contract_digest=scope_contract_digest,
            semantic_pair_digest_value=pair_digest,
            fused_rank=fused_rank,
            reasons=canonical,
            mandatory_lineage=mandatory,
        )
        return cls(
            root_job_id,
            scope_contract_digest,
            pair,
            pair_digest,
            fused_rank,
            canonical,
            mandatory,
            digest,
        )


def validate_requirement_scope_selections(
    selections: tuple[M5RequirementScopeSelection, ...],
    hits: tuple[M5RequirementChannelHit, ...],
) -> None:
    _require_tuple("scope selections", selections)
    expected = tuple(
        sorted(
            selections, key=lambda item: (item.fused_rank, item.semantic_pair_digest)
        )
    )
    if selections != expected:
        raise ValidationError("scope selections must use canonical fused order")
    if tuple(selection.fused_rank for selection in selections) != tuple(
        range(1, len(selections) + 1)
    ):
        raise ValidationError("selection fused ranks must be dense from one")
    if len({selection.semantic_pair_digest for selection in selections}) != len(
        selections
    ):
        raise ValidationError("scope selections must not repeat a pair")
    hit_keys = {
        (hit.root_job_id, hit.semantic_pair_digest, hit.channel) for hit in hits
    }
    for selection in selections:
        for reason in selection.reasons:
            if (
                selection.root_job_id,
                selection.semantic_pair_digest,
                reason,
            ) not in hit_keys:
                raise ValidationError("selection reason has no matching channel hit")


@dataclass(frozen=True, slots=True)
class M5RequirementDiscoveryResult:
    root_job_id: str
    scope_contract_digest: str
    termination: M5RetrievalTermination
    channel_hits: tuple[M5RequirementChannelHit, ...]
    selections: tuple[M5RequirementScopeSelection, ...]
    approximate_selection_count: int
    mandatory_lineage_only_count: int
    result_artifact_hash: str
    result_artifact_id: str

    def __post_init__(self) -> None:
        _require_text("root_job_id", self.root_job_id)
        _require_hash("scope_contract_digest", self.scope_contract_digest)
        if not isinstance(self.termination, M5RetrievalTermination):
            raise ValidationError("termination must be an M5RetrievalTermination")
        validate_requirement_channel_hits(self.channel_hits)
        validate_requirement_scope_selections(self.selections, self.channel_hits)
        if len({hit.epoch_id for hit in self.channel_hits}) > 1:
            raise ValidationError("one discovery result cannot mix epochs")
        if len({hit.candidate_policy_id for hit in self.channel_hits}) > 1:
            raise ValidationError("one discovery result cannot mix policies")
        for nested_hit in self.channel_hits:
            if (
                nested_hit.root_job_id != self.root_job_id
                or nested_hit.scope_contract_digest != self.scope_contract_digest
            ):
                raise ValidationError("nested discovery row has another root or scope")
        for nested_selection in self.selections:
            if (
                nested_selection.root_job_id != self.root_job_id
                or nested_selection.scope_contract_digest != self.scope_contract_digest
            ):
                raise ValidationError("nested discovery row has another root or scope")
        _require_int("approximate_selection_count", self.approximate_selection_count)
        _require_int("mandatory_lineage_only_count", self.mandatory_lineage_only_count)
        approximate = sum(
            bool(
                set(selection.reasons)
                & {
                    M5RequirementAdmissionChannel.VECTOR,
                    M5RequirementAdmissionChannel.LEXICAL,
                }
            )
            for selection in self.selections
        )
        lineage_only = sum(
            selection.reasons == (M5RequirementAdmissionChannel.LINEAGE,)
            for selection in self.selections
        )
        if approximate != self.approximate_selection_count:
            raise ValidationError("approximate selection count is not exact")
        if lineage_only != self.mandatory_lineage_only_count:
            raise ValidationError("lineage-only selection count is not exact")
        if approximate + lineage_only != len(self.selections):
            raise ValidationError("every selection must be approximate or lineage-only")
        lineage_tail = tuple(
            selection
            for selection in self.selections
            if selection.reasons == (M5RequirementAdmissionChannel.LINEAGE,)
        )
        if lineage_tail:
            first_lineage_rank = lineage_tail[0].fused_rank
            if any(
                selection.reasons != (M5RequirementAdmissionChannel.LINEAGE,)
                for selection in self.selections[first_lineage_rank - 1 :]
            ):
                raise ValidationError("lineage-only selections must form the tail")
            if tuple(item.semantic_pair_digest for item in lineage_tail) != tuple(
                sorted(item.semantic_pair_digest for item in lineage_tail)
            ):
                raise ValidationError("lineage-only tail must be pair-digest sorted")
        _require_identity(
            "result_artifact_hash",
            self.result_artifact_hash,
            digests.requirement_discovery_result_digest(
                root_job_id=self.root_job_id,
                scope_contract_digest=self.scope_contract_digest,
                termination=self.termination,
                hit_digests=(hit.hit_digest for hit in self.channel_hits),
                selection_digests=(
                    selection.selection_digest for selection in self.selections
                ),
                approximate_selection_count=self.approximate_selection_count,
                mandatory_lineage_only_count=self.mandatory_lineage_only_count,
            ),
        )
        _require_identity(
            "result_artifact_id",
            self.result_artifact_id,
            digests.requirement_discovery_artifact_id(
                self.root_job_id, self.result_artifact_hash
            ),
        )

    @classmethod
    def build(
        cls,
        *,
        root_job_id: str,
        scope_contract_digest: str,
        termination: M5RetrievalTermination,
        channel_hits: tuple[M5RequirementChannelHit, ...],
        selections: tuple[M5RequirementScopeSelection, ...],
    ) -> M5RequirementDiscoveryResult:
        approximate = sum(
            bool(
                set(selection.reasons)
                & {
                    M5RequirementAdmissionChannel.VECTOR,
                    M5RequirementAdmissionChannel.LEXICAL,
                }
            )
            for selection in selections
        )
        lineage_only = sum(
            selection.reasons == (M5RequirementAdmissionChannel.LINEAGE,)
            for selection in selections
        )
        result_hash = digests.requirement_discovery_result_digest(
            root_job_id=root_job_id,
            scope_contract_digest=scope_contract_digest,
            termination=termination,
            hit_digests=(hit.hit_digest for hit in channel_hits),
            selection_digests=(item.selection_digest for item in selections),
            approximate_selection_count=approximate,
            mandatory_lineage_only_count=lineage_only,
        )
        return cls(
            root_job_id,
            scope_contract_digest,
            termination,
            channel_hits,
            selections,
            approximate,
            lineage_only,
            result_hash,
            digests.requirement_discovery_artifact_id(root_job_id, result_hash),
        )

    def validate_policy(
        self,
        *,
        direction: M5DiscoveryDirection,
        manifest: M5CandidatePolicyManifest,
        eligible_snapshot_exhausted: bool,
    ) -> None:
        budget = (
            manifest.forward_budget_per_requirement
            if direction is M5DiscoveryDirection.FORWARD_REQUIREMENT
            else manifest.reverse_budget_per_inserted_chunk
        )
        if self.approximate_selection_count > budget:
            raise ValidationError(
                "discovery result exceeds its frozen direction budget"
            )
        if self.mandatory_lineage_only_count and not manifest.lineage_safety_override:
            raise ValidationError("lineage-only selection requires the safety override")
        if self.termination is M5RetrievalTermination.BUDGET_FILLED:
            if self.approximate_selection_count != budget:
                raise ValidationError("budget_filled requires the complete budget")
        elif (
            self.approximate_selection_count >= budget
            or not eligible_snapshot_exhausted
        ):
            raise ValidationError(
                "snapshot_exhausted requires a short result and exhaustion evidence"
            )


@dataclass(frozen=True, slots=True)
class M5RequirementAdmittedPairSource:
    root_job_id: str
    scope_contract_digest: str
    selection_digest: str

    def __post_init__(self) -> None:
        _require_text("root_job_id", self.root_job_id)
        _require_hash("scope_contract_digest", self.scope_contract_digest)
        _require_hash("selection_digest", self.selection_digest)

    @property
    def digest_row(self) -> digests.AdmittedPairSourceRow:
        return self.root_job_id, self.scope_contract_digest, self.selection_digest


@dataclass(frozen=True, slots=True)
class M5RequirementAdmittedPair:
    epoch_id: int
    pair: SemanticPairKey
    semantic_pair_digest: str
    candidate_policy_id: str
    owner_root_job_id: str
    sources: tuple[M5RequirementAdmittedPairSource, ...]
    reasons: tuple[M5RequirementAdmissionChannel, ...]
    mandatory_lineage: bool
    admitted_pair_digest: str

    def __post_init__(self) -> None:
        _require_int("epoch_id", self.epoch_id, positive=True)
        _require_identity(
            "semantic_pair_digest",
            self.semantic_pair_digest,
            self.pair.semantic_pair_digest,
        )
        _require_text("candidate_policy_id", self.candidate_policy_id)
        _require_text("owner_root_job_id", self.owner_root_job_id)
        _require_tuple("admitted-pair sources", self.sources)
        if not self.sources:
            raise ValidationError("admitted pair requires at least one source")
        source_ids = tuple(source.root_job_id for source in self.sources)
        if source_ids != tuple(sorted(set(source_ids))):
            raise ValidationError("admitted-pair sources must be root-ID sorted unique")
        if self.owner_root_job_id != source_ids[0]:
            raise ValidationError("owner root must be the least UTF-8 root ID")
        _require_tuple("admitted-pair reasons", self.reasons)
        if not self.reasons or not all(
            isinstance(reason, M5RequirementAdmissionChannel) for reason in self.reasons
        ):
            raise ValidationError("admitted-pair reasons must be M5 channels")
        if self.reasons != tuple(
            sorted(set(self.reasons), key=lambda item: item.value)
        ):
            raise ValidationError("admitted-pair reasons must be wire-sorted unique")
        if self.mandatory_lineage != (
            M5RequirementAdmissionChannel.LINEAGE in self.reasons
        ):
            raise ValidationError("mandatory_lineage must equal reason membership")
        _require_identity(
            "admitted_pair_digest",
            self.admitted_pair_digest,
            digests.requirement_admitted_pair_digest(
                epoch_id=self.epoch_id,
                semantic_pair_digest_value=self.semantic_pair_digest,
                candidate_policy_id=self.candidate_policy_id,
                owner_root_job_id=self.owner_root_job_id,
                sources=(source.digest_row for source in self.sources),
                reasons=self.reasons,
                mandatory_lineage=self.mandatory_lineage,
            ),
        )

    @classmethod
    def build(
        cls,
        *,
        epoch_id: int,
        pair: SemanticPairKey,
        candidate_policy_id: str,
        sources: tuple[M5RequirementAdmittedPairSource, ...],
        reasons: tuple[M5RequirementAdmissionChannel, ...],
    ) -> M5RequirementAdmittedPair:
        canonical_sources = tuple(sorted(sources, key=lambda item: item.root_job_id))
        if not canonical_sources:
            raise ValidationError("admitted pair requires sources")
        if not all(
            isinstance(reason, M5RequirementAdmissionChannel) for reason in reasons
        ):
            raise ValidationError("admitted-pair reasons must be M5 channels")
        canonical_reasons = tuple(sorted(set(reasons), key=lambda item: item.value))
        pair_digest = pair.semantic_pair_digest
        owner = canonical_sources[0].root_job_id
        mandatory = M5RequirementAdmissionChannel.LINEAGE in canonical_reasons
        digest = digests.requirement_admitted_pair_digest(
            epoch_id=epoch_id,
            semantic_pair_digest_value=pair_digest,
            candidate_policy_id=candidate_policy_id,
            owner_root_job_id=owner,
            sources=(source.digest_row for source in canonical_sources),
            reasons=canonical_reasons,
            mandatory_lineage=mandatory,
        )
        return cls(
            epoch_id,
            pair,
            pair_digest,
            candidate_policy_id,
            owner,
            canonical_sources,
            canonical_reasons,
            mandatory,
            digest,
        )


@dataclass(frozen=True, slots=True)
class M5RequirementPairInput:
    pair: SemanticPairKey
    semantic_pair_digest: str
    scope_contract_digest: str
    candidate_policy_id: str
    owner_claim_id: str
    group_version_id: str
    group_family_id: str
    requirement_ordinal: int
    normalized_requirement_text: str
    requirement_text_hash: str
    document_version_id: str
    chunk_index: int
    chunk_text: str
    stored_chunk_text_hash: str
    m5_chunk_text_hash: str
    chunker_artifact_id: str
    normalizer_id: str
    normalizer_provenance_hash: str
    pair_input_hash: str

    def __post_init__(self) -> None:
        _require_identity(
            "semantic_pair_digest",
            self.semantic_pair_digest,
            self.pair.semantic_pair_digest,
        )
        _require_hash("scope_contract_digest", self.scope_contract_digest)
        for name in (
            "candidate_policy_id",
            "owner_claim_id",
            "group_version_id",
            "group_family_id",
            "normalized_requirement_text",
            "document_version_id",
            "chunk_text",
            "chunker_artifact_id",
            "normalizer_id",
        ):
            _require_text(name, getattr(self, name))
        _require_int("requirement_ordinal", self.requirement_ordinal)
        _require_int("chunk_index", self.chunk_index)
        if normalize_text_v1(self.normalized_requirement_text) != (
            self.normalized_requirement_text
        ):
            raise ValidationError("requirement text must already be M5-normalized")
        _require_identity(
            "requirement_text_hash",
            self.requirement_text_hash,
            normalized_text_hash_v1(self.normalized_requirement_text),
        )
        if not normalize_text_v1(self.chunk_text):
            raise ValidationError("M5-normalized chunk text must be nonempty")
        _require_hash("stored_chunk_text_hash", self.stored_chunk_text_hash)
        _require_identity(
            "m5_chunk_text_hash",
            self.m5_chunk_text_hash,
            normalized_text_hash_v1(self.chunk_text),
        )
        provenance = M5TextNormalizerProvenance()
        if self.normalizer_id != provenance.normalizer_id:
            raise ValidationError("pair input normalizer_id is not frozen M5 v1")
        _require_identity(
            "normalizer_provenance_hash",
            self.normalizer_provenance_hash,
            provenance.normalizer_provenance_hash,
        )
        _require_identity(
            "pair_input_hash",
            self.pair_input_hash,
            self.expected_pair_input_hash,
        )

    @property
    def expected_pair_input_hash(self) -> str:
        return digests.requirement_pair_input_digest(
            semantic_pair_digest_value=self.semantic_pair_digest,
            scope_contract_digest=self.scope_contract_digest,
            candidate_policy_id=self.candidate_policy_id,
            owner_claim_id=self.owner_claim_id,
            group_version_id=self.group_version_id,
            group_family_id=self.group_family_id,
            requirement_ordinal=self.requirement_ordinal,
            normalized_requirement_text=self.normalized_requirement_text,
            requirement_text_hash=self.requirement_text_hash,
            document_version_id=self.document_version_id,
            chunk_index=self.chunk_index,
            chunk_text=self.chunk_text,
            stored_chunk_text_hash=self.stored_chunk_text_hash,
            m5_chunk_text_hash=self.m5_chunk_text_hash,
            chunker_artifact_id=self.chunker_artifact_id,
            normalizer_id=self.normalizer_id,
            normalizer_provenance_hash=self.normalizer_provenance_hash,
        )

    @classmethod
    def build(
        cls,
        *,
        pair: SemanticPairKey,
        scope_contract_digest: str,
        candidate_policy_id: str,
        owner_claim_id: str,
        group_version_id: str,
        group_family_id: str,
        requirement_ordinal: int,
        requirement_text: str,
        document_version_id: str,
        chunk_index: int,
        chunk_text: str,
        stored_chunk_text_hash: str,
        chunker_artifact_id: str,
    ) -> M5RequirementPairInput:
        normalized_requirement = normalize_text_v1(requirement_text)
        normalizer = M5TextNormalizerProvenance()
        requirement_hash = normalized_text_hash_v1(normalized_requirement)
        chunk_hash = normalized_text_hash_v1(chunk_text)
        pair_input_hash = digests.requirement_pair_input_digest(
            semantic_pair_digest_value=pair.semantic_pair_digest,
            scope_contract_digest=scope_contract_digest,
            candidate_policy_id=candidate_policy_id,
            owner_claim_id=owner_claim_id,
            group_version_id=group_version_id,
            group_family_id=group_family_id,
            requirement_ordinal=requirement_ordinal,
            normalized_requirement_text=normalized_requirement,
            requirement_text_hash=requirement_hash,
            document_version_id=document_version_id,
            chunk_index=chunk_index,
            chunk_text=chunk_text,
            stored_chunk_text_hash=stored_chunk_text_hash,
            m5_chunk_text_hash=chunk_hash,
            chunker_artifact_id=chunker_artifact_id,
            normalizer_id=normalizer.normalizer_id,
            normalizer_provenance_hash=normalizer.normalizer_provenance_hash,
        )
        return cls(
            pair=pair,
            semantic_pair_digest=pair.semantic_pair_digest,
            scope_contract_digest=scope_contract_digest,
            candidate_policy_id=candidate_policy_id,
            owner_claim_id=owner_claim_id,
            group_version_id=group_version_id,
            group_family_id=group_family_id,
            requirement_ordinal=requirement_ordinal,
            normalized_requirement_text=normalized_requirement,
            requirement_text_hash=requirement_hash,
            document_version_id=document_version_id,
            chunk_index=chunk_index,
            chunk_text=chunk_text,
            stored_chunk_text_hash=stored_chunk_text_hash,
            m5_chunk_text_hash=chunk_hash,
            chunker_artifact_id=chunker_artifact_id,
            normalizer_id=normalizer.normalizer_id,
            normalizer_provenance_hash=normalizer.normalizer_provenance_hash,
            pair_input_hash=pair_input_hash,
        )

    def validate_bound_rows(
        self,
        *,
        requirement_entry: RequirementRegistrySnapshotEntry,
        chunk_entry: ActiveChunkSnapshotEntry,
    ) -> None:
        expected_requirement = (
            requirement_entry.requirement_version_id,
            requirement_entry.group_version_id,
            requirement_entry.group_family_id,
            requirement_entry.owner_claim_id,
            requirement_entry.normalized_requirement_text,
            requirement_entry.requirement_text_hash,
        )
        actual_requirement = (
            self.pair.subject_id,
            self.group_version_id,
            self.group_family_id,
            self.owner_claim_id,
            self.normalized_requirement_text,
            self.requirement_text_hash,
        )
        if actual_requirement != expected_requirement:
            raise ValidationError("pair input disagrees with requirement snapshot row")
        if (
            self.pair.chunk_version_id != chunk_entry.chunk_version_id
            or self.m5_chunk_text_hash != chunk_entry.text_hash
        ):
            raise ValidationError("pair input disagrees with active chunk snapshot row")


@dataclass(frozen=True, slots=True)
class M5RequirementVerifierArtifact:
    artifact_id: str
    artifact_hash: str
    pair: SemanticPairKey
    semantic_pair_digest: str
    pair_input_hash: str
    execution_spec_hash: str
    model_artifact_id: str
    model_id: str
    model_revision: str
    prompt_artifact_id: str
    prompt_version: str
    calibration_version: str
    calibration_artifact_hash: str | None
    temperature: float
    decision_policy_version: str
    decision_policy_hash: str
    support_score: float
    refute_score: float
    neutral_score: float
    raw_logits: tuple[float, float, float]
    raw_output_hash: str
    operational_label: VerificationLabel

    def __post_init__(self) -> None:
        _require_identity(
            "semantic_pair_digest",
            self.semantic_pair_digest,
            self.pair.semantic_pair_digest,
        )
        for name in (
            "pair_input_hash",
            "execution_spec_hash",
            "decision_policy_hash",
            "raw_output_hash",
        ):
            _require_hash(name, getattr(self, name))
        if self.calibration_artifact_hash is not None:
            _require_hash("calibration_artifact_hash", self.calibration_artifact_hash)
        for name in (
            "model_artifact_id",
            "model_id",
            "model_revision",
            "prompt_artifact_id",
            "prompt_version",
            "calibration_version",
            "decision_policy_version",
        ):
            _require_text(name, getattr(self, name))
        _require_tuple("raw_logits", self.raw_logits)
        if len(self.raw_logits) != 3:
            raise ValidationError("raw_logits must contain exactly three values")
        numeric_values = (
            self.temperature,
            self.support_score,
            self.refute_score,
            self.neutral_score,
            *self.raw_logits,
        )
        if not all(
            not isinstance(value, bool)
            and isinstance(value, (int, float))
            and math.isfinite(value)
            for value in numeric_values
        ):
            raise ValidationError("verifier numeric values must be finite")
        if self.temperature <= 0:
            raise ValidationError("temperature must be positive")
        for name in ("support_score", "refute_score", "neutral_score"):
            score = float(getattr(self, name))
            if not 0.0 <= score <= 1.0:
                raise ValidationError(f"{name} must lie in [0, 1]")
        total = (self.support_score + self.refute_score) + self.neutral_score
        if abs(total - 1.0) > 1e-6:
            raise ValidationError("verifier scores must sum to one within 1e-6")
        if not isinstance(self.operational_label, VerificationLabel):
            raise ValidationError("operational_label must be a VerificationLabel")
        _require_identity(
            "artifact_hash", self.artifact_hash, self.expected_artifact_hash
        )
        _require_identity(
            "artifact_id",
            self.artifact_id,
            digests.requirement_verifier_artifact_id(
                self.semantic_pair_digest, self.pair_input_hash, self.artifact_hash
            ),
        )

    @property
    def expected_artifact_hash(self) -> str:
        return digests.requirement_verifier_result_digest(
            semantic_pair_digest_value=self.semantic_pair_digest,
            pair_input_hash=self.pair_input_hash,
            execution_spec_hash=self.execution_spec_hash,
            model_artifact_id=self.model_artifact_id,
            model_id=self.model_id,
            model_revision=self.model_revision,
            prompt_artifact_id=self.prompt_artifact_id,
            prompt_version=self.prompt_version,
            calibration_version=self.calibration_version,
            calibration_artifact_hash=self.calibration_artifact_hash,
            temperature=self.temperature,
            decision_policy_version=self.decision_policy_version,
            decision_policy_hash=self.decision_policy_hash,
            support_score=self.support_score,
            refute_score=self.refute_score,
            neutral_score=self.neutral_score,
            raw_logits=self.raw_logits,
            raw_output_hash=self.raw_output_hash,
            operational_label=self.operational_label,
        )

    @classmethod
    def build_checked(
        cls,
        *,
        pair: SemanticPairKey,
        pair_input_hash: str,
        execution_spec_hash: str,
        model_artifact_id: str,
        model_id: str,
        model_revision: str,
        prompt_artifact_id: str,
        prompt_version: str,
        calibration_version: str,
        calibration_artifact_hash: str | None,
        temperature: float,
        decision_policy: DecisionPolicy,
        support_score: float,
        refute_score: float,
        neutral_score: float,
        raw_logits: tuple[float, float, float],
        raw_output_hash: str,
    ) -> M5RequirementVerifierArtifact:
        transient = SemanticObservation(
            observation_id="m5-transient-decision",
            subject_kind=SubjectKind.REQUIREMENT,
            subject_id=pair.subject_id,
            chunk_version_id=pair.chunk_version_id,
            task_type=M5_REQUIREMENT_TASK,
            support_score=support_score,
            refute_score=refute_score,
            neutral_score=neutral_score,
            producer=ModelStamp(model_id, model_revision, prompt_version),
            input_hash=pair_input_hash,
        )
        label = decide(transient, decision_policy)
        policy_hash = _decision_policy_hash(decision_policy)
        pair_digest = pair.semantic_pair_digest
        artifact_hash = digests.requirement_verifier_result_digest(
            semantic_pair_digest_value=pair_digest,
            pair_input_hash=pair_input_hash,
            execution_spec_hash=execution_spec_hash,
            model_artifact_id=model_artifact_id,
            model_id=model_id,
            model_revision=model_revision,
            prompt_artifact_id=prompt_artifact_id,
            prompt_version=prompt_version,
            calibration_version=calibration_version,
            calibration_artifact_hash=calibration_artifact_hash,
            temperature=temperature,
            decision_policy_version=decision_policy.policy_version,
            decision_policy_hash=policy_hash,
            support_score=support_score,
            refute_score=refute_score,
            neutral_score=neutral_score,
            raw_logits=raw_logits,
            raw_output_hash=raw_output_hash,
            operational_label=label,
        )
        return cls(
            artifact_id=digests.requirement_verifier_artifact_id(
                pair_digest, pair_input_hash, artifact_hash
            ),
            artifact_hash=artifact_hash,
            pair=pair,
            semantic_pair_digest=pair_digest,
            pair_input_hash=pair_input_hash,
            execution_spec_hash=execution_spec_hash,
            model_artifact_id=model_artifact_id,
            model_id=model_id,
            model_revision=model_revision,
            prompt_artifact_id=prompt_artifact_id,
            prompt_version=prompt_version,
            calibration_version=calibration_version,
            calibration_artifact_hash=calibration_artifact_hash,
            temperature=temperature,
            decision_policy_version=decision_policy.policy_version,
            decision_policy_hash=policy_hash,
            support_score=support_score,
            refute_score=refute_score,
            neutral_score=neutral_score,
            raw_logits=raw_logits,
            raw_output_hash=raw_output_hash,
            operational_label=label,
        )

    def validate_decision_policy(self, policy: DecisionPolicy) -> None:
        if (
            self.decision_policy_version != policy.policy_version
            or self.decision_policy_hash != _decision_policy_hash(policy)
        ):
            raise ValidationError("verifier artifact decision-policy binding drift")
        observation = self.to_semantic_observation()
        if decide(observation, policy) is not self.operational_label:
            raise ValidationError("operational label was not derived by bound policy")

    def to_semantic_observation(self) -> SemanticObservation:
        return SemanticObservation(
            observation_id=digests.requirement_semantic_observation_id(
                self.artifact_id, M5_REQUIREMENT_TASK
            ),
            subject_kind=SubjectKind.REQUIREMENT,
            subject_id=self.pair.subject_id,
            chunk_version_id=self.pair.chunk_version_id,
            task_type=M5_REQUIREMENT_TASK,
            support_score=self.support_score,
            refute_score=self.refute_score,
            neutral_score=self.neutral_score,
            producer=ModelStamp(
                self.model_id, self.model_revision, self.prompt_version
            ),
            input_hash=self.pair_input_hash,
        )


@dataclass(frozen=True, slots=True)
class M5LogicalJobSpec:
    logical_job_id: str
    structural_event_id: str
    job_kind: M5JobKind
    candidate_policy_id: str
    candidate_policy_manifest_hash: str
    parent_job_id: str | None
    pair: SemanticPairKey | None
    semantic_pair_digest: str | None
    scope_contract_digest: str | None
    requirement_registry_snapshot_digest: str
    active_chunk_snapshot_digest: str
    role_template_hash: str
    execution_spec_hash: str
    expandable: bool
    payload_hash: str

    def __post_init__(self) -> None:
        _require_text("structural_event_id", self.structural_event_id)
        _require_text("candidate_policy_id", self.candidate_policy_id)
        for name in (
            "candidate_policy_manifest_hash",
            "requirement_registry_snapshot_digest",
            "active_chunk_snapshot_digest",
            "role_template_hash",
            "execution_spec_hash",
        ):
            _require_hash(name, getattr(self, name))
        _require_bool("expandable", self.expandable)
        pair_present = self.pair is not None
        if pair_present != (self.semantic_pair_digest is not None):
            raise ValidationError(
                "job pair and semantic digest must be jointly present"
            )
        if self.pair is not None:
            assert self.semantic_pair_digest is not None
            _require_identity(
                "semantic_pair_digest",
                self.semantic_pair_digest,
                self.pair.semantic_pair_digest,
            )
        if self.scope_contract_digest is None:
            raise ValidationError("every M5 runtime job requires an enclosing scope")
        _require_hash("scope_contract_digest", self.scope_contract_digest)
        if self.job_kind in {
            M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
            M5JobKind.REVERSE_REQUIREMENT_DISCOVERY,
        }:
            if (
                self.pair is not None
                or self.parent_job_id is not None
                or not self.expandable
            ):
                raise ValidationError("root job violates the frozen kind/null table")
        elif self.job_kind is M5JobKind.VERIFY_REQUIREMENT_PAIR:
            if self.pair is None or self.parent_job_id is None or self.expandable:
                raise ValidationError(
                    "verifier job violates the frozen kind/null table"
                )
            _require_hash("parent_job_id", self.parent_job_id)
        else:
            raise ValidationError("job_kind must be an M5JobKind")
        expected_payload = digests.job_payload_digest(
            job_kind=self.job_kind,
            candidate_policy_id=self.candidate_policy_id,
            candidate_policy_manifest_hash=self.candidate_policy_manifest_hash,
            parent_job_id=self.parent_job_id,
            semantic_pair_digest_value=self.semantic_pair_digest,
            scope_contract_digest=self.scope_contract_digest,
            requirement_registry_snapshot_digest_value=(
                self.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest_value=self.active_chunk_snapshot_digest,
            role_template_hash=self.role_template_hash,
            execution_spec_hash=self.execution_spec_hash,
            expandable=self.expandable,
        )
        _require_identity("payload_hash", self.payload_hash, expected_payload)
        _require_identity(
            "logical_job_id",
            self.logical_job_id,
            digests.logical_job_id(self.structural_event_id, self.payload_hash),
        )
        if self.parent_job_id == self.logical_job_id:
            raise ValidationError("a job cannot be its own parent")

    @classmethod
    def build(
        cls,
        *,
        structural_event_id: str,
        job_kind: M5JobKind,
        manifest: M5CandidatePolicyManifest,
        scope: M5DiscoveryScopeContract,
        parent_job_id: str | None = None,
        pair: SemanticPairKey | None = None,
    ) -> M5LogicalJobSpec:
        if job_kind is M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL:
            role_hash = manifest.requirement_role_template_hash
            execution_hash = manifest.forward_retrieval_execution_spec_hash
            expandable = True
        elif job_kind is M5JobKind.REVERSE_REQUIREMENT_DISCOVERY:
            role_hash = manifest.chunk_role_template_hash
            execution_hash = manifest.reverse_retrieval_execution_spec_hash
            expandable = True
        else:
            role_hash = manifest.requirement_verifier_role_binding_hash
            execution_hash = manifest.verifier_execution_spec_hash
            expandable = False
        pair_digest = pair.semantic_pair_digest if pair is not None else None
        payload = digests.job_payload_digest(
            job_kind=job_kind,
            candidate_policy_id=manifest.candidate_policy_id,
            candidate_policy_manifest_hash=manifest.manifest_hash,
            parent_job_id=parent_job_id,
            semantic_pair_digest_value=pair_digest,
            scope_contract_digest=scope.scope_contract_digest,
            requirement_registry_snapshot_digest_value=(
                scope.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest_value=scope.active_chunk_snapshot_digest,
            role_template_hash=role_hash,
            execution_spec_hash=execution_hash,
            expandable=expandable,
        )
        job = cls(
            logical_job_id=digests.logical_job_id(structural_event_id, payload),
            structural_event_id=structural_event_id,
            job_kind=job_kind,
            candidate_policy_id=manifest.candidate_policy_id,
            candidate_policy_manifest_hash=manifest.manifest_hash,
            parent_job_id=parent_job_id,
            pair=pair,
            semantic_pair_digest=pair_digest,
            scope_contract_digest=scope.scope_contract_digest,
            requirement_registry_snapshot_digest=(
                scope.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=scope.active_chunk_snapshot_digest,
            role_template_hash=role_hash,
            execution_spec_hash=execution_hash,
            expandable=expandable,
            payload_hash=payload,
        )
        job.validate_manifest_and_scope(manifest, scope)
        return job

    def validate_manifest_and_scope(
        self,
        manifest: M5CandidatePolicyManifest,
        scope: M5DiscoveryScopeContract,
    ) -> None:
        if (
            self.candidate_policy_id != manifest.candidate_policy_id
            or self.candidate_policy_manifest_hash != manifest.manifest_hash
            or self.scope_contract_digest != scope.scope_contract_digest
        ):
            raise ValidationError("job manifest/scope identity mismatch")
        if self.job_kind is M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL:
            expected = (
                M5DiscoveryDirection.FORWARD_REQUIREMENT,
                manifest.requirement_role_template_hash,
                manifest.forward_retrieval_execution_spec_hash,
            )
        elif self.job_kind is M5JobKind.REVERSE_REQUIREMENT_DISCOVERY:
            expected = (
                M5DiscoveryDirection.REVERSE_CHUNK,
                manifest.chunk_role_template_hash,
                manifest.reverse_retrieval_execution_spec_hash,
            )
        else:
            expected = (
                scope.direction,
                manifest.requirement_verifier_role_binding_hash,
                manifest.verifier_execution_spec_hash,
            )
            assert self.pair is not None
            scope.validate_pair(self.pair)
        if (
            scope.direction,
            self.role_template_hash,
            self.execution_spec_hash,
        ) != expected:
            raise ValidationError("job role/execution/direction binding mismatch")


@dataclass(frozen=True, slots=True)
class M5JobCompletion:
    logical_job_id: str
    payload_hash: str
    execution_spec_hash: str
    terminal_state: M5JobState
    result_artifact_id: str | None
    result_artifact_hash: str | None
    scope_closure_digest: str | None
    child_set_hash: str | None
    archive_reason: M5TerminalReason | None
    completion_digest: str

    def __post_init__(self) -> None:
        _require_hash("logical_job_id", self.logical_job_id)
        _require_hash("payload_hash", self.payload_hash)
        _require_hash("execution_spec_hash", self.execution_spec_hash)
        if (
            not isinstance(self.terminal_state, M5JobState)
            or not self.terminal_state.terminal
        ):
            raise ValidationError("completion requires a terminal M5 job state")
        result_present = self.result_artifact_id is not None
        if result_present != (self.result_artifact_hash is not None):
            raise ValidationError("result artifact ID/hash must be jointly present")
        if self.result_artifact_id is not None:
            _require_hash("result_artifact_id", self.result_artifact_id)
            assert self.result_artifact_hash is not None
            _require_hash("result_artifact_hash", self.result_artifact_hash)
        closure_present = self.scope_closure_digest is not None
        if closure_present != (self.child_set_hash is not None):
            raise ValidationError(
                "scope closure and child-set hash are jointly present"
            )
        if self.scope_closure_digest is not None:
            _require_hash("scope_closure_digest", self.scope_closure_digest)
            assert self.child_set_hash is not None
            _require_hash("child_set_hash", self.child_set_hash)
        inactive_reasons = {
            M5TerminalReason.CHUNK_INACTIVE,
            M5TerminalReason.SUBJECT_INACTIVE,
            M5TerminalReason.EPOCH_FAILED,
        }
        cancel_reasons = {
            M5TerminalReason.SUBJECT_INACTIVE,
            M5TerminalReason.SCOPE_RETIRED,
            M5TerminalReason.EPOCH_FAILED,
        }
        failure_reasons = {
            M5TerminalReason.RETRY_EXHAUSTED,
            M5TerminalReason.RETRIEVAL_ERROR,
            M5TerminalReason.VERIFIER_ERROR,
            M5TerminalReason.INVALID_ARTIFACT,
        }
        if self.terminal_state is M5JobState.COMPLETED_ACTIVE:
            if not result_present or self.archive_reason is not None:
                raise ValidationError(
                    "active completion requires result and NULL reason"
                )
        elif self.terminal_state is M5JobState.COMPLETED_INACTIVE:
            if not result_present or self.archive_reason not in inactive_reasons:
                raise ValidationError(
                    "inactive completion result/reason shape is invalid"
                )
        elif self.terminal_state is M5JobState.CANCELLED:
            if (
                result_present
                or closure_present
                or self.archive_reason not in cancel_reasons
            ):
                raise ValidationError("cancelled completion shape/reason is invalid")
        elif (
            result_present
            or closure_present
            or self.archive_reason not in failure_reasons
        ):
            raise ValidationError("terminal-failure completion shape/reason is invalid")
        _require_identity(
            "completion_digest",
            self.completion_digest,
            self.expected_completion_digest,
        )

    @property
    def expected_completion_digest(self) -> str:
        return digests.job_completion_digest(
            logical_job_id_value=self.logical_job_id,
            payload_hash=self.payload_hash,
            execution_spec_hash=self.execution_spec_hash,
            terminal_state=self.terminal_state,
            result_artifact_id=self.result_artifact_id,
            result_artifact_hash=self.result_artifact_hash,
            scope_closure_digest=self.scope_closure_digest,
            child_set_hash=self.child_set_hash,
            archive_reason=self.archive_reason,
        )

    @classmethod
    def build(
        cls,
        *,
        job: M5LogicalJobSpec,
        terminal_state: M5JobState,
        result_artifact_id: str | None = None,
        result_artifact_hash: str | None = None,
        scope_closure_digest: str | None = None,
        child_set_hash: str | None = None,
        archive_reason: M5TerminalReason | None = None,
    ) -> M5JobCompletion:
        completion = digests.job_completion_digest(
            logical_job_id_value=job.logical_job_id,
            payload_hash=job.payload_hash,
            execution_spec_hash=job.execution_spec_hash,
            terminal_state=terminal_state,
            result_artifact_id=result_artifact_id,
            result_artifact_hash=result_artifact_hash,
            scope_closure_digest=scope_closure_digest,
            child_set_hash=child_set_hash,
            archive_reason=archive_reason,
        )
        value = cls(
            job.logical_job_id,
            job.payload_hash,
            job.execution_spec_hash,
            terminal_state,
            result_artifact_id,
            result_artifact_hash,
            scope_closure_digest,
            child_set_hash,
            archive_reason,
            completion,
        )
        value.validate_job(job)
        return value

    def validate_job(self, job: M5LogicalJobSpec) -> None:
        if (
            self.logical_job_id != job.logical_job_id
            or self.payload_hash != job.payload_hash
            or self.execution_spec_hash != job.execution_spec_hash
        ):
            raise ValidationError("completion belongs to another logical job")
        closure_present = self.scope_closure_digest is not None
        if job.expandable != closure_present and self.terminal_state in {
            M5JobState.COMPLETED_ACTIVE,
            M5JobState.COMPLETED_INACTIVE,
        }:
            raise ValidationError("completion closure shape conflicts with job kind")
        if job.expandable and self.terminal_state is M5JobState.COMPLETED_INACTIVE:
            assert job.scope_contract_digest is not None
            if self.scope_closure_digest != digests.discovery_scope_closure_digest(
                job.scope_contract_digest, ()
            ) or self.child_set_hash != digests.child_set_digest(()):
                raise ValidationError("inactive root must use canonical empty closures")
        if self.terminal_state is M5JobState.COMPLETED_INACTIVE:
            if (
                job.job_kind is M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL
                and self.archive_reason is M5TerminalReason.CHUNK_INACTIVE
            ):
                raise ValidationError("forward root has no applicable chunk activity")
            if (
                job.job_kind is M5JobKind.REVERSE_REQUIREMENT_DISCOVERY
                and self.archive_reason is M5TerminalReason.SUBJECT_INACTIVE
            ):
                raise ValidationError("reverse root has no applicable subject activity")


@dataclass(frozen=True, slots=True)
class M5JobAttempt:
    attempt_id: str
    logical_job_id: str
    attempt_ordinal: int
    execution_spec_hash: str
    lease_token_hash: str
    lease_expires_at: datetime | None = None
    attempt_work_digest: str | None = None

    def __post_init__(self) -> None:
        _require_hash("logical_job_id", self.logical_job_id)
        _require_int("attempt_ordinal", self.attempt_ordinal, positive=True)
        _require_hash("execution_spec_hash", self.execution_spec_hash)
        _require_hash("lease_token_hash", self.lease_token_hash)
        operational_values = (self.lease_expires_at, self.attempt_work_digest)
        if all(value is None for value in operational_values):
            pass
        elif any(value is None for value in operational_values):
            raise ValidationError(
                "attempt operational lease fields must be jointly present or absent"
            )
        else:
            assert self.lease_expires_at is not None
            assert self.attempt_work_digest is not None
            _require_timestamptz("lease_expires_at", self.lease_expires_at)
            _require_hash("attempt_work_digest", self.attempt_work_digest)
        _require_identity(
            "attempt_id",
            self.attempt_id,
            digests.job_attempt_id(
                self.logical_job_id, self.attempt_ordinal, self.execution_spec_hash
            ),
        )

    @classmethod
    def build(
        cls,
        *,
        logical_job_id: str,
        attempt_ordinal: int,
        execution_spec_hash: str,
        lease_token_hash: str,
        lease_expires_at: datetime | None = None,
        attempt_work_digest: str | None = None,
    ) -> M5JobAttempt:
        return cls(
            digests.job_attempt_id(
                logical_job_id, attempt_ordinal, execution_spec_hash
            ),
            logical_job_id,
            attempt_ordinal,
            execution_spec_hash,
            lease_token_hash,
            lease_expires_at,
            attempt_work_digest,
        )

    @property
    def has_operational_lease(self) -> bool:
        return self.lease_expires_at is not None

    def validate_previous(self, previous: M5JobAttempt | None) -> None:
        expected = 1 if previous is None else previous.attempt_ordinal + 1
        if self.attempt_ordinal != expected:
            raise ValidationError("attempt ordinal is not dense for its job")
        if previous is not None and previous.logical_job_id != self.logical_job_id:
            raise ValidationError("previous attempt belongs to another job")


@dataclass(frozen=True, slots=True)
class M5AttemptOutput:
    attempt_id: str
    logical_job_id: str
    job_epoch_id: int
    payload_hash: str
    execution_spec_hash: str
    result_artifact_id: str
    result_artifact_hash: str
    attempt_output_digest: str

    def __post_init__(self) -> None:
        for name in (
            "attempt_id",
            "logical_job_id",
            "payload_hash",
            "execution_spec_hash",
            "result_artifact_id",
            "result_artifact_hash",
        ):
            _require_hash(name, getattr(self, name))
        _require_int("job_epoch_id", self.job_epoch_id, positive=True)
        _require_identity(
            "attempt_output_digest",
            self.attempt_output_digest,
            digests.attempt_output_digest(
                attempt_id=self.attempt_id,
                logical_job_id_value=self.logical_job_id,
                job_epoch_id=self.job_epoch_id,
                payload_hash=self.payload_hash,
                execution_spec_hash=self.execution_spec_hash,
                result_artifact_id=self.result_artifact_id,
                result_artifact_hash=self.result_artifact_hash,
            ),
        )

    @classmethod
    def build(
        cls,
        *,
        attempt: M5JobAttempt,
        job_epoch_id: int,
        payload_hash: str,
        result_artifact_id: str,
        result_artifact_hash: str,
    ) -> M5AttemptOutput:
        digest = digests.attempt_output_digest(
            attempt_id=attempt.attempt_id,
            logical_job_id_value=attempt.logical_job_id,
            job_epoch_id=job_epoch_id,
            payload_hash=payload_hash,
            execution_spec_hash=attempt.execution_spec_hash,
            result_artifact_id=result_artifact_id,
            result_artifact_hash=result_artifact_hash,
        )
        return cls(
            attempt.attempt_id,
            attempt.logical_job_id,
            job_epoch_id,
            payload_hash,
            attempt.execution_spec_hash,
            result_artifact_id,
            result_artifact_hash,
            digest,
        )


def _activity_archive_reason(
    *,
    epoch_active: bool,
    requirement_active: bool | None,
    group_active: bool | None,
    chunk_active: bool | None,
    job_already_terminal: bool,
) -> M5AttemptArchiveReason | None:
    if not epoch_active:
        return M5AttemptArchiveReason.EPOCH_FAILED
    if requirement_active is False or group_active is False:
        return M5AttemptArchiveReason.SUBJECT_INACTIVE
    if chunk_active is False:
        return M5AttemptArchiveReason.CHUNK_INACTIVE
    if job_already_terminal:
        return M5AttemptArchiveReason.JOB_ALREADY_TERMINAL
    return None


@dataclass(frozen=True, slots=True)
class M5AttemptResultArtifact:
    attempt_result_artifact_id: str
    attempt_result_artifact_hash: str
    attempt_output_digest: str
    attempt_id: str
    logical_job_id: str
    job_epoch_id: int
    job_state_at_receipt: M5JobState
    job_state_after: M5JobState
    disposition: M5AttemptDisposition
    activity_snapshot_epoch_id: int
    activity_snapshot_revision: int
    epoch_active: bool
    chunk_active: bool | None
    requirement_active: bool | None
    group_active: bool | None
    archive_reason: M5AttemptArchiveReason | None
    cancelled_by_event_id: str | None
    cancelled_by_epoch_id: int | None
    cancellation_reason: M5TerminalReason | None

    def __post_init__(self) -> None:
        for name in ("attempt_output_digest", "attempt_id", "logical_job_id"):
            _require_hash(name, getattr(self, name))
        _require_int("job_epoch_id", self.job_epoch_id, positive=True)
        _require_int(
            "activity_snapshot_epoch_id",
            self.activity_snapshot_epoch_id,
            positive=True,
        )
        _require_int("activity_snapshot_revision", self.activity_snapshot_revision)
        _require_bool("epoch_active", self.epoch_active)
        for name in ("chunk_active", "requirement_active", "group_active"):
            value = getattr(self, name)
            if value is not None:
                _require_bool(name, value)
        if not isinstance(self.job_state_at_receipt, M5JobState) or not isinstance(
            self.job_state_after, M5JobState
        ):
            raise ValidationError("attempt result states must be M5JobState values")
        if not isinstance(self.disposition, M5AttemptDisposition):
            raise ValidationError("attempt result disposition is invalid")
        cancellation_values = (
            self.cancelled_by_event_id,
            self.cancelled_by_epoch_id,
            self.cancellation_reason,
        )
        if self.job_state_at_receipt is M5JobState.CANCELLED:
            if any(value is None for value in cancellation_values):
                raise ValidationError("cancelled receipt requires full attribution")
            assert self.cancelled_by_event_id is not None
            assert self.cancelled_by_epoch_id is not None
            assert self.cancellation_reason is not None
            _require_text("cancelled_by_event_id", self.cancelled_by_event_id)
            _require_int(
                "cancelled_by_epoch_id", self.cancelled_by_epoch_id, positive=True
            )
            if self.cancellation_reason not in {
                M5TerminalReason.SUBJECT_INACTIVE,
                M5TerminalReason.SCOPE_RETIRED,
                M5TerminalReason.EPOCH_FAILED,
            }:
                raise ValidationError("cancellation attribution reason is invalid")
        elif any(value is not None for value in cancellation_values):
            raise ValidationError(
                "cancellation attribution is present for a non-cancelled job"
            )

        expected_reason = _activity_archive_reason(
            epoch_active=self.epoch_active,
            requirement_active=self.requirement_active,
            group_active=self.group_active,
            chunk_active=self.chunk_active,
            job_already_terminal=self.job_state_at_receipt.terminal,
        )
        if self.disposition is M5AttemptDisposition.ROOT_RESULT_STAGED:
            if (
                self.job_state_at_receipt is not M5JobState.RUNNING
                or self.job_state_after is not M5JobState.RUNNING
                or self.archive_reason is not expected_reason
            ):
                raise ValidationError("root-result staged transition/reason is invalid")
        elif self.disposition is M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE:
            if (
                self.job_state_at_receipt is not M5JobState.RUNNING
                or self.job_state_after is not M5JobState.COMPLETED_ACTIVE
                or self.archive_reason is not None
                or expected_reason is not None
            ):
                raise ValidationError(
                    "active verifier completion transition is invalid"
                )
        elif self.disposition is M5AttemptDisposition.VERIFIER_COMPLETED_INACTIVE:
            if (
                self.job_state_at_receipt is not M5JobState.RUNNING
                or self.job_state_after is not M5JobState.COMPLETED_INACTIVE
                or expected_reason
                not in {
                    M5AttemptArchiveReason.EPOCH_FAILED,
                    M5AttemptArchiveReason.SUBJECT_INACTIVE,
                    M5AttemptArchiveReason.CHUNK_INACTIVE,
                }
                or self.archive_reason is not expected_reason
            ):
                raise ValidationError(
                    "inactive verifier completion transition is invalid"
                )
        elif self.archive_reason is M5AttemptArchiveReason.ATTEMPT_EXPIRED:
            if (
                self.disposition is not M5AttemptDisposition.TERMINAL_AUDIT_ONLY
                or self.job_state_at_receipt is not M5JobState.RUNNING
                or self.job_state_after is not M5JobState.RUNNING
            ):
                raise ValidationError(
                    "expired-attempt audit transition must remain running"
                )
        elif (
            not self.job_state_at_receipt.terminal
            or self.job_state_after is not self.job_state_at_receipt
            or self.archive_reason is not expected_reason
        ):
            raise ValidationError("terminal audit-only transition/reason is invalid")

        _require_identity(
            "attempt_result_artifact_hash",
            self.attempt_result_artifact_hash,
            self.expected_artifact_hash,
        )
        _require_identity(
            "attempt_result_artifact_id",
            self.attempt_result_artifact_id,
            digests.attempt_result_artifact_id(
                self.attempt_id, self.attempt_result_artifact_hash
            ),
        )

    @property
    def digest_values(self) -> digests.AttemptResultValues:
        return (
            self.attempt_id,
            self.logical_job_id,
            self.job_epoch_id,
            self.job_state_at_receipt,
            self.job_state_after,
            self.disposition,
            self.activity_snapshot_epoch_id,
            self.activity_snapshot_revision,
            self.epoch_active,
            self.chunk_active,
            self.requirement_active,
            self.group_active,
            self.archive_reason,
            self.cancelled_by_event_id,
            self.cancelled_by_epoch_id,
            self.cancellation_reason,
        )

    @property
    def expected_artifact_hash(self) -> str:
        return digests.attempt_result_artifact_digest(
            self.attempt_output_digest, self.digest_values
        )

    def validate_job_shape(self, job_kind: M5JobKind) -> None:
        actual = (
            self.chunk_active is not None,
            self.requirement_active is not None,
            self.group_active is not None,
        )
        expected = {
            M5JobKind.REVERSE_REQUIREMENT_DISCOVERY: (True, False, False),
            M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL: (False, True, True),
            M5JobKind.VERIFY_REQUIREMENT_PAIR: (True, True, True),
        }[job_kind]
        if actual != expected:
            raise ValidationError("attempt activity fields conflict with job shape")

    @classmethod
    def build(
        cls,
        *,
        attempt_output: M5AttemptOutput,
        job_state_at_receipt: M5JobState,
        job_state_after: M5JobState,
        disposition: M5AttemptDisposition,
        activity_snapshot_epoch_id: int,
        activity_snapshot_revision: int,
        epoch_active: bool,
        chunk_active: bool | None,
        requirement_active: bool | None,
        group_active: bool | None,
        archive_reason: M5AttemptArchiveReason | None,
        cancelled_by_event_id: str | None = None,
        cancelled_by_epoch_id: int | None = None,
        cancellation_reason: M5TerminalReason | None = None,
    ) -> M5AttemptResultArtifact:
        values: digests.AttemptResultValues = (
            attempt_output.attempt_id,
            attempt_output.logical_job_id,
            attempt_output.job_epoch_id,
            job_state_at_receipt,
            job_state_after,
            disposition,
            activity_snapshot_epoch_id,
            activity_snapshot_revision,
            epoch_active,
            chunk_active,
            requirement_active,
            group_active,
            archive_reason,
            cancelled_by_event_id,
            cancelled_by_epoch_id,
            cancellation_reason,
        )
        artifact_hash = digests.attempt_result_artifact_digest(
            attempt_output.attempt_output_digest, values
        )
        return cls(
            attempt_result_artifact_id=digests.attempt_result_artifact_id(
                attempt_output.attempt_id, artifact_hash
            ),
            attempt_result_artifact_hash=artifact_hash,
            attempt_output_digest=attempt_output.attempt_output_digest,
            attempt_id=attempt_output.attempt_id,
            logical_job_id=attempt_output.logical_job_id,
            job_epoch_id=attempt_output.job_epoch_id,
            job_state_at_receipt=job_state_at_receipt,
            job_state_after=job_state_after,
            disposition=disposition,
            activity_snapshot_epoch_id=activity_snapshot_epoch_id,
            activity_snapshot_revision=activity_snapshot_revision,
            epoch_active=epoch_active,
            chunk_active=chunk_active,
            requirement_active=requirement_active,
            group_active=group_active,
            archive_reason=archive_reason,
            cancelled_by_event_id=cancelled_by_event_id,
            cancelled_by_epoch_id=cancelled_by_epoch_id,
            cancellation_reason=cancellation_reason,
        )


@dataclass(frozen=True, slots=True)
class M5ActivationRequest:
    activation_id: str
    expected_mode_revision: int
    expected_base_m4_epoch_id: int
    expected_m4_publication_id: str
    core_schema_bundle_sha256: str
    bootstrap_state_hash: str
    payload_hash: str

    def __post_init__(self) -> None:
        _require_text("activation_id", self.activation_id)
        _require_int("expected_mode_revision", self.expected_mode_revision)
        _require_int(
            "expected_base_m4_epoch_id",
            self.expected_base_m4_epoch_id,
            positive=True,
        )
        expected_publication = stable_m4_digest(
            "m4-publication-v1", str(self.expected_base_m4_epoch_id)
        )
        if self.expected_m4_publication_id != expected_publication:
            raise ValidationError(
                "activation M4 publication ID does not match base epoch"
            )
        _require_hash("core_schema_bundle_sha256", self.core_schema_bundle_sha256)
        _require_hash("bootstrap_state_hash", self.bootstrap_state_hash)
        _require_identity(
            "payload_hash",
            self.payload_hash,
            digests.activation_request_digest(
                activation_id=self.activation_id,
                expected_mode_revision=self.expected_mode_revision,
                expected_base_m4_epoch_id=self.expected_base_m4_epoch_id,
                expected_m4_publication_id=self.expected_m4_publication_id,
                core_schema_bundle_sha256=self.core_schema_bundle_sha256,
                bootstrap_state_hash=self.bootstrap_state_hash,
            ),
        )

    @classmethod
    def build(
        cls,
        *,
        activation_id: str,
        expected_mode_revision: int,
        expected_base_m4_epoch_id: int,
        core_schema_bundle_sha256: str,
        bootstrap_state_hash: str,
    ) -> M5ActivationRequest:
        publication = stable_m4_digest(
            "m4-publication-v1", str(expected_base_m4_epoch_id)
        )
        payload = digests.activation_request_digest(
            activation_id=activation_id,
            expected_mode_revision=expected_mode_revision,
            expected_base_m4_epoch_id=expected_base_m4_epoch_id,
            expected_m4_publication_id=publication,
            core_schema_bundle_sha256=core_schema_bundle_sha256,
            bootstrap_state_hash=bootstrap_state_hash,
        )
        return cls(
            activation_id,
            expected_mode_revision,
            expected_base_m4_epoch_id,
            publication,
            core_schema_bundle_sha256,
            bootstrap_state_hash,
            payload,
        )


@dataclass(frozen=True, slots=True)
class M5ActivationReceipt:
    activation_id: str
    payload_hash: str
    base_m4_epoch_id: int
    m4_publication_id: str
    m5_publication_epoch_id: int
    mode_revision: int
    bootstrap_state_hash: str
    receipt_hash: str
    replayed: bool

    def __post_init__(self) -> None:
        _require_text("activation_id", self.activation_id)
        _require_hash("payload_hash", self.payload_hash)
        _require_int("base_m4_epoch_id", self.base_m4_epoch_id, positive=True)
        _require_text("m4_publication_id", self.m4_publication_id)
        _require_int(
            "m5_publication_epoch_id", self.m5_publication_epoch_id, positive=True
        )
        _require_int("mode_revision", self.mode_revision, positive=True)
        if self.m5_publication_epoch_id != self.base_m4_epoch_id:
            raise ValidationError("activation must not create a synthetic epoch")
        if self.m4_publication_id != stable_m4_digest(
            "m4-publication-v1", str(self.base_m4_epoch_id)
        ):
            raise ValidationError("activation receipt M4 publication ID drift")
        _require_hash("bootstrap_state_hash", self.bootstrap_state_hash)
        _require_bool("replayed", self.replayed)
        _require_identity(
            "receipt_hash",
            self.receipt_hash,
            digests.activation_receipt_digest(
                activation_id=self.activation_id,
                payload_hash=self.payload_hash,
                base_m4_epoch_id=self.base_m4_epoch_id,
                m4_publication_id=self.m4_publication_id,
                m5_publication_epoch_id=self.m5_publication_epoch_id,
                mode_revision=self.mode_revision,
                bootstrap_state_hash=self.bootstrap_state_hash,
            ),
        )

    @classmethod
    def build(
        cls, request: M5ActivationRequest, *, replayed: bool = False
    ) -> M5ActivationReceipt:
        mode_revision = request.expected_mode_revision + 1
        digest = digests.activation_receipt_digest(
            activation_id=request.activation_id,
            payload_hash=request.payload_hash,
            base_m4_epoch_id=request.expected_base_m4_epoch_id,
            m4_publication_id=request.expected_m4_publication_id,
            m5_publication_epoch_id=request.expected_base_m4_epoch_id,
            mode_revision=mode_revision,
            bootstrap_state_hash=request.bootstrap_state_hash,
        )
        return cls(
            request.activation_id,
            request.payload_hash,
            request.expected_base_m4_epoch_id,
            request.expected_m4_publication_id,
            request.expected_base_m4_epoch_id,
            mode_revision,
            request.bootstrap_state_hash,
            digest,
            replayed,
        )

    def validate_request(self, request: M5ActivationRequest) -> None:
        if (
            self.activation_id != request.activation_id
            or self.payload_hash != request.payload_hash
            or self.base_m4_epoch_id != request.expected_base_m4_epoch_id
            or self.mode_revision != request.expected_mode_revision + 1
            or self.bootstrap_state_hash != request.bootstrap_state_hash
        ):
            raise ValidationError("activation receipt disagrees with request")


@dataclass(frozen=True, slots=True)
class M5ChangedStateReference:
    kind: M5StateReferenceKind
    object_id: str
    epoch_id: int
    revision: int
    state_artifact_hash: str
    reference_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.kind, M5StateReferenceKind):
            raise ValidationError("changed-state kind must be an M5StateReferenceKind")
        _require_text("object_id", self.object_id)
        _require_int("epoch_id", self.epoch_id, positive=True)
        _require_int("revision", self.revision)
        _require_hash("state_artifact_hash", self.state_artifact_hash)
        _require_identity(
            "reference_digest",
            self.reference_digest,
            digests.changed_state_reference_digest(self.digest_values),
        )

    @property
    def digest_values(self) -> digests.ChangedStateReferenceValues:
        return (
            self.kind,
            self.object_id,
            self.epoch_id,
            self.revision,
            self.state_artifact_hash,
        )

    @classmethod
    def build(
        cls,
        *,
        kind: M5StateReferenceKind,
        object_id: str,
        epoch_id: int,
        revision: int,
        state_artifact_hash: str,
    ) -> M5ChangedStateReference:
        values: digests.ChangedStateReferenceValues = (
            kind,
            object_id,
            epoch_id,
            revision,
            state_artifact_hash,
        )
        return cls(
            kind,
            object_id,
            epoch_id,
            revision,
            state_artifact_hash,
            digests.changed_state_reference_digest(values),
        )


def validate_changed_state_references(
    references: tuple[M5ChangedStateReference, ...],
) -> None:
    _require_tuple("changed state references", references)
    if references != tuple(
        sorted(
            references,
            key=lambda item: (
                item.kind.value,
                item.object_id,
                item.reference_digest,
            ),
        )
    ):
        raise ValidationError("changed-state references are not canonically sorted")
    object_keys = tuple((item.kind, item.object_id) for item in references)
    if len(set(object_keys)) != len(object_keys):
        raise ValidationError("changed-state set repeats a kind/object key")


@dataclass(frozen=True, slots=True)
class M5RuntimeWork:
    deactivated_chunk_count: int = 0
    withdrawn_candidate_edge_count: int = 0
    withdrawn_current_observation_count: int = 0
    direct_discovery_call_count: int = 0
    direct_verifier_call_count: int = 0
    direct_observation_artifact_count: int = 0
    direct_effective_observation_count: int = 0
    direct_inactive_completion_count: int = 0
    requirement_forward_retrieval_call_count: int = 0
    requirement_reverse_retrieval_call_count: int = 0
    requirement_fallback_forward_call_count: int = 0
    requirement_verifier_call_count: int = 0
    requirement_observation_artifact_count: int = 0
    requirement_effective_observation_count: int = 0
    requirement_inactive_completion_count: int = 0
    requirement_cancelled_job_count: int = 0
    requirement_late_attempt_artifact_count: int = 0
    requirement_channel_hit_count: int = 0
    requirement_pre_dedup_selection_count: int = 0
    requirement_admitted_pair_count: int = 0
    group_state_write_count: int = 0
    claim_state_write_count: int = 0
    answer_state_write_count: int = 0
    certificate_binding_write_count: int = 0
    public_delta_count: int = 0
    bytes_hashed: int = 0
    bytes_serialized: int = 0
    embedding_model_call_count: int = 0
    verifier_model_call_count: int = 0
    embedding_input_token_count: int = 0
    verifier_input_token_count: int = 0
    verifier_output_token_count: int = 0
    work_digest: str = ""

    def __post_init__(self) -> None:
        for name, value in zip(
            self.counter_names(), self.counter_values(), strict=True
        ):
            _require_int(name, value)
        expected = digests.runtime_work_digest(self.counter_values())
        if not self.work_digest:
            object.__setattr__(self, "work_digest", expected)
        else:
            _require_identity("work_digest", self.work_digest, expected)

    @classmethod
    def counter_names(cls) -> tuple[str, ...]:
        return tuple(field.name for field in fields(cls) if field.name != "work_digest")

    def counter_values(self) -> tuple[int, ...]:
        return tuple(getattr(self, name) for name in self.counter_names())

    @property
    def is_zero(self) -> bool:
        return all(value == 0 for value in self.counter_values())


_AMBIGUOUS_CALL_COUNTERS = (
    "direct_discovery_call_count",
    "direct_verifier_call_count",
    "requirement_forward_retrieval_call_count",
    "requirement_reverse_retrieval_call_count",
    "requirement_fallback_forward_call_count",
    "requirement_verifier_call_count",
    "embedding_model_call_count",
    "verifier_model_call_count",
)
_MODEL_TOKEN_COUNTERS = (
    "embedding_input_token_count",
    "verifier_input_token_count",
    "verifier_output_token_count",
)
_EXTERNAL_ATTEMPT_BYTE_COUNTERS = ("bytes_hashed", "bytes_serialized")


def _runtime_work(**counters: int) -> M5RuntimeWork:
    unknown = set(counters).difference(M5RuntimeWork.counter_names())
    if unknown:
        raise ValidationError(f"unknown runtime-work counters: {sorted(unknown)!r}")
    return M5RuntimeWork(
        deactivated_chunk_count=counters.get("deactivated_chunk_count", 0),
        withdrawn_candidate_edge_count=counters.get(
            "withdrawn_candidate_edge_count", 0
        ),
        withdrawn_current_observation_count=counters.get(
            "withdrawn_current_observation_count", 0
        ),
        direct_discovery_call_count=counters.get("direct_discovery_call_count", 0),
        direct_verifier_call_count=counters.get("direct_verifier_call_count", 0),
        direct_observation_artifact_count=counters.get(
            "direct_observation_artifact_count", 0
        ),
        direct_effective_observation_count=counters.get(
            "direct_effective_observation_count", 0
        ),
        direct_inactive_completion_count=counters.get(
            "direct_inactive_completion_count", 0
        ),
        requirement_forward_retrieval_call_count=counters.get(
            "requirement_forward_retrieval_call_count", 0
        ),
        requirement_reverse_retrieval_call_count=counters.get(
            "requirement_reverse_retrieval_call_count", 0
        ),
        requirement_fallback_forward_call_count=counters.get(
            "requirement_fallback_forward_call_count", 0
        ),
        requirement_verifier_call_count=counters.get(
            "requirement_verifier_call_count", 0
        ),
        requirement_observation_artifact_count=counters.get(
            "requirement_observation_artifact_count", 0
        ),
        requirement_effective_observation_count=counters.get(
            "requirement_effective_observation_count", 0
        ),
        requirement_inactive_completion_count=counters.get(
            "requirement_inactive_completion_count", 0
        ),
        requirement_cancelled_job_count=counters.get(
            "requirement_cancelled_job_count", 0
        ),
        requirement_late_attempt_artifact_count=counters.get(
            "requirement_late_attempt_artifact_count", 0
        ),
        requirement_channel_hit_count=counters.get("requirement_channel_hit_count", 0),
        requirement_pre_dedup_selection_count=counters.get(
            "requirement_pre_dedup_selection_count", 0
        ),
        requirement_admitted_pair_count=counters.get(
            "requirement_admitted_pair_count", 0
        ),
        group_state_write_count=counters.get("group_state_write_count", 0),
        claim_state_write_count=counters.get("claim_state_write_count", 0),
        answer_state_write_count=counters.get("answer_state_write_count", 0),
        certificate_binding_write_count=counters.get(
            "certificate_binding_write_count", 0
        ),
        public_delta_count=counters.get("public_delta_count", 0),
        bytes_hashed=counters.get("bytes_hashed", 0),
        bytes_serialized=counters.get("bytes_serialized", 0),
        embedding_model_call_count=counters.get("embedding_model_call_count", 0),
        verifier_model_call_count=counters.get("verifier_model_call_count", 0),
        embedding_input_token_count=counters.get("embedding_input_token_count", 0),
        verifier_input_token_count=counters.get("verifier_input_token_count", 0),
        verifier_output_token_count=counters.get("verifier_output_token_count", 0),
    )


def _maximum_ambiguous_call_work(
    *,
    subgraph: M5RuntimeSubgraph,
    job_kind: str,
    fallback_required: bool,
) -> M5RuntimeWork:
    if subgraph is M5RuntimeSubgraph.DIRECT:
        if fallback_required:
            raise ValidationError("typed-direct dispatch cannot require fallback")
        if job_kind in {
            M4JobKind.IMPACT_DISCOVERY.value,
            M4JobKind.FRONTIER_RETRIEVE.value,
        }:
            return _runtime_work(
                direct_discovery_call_count=1,
                embedding_model_call_count=1,
            )
        if job_kind == M4JobKind.VERIFY_PAIR.value:
            return _runtime_work(
                direct_verifier_call_count=1,
                verifier_model_call_count=1,
            )
    elif job_kind == M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL.value:
        return _runtime_work(
            requirement_forward_retrieval_call_count=1,
            requirement_fallback_forward_call_count=int(fallback_required),
            embedding_model_call_count=1,
        )
    elif job_kind == M5JobKind.REVERSE_REQUIREMENT_DISCOVERY.value:
        if fallback_required:
            raise ValidationError(
                "reverse requirement dispatch cannot require fallback"
            )
        return _runtime_work(
            requirement_reverse_retrieval_call_count=1,
            embedding_model_call_count=1,
        )
    elif job_kind == M5JobKind.VERIFY_REQUIREMENT_PAIR.value:
        if fallback_required:
            raise ValidationError("requirement verifier cannot require fallback")
        return _runtime_work(
            requirement_verifier_call_count=1,
            verifier_model_call_count=1,
        )
    raise ValidationError("dispatch subgraph and job kind are incompatible")


@dataclass(frozen=True, slots=True)
class M5RuntimeOperationalConfig:
    lease_duration_ms: int
    config_digest: str

    def __post_init__(self) -> None:
        _require_int("lease_duration_ms", self.lease_duration_ms, positive=True)
        if self.lease_duration_ms > 86_400_000:
            raise ValidationError("lease_duration_ms must not exceed 86400000")
        _require_identity(
            "config_digest",
            self.config_digest,
            digests.runtime_operational_config_digest(self.lease_duration_ms),
        )

    @classmethod
    def build(cls, lease_duration_ms: int) -> M5RuntimeOperationalConfig:
        return cls(
            lease_duration_ms,
            digests.runtime_operational_config_digest(lease_duration_ms),
        )


@dataclass(frozen=True, slots=True)
class M5DispatchRecord:
    epoch_id: int
    subgraph: M5RuntimeSubgraph
    attempt_id: str
    logical_job_id: str
    attempt_ordinal: int
    job_kind: str
    fallback_required: bool
    dispatched_revision: int
    lease_expires_at: datetime
    maximum_ambiguous_call_work: M5RuntimeWork
    record_digest: str

    def __post_init__(self) -> None:
        _require_int("epoch_id", self.epoch_id, positive=True)
        if not isinstance(self.subgraph, M5RuntimeSubgraph):
            raise ValidationError("subgraph must be an M5RuntimeSubgraph")
        _require_text("attempt_id", self.attempt_id)
        _require_text("logical_job_id", self.logical_job_id)
        if self.subgraph is M5RuntimeSubgraph.REQUIREMENT:
            _require_hash("requirement attempt_id", self.attempt_id)
            _require_hash("requirement logical_job_id", self.logical_job_id)
        _require_int("attempt_ordinal", self.attempt_ordinal, positive=True)
        _require_text("job_kind", self.job_kind)
        _require_bool("fallback_required", self.fallback_required)
        _require_int("dispatched_revision", self.dispatched_revision, positive=True)
        _require_timestamptz("lease_expires_at", self.lease_expires_at)
        if not isinstance(self.maximum_ambiguous_call_work, M5RuntimeWork):
            raise ValidationError("maximum_ambiguous_call_work must be M5RuntimeWork")
        expected_maximum = _maximum_ambiguous_call_work(
            subgraph=self.subgraph,
            job_kind=self.job_kind,
            fallback_required=self.fallback_required,
        )
        if self.maximum_ambiguous_call_work != expected_maximum:
            raise ValidationError(
                "dispatch maximum ambiguous work differs from its exact job vector"
            )
        _require_identity(
            "record_digest",
            self.record_digest,
            digests.dispatch_record_digest(
                epoch_id=self.epoch_id,
                subgraph=self.subgraph,
                attempt_id=self.attempt_id,
                logical_job_id=self.logical_job_id,
                attempt_ordinal=self.attempt_ordinal,
                job_kind=self.job_kind,
                fallback_required=self.fallback_required,
                dispatched_revision=self.dispatched_revision,
                maximum_ambiguous_call_work_digest=(
                    self.maximum_ambiguous_call_work.work_digest
                ),
            ),
        )

    @classmethod
    def build(
        cls,
        *,
        epoch_id: int,
        subgraph: M5RuntimeSubgraph,
        attempt_id: str,
        logical_job_id: str,
        attempt_ordinal: int,
        job_kind: str,
        fallback_required: bool,
        dispatched_revision: int,
        lease_expires_at: datetime,
    ) -> M5DispatchRecord:
        maximum = _maximum_ambiguous_call_work(
            subgraph=subgraph,
            job_kind=job_kind,
            fallback_required=fallback_required,
        )
        return cls(
            epoch_id=epoch_id,
            subgraph=subgraph,
            attempt_id=attempt_id,
            logical_job_id=logical_job_id,
            attempt_ordinal=attempt_ordinal,
            job_kind=job_kind,
            fallback_required=fallback_required,
            dispatched_revision=dispatched_revision,
            lease_expires_at=lease_expires_at,
            maximum_ambiguous_call_work=maximum,
            record_digest=digests.dispatch_record_digest(
                epoch_id=epoch_id,
                subgraph=subgraph,
                attempt_id=attempt_id,
                logical_job_id=logical_job_id,
                attempt_ordinal=attempt_ordinal,
                job_kind=job_kind,
                fallback_required=fallback_required,
                dispatched_revision=dispatched_revision,
                maximum_ambiguous_call_work_digest=maximum.work_digest,
            ),
        )


@dataclass(frozen=True, slots=True)
class M5RequirementRootProvenance:
    epoch_id: int
    root_job_id: str
    fallback_required: bool
    provenance_digest: str

    def __post_init__(self) -> None:
        _require_int("epoch_id", self.epoch_id, positive=True)
        _require_hash("root_job_id", self.root_job_id)
        _require_bool("fallback_required", self.fallback_required)
        _require_identity(
            "provenance_digest",
            self.provenance_digest,
            digests.requirement_root_provenance_digest(
                epoch_id=self.epoch_id,
                root_job_id=self.root_job_id,
                fallback_required=self.fallback_required,
            ),
        )

    @classmethod
    def build(
        cls, *, epoch_id: int, root_job_id: str, fallback_required: bool
    ) -> M5RequirementRootProvenance:
        return cls(
            epoch_id,
            root_job_id,
            fallback_required,
            digests.requirement_root_provenance_digest(
                epoch_id=epoch_id,
                root_job_id=root_job_id,
                fallback_required=fallback_required,
            ),
        )


@dataclass(frozen=True, slots=True)
class M5AttemptExecutionEvidence:
    epoch_id: int
    subgraph: M5RuntimeSubgraph
    attempt_id: str
    disposition: M5ExecutionEvidenceDisposition
    result_or_error_hash: str
    attempt_work: M5RuntimeWork
    attempt_timing_digest: str
    evidence_digest: str

    def __post_init__(self) -> None:
        _require_int("epoch_id", self.epoch_id, positive=True)
        if not isinstance(self.subgraph, M5RuntimeSubgraph):
            raise ValidationError("subgraph must be an M5RuntimeSubgraph")
        _require_text("attempt_id", self.attempt_id)
        if self.subgraph is M5RuntimeSubgraph.REQUIREMENT:
            _require_hash("requirement attempt_id", self.attempt_id)
        if not isinstance(self.disposition, M5ExecutionEvidenceDisposition):
            raise ValidationError(
                "disposition must be an M5ExecutionEvidenceDisposition"
            )
        _require_hash("result_or_error_hash", self.result_or_error_hash)
        if not isinstance(self.attempt_work, M5RuntimeWork):
            raise ValidationError("attempt_work must be M5RuntimeWork")
        _require_hash("attempt_timing_digest", self.attempt_timing_digest)
        if self.disposition is M5ExecutionEvidenceDisposition.REUSED_ARTIFACT:
            for name in (*_AMBIGUOUS_CALL_COUNTERS, *_MODEL_TOKEN_COUNTERS):
                if getattr(self.attempt_work, name) != 0:
                    raise ValidationError(
                        "reused-artifact evidence cannot charge calls or tokens"
                    )
        _require_identity(
            "evidence_digest",
            self.evidence_digest,
            digests.attempt_execution_evidence_digest(
                epoch_id=self.epoch_id,
                subgraph=self.subgraph,
                attempt_id=self.attempt_id,
                disposition=self.disposition,
                result_or_error_hash=self.result_or_error_hash,
                attempt_work_digest=self.attempt_work.work_digest,
                attempt_timing_digest=self.attempt_timing_digest,
            ),
        )

    @classmethod
    def build(
        cls,
        *,
        epoch_id: int,
        subgraph: M5RuntimeSubgraph,
        attempt_id: str,
        disposition: M5ExecutionEvidenceDisposition,
        result_or_error_hash: str,
        attempt_work: M5RuntimeWork,
        attempt_timing_digest: str,
    ) -> M5AttemptExecutionEvidence:
        return cls(
            epoch_id=epoch_id,
            subgraph=subgraph,
            attempt_id=attempt_id,
            disposition=disposition,
            result_or_error_hash=result_or_error_hash,
            attempt_work=attempt_work,
            attempt_timing_digest=attempt_timing_digest,
            evidence_digest=digests.attempt_execution_evidence_digest(
                epoch_id=epoch_id,
                subgraph=subgraph,
                attempt_id=attempt_id,
                disposition=disposition,
                result_or_error_hash=result_or_error_hash,
                attempt_work_digest=attempt_work.work_digest,
                attempt_timing_digest=attempt_timing_digest,
            ),
        )

    def validate_dispatch(self, dispatch: M5DispatchRecord) -> None:
        if (
            dispatch.epoch_id != self.epoch_id
            or dispatch.subgraph is not self.subgraph
            or dispatch.attempt_id != self.attempt_id
        ):
            raise ValidationError("execution evidence belongs to another dispatch")
        allowed = set(_EXTERNAL_ATTEMPT_BYTE_COUNTERS)
        if dispatch.subgraph is M5RuntimeSubgraph.DIRECT:
            if dispatch.job_kind in {
                M4JobKind.IMPACT_DISCOVERY.value,
                M4JobKind.FRONTIER_RETRIEVE.value,
            }:
                allowed.update(
                    {
                        "direct_discovery_call_count",
                        "embedding_model_call_count",
                        "embedding_input_token_count",
                    }
                )
            else:
                allowed.update(
                    {
                        "direct_verifier_call_count",
                        "verifier_model_call_count",
                        "verifier_input_token_count",
                        "verifier_output_token_count",
                    }
                )
        elif dispatch.job_kind in {
            M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL.value,
            M5JobKind.REVERSE_REQUIREMENT_DISCOVERY.value,
        }:
            allowed.update(
                {
                    "requirement_forward_retrieval_call_count",
                    "requirement_reverse_retrieval_call_count",
                    "requirement_fallback_forward_call_count",
                    "embedding_model_call_count",
                    "embedding_input_token_count",
                }
            )
        else:
            allowed.update(
                {
                    "requirement_verifier_call_count",
                    "verifier_model_call_count",
                    "verifier_input_token_count",
                    "verifier_output_token_count",
                }
            )
        for name in M5RuntimeWork.counter_names():
            value = getattr(self.attempt_work, name)
            if name not in allowed and value:
                raise ValidationError(
                    f"attempt_work counter {name} belongs to persistence"
                )
        for name in _AMBIGUOUS_CALL_COUNTERS:
            if getattr(self.attempt_work, name) > getattr(
                dispatch.maximum_ambiguous_call_work, name
            ):
                raise ValidationError(
                    f"attempt_work counter {name} exceeds dispatch maximum"
                )
        forward = self.attempt_work.requirement_forward_retrieval_call_count
        fallback = self.attempt_work.requirement_fallback_forward_call_count
        if dispatch.fallback_required and fallback != forward:
            raise ValidationError(
                "fallback call count must equal confirmed forward retrieval count"
            )

    def validate_timing(self, observation: M5RuntimeTimingObservation) -> None:
        expected = digests.attempt_runtime_timing_digest(
            epoch_id=self.epoch_id,
            subgraph=self.subgraph,
            attempt_id=self.attempt_id,
            observation_digest=observation.observation_digest,
        )
        _require_identity("attempt_timing_digest", self.attempt_timing_digest, expected)


@dataclass(frozen=True, slots=True)
class M5CallAmbiguityReport:
    durable_dispatch_count: int
    confirmed_execution_count: int
    unresolved_dispatch_count: int
    confirmed_call_lower: M5RuntimeWork
    possible_call_upper: M5RuntimeWork

    def __post_init__(self) -> None:
        for name in (
            "durable_dispatch_count",
            "confirmed_execution_count",
            "unresolved_dispatch_count",
        ):
            _require_int(name, getattr(self, name))
        if (
            self.durable_dispatch_count
            != self.confirmed_execution_count + self.unresolved_dispatch_count
        ):
            raise ValidationError(
                "dispatch count must equal confirmed plus unresolved counts"
            )
        for work_name in ("confirmed_call_lower", "possible_call_upper"):
            work = getattr(self, work_name)
            if not isinstance(work, M5RuntimeWork):
                raise ValidationError(f"{work_name} must be M5RuntimeWork")
            for name in M5RuntimeWork.counter_names():
                if name not in _AMBIGUOUS_CALL_COUNTERS and getattr(work, name):
                    raise ValidationError(
                        f"{work_name} may contain only frozen call counters"
                    )
        for name in _AMBIGUOUS_CALL_COUNTERS:
            if getattr(self.confirmed_call_lower, name) > getattr(
                self.possible_call_upper, name
            ):
                raise ValidationError("confirmed call lower exceeds possible upper")

    @classmethod
    def build(
        cls,
        *,
        dispatches: tuple[M5DispatchRecord, ...],
        execution_evidence: tuple[M5AttemptExecutionEvidence, ...],
    ) -> M5CallAmbiguityReport:
        _require_tuple("dispatches", dispatches)
        _require_tuple("execution_evidence", execution_evidence)
        dispatch_by_key: dict[tuple[M5RuntimeSubgraph, str], M5DispatchRecord] = {}
        for dispatch in dispatches:
            key = (dispatch.subgraph, dispatch.attempt_id)
            if key in dispatch_by_key:
                raise ValidationError("ambiguity input repeats a durable dispatch")
            dispatch_by_key[key] = dispatch
        evidence_by_key: dict[
            tuple[M5RuntimeSubgraph, str], M5AttemptExecutionEvidence
        ] = {}
        for evidence in execution_evidence:
            key = (evidence.subgraph, evidence.attempt_id)
            if key in evidence_by_key:
                raise ValidationError("ambiguity input repeats execution evidence")
            stored_dispatch = dispatch_by_key.get(key)
            if stored_dispatch is None:
                raise ValidationError("execution evidence has no durable dispatch")
            evidence.validate_dispatch(stored_dispatch)
            evidence_by_key[key] = evidence

        lower_values = {
            name: sum(
                getattr(evidence.attempt_work, name)
                for evidence in evidence_by_key.values()
            )
            for name in _AMBIGUOUS_CALL_COUNTERS
        }
        upper_values = dict(lower_values)
        for key, dispatch in dispatch_by_key.items():
            if key in evidence_by_key:
                continue
            for name in _AMBIGUOUS_CALL_COUNTERS:
                upper_values[name] += getattr(
                    dispatch.maximum_ambiguous_call_work, name
                )
        return cls(
            durable_dispatch_count=len(dispatch_by_key),
            confirmed_execution_count=len(evidence_by_key),
            unresolved_dispatch_count=(len(dispatch_by_key) - len(evidence_by_key)),
            confirmed_call_lower=_runtime_work(**lower_values),
            possible_call_upper=_runtime_work(**upper_values),
        )


@dataclass(frozen=True, slots=True)
class M5ExpiredAttemptReturn:
    subgraph: M5RuntimeSubgraph
    epoch_id: int
    attempt_id: str
    logical_job_id: str
    worker_output_digest: str
    worker_artifact_hash: str
    activity_snapshot_epoch_id: int
    activity_snapshot_revision: int
    received_after_terminal: bool
    expired_return_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.subgraph, M5RuntimeSubgraph):
            raise ValidationError("subgraph must be an M5RuntimeSubgraph")
        _require_int("epoch_id", self.epoch_id, positive=True)
        _require_text("attempt_id", self.attempt_id)
        _require_text("logical_job_id", self.logical_job_id)
        for name in ("worker_output_digest", "worker_artifact_hash"):
            _require_hash(name, getattr(self, name))
        _require_int(
            "activity_snapshot_epoch_id",
            self.activity_snapshot_epoch_id,
            positive=True,
        )
        _require_int("activity_snapshot_revision", self.activity_snapshot_revision)
        _require_bool("received_after_terminal", self.received_after_terminal)
        _require_identity(
            "expired_return_digest",
            self.expired_return_digest,
            digests.expired_attempt_return_digest(
                subgraph=self.subgraph,
                epoch_id=self.epoch_id,
                attempt_id=self.attempt_id,
                logical_job_id=self.logical_job_id,
                worker_output_digest=self.worker_output_digest,
                worker_artifact_hash=self.worker_artifact_hash,
                activity_snapshot_epoch_id=self.activity_snapshot_epoch_id,
                activity_snapshot_revision=self.activity_snapshot_revision,
                received_after_terminal=self.received_after_terminal,
            ),
        )

    @classmethod
    def build(
        cls,
        *,
        subgraph: M5RuntimeSubgraph,
        epoch_id: int,
        attempt_id: str,
        logical_job_id: str,
        worker_output_digest: str,
        worker_artifact_hash: str,
        activity_snapshot_epoch_id: int,
        activity_snapshot_revision: int,
        received_after_terminal: bool,
    ) -> M5ExpiredAttemptReturn:
        return cls(
            subgraph=subgraph,
            epoch_id=epoch_id,
            attempt_id=attempt_id,
            logical_job_id=logical_job_id,
            worker_output_digest=worker_output_digest,
            worker_artifact_hash=worker_artifact_hash,
            activity_snapshot_epoch_id=activity_snapshot_epoch_id,
            activity_snapshot_revision=activity_snapshot_revision,
            received_after_terminal=received_after_terminal,
            expired_return_digest=digests.expired_attempt_return_digest(
                subgraph=subgraph,
                epoch_id=epoch_id,
                attempt_id=attempt_id,
                logical_job_id=logical_job_id,
                worker_output_digest=worker_output_digest,
                worker_artifact_hash=worker_artifact_hash,
                activity_snapshot_epoch_id=activity_snapshot_epoch_id,
                activity_snapshot_revision=activity_snapshot_revision,
                received_after_terminal=received_after_terminal,
            ),
        )


@dataclass(frozen=True, slots=True)
class M5RuntimeTiming:
    coordinator_non_db_non_neural_ns: int = 0
    neural_wall_ns: int = 0
    postgres_roundtrip_wall_ns: int = 0
    external_io_wall_ns: int = 0
    end_to_end_wall_ns: int = 0
    postgres_server_execution_ns: int | None = None
    postgres_lock_wait_ns: int | None = None
    postgres_wal_bytes: int | None = None
    postgres_shared_block_reads: int | None = None

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            if value is not None:
                _require_int(field.name, value)


@dataclass(frozen=True, slots=True)
class M5RuntimeTimingObservation:
    required_interval_observed: bool
    timing: M5RuntimeTiming | None
    observation_digest: str

    def __post_init__(self) -> None:
        _require_bool("required_interval_observed", self.required_interval_observed)
        if self.required_interval_observed != (self.timing is not None):
            raise ValidationError(
                "required timing fields must be all observed or all missing"
            )
        if self.timing is not None and not isinstance(self.timing, M5RuntimeTiming):
            raise ValidationError("timing must be M5RuntimeTiming when observed")
        _require_identity(
            "observation_digest",
            self.observation_digest,
            self.expected_observation_digest,
        )

    @property
    def expected_observation_digest(self) -> str:
        timing = self.timing
        return digests.runtime_timing_observation_digest(
            required_interval_observed=self.required_interval_observed,
            coordinator_non_db_non_neural_ns=(
                None if timing is None else timing.coordinator_non_db_non_neural_ns
            ),
            neural_wall_ns=None if timing is None else timing.neural_wall_ns,
            postgres_roundtrip_wall_ns=(
                None if timing is None else timing.postgres_roundtrip_wall_ns
            ),
            external_io_wall_ns=(
                None if timing is None else timing.external_io_wall_ns
            ),
            end_to_end_wall_ns=(None if timing is None else timing.end_to_end_wall_ns),
            postgres_server_execution_ns=(
                None if timing is None else timing.postgres_server_execution_ns
            ),
            postgres_lock_wait_ns=(
                None if timing is None else timing.postgres_lock_wait_ns
            ),
            postgres_wal_bytes=(None if timing is None else timing.postgres_wal_bytes),
            postgres_shared_block_reads=(
                None if timing is None else timing.postgres_shared_block_reads
            ),
        )

    @classmethod
    def build(cls, timing: M5RuntimeTiming | None) -> M5RuntimeTimingObservation:
        required_interval_observed = timing is not None
        digest = digests.runtime_timing_observation_digest(
            required_interval_observed=required_interval_observed,
            coordinator_non_db_non_neural_ns=(
                None if timing is None else timing.coordinator_non_db_non_neural_ns
            ),
            neural_wall_ns=None if timing is None else timing.neural_wall_ns,
            postgres_roundtrip_wall_ns=(
                None if timing is None else timing.postgres_roundtrip_wall_ns
            ),
            external_io_wall_ns=(
                None if timing is None else timing.external_io_wall_ns
            ),
            end_to_end_wall_ns=(None if timing is None else timing.end_to_end_wall_ns),
            postgres_server_execution_ns=(
                None if timing is None else timing.postgres_server_execution_ns
            ),
            postgres_lock_wait_ns=(
                None if timing is None else timing.postgres_lock_wait_ns
            ),
            postgres_wal_bytes=(None if timing is None else timing.postgres_wal_bytes),
            postgres_shared_block_reads=(
                None if timing is None else timing.postgres_shared_block_reads
            ),
        )
        return cls(required_interval_observed, timing, digest)


@dataclass(frozen=True, slots=True)
class M5RuntimeTimingCoverage:
    required_expected_count: int
    required_observed_count: int
    required_missing_count: int
    postgres_server_execution_expected_count: int
    postgres_server_execution_observed_count: int
    postgres_server_execution_missing_count: int
    postgres_lock_wait_expected_count: int
    postgres_lock_wait_observed_count: int
    postgres_lock_wait_missing_count: int
    postgres_wal_bytes_expected_count: int
    postgres_wal_bytes_observed_count: int
    postgres_wal_bytes_missing_count: int
    postgres_shared_block_reads_expected_count: int
    postgres_shared_block_reads_observed_count: int
    postgres_shared_block_reads_missing_count: int
    terminal_client_roundtrip_included: bool

    def __post_init__(self) -> None:
        triples = (
            (
                "required",
                self.required_expected_count,
                self.required_observed_count,
                self.required_missing_count,
            ),
            (
                "postgres_server_execution",
                self.postgres_server_execution_expected_count,
                self.postgres_server_execution_observed_count,
                self.postgres_server_execution_missing_count,
            ),
            (
                "postgres_lock_wait",
                self.postgres_lock_wait_expected_count,
                self.postgres_lock_wait_observed_count,
                self.postgres_lock_wait_missing_count,
            ),
            (
                "postgres_wal_bytes",
                self.postgres_wal_bytes_expected_count,
                self.postgres_wal_bytes_observed_count,
                self.postgres_wal_bytes_missing_count,
            ),
            (
                "postgres_shared_block_reads",
                self.postgres_shared_block_reads_expected_count,
                self.postgres_shared_block_reads_observed_count,
                self.postgres_shared_block_reads_missing_count,
            ),
        )
        for name, expected, observed, missing in triples:
            _require_int(f"{name}_expected_count", expected)
            _require_int(f"{name}_observed_count", observed)
            _require_int(f"{name}_missing_count", missing)
            if expected != observed + missing:
                raise ValidationError(
                    f"{name} expected count must equal observed plus missing"
                )
            if name != "required" and expected != self.required_expected_count:
                raise ValidationError(
                    f"{name} expected count must equal required expected count"
                )
        _require_bool(
            "terminal_client_roundtrip_included",
            self.terminal_client_roundtrip_included,
        )

    @classmethod
    def single_point(
        cls,
        timing: M5RuntimeTiming | None,
        *,
        terminal_client_roundtrip_included: bool,
    ) -> M5RuntimeTimingCoverage:
        observed = int(timing is not None)
        missing = 1 - observed

        def optional_counts(value: int | None) -> tuple[int, int, int]:
            optional_observed = int(timing is not None and value is not None)
            return (1, optional_observed, 1 - optional_observed)

        server = optional_counts(
            None if timing is None else timing.postgres_server_execution_ns
        )
        lock = optional_counts(None if timing is None else timing.postgres_lock_wait_ns)
        wal = optional_counts(None if timing is None else timing.postgres_wal_bytes)
        blocks = optional_counts(
            None if timing is None else timing.postgres_shared_block_reads
        )
        return cls(
            1,
            observed,
            missing,
            *server,
            *lock,
            *wal,
            *blocks,
            terminal_client_roundtrip_included,
        )

    def validate_aggregate(self, timing: M5RuntimeTiming) -> None:
        if not isinstance(timing, M5RuntimeTiming):
            raise ValidationError("timing aggregate must be M5RuntimeTiming")
        if self.required_observed_count == 0 and any(
            (
                timing.coordinator_non_db_non_neural_ns,
                timing.neural_wall_ns,
                timing.postgres_roundtrip_wall_ns,
                timing.external_io_wall_ns,
                timing.end_to_end_wall_ns,
            )
        ):
            raise ValidationError(
                "missing required timing points cannot have nonzero aggregates"
            )
        optional_shapes = (
            (
                "postgres_server_execution_ns",
                timing.postgres_server_execution_ns,
                self.postgres_server_execution_observed_count,
                self.postgres_server_execution_missing_count,
            ),
            (
                "postgres_lock_wait_ns",
                timing.postgres_lock_wait_ns,
                self.postgres_lock_wait_observed_count,
                self.postgres_lock_wait_missing_count,
            ),
            (
                "postgres_wal_bytes",
                timing.postgres_wal_bytes,
                self.postgres_wal_bytes_observed_count,
                self.postgres_wal_bytes_missing_count,
            ),
            (
                "postgres_shared_block_reads",
                timing.postgres_shared_block_reads,
                self.postgres_shared_block_reads_observed_count,
                self.postgres_shared_block_reads_missing_count,
            ),
        )
        for name, value, observed, missing in optional_shapes:
            should_be_present = observed > 0 and missing == 0
            if should_be_present != (value is not None):
                raise ValidationError(
                    f"{name} aggregate presence disagrees with timing coverage"
                )


@dataclass(frozen=True, slots=True)
class M5TransitionTimingAnchor:
    epoch_id: int
    contribution_kind: M5RuntimeWorkContributionKind
    source_id: str
    contribution_key_digest: str
    anchor_revision: int
    terminal_transition: bool

    def __post_init__(self) -> None:
        _require_int("epoch_id", self.epoch_id, positive=True)
        if not isinstance(self.contribution_kind, M5RuntimeWorkContributionKind):
            raise ValidationError(
                "contribution_kind must be M5RuntimeWorkContributionKind"
            )
        _require_text("source_id", self.source_id)
        _require_identity(
            "contribution_key_digest",
            self.contribution_key_digest,
            digests.runtime_work_contribution_key_digest(
                epoch_id=self.epoch_id,
                contribution_kind=self.contribution_kind,
                source_id=self.source_id,
            ),
        )
        _require_int("anchor_revision", self.anchor_revision, positive=True)
        _require_bool("terminal_transition", self.terminal_transition)

    @classmethod
    def build(
        cls,
        *,
        epoch_id: int,
        contribution_kind: M5RuntimeWorkContributionKind,
        source_id: str,
        anchor_revision: int,
        terminal_transition: bool,
    ) -> M5TransitionTimingAnchor:
        return cls(
            epoch_id=epoch_id,
            contribution_kind=contribution_kind,
            source_id=source_id,
            contribution_key_digest=digests.runtime_work_contribution_key_digest(
                epoch_id=epoch_id,
                contribution_kind=contribution_kind,
                source_id=source_id,
            ),
            anchor_revision=anchor_revision,
            terminal_transition=terminal_transition,
        )


@dataclass(frozen=True, slots=True)
class M5TransitionTimingReceipt:
    anchor: M5TransitionTimingAnchor
    transition_timing_digest: str
    event_timing: M5RuntimeTiming
    event_timing_coverage: M5RuntimeTimingCoverage
    resulting_revision: int
    exact_replay: bool

    def __post_init__(self) -> None:
        if not isinstance(self.anchor, M5TransitionTimingAnchor):
            raise ValidationError("anchor must be M5TransitionTimingAnchor")
        _require_hash("transition_timing_digest", self.transition_timing_digest)
        self.event_timing_coverage.validate_aggregate(self.event_timing)
        if self.event_timing_coverage.terminal_client_roundtrip_included:
            raise ValidationError(
                "event timing coverage cannot include terminal client roundtrip"
            )
        _require_int("resulting_revision", self.resulting_revision, positive=True)
        _require_bool("exact_replay", self.exact_replay)
        if self.resulting_revision < self.anchor.anchor_revision:
            raise ValidationError(
                "transition timing receipt cannot precede its anchor revision"
            )

    def validate_observation(self, observation: M5RuntimeTimingObservation) -> None:
        expected = digests.transition_call_timing_digest(
            epoch_id=self.anchor.epoch_id,
            contribution_kind=self.anchor.contribution_kind,
            source_id=self.anchor.source_id,
            contribution_key_digest=self.anchor.contribution_key_digest,
            anchor_revision=self.anchor.anchor_revision,
            observation_digest=observation.observation_digest,
        )
        _require_identity(
            "transition_timing_digest", self.transition_timing_digest, expected
        )


@dataclass(frozen=True, slots=True)
class M5TypedDirectVerificationExecution:
    observation_id: str
    job_id: str
    admitted_pair_id: str
    model_artifact_id: str
    prompt_artifact_id: str
    execution_spec_hash: str
    pair_input_hash: str
    calibration_version: str
    calibration_artifact_sha256: str
    temperature: float
    raw_logits: tuple[float, float, float]
    raw_output_hash: str
    reused_from_observation_id: str | None

    def __post_init__(self) -> None:
        for name in (
            "observation_id",
            "job_id",
            "model_artifact_id",
            "prompt_artifact_id",
            "calibration_version",
        ):
            _require_text(name, getattr(self, name))
        for name in (
            "admitted_pair_id",
            "execution_spec_hash",
            "pair_input_hash",
            "calibration_artifact_sha256",
            "raw_output_hash",
        ):
            _require_hash(name, getattr(self, name))
        _require_finite_number("temperature", self.temperature)
        if self.temperature <= 0:
            raise ValidationError("temperature must be positive")
        _require_tuple("raw_logits", self.raw_logits)
        if len(self.raw_logits) != 3:
            raise ValidationError("raw_logits must contain exactly three values")
        for index, logit in enumerate(self.raw_logits):
            _require_finite_number(f"raw_logits[{index}]", logit)
        if self.reused_from_observation_id is not None:
            _require_text("reused_from_observation_id", self.reused_from_observation_id)
            if self.reused_from_observation_id == self.observation_id:
                raise ValidationError(
                    "reused_from_observation_id must differ from observation_id"
                )

    @property
    def digest_values(self) -> digests.TypedDirectVerificationExecutionValues:
        return (
            self.observation_id,
            self.job_id,
            self.admitted_pair_id,
            self.model_artifact_id,
            self.prompt_artifact_id,
            self.execution_spec_hash,
            self.pair_input_hash,
            self.calibration_version,
            self.calibration_artifact_sha256,
            self.temperature,
            self.raw_logits,
            self.raw_output_hash,
            self.reused_from_observation_id,
        )


def _m4_channel_set_hash(hits: tuple[M4ChannelHit, ...]) -> str:
    identities: list[str] = []
    for hit in hits:
        identities.append(
            stable_m4_digest(
                "m4-discovery-channel-v1",
                str(hit.epoch_id),
                hit.pair.claim_id,
                hit.pair.chunk_version_id,
                hit.candidate_policy_id,
                hit.channel.value,
                str(hit.rank),
                "" if hit.score is None else format(hit.score, ".17g"),
                hit.channel_artifact_hash,
            )
        )
    return stable_m4_digest("m4-discovery-channel-set-v1", *sorted(identities))


def _m4_admitted_pair_id(epoch_id: int, job: M4LogicalJobSpec) -> str:
    pair = job.pair
    if pair is None:
        raise ValidationError("typed-direct verifier job requires a pair")
    return stable_m4_digest(
        "m4-admitted-pair-v1",
        str(epoch_id),
        pair.claim_id,
        pair.chunk_version_id,
        job.candidate_policy_id,
    )


def _m4_admitted_pair_set_hash(pairs: tuple[M4AdmittedPair, ...]) -> str:
    identities: list[str] = []
    for admitted in pairs:
        identities.append(
            stable_m4_digest(
                "m4-admitted-pair-v1",
                str(admitted.epoch_id),
                admitted.pair.claim_id,
                admitted.pair.chunk_version_id,
                admitted.candidate_policy_id,
            )
        )
    return stable_m4_digest("m4-discovery-admitted-set-v1", *sorted(identities))


@dataclass(frozen=True, slots=True)
class M5TypedDirectLateReturnEnvelope:
    epoch_id: int
    return_kind: M5TypedDirectReturnKind
    job_id: str
    attempt_id: str
    result_artifact_id: str
    result_artifact_hash: str
    verification_execution_present: bool | None
    observation_eligible_for_currency: bool | None
    requested_make_effective: bool | None
    job: M4LogicalJobSpec
    attempt: M4JobAttempt
    completion: M4JobCompletion
    discovery: M4DiscoveryResult | None
    scope: M4DiscoveryScope | None
    persisted_scope_kind: M5TypedDirectScopeKind | None
    explicit_claim_ids: tuple[str, ...] | None
    closed_revision: int | None
    verification_execution: M5TypedDirectVerificationExecution | None
    observation: SemanticObservation | None
    observation_produced_epoch: int | None
    observation_raw_output_hash: str | None
    job_binding_digest: str
    attempt_binding_digest: str
    completion_binding_digest: str
    discovery_binding_digest: str | None
    scope_binding_digest: str | None
    verifier_binding_digest: str | None
    envelope_digest: str

    def __post_init__(self) -> None:
        _require_int("epoch_id", self.epoch_id, positive=True)
        if not isinstance(self.return_kind, M5TypedDirectReturnKind):
            raise ValidationError("return_kind must be an M5TypedDirectReturnKind")
        if not isinstance(self.job, M4LogicalJobSpec):
            raise ValidationError("job must be an immutable M4 LogicalJobSpec")
        if not isinstance(self.attempt, M4JobAttempt):
            raise ValidationError("attempt must be an immutable M4 JobAttempt")
        if not isinstance(self.completion, M4JobCompletion):
            raise ValidationError("completion must be an immutable M4 JobCompletion")
        for name in ("job_id", "attempt_id", "result_artifact_id"):
            _require_text(name, getattr(self, name))
        _require_hash("result_artifact_hash", self.result_artifact_hash)
        if (
            self.job_id != self.job.job_id
            or self.attempt_id != self.attempt.attempt_id
            or self.attempt.job_id != self.job_id
            or self.attempt.execution_spec_hash != self.job.execution_spec_hash
            or self.completion.job_id != self.job_id
            or self.completion.payload_hash != self.job.payload_hash
            or self.completion.execution_spec_hash != self.job.execution_spec_hash
            or self.result_artifact_id != self.completion.result_artifact_id
            or self.result_artifact_hash != self.completion.result_artifact_hash
        ):
            raise ValidationError(
                "typed-direct late return has inconsistent job/attempt/completion"
            )
        if self.attempt.attempt_id != stable_m4_digest(
            "m4-job-attempt-v1", self.job_id, str(self.attempt.attempt_ordinal)
        ) or self.attempt.lease_token_hash != stable_m4_digest(
            "m4-lease-token-v1", self.job_id, str(self.attempt.attempt_ordinal)
        ):
            raise ValidationError(
                "typed-direct late attempt violates frozen M4 identities"
            )
        closure = self.completion.child_closure
        if self.job.expandable != (closure is not None):
            raise ValidationError(
                "typed-direct completion closure conflicts with its job kind"
            )
        if closure is not None and closure.completion_digest != stable_m4_digest(
            "m4-expandable-completion-v1",
            self.job_id,
            self.result_artifact_hash,
            closure.child_set_hash,
        ):
            raise ValidationError(
                "typed-direct child closure violates frozen M4 identity"
            )
        expected_job = self._expected_job_binding_digest()
        expected_attempt = self._expected_attempt_binding_digest()
        expected_completion = self._expected_completion_binding_digest()
        _require_identity("job_binding_digest", self.job_binding_digest, expected_job)
        _require_identity(
            "attempt_binding_digest", self.attempt_binding_digest, expected_attempt
        )
        _require_identity(
            "completion_binding_digest",
            self.completion_binding_digest,
            expected_completion,
        )
        if self.return_kind is M5TypedDirectReturnKind.DISCOVERY:
            self._validate_discovery_shape()
        else:
            self._validate_verifier_shape()
        _require_identity(
            "envelope_digest",
            self.envelope_digest,
            digests.typed_direct_late_return_envelope_digest(
                epoch_id=self.epoch_id,
                return_kind=self.return_kind,
                job_binding_digest=self.job_binding_digest,
                attempt_binding_digest=self.attempt_binding_digest,
                completion_binding_digest=self.completion_binding_digest,
                discovery_binding_digest=self.discovery_binding_digest,
                scope_binding_digest=self.scope_binding_digest,
                verifier_binding_digest=self.verifier_binding_digest,
            ),
        )

    def _expected_job_binding_digest(self) -> str:
        pair = self.job.pair
        return digests.typed_direct_late_job_binding_digest(
            job_id=self.job.job_id,
            event_id=self.job.event_id,
            job_kind=self.job.kind,
            candidate_policy_id=self.job.candidate_policy_id,
            payload_hash=self.job.payload_hash,
            execution_spec_hash=self.job.execution_spec_hash,
            parent_job_id=self.job.parent_job_id,
            pair_claim_id=None if pair is None else pair.claim_id,
            pair_chunk_version_id=(None if pair is None else pair.chunk_version_id),
            target_claim_id=self.job.target_claim_id,
            target_chunk_version_id=self.job.target_chunk_version_id,
            expandable=self.job.expandable,
        )

    def _expected_attempt_binding_digest(self) -> str:
        return digests.typed_direct_late_attempt_binding_digest(
            attempt_id=self.attempt.attempt_id,
            job_id=self.attempt.job_id,
            execution_spec_hash=self.attempt.execution_spec_hash,
            attempt_ordinal=self.attempt.attempt_ordinal,
            lease_token_hash=self.attempt.lease_token_hash,
        )

    def _expected_completion_binding_digest(self) -> str:
        closure = self.completion.child_closure
        return digests.typed_direct_late_completion_binding_digest(
            job_id=self.completion.job_id,
            payload_hash=self.completion.payload_hash,
            execution_spec_hash=self.completion.execution_spec_hash,
            result_artifact_id=self.completion.result_artifact_id,
            result_artifact_hash=self.completion.result_artifact_hash,
            terminal_state=self.completion.terminal_state,
            completion_digest=self.completion.completion_digest,
            child_parent_job_id=(None if closure is None else closure.parent_job_id),
            child_completion_digest=(
                None if closure is None else closure.completion_digest
            ),
            child_set_hash=None if closure is None else closure.child_set_hash,
            child_job_ids=() if closure is None else closure.child_job_ids,
        )

    def _validate_discovery_shape(self) -> None:
        if self.job.kind not in {
            M4JobKind.IMPACT_DISCOVERY,
            M4JobKind.FRONTIER_RETRIEVE,
        }:
            raise ValidationError("discovery late return requires a discovery job")
        if not isinstance(self.discovery, M4DiscoveryResult) or not isinstance(
            self.scope, M4DiscoveryScope
        ):
            raise ValidationError("discovery late return requires result and scope")
        verifier_only = (
            self.verification_execution_present,
            self.observation_eligible_for_currency,
            self.requested_make_effective,
            self.verification_execution,
            self.observation,
            self.observation_produced_epoch,
            self.observation_raw_output_hash,
            self.verifier_binding_digest,
        )
        if any(value is not None for value in verifier_only):
            raise ValidationError(
                "discovery late return cannot carry verifier-only fields"
            )
        if self.persisted_scope_kind is None:
            raise ValidationError("discovery late return requires persisted scope kind")
        if (
            self.discovery.root_job_id != self.job_id
            or self.discovery.result_artifact_id != self.result_artifact_id
            or self.discovery.result_artifact_hash != self.result_artifact_hash
            or self.scope.root_job_id != self.job_id
        ):
            raise ValidationError("discovery late return closure is inconsistent")
        if self.persisted_scope_kind is M5TypedDirectScopeKind.EXPLICIT_CLAIMS:
            if self.explicit_claim_ids is None:
                raise ValidationError("explicit scope requires explicit claim IDs")
            _require_sorted_unique_text("explicit_claim_ids", self.explicit_claim_ids)
            if self.explicit_claim_ids != self.scope.registered_claim_ids:
                raise ValidationError(
                    "explicit scope IDs must match the immutable scope registry"
                )
        elif self.explicit_claim_ids is not None:
            raise ValidationError("all-registered scope cannot carry explicit IDs")
        if self.scope.closed != (self.closed_revision is not None):
            raise ValidationError(
                "scope closed flag and closed revision must be jointly shaped"
            )
        if self.closed_revision is not None:
            _require_int("closed_revision", self.closed_revision)
        for hit in self.discovery.channel_hits:
            if (
                hit.epoch_id != self.epoch_id
                or hit.candidate_policy_id != self.job.candidate_policy_id
            ):
                raise ValidationError(
                    "discovery channel hit escaped the enclosing epoch or policy"
                )
            if (
                self.job.kind is M4JobKind.IMPACT_DISCOVERY
                and hit.pair.chunk_version_id != self.job.target_chunk_version_id
            ):
                raise ValidationError("impact channel hit escaped its chunk scope")
            if (
                self.job.kind is M4JobKind.FRONTIER_RETRIEVE
                and hit.pair.claim_id != self.job.target_claim_id
            ):
                raise ValidationError("frontier channel hit escaped its claim scope")
        for admitted in self.discovery.admitted_pairs:
            if (
                admitted.epoch_id != self.epoch_id
                or admitted.candidate_policy_id != self.job.candidate_policy_id
            ):
                raise ValidationError(
                    "admitted pair escaped the enclosing epoch or policy"
                )
            if (
                self.job.kind is M4JobKind.IMPACT_DISCOVERY
                and admitted.pair.chunk_version_id != self.job.target_chunk_version_id
            ):
                raise ValidationError("impact admission escaped its chunk scope")
            if (
                self.job.kind is M4JobKind.FRONTIER_RETRIEVE
                and admitted.pair.claim_id != self.job.target_claim_id
            ):
                raise ValidationError("frontier admission escaped its claim scope")
        channel_set_hash = _m4_channel_set_hash(self.discovery.channel_hits)
        admitted_pair_set_hash = _m4_admitted_pair_set_hash(
            self.discovery.admitted_pairs
        )
        channel_rows: tuple[digests.TypedDirectChannelHitValues, ...] = tuple(
            (
                hit.epoch_id,
                hit.pair.claim_id,
                hit.pair.chunk_version_id,
                hit.candidate_policy_id,
                hit.channel,
                hit.rank,
                hit.score,
                hit.channel_artifact_hash,
            )
            for hit in self.discovery.channel_hits
        )
        admitted_rows: tuple[digests.TypedDirectAdmittedPairValues, ...] = tuple(
            (
                admitted.epoch_id,
                admitted.pair.claim_id,
                admitted.pair.chunk_version_id,
                admitted.candidate_policy_id,
                admitted.fused_rank,
                admitted.reasons,
                admitted.mandatory_lineage,
            )
            for admitted in self.discovery.admitted_pairs
        )
        expected_discovery = digests.typed_direct_late_discovery_binding_digest(
            root_job_id=self.discovery.root_job_id,
            result_artifact_id=self.discovery.result_artifact_id,
            result_artifact_hash=self.discovery.result_artifact_hash,
            fallback_satisfied=self.discovery.fallback_satisfied,
            channel_hit_count=len(self.discovery.channel_hits),
            admitted_pair_count=len(self.discovery.admitted_pairs),
            channel_set_hash=channel_set_hash,
            admitted_pair_set_hash=admitted_pair_set_hash,
            channel_hits=channel_rows,
            admitted_pairs=admitted_rows,
        )
        expected_scope = digests.typed_direct_late_scope_binding_digest(
            root_job_id=self.scope.root_job_id,
            epoch_id=self.epoch_id,
            registry_snapshot_id=self.scope.registry_snapshot_id,
            registered_claim_ids=self.scope.registered_claim_ids,
            closed=self.scope.closed,
            persisted_scope_kind=self.persisted_scope_kind,
            explicit_claim_ids=self.explicit_claim_ids,
            closed_revision=self.closed_revision,
        )
        if self.discovery_binding_digest is None or self.scope_binding_digest is None:
            raise ValidationError("discovery and scope binding digests are required")
        _require_identity(
            "discovery_binding_digest",
            self.discovery_binding_digest,
            expected_discovery,
        )
        _require_identity(
            "scope_binding_digest", self.scope_binding_digest, expected_scope
        )

    def _validate_verifier_shape(self) -> None:
        if self.job.kind is not M4JobKind.VERIFY_PAIR:
            raise ValidationError("verifier late return requires a verifier job")
        if any(
            value is not None
            for value in (
                self.discovery,
                self.scope,
                self.persisted_scope_kind,
                self.explicit_claim_ids,
                self.closed_revision,
                self.discovery_binding_digest,
                self.scope_binding_digest,
            )
        ):
            raise ValidationError(
                "verifier late return cannot carry discovery-only fields"
            )
        for name in (
            "verification_execution_present",
            "observation_eligible_for_currency",
            "requested_make_effective",
        ):
            value = getattr(self, name)
            if value is None:
                raise ValidationError(f"verifier late return requires {name}")
            _require_bool(name, value)
        if not isinstance(self.observation, SemanticObservation):
            raise ValidationError("verifier late return requires an observation")
        if self.observation_produced_epoch is None:
            raise ValidationError(
                "verifier late return requires observation_produced_epoch"
            )
        _require_int(
            "observation_produced_epoch",
            self.observation_produced_epoch,
            positive=True,
        )
        if self.observation_produced_epoch != self.epoch_id:
            raise ValidationError(
                "observation produced epoch must equal the enclosing epoch"
            )
        if self.observation_raw_output_hash is None:
            raise ValidationError(
                "verifier late return requires observation_raw_output_hash"
            )
        _require_hash("observation_raw_output_hash", self.observation_raw_output_hash)
        assert self.verification_execution_present is not None
        if self.verification_execution_present != (
            self.verification_execution is not None
        ):
            raise ValidationError(
                "verification execution flag must match its whole optional tuple"
            )
        if self.observation_eligible_for_currency is not True:
            raise ValidationError(
                "unchanged direct insertion requires currency-eligible observation"
            )
        pair = self.job.pair
        assert pair is not None
        if (
            self.observation.subject_kind is not SubjectKind.CLAIM
            or self.observation.subject_id != pair.claim_id
            or self.observation.chunk_version_id != pair.chunk_version_id
        ):
            raise ValidationError("late observation belongs to another semantic pair")
        execution_values: digests.TypedDirectVerificationExecutionValues | None = None
        if self.verification_execution is not None:
            execution = self.verification_execution
            if (
                execution.observation_id != self.observation.observation_id
                or execution.job_id != self.job_id
                or execution.admitted_pair_id
                != _m4_admitted_pair_id(self.epoch_id, self.job)
                or execution.execution_spec_hash != self.job.execution_spec_hash
                or execution.raw_output_hash != self.observation_raw_output_hash
            ):
                raise ValidationError(
                    "verification execution disagrees with enclosing late return"
                )
            execution_values = execution.digest_values
        elif self.observation_raw_output_hash != self.completion.result_artifact_hash:
            raise ValidationError(
                "absent execution requires completion artifact as raw-output hash"
            )
        assert self.observation_produced_epoch is not None
        assert self.observation_raw_output_hash is not None
        assert self.observation_eligible_for_currency is not None
        assert self.requested_make_effective is not None
        expected_verifier = digests.typed_direct_late_verifier_binding_digest(
            result_artifact_id=self.result_artifact_id,
            result_artifact_hash=self.result_artifact_hash,
            verification_execution_present=self.verification_execution_present,
            verification_execution=execution_values,
            observation_id=self.observation.observation_id,
            observation_subject_kind=self.observation.subject_kind,
            observation_subject_id=self.observation.subject_id,
            observation_chunk_version_id=self.observation.chunk_version_id,
            observation_task_type=self.observation.task_type,
            observation_support_score=self.observation.support_score,
            observation_refute_score=self.observation.refute_score,
            observation_neutral_score=self.observation.neutral_score,
            observation_model_id=self.observation.producer.model_id,
            observation_model_version=self.observation.producer.model_version,
            observation_prompt_version=self.observation.producer.prompt_version,
            observation_input_hash=self.observation.input_hash,
            observation_produced_epoch=self.observation_produced_epoch,
            observation_raw_output_hash=self.observation_raw_output_hash,
            observation_eligible_for_currency=(self.observation_eligible_for_currency),
            requested_make_effective=self.requested_make_effective,
        )
        if self.verifier_binding_digest is None:
            raise ValidationError("verifier binding digest is required")
        _require_identity(
            "verifier_binding_digest",
            self.verifier_binding_digest,
            expected_verifier,
        )

    @classmethod
    def build_discovery(
        cls,
        *,
        epoch_id: int,
        job: M4LogicalJobSpec,
        attempt: M4JobAttempt,
        completion: M4JobCompletion,
        discovery: M4DiscoveryResult,
        scope: M4DiscoveryScope,
        persisted_scope_kind: M5TypedDirectScopeKind,
        explicit_claim_ids: tuple[str, ...] | None,
        closed_revision: int | None,
    ) -> M5TypedDirectLateReturnEnvelope:
        job_digest = digests.typed_direct_late_job_binding_digest(
            job_id=job.job_id,
            event_id=job.event_id,
            job_kind=job.kind,
            candidate_policy_id=job.candidate_policy_id,
            payload_hash=job.payload_hash,
            execution_spec_hash=job.execution_spec_hash,
            parent_job_id=job.parent_job_id,
            pair_claim_id=None if job.pair is None else job.pair.claim_id,
            pair_chunk_version_id=(
                None if job.pair is None else job.pair.chunk_version_id
            ),
            target_claim_id=job.target_claim_id,
            target_chunk_version_id=job.target_chunk_version_id,
            expandable=job.expandable,
        )
        attempt_digest = digests.typed_direct_late_attempt_binding_digest(
            attempt_id=attempt.attempt_id,
            job_id=attempt.job_id,
            execution_spec_hash=attempt.execution_spec_hash,
            attempt_ordinal=attempt.attempt_ordinal,
            lease_token_hash=attempt.lease_token_hash,
        )
        closure = completion.child_closure
        completion_digest = digests.typed_direct_late_completion_binding_digest(
            job_id=completion.job_id,
            payload_hash=completion.payload_hash,
            execution_spec_hash=completion.execution_spec_hash,
            result_artifact_id=completion.result_artifact_id,
            result_artifact_hash=completion.result_artifact_hash,
            terminal_state=completion.terminal_state,
            completion_digest=completion.completion_digest,
            child_parent_job_id=(None if closure is None else closure.parent_job_id),
            child_completion_digest=(
                None if closure is None else closure.completion_digest
            ),
            child_set_hash=None if closure is None else closure.child_set_hash,
            child_job_ids=() if closure is None else closure.child_job_ids,
        )
        channel_set_hash = _m4_channel_set_hash(discovery.channel_hits)
        admitted_pair_set_hash = _m4_admitted_pair_set_hash(discovery.admitted_pairs)
        discovery_digest = digests.typed_direct_late_discovery_binding_digest(
            root_job_id=discovery.root_job_id,
            result_artifact_id=discovery.result_artifact_id,
            result_artifact_hash=discovery.result_artifact_hash,
            fallback_satisfied=discovery.fallback_satisfied,
            channel_hit_count=len(discovery.channel_hits),
            admitted_pair_count=len(discovery.admitted_pairs),
            channel_set_hash=channel_set_hash,
            admitted_pair_set_hash=admitted_pair_set_hash,
            channel_hits=tuple(
                (
                    hit.epoch_id,
                    hit.pair.claim_id,
                    hit.pair.chunk_version_id,
                    hit.candidate_policy_id,
                    hit.channel,
                    hit.rank,
                    hit.score,
                    hit.channel_artifact_hash,
                )
                for hit in discovery.channel_hits
            ),
            admitted_pairs=tuple(
                (
                    admitted.epoch_id,
                    admitted.pair.claim_id,
                    admitted.pair.chunk_version_id,
                    admitted.candidate_policy_id,
                    admitted.fused_rank,
                    admitted.reasons,
                    admitted.mandatory_lineage,
                )
                for admitted in discovery.admitted_pairs
            ),
        )
        scope_digest = digests.typed_direct_late_scope_binding_digest(
            root_job_id=scope.root_job_id,
            epoch_id=epoch_id,
            registry_snapshot_id=scope.registry_snapshot_id,
            registered_claim_ids=scope.registered_claim_ids,
            closed=scope.closed,
            persisted_scope_kind=persisted_scope_kind,
            explicit_claim_ids=explicit_claim_ids,
            closed_revision=closed_revision,
        )
        envelope_digest = digests.typed_direct_late_return_envelope_digest(
            epoch_id=epoch_id,
            return_kind=M5TypedDirectReturnKind.DISCOVERY,
            job_binding_digest=job_digest,
            attempt_binding_digest=attempt_digest,
            completion_binding_digest=completion_digest,
            discovery_binding_digest=discovery_digest,
            scope_binding_digest=scope_digest,
            verifier_binding_digest=None,
        )
        return cls(
            epoch_id=epoch_id,
            return_kind=M5TypedDirectReturnKind.DISCOVERY,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            result_artifact_id=completion.result_artifact_id,
            result_artifact_hash=completion.result_artifact_hash,
            verification_execution_present=None,
            observation_eligible_for_currency=None,
            requested_make_effective=None,
            job=job,
            attempt=attempt,
            completion=completion,
            discovery=discovery,
            scope=scope,
            persisted_scope_kind=persisted_scope_kind,
            explicit_claim_ids=explicit_claim_ids,
            closed_revision=closed_revision,
            verification_execution=None,
            observation=None,
            observation_produced_epoch=None,
            observation_raw_output_hash=None,
            job_binding_digest=job_digest,
            attempt_binding_digest=attempt_digest,
            completion_binding_digest=completion_digest,
            discovery_binding_digest=discovery_digest,
            scope_binding_digest=scope_digest,
            verifier_binding_digest=None,
            envelope_digest=envelope_digest,
        )

    @classmethod
    def build_verifier(
        cls,
        *,
        epoch_id: int,
        job: M4LogicalJobSpec,
        attempt: M4JobAttempt,
        completion: M4JobCompletion,
        verification_execution: M5TypedDirectVerificationExecution | None,
        observation: SemanticObservation,
        observation_produced_epoch: int,
        observation_raw_output_hash: str,
        observation_eligible_for_currency: bool,
        requested_make_effective: bool,
    ) -> M5TypedDirectLateReturnEnvelope:
        job_digest = digests.typed_direct_late_job_binding_digest(
            job_id=job.job_id,
            event_id=job.event_id,
            job_kind=job.kind,
            candidate_policy_id=job.candidate_policy_id,
            payload_hash=job.payload_hash,
            execution_spec_hash=job.execution_spec_hash,
            parent_job_id=job.parent_job_id,
            pair_claim_id=None if job.pair is None else job.pair.claim_id,
            pair_chunk_version_id=(
                None if job.pair is None else job.pair.chunk_version_id
            ),
            target_claim_id=job.target_claim_id,
            target_chunk_version_id=job.target_chunk_version_id,
            expandable=job.expandable,
        )
        attempt_digest = digests.typed_direct_late_attempt_binding_digest(
            attempt_id=attempt.attempt_id,
            job_id=attempt.job_id,
            execution_spec_hash=attempt.execution_spec_hash,
            attempt_ordinal=attempt.attempt_ordinal,
            lease_token_hash=attempt.lease_token_hash,
        )
        closure = completion.child_closure
        completion_digest = digests.typed_direct_late_completion_binding_digest(
            job_id=completion.job_id,
            payload_hash=completion.payload_hash,
            execution_spec_hash=completion.execution_spec_hash,
            result_artifact_id=completion.result_artifact_id,
            result_artifact_hash=completion.result_artifact_hash,
            terminal_state=completion.terminal_state,
            completion_digest=completion.completion_digest,
            child_parent_job_id=(None if closure is None else closure.parent_job_id),
            child_completion_digest=(
                None if closure is None else closure.completion_digest
            ),
            child_set_hash=None if closure is None else closure.child_set_hash,
            child_job_ids=() if closure is None else closure.child_job_ids,
        )
        verifier_digest = digests.typed_direct_late_verifier_binding_digest(
            result_artifact_id=completion.result_artifact_id,
            result_artifact_hash=completion.result_artifact_hash,
            verification_execution_present=verification_execution is not None,
            verification_execution=(
                None
                if verification_execution is None
                else verification_execution.digest_values
            ),
            observation_id=observation.observation_id,
            observation_subject_kind=observation.subject_kind,
            observation_subject_id=observation.subject_id,
            observation_chunk_version_id=observation.chunk_version_id,
            observation_task_type=observation.task_type,
            observation_support_score=observation.support_score,
            observation_refute_score=observation.refute_score,
            observation_neutral_score=observation.neutral_score,
            observation_model_id=observation.producer.model_id,
            observation_model_version=observation.producer.model_version,
            observation_prompt_version=observation.producer.prompt_version,
            observation_input_hash=observation.input_hash,
            observation_produced_epoch=observation_produced_epoch,
            observation_raw_output_hash=observation_raw_output_hash,
            observation_eligible_for_currency=observation_eligible_for_currency,
            requested_make_effective=requested_make_effective,
        )
        envelope_digest = digests.typed_direct_late_return_envelope_digest(
            epoch_id=epoch_id,
            return_kind=M5TypedDirectReturnKind.VERIFIER,
            job_binding_digest=job_digest,
            attempt_binding_digest=attempt_digest,
            completion_binding_digest=completion_digest,
            discovery_binding_digest=None,
            scope_binding_digest=None,
            verifier_binding_digest=verifier_digest,
        )
        return cls(
            epoch_id=epoch_id,
            return_kind=M5TypedDirectReturnKind.VERIFIER,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            result_artifact_id=completion.result_artifact_id,
            result_artifact_hash=completion.result_artifact_hash,
            verification_execution_present=verification_execution is not None,
            observation_eligible_for_currency=observation_eligible_for_currency,
            requested_make_effective=requested_make_effective,
            job=job,
            attempt=attempt,
            completion=completion,
            discovery=None,
            scope=None,
            persisted_scope_kind=None,
            explicit_claim_ids=None,
            closed_revision=None,
            verification_execution=verification_execution,
            observation=observation,
            observation_produced_epoch=observation_produced_epoch,
            observation_raw_output_hash=observation_raw_output_hash,
            job_binding_digest=job_digest,
            attempt_binding_digest=attempt_digest,
            completion_binding_digest=completion_digest,
            discovery_binding_digest=None,
            scope_binding_digest=None,
            verifier_binding_digest=verifier_digest,
            envelope_digest=envelope_digest,
        )


@dataclass(frozen=True, slots=True)
class M5OwnerPendingCounter:
    epoch_id: int
    owner_claim_id: str
    broad_reverse_scope_count: int
    forward_scope_count: int
    verifier_job_count: int
    blocking_failure_count: int

    def __post_init__(self) -> None:
        _require_int("epoch_id", self.epoch_id, positive=True)
        _require_text("owner_claim_id", self.owner_claim_id)
        for name in (
            "broad_reverse_scope_count",
            "forward_scope_count",
            "verifier_job_count",
            "blocking_failure_count",
        ):
            _require_int(name, getattr(self, name))

    @property
    def pending_multiplicity(self) -> int:
        return (
            self.broad_reverse_scope_count
            + self.forward_scope_count
            + self.verifier_job_count
            + self.blocking_failure_count
        )


def validate_combined_deltas(deltas: tuple[StatusDelta, ...], event_id: str) -> None:
    _require_tuple("combined deltas", deltas)
    keys = tuple((delta.object_type, delta.object_id) for delta in deltas)
    if keys != tuple(sorted(set(keys))):
        raise ValidationError("combined deltas must be sorted and unique by object")
    for delta in deltas:
        if delta.event_id != event_id:
            raise ValidationError("combined delta belongs to another event")
        if delta.object_type not in {"claim", "answer"}:
            raise ValidationError(
                "M5 public deltas may contain only claims and answers"
            )
        for name in ("object_id", "old_status", "new_status", "reason"):
            _require_text(f"delta {name}", getattr(delta, name))


def _original_open_binding(epoch_id: int) -> str:
    return digests.open_event_receipt_binding_digest(
        epoch_id=epoch_id,
        replayed=False,
        already_sealed=False,
        publication_id=None,
        already_failed=False,
        failure_reason=None,
    )


@dataclass(frozen=True, slots=True)
class M5EventRunResult:
    event_id: str
    payload_hash: str
    epoch_id: int
    state: M5RunState
    replayed_outcome: M5ReplayedOutcome | None
    open_receipt: OpenEventReceipt
    publication_receipt: PublicationReceipt | None
    event_work: M5RuntimeWork
    call_work: M5RuntimeWork
    event_timing: M5RuntimeTiming
    call_timing: M5RuntimeTiming
    combined_deltas: tuple[StatusDelta, ...]
    changed_state_references: tuple[M5ChangedStateReference, ...]
    failure_reason: M5RunFailureReason | None
    logical_result_hash: str | None
    event_timing_coverage: M5RuntimeTimingCoverage | None = None
    call_timing_coverage: M5RuntimeTimingCoverage | None = None

    @classmethod
    def build(
        cls,
        *,
        event_id: str,
        payload_hash: str,
        epoch_id: int,
        state: M5RunState,
        replayed_outcome: M5ReplayedOutcome | None,
        open_receipt: OpenEventReceipt,
        publication_receipt: PublicationReceipt | None,
        event_work: M5RuntimeWork,
        call_work: M5RuntimeWork,
        event_timing: M5RuntimeTiming,
        call_timing: M5RuntimeTiming,
        combined_deltas: tuple[StatusDelta, ...],
        changed_state_references: tuple[M5ChangedStateReference, ...],
        failure_reason: M5RunFailureReason | None,
        event_timing_coverage: M5RuntimeTimingCoverage | None = None,
        call_timing_coverage: M5RuntimeTimingCoverage | None = None,
    ) -> M5EventRunResult:
        if state is M5RunState.BLOCKED:
            logical_result_hash = None
        else:
            if state is M5RunState.REPLAYED:
                if replayed_outcome is None:
                    raise ValidationError("replay build requires its durable outcome")
                outcome = replayed_outcome
            elif state is M5RunState.SEALED:
                outcome = M5ReplayedOutcome.SEALED
            elif state is M5RunState.FAILED:
                outcome = M5ReplayedOutcome.FAILED
            else:
                raise ValidationError("unsupported run-result state")
            publication_hash: str | None = None
            if outcome is M5ReplayedOutcome.SEALED:
                if publication_receipt is None:
                    raise ValidationError("sealed result requires publication receipt")
                publication_hash = digests.publication_receipt_binding_digest(
                    epoch_id=publication_receipt.epoch_id,
                    publication_id=publication_receipt.publication_id,
                    replayed=False,
                )
            logical_result_hash = digests.event_run_logical_result_digest(
                event_id=event_id,
                payload_hash=payload_hash,
                epoch_id=epoch_id,
                sealed_or_failed_outcome=outcome,
                original_open_receipt_binding_hash=_original_open_binding(epoch_id),
                original_publication_receipt_binding_hash=publication_hash,
                event_work_digest=event_work.work_digest,
                combined_status_delta_set_hash=(
                    digests.combined_status_delta_set_digest(combined_deltas)
                ),
                changed_state_set_hash=digests.changed_state_set_digest(
                    reference.reference_digest for reference in changed_state_references
                ),
                failure_reason=failure_reason,
            )
        return cls(
            event_id,
            payload_hash,
            epoch_id,
            state,
            replayed_outcome,
            open_receipt,
            publication_receipt,
            event_work,
            call_work,
            event_timing,
            call_timing,
            combined_deltas,
            changed_state_references,
            failure_reason,
            logical_result_hash,
            event_timing_coverage,
            call_timing_coverage,
        )

    def __post_init__(self) -> None:
        _require_text("event_id", self.event_id)
        _require_hash("payload_hash", self.payload_hash)
        _require_int("epoch_id", self.epoch_id, positive=True)
        if not isinstance(self.state, M5RunState):
            raise ValidationError("run state must be an M5RunState")
        if self.replayed_outcome is not None and not isinstance(
            self.replayed_outcome, M5ReplayedOutcome
        ):
            raise ValidationError("replayed_outcome must be an M5ReplayedOutcome")
        if self.failure_reason is not None and not isinstance(
            self.failure_reason, M5RunFailureReason
        ):
            raise ValidationError("failure_reason must be an M5RunFailureReason")
        if self.open_receipt.epoch_id != self.epoch_id:
            raise ValidationError("open receipt belongs to another epoch")
        if (
            self.publication_receipt is not None
            and self.publication_receipt.epoch_id != self.epoch_id
        ):
            raise ValidationError("publication receipt belongs to another epoch")
        if self.publication_receipt is not None and (
            self.publication_receipt.publication_id
            != stable_m4_digest("m4-publication-v1", str(self.epoch_id))
        ):
            raise ValidationError("publication receipt identity does not match epoch")
        validate_combined_deltas(self.combined_deltas, self.event_id)
        validate_changed_state_references(self.changed_state_references)
        coverage_values = (
            self.event_timing_coverage,
            self.call_timing_coverage,
        )
        if any(value is None for value in coverage_values) and not all(
            value is None for value in coverage_values
        ):
            raise ValidationError(
                "event and call timing coverage are jointly present or absent"
            )
        if self.event_timing_coverage is not None:
            assert self.call_timing_coverage is not None
            self.event_timing_coverage.validate_aggregate(self.event_timing)
            self.call_timing_coverage.validate_aggregate(self.call_timing)
            if self.event_timing_coverage.terminal_client_roundtrip_included:
                raise ValidationError(
                    "durable event coverage cannot include terminal client roundtrip"
                )
        if self.state is M5RunState.SEALED:
            if (
                self.open_receipt.already_sealed
                or self.open_receipt.already_failed
                or self.publication_receipt is None
                or self.publication_receipt.replayed
                or self.replayed_outcome is not None
                or self.failure_reason is not None
                or self.logical_result_hash is None
            ):
                raise ValidationError("sealed run-result shape is invalid")
        elif self.state is M5RunState.FAILED:
            if (
                self.open_receipt.already_sealed
                or self.open_receipt.already_failed
                or self.publication_receipt is not None
                or self.replayed_outcome is not None
                or self.failure_reason is None
                or self.failure_reason is M5RunFailureReason.WORK_IN_PROGRESS
                or self.logical_result_hash is None
            ):
                raise ValidationError("failed run-result shape is invalid")
        elif self.state is M5RunState.BLOCKED:
            if (
                self.open_receipt.already_sealed
                or self.open_receipt.already_failed
                or self.publication_receipt is not None
                or self.replayed_outcome is not None
                or self.failure_reason is None
                or self.failure_reason is M5RunFailureReason.INVARIANT_FAILURE
                or self.logical_result_hash is not None
            ):
                raise ValidationError("blocked run-result shape is invalid")
        else:
            self._validate_replay_shape()
        if (
            self.failure_reason is M5RunFailureReason.WORK_IN_PROGRESS
            and self.state is not M5RunState.BLOCKED
        ):
            raise ValidationError("work_in_progress is valid only for BLOCKED")
        if self.logical_result_hash is not None:
            _require_identity(
                "logical_result_hash",
                self.logical_result_hash,
                self.expected_logical_result_hash,
            )

    def _validate_replay_shape(self) -> None:
        if self.replayed_outcome is None or not self.call_work.is_zero:
            raise ValidationError("replay requires an outcome and zero call work")
        if self.replayed_outcome is M5ReplayedOutcome.SEALED:
            if (
                not self.open_receipt.replayed
                or not self.open_receipt.already_sealed
                or self.open_receipt.already_failed
                or self.publication_receipt is None
                or not self.publication_receipt.replayed
                or self.failure_reason is not None
                or self.logical_result_hash is None
            ):
                raise ValidationError("sealed replay shape is invalid")
            assert self.publication_receipt is not None
            if self.open_receipt.publication_id != (
                self.publication_receipt.publication_id
            ):
                raise ValidationError("sealed replay receipt identities disagree")
        elif (
            not self.open_receipt.replayed
            or not self.open_receipt.already_failed
            or self.open_receipt.already_sealed
            or self.publication_receipt is not None
            or self.failure_reason is None
            or self.logical_result_hash is None
        ):
            raise ValidationError("failed replay shape is invalid")
        else:
            assert self.failure_reason is not None
            if self.open_receipt.failure_reason != self.failure_reason.value:
                raise ValidationError(
                    "failed replay reason disagrees with durable result"
                )

    @property
    def expected_logical_result_hash(self) -> str:
        if self.state is M5RunState.BLOCKED:
            raise ValidationError("blocked results have no logical result identity")
        if self.state is M5RunState.REPLAYED:
            assert self.replayed_outcome is not None
            outcome = self.replayed_outcome
        elif self.state is M5RunState.SEALED:
            outcome = M5ReplayedOutcome.SEALED
        else:
            outcome = M5ReplayedOutcome.FAILED
        publication_hash: str | None = None
        if outcome is M5ReplayedOutcome.SEALED:
            assert self.publication_receipt is not None
            publication_hash = digests.publication_receipt_binding_digest(
                epoch_id=self.publication_receipt.epoch_id,
                publication_id=self.publication_receipt.publication_id,
                replayed=False,
            )
        return digests.event_run_logical_result_digest(
            event_id=self.event_id,
            payload_hash=self.payload_hash,
            epoch_id=self.epoch_id,
            sealed_or_failed_outcome=outcome,
            original_open_receipt_binding_hash=_original_open_binding(self.epoch_id),
            original_publication_receipt_binding_hash=publication_hash,
            event_work_digest=self.event_work.work_digest,
            combined_status_delta_set_hash=digests.combined_status_delta_set_digest(
                self.combined_deltas
            ),
            changed_state_set_hash=digests.changed_state_set_digest(
                reference.reference_digest
                for reference in self.changed_state_references
            ),
            failure_reason=self.failure_reason,
        )


@dataclass(frozen=True, slots=True, order=True)
class M5RequirementFallbackKey:
    requirement_version_id: str
    candidate_policy_id: str

    def __post_init__(self) -> None:
        _require_text("requirement_version_id", self.requirement_version_id)
        _require_text("candidate_policy_id", self.candidate_policy_id)


@dataclass(frozen=True, slots=True)
class M5RequirementWithdrawalPlan:
    event_id: str
    deactivated_chunk_version_ids: tuple[str, ...]
    withdrawn_candidate_pair_digests: tuple[str, ...]
    withdrawn_observation_ids: tuple[str, ...]
    cancelled_job_ids: tuple[str, ...]
    fallback_keys: tuple[M5RequirementFallbackKey, ...]
    plan_digest: str

    def __post_init__(self) -> None:
        _require_text("event_id", self.event_id)
        for name in (
            "deactivated_chunk_version_ids",
            "withdrawn_observation_ids",
        ):
            _require_sorted_unique_text(name, getattr(self, name))
        _require_sorted_unique_text("cancelled_job_ids", self.cancelled_job_ids)
        for job_id in self.cancelled_job_ids:
            _require_hash("cancelled job ID", job_id)
        _require_tuple(
            "withdrawn_candidate_pair_digests",
            self.withdrawn_candidate_pair_digests,
        )
        if self.withdrawn_candidate_pair_digests != tuple(
            sorted(set(self.withdrawn_candidate_pair_digests))
        ):
            raise ValidationError("withdrawn pair digests must be sorted and unique")
        for value in self.withdrawn_candidate_pair_digests:
            _require_hash("withdrawn pair digest", value)
        _require_tuple("fallback_keys", self.fallback_keys)
        if self.fallback_keys != tuple(sorted(set(self.fallback_keys))):
            raise ValidationError("fallback keys must be sorted and unique")
        _require_identity(
            "plan_digest",
            self.plan_digest,
            digests.requirement_withdrawal_plan_digest(
                event_id=self.event_id,
                deactivated_chunk_version_ids=self.deactivated_chunk_version_ids,
                withdrawn_candidate_pair_digests=(
                    self.withdrawn_candidate_pair_digests
                ),
                withdrawn_observation_ids=self.withdrawn_observation_ids,
                cancelled_job_ids=self.cancelled_job_ids,
                fallback_keys=(
                    (key.requirement_version_id, key.candidate_policy_id)
                    for key in self.fallback_keys
                ),
            ),
        )


@dataclass(frozen=True, slots=True)
class M5RequirementFrontierHead:
    requirement_version_id: str
    candidate_policy_id: str
    latest_root_job_id: str
    latest_scope_contract_digest: str
    latest_active_chunk_snapshot_digest: str
    latest_discovery_result_artifact_hash: str
    latest_scope_closure_digest: str
    latest_completion_digest: str
    completed_epoch_id: int
    completed_revision: int

    def __post_init__(self) -> None:
        for name in (
            "requirement_version_id",
            "candidate_policy_id",
        ):
            _require_text(name, getattr(self, name))
        _require_hash("latest_root_job_id", self.latest_root_job_id)
        for name in (
            "latest_scope_contract_digest",
            "latest_active_chunk_snapshot_digest",
            "latest_discovery_result_artifact_hash",
            "latest_scope_closure_digest",
            "latest_completion_digest",
        ):
            _require_hash(name, getattr(self, name))
        _require_int("completed_epoch_id", self.completed_epoch_id, positive=True)
        _require_int("completed_revision", self.completed_revision)


@dataclass(frozen=True, slots=True)
class M5LeaseTerminalProjection:
    terminal_state: M5JobState
    terminal_reason: M5TerminalReason | None
    completion_digest: str
    terminal_identity_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.terminal_state, M5JobState) or not (
            self.terminal_state.terminal
        ):
            raise ValidationError("terminal projection requires a terminal job state")
        if self.terminal_reason is not None and not isinstance(
            self.terminal_reason, M5TerminalReason
        ):
            raise ValidationError("terminal_reason must be M5TerminalReason")
        if self.terminal_state is M5JobState.COMPLETED_ACTIVE:
            if self.terminal_reason is not None:
                raise ValidationError("completed_active projection has no reason")
        elif self.terminal_reason is None:
            raise ValidationError("non-active terminal projection requires its reason")
        _require_hash("completion_digest", self.completion_digest)
        _require_hash("terminal_identity_hash", self.terminal_identity_hash)

    @classmethod
    def build(
        cls,
        *,
        logical_job_id: str,
        terminal_state: M5JobState,
        terminal_reason: M5TerminalReason | None,
        completion_digest: str,
    ) -> M5LeaseTerminalProjection:
        return cls(
            terminal_state=terminal_state,
            terminal_reason=terminal_reason,
            completion_digest=completion_digest,
            terminal_identity_hash=digests.lease_terminal_projection_digest(
                logical_job_id=logical_job_id,
                terminal_state=terminal_state,
                terminal_reason=terminal_reason,
                completion_digest=completion_digest,
            ),
        )

    def validate_job(self, logical_job_id: str) -> None:
        _require_hash("logical_job_id", logical_job_id)
        _require_identity(
            "terminal_identity_hash",
            self.terminal_identity_hash,
            digests.lease_terminal_projection_digest(
                logical_job_id=logical_job_id,
                terminal_state=self.terminal_state,
                terminal_reason=self.terminal_reason,
                completion_digest=self.completion_digest,
            ),
        )


@dataclass(frozen=True, slots=True)
class M5JobLease:
    logical_job_id: str
    attempt: M5JobAttempt | None
    resulting_revision: int
    should_execute: bool
    exact_replay: bool
    lease_expires_at: datetime | None = None
    dispatch_record_digest: str | None = None
    disposition: M5AcquisitionDisposition | None = None
    terminal_projection: M5LeaseTerminalProjection | None = None

    def __post_init__(self) -> None:
        _require_hash("logical_job_id", self.logical_job_id)
        _require_int("resulting_revision", self.resulting_revision, positive=True)
        _require_bool("should_execute", self.should_execute)
        _require_bool("exact_replay", self.exact_replay)
        if (
            self.attempt is not None
            and self.attempt.logical_job_id != self.logical_job_id
        ):
            raise ValidationError("lease attempt belongs to another job")
        if self.disposition is None:
            self._validate_legacy_bridge()
            return
        if not isinstance(self.disposition, M5AcquisitionDisposition):
            raise ValidationError("disposition must be an M5AcquisitionDisposition")
        if self.attempt is not None and not self.attempt.has_operational_lease:
            raise ValidationError(
                "D24 acquisition cannot use a legacy attempt without lease metadata"
            )
        if self.attempt is not None:
            assert self.attempt.lease_expires_at is not None
            if self.lease_expires_at != self.attempt.lease_expires_at:
                raise ValidationError(
                    "lease deadline must equal the attempt operational deadline"
                )
        elif self.lease_expires_at is not None:
            raise ValidationError("lease without an attempt cannot carry a deadline")
        if self.lease_expires_at is not None:
            _require_timestamptz("lease_expires_at", self.lease_expires_at)
        if self.dispatch_record_digest is not None:
            _require_hash("dispatch_record_digest", self.dispatch_record_digest)

        active_dispositions = {
            M5AcquisitionDisposition.DISPATCH_NEW,
            M5AcquisitionDisposition.DISPATCH_TAKEOVER,
            M5AcquisitionDisposition.LIVE_LEASE,
            M5AcquisitionDisposition.RESULT_RESERVED,
        }
        if self.disposition in active_dispositions:
            if (
                self.attempt is None
                or self.lease_expires_at is None
                or self.dispatch_record_digest is None
                or self.terminal_projection is not None
            ):
                raise ValidationError(
                    "nonterminal acquisition requires attempt, deadline, and dispatch"
                )
            executing = self.disposition in {
                M5AcquisitionDisposition.DISPATCH_NEW,
                M5AcquisitionDisposition.DISPATCH_TAKEOVER,
            }
            if self.should_execute is not executing or self.exact_replay is executing:
                raise ValidationError(
                    "acquisition execute/replay flags disagree with disposition"
                )
            return

        if (
            self.should_execute
            or not self.exact_replay
            or self.terminal_projection is None
        ):
            raise ValidationError("terminal acquisition total shape is invalid")
        if (self.attempt is None) != (self.dispatch_record_digest is None):
            raise ValidationError(
                "terminal attempt and matching dispatch record are jointly present"
            )
        self.terminal_projection.validate_job(self.logical_job_id)

    def _validate_legacy_bridge(self) -> None:
        if any(
            value is not None
            for value in (
                self.lease_expires_at,
                self.dispatch_record_digest,
                self.terminal_projection,
            )
        ):
            raise ValidationError(
                "legacy acquisition bridge cannot carry D24 operational fields"
            )
        if self.attempt is not None and self.attempt.has_operational_lease:
            raise ValidationError(
                "operational attempt requires an explicit D24 disposition"
            )
        if self.should_execute and self.attempt is None:
            raise ValidationError("legacy executable lease requires an attempt")
        if self.exact_replay and self.should_execute:
            raise ValidationError("exact acquisition replay cannot dispatch model work")


@dataclass(frozen=True, slots=True)
class M5TypedDirectTerminalProjection:
    terminal_state: M4JobState
    terminal_reason: str | None
    m4_completion_digest: str | None
    completed_revision: int
    terminal_identity_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.terminal_state, M4JobState) or not (
            self.terminal_state.terminal
        ):
            raise ValidationError(
                "typed-direct projection requires a terminal M4 state"
            )
        if self.terminal_state is M4JobState.COMPLETED_ACTIVE:
            if self.terminal_reason is not None or self.m4_completion_digest is None:
                raise ValidationError(
                    "completed_active direct projection has no reason and a completion"
                )
        elif self.terminal_state is M4JobState.COMPLETED_INACTIVE:
            if (
                self.terminal_reason != "inactive_at_completion"
                or self.m4_completion_digest is None
            ):
                raise ValidationError(
                    "completed_inactive direct projection requires exact reason"
                )
        elif self.terminal_reason is None:
            raise ValidationError(
                "failed or cancelled direct projection requires its exact reason"
            )
        if self.terminal_reason is not None:
            _require_text("terminal_reason", self.terminal_reason)
        if self.m4_completion_digest is not None:
            _require_hash("m4_completion_digest", self.m4_completion_digest)
        _require_int("completed_revision", self.completed_revision, positive=True)
        _require_hash("terminal_identity_hash", self.terminal_identity_hash)

    @classmethod
    def build(
        cls,
        *,
        job_id: str,
        terminal_state: M4JobState,
        terminal_reason: str | None,
        m4_completion_digest: str | None,
        completed_revision: int,
    ) -> M5TypedDirectTerminalProjection:
        return cls(
            terminal_state=terminal_state,
            terminal_reason=terminal_reason,
            m4_completion_digest=m4_completion_digest,
            completed_revision=completed_revision,
            terminal_identity_hash=(
                digests.typed_direct_terminal_projection_digest(
                    job_id=job_id,
                    terminal_state=terminal_state,
                    terminal_reason=terminal_reason,
                    m4_completion_digest=m4_completion_digest,
                    completed_revision=completed_revision,
                )
            ),
        )

    def validate_job(self, job_id: str) -> None:
        _require_text("job_id", job_id)
        _require_identity(
            "terminal_identity_hash",
            self.terminal_identity_hash,
            digests.typed_direct_terminal_projection_digest(
                job_id=job_id,
                terminal_state=self.terminal_state,
                terminal_reason=self.terminal_reason,
                m4_completion_digest=self.m4_completion_digest,
                completed_revision=self.completed_revision,
            ),
        )


@dataclass(frozen=True, slots=True)
class M5TypedDirectJobLease:
    job_id: str
    attempt_id: str | None
    lease_token_hash: str | None
    lease_expires_at: datetime | None
    dispatch_record_digest: str | None
    resulting_revision: int
    disposition: M5AcquisitionDisposition
    should_execute: bool
    exact_replay: bool
    already_completed: bool
    terminal_projection: M5TypedDirectTerminalProjection | None

    def __post_init__(self) -> None:
        _require_text("job_id", self.job_id)
        _require_int("resulting_revision", self.resulting_revision, positive=True)
        if not isinstance(self.disposition, M5AcquisitionDisposition):
            raise ValidationError("disposition must be an M5AcquisitionDisposition")
        if self.disposition is M5AcquisitionDisposition.RESULT_RESERVED:
            raise ValidationError("result_reserved is illegal for direct jobs")
        for name in ("should_execute", "exact_replay", "already_completed"):
            _require_bool(name, getattr(self, name))
        attempt_binding = (
            self.attempt_id,
            self.lease_token_hash,
            self.lease_expires_at,
        )
        if any(value is None for value in attempt_binding) and not all(
            value is None for value in attempt_binding
        ):
            raise ValidationError(
                "typed-direct attempt, token, and deadline are jointly shaped"
            )
        if self.attempt_id is not None:
            _require_text("attempt_id", self.attempt_id)
            assert self.lease_token_hash is not None
            assert self.lease_expires_at is not None
            _require_hash("lease_token_hash", self.lease_token_hash)
            _require_timestamptz("lease_expires_at", self.lease_expires_at)
        if self.dispatch_record_digest is not None:
            _require_hash("dispatch_record_digest", self.dispatch_record_digest)

        if self.disposition in {
            M5AcquisitionDisposition.DISPATCH_NEW,
            M5AcquisitionDisposition.DISPATCH_TAKEOVER,
            M5AcquisitionDisposition.LIVE_LEASE,
        }:
            if (
                self.attempt_id is None
                or self.dispatch_record_digest is None
                or self.terminal_projection is not None
                or self.already_completed
            ):
                raise ValidationError("active typed-direct lease shape is invalid")
            executing = self.disposition in {
                M5AcquisitionDisposition.DISPATCH_NEW,
                M5AcquisitionDisposition.DISPATCH_TAKEOVER,
            }
            if self.should_execute is not executing or self.exact_replay is executing:
                raise ValidationError(
                    "typed-direct execute/replay flags disagree with disposition"
                )
            return

        if (
            self.should_execute
            or not self.exact_replay
            or self.terminal_projection is None
        ):
            raise ValidationError("terminal typed-direct lease shape is invalid")
        if (self.attempt_id is None) != (self.dispatch_record_digest is None):
            raise ValidationError(
                "terminal direct attempt and dispatch are jointly present"
            )
        self.terminal_projection.validate_job(self.job_id)
        expected_completed = self.terminal_projection.terminal_state in {
            M4JobState.COMPLETED_ACTIVE,
            M4JobState.COMPLETED_INACTIVE,
        }
        if self.already_completed is not expected_completed:
            raise ValidationError(
                "already_completed must exactly project completed M4 states"
            )


@dataclass(frozen=True, slots=True)
class M5AttemptCompletionReceipt:
    logical_job_id: str
    attempt_id: str
    resulting_revision: int
    exact_replay: bool

    def __post_init__(self) -> None:
        _require_hash("logical_job_id", self.logical_job_id)
        _require_hash("attempt_id", self.attempt_id)
        _require_int("resulting_revision", self.resulting_revision, positive=True)
        _require_bool("exact_replay", self.exact_replay)


@dataclass(frozen=True, slots=True)
class M5CancellationPlan:
    structural_event_id: str
    epoch_id: int
    cancelled_job_ids: tuple[str, ...]
    reason: M5TerminalReason
    plan_digest: str

    def __post_init__(self) -> None:
        _require_text("structural_event_id", self.structural_event_id)
        _require_int("epoch_id", self.epoch_id, positive=True)
        _require_tuple("cancelled_job_ids", self.cancelled_job_ids)
        if not self.cancelled_job_ids:
            raise ValidationError("cancelled_job_ids must be nonempty")
        for job_id in self.cancelled_job_ids:
            _require_hash("cancelled job ID", job_id)
        encoded_job_ids = tuple(
            job_id.encode("utf-8") for job_id in self.cancelled_job_ids
        )
        if any(
            left >= right
            for left, right in zip(encoded_job_ids, encoded_job_ids[1:], strict=False)
        ):
            raise ValidationError(
                "cancelled_job_ids must be strictly increasing by UTF-8 byte order"
            )
        if not isinstance(self.reason, M5TerminalReason) or self.reason not in {
            M5TerminalReason.SUBJECT_INACTIVE,
            M5TerminalReason.SCOPE_RETIRED,
            M5TerminalReason.EPOCH_FAILED,
        }:
            raise ValidationError("cancellation plan reason is invalid")
        _require_identity(
            "plan_digest",
            self.plan_digest,
            digests.cancellation_plan_digest(
                structural_event_id=self.structural_event_id,
                epoch_id=self.epoch_id,
                cancelled_job_ids=self.cancelled_job_ids,
                reason=self.reason,
            ),
        )

    @classmethod
    def build(
        cls,
        *,
        structural_event_id: str,
        epoch_id: int,
        cancelled_job_ids: tuple[str, ...],
        reason: M5TerminalReason,
    ) -> M5CancellationPlan:
        return cls(
            structural_event_id=structural_event_id,
            epoch_id=epoch_id,
            cancelled_job_ids=cancelled_job_ids,
            reason=reason,
            plan_digest=digests.cancellation_plan_digest(
                structural_event_id=structural_event_id,
                epoch_id=epoch_id,
                cancelled_job_ids=cancelled_job_ids,
                reason=reason,
            ),
        )


@dataclass(frozen=True, slots=True)
class M5RootBarrierReceipt:
    requirement_root_set_hash: str
    barrier_completion_hash: str
    resulting_revision: int
    exact_replay: bool

    def __post_init__(self) -> None:
        _require_hash("requirement_root_set_hash", self.requirement_root_set_hash)
        _require_hash("barrier_completion_hash", self.barrier_completion_hash)
        _require_int("resulting_revision", self.resulting_revision, positive=True)
        _require_bool("exact_replay", self.exact_replay)


@dataclass(frozen=True, slots=True)
class M5CancellationReceipt:
    cancelled_job_ids: tuple[str, ...]
    resulting_revision: int
    exact_replay: bool

    def __post_init__(self) -> None:
        _require_sorted_unique_text("cancelled_job_ids", self.cancelled_job_ids)
        for job_id in self.cancelled_job_ids:
            _require_hash("cancelled job ID", job_id)
        _require_int("resulting_revision", self.resulting_revision, positive=True)
        _require_bool("exact_replay", self.exact_replay)


M5TypedRuntimeEvent = LegacyEvent | M5Event


@dataclass(frozen=True, slots=True)
class M5TypedEventPlan:
    structural_event_id: str
    event: M5TypedRuntimeEvent
    payload_hash: str
    direct_plan: DynamicEventPlan | None
    candidate_policy_id: str
    candidate_policy_manifest_hash: str
    requirement_registry_snapshot: RequirementRegistrySnapshot
    active_chunk_snapshot: ActiveChunkSnapshot
    expected_previous_published_epoch_id: int

    def __post_init__(self) -> None:
        _require_text("structural_event_id", self.structural_event_id)
        _require_text("candidate_policy_id", self.candidate_policy_id)
        _require_hash(
            "candidate_policy_manifest_hash", self.candidate_policy_manifest_hash
        )
        if self.candidate_policy_id != self.candidate_policy_manifest_hash:
            raise ValidationError("candidate policy ID must name its manifest hash")
        _require_int(
            "expected_previous_published_epoch_id",
            self.expected_previous_published_epoch_id,
            positive=True,
        )
        accepted_event_types = (
            InsertDocumentEvent,
            DeleteDocumentVersionEvent,
            ReplaceDocumentVersionEvent,
            PolicyChangeEvent,
            ObserveEvent,
            RegisterGroupEvent,
            ReplaceGroupEvent,
            RetireGroupEvent,
            ObserveRequirementEvent,
        )
        if not isinstance(self.event, accepted_event_types):
            raise ValidationError("typed plan event has an unsupported concrete type")
        if self.event.event_id != self.structural_event_id:
            raise ValidationError("typed plan event IDs disagree")
        if isinstance(
            self.event,
            (
                RegisterGroupEvent,
                ReplaceGroupEvent,
                RetireGroupEvent,
                ObserveRequirementEvent,
            ),
        ):
            expected_payload = m5_event_payload_digest(self.event)
        else:
            expected_payload = legacy_event_payload_digest(self.event)
        _require_identity("payload_hash", self.payload_hash, expected_payload)
        needs_direct = isinstance(
            self.event,
            (
                InsertDocumentEvent,
                DeleteDocumentVersionEvent,
                ReplaceDocumentVersionEvent,
            ),
        )
        if needs_direct != (self.direct_plan is not None):
            raise ValidationError(
                "direct_plan is present exactly for document insert/delete/replace"
            )
        if self.direct_plan is not None:
            update = self.direct_plan.update
            if (
                update.event_id != self.structural_event_id
                or update.payload_hash != self.payload_hash
                or update.candidate_policy_id != self.candidate_policy_id
                or update.previous_published_epoch_id
                != self.expected_previous_published_epoch_id
            ):
                raise ValidationError("direct plan disagrees with typed envelope")


class M5ActivationPort(Protocol):
    def activate(self, request: M5ActivationRequest) -> M5ActivationReceipt: ...


class M5TypedApplication(Protocol):
    def run_event(self, event: M5TypedEventPlan) -> M5EventRunResult: ...


__all__ = [
    "ActiveChunkSnapshot",
    "ActiveChunkSnapshotEntry",
    "M5ActivationPort",
    "M5ActivationReceipt",
    "M5ActivationRequest",
    "M5AcquisitionDisposition",
    "M5AttemptArchiveReason",
    "M5AttemptCompletionReceipt",
    "M5AttemptDisposition",
    "M5AttemptExecutionEvidence",
    "M5AttemptOutput",
    "M5AttemptResultArtifact",
    "M5CallAmbiguityReport",
    "M5CancellationPlan",
    "M5CancellationReceipt",
    "M5CandidatePolicyManifest",
    "M5ChangedStateReference",
    "M5DiscoveryDirection",
    "M5DiscoveryScopeContract",
    "M5DispatchRecord",
    "M5EventRunResult",
    "M5ExecutionEvidenceDisposition",
    "M5ExpiredAttemptReturn",
    "M5JobAttempt",
    "M5JobCompletion",
    "M5JobKind",
    "M5JobLease",
    "M5JobState",
    "M5LeaseTerminalProjection",
    "M5LogicalJobSpec",
    "M5OwnerPendingCounter",
    "M5RequirementAdmissionChannel",
    "M5RequirementAdmittedPair",
    "M5RequirementAdmittedPairSource",
    "M5RequirementChannelHit",
    "M5RequirementDiscoveryResult",
    "M5RequirementFallbackKey",
    "M5RequirementFrontierHead",
    "M5RequirementPairInput",
    "M5RequirementRootProvenance",
    "M5RequirementScopeContract",
    "M5RequirementScopeSelection",
    "M5RequirementVerifierArtifact",
    "M5RequirementWithdrawalPlan",
    "M5ReplayedOutcome",
    "M5RetrievalTermination",
    "M5RootBarrierReceipt",
    "M5RunFailureReason",
    "M5RunState",
    "M5RuntimeTiming",
    "M5RuntimeTimingCoverage",
    "M5RuntimeTimingObservation",
    "M5RuntimeOperationalConfig",
    "M5RuntimeSubgraph",
    "M5RuntimeWork",
    "M5RuntimeWorkContributionKind",
    "M5ScopeState",
    "M5StateReferenceKind",
    "M5TerminalReason",
    "M5TextNormalizerProvenance",
    "M5TypedApplication",
    "M5TypedDirectJobLease",
    "M5TypedDirectLateReturnEnvelope",
    "M5TypedDirectReturnKind",
    "M5TypedDirectScopeKind",
    "M5TypedDirectTerminalProjection",
    "M5TypedDirectVerificationExecution",
    "M5TypedEventPlan",
    "M5TypedRuntimeEvent",
    "M5TransitionTimingAnchor",
    "M5TransitionTimingReceipt",
    "M5_NORMALIZER_PROVENANCE_HASH",
    "M5_REQUIREMENT_TASK",
    "RequirementRegistrySnapshot",
    "RequirementRegistrySnapshotEntry",
    "SemanticPairKey",
    "validate_changed_state_references",
    "validate_combined_deltas",
    "validate_requirement_channel_hits",
    "validate_requirement_scope_selections",
]
