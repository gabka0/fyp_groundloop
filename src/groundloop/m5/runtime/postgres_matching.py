"""Cursor-local PostgreSQL primitives for the persisted M5 matching image.

The functions in this module never own a transaction and never commit, roll
back, or perform external work.  First application is deliberately split into
explicit cursor-local prepare, stage, and finalise phases.  The public
``apply_matching_transition`` entry point is retained for exact historical
replay only.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal, NoReturn

from psycopg import Cursor, sql

from groundloop.ai.contracts import (
    ScoreTriple,
)
from groundloop.ai.contracts import (
    VerificationResult as AIVerificationResult,
)
from groundloop.ai.contracts import stable_digest as stable_ai_digest
from groundloop.ai.verification.adapter import logits_to_score_triple
from groundloop.domain import (
    AnswerStatus,
    ClaimStatus,
    DecisionPolicy,
    ModelStamp,
    SemanticObservation,
    StatusDelta,
    SubjectKind,
    VerificationLabel,
    normalized_text_hash,
)
from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    PairKey,
    VectorIndexKind,
)
from groundloop.m4.contracts import (
    CandidatePolicyManifest as M4CandidatePolicyManifest,
)
from groundloop.m4.models.contracts import (
    PairVerificationArtifact,
    PairVerificationInput,
    decision_policy_hash,
    derive_operational_label,
)
from groundloop.m4.models.ports import verification_artifact_payload_hash
from groundloop.m5.claim_certificates import WorkingClaimCertificateBinding
from groundloop.m5.digests import hash_field, text_field
from groundloop.m5.domain import (
    ClaimCertificateArtifact,
    ClaimSupportKind,
    CombinedAnswerState,
    CombinedClaimState,
    GroupCertificateRow,
    GroupMatchingCertificateArtifact,
    GroupState,
    RequirementState,
)
from groundloop.m5.incremental_overlay import M5OverlayWork
from groundloop.m5.matching import (
    HallMaskState,
    HashMaskTransition,
    MatchingWorkCounters,
    WorkingGroupCertificateBinding,
    affected_group_matching,
    apply_hash_mask_transitions,
    initialize_hall_mask_state,
    reconstruct_certificate,
    touched_state_work,
)
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    MATCHING_WORK_COUNTER_NAMES,
    M5MatchingEdgeChange,
    M5MatchingEdgeCurrent,
    M5MatchingEdgePoint,
    M5MatchingEdgeWorking,
    M5MatchingGroupShape,
    M5MatchingHallChange,
    M5MatchingHallCurrent,
    M5MatchingHallPoint,
    M5MatchingHallWorking,
    M5MatchingImagePoint,
    M5MatchingLayer,
    M5MatchingMaskChange,
    M5MatchingMaskCurrent,
    M5MatchingMaskPoint,
    M5MatchingMaskWorking,
    M5MatchingObservationChange,
    M5MatchingObservationCurrent,
    M5MatchingObservationPoint,
    M5MatchingObservationWorking,
    M5PersistedBindingKind,
    M5PersistedCertificateBindingRow,
    M5PersistedLogicalChange,
    M5PersistedLogicalChangeKind,
    M5PersistedLogicalOverlayPatch,
    M5PersistedMatchingContribution,
    M5PersistedMatchingPatch,
    M5PersistedMatchingPatchArtifact,
    M5PersistedMatchingPatchReceipt,
    M5PersistedMatchingSourceKind,
    M5PersistedMatchingTransitionIntent,
    M5TypedDirectVerificationExecution,
    m5_overlay_work_values,
)

_PERSISTED_MATCHING_BUNDLE_ROW = (
    "m5-persisted-matching-schema-bundle-v1",
    "52240e19968926d0c051fe6146b3c7d877cf582014341efbfcc78637f3ff5761",
    "e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c",
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565",
)
_EMPTY_LOGICAL_OUTPUT_BYTES = bytes.fromhex(
    "710000000000000002000000000000002573000000000000001c"
    "6d352d6f7665726c61792d6c6f676963616c2d6f75747075742d7632"
    "0000000000000009710000000000000000"
)
_EMPTY_LOGICAL_OUTPUT_DIGEST = (
    "b4e641b66a06cb7d204377c37cfe031d958ce6d959832620fc2e9441339581c3"
)
_EMPTY_OUTPUT_BYTES = 71
_MATCHING_ARTIFACT_LOCK_NAMESPACE = 1_295_338_832  # signed int32 for b"M5MP"
_MATCHING_ABSENCE_LOCK_NAMESPACE = 1_295_338_833


class _MatchingPhase(StrEnum):
    READY_TO_ADVANCE = "ready_to_advance"
    STAGED_THROUGH_TIER_12 = "staged_through_tier_12"
    STAGED_THROUGH_TIER_13 = "staged_through_tier_13"
    STAGED = "staged"
    CONSUMED = "consumed"


@dataclass(frozen=True, slots=True)
class _CursorBinding:
    """Non-serialisable identity of the one authorised outer transaction."""

    cursor_object_identity: int
    backend_identity: int
    transaction_identity: int
    session_role: str
    matching_context_identity: int
    matching_journal_identity: int
    matching_expected_identity: int
    epoch_id: int
    expected_revision: int
    resulting_revision: int
    source_kind: M5PersistedMatchingSourceKind
    source_id: str
    source_identity_hash: str


@dataclass(frozen=True, slots=True)
class _D24OwnedWriteCounts:
    """The five physical write coordinates owned by D24 (and no alias)."""

    group_state_write_count: int = 0
    claim_state_write_count: int = 0
    answer_state_write_count: int = 0
    certificate_binding_write_count: int = 0
    public_delta_write_count: int = 0


@dataclass(frozen=True, slots=True)
class _MatchingWritePlan:
    """Complete lower-tier after-image plan derived before the first write."""

    observation_currency_rows: tuple[_ObservationCurrencyWrite, ...] = ()
    observation_rows: tuple[M5MatchingObservationWorking, ...] = ()
    edge_rows: tuple[M5MatchingEdgeWorking, ...] = ()
    mask_rows: tuple[M5MatchingMaskWorking, ...] = ()
    hall_rows: tuple[M5MatchingHallWorking, ...] = ()
    requirement_state_rows: tuple[_RequirementStateWrite, ...] = ()
    group_state_rows: tuple[_GroupStateWrite, ...] = ()
    claim_state_rows: tuple[_ClaimStateWrite, ...] = ()
    answer_state_rows: tuple[CombinedAnswerState, ...] = ()
    group_certificate_artifact_rows: tuple[GroupMatchingCertificateArtifact, ...] = ()
    claim_certificate_artifact_rows: tuple[ClaimCertificateArtifact, ...] = ()
    group_binding_rows: tuple[WorkingGroupCertificateBinding, ...] = ()
    claim_binding_rows: tuple[WorkingClaimCertificateBinding, ...] = ()
    document_direct_plan: _DocumentDirectPlan | None = None
    expected_direct_m4_claim_after_images: tuple[_DirectStageImage, ...] = ()
    expected_direct_m4_answer_after_images: tuple[_DirectStageImage, ...] = ()


@dataclass(frozen=True, slots=True)
class _MatchingStageCoordinate:
    """One exact migration-017 journal coordinate in stage order."""

    relation_name: str
    key_columns: tuple[str, ...]
    key_parts: tuple[object, ...]

    @property
    def journal_key(self) -> tuple[str, bytes]:
        return (
            self.relation_name,
            _matching_journal_key_preimage(self.relation_name, self.key_parts),
        )


@dataclass(frozen=True, slots=True)
class _MatchingStageBeforeImage:
    """Locked first-old image for one planned stage mutation."""

    coordinate: _MatchingStageCoordinate
    row_json: object | None


@dataclass(frozen=True, slots=True)
class _ObservationCurrencyWrite:
    """Exact tier-11a immutable observation-currency delta."""

    epoch_id: int
    subject_id: str
    chunk_version_id: str
    observation_id: str | None
    valid_from_revision: int
    valid_to_revision: int | None
    subject_kind: str = "requirement"
    task_type: str = "verify_requirement_v1"


@dataclass(frozen=True, slots=True)
class _ObservationCurrencyBeforeImage:
    """Held tier-11a source/currency bytes captured before D25 stage."""

    key: tuple[int, str, str, str, str]
    semantic_observation_rows: tuple[tuple[object, ...], ...]
    current_currency_row: tuple[object, ...] | None
    published_currency_row: tuple[object, ...] | None
    working_currency_row: tuple[object, ...] | None


@dataclass(frozen=True, slots=True)
class _RequirementCompletionSource:
    """Read-only projection of the caller-held tier-9/10 source closure."""

    epoch_id: int
    expected_runtime_revision: int
    resulting_revision: int
    attempt_id: str
    attempt_result_artifact_hash: str
    logical_job_id: str
    observation_id: str
    requirement_version_id: str
    group_version_id: str
    group_family_id: str
    owner_claim_id: str
    answer_version_id: str
    owner_claim_required: bool
    requirement_ordinal: int
    chunk_version_id: str
    text_hash: str
    operational_label: VerificationLabel
    decision_policy_version: str
    previous_published_epoch_id: int


@dataclass(frozen=True, slots=True)
class _RequirementCurrencyProjection:
    """Pre-tier-11 locator result; it conveys no row-lock authority."""

    base_observation_id: str | None
    base_operational_label: VerificationLabel | None


@dataclass(frozen=True, slots=True)
class _BoundedCertificateView:
    """The frozen bounded representative image, never a full group scan."""

    epoch_id: int
    revision: int
    decision_policy_version: str
    group_version_id: str
    requirement_version_ids: tuple[str, ...]
    candidates: tuple[tuple[str, int], ...]
    selected_observations: tuple[tuple[int, str, str], ...]
    histogram: tuple[int, ...]

    @property
    def requirement_count(self) -> int:
        return len(self.requirement_version_ids)

    def representative_hash_masks(self) -> tuple[tuple[str, int], ...]:
        return self.candidates

    def edge_active(self, requirement_ordinal: int, text_hash: str) -> bool:
        return any(
            candidate_hash == text_hash and mask & (1 << requirement_ordinal)
            for candidate_hash, mask in self.candidates
        )

    def least_observation_id(
        self, requirement_ordinal: int, text_hash: str
    ) -> str | None:
        return next(
            (
                observation_id
                for ordinal, candidate_hash, observation_id in (
                    self.selected_observations
                )
                if ordinal == requirement_ordinal and candidate_hash == text_hash
            ),
            None,
        )

    def observation_active(
        self,
        requirement_ordinal: int,
        text_hash: str,
        observation_id: str,
    ) -> bool:
        return (
            requirement_ordinal,
            text_hash,
            observation_id,
        ) in self.selected_observations

    def hall_histogram(self) -> tuple[int, ...]:
        return self.histogram


@dataclass(frozen=True, slots=True)
class _RequirementIntentPreview:
    affected: dict[str, object]
    currency_projection: _RequirementCurrencyProjection
    certificate_view: _BoundedCertificateView
    certificate_artifact: GroupMatchingCertificateArtifact | None
    observation_before: tuple[M5MatchingObservationPoint, ...]
    edge_before: M5MatchingEdgePoint | None
    edge_after_refcount: int
    mask_before: M5MatchingMaskPoint | None
    mask_after: int
    hall_before: M5MatchingHallPoint
    hall_after: HallMaskState
    hall_work: MatchingWorkCounters


@dataclass(frozen=True, slots=True)
class _DocumentEdgeTransition:
    before: M5MatchingEdgeCurrent
    after_refcount: int


@dataclass(frozen=True, slots=True)
class _DocumentMaskTransition:
    before: M5MatchingMaskCurrent
    after_mask: int


@dataclass(frozen=True, slots=True)
class _DocumentGroupPreview:
    shape: M5MatchingGroupShape
    hall_before: M5MatchingHallCurrent
    hall_after: HallMaskState
    hall_work: MatchingWorkCounters
    certificate_view: _BoundedCertificateView
    certificate_artifact: GroupMatchingCertificateArtifact | None


@dataclass(frozen=True, slots=True)
class _DocumentWithdrawalPreview:
    currency_before_images: tuple[_ObservationCurrencyBeforeImage, ...]
    removed_observations: tuple[M5MatchingObservationCurrent, ...]
    edge_transitions: tuple[_DocumentEdgeTransition, ...]
    mask_transitions: tuple[_DocumentMaskTransition, ...]
    groups: tuple[_DocumentGroupPreview, ...]


@dataclass(frozen=True, slots=True)
class _DocumentDirectClaimBefore:
    """One exact published M4 direct claim point and its owner metadata."""

    claim_id: str
    answer_version_id: str
    required: bool
    support_count: int
    refute_count: int
    best_support_score: float | None
    best_refute_score: float | None
    supporting_observation_ids: tuple[str, ...]
    refuting_observation_ids: tuple[str, ...]
    status: ClaimStatus
    certificate_digest: str


@dataclass(frozen=True, slots=True)
class _DocumentDirectAnswerBefore:
    """One exact published M4 direct answer point."""

    state: CombinedAnswerState


@dataclass(frozen=True, slots=True)
class _DocumentDirectClaimPlan:
    """Exact current-direct claim before/after image for one withdrawal."""

    claim_id: str
    answer_version_id: str
    required: bool
    before_state: CombinedClaimState
    before_certificate_digest: str
    after_state: CombinedClaimState
    after_certificate_digest: str
    withdrawn_observation_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _DocumentDirectAnswerPlan:
    """Exact direct-M4 answer before/after image after coalesced claim removal."""

    before_state: CombinedAnswerState
    after_state: CombinedAnswerState


@dataclass(frozen=True, slots=True)
class _DocumentDirectPlan:
    """Bounded point-derived direct state affected by one document withdrawal."""

    claims: tuple[_DocumentDirectClaimPlan, ...] = ()
    answers: tuple[_DocumentDirectAnswerPlan, ...] = ()
    withdrawn_observations: tuple[SemanticObservation, ...] = ()
    remaining_observations: tuple[SemanticObservation, ...] = ()
    observation_text_hashes: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class _EffectiveGroupBinding:
    certificate_digest: str
    valid_from_revision: int
    working: bool


@dataclass(frozen=True, slots=True)
class _EffectiveClaimBinding:
    certificate_digest: str
    valid_from_revision: int
    working: bool


@dataclass(frozen=True, slots=True)
class _RequirementLogicalTransition:
    requirement_before: _RequirementStateWrite
    requirement_after: _RequirementStateWrite
    group_before: _GroupStateWrite
    group_after: _GroupStateWrite
    claim_before: _ClaimStateWrite
    claim_after: _ClaimStateWrite
    answer_before: CombinedAnswerState
    answer_after: CombinedAnswerState
    claim_artifact: ClaimCertificateArtifact
    status_deltas: tuple[StatusDelta, ...]


@dataclass(frozen=True, slots=True)
class _ClaimStateWrite:
    state: CombinedClaimState
    decision_policy_version: str
    certificate_digest: str


@dataclass(frozen=True, slots=True)
class _RequirementStateWrite:
    state: RequirementState
    decision_policy_version: str


@dataclass(frozen=True, slots=True)
class _GroupStateWrite:
    state: GroupState
    decision_policy_version: str
    certificate_digest: str | None


@dataclass(frozen=True, slots=True)
class _BaseHeaderAfterImage:
    epoch_id: int
    revision: int
    structural_status: str
    semantic_status: str
    evaluation_state: str
    publication_mode: str
    sealed: bool
    open_job_count: int
    open_scope_count: int


@dataclass(frozen=True, slots=True)
class _RuntimeHeaderAfterImage:
    epoch_id: int
    revision: int
    runtime_state: str
    open_work_count: int
    open_scope_count: int
    blocking_failure_count: int
    terminal: bool


@dataclass(frozen=True, slots=True)
class _DirectAffectedProjection:
    """Pre-source D25 coordinates with deliberately no source authority."""

    group_shapes: tuple[M5MatchingGroupShape, ...] = ()
    observation_ids: tuple[str, ...] = ()
    edge_keys: tuple[tuple[str, int, str, str], ...] = ()
    mask_keys: tuple[tuple[str, str], ...] = ()
    hall_group_ids: tuple[str, ...] = ()
    requirement_state_ids: tuple[str, ...] = ()
    group_state_ids: tuple[str, ...] = ()
    claim_state_ids: tuple[str, ...] = ()
    answer_state_ids: tuple[str, ...] = ()
    group_certificate_ids: tuple[str, ...] = ()
    claim_certificate_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _DirectPrecursor:
    """Byte-total store projection used to derive direct source authority."""

    epoch_id: int
    expected_revision: int
    resulting_revision: int
    job_id: str
    attempt_id: str
    parent_job_id: str | None
    job_kind: str
    candidate_policy_id: str
    candidate_policy: M4CandidatePolicyManifest
    registry_snapshot_id: str
    decision_policy_version: str
    support_threshold: float
    refute_threshold: float
    payload_hash: str
    execution_spec_hash: str
    result_artifact_id: str
    result_artifact_hash: str
    attempt_ordinal: int
    source_id: str
    source_identity_hash: str
    scope_delta: int
    claim_job_deltas: tuple[tuple[str, int], ...]
    answer_job_deltas: tuple[tuple[str, int], ...]
    child_job_ids: tuple[str, ...]
    claim_id: str | None
    answer_version_id: str | None
    owner_claim_required: bool | None
    chunk_version_id: str | None
    observation_id: str | None
    admitted_pair_id: str | None
    model_authority: tuple[object, ...] | None
    prompt_authority: tuple[object, ...] | None


@dataclass(frozen=True, slots=True)
class _DirectStageCoordinate:
    """One exact M4/D24 coordinate reserved before the private M4 stage."""

    relation_name: str
    key_columns: tuple[str, ...]
    key_parts: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class _DirectStageImage:
    """The first-old or final-new JSON image for a reserved coordinate."""

    coordinate: _DirectStageCoordinate
    row_json: object | None


@dataclass(frozen=True, slots=True)
class _DirectAuthorityImage:
    """Immutable exact rows that the M4 stage is not authorized to mutate."""

    authority_name: str
    key_parts: tuple[object, ...]
    rows: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class _DirectM4MutationRecord:
    """One lexical Lane-M statement result, captured at its DML edge."""

    coordinate: _DirectStageCoordinate
    first_old: object | None
    final_new: object | None
    statement_rowcount: int


@dataclass(frozen=True, slots=True)
class _DirectLogicalPlan:
    """Combined-state result frozen from precursor and held before images."""

    claim_before: _ClaimStateWrite | None
    claim_after: _ClaimStateWrite | None
    answer_before: CombinedAnswerState | None
    answer_after: CombinedAnswerState | None
    claim_artifact: ClaimCertificateArtifact | None
    claim_binding_before: _EffectiveClaimBinding | None
    claim_artifact_already_stored: bool
    base_observation_id: str | None
    status_deltas: tuple[StatusDelta, ...]


@dataclass(frozen=True, slots=True)
class _StructuralLogicalPresence:
    requirement_state_ids: tuple[str, ...] = ()
    group_state_ids: tuple[str, ...] = ()
    group_certificate_ids: tuple[str, ...] = ()
    claim_state_ids: tuple[str, ...] = ()
    claim_certificate_ids: tuple[str, ...] = ()
    answer_state_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _PreparedAuthoritySnapshot:
    """Deep immutable copy of every field that may authorize D25 writes."""

    snapshot_identity: int
    binding: _CursorBinding
    prepared_identity: int
    official_intent: M5PersistedMatchingTransitionIntent
    prewrite_matching_revision: int
    matching_image_base_epoch_id: int
    matching_image_base_revision: int
    prewrite_patch_artifact: M5PersistedMatchingPatchArtifact
    physical_and_logical_write_plan: _MatchingWritePlan
    stage_before_images: tuple[_MatchingStageBeforeImage, ...]
    observation_currency_before_images: tuple[_ObservationCurrencyBeforeImage, ...]
    base_header_after_image: _BaseHeaderAfterImage
    runtime_header_after_image: _RuntimeHeaderAfterImage
    d25_contribution_work: M5OverlayWork
    d24_owned_planned_write_counts: _D24OwnedWriteCounts
    requirement_state_write_count_diagnostic: int
    expected_patch_digest: str | None
    expected_work: M5OverlayWork | None
    direct_reservation_or_none: _DirectMatchingReservation | None
    direct_m4_stage_evidence_or_none: _DirectM4StageResult | None


@dataclass(slots=True)
class _PreparedMatchingTransition:
    """Single-use, same-cursor authority for one D25 first application."""

    binding: _CursorBinding
    prepared_identity: int
    official_intent: M5PersistedMatchingTransitionIntent
    prewrite_matching_revision: int
    matching_image_base_epoch_id: int
    matching_image_base_revision: int
    prewrite_patch_artifact: M5PersistedMatchingPatchArtifact
    physical_and_logical_write_plan: _MatchingWritePlan
    stage_before_images: tuple[_MatchingStageBeforeImage, ...]
    observation_currency_before_images: tuple[_ObservationCurrencyBeforeImage, ...]
    base_header_after_image: _BaseHeaderAfterImage
    runtime_header_after_image: _RuntimeHeaderAfterImage
    d25_contribution_work: M5OverlayWork
    d24_owned_planned_write_counts: _D24OwnedWriteCounts
    requirement_state_write_count_diagnostic: int
    authority_snapshot: _PreparedAuthoritySnapshot | None = None
    expected_patch_digest: str | None = None
    expected_work: M5OverlayWork | None = None
    direct_reservation_or_none: _DirectMatchingReservation | None = None
    direct_m4_stage_evidence_or_none: _DirectM4StageResult | None = None
    phase: _MatchingPhase = _MatchingPhase.READY_TO_ADVANCE


@dataclass(frozen=True, slots=True)
class _DirectMatchingReservation:
    """Explicit direct-only pre-source reservation; never persisted or cached."""

    binding: _CursorBinding
    reservation_identity: int
    precursor: _DirectPrecursor
    d25_projection: _DirectAffectedProjection
    logical_plan: _DirectLogicalPlan
    stage_coordinates: tuple[_DirectStageCoordinate, ...]
    remainder_coordinates: tuple[_DirectStageCoordinate, ...]
    before_images: tuple[_DirectStageImage, ...]
    before_image_authority: tuple[object, ...]
    conflict_before_images: tuple[_DirectAuthorityImage, ...]
    authority_before_images: tuple[_DirectAuthorityImage, ...]
    base_header_before_image: _BaseHeaderAfterImage
    runtime_header_before_image: _RuntimeHeaderAfterImage
    phase: Literal["reserved", "consumed"] = "reserved"


@dataclass(frozen=True, slots=True)
class _DirectM4StageResult:
    """Lexical evidence returned by the private store-derived M4 stage."""

    cursor_object_identity: int
    backend_identity: int
    transaction_identity: int
    matching_context_identity: int
    matching_journal_identity: int
    matching_expected_identity: int
    reservation_identity: int
    stage_result_identity: int
    source_id: str
    source_identity_hash: str
    mutation_records: tuple[_DirectM4MutationRecord, ...]
    first_old_by_key: tuple[_DirectStageImage, ...]
    final_new_by_key: tuple[_DirectStageImage, ...]
    actual_write_counts: tuple[tuple[str, int], ...]
    phase: Literal["staged", "consumed"] = "staged"


def _capture_prepared_authority_snapshot(
    prepared: _PreparedMatchingTransition,
    *,
    direct_consumed: bool = False,
) -> _PreparedAuthoritySnapshot:
    """Copy prepared authority so later caller mutation is compare-only visible."""

    reservation = deepcopy(prepared.direct_reservation_or_none)
    stage_result = deepcopy(prepared.direct_m4_stage_evidence_or_none)
    if direct_consumed:
        if (
            type(reservation) is not _DirectMatchingReservation
            or type(stage_result) is not _DirectM4StageResult
            or reservation.phase != "reserved"
            or stage_result.phase != "staged"
        ):
            raise EventConflictError("direct prepared evidence changed before sealing")
        object.__setattr__(reservation, "phase", "consumed")
        object.__setattr__(stage_result, "phase", "consumed")
    snapshot = _PreparedAuthoritySnapshot(
        snapshot_identity=0,
        binding=deepcopy(prepared.binding),
        prepared_identity=prepared.prepared_identity,
        official_intent=deepcopy(prepared.official_intent),
        prewrite_matching_revision=prepared.prewrite_matching_revision,
        matching_image_base_epoch_id=prepared.matching_image_base_epoch_id,
        matching_image_base_revision=prepared.matching_image_base_revision,
        prewrite_patch_artifact=deepcopy(prepared.prewrite_patch_artifact),
        physical_and_logical_write_plan=deepcopy(
            prepared.physical_and_logical_write_plan
        ),
        stage_before_images=deepcopy(prepared.stage_before_images),
        observation_currency_before_images=deepcopy(
            prepared.observation_currency_before_images
        ),
        base_header_after_image=deepcopy(prepared.base_header_after_image),
        runtime_header_after_image=deepcopy(prepared.runtime_header_after_image),
        d25_contribution_work=deepcopy(prepared.d25_contribution_work),
        d24_owned_planned_write_counts=deepcopy(
            prepared.d24_owned_planned_write_counts
        ),
        requirement_state_write_count_diagnostic=(
            prepared.requirement_state_write_count_diagnostic
        ),
        expected_patch_digest=prepared.expected_patch_digest,
        expected_work=deepcopy(prepared.expected_work),
        direct_reservation_or_none=reservation,
        direct_m4_stage_evidence_or_none=stage_result,
    )
    object.__setattr__(snapshot, "snapshot_identity", id(snapshot))
    return snapshot


def _seal_prepared_authority(
    prepared: _PreparedMatchingTransition,
    *,
    direct_consumed: bool = False,
) -> None:
    if prepared.authority_snapshot is not None:
        raise EventConflictError("matching prepared authority was already sealed")
    prepared.authority_snapshot = _capture_prepared_authority_snapshot(
        prepared,
        direct_consumed=direct_consumed,
    )


def _require_positive_int(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValidationError(f"{name} must be a positive integer")


def _require_nonnegative_int(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValidationError(f"{name} must be a nonnegative integer")


def _require_text(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be non-empty text")


def _require_sha256(name: str, value: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _text(value: object) -> str:
    """Decode a PostgreSQL text value without changing its identity bytes."""

    return str(value)


def _sha256_text(value: object) -> str:
    """Decode a fixed-width PostgreSQL digest, removing only CHAR padding."""

    digest = str(value).rstrip(" ")
    _require_sha256("persisted digest", digest)
    return digest


def _matching_journal_key_preimage(
    relation_name: str, key_parts: Sequence[object]
) -> bytes:
    """Mirror migration-017's immutable length-prefixed journal-key recipe."""

    payload = bytearray()
    for part in (relation_name, *(_text(value) for value in key_parts)):
        encoded = part.encode("utf-8")
        payload.extend(len(encoded).to_bytes(8, byteorder="big", signed=True))
        payload.extend(encoded)
    return bytes(payload)


def _matching_absence_lock_key(relation_name: str, key_parts: Sequence[object]) -> int:
    preimage = b"m5-d28-matching-absence-v1" + _matching_journal_key_preimage(
        relation_name, key_parts
    )
    key = int.from_bytes(hashlib.sha256(preimage).digest()[:4], "big")
    return key if key < 2**31 else key - 2**32


def _reserve_matching_absence(
    cursor: Cursor[Any], relation_name: str, key_parts: Sequence[object]
) -> None:
    cursor.execute(
        "SELECT pg_advisory_xact_lock(%s, %s)",
        (
            _MATCHING_ABSENCE_LOCK_NAMESPACE,
            _matching_absence_lock_key(relation_name, key_parts),
        ),
    )


def _require_matching_absence_reservation(
    cursor: Cursor[Any], relation_name: str, key_parts: Sequence[object]
) -> None:
    key = _matching_absence_lock_key(relation_name, key_parts)
    row = cursor.execute(
        """
        SELECT count(*)
        FROM pg_locks
        WHERE locktype = 'advisory'
          AND pid = pg_backend_pid()
          AND classid::bigint = %s
          AND objid::bigint = %s
          AND objsubid = 2
          AND mode = 'ExclusiveLock'
          AND granted
        """,
        (
            _MATCHING_ABSENCE_LOCK_NAMESPACE & 0xFFFFFFFF,
            key & 0xFFFFFFFF,
        ),
    ).fetchone()
    if row is None or int(row[0]) != 1:
        raise EventConflictError(
            "matching insert-on-absence coordinate is not reserved"
        )


def _tuple_ints(value: object) -> tuple[int, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValidationError("persisted Hall array has an invalid shape")
    return tuple(int(item) for item in value)


def require_persisted_matching_bundle(cursor: Cursor[Any]) -> None:
    """Require the literal accepted migration-017 five-field ledger tuple."""

    row = cursor.execute(
        """
        SELECT bundle_id, bundle_sha256, migration_sha256,
               oracle_sha256, prerequisite_sha256
        FROM groundloop_m5_schema_bundle
        WHERE bundle_id = %s
        """,
        (_PERSISTED_MATCHING_BUNDLE_ROW[0],),
    ).fetchone()
    actual = (
        None
        if row is None
        else (
            _text(row[0]),
            _sha256_text(row[1]),
            _sha256_text(row[2]),
            _sha256_text(row[3]),
            _sha256_text(row[4]),
        )
    )
    if actual != _PERSISTED_MATCHING_BUNDLE_ROW:
        raise InvalidEventError(
            "typed M5 matching requires the exact accepted migration-017 bundle"
        )


def _transition_context_row(cursor: Cursor[Any]) -> tuple[Any, ...]:
    row = cursor.execute(
        """
        SELECT pg_backend_pid(), pg_current_xact_id()::text,
               session_user,
               nullif(current_setting(
                 'groundloop.m5_matching_context_oid', true), '')::oid,
               nullif(current_setting(
                 'groundloop.m5_matching_journal_oid', true), '')::oid,
               nullif(current_setting(
                 'groundloop.m5_matching_expected_oid', true), '')::oid,
               nullif(current_setting(
                 'groundloop.m5_matching_mode', true), ''),
               nullif(current_setting(
                 'groundloop.m5_matching_epoch_id', true), '')::bigint,
               nullif(current_setting(
                 'groundloop.m5_matching_expected_revision', true), '')::bigint,
               nullif(current_setting(
                 'groundloop.m5_matching_resulting_revision', true), '')::bigint,
               nullif(current_setting(
                 'groundloop.m5_matching_source_kind', true), ''),
               nullif(current_setting(
                 'groundloop.m5_matching_source_id', true), ''),
               to_regclass(
                 'pg_temp.groundloop_m5_matching_transition_context')::oid,
               to_regclass(
                 'pg_temp.groundloop_m5_matching_change_journal')::oid,
               to_regclass(
                 'pg_temp.groundloop_m5_matching_expected_changes')::oid
        """
    ).fetchone()
    if row is None:
        raise ValidationError("persisted matching transition context is missing")
    return tuple(row)


def _capture_cursor_binding(
    cursor: Cursor[Any], intent: M5PersistedMatchingTransitionIntent
) -> _CursorBinding:
    row = _transition_context_row(cursor)
    expected_revision = (
        1
        if intent.source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN
        else intent.before_revision
    )
    if (
        row[6] != "transition"
        or row[3] is None
        or row[4] is None
        or row[5] is None
        or row[7] is None
        or row[8] is None
        or row[9] is None
        or row[10] is None
        or row[11] is None
        or int(row[7]) != intent.resulting_epoch_id
        or int(row[8]) != expected_revision
        or int(row[9]) != intent.resulting_revision
        or row[10] != intent.source_kind.value
        or row[11] != intent.source_id
        or row[3] != row[12]
        or row[4] != row[13]
        or row[5] != row[14]
    ):
        raise ValidationError("persisted matching transition context changed")
    context = cursor.execute(
        """
        SELECT backend_pid, transaction_id::text, session_role, epoch_id,
               expected_revision, resulting_revision, source_kind, source_id,
               validation_started, validation_done
        FROM pg_temp.groundloop_m5_matching_transition_context
        """
    ).fetchall()
    if len(context) != 1 or (
        int(context[0][0]) != int(row[0])
        or str(context[0][1]) != str(row[1])
        or str(context[0][2]) != str(row[2])
        or int(context[0][3]) != intent.resulting_epoch_id
        or int(context[0][4]) != expected_revision
        or int(context[0][5]) != intent.resulting_revision
        or str(context[0][6]) != intent.source_kind.value
        or str(context[0][7]) != intent.source_id
        or bool(context[0][8])
        or bool(context[0][9])
    ):
        raise ValidationError("persisted matching transition context is not pristine")
    return _CursorBinding(
        cursor_object_identity=id(cursor),
        backend_identity=int(row[0]),
        transaction_identity=int(row[1]),
        session_role=str(row[2]),
        matching_context_identity=int(row[3]),
        matching_journal_identity=int(row[4]),
        matching_expected_identity=int(row[5]),
        epoch_id=intent.resulting_epoch_id,
        expected_revision=expected_revision,
        resulting_revision=intent.resulting_revision,
        source_kind=intent.source_kind,
        source_id=intent.source_id,
        source_identity_hash=intent.source_identity_hash,
    )


def _validate_cursor_binding(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    binding: _CursorBinding,
) -> None:
    if type(binding) is not _CursorBinding or binding.cursor_object_identity != id(
        cursor
    ):
        raise ValidationError("prepared matching authority belongs to another cursor")
    current = _capture_cursor_binding(cursor, intent)
    if current != binding:
        raise ValidationError("prepared matching authority changed transaction context")


def _capture_direct_context_binding(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    resulting_revision: int,
    source_id: str,
    source_identity_hash: str,
) -> _CursorBinding:
    """Capture direct compare-only context without constructing an intent."""

    row = _transition_context_row(cursor)
    if (
        any(row[index] is None for index in range(3, 12))
        or row[6] != "transition"
        or int(row[7]) != epoch_id
        or int(row[8]) != expected_revision
        or int(row[9]) != resulting_revision
        or _text(row[10]) != M5PersistedMatchingSourceKind.DIRECT_TRANSITION.value
        or _text(row[11]) != source_id
        or row[3] != row[12]
        or row[4] != row[13]
        or row[5] != row[14]
    ):
        raise ValidationError("direct matching transition context changed")
    context = cursor.execute(
        """
        SELECT backend_pid, transaction_id::text, session_role, epoch_id,
               expected_revision, resulting_revision, source_kind, source_id,
               validation_started, validation_done
        FROM pg_temp.groundloop_m5_matching_transition_context
        """
    ).fetchall()
    if len(context) != 1 or (
        int(context[0][0]) != int(row[0])
        or str(context[0][1]) != str(row[1])
        or str(context[0][2]) != str(row[2])
        or int(context[0][3]) != epoch_id
        or int(context[0][4]) != expected_revision
        or int(context[0][5]) != resulting_revision
        or str(context[0][6]) != M5PersistedMatchingSourceKind.DIRECT_TRANSITION.value
        or str(context[0][7]) != source_id
        or bool(context[0][8])
        or bool(context[0][9])
    ):
        raise ValidationError("direct matching transition context is not pristine")
    return _CursorBinding(
        cursor_object_identity=id(cursor),
        backend_identity=int(row[0]),
        transaction_identity=int(row[1]),
        session_role=str(row[2]),
        matching_context_identity=int(row[3]),
        matching_journal_identity=int(row[4]),
        matching_expected_identity=int(row[5]),
        epoch_id=epoch_id,
        expected_revision=expected_revision,
        resulting_revision=resulting_revision,
        source_kind=M5PersistedMatchingSourceKind.DIRECT_TRANSITION,
        source_id=source_id,
        source_identity_hash=source_identity_hash,
    )


def _validate_direct_context_binding(
    cursor: Cursor[Any], binding: _CursorBinding
) -> None:
    if type(binding) is not _CursorBinding or binding.cursor_object_identity != id(
        cursor
    ):
        raise ValidationError("direct matching authority belongs to another cursor")
    current = _capture_direct_context_binding(
        cursor,
        epoch_id=binding.epoch_id,
        expected_revision=binding.expected_revision,
        resulting_revision=binding.resulting_revision,
        source_id=binding.source_id,
        source_identity_hash=binding.source_identity_hash,
    )
    if current != binding:
        raise ValidationError("direct matching transaction context changed")


def _authorize_matching_transition(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_runtime_revision: int,
    resulting_revision: int,
    source_kind: M5PersistedMatchingSourceKind,
    source_id: str,
) -> None:
    """Install migration-017 authority once for a caller-owned transaction.

    Structural callers must already hold their tier-5 source prefix.  Later
    callers must already hold the operation-specific tier-6 serializer.
    """

    require_persisted_matching_bundle(cursor)
    _authorize_checked_prefix(
        cursor,
        epoch_id=epoch_id,
        expected_revision=expected_runtime_revision,
    )
    cursor.execute(
        "SELECT groundloop_m5_authorize_persisted_matching_transition(%s,%s,%s,%s,%s)",
        (
            epoch_id,
            expected_runtime_revision,
            resulting_revision,
            source_kind.value,
            source_id,
        ),
    )


def _authorize_checked_prefix(
    cursor: Cursor[Any], *, epoch_id: int, expected_revision: int
) -> None:
    """Acquire the tier-6 CAS prefix and bind a transaction-local read scope."""

    cursor.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
        (epoch_id, expected_revision),
    )
    cursor.execute(
        """
        SELECT set_config(
                 'groundloop.m5_matching_reader_epoch_id', %s, true
               ),
               set_config(
                 'groundloop.m5_matching_reader_revision', %s, true
               ),
               set_config(
                 'groundloop.m5_matching_reader_backend_pid',
                 pg_backend_pid()::text, true
               ),
               set_config(
                 'groundloop.m5_matching_reader_transaction_id',
                 pg_current_xact_id()::text, true
               )
        """,
        (str(epoch_id), str(expected_revision)),
    )


def _require_checked_prefix(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int | None = None,
    reauthorize: bool = True,
) -> tuple[int, int]:
    """Assert the scoped epoch and re-enter the real lock/CAS authorizer.

    The reader settings bind this module's cursor-local scope, but are never
    accepted as proof of a held row lock.  Only the migration-015 authorizer
    can establish that proof; invoking it again is idempotent when the caller
    already owns the tier-5/tier-6 rows and safely acquires them otherwise.
    """

    setting = cursor.execute(
        """
        SELECT current_setting('groundloop.m5_checked_transition', true),
               current_setting(
                 'groundloop.m5_matching_reader_epoch_id', true
               ),
               current_setting(
                 'groundloop.m5_matching_reader_revision', true
               ),
               current_setting(
                 'groundloop.m5_matching_reader_backend_pid', true
               ),
               current_setting(
                 'groundloop.m5_matching_reader_transaction_id', true
               ),
               pg_backend_pid()::text,
               pg_current_xact_id()::text
        """
    ).fetchone()
    if (
        setting is None
        or setting[0] != "on"
        or setting[1] != str(epoch_id)
        or not setting[2]
        or setting[3] != setting[5]
        or setting[4] != setting[6]
    ):
        raise ValidationError(
            "matching point read lacks the exact checked epoch prefix"
        )
    scoped_revision = int(setting[2])
    if expected_revision is not None and scoped_revision != expected_revision:
        raise EventConflictError("matching point read has a different scoped revision")
    if reauthorize:
        cursor.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (epoch_id, scoped_revision),
        )
    row = cursor.execute(
        """
        SELECT epoch.revision, runtime.revision
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        WHERE epoch.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise InvalidEventError("matching point read names an unknown typed epoch")
    revisions = (int(row[0]), int(row[1]))
    if revisions[0] != revisions[1] or revisions[0] != scoped_revision:
        raise EventConflictError("matching point read lost its epoch/runtime CAS")
    return revisions


def _requirement_completion_source(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_runtime_revision: int,
    resulting_revision: int,
    proposed_attempt_id: str,
    expected_source_identity_hash: str | None,
    headers_installed: bool = False,
) -> _RequirementCompletionSource:
    """Byte-check, but never acquire, the caller-held tier-9/10 closure.

    The C2 outer transaction owns source persistence and row locks before the
    sole public derivation call.  Every query here is deliberately read-only:
    acquiring or reacquiring a tier-9/10 lock from this module would invert the
    frozen D28 order.
    """

    if resulting_revision != expected_runtime_revision + 1:
        raise EventConflictError(
            "requirement matching source must advance exactly one revision"
        )
    job_hint = cursor.execute(
        """
        SELECT logical_job_id
        FROM groundloop_m5_job_attempt
        WHERE attempt_id = %s
        """,
        (proposed_attempt_id,),
    ).fetchone()
    if job_hint is None:
        raise InvalidEventError("requirement matching source attempt is absent")
    logical_job_id = _sha256_text(job_hint[0])
    job = cursor.execute(
        """
        SELECT epoch_id, job_kind, job_state, result_artifact_id,
               result_artifact_hash, completed_revision, payload_hash,
               execution_spec_hash, semantic_pair_digest, subject_kind,
               subject_id, chunk_version_id, candidate_policy_id
        FROM groundloop_m5_semantic_job
        WHERE logical_job_id = %s AND epoch_id = %s
        """,
        (logical_job_id, epoch_id),
    ).fetchone()
    attempt = cursor.execute(
        """
        SELECT logical_job_id, attempt_state, attempt_output_digest,
               execution_spec_hash
        FROM groundloop_m5_job_attempt
        WHERE attempt_id = %s AND logical_job_id = %s
        """,
        (proposed_attempt_id, logical_job_id),
    ).fetchone()
    artifact = cursor.execute(
        """
        SELECT attempt_result_artifact_hash, disposition, logical_job_id,
               job_epoch_id, attempt_output_digest, execution_spec_hash,
               payload_hash, job_state_at_receipt, job_state_after,
               result_artifact_id, result_artifact_hash,
               activity_snapshot_epoch_id, activity_snapshot_revision,
               epoch_active, chunk_active, requirement_active, group_active,
               archive_reason, cancelled_by_event_id, cancelled_by_epoch_id,
               cancellation_reason
        FROM groundloop_m5_attempt_result_artifact
        WHERE attempt_id = %s AND job_epoch_id = %s
        """,
        (proposed_attempt_id, epoch_id),
    ).fetchone()
    if job is None or attempt is None or artifact is None:
        raise InvalidEventError("requirement matching source closure is incomplete")
    execution = cursor.execute(
        """
        SELECT observation_id, artifact_id, artifact_hash, logical_job_id,
               attempt_id, pair_input_hash, decision_policy_version,
               decision_policy_hash, eligible_for_currency, produced_epoch_id
        FROM groundloop_m5_requirement_verifier_execution
        WHERE logical_job_id = %s AND attempt_id = %s
        """,
        (logical_job_id, proposed_attempt_id),
    ).fetchone()
    if execution is None:
        raise InvalidEventError("requirement matching execution is absent")
    verifier = cursor.execute(
        """
        SELECT artifact_id, artifact_hash, execution_spec_hash,
               semantic_pair_digest, subject_kind, subject_id,
               chunk_version_id, pair_input_hash, decision_policy_version,
               decision_policy_hash, raw_output_hash, operational_label
        FROM groundloop_m5_requirement_verifier_artifact
        WHERE artifact_id = %s AND artifact_hash = %s
        """,
        (_text(execution[1]), _sha256_text(execution[2])),
    ).fetchone()
    pair = cursor.execute(
        """
        SELECT pair_input_hash, subject_kind, subject_id, chunk_version_id,
               semantic_pair_digest, candidate_policy_id, owner_claim_id,
               group_version_id, group_family_id, requirement_ordinal,
               m5_chunk_text_hash
        FROM groundloop_m5_requirement_pair_input
        WHERE pair_input_hash = %s
        """,
        (_sha256_text(execution[5]),),
    ).fetchone()
    observation = cursor.execute(
        """
        SELECT observation_id, subject_kind, subject_id, chunk_version_id,
               task_type, input_hash, produced_epoch, raw_output_hash,
               eligible_for_currency
        FROM groundloop_semantic_observation
        WHERE observation_id = %s
        """,
        (_text(execution[0]),),
    ).fetchone()
    authority = cursor.execute(
        """
        SELECT typed_update.previous_published_epoch_id,
               typed_update.decision_policy_version,
               runtime.candidate_policy_id,
               typed_policy.decision_policy_version,
               epoch.revision, runtime.revision
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        JOIN groundloop_m5_candidate_policy AS typed_policy
          ON typed_policy.candidate_policy_id = runtime.candidate_policy_id
        WHERE epoch.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    structure = cursor.execute(
        """
        SELECT requirement.group_version_id, requirement.ordinal,
               group_version.group_family_id, family.claim_id,
               claim.answer_version_id, claim.required
        FROM groundloop_m5_requirement_version AS requirement
        JOIN groundloop_m5_group_version AS group_version
          USING (group_version_id)
        JOIN groundloop_m5_group_family AS family USING (group_family_id)
        JOIN groundloop_claim AS claim ON claim.claim_id = family.claim_id
        WHERE requirement.requirement_version_id = %s
        """,
        (_text(pair[2]) if pair is not None else "",),
    ).fetchone()
    if (
        verifier is None
        or pair is None
        or observation is None
        or authority is None
        or structure is None
    ):
        raise InvalidEventError("requirement matching immutable closure is incomplete")

    source_identity_hash = _sha256_text(artifact[0])
    if expected_source_identity_hash is not None and (
        source_identity_hash != expected_source_identity_hash
    ):
        raise EventConflictError("requirement matching source hash changed")
    observation_id = _text(execution[0])
    requirement_id = _text(pair[2])
    chunk_id = _text(pair[3])
    decision_policy_version = _text(authority[1])
    if (
        _text(job[1]) != "verify_requirement_pair"
        or _text(job[2]) != "running"
        or job[3] is not None
        or job[4] is not None
        or job[5] is not None
        or _text(job[9]) != "requirement"
        or _text(job[10]) != requirement_id
        or _text(job[11]) != chunk_id
        or _sha256_text(job[8]) != _sha256_text(pair[4])
        or _text(job[12]) != _text(pair[5])
        or _sha256_text(attempt[0]) != logical_job_id
        or _text(attempt[1]) != "result_reserved"
        or attempt[2] is None
        or _sha256_text(attempt[2]) != _sha256_text(artifact[4])
        or _sha256_text(attempt[3]) != _sha256_text(job[7])
        or _text(artifact[1]) != "verifier_completed_active"
        or _sha256_text(artifact[2]) != logical_job_id
        or int(artifact[3]) != epoch_id
        or _sha256_text(artifact[5]) != _sha256_text(job[7])
        or _sha256_text(artifact[6]) != _sha256_text(job[6])
        or _text(artifact[7]) != "running"
        or _text(artifact[8]) != "completed_active"
        or int(artifact[11]) != epoch_id
        or int(artifact[12]) != expected_runtime_revision
        or not all(bool(artifact[index]) for index in range(13, 17))
        or any(artifact[index] is not None for index in range(17, 21))
        or _text(execution[3]) != logical_job_id
        or _text(execution[4]) != proposed_attempt_id
        or _text(execution[1]) != _text(artifact[9])
        or _sha256_text(execution[2]) != _sha256_text(artifact[10])
        or _text(execution[6]) != decision_policy_version
        or not bool(execution[8])
        or int(execution[9]) != epoch_id
        or _text(verifier[0]) != _text(execution[1])
        or _sha256_text(verifier[1]) != _sha256_text(execution[2])
        or _sha256_text(verifier[2]) != _sha256_text(job[7])
        or _sha256_text(verifier[3]) != _sha256_text(job[8])
        or _text(verifier[4]) != "requirement"
        or _text(verifier[5]) != requirement_id
        or _text(verifier[6]) != chunk_id
        or _sha256_text(verifier[7]) != _sha256_text(pair[0])
        or _text(verifier[8]) != decision_policy_version
        or _sha256_text(verifier[9]) != _sha256_text(execution[7])
        or _sha256_text(pair[0]) != _sha256_text(execution[5])
        or _text(pair[1]) != "requirement"
        or _text(observation[0]) != observation_id
        or _text(observation[1]) != "requirement"
        or _text(observation[2]) != requirement_id
        or _text(observation[3]) != chunk_id
        or _text(observation[4]) != "verify_requirement_v1"
        or _sha256_text(observation[5]) != _sha256_text(pair[0])
        or int(observation[6]) != epoch_id
        or _sha256_text(observation[7]) != _sha256_text(verifier[10])
        or not bool(observation[8])
        or _text(authority[2]) != _text(pair[5])
        or _text(authority[3]) != decision_policy_version
        or int(authority[4])
        != (resulting_revision if headers_installed else expected_runtime_revision)
        or int(authority[5])
        != (resulting_revision if headers_installed else expected_runtime_revision)
        or _text(structure[0]) != _text(pair[7])
        or int(structure[1]) != int(pair[9])
        or _text(structure[2]) != _text(pair[8])
        or _text(structure[3]) != _text(pair[6])
    ):
        raise EventConflictError("requirement matching source closure changed")
    try:
        label = VerificationLabel(_text(verifier[11]))
    except ValueError as error:
        raise EventConflictError("requirement matching source label changed") from error
    return _RequirementCompletionSource(
        epoch_id=epoch_id,
        expected_runtime_revision=expected_runtime_revision,
        resulting_revision=resulting_revision,
        attempt_id=proposed_attempt_id,
        attempt_result_artifact_hash=source_identity_hash,
        logical_job_id=logical_job_id,
        observation_id=observation_id,
        requirement_version_id=requirement_id,
        group_version_id=_text(pair[7]),
        group_family_id=_text(pair[8]),
        owner_claim_id=_text(pair[6]),
        answer_version_id=_text(structure[4]),
        owner_claim_required=bool(structure[5]),
        requirement_ordinal=int(pair[9]),
        chunk_version_id=chunk_id,
        text_hash=_sha256_text(pair[10]),
        operational_label=label,
        decision_policy_version=decision_policy_version,
        previous_published_epoch_id=int(authority[0]),
    )


def _requirement_currency_projection(
    cursor: Cursor[Any], source: _RequirementCompletionSource
) -> _RequirementCurrencyProjection:
    """Gather the exact currency locator before the tier-11 lock sequence."""

    rows = cursor.execute(
        """
        SELECT currency.observation_id, artifact.operational_label
        FROM groundloop_m5_currency_at(%s, %s) AS currency
        LEFT JOIN groundloop_m5_requirement_verifier_execution AS execution
          ON execution.observation_id = currency.observation_id
        LEFT JOIN groundloop_m5_requirement_verifier_artifact AS artifact
          ON artifact.artifact_id = execution.artifact_id
         AND artifact.artifact_hash = execution.artifact_hash
        WHERE currency.subject_kind = 'requirement'
          AND currency.subject_id = %s
          AND currency.chunk_version_id = %s
          AND currency.task_type = 'verify_requirement_v1'
        """,
        (
            source.epoch_id,
            source.expected_runtime_revision,
            source.requirement_version_id,
            source.chunk_version_id,
        ),
    ).fetchall()
    if len(rows) > 1:
        raise EventConflictError("requirement observation currency is not unique")
    if not rows:
        return _RequirementCurrencyProjection(None, None)
    if rows[0][0] is None:
        if rows[0][1] is not None:
            raise EventConflictError("requirement tombstone currency changed")
        return _RequirementCurrencyProjection(None, None)
    if rows[0][1] is None:
        raise EventConflictError("requirement currency verifier closure changed")
    try:
        label = VerificationLabel(_text(rows[0][1]))
    except ValueError as error:
        raise EventConflictError(
            "requirement base observation label changed"
        ) from error
    return _RequirementCurrencyProjection(_text(rows[0][0]), label)


def _capture_requirement_currency_before_image(
    cursor: Cursor[Any],
    source: _RequirementCompletionSource,
    projection: _RequirementCurrencyProjection,
    *,
    lock: bool,
) -> _ObservationCurrencyBeforeImage:
    """Capture tier-11a bytes, locking only on the sole derive call."""

    suffix = " FOR UPDATE" if lock else ""
    observation_ids = tuple(
        sorted(
            {
                source.observation_id,
                *(
                    ()
                    if projection.base_observation_id is None
                    else (projection.base_observation_id,)
                ),
            }
        )
    )
    observation_rows: list[tuple[object, ...]] = []
    for observation_id in observation_ids:
        observation = cursor.execute(
            """
            SELECT observation_id, subject_kind, subject_id, chunk_version_id,
                   task_type, input_hash, produced_epoch, raw_output_hash,
                   eligible_for_currency
            FROM groundloop_semantic_observation
            WHERE observation_id = %s
            """
            + suffix,
            (observation_id,),
        ).fetchone()
        if observation is None or (
            _text(observation[0]) != observation_id
            or _text(observation[1]) != "requirement"
            or _text(observation[2]) != source.requirement_version_id
            or _text(observation[3]) != source.chunk_version_id
            or _text(observation[4]) != "verify_requirement_v1"
            or not bool(observation[8])
            or (
                observation_id == source.observation_id
                and int(observation[6]) != source.epoch_id
            )
        ):
            raise EventConflictError("requirement semantic observation changed")
        observation_rows.append(tuple(observation))

    key = (
        source.epoch_id,
        "requirement",
        source.requirement_version_id,
        source.chunk_version_id,
        "verify_requirement_v1",
    )
    if lock:
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_working_currency_history",
            (*key, source.resulting_revision),
        )
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_working_currency_history#open",
            key,
        )
    target = cursor.execute(
        """
        SELECT epoch_id, subject_kind, subject_id, chunk_version_id,
               task_type, observation_id, valid_from_revision,
               valid_to_revision
        FROM groundloop_m5_working_currency_history
        WHERE epoch_id = %s AND subject_kind = %s AND subject_id = %s
          AND chunk_version_id = %s AND task_type = %s
          AND valid_from_revision = %s
        """
        + suffix,
        (*key, source.resulting_revision),
    ).fetchone()
    if target is not None:
        raise EventConflictError("requirement currency target appeared")
    working = cursor.execute(
        """
        SELECT epoch_id, subject_kind, subject_id, chunk_version_id,
               task_type, observation_id, valid_from_revision,
               valid_to_revision
        FROM groundloop_m5_working_currency_history
        WHERE epoch_id = %s AND subject_kind = %s AND subject_id = %s
          AND chunk_version_id = %s AND task_type = %s
          AND valid_to_revision IS NULL
        """
        + suffix,
        key,
    ).fetchone()
    current = cursor.execute(
        """
        SELECT subject_kind, subject_id, chunk_version_id, task_type,
               observation_id, installed_revision
        FROM groundloop_observation_currency
        WHERE subject_kind = %s AND subject_id = %s
          AND chunk_version_id = %s AND task_type = %s
        """
        + suffix,
        key[1:],
    ).fetchone()
    published = cursor.execute(
        """
        SELECT subject_kind, subject_id, chunk_version_id, task_type,
               observation_id, valid_from_epoch, valid_to_epoch
        FROM groundloop_published_observation_currency
        WHERE subject_kind = 'requirement' AND subject_id = %s
          AND chunk_version_id = %s AND task_type = 'verify_requirement_v1'
          AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """
        + suffix,
        (
            source.requirement_version_id,
            source.chunk_version_id,
            source.previous_published_epoch_id,
            source.previous_published_epoch_id,
        ),
    ).fetchall()
    if len(published) > 1:
        raise EventConflictError("requirement published currency changed")
    published_row = None if not published else tuple(published[0])
    current_row = None if current is None else tuple(current)
    if (
        current_row is not None
        and (
            _text(current_row[0]),
            _text(current_row[1]),
            _text(current_row[2]),
            _text(current_row[3]),
        )
        != key[1:]
    ):
        raise EventConflictError("requirement current currency key changed")
    if working is not None:
        actual_base = None if working[5] is None else _text(working[5])
    elif current_row is not None:
        actual_base = None if current_row[4] is None else _text(current_row[4])
    elif published_row is not None:
        actual_base = None if published_row[4] is None else _text(published_row[4])
    else:
        actual_base = None
    if actual_base != projection.base_observation_id:
        raise EventConflictError("requirement base currency changed")
    if working is not None and (
        int(working[6]) > source.expected_runtime_revision or working[7] is not None
    ):
        raise EventConflictError("requirement working currency changed")
    return _ObservationCurrencyBeforeImage(
        key=key,
        semantic_observation_rows=tuple(observation_rows),
        current_currency_row=current_row,
        published_currency_row=published_row,
        working_currency_row=None if working is None else tuple(working),
    )


def _lock_requirement_matching_image(
    cursor: Cursor[Any], source: _RequirementCompletionSource
) -> int:
    """Acquire current then working image rows at the frozen tier 11b."""

    current = cursor.execute(
        """
        SELECT decision_policy_version, installed_epoch_id, installed_revision
        FROM groundloop_m5_matching_image_current
        WHERE singleton
        FOR UPDATE
        """
    ).fetchone()
    working = cursor.execute(
        """
        SELECT base_epoch_id, base_revision, decision_policy_version,
               updated_revision
        FROM groundloop_m5_matching_image_working
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (source.epoch_id,),
    ).fetchone()
    if (
        current is None
        or working is None
        or (
            _text(current[0]) != source.decision_policy_version
            or int(working[0]) != int(current[1])
            or int(working[1]) != int(current[2])
            or _text(working[2]) != source.decision_policy_version
            or int(working[3]) > source.expected_runtime_revision
        )
    ):
        raise EventConflictError("requirement matching image changed")
    return int(working[3])


def _resolved_requirement_observation_point(
    cursor: Cursor[Any], epoch_id: int, observation_id: str
) -> M5MatchingObservationPoint | None:
    """Read one already-serialized point without acquiring another lock."""

    working = cursor.execute(
        """
        SELECT epoch_id, observation_id, requirement_version_id,
               group_version_id, requirement_ordinal, text_hash,
               present, updated_revision
        FROM groundloop_m5_matching_observation_working
        WHERE epoch_id = %s AND observation_id = %s
        """,
        (epoch_id, observation_id),
    ).fetchone()
    if working is not None:
        return M5MatchingObservationWorking(
            M5MatchingLayer.WORKING,
            int(working[0]),
            _text(working[1]),
            _text(working[2]),
            _text(working[3]),
            int(working[4]),
            _sha256_text(working[5]),
            bool(working[6]),
            int(working[7]),
        )
    current = cursor.execute(
        """
        SELECT observation_id, requirement_version_id, group_version_id,
               requirement_ordinal, text_hash, installed_epoch_id,
               installed_revision
        FROM groundloop_m5_matching_observation_current
        WHERE observation_id = %s
        """,
        (observation_id,),
    ).fetchone()
    if current is None:
        return None
    return M5MatchingObservationCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        _text(current[1]),
        _text(current[2]),
        int(current[3]),
        _sha256_text(current[4]),
        int(current[5]),
        int(current[6]),
    )


def _resolved_requirement_edge_point(
    cursor: Cursor[Any],
    epoch_id: int,
    requirement_version_id: str,
    text_hash: str,
) -> M5MatchingEdgePoint | None:
    working = cursor.execute(
        """
        SELECT epoch_id, requirement_version_id, text_hash,
               group_version_id, requirement_ordinal, refcount,
               updated_revision
        FROM groundloop_m5_matching_edge_working
        WHERE epoch_id = %s AND requirement_version_id = %s
          AND text_hash = %s
        """,
        (epoch_id, requirement_version_id, text_hash),
    ).fetchone()
    if working is not None:
        return M5MatchingEdgeWorking(
            M5MatchingLayer.WORKING,
            int(working[0]),
            _text(working[1]),
            _sha256_text(working[2]),
            _text(working[3]),
            int(working[4]),
            int(working[5]),
            int(working[6]),
        )
    current = cursor.execute(
        """
        SELECT requirement_version_id, text_hash, group_version_id,
               requirement_ordinal, refcount, installed_epoch_id,
               installed_revision
        FROM groundloop_m5_matching_edge_current
        WHERE requirement_version_id = %s AND text_hash = %s
        """,
        (requirement_version_id, text_hash),
    ).fetchone()
    if current is None:
        return None
    return M5MatchingEdgeCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        _sha256_text(current[1]),
        _text(current[2]),
        int(current[3]),
        int(current[4]),
        int(current[5]),
        int(current[6]),
    )


def _resolved_requirement_mask_point(
    cursor: Cursor[Any], epoch_id: int, group_version_id: str, text_hash: str
) -> M5MatchingMaskPoint | None:
    working = cursor.execute(
        """
        SELECT epoch_id, group_version_id, text_hash, mask, updated_revision
        FROM groundloop_m5_matching_hash_mask_working
        WHERE epoch_id = %s AND group_version_id = %s AND text_hash = %s
        """,
        (epoch_id, group_version_id, text_hash),
    ).fetchone()
    if working is not None:
        return M5MatchingMaskWorking(
            M5MatchingLayer.WORKING,
            int(working[0]),
            _text(working[1]),
            _sha256_text(working[2]),
            int(working[3]),
            int(working[4]),
        )
    current = cursor.execute(
        """
        SELECT group_version_id, text_hash, mask,
               installed_epoch_id, installed_revision
        FROM groundloop_m5_matching_hash_mask_current
        WHERE group_version_id = %s AND text_hash = %s
        """,
        (group_version_id, text_hash),
    ).fetchone()
    if current is None:
        return None
    return M5MatchingMaskCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        _sha256_text(current[1]),
        int(current[2]),
        int(current[3]),
        int(current[4]),
    )


def _resolved_requirement_hall_point(
    cursor: Cursor[Any], epoch_id: int, group_version_id: str
) -> M5MatchingHallPoint | None:
    working = cursor.execute(
        """
        SELECT epoch_id, group_version_id, present, requirement_count,
               mask_histogram, neighbor_counts, deficiencies,
               maximum_deficiency, matching_size, distinct_hash_count,
               updated_revision
        FROM groundloop_m5_matching_hall_working
        WHERE epoch_id = %s AND group_version_id = %s
        """,
        (epoch_id, group_version_id),
    ).fetchone()
    if working is not None:
        present = bool(working[2])
        return M5MatchingHallWorking(
            M5MatchingLayer.WORKING,
            int(working[0]),
            _text(working[1]),
            present,
            None if working[3] is None else int(working[3]),
            None if working[4] is None else _tuple_ints(working[4]),
            None if working[5] is None else _tuple_ints(working[5]),
            None if working[6] is None else _tuple_ints(working[6]),
            None if working[7] is None else int(working[7]),
            None if working[8] is None else int(working[8]),
            None if working[9] is None else int(working[9]),
            int(working[10]),
        )
    current = cursor.execute(
        """
        SELECT group_version_id, requirement_count, mask_histogram,
               neighbor_counts, deficiencies, maximum_deficiency,
               matching_size, distinct_hash_count, installed_epoch_id,
               installed_revision
        FROM groundloop_m5_matching_hall_current
        WHERE group_version_id = %s
        """,
        (group_version_id,),
    ).fetchone()
    if current is None:
        return None
    return M5MatchingHallCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        int(current[1]),
        _tuple_ints(current[2]),
        _tuple_ints(current[3]),
        _tuple_ints(current[4]),
        int(current[5]),
        int(current[6]),
        int(current[7]),
        int(current[8]),
        int(current[9]),
    )


def _hall_state_from_point(point: M5MatchingHallPoint) -> HallMaskState:
    if isinstance(point, M5MatchingHallWorking) and not point.present:
        raise EventConflictError("requirement completion names a retired group")
    assert point.requirement_count is not None
    assert point.mask_histogram is not None
    assert point.neighbor_counts is not None
    assert point.deficiencies is not None
    assert point.maximum_deficiency is not None
    assert point.matching_size is not None
    assert point.distinct_hash_count is not None
    return HallMaskState(
        point.requirement_count,
        point.mask_histogram,
        point.neighbor_counts,
        point.deficiencies,
        point.maximum_deficiency,
        point.matching_size,
        point.distinct_hash_count,
    )


def _requirement_label_supports(label: VerificationLabel | None) -> bool:
    return label is VerificationLabel.SUPPORT


def _requirement_representative_candidates(
    cursor: Cursor[Any],
    *,
    source: _RequirementCompletionSource,
    requirement_count: int,
    changed_mask: int,
    hall_after: HallMaskState,
) -> tuple[tuple[str, int], ...]:
    """Perform the one bounded mask-representative discovery for this source."""

    rows = cursor.execute(
        """
        WITH effective AS (
          SELECT working.text_hash::text AS text_hash, working.mask
          FROM groundloop_m5_matching_hash_mask_working AS working
          WHERE working.epoch_id = %s
            AND working.group_version_id = %s
            AND working.text_hash <> %s AND working.mask > 0
          UNION ALL
          SELECT current_row.text_hash::text AS text_hash, current_row.mask
          FROM groundloop_m5_matching_hash_mask_current AS current_row
          WHERE current_row.group_version_id = %s
            AND current_row.text_hash <> %s
            AND NOT EXISTS (
              SELECT 1
              FROM groundloop_m5_matching_hash_mask_working AS shadow
              WHERE shadow.epoch_id = %s
                AND shadow.group_version_id = current_row.group_version_id
                AND shadow.text_hash = current_row.text_hash
            )
        ), patched AS (
          SELECT text_hash, mask FROM effective
          UNION ALL
          SELECT %s::text, %s::integer WHERE %s > 0
        ), ranked AS (
          SELECT text_hash, mask,
                 row_number() OVER (
                   PARTITION BY mask ORDER BY text_hash COLLATE "C"
                 ) AS representative_ordinal
          FROM patched
        )
        SELECT text_hash, mask
        FROM ranked
        WHERE representative_ordinal <= %s
        ORDER BY text_hash COLLATE "C"
        """,
        (
            source.epoch_id,
            source.group_version_id,
            source.text_hash,
            source.group_version_id,
            source.text_hash,
            source.epoch_id,
            source.text_hash,
            changed_mask,
            changed_mask,
            requirement_count,
        ),
    ).fetchall()
    candidates = tuple((_sha256_text(row[0]), int(row[1])) for row in rows)
    if len({text_hash for text_hash, _ in candidates}) != len(candidates):
        raise EventConflictError("requirement representative hashes changed")
    actual_by_mask = Counter(mask for _, mask in candidates)
    expected_by_mask = Counter(
        {
            mask: min(hall_after.mask_histogram[mask], requirement_count)
            for mask in range(1, 1 << requirement_count)
            if hall_after.mask_histogram[mask]
        }
    )
    if actual_by_mask != expected_by_mask:
        raise EventConflictError(
            "requirement representative discovery changed cardinality"
        )
    return candidates


def _requirement_selected_observations(
    cursor: Cursor[Any],
    *,
    source: _RequirementCompletionSource,
    projection: _RequirementCurrencyProjection,
    pairs: Sequence[tuple[int, str]],
) -> tuple[tuple[int, str, str], ...]:
    """Read one bounded least observation for each selected matching edge."""

    if not pairs:
        return ()
    ordinals = [ordinal for ordinal, _ in pairs]
    hashes = [text_hash for _, text_hash in pairs]
    old_observation_id = projection.base_observation_id or ""
    rows = cursor.execute(
        """
        WITH selected(requirement_ordinal, text_hash) AS (
          SELECT * FROM unnest(%s::integer[], %s::text[])
        ), effective AS (
          SELECT working.requirement_ordinal,
                 working.text_hash::text AS text_hash,
                 working.observation_id
          FROM groundloop_m5_matching_observation_working AS working
          JOIN selected USING (requirement_ordinal, text_hash)
          WHERE working.epoch_id = %s
            AND working.group_version_id = %s
            AND working.present
            AND working.observation_id <> %s
            AND working.observation_id <> %s
          UNION ALL
          SELECT current_row.requirement_ordinal,
                 current_row.text_hash::text AS text_hash,
                 current_row.observation_id
          FROM groundloop_m5_matching_observation_current AS current_row
          JOIN selected USING (requirement_ordinal, text_hash)
          WHERE current_row.group_version_id = %s
            AND current_row.observation_id <> %s
            AND current_row.observation_id <> %s
            AND NOT EXISTS (
              SELECT 1
              FROM groundloop_m5_matching_observation_working AS shadow
              WHERE shadow.epoch_id = %s
                AND shadow.observation_id = current_row.observation_id
            )
          UNION ALL
          SELECT %s::integer, %s::text, %s::text
          WHERE %s
        )
        SELECT DISTINCT ON (requirement_ordinal, text_hash COLLATE "C")
               requirement_ordinal, text_hash, observation_id
        FROM effective
        ORDER BY requirement_ordinal, text_hash COLLATE "C",
                 observation_id COLLATE "C"
        """,
        (
            ordinals,
            hashes,
            source.epoch_id,
            source.group_version_id,
            old_observation_id,
            source.observation_id,
            source.group_version_id,
            old_observation_id,
            source.observation_id,
            source.epoch_id,
            source.requirement_ordinal,
            source.text_hash,
            source.observation_id,
            _requirement_label_supports(source.operational_label),
        ),
    ).fetchall()
    selected = tuple((int(row[0]), _sha256_text(row[1]), _text(row[2])) for row in rows)
    expected = tuple(sorted(pairs))
    if tuple((ordinal, text_hash) for ordinal, text_hash, _ in selected) != expected:
        raise EventConflictError(
            "requirement representative observation discovery changed"
        )
    return selected


def _preliminary_complete_group_ids(
    cursor: Cursor[Any], source: _RequirementCompletionSource
) -> tuple[str, ...]:
    working = cursor.execute(
        """
        SELECT complete_group_ids
        FROM groundloop_m5_working_claim_state
        WHERE epoch_id = %s AND claim_id = %s
        """,
        (source.epoch_id, source.owner_claim_id),
    ).fetchone()
    if working is not None:
        return tuple(_text(value) for value in working[0])
    published = cursor.execute(
        """
        SELECT complete_group_ids
        FROM groundloop_m5_published_claim_state
        WHERE claim_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (
            source.owner_claim_id,
            source.previous_published_epoch_id,
            source.previous_published_epoch_id,
        ),
    ).fetchall()
    if len(published) != 1:
        raise EventConflictError("requirement owner claim state changed")
    return tuple(_text(value) for value in published[0][0])


def _requirement_intent_preview(
    cursor: Cursor[Any],
    *,
    source: _RequirementCompletionSource,
    projection: _RequirementCurrencyProjection,
    shape: M5MatchingGroupShape,
    old_complete_groups: tuple[str, ...],
) -> _RequirementIntentPreview:
    """Derive the bounded after-image and the complete transient lock plan."""

    if (
        shape.group_version_id != source.group_version_id
        or shape.requirements[source.requirement_ordinal][1]
        != source.requirement_version_id
    ):
        raise EventConflictError("requirement group shape changed")
    old_support = _requirement_label_supports(projection.base_operational_label)
    new_support = _requirement_label_supports(source.operational_label)
    old_observation_id = projection.base_observation_id
    observation_before: list[M5MatchingObservationPoint] = []
    if old_support:
        if old_observation_id is None:
            raise EventConflictError("supporting base currency has no observation")
        point = _resolved_requirement_observation_point(
            cursor, source.epoch_id, old_observation_id
        )
        if (
            point is None
            or (isinstance(point, M5MatchingObservationWorking) and not point.present)
            or (
                point.requirement_version_id != source.requirement_version_id
                or point.group_version_id != source.group_version_id
                or point.requirement_ordinal != source.requirement_ordinal
                or point.text_hash != source.text_hash
            )
        ):
            raise EventConflictError("requirement base matching observation changed")
        observation_before.append(point)
    if (
        _resolved_requirement_observation_point(
            cursor, source.epoch_id, source.observation_id
        )
        is not None
    ):
        raise EventConflictError("requirement result observation already matched")

    edge_before = _resolved_requirement_edge_point(
        cursor,
        source.epoch_id,
        source.requirement_version_id,
        source.text_hash,
    )
    if edge_before is not None and (
        edge_before.group_version_id != source.group_version_id
        or edge_before.requirement_ordinal != source.requirement_ordinal
    ):
        raise EventConflictError("requirement matching edge changed")
    old_refcount = 0 if edge_before is None else edge_before.refcount
    edge_after_refcount = old_refcount - int(old_support) + int(new_support)
    if edge_after_refcount < 0:
        raise EventConflictError("requirement matching edge underflowed")
    if (old_refcount > 0) != old_support and old_observation_id is not None:
        # Another active observation may retain the edge.  Only an absent edge
        # while withdrawing SUPPORT is impossible.
        if old_support and old_refcount == 0:
            raise EventConflictError("supporting currency lacks its matching edge")

    mask_before = _resolved_requirement_mask_point(
        cursor, source.epoch_id, source.group_version_id, source.text_hash
    )
    old_mask = 0 if mask_before is None else mask_before.mask
    bit = 1 << source.requirement_ordinal
    if bool(old_mask & bit) != (old_refcount > 0):
        raise EventConflictError("requirement edge and mask changed inconsistently")
    mask_after = old_mask | bit if edge_after_refcount > 0 else old_mask & ~bit
    hall_before = _resolved_requirement_hall_point(
        cursor, source.epoch_id, source.group_version_id
    )
    if hall_before is None:
        raise EventConflictError("requirement group Hall state is absent")
    old_hall = _hall_state_from_point(hall_before)
    if old_hall.requirement_count != shape.requirement_count:
        raise EventConflictError("requirement Hall shape changed")
    if old_mask == mask_after:
        hall_after = old_hall
        hall_work = MatchingWorkCounters()
    else:
        hall_result = apply_hash_mask_transitions(
            old_hall,
            (HashMaskTransition(source.text_hash, old_mask, mask_after),),
        )
        hall_after = hall_result.state
        hall_work = hall_result.work

    candidates = _requirement_representative_candidates(
        cursor,
        source=source,
        requirement_count=shape.requirement_count,
        changed_mask=mask_after,
        hall_after=hall_after,
    )
    matching = affected_group_matching(shape.requirement_count, candidates)
    selected = _requirement_selected_observations(
        cursor,
        source=source,
        projection=projection,
        pairs=tuple(
            (pair.requirement_ordinal, pair.text_hash) for pair in matching.pairs
        )
        if matching.complete
        else (),
    )
    view = _BoundedCertificateView(
        epoch_id=source.epoch_id,
        revision=source.resulting_revision,
        decision_policy_version=source.decision_policy_version,
        group_version_id=source.group_version_id,
        requirement_version_ids=tuple(
            requirement_id for _, requirement_id in shape.requirements
        ),
        candidates=candidates,
        selected_observations=selected,
        histogram=hall_after.mask_histogram,
    )
    reconstruction = reconstruct_certificate(view)
    if reconstruction.matching != matching:
        raise AssertionError("bounded matching reconstruction changed")

    observation_ids = {observation_id for _, _, observation_id in selected}
    if old_support:
        assert old_observation_id is not None
        observation_ids.add(old_observation_id)
    if new_support:
        observation_ids.add(source.observation_id)
    edge_keys = {
        (
            source.group_version_id,
            source.requirement_ordinal,
            source.text_hash,
            source.requirement_version_id,
        )
    }
    edge_keys.update(
        (
            source.group_version_id,
            ordinal,
            text_hash,
            shape.requirements[ordinal][1],
        )
        for ordinal, text_hash, _ in selected
    )
    mask_keys = {
        (source.group_version_id, source.text_hash),
        *((source.group_version_id, text_hash) for text_hash, _ in candidates),
    }
    if (source.group_version_id in old_complete_groups) != old_hall.complete:
        raise EventConflictError("requirement claim/group completion index changed")
    after_complete_groups = set(old_complete_groups)
    if hall_after.complete:
        after_complete_groups.add(source.group_version_id)
    else:
        after_complete_groups.discard(source.group_version_id)
    selected_after_group = (
        None if not after_complete_groups else min(after_complete_groups)
    )
    group_certificate_ids = {source.group_version_id}
    if selected_after_group is not None:
        group_certificate_ids.add(selected_after_group)
    affected: dict[str, object] = {
        "group_shapes": (shape,),
        "observation_ids": tuple(sorted(observation_ids)),
        "edge_keys": tuple(sorted(edge_keys)),
        "mask_keys": tuple(sorted(mask_keys)),
        "hall_group_ids": (source.group_version_id,),
        "requirement_state_ids": (source.requirement_version_id,),
        "group_state_ids": (source.group_version_id,),
        "claim_state_ids": (source.owner_claim_id,),
        "answer_state_ids": (source.answer_version_id,),
        "group_certificate_ids": tuple(sorted(group_certificate_ids)),
        "claim_certificate_ids": (source.owner_claim_id,),
    }
    return _RequirementIntentPreview(
        affected=affected,
        currency_projection=projection,
        certificate_view=view,
        certificate_artifact=reconstruction.artifact,
        observation_before=tuple(observation_before),
        edge_before=edge_before,
        edge_after_refcount=edge_after_refcount,
        mask_before=mask_before,
        mask_after=mask_after,
        hall_before=hall_before,
        hall_after=hall_after,
        hall_work=hall_work,
    )


def _effective_requirement_state(
    cursor: Cursor[Any],
    source: _RequirementCompletionSource,
    *,
    lock: bool,
) -> _RequirementStateWrite:
    suffix = " FOR UPDATE" if lock else ""
    published = cursor.execute(
        """
        SELECT witness_hashes, supporting_observation_ids, witness_count,
               satisfied, decision_policy_version
        FROM groundloop_m5_published_requirement_state
        WHERE requirement_version_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """
        + suffix,
        (
            source.requirement_version_id,
            source.previous_published_epoch_id,
            source.previous_published_epoch_id,
        ),
    ).fetchall()
    if lock:
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_working_requirement_state",
            (source.epoch_id, source.requirement_version_id),
        )
    working = cursor.execute(
        """
        SELECT witness_hashes, supporting_observation_ids, witness_count,
               satisfied, decision_policy_version, updated_revision
        FROM groundloop_m5_working_requirement_state
        WHERE epoch_id = %s AND requirement_version_id = %s
        """
        + suffix,
        (source.epoch_id, source.requirement_version_id),
    ).fetchone()
    if len(published) > 1 or (working is None and len(published) != 1):
        raise EventConflictError("requirement published state changed")
    row = published[0] if working is None else working
    if _text(row[4]) != source.decision_policy_version or (
        working is not None and int(working[5]) > source.expected_runtime_revision
    ):
        raise EventConflictError("requirement state policy/revision changed")
    state = RequirementState(
        source.requirement_version_id,
        tuple(_sha256_text(value) for value in row[0]),
        tuple(_text(value) for value in row[1]),
        int(row[2]),
        bool(row[3]),
    )
    if state.witness_count != len(state.witness_hashes) or state.satisfied != bool(
        state.witness_hashes
    ):
        raise EventConflictError("requirement state bytes changed")
    return _RequirementStateWrite(state, _text(row[4]))


def _effective_group_state(
    cursor: Cursor[Any],
    source: _RequirementCompletionSource,
    *,
    lock: bool,
) -> _GroupStateWrite:
    suffix = " FOR UPDATE" if lock else ""
    published = cursor.execute(
        """
        SELECT requirement_count, satisfied_count, matching_size, complete,
               decision_policy_version, certificate_digest
        FROM groundloop_m5_published_group_state
        WHERE group_version_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """
        + suffix,
        (
            source.group_version_id,
            source.previous_published_epoch_id,
            source.previous_published_epoch_id,
        ),
    ).fetchall()
    if lock:
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_working_group_state",
            (source.epoch_id, source.group_version_id),
        )
    working = cursor.execute(
        """
        SELECT requirement_count, satisfied_count, matching_size, complete,
               decision_policy_version, certificate_digest, updated_revision
        FROM groundloop_m5_working_group_state
        WHERE epoch_id = %s AND group_version_id = %s
        """
        + suffix,
        (source.epoch_id, source.group_version_id),
    ).fetchone()
    if len(published) > 1 or (working is None and len(published) != 1):
        raise EventConflictError("requirement published group state changed")
    row = published[0] if working is None else working
    if _text(row[4]) != source.decision_policy_version or (
        working is not None and int(working[6]) > source.expected_runtime_revision
    ):
        raise EventConflictError("requirement group state policy/revision changed")
    state = GroupState(
        source.group_version_id,
        int(row[0]),
        int(row[1]),
        int(row[2]),
        bool(row[3]),
    )
    digest = None if row[5] is None else _sha256_text(row[5])
    if state.complete != (digest is not None):
        raise EventConflictError("requirement group certificate pointer changed")
    return _GroupStateWrite(state, _text(row[4]), digest)


def _effective_claim_state(
    cursor: Cursor[Any],
    source: _RequirementCompletionSource,
    *,
    lock: bool,
) -> _ClaimStateWrite:
    suffix = " FOR UPDATE" if lock else ""
    published = cursor.execute(
        """
        SELECT support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, complete_group_count,
               complete_group_ids, status, decision_policy_version,
               certificate_digest
        FROM groundloop_m5_published_claim_state
        WHERE claim_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """
        + suffix,
        (
            source.owner_claim_id,
            source.previous_published_epoch_id,
            source.previous_published_epoch_id,
        ),
    ).fetchall()
    if lock:
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_working_claim_state",
            (source.epoch_id, source.owner_claim_id),
        )
    working = cursor.execute(
        """
        SELECT support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, complete_group_count,
               complete_group_ids, status, decision_policy_version,
               certificate_digest, updated_revision
        FROM groundloop_m5_working_claim_state
        WHERE epoch_id = %s AND claim_id = %s
        """
        + suffix,
        (source.epoch_id, source.owner_claim_id),
    ).fetchone()
    if len(published) > 1 or (working is None and len(published) != 1):
        raise EventConflictError("requirement published claim state changed")
    row = published[0] if working is None else working
    if _text(row[9]) != source.decision_policy_version or (
        working is not None and int(working[11]) > source.expected_runtime_revision
    ):
        raise EventConflictError("requirement claim state policy/revision changed")
    groups = tuple(_text(value) for value in row[7])
    state = CombinedClaimState(
        source.owner_claim_id,
        int(row[0]),
        int(row[1]),
        None if row[2] is None else float(row[2]),
        None if row[3] is None else float(row[3]),
        tuple(_text(value) for value in row[4]),
        tuple(_text(value) for value in row[5]),
        int(row[6]),
        groups,
        ClaimStatus(_text(row[8])),
    )
    if state.complete_group_count != len(groups) or state.status is not _claim_status(
        supported=bool(state.support_count or groups),
        refuted=bool(state.refute_count),
    ):
        raise EventConflictError("requirement claim state bytes changed")
    return _ClaimStateWrite(
        state,
        _text(row[9]),
        _sha256_text(row[10]),
    )


def _effective_answer_state(
    cursor: Cursor[Any],
    source: _RequirementCompletionSource,
    *,
    lock: bool,
) -> CombinedAnswerState:
    suffix = " FOR UPDATE" if lock else ""
    published = cursor.execute(
        """
        SELECT required_claim_count, supported_count, unsupported_count,
               refuted_count, conflicted_count, status
        FROM groundloop_m5_published_answer_state
        WHERE answer_version_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """
        + suffix,
        (
            source.answer_version_id,
            source.previous_published_epoch_id,
            source.previous_published_epoch_id,
        ),
    ).fetchall()
    if lock:
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_working_answer_state",
            (source.epoch_id, source.answer_version_id),
        )
    working = cursor.execute(
        """
        SELECT required_claim_count, supported_count, unsupported_count,
               refuted_count, conflicted_count, status, updated_revision
        FROM groundloop_m5_working_answer_state
        WHERE epoch_id = %s AND answer_version_id = %s
        """
        + suffix,
        (source.epoch_id, source.answer_version_id),
    ).fetchone()
    if len(published) > 1 or (working is None and len(published) != 1):
        raise EventConflictError("requirement published answer state changed")
    row = published[0] if working is None else working
    if working is not None and int(working[6]) > source.expected_runtime_revision:
        raise EventConflictError("requirement answer state revision changed")
    state = CombinedAnswerState(
        source.answer_version_id,
        int(row[0]),
        int(row[1]),
        int(row[2]),
        int(row[3]),
        int(row[4]),
        AnswerStatus(_text(row[5])),
    )
    counts = Counter(
        {
            ClaimStatus.SUPPORTED: state.supported_count,
            ClaimStatus.UNSUPPORTED: state.unsupported_count,
            ClaimStatus.REFUTED: state.refuted_count,
            ClaimStatus.CONFLICTED: state.conflicted_count,
        }
    )
    if state.status is not _answer_status(counts, state.required_claim_count):
        raise EventConflictError("requirement answer state bytes changed")
    return state


def _effective_group_binding(
    cursor: Cursor[Any],
    source: _RequirementCompletionSource,
    group_id: str,
    *,
    lock: bool,
) -> _EffectiveGroupBinding | None:
    suffix = " FOR UPDATE" if lock else ""
    published = cursor.execute(
        """
        SELECT certificate_digest, sealed_revision
        FROM groundloop_m5_published_group_certificate_binding
        WHERE group_version_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """
        + suffix,
        (
            group_id,
            source.previous_published_epoch_id,
            source.previous_published_epoch_id,
        ),
    ).fetchall()
    if len(published) > 1:
        raise EventConflictError("requirement published group binding changed")
    working = cursor.execute(
        """
        SELECT certificate_digest, valid_from_revision
        FROM groundloop_m5_working_group_certificate_binding
        WHERE epoch_id = %s AND group_version_id = %s
          AND valid_to_revision IS NULL
        """
        + suffix,
        (source.epoch_id, group_id),
    ).fetchall()
    if len(working) > 1:
        raise EventConflictError("requirement working group binding changed")
    if working:
        return _EffectiveGroupBinding(
            _sha256_text(working[0][0]), int(working[0][1]), True
        )
    if published:
        return _EffectiveGroupBinding(
            _sha256_text(published[0][0]), int(published[0][1]), False
        )
    return None


def _effective_claim_binding(
    cursor: Cursor[Any],
    source: _RequirementCompletionSource,
    *,
    lock: bool,
) -> _EffectiveClaimBinding | None:
    suffix = " FOR UPDATE" if lock else ""
    published = cursor.execute(
        """
        SELECT certificate_digest, sealed_revision
        FROM groundloop_m5_published_claim_certificate_binding
        WHERE claim_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """
        + suffix,
        (
            source.owner_claim_id,
            source.previous_published_epoch_id,
            source.previous_published_epoch_id,
        ),
    ).fetchall()
    if len(published) > 1:
        raise EventConflictError("requirement published claim binding changed")
    working = cursor.execute(
        """
        SELECT certificate_digest, valid_from_revision
        FROM groundloop_m5_working_claim_certificate_binding
        WHERE epoch_id = %s AND claim_id = %s
          AND valid_to_revision IS NULL
        """
        + suffix,
        (source.epoch_id, source.owner_claim_id),
    ).fetchall()
    if len(working) > 1:
        raise EventConflictError("requirement working claim binding changed")
    if working:
        return _EffectiveClaimBinding(
            _sha256_text(working[0][0]), int(working[0][1]), True
        )
    if published:
        return _EffectiveClaimBinding(
            _sha256_text(published[0][0]), int(published[0][1]), False
        )
    return None


def _stored_group_certificate_artifact(
    cursor: Cursor[Any], certificate_digest: str, *, lock: bool
) -> GroupMatchingCertificateArtifact | None:
    suffix = " FOR UPDATE" if lock else ""
    header = cursor.execute(
        """
        SELECT decision_policy_version, certificate_version,
               group_version_id, requirement_count
        FROM groundloop_m5_group_certificate_artifact
        WHERE certificate_digest = %s
        """
        + suffix,
        (certificate_digest,),
    ).fetchone()
    rows = cursor.execute(
        """
        SELECT requirement_ordinal, requirement_version_id, text_hash,
               selected_observation_id
        FROM groundloop_m5_group_certificate_artifact_row
        WHERE certificate_digest = %s
        ORDER BY requirement_ordinal
        """
        + suffix,
        (certificate_digest,),
    ).fetchall()
    if header is None:
        if rows:
            raise EventConflictError("matching group certificate digest collision")
        return None
    if _text(header[1]) != "m5-group-certificate-v1" or int(header[3]) != len(rows):
        raise EventConflictError("matching group certificate digest collision")
    try:
        return GroupMatchingCertificateArtifact(
            decision_policy_version=_text(header[0]),
            group_version_id=_text(header[2]),
            rows=tuple(
                GroupCertificateRow(
                    int(row[0]),
                    _text(row[1]),
                    _sha256_text(row[2]),
                    _text(row[3]),
                )
                for row in rows
            ),
            certificate_digest=certificate_digest,
        )
    except ValidationError as error:
        raise EventConflictError(
            "matching group certificate digest collision"
        ) from error


def _lock_or_reserve_group_certificate_artifact(
    cursor: Cursor[Any], artifact: GroupMatchingCertificateArtifact
) -> bool:
    _reserve_matching_absence(
        cursor,
        "groundloop_m5_group_certificate_artifact",
        (artifact.certificate_digest,),
    )
    stored = _stored_group_certificate_artifact(
        cursor, artifact.certificate_digest, lock=True
    )
    if stored is not None:
        if stored != artifact:
            raise EventConflictError("matching group certificate digest collision")
        return True
    for row in artifact.rows:
        key = (artifact.certificate_digest, row.requirement_ordinal)
        _reserve_matching_absence(
            cursor, "groundloop_m5_group_certificate_artifact_row", key
        )
        existing = cursor.execute(
            """
            SELECT requirement_version_id, text_hash, selected_observation_id
            FROM groundloop_m5_group_certificate_artifact_row
            WHERE certificate_digest = %s AND requirement_ordinal = %s
            FOR UPDATE
            """,
            key,
        ).fetchone()
        if existing is not None:
            raise EventConflictError("matching group certificate digest collision")
    return False


def _stored_claim_certificate_artifact(
    cursor: Cursor[Any], certificate_digest: str, *, lock: bool
) -> ClaimCertificateArtifact | None:
    suffix = " FOR UPDATE" if lock else ""
    row = cursor.execute(
        """
        SELECT certificate_version, claim_id, decision_policy_version,
               support_kind, direct_support_observation_id,
               group_version_id, group_certificate_digest,
               direct_refute_observation_id
        FROM groundloop_m5_claim_certificate_artifact
        WHERE certificate_digest = %s
        """
        + suffix,
        (certificate_digest,),
    ).fetchone()
    if row is None:
        return None
    try:
        return ClaimCertificateArtifact(
            claim_id=_text(row[1]),
            decision_policy_version=_text(row[2]),
            support_kind=ClaimSupportKind(_text(row[3])),
            direct_support_observation_id=(None if row[4] is None else _text(row[4])),
            group_version_id=None if row[5] is None else _text(row[5]),
            group_certificate_digest=(None if row[6] is None else _sha256_text(row[6])),
            direct_refute_observation_id=(None if row[7] is None else _text(row[7])),
            certificate_version=_text(row[0]),
            certificate_digest=certificate_digest,
        )
    except (ValidationError, ValueError) as error:
        raise EventConflictError(
            "matching claim certificate digest collision"
        ) from error


def _lock_or_reserve_claim_certificate_artifact(
    cursor: Cursor[Any], artifact: ClaimCertificateArtifact
) -> bool:
    _reserve_matching_absence(
        cursor,
        "groundloop_m5_claim_certificate_artifact",
        (artifact.certificate_digest,),
    )
    stored = _stored_claim_certificate_artifact(
        cursor, artifact.certificate_digest, lock=True
    )
    if stored is not None:
        if stored != artifact:
            raise EventConflictError("matching claim certificate digest collision")
        return True
    return False


def _claim_artifact_for_requirement(
    source: _RequirementCompletionSource,
    claim_state: CombinedClaimState,
    group_artifact: GroupMatchingCertificateArtifact | None,
    group_bindings: dict[str, _EffectiveGroupBinding | None],
) -> ClaimCertificateArtifact:
    if claim_state.supporting_observation_ids:
        support_kind = ClaimSupportKind.DIRECT
        direct_support = claim_state.supporting_observation_ids[0]
        selected_group_id = None
        selected_group_digest = None
    elif claim_state.complete_group_ids:
        support_kind = ClaimSupportKind.GROUP
        direct_support = None
        selected_group_id = claim_state.complete_group_ids[0]
        if selected_group_id == source.group_version_id:
            if group_artifact is None:
                raise EventConflictError(
                    "complete requirement group lacks its certificate"
                )
            selected_group_digest = group_artifact.certificate_digest
        else:
            binding = group_bindings.get(selected_group_id)
            if binding is None:
                raise EventConflictError(
                    "selected complete group has no effective certificate"
                )
            selected_group_digest = binding.certificate_digest
    else:
        support_kind = ClaimSupportKind.NONE
        direct_support = None
        selected_group_id = None
        selected_group_digest = None
    return ClaimCertificateArtifact(
        claim_id=source.owner_claim_id,
        decision_policy_version=source.decision_policy_version,
        support_kind=support_kind,
        direct_support_observation_id=direct_support,
        group_version_id=selected_group_id,
        group_certificate_digest=selected_group_digest,
        direct_refute_observation_id=(
            claim_state.refuting_observation_ids[0]
            if claim_state.refuting_observation_ids
            else None
        ),
    )


def _requirement_logical_transition(
    *,
    source: _RequirementCompletionSource,
    preview: _RequirementIntentPreview,
    requirement_before: _RequirementStateWrite,
    group_before: _GroupStateWrite,
    claim_before: _ClaimStateWrite,
    answer_before: CombinedAnswerState,
    group_bindings: dict[str, _EffectiveGroupBinding | None],
) -> _RequirementLogicalTransition:
    old_support = _requirement_label_supports(
        preview.currency_projection.base_operational_label
    )
    new_support = _requirement_label_supports(source.operational_label)
    old_observation_id = preview.currency_projection.base_observation_id
    hashes = set(requirement_before.state.witness_hashes)
    observations = set(requirement_before.state.supporting_observation_ids)
    old_edge_active = (
        preview.edge_before is not None and preview.edge_before.refcount > 0
    )
    if (source.text_hash in hashes) != old_edge_active:
        raise EventConflictError("requirement state and matching edge changed")
    if old_support:
        if old_observation_id is None or old_observation_id not in observations:
            raise EventConflictError("requirement support provenance changed")
        observations.remove(old_observation_id)
    elif old_observation_id is not None and old_observation_id in observations:
        raise EventConflictError("non-support currency has support provenance")
    if new_support:
        observations.add(source.observation_id)
    if preview.edge_after_refcount > 0:
        hashes.add(source.text_hash)
    else:
        hashes.discard(source.text_hash)
    after_hashes = tuple(sorted(hashes))
    after_observations = tuple(sorted(observations))
    requirement_state = RequirementState(
        source.requirement_version_id,
        after_hashes,
        after_observations,
        len(after_hashes),
        bool(after_hashes),
    )
    requirement_after = _RequirementStateWrite(
        requirement_state, source.decision_policy_version
    )

    before_group = group_before.state
    before_hall = _hall_state_from_point(preview.hall_before)
    if (
        before_group.requirement_count != before_hall.requirement_count
        or before_group.matching_size != before_hall.matching_size
        or before_group.complete != before_hall.complete
        or (source.group_version_id in claim_before.state.complete_group_ids)
        != before_group.complete
        or requirement_before.state.satisfied
        != (bool(requirement_before.state.witness_hashes))
    ):
        raise EventConflictError("requirement logical/matching before image changed")
    satisfied_count = before_group.satisfied_count
    if requirement_before.state.satisfied != requirement_after.state.satisfied:
        satisfied_count += 1 if requirement_after.state.satisfied else -1
    group_artifact = preview.certificate_artifact
    if preview.hall_after.complete != (group_artifact is not None):
        raise EventConflictError("requirement certificate reconstruction changed")
    group_after_state = GroupState(
        source.group_version_id,
        before_group.requirement_count,
        satisfied_count,
        preview.hall_after.matching_size,
        preview.hall_after.complete,
    )
    group_after = _GroupStateWrite(
        group_after_state,
        source.decision_policy_version,
        None if group_artifact is None else group_artifact.certificate_digest,
    )

    complete_groups = set(claim_before.state.complete_group_ids)
    if group_after_state.complete:
        complete_groups.add(source.group_version_id)
    else:
        complete_groups.discard(source.group_version_id)
    complete_group_ids = tuple(sorted(complete_groups))
    claim_status = _claim_status(
        supported=bool(claim_before.state.support_count or complete_group_ids),
        refuted=bool(claim_before.state.refute_count),
    )
    provisional_claim = CombinedClaimState(
        source.owner_claim_id,
        claim_before.state.support_count,
        claim_before.state.refute_count,
        claim_before.state.best_support_score,
        claim_before.state.best_refute_score,
        claim_before.state.supporting_observation_ids,
        claim_before.state.refuting_observation_ids,
        len(complete_group_ids),
        complete_group_ids,
        claim_status,
    )
    claim_artifact = _claim_artifact_for_requirement(
        source, provisional_claim, group_artifact, group_bindings
    )
    claim_after = _ClaimStateWrite(
        provisional_claim,
        source.decision_policy_version,
        claim_artifact.certificate_digest,
    )

    answer_after = answer_before
    status_deltas: list[StatusDelta] = []
    reason = f"attempt={source.attempt_id} op=RequirementCompletion"
    if claim_after.state.status is not claim_before.state.status:
        status_deltas.append(
            StatusDelta(
                source.attempt_id,
                "claim",
                source.owner_claim_id,
                claim_before.state.status.value,
                claim_after.state.status.value,
                reason,
            )
        )
        if source.owner_claim_required:
            answer_counts: Counter[ClaimStatus] = Counter(
                {
                    ClaimStatus.SUPPORTED: answer_before.supported_count,
                    ClaimStatus.UNSUPPORTED: answer_before.unsupported_count,
                    ClaimStatus.REFUTED: answer_before.refuted_count,
                    ClaimStatus.CONFLICTED: answer_before.conflicted_count,
                }
            )
            answer_counts[claim_before.state.status] -= 1
            answer_counts[claim_after.state.status] += 1
            next_answer_status = _answer_status(
                answer_counts, answer_before.required_claim_count
            )
            answer_after = CombinedAnswerState(
                source.answer_version_id,
                answer_before.required_claim_count,
                answer_counts[ClaimStatus.SUPPORTED],
                answer_counts[ClaimStatus.UNSUPPORTED],
                answer_counts[ClaimStatus.REFUTED],
                answer_counts[ClaimStatus.CONFLICTED],
                next_answer_status,
            )
            if next_answer_status is not answer_before.status:
                status_deltas.append(
                    StatusDelta(
                        source.attempt_id,
                        "answer",
                        source.answer_version_id,
                        answer_before.status.value,
                        next_answer_status.value,
                        reason,
                    )
                )
    return _RequirementLogicalTransition(
        requirement_before=requirement_before,
        requirement_after=requirement_after,
        group_before=group_before,
        group_after=group_after,
        claim_before=claim_before,
        claim_after=claim_after,
        answer_before=answer_before,
        answer_after=answer_after,
        claim_artifact=claim_artifact,
        status_deltas=tuple(status_deltas),
    )


def _lock_requirement_intent_rows(
    cursor: Cursor[Any],
    *,
    source: _RequirementCompletionSource,
    intent: M5PersistedMatchingTransitionIntent,
    preview: _RequirementIntentPreview,
) -> None:
    """Acquire the exact discovered tier-11c-through-14 requirement plan."""

    if any(getattr(intent, name) != value for name, value in preview.affected.items()):
        raise EventConflictError("requirement matching official intent changed")
    selected_observations = {
        observation_id
        for _, _, observation_id in preview.certificate_view.selected_observations
    }
    old_observation_id = preview.currency_projection.base_observation_id
    old_support = _requirement_label_supports(
        preview.currency_projection.base_operational_label
    )
    new_support = _requirement_label_supports(source.operational_label)

    for observation_id in intent.observation_ids:
        cursor.execute(
            """
            SELECT observation_id
            FROM groundloop_m5_matching_observation_current
            WHERE observation_id = %s
            FOR UPDATE
            """,
            (observation_id,),
        ).fetchone()
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_matching_observation_working",
            (source.epoch_id, observation_id),
        )
        cursor.execute(
            """
            SELECT observation_id
            FROM groundloop_m5_matching_observation_working
            WHERE epoch_id = %s AND observation_id = %s
            FOR UPDATE
            """,
            (source.epoch_id, observation_id),
        ).fetchone()
        observation_point = _resolved_requirement_observation_point(
            cursor, source.epoch_id, observation_id
        )
        expected_active = observation_id in selected_observations or (
            old_support and observation_id == old_observation_id
        )
        expected_absent = new_support and observation_id == source.observation_id
        if expected_absent:
            if observation_point is not None:
                raise EventConflictError(
                    "requirement result matching observation appeared"
                )
        elif expected_active:
            if observation_point is None or (
                isinstance(observation_point, M5MatchingObservationWorking)
                and not observation_point.present
            ):
                raise EventConflictError(
                    "requirement representative observation changed"
                )
        else:
            raise EventConflictError("requirement observation lock plan changed")

    selected_edges = {
        (ordinal, text_hash)
        for ordinal, text_hash, _ in preview.certificate_view.selected_observations
    }
    for group_id, ordinal, text_hash, requirement_id in intent.edge_keys:
        cursor.execute(
            """
            SELECT requirement_version_id
            FROM groundloop_m5_matching_edge_current
            WHERE requirement_version_id = %s AND text_hash = %s
            FOR UPDATE
            """,
            (requirement_id, text_hash),
        ).fetchone()
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_matching_edge_working",
            (source.epoch_id, requirement_id, text_hash),
        )
        cursor.execute(
            """
            SELECT requirement_version_id
            FROM groundloop_m5_matching_edge_working
            WHERE epoch_id = %s AND requirement_version_id = %s
              AND text_hash = %s
            FOR UPDATE
            """,
            (source.epoch_id, requirement_id, text_hash),
        ).fetchone()
        edge_point = _resolved_requirement_edge_point(
            cursor, source.epoch_id, requirement_id, text_hash
        )
        changed_edge_key = (
            requirement_id == source.requirement_version_id
            and text_hash == source.text_hash
        )
        effective_refcount = (
            preview.edge_after_refcount
            if changed_edge_key
            else (0 if edge_point is None else edge_point.refcount)
        )
        if (ordinal, text_hash) in selected_edges and effective_refcount <= 0:
            raise EventConflictError("requirement representative edge changed")
        if edge_point is not None and (
            edge_point.group_version_id != group_id
            or edge_point.requirement_ordinal != ordinal
        ):
            raise EventConflictError("requirement matching edge coordinates changed")
    changed_edge = _resolved_requirement_edge_point(
        cursor,
        source.epoch_id,
        source.requirement_version_id,
        source.text_hash,
    )
    if changed_edge != preview.edge_before:
        raise EventConflictError("requirement changed edge before image changed")

    candidate_masks = dict(preview.certificate_view.candidates)
    for group_id, text_hash in intent.mask_keys:
        cursor.execute(
            """
            SELECT group_version_id
            FROM groundloop_m5_matching_hash_mask_current
            WHERE group_version_id = %s AND text_hash = %s
            FOR UPDATE
            """,
            (group_id, text_hash),
        ).fetchone()
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_matching_hash_mask_working",
            (source.epoch_id, group_id, text_hash),
        )
        cursor.execute(
            """
            SELECT group_version_id
            FROM groundloop_m5_matching_hash_mask_working
            WHERE epoch_id = %s AND group_version_id = %s AND text_hash = %s
            FOR UPDATE
            """,
            (source.epoch_id, group_id, text_hash),
        ).fetchone()
        mask_point = _resolved_requirement_mask_point(
            cursor, source.epoch_id, group_id, text_hash
        )
        if text_hash in candidate_masks:
            actual = 0 if mask_point is None else mask_point.mask
            if text_hash == source.text_hash:
                actual = preview.mask_after
            if actual != candidate_masks[text_hash]:
                raise EventConflictError("requirement representative mask changed")
    changed_mask = _resolved_requirement_mask_point(
        cursor, source.epoch_id, source.group_version_id, source.text_hash
    )
    if changed_mask != preview.mask_before:
        raise EventConflictError("requirement changed mask before image changed")

    for group_id in intent.hall_group_ids:
        cursor.execute(
            """
            SELECT group_version_id
            FROM groundloop_m5_matching_hall_current
            WHERE group_version_id = %s
            FOR UPDATE
            """,
            (group_id,),
        ).fetchone()
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_matching_hall_working",
            (source.epoch_id, group_id),
        )
        cursor.execute(
            """
            SELECT group_version_id
            FROM groundloop_m5_matching_hall_working
            WHERE epoch_id = %s AND group_version_id = %s
            FOR UPDATE
            """,
            (source.epoch_id, group_id),
        ).fetchone()
    if (
        _resolved_requirement_hall_point(
            cursor, source.epoch_id, source.group_version_id
        )
        != preview.hall_before
    ):
        raise EventConflictError("requirement Hall before image changed")

    requirement_before = _effective_requirement_state(cursor, source, lock=True)
    group_before = _effective_group_state(cursor, source, lock=True)
    group_bindings = {
        group_id: _effective_group_binding(cursor, source, group_id, lock=True)
        for group_id in intent.group_certificate_ids
    }
    source_binding = group_bindings.get(source.group_version_id)
    if (None if source_binding is None else source_binding.certificate_digest) != (
        group_before.certificate_digest
    ):
        raise EventConflictError("requirement group binding/state changed")
    if preview.certificate_artifact is not None:
        _lock_or_reserve_group_certificate_artifact(
            cursor, preview.certificate_artifact
        )
    for group_id, binding in group_bindings.items():
        if binding is None or group_id == source.group_version_id:
            continue
        artifact = _stored_group_certificate_artifact(
            cursor, binding.certificate_digest, lock=True
        )
        if artifact is None or (
            artifact.group_version_id != group_id
            or artifact.decision_policy_version != source.decision_policy_version
        ):
            raise EventConflictError("requirement selected group artifact changed")

    claim_before = _effective_claim_state(cursor, source, lock=True)
    claim_binding = _effective_claim_binding(cursor, source, lock=True)
    if claim_binding is None or (
        claim_binding.certificate_digest != claim_before.certificate_digest
    ):
        raise EventConflictError("requirement claim binding/state changed")
    prior_claim_artifact = _stored_claim_certificate_artifact(
        cursor, claim_before.certificate_digest, lock=True
    )
    if prior_claim_artifact is None or (
        prior_claim_artifact.claim_id != source.owner_claim_id
        or prior_claim_artifact.decision_policy_version
        != source.decision_policy_version
    ):
        raise EventConflictError("requirement prior claim artifact changed")

    preliminary_answer = _effective_answer_state(cursor, source, lock=False)
    preliminary_transition = _requirement_logical_transition(
        source=source,
        preview=preview,
        requirement_before=requirement_before,
        group_before=group_before,
        claim_before=claim_before,
        answer_before=preliminary_answer,
        group_bindings=group_bindings,
    )
    _lock_or_reserve_claim_certificate_artifact(
        cursor, preliminary_transition.claim_artifact
    )

    if (
        preliminary_transition.group_after.certificate_digest
        != group_before.certificate_digest
    ):
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_working_group_certificate_binding",
            (
                source.epoch_id,
                source.group_version_id,
                source.resulting_revision,
            ),
        )
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_working_group_certificate_binding#open",
            (source.epoch_id, source.group_version_id),
        )
        existing = cursor.execute(
            """
            SELECT 1
            FROM groundloop_m5_working_group_certificate_binding
            WHERE epoch_id = %s AND group_version_id = %s
              AND valid_from_revision = %s
            FOR UPDATE
            """,
            (
                source.epoch_id,
                source.group_version_id,
                source.resulting_revision,
            ),
        ).fetchone()
        if existing is not None:
            raise EventConflictError("requirement group binding target appeared")
    claim_digest_changed = (
        preliminary_transition.claim_after.certificate_digest
        != claim_before.certificate_digest
    )
    claim_state_changed = preliminary_transition.claim_after != claim_before
    if claim_digest_changed or (claim_state_changed and not claim_binding.working):
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_working_claim_certificate_binding",
            (
                source.epoch_id,
                source.owner_claim_id,
                source.resulting_revision,
            ),
        )
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_working_claim_certificate_binding#open",
            (source.epoch_id, source.owner_claim_id),
        )
        existing = cursor.execute(
            """
            SELECT 1
            FROM groundloop_m5_working_claim_certificate_binding
            WHERE epoch_id = %s AND claim_id = %s
              AND valid_from_revision = %s
            FOR UPDATE
            """,
            (
                source.epoch_id,
                source.owner_claim_id,
                source.resulting_revision,
            ),
        ).fetchone()
        if existing is not None:
            raise EventConflictError("requirement claim binding target appeared")

    answer_before = _effective_answer_state(cursor, source, lock=True)
    transition = _requirement_logical_transition(
        source=source,
        preview=preview,
        requirement_before=requirement_before,
        group_before=group_before,
        claim_before=claim_before,
        answer_before=answer_before,
        group_bindings=group_bindings,
    )
    if transition.claim_artifact != preliminary_transition.claim_artifact:
        raise EventConflictError("requirement claim artifact changed before lock")


def _requirement_preview_from_locked_intent(
    cursor: Cursor[Any],
    *,
    source: _RequirementCompletionSource,
    projection: _RequirementCurrencyProjection,
    intent: M5PersistedMatchingTransitionIntent,
) -> _RequirementIntentPreview:
    """Recompute the preview only from the official already-locked key set."""

    if len(intent.group_shapes) != 1:
        raise EventConflictError("requirement intent group shape changed")
    shape = intent.group_shapes[0]
    old_support = _requirement_label_supports(projection.base_operational_label)
    new_support = _requirement_label_supports(source.operational_label)
    old_observation_id = projection.base_observation_id
    observation_before: list[M5MatchingObservationPoint] = []
    if old_support:
        if old_observation_id is None:
            raise EventConflictError("supporting base currency has no observation")
        old_point = _resolved_requirement_observation_point(
            cursor, source.epoch_id, old_observation_id
        )
        if old_point is None or (
            isinstance(old_point, M5MatchingObservationWorking)
            and not old_point.present
        ):
            raise EventConflictError("requirement base observation changed")
        observation_before.append(old_point)
    if (
        _resolved_requirement_observation_point(
            cursor, source.epoch_id, source.observation_id
        )
        is not None
    ):
        raise EventConflictError("requirement result observation already matched")
    edge_before = _resolved_requirement_edge_point(
        cursor,
        source.epoch_id,
        source.requirement_version_id,
        source.text_hash,
    )
    old_refcount = 0 if edge_before is None else edge_before.refcount
    edge_after_refcount = old_refcount - int(old_support) + int(new_support)
    if edge_after_refcount < 0:
        raise EventConflictError("requirement matching edge underflowed")
    mask_before = _resolved_requirement_mask_point(
        cursor, source.epoch_id, source.group_version_id, source.text_hash
    )
    old_mask = 0 if mask_before is None else mask_before.mask
    bit = 1 << source.requirement_ordinal
    mask_after = old_mask | bit if edge_after_refcount else old_mask & ~bit
    hall_before = _resolved_requirement_hall_point(
        cursor, source.epoch_id, source.group_version_id
    )
    if hall_before is None:
        raise EventConflictError("requirement Hall state disappeared")
    old_hall = _hall_state_from_point(hall_before)
    if old_mask == mask_after:
        hall_after = old_hall
        hall_work = MatchingWorkCounters()
    else:
        result = apply_hash_mask_transitions(
            old_hall,
            (HashMaskTransition(source.text_hash, old_mask, mask_after),),
        )
        hall_after = result.state
        hall_work = result.work

    by_mask: dict[int, list[str]] = {}
    for group_id, text_hash in intent.mask_keys:
        if group_id != source.group_version_id:
            raise EventConflictError("requirement intent mask group changed")
        mask_point = _resolved_requirement_mask_point(
            cursor, source.epoch_id, group_id, text_hash
        )
        effective_mask = 0 if mask_point is None else mask_point.mask
        if text_hash == source.text_hash:
            effective_mask = mask_after
        if effective_mask:
            by_mask.setdefault(effective_mask, []).append(text_hash)
    candidates = tuple(
        sorted(
            (
                (text_hash, mask)
                for mask, hashes in by_mask.items()
                for text_hash in sorted(set(hashes))[: shape.requirement_count]
            ),
            key=lambda item: item[0],
        )
    )
    actual_by_mask = Counter(mask for _, mask in candidates)
    expected_by_mask = Counter(
        {
            mask: min(hall_after.mask_histogram[mask], shape.requirement_count)
            for mask in range(1, 1 << shape.requirement_count)
            if hall_after.mask_histogram[mask]
        }
    )
    if actual_by_mask != expected_by_mask:
        raise EventConflictError("requirement locked representative set changed")
    matching = affected_group_matching(shape.requirement_count, candidates)

    after_observations: list[tuple[int, str, str]] = []
    for observation_id in intent.observation_ids:
        if old_support and observation_id == old_observation_id:
            continue
        if observation_id == source.observation_id:
            if new_support:
                after_observations.append(
                    (
                        source.requirement_ordinal,
                        source.text_hash,
                        observation_id,
                    )
                )
            continue
        observation_point = _resolved_requirement_observation_point(
            cursor, source.epoch_id, observation_id
        )
        if observation_point is not None and not (
            isinstance(observation_point, M5MatchingObservationWorking)
            and not observation_point.present
        ):
            after_observations.append(
                (
                    observation_point.requirement_ordinal,
                    observation_point.text_hash,
                    observation_id,
                )
            )
    selected: list[tuple[int, str, str]] = []
    if matching.complete:
        for pair in matching.pairs:
            values = sorted(
                observation_id
                for ordinal, text_hash, observation_id in after_observations
                if ordinal == pair.requirement_ordinal and text_hash == pair.text_hash
            )
            if not values:
                raise EventConflictError(
                    "requirement locked representative observation changed"
                )
            selected.append((pair.requirement_ordinal, pair.text_hash, values[0]))
    view = _BoundedCertificateView(
        epoch_id=source.epoch_id,
        revision=source.resulting_revision,
        decision_policy_version=source.decision_policy_version,
        group_version_id=source.group_version_id,
        requirement_version_ids=tuple(
            requirement_id for _, requirement_id in shape.requirements
        ),
        candidates=candidates,
        selected_observations=tuple(selected),
        histogram=hall_after.mask_histogram,
    )
    reconstruction = reconstruct_certificate(view)
    claim_before = _effective_claim_state(cursor, source, lock=False)
    complete_groups = set(claim_before.state.complete_group_ids)
    if hall_after.complete:
        complete_groups.add(source.group_version_id)
    else:
        complete_groups.discard(source.group_version_id)
    selected_after_group = None if not complete_groups else min(complete_groups)
    group_certificate_ids = {source.group_version_id}
    if selected_after_group is not None:
        group_certificate_ids.add(selected_after_group)
    observation_ids = {observation_id for _, _, observation_id in selected}
    if old_support:
        assert old_observation_id is not None
        observation_ids.add(old_observation_id)
    if new_support:
        observation_ids.add(source.observation_id)
    edge_keys = {
        (
            source.group_version_id,
            source.requirement_ordinal,
            source.text_hash,
            source.requirement_version_id,
        ),
        *(
            (
                source.group_version_id,
                ordinal,
                text_hash,
                shape.requirements[ordinal][1],
            )
            for ordinal, text_hash, _ in selected
        ),
    }
    affected: dict[str, object] = {
        "group_shapes": (shape,),
        "observation_ids": tuple(sorted(observation_ids)),
        "edge_keys": tuple(sorted(edge_keys)),
        "mask_keys": tuple(
            sorted(
                {
                    (source.group_version_id, source.text_hash),
                    *((source.group_version_id, value) for value, _ in candidates),
                }
            )
        ),
        "hall_group_ids": (source.group_version_id,),
        "requirement_state_ids": (source.requirement_version_id,),
        "group_state_ids": (source.group_version_id,),
        "claim_state_ids": (source.owner_claim_id,),
        "answer_state_ids": (source.answer_version_id,),
        "group_certificate_ids": tuple(sorted(group_certificate_ids)),
        "claim_certificate_ids": (source.owner_claim_id,),
    }
    if any(getattr(intent, name) != value for name, value in affected.items()):
        raise EventConflictError("requirement official intent changed under held locks")
    return _RequirementIntentPreview(
        affected=affected,
        currency_projection=projection,
        certificate_view=view,
        certificate_artifact=reconstruction.artifact,
        observation_before=tuple(observation_before),
        edge_before=edge_before,
        edge_after_refcount=edge_after_refcount,
        mask_before=mask_before,
        mask_after=mask_after,
        hall_before=hall_before,
        hall_after=hall_after,
        hall_work=hall_work,
    )


def effective_matching_image(
    cursor: Cursor[Any], epoch_id: int
) -> M5MatchingImagePoint:
    """Return and lock the exact current-plus-requested-working image headers."""

    _require_positive_int("epoch_id", epoch_id)
    require_persisted_matching_bundle(cursor)
    _, scoped_runtime_revision = _require_checked_prefix(cursor, epoch_id=epoch_id)
    current = cursor.execute(
        """
        SELECT decision_policy_version, installed_epoch_id, installed_revision
        FROM groundloop_m5_matching_image_current
        WHERE singleton
        FOR UPDATE
        """
    ).fetchone()
    if current is None:
        raise ValidationError("persisted matching current image is missing")
    current_policy = _text(current[0])
    current_epoch = int(current[1])
    current_revision = int(current[2])

    current_authority = cursor.execute(
        """
        SELECT m4_head.epoch_id, m5_head.epoch_id, m5_head.sealed_revision,
               predecessor.revision,
               predecessor.structural_status,
               predecessor.semantic_status,
               predecessor.evaluation_state,
               predecessor.sealed_at IS NOT NULL,
               (SELECT count(*)
                  FROM groundloop_decision_policy AS strict_policy
                 WHERE strict_policy.valid_from_epoch <= %s
                   AND (strict_policy.valid_to_epoch IS NULL
                        OR %s < strict_policy.valid_to_epoch)),
               EXISTS (
                 SELECT 1
                   FROM groundloop_decision_policy AS selected_policy
                  WHERE selected_policy.policy_version = %s
                    AND selected_policy.valid_from_epoch <= %s
                    AND (selected_policy.valid_to_epoch IS NULL
                         OR %s < selected_policy.valid_to_epoch)
               )
        FROM groundloop_m4_publication_head AS m4_head
        JOIN groundloop_m5_publication_head AS m5_head ON m5_head.singleton
        JOIN groundloop_epoch AS predecessor ON predecessor.epoch_id = %s
        WHERE m4_head.singleton
        """,
        (
            current_epoch,
            current_epoch,
            current_policy,
            current_epoch,
            current_epoch,
            current_epoch,
        ),
    ).fetchone()
    if (
        current_authority is None
        or int(current_authority[0]) != current_epoch
        or int(current_authority[1]) != current_epoch
        or int(current_authority[2]) != current_revision
        or int(current_authority[3]) != current_revision
        or _text(current_authority[4]) != "committed"
        or _text(current_authority[5]) != "sealed"
        or _text(current_authority[6]) != "complete"
        or not bool(current_authority[7])
        or int(current_authority[8]) != 1
        or not bool(current_authority[9])
    ):
        raise ValidationError("persisted matching current image is inconsistent")

    working = cursor.execute(
        """
        SELECT epoch_id, base_epoch_id, base_revision,
               decision_policy_version, updated_revision
        FROM groundloop_m5_matching_image_working
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if working is None:
        raise ValidationError("persisted matching working image is missing")
    working_epoch = int(working[0])
    working_base_epoch = int(working[1])
    working_base_revision = int(working[2])
    working_policy = _text(working[3])
    working_updated_revision = int(working[4])

    policy_authority = cursor.execute(
        """
        SELECT runtime.expected_previous_published_epoch_id,
               runtime.candidate_policy_id,
               runtime.candidate_policy_manifest_hash,
               typed_update.previous_published_epoch_id,
               typed_update.decision_policy_version,
               typed_policy.candidate_policy_manifest_hash,
               typed_policy.decision_policy_version,
               direct_update.epoch_id,
               direct_update.previous_published_epoch_id,
               direct_update.candidate_policy_id,
               direct_policy.decision_policy_version
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        JOIN groundloop_m5_candidate_policy AS typed_policy
          ON typed_policy.candidate_policy_id = runtime.candidate_policy_id
        LEFT JOIN groundloop_m4_update AS direct_update
          ON direct_update.epoch_id = runtime.epoch_id
        LEFT JOIN groundloop_candidate_policy AS direct_policy
          ON direct_policy.candidate_policy_id = direct_update.candidate_policy_id
        WHERE runtime.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    direct_present = policy_authority is not None and policy_authority[7] is not None
    if (
        policy_authority is None
        or working_epoch != epoch_id
        or working_base_epoch != current_epoch
        or working_base_revision != current_revision
        or int(policy_authority[0]) != current_epoch
        or int(policy_authority[3]) != current_epoch
        or working_updated_revision > scoped_runtime_revision
        or _sha256_text(policy_authority[2]) != _sha256_text(policy_authority[5])
        or _text(policy_authority[4]) != working_policy
        or _text(policy_authority[6]) != working_policy
        or (
            direct_present
            and (
                int(policy_authority[8]) != current_epoch
                or _text(policy_authority[9]) != _text(policy_authority[1])
                or _text(policy_authority[10]) != working_policy
            )
        )
    ):
        raise ValidationError("persisted matching image policy is inconsistent")
    return M5MatchingImagePoint(
        current_policy,
        current_epoch,
        current_revision,
        working_epoch,
        working_base_epoch,
        working_base_revision,
        working_policy,
        working_updated_revision,
    )


def resolved_matching_observation_point(
    cursor: Cursor[Any], epoch_id: int, observation_id: str
) -> M5MatchingObservationPoint | None:
    """Resolve working-before-current without filtering a working tombstone."""

    _require_positive_int("epoch_id", epoch_id)
    _require_text("observation_id", observation_id)
    effective_matching_image(cursor, epoch_id)
    current = cursor.execute(
        """
        SELECT observation_id, requirement_version_id, group_version_id,
               requirement_ordinal, text_hash, installed_epoch_id,
               installed_revision
        FROM groundloop_m5_matching_observation_current
        WHERE observation_id = %s
        FOR UPDATE
        """,
        (observation_id,),
    ).fetchone()
    working = cursor.execute(
        """
        SELECT epoch_id, observation_id, requirement_version_id,
               group_version_id, requirement_ordinal, text_hash,
               present, updated_revision
        FROM groundloop_m5_matching_observation_working
        WHERE epoch_id = %s AND observation_id = %s
        FOR UPDATE
        """,
        (epoch_id, observation_id),
    ).fetchone()
    if working is not None:
        return M5MatchingObservationWorking(
            M5MatchingLayer.WORKING,
            int(working[0]),
            _text(working[1]),
            _text(working[2]),
            _text(working[3]),
            int(working[4]),
            _sha256_text(working[5]),
            bool(working[6]),
            int(working[7]),
        )
    if current is None:
        return None
    return M5MatchingObservationCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        _text(current[1]),
        _text(current[2]),
        int(current[3]),
        _sha256_text(current[4]),
        int(current[5]),
        int(current[6]),
    )


def resolved_matching_edge_point(
    cursor: Cursor[Any],
    epoch_id: int,
    requirement_version_id: str,
    text_hash: str,
) -> M5MatchingEdgePoint | None:
    """Resolve one persisted edge point without collapsing a zero tombstone."""

    _require_positive_int("epoch_id", epoch_id)
    _require_text("requirement_version_id", requirement_version_id)
    _require_sha256("text_hash", text_hash)
    effective_matching_image(cursor, epoch_id)
    current = cursor.execute(
        """
        SELECT requirement_version_id, text_hash, group_version_id,
               requirement_ordinal, refcount, installed_epoch_id,
               installed_revision
        FROM groundloop_m5_matching_edge_current
        WHERE requirement_version_id = %s AND text_hash = %s
        FOR UPDATE
        """,
        (requirement_version_id, text_hash),
    ).fetchone()
    working = cursor.execute(
        """
        SELECT epoch_id, requirement_version_id, text_hash,
               group_version_id, requirement_ordinal, refcount,
               updated_revision
        FROM groundloop_m5_matching_edge_working
        WHERE epoch_id = %s AND requirement_version_id = %s AND text_hash = %s
        FOR UPDATE
        """,
        (epoch_id, requirement_version_id, text_hash),
    ).fetchone()
    if working is not None:
        return M5MatchingEdgeWorking(
            M5MatchingLayer.WORKING,
            int(working[0]),
            _text(working[1]),
            _sha256_text(working[2]),
            _text(working[3]),
            int(working[4]),
            int(working[5]),
            int(working[6]),
        )
    if current is None:
        return None
    return M5MatchingEdgeCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        _sha256_text(current[1]),
        _text(current[2]),
        int(current[3]),
        int(current[4]),
        int(current[5]),
        int(current[6]),
    )


def resolved_matching_mask_point(
    cursor: Cursor[Any], epoch_id: int, group_version_id: str, text_hash: str
) -> M5MatchingMaskPoint | None:
    """Resolve one hash-mask point without collapsing a zero tombstone."""

    _require_positive_int("epoch_id", epoch_id)
    _require_text("group_version_id", group_version_id)
    _require_sha256("text_hash", text_hash)
    effective_matching_image(cursor, epoch_id)
    current = cursor.execute(
        """
        SELECT group_version_id, text_hash, mask,
               installed_epoch_id, installed_revision
        FROM groundloop_m5_matching_hash_mask_current
        WHERE group_version_id = %s AND text_hash = %s
        FOR UPDATE
        """,
        (group_version_id, text_hash),
    ).fetchone()
    working = cursor.execute(
        """
        SELECT epoch_id, group_version_id, text_hash, mask, updated_revision
        FROM groundloop_m5_matching_hash_mask_working
        WHERE epoch_id = %s AND group_version_id = %s AND text_hash = %s
        FOR UPDATE
        """,
        (epoch_id, group_version_id, text_hash),
    ).fetchone()
    if working is not None:
        return M5MatchingMaskWorking(
            M5MatchingLayer.WORKING,
            int(working[0]),
            _text(working[1]),
            _sha256_text(working[2]),
            int(working[3]),
            int(working[4]),
        )
    if current is None:
        return None
    return M5MatchingMaskCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        _sha256_text(current[1]),
        int(current[2]),
        int(current[3]),
        int(current[4]),
    )


def resolved_matching_hall_point(
    cursor: Cursor[Any], epoch_id: int, group_version_id: str
) -> M5MatchingHallPoint | None:
    """Resolve one Hall point while retaining a working absent payload."""

    _require_positive_int("epoch_id", epoch_id)
    _require_text("group_version_id", group_version_id)
    effective_matching_image(cursor, epoch_id)
    current = cursor.execute(
        """
        SELECT group_version_id, requirement_count, mask_histogram,
               neighbor_counts, deficiencies, maximum_deficiency,
               matching_size, distinct_hash_count, installed_epoch_id,
               installed_revision
        FROM groundloop_m5_matching_hall_current
        WHERE group_version_id = %s
        FOR UPDATE
        """,
        (group_version_id,),
    ).fetchone()
    working = cursor.execute(
        """
        SELECT epoch_id, group_version_id, present, requirement_count,
               mask_histogram, neighbor_counts, deficiencies,
               maximum_deficiency, matching_size, distinct_hash_count,
               updated_revision
        FROM groundloop_m5_matching_hall_working
        WHERE epoch_id = %s AND group_version_id = %s
        FOR UPDATE
        """,
        (epoch_id, group_version_id),
    ).fetchone()
    if working is not None:
        present = bool(working[2])
        return M5MatchingHallWorking(
            M5MatchingLayer.WORKING,
            int(working[0]),
            _text(working[1]),
            present,
            None if working[3] is None else int(working[3]),
            None if working[4] is None else _tuple_ints(working[4]),
            None if working[5] is None else _tuple_ints(working[5]),
            None if working[6] is None else _tuple_ints(working[6]),
            None if working[7] is None else int(working[7]),
            None if working[8] is None else int(working[8]),
            None if working[9] is None else int(working[9]),
            int(working[10]),
        )
    if current is None:
        return None
    return M5MatchingHallCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        int(current[1]),
        _tuple_ints(current[2]),
        _tuple_ints(current[3]),
        _tuple_ints(current[4]),
        int(current[5]),
        int(current[6]),
        int(current[7]),
        int(current[8]),
        int(current[9]),
    )


def effective_matching_observation(
    cursor: Cursor[Any], epoch_id: int, observation_id: str
) -> M5MatchingObservationPoint | None:
    point = resolved_matching_observation_point(cursor, epoch_id, observation_id)
    if isinstance(point, M5MatchingObservationWorking) and not point.present:
        return None
    return point


def effective_matching_edge(
    cursor: Cursor[Any],
    epoch_id: int,
    requirement_version_id: str,
    text_hash: str,
) -> M5MatchingEdgePoint | None:
    point = resolved_matching_edge_point(
        cursor, epoch_id, requirement_version_id, text_hash
    )
    if isinstance(point, M5MatchingEdgeWorking) and point.refcount == 0:
        return None
    return point


def effective_matching_mask(
    cursor: Cursor[Any], epoch_id: int, group_version_id: str, text_hash: str
) -> int:
    point = resolved_matching_mask_point(cursor, epoch_id, group_version_id, text_hash)
    return 0 if point is None else point.mask


def effective_matching_hall(
    cursor: Cursor[Any], epoch_id: int, group_version_id: str
) -> HallMaskState | None:
    point = resolved_matching_hall_point(cursor, epoch_id, group_version_id)
    if point is None or (
        isinstance(point, M5MatchingHallWorking) and not point.present
    ):
        return None
    return HallMaskState(
        point.requirement_count,  # type: ignore[arg-type]
        point.mask_histogram,  # type: ignore[arg-type]
        point.neighbor_counts,  # type: ignore[arg-type]
        point.deficiencies,  # type: ignore[arg-type]
        point.maximum_deficiency,  # type: ignore[arg-type]
        point.matching_size,  # type: ignore[arg-type]
        point.distinct_hash_count,  # type: ignore[arg-type]
    )


def least_effective_observation(
    cursor: Cursor[Any],
    epoch_id: int,
    group_version_id: str,
    requirement_ordinal: int,
    text_hash: str,
) -> str | None:
    """Return the least effective observation under PostgreSQL C collation."""

    _require_positive_int("epoch_id", epoch_id)
    _require_text("group_version_id", group_version_id)
    _require_nonnegative_int("requirement_ordinal", requirement_ordinal)
    _require_sha256("text_hash", text_hash)
    effective_matching_image(cursor, epoch_id)
    row = cursor.execute(
        """
        SELECT observation_id
        FROM (
          SELECT working.observation_id
          FROM groundloop_m5_matching_observation_working AS working
          WHERE working.epoch_id = %s
            AND working.group_version_id = %s
            AND working.requirement_ordinal = %s
            AND working.text_hash = %s
            AND working.present
          UNION ALL
          SELECT current_row.observation_id
          FROM groundloop_m5_matching_observation_current AS current_row
          WHERE current_row.group_version_id = %s
            AND current_row.requirement_ordinal = %s
            AND current_row.text_hash = %s
            AND NOT EXISTS (
              SELECT 1
              FROM groundloop_m5_matching_observation_working AS shadow
              WHERE shadow.epoch_id = %s
                AND shadow.observation_id = current_row.observation_id
            )
        ) AS effective
        ORDER BY observation_id COLLATE "C"
        LIMIT 1
        """,
        (
            epoch_id,
            group_version_id,
            requirement_ordinal,
            text_hash,
            group_version_id,
            requirement_ordinal,
            text_hash,
            epoch_id,
        ),
    ).fetchone()
    return None if row is None else _text(row[0])


def representative_effective_hashes(
    cursor: Cursor[Any],
    epoch_id: int,
    group_version_id: str,
    mask: int,
    limit: int,
) -> tuple[str, ...]:
    """Return the bounded C-ordered effective hashes for one exact mask."""

    _require_positive_int("epoch_id", epoch_id)
    _require_text("group_version_id", group_version_id)
    _require_positive_int("mask", mask)
    _require_nonnegative_int("limit", limit)
    if mask > 255 or limit > 8:
        raise ValidationError("representative request exceeds the frozen bound")
    effective_matching_image(cursor, epoch_id)
    hall = effective_matching_hall(cursor, epoch_id, group_version_id)
    if hall is None:
        raise ValidationError("representative request names no effective Hall group")
    if mask >= 1 << hall.requirement_count or limit != hall.requirement_count:
        raise ValidationError("representative request exceeds its group shape")
    rows = cursor.execute(
        """
        SELECT text_hash
        FROM (
          SELECT working.text_hash::text AS text_hash
          FROM groundloop_m5_matching_hash_mask_working AS working
          WHERE working.epoch_id = %s
            AND working.group_version_id = %s
            AND working.mask = %s
          UNION ALL
          SELECT current_row.text_hash::text AS text_hash
          FROM groundloop_m5_matching_hash_mask_current AS current_row
          WHERE current_row.group_version_id = %s
            AND current_row.mask = %s
            AND NOT EXISTS (
              SELECT 1
              FROM groundloop_m5_matching_hash_mask_working AS shadow
              WHERE shadow.epoch_id = %s
                AND shadow.group_version_id = current_row.group_version_id
                AND shadow.text_hash = current_row.text_hash
            )
        ) AS effective
        ORDER BY text_hash COLLATE "C"
        LIMIT %s
        """,
        (epoch_id, group_version_id, mask, group_version_id, mask, epoch_id, limit),
    ).fetchall()
    hashes = tuple(_sha256_text(row[0]) for row in rows)
    expected_count = min(hall.mask_histogram[mask], hall.requirement_count)
    if len(hashes) != expected_count:
        raise ValidationError(
            "representative query cardinality differs from the effective Hall image"
        )
    return hashes


def _fail_nonempty(source_description: str) -> NoReturn:
    raise InvalidEventError(
        "persisted matching store-core checkpoint rejects non-empty "
        f"{source_description} before its first write"
    )


def _group_shapes_for_ids(
    cursor: Cursor[Any], group_ids: Sequence[str]
) -> tuple[M5MatchingGroupShape, ...]:
    if not group_ids:
        return ()
    rows = cursor.execute(
        """
        SELECT version.group_version_id, requirement.ordinal,
               requirement.requirement_version_id
        FROM groundloop_m5_group_version AS version
        JOIN groundloop_m5_requirement_version AS requirement
          USING (group_version_id)
        WHERE version.group_version_id = ANY(%s)
        ORDER BY version.group_version_id COLLATE "C", requirement.ordinal
        """,
        (list(group_ids),),
    ).fetchall()
    by_group: dict[str, list[tuple[int, str]]] = {
        group_id: [] for group_id in group_ids
    }
    for row in rows:
        group_id = _text(row[0])
        if group_id not in by_group:
            raise ValidationError("matching group-shape query returned another group")
        by_group[group_id].append((int(row[1]), _text(row[2])))
    result = tuple(
        M5MatchingGroupShape(
            group_id,
            len(by_group[group_id]),
            tuple(by_group[group_id]),
        )
        for group_id in sorted(by_group)
    )
    if any(shape.requirement_count == 0 for shape in result):
        raise InvalidEventError("matching group has no bounded requirement shape")
    return result


def _structural_group_projection(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    source_id: str,
    update_kind: str,
) -> dict[str, object]:
    """Derive the bounded group-event lock plan from persisted structure."""

    successor_groups: tuple[str, ...]
    if update_kind == "register_group":
        rows = cursor.execute(
            """
            SELECT group_version_id
            FROM groundloop_m5_group_version
            WHERE creator_epoch_id = %s
            ORDER BY group_version_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
        if len(rows) != 1:
            raise InvalidEventError("group registration must create exactly one group")
        predecessor_groups: tuple[str, ...] = ()
        successor_groups = (_text(rows[0][0]),)
    else:
        row = cursor.execute(
            """
            SELECT group_version_id, action, successor_group_version_id, event_id
            FROM groundloop_m5_group_deactivation
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchall()
        if len(row) != 1:
            raise InvalidEventError("group transition requires one deactivation")
        deactivation = row[0]
        expected_action = "REPLACE" if update_kind == "replace_group" else "RETIRE"
        if (
            _text(deactivation[1]) != expected_action
            or _text(deactivation[3]) != source_id
        ):
            raise EventConflictError(
                "group deactivation differs from structural source"
            )
        predecessor_groups = (_text(deactivation[0]),)
        successor = None if deactivation[2] is None else _text(deactivation[2])
        if (update_kind == "replace_group") != (successor is not None):
            raise EventConflictError("group successor differs from update kind")
        successor_groups = () if successor is None else (successor,)
    group_ids = tuple(sorted((*predecessor_groups, *successor_groups)))
    shapes = _group_shapes_for_ids(cursor, group_ids)
    observation_rows = cursor.execute(
        """
        SELECT observation_id, requirement_version_id, group_version_id,
               requirement_ordinal, text_hash
        FROM groundloop_m5_matching_observation_current
        WHERE group_version_id = ANY(%s)
        ORDER BY observation_id COLLATE "C"
        """,
        (list(predecessor_groups),),
    ).fetchall()
    edge_rows = cursor.execute(
        """
        SELECT group_version_id, requirement_ordinal, text_hash,
               requirement_version_id
        FROM groundloop_m5_matching_edge_current
        WHERE group_version_id = ANY(%s)
        ORDER BY group_version_id COLLATE "C", requirement_ordinal,
                 text_hash COLLATE "C", requirement_version_id COLLATE "C"
        """,
        (list(predecessor_groups),),
    ).fetchall()
    mask_rows = cursor.execute(
        """
        SELECT group_version_id, text_hash
        FROM groundloop_m5_matching_hash_mask_current
        WHERE group_version_id = ANY(%s)
        ORDER BY group_version_id COLLATE "C", text_hash COLLATE "C"
        """,
        (list(predecessor_groups),),
    ).fetchall()
    hall_rows = cursor.execute(
        """
        SELECT group_version_id
        FROM groundloop_m5_matching_hall_current
        WHERE group_version_id = ANY(%s)
        ORDER BY group_version_id COLLATE "C"
        """,
        (list(predecessor_groups),),
    ).fetchall()
    requirements = tuple(
        requirement_id for shape in shapes for _, requirement_id in shape.requirements
    )
    owners = cursor.execute(
        """
        SELECT validity.claim_id, claim.answer_version_id, claim.required
        FROM groundloop_m5_group_validity AS validity
        JOIN groundloop_claim AS claim USING (claim_id)
        WHERE validity.group_version_id = ANY(%s)
        ORDER BY validity.claim_id COLLATE "C", claim.answer_version_id COLLATE "C"
        """,
        (list(group_ids),),
    ).fetchall()
    claim_ids = tuple(sorted({_text(row[0]) for row in owners}))
    answer_ids = tuple(sorted({_text(row[1]) for row in owners}))
    group_certificate_ids = group_ids
    claim_certificate_ids = claim_ids
    if update_kind == "retire_group":
        if len(owners) != 1:
            raise EventConflictError("retired group owner authority changed")
        before_row = cursor.execute(
            """
            SELECT previous_published_epoch_id
            FROM groundloop_m5_update
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        if before_row is None:
            raise EventConflictError("retired group predecessor changed")
        before_epoch_id = int(before_row[0])
        group_state = cursor.execute(
            """
            SELECT complete
            FROM groundloop_m5_published_group_state
            WHERE group_version_id = %s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            """,
            (predecessor_groups[0], before_epoch_id, before_epoch_id),
        ).fetchone()
        if group_state is None:
            raise EventConflictError("retired group state authority changed")
        if bool(group_state[0]):
            claim_state = cursor.execute(
                """
                SELECT complete_group_ids
                FROM groundloop_m5_published_claim_state
                WHERE claim_id = %s AND valid_from_epoch <= %s
                  AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
                """,
                (_text(owners[0][0]), before_epoch_id, before_epoch_id),
            ).fetchone()
            if claim_state is None:
                raise EventConflictError("retired claim state authority changed")
            complete_groups = tuple(_text(value) for value in claim_state[0])
            if predecessor_groups[0] not in complete_groups:
                raise EventConflictError("retired complete group index changed")
            after_groups = tuple(
                value for value in complete_groups if value != predecessor_groups[0]
            )
            if after_groups:
                group_certificate_ids = tuple(
                    sorted({*group_certificate_ids, after_groups[0]})
                )
    return {
        "group_shapes": shapes,
        "observation_ids": tuple(_text(row[0]) for row in observation_rows),
        "edge_keys": tuple(
            (_text(row[0]), int(row[1]), _sha256_text(row[2]), _text(row[3]))
            for row in edge_rows
        ),
        "mask_keys": tuple((_text(row[0]), _sha256_text(row[1])) for row in mask_rows),
        "hall_group_ids": tuple(
            sorted({*(_text(row[0]) for row in hall_rows), *successor_groups})
        ),
        "requirement_state_ids": tuple(sorted(requirements)),
        "group_state_ids": group_ids,
        "claim_state_ids": claim_ids,
        "answer_state_ids": answer_ids,
        "group_certificate_ids": group_certificate_ids,
        "claim_certificate_ids": claim_certificate_ids,
    }


def _document_deactivated_chunks(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    source_id: str,
    update_kind: str,
) -> tuple[str, ...]:
    """Revalidate the already-held D29 structural source and derive exact D."""

    expected_direct_kind = {
        "document_delete": "delete",
        "document_replace": "replace",
    }.get(update_kind)
    if expected_direct_kind is None:
        raise InvalidEventError("document withdrawal received another update kind")
    source = cursor.execute(
        """
        SELECT direct.update_kind, epoch.event_id,
               typed_update.update_kind, deactivation.document_version_id
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m4_update AS direct USING (epoch_id)
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        LEFT JOIN groundloop_m4_structural_deactivation AS deactivation
          USING (epoch_id)
        WHERE epoch.epoch_id = %s
        ORDER BY deactivation.document_version_id COLLATE "C"
        """,
        (epoch_id,),
    ).fetchall()
    if len(source) != 1 or (
        _text(source[0][0]) != expected_direct_kind
        or _text(source[0][1]) != source_id
        or _text(source[0][2]) != update_kind
        or source[0][3] is None
    ):
        raise EventConflictError("document withdrawal source changed")
    document_version_id = _text(source[0][3])
    rows = cursor.execute(
        """
        SELECT chunk.chunk_version_id, version.valid_to_epoch,
               chunk.valid_to_epoch
        FROM groundloop_document_version AS version
        JOIN groundloop_chunk_version AS chunk USING (document_version_id)
        WHERE version.document_version_id = %s
        ORDER BY chunk.chunk_version_id COLLATE "C"
        """,
        (document_version_id,),
    ).fetchall()
    chunks = tuple(_text(row[0]) for row in rows)
    if (
        not chunks
        or len(chunks) != len(set(chunks))
        or any(
            row[1] is None
            or int(row[1]) != epoch_id
            or row[2] is None
            or int(row[2]) != epoch_id
            for row in rows
        )
    ):
        raise EventConflictError("document withdrawal chunk authority changed")
    return chunks


def _document_currency_before_images(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    before_epoch_id: int,
    resulting_revision: int,
    source_id: str,
    update_kind: str,
    reserve_targets: bool,
) -> tuple[_ObservationCurrencyBeforeImage, ...]:
    """Bind every held current requirement-currency key on withdrawn chunks."""

    chunks = _document_deactivated_chunks(
        cursor,
        epoch_id=epoch_id,
        source_id=source_id,
        update_kind=update_kind,
    )
    located: list[tuple[object, ...]] = []
    for chunk_id in chunks:
        located.extend(
            tuple(row)
            for row in cursor.execute(
                """
                SELECT currency.subject_kind::text, currency.subject_id,
                       currency.chunk_version_id, currency.task_type,
                       currency.observation_id, currency.installed_revision
                FROM groundloop_observation_currency AS currency
                WHERE currency.subject_kind = 'requirement'
                  AND currency.chunk_version_id = %s
                ORDER BY currency.subject_id COLLATE "C",
                         currency.task_type COLLATE "C",
                         currency.observation_id COLLATE "C"
                """,
                (chunk_id,),
            ).fetchall()
        )
    ordered = tuple(
        sorted(
            located,
            key=lambda row: (
                _text(row[1]).encode(),
                _text(row[2]).encode(),
                _text(row[3]).encode(),
                _text(row[4]).encode(),
            ),
        )
    )
    keys = tuple((_text(row[1]), _text(row[2]), _text(row[3])) for row in ordered)
    if len(keys) != len(set(keys)):
        raise EventConflictError("document withdrawal current currency is not unique")

    result: list[_ObservationCurrencyBeforeImage] = []
    for current in ordered:
        subject_id = _text(current[1])
        chunk_id = _text(current[2])
        task_type = _text(current[3])
        observation_id = _text(current[4])
        semantic = cursor.execute(
            """
            SELECT observation_id, subject_kind, subject_id, chunk_version_id,
                   task_type, input_hash, produced_epoch, raw_output_hash,
                   eligible_for_currency
            FROM groundloop_semantic_observation
            WHERE observation_id = %s
            """,
            (observation_id,),
        ).fetchone()
        if semantic is None or (
            _text(semantic[0]) != observation_id
            or _text(semantic[1]) != "requirement"
            or _text(semantic[2]) != subject_id
            or _text(semantic[3]) != chunk_id
            or _text(semantic[4]) != task_type
            or not bool(semantic[8])
        ):
            raise EventConflictError("document withdrawal observation changed")
        published = cursor.execute(
            """
            SELECT subject_kind::text, subject_id, chunk_version_id, task_type,
                   observation_id, valid_from_epoch, valid_to_epoch
            FROM groundloop_published_observation_currency
            WHERE subject_kind = 'requirement' AND subject_id = %s
              AND chunk_version_id = %s AND task_type = %s
              AND valid_from_epoch <= %s AND valid_to_epoch IS NULL
            """,
            (subject_id, chunk_id, task_type, before_epoch_id),
        ).fetchall()
        if len(published) != 1 or _text(published[0][4]) != observation_id:
            raise EventConflictError("document withdrawal published currency changed")
        key = (epoch_id, "requirement", subject_id, chunk_id, task_type)
        if reserve_targets:
            _reserve_matching_absence(
                cursor,
                "groundloop_m5_working_currency_history",
                (*key, resulting_revision),
            )
            _reserve_matching_absence(
                cursor,
                "groundloop_m5_working_currency_history#open",
                key,
            )
        suffix = " FOR UPDATE" if reserve_targets else ""
        target = cursor.execute(
            """
            SELECT epoch_id, subject_kind, subject_id, chunk_version_id,
                   task_type, observation_id, valid_from_revision,
                   valid_to_revision
            FROM groundloop_m5_working_currency_history
            WHERE epoch_id = %s AND subject_kind = %s AND subject_id = %s
              AND chunk_version_id = %s AND task_type = %s
              AND (valid_from_revision = %s OR valid_to_revision IS NULL)
            """
            + suffix,
            (*key, resulting_revision),
        ).fetchall()
        if target:
            raise EventConflictError(
                "document withdrawal working currency target appeared"
            )
        result.append(
            _ObservationCurrencyBeforeImage(
                key=key,
                semantic_observation_rows=(tuple(semantic),),
                current_currency_row=tuple(current),
                published_currency_row=tuple(published[0]),
                working_currency_row=None,
            )
        )
    return tuple(result)


def _document_removed_observations(
    cursor: Cursor[Any],
    *,
    before_epoch_id: int,
    before_revision: int,
    currency_before_images: tuple[_ObservationCurrencyBeforeImage, ...],
) -> tuple[M5MatchingObservationCurrent, ...]:
    """Project only canonical current SUPPORT memberships from withdrawn currency."""

    removed: list[M5MatchingObservationCurrent] = []
    for before in currency_before_images:
        current = before.current_currency_row
        if current is None or current[4] is None:
            raise EventConflictError("document withdrawal current currency disappeared")
        observation_id = _text(current[4])
        point = cursor.execute(
            """
            SELECT observation_id, requirement_version_id, group_version_id,
                   requirement_ordinal, text_hash, installed_epoch_id,
                   installed_revision
            FROM groundloop_m5_matching_observation_current
            WHERE observation_id = %s
            """,
            (observation_id,),
        ).fetchone()
        task_type = before.key[4]
        if point is None:
            continue
        if task_type != "verify_requirement_v1":
            raise EventConflictError(
                "matching-inert document currency has a matching membership"
            )
        observation = M5MatchingObservationCurrent(
            M5MatchingLayer.CURRENT,
            _text(point[0]),
            _text(point[1]),
            _text(point[2]),
            int(point[3]),
            _sha256_text(point[4]),
            int(point[5]),
            int(point[6]),
        )
        if (
            observation.observation_id != observation_id
            or observation.requirement_version_id != before.key[2]
            or observation.installed_epoch_id != before_epoch_id
            or observation.installed_revision != before_revision
        ):
            raise EventConflictError("document withdrawal matching observation changed")
        removed.append(observation)
    return tuple(sorted(removed, key=lambda row: row.observation_id.encode()))


def _document_edge_transitions(
    cursor: Cursor[Any],
    removed: tuple[M5MatchingObservationCurrent, ...],
) -> tuple[_DocumentEdgeTransition, ...]:
    removals = Counter((row.requirement_version_id, row.text_hash) for row in removed)
    result: list[_DocumentEdgeTransition] = []
    coordinates = {
        (row.requirement_version_id, row.text_hash): (
            row.group_version_id,
            row.requirement_ordinal,
        )
        for row in removed
    }
    if len(coordinates) != len(
        {
            (
                row.requirement_version_id,
                row.text_hash,
                row.group_version_id,
                row.requirement_ordinal,
            )
            for row in removed
        }
    ):
        raise EventConflictError("document withdrawal edge coordinates disagree")
    for requirement_id, text_hash in sorted(removals):
        row = cursor.execute(
            """
            SELECT requirement_version_id, text_hash, group_version_id,
                   requirement_ordinal, refcount, installed_epoch_id,
                   installed_revision
            FROM groundloop_m5_matching_edge_current
            WHERE requirement_version_id = %s AND text_hash = %s
            """,
            (requirement_id, text_hash),
        ).fetchone()
        if row is None:
            raise EventConflictError("document withdrawal matching edge disappeared")
        before = M5MatchingEdgeCurrent(
            M5MatchingLayer.CURRENT,
            _text(row[0]),
            _sha256_text(row[1]),
            _text(row[2]),
            int(row[3]),
            int(row[4]),
            int(row[5]),
            int(row[6]),
        )
        if (before.group_version_id, before.requirement_ordinal) != coordinates[
            (requirement_id, text_hash)
        ] or before.refcount < removals[(requirement_id, text_hash)]:
            raise EventConflictError("document withdrawal matching edge changed")
        result.append(
            _DocumentEdgeTransition(
                before,
                before.refcount - removals[(requirement_id, text_hash)],
            )
        )
    return tuple(result)


def _document_mask_transitions(
    cursor: Cursor[Any],
    edges: tuple[_DocumentEdgeTransition, ...],
) -> tuple[_DocumentMaskTransition, ...]:
    cleared_bits: dict[tuple[str, str], int] = {}
    for transition in edges:
        edge_before = transition.before
        if transition.after_refcount == 0:
            key = (edge_before.group_version_id, edge_before.text_hash)
            cleared_bits[key] = cleared_bits.get(key, 0) | (
                1 << edge_before.requirement_ordinal
            )
    result: list[_DocumentMaskTransition] = []
    for group_id, text_hash in sorted(cleared_bits):
        row = cursor.execute(
            """
            SELECT group_version_id, text_hash, mask,
                   installed_epoch_id, installed_revision
            FROM groundloop_m5_matching_hash_mask_current
            WHERE group_version_id = %s AND text_hash = %s
            """,
            (group_id, text_hash),
        ).fetchone()
        if row is None:
            raise EventConflictError("document withdrawal matching mask disappeared")
        mask_before = M5MatchingMaskCurrent(
            M5MatchingLayer.CURRENT,
            _text(row[0]),
            _sha256_text(row[1]),
            int(row[2]),
            int(row[3]),
            int(row[4]),
        )
        bits = cleared_bits[(group_id, text_hash)]
        if mask_before.mask & bits != bits:
            raise EventConflictError("document withdrawal matching mask changed")
        result.append(_DocumentMaskTransition(mask_before, mask_before.mask & ~bits))
    return tuple(result)


def _document_representative_candidates(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    shape: M5MatchingGroupShape,
    hall_after: HallMaskState,
    masks: tuple[_DocumentMaskTransition, ...],
    intent: M5PersistedMatchingTransitionIntent | None,
) -> tuple[tuple[str, int], ...]:
    """Perform the sole discovery, or revalidate only its retained lock keys."""

    patched = {
        row.before.text_hash: row.after_mask
        for row in masks
        if row.before.group_version_id == shape.group_version_id
    }
    if intent is None:
        changed_hashes = sorted(patched)
        rows = cursor.execute(
            """
            WITH changed(text_hash, mask) AS (
              SELECT * FROM unnest(%s::text[], %s::integer[])
            ), effective AS (
              SELECT current_row.text_hash::text AS text_hash,
                     COALESCE(changed.mask, current_row.mask) AS mask
              FROM groundloop_m5_matching_hash_mask_current AS current_row
              LEFT JOIN changed USING (text_hash)
              WHERE current_row.group_version_id = %s
            ), ranked AS (
              SELECT text_hash, mask,
                     row_number() OVER (
                       PARTITION BY mask ORDER BY text_hash COLLATE "C"
                     ) AS representative_ordinal
              FROM effective
              WHERE mask > 0
            )
            SELECT text_hash, mask
            FROM ranked
            WHERE representative_ordinal <= %s
            ORDER BY text_hash COLLATE "C"
            """,
            (
                changed_hashes,
                [patched[text_hash] for text_hash in changed_hashes],
                shape.group_version_id,
                shape.requirement_count,
            ),
        ).fetchall()
        effective = tuple((_sha256_text(row[0]), int(row[1])) for row in rows)
    else:
        effective_values: list[tuple[str, int]] = []
        for group_id, text_hash in intent.mask_keys:
            if group_id != shape.group_version_id:
                continue
            row = cursor.execute(
                """
                SELECT mask
                FROM groundloop_m5_matching_hash_mask_current
                WHERE group_version_id = %s AND text_hash = %s
                """,
                (group_id, text_hash),
            ).fetchone()
            if row is None:
                raise EventConflictError(
                    "document representative matching mask disappeared"
                )
            effective_values.append((text_hash, patched.get(text_hash, int(row[0]))))
        effective = tuple(effective_values)
    by_mask: dict[int, list[str]] = {}
    for text_hash, mask in effective:
        if mask:
            by_mask.setdefault(mask, []).append(text_hash)
    candidates = tuple(
        sorted(
            (
                (text_hash, mask)
                for mask, hashes in by_mask.items()
                for text_hash in sorted(set(hashes))[: shape.requirement_count]
            ),
            key=lambda item: item[0],
        )
    )
    expected = Counter(
        {
            mask: min(hall_after.mask_histogram[mask], shape.requirement_count)
            for mask in range(1, 1 << shape.requirement_count)
            if hall_after.mask_histogram[mask]
        }
    )
    if Counter(mask for _, mask in candidates) != expected:
        raise EventConflictError("document representative matching mask set changed")
    return candidates


def _document_selected_observations(
    cursor: Cursor[Any],
    *,
    shape: M5MatchingGroupShape,
    pairs: tuple[tuple[int, str], ...],
    removed_ids: frozenset[str],
    intent: M5PersistedMatchingTransitionIntent | None,
) -> tuple[tuple[int, str, str], ...]:
    if not pairs:
        return ()
    pair_set = set(pairs)
    if intent is None:
        ordered_pairs = tuple(sorted(pair_set))
        rows = cursor.execute(
            """
            WITH selected(requirement_ordinal, text_hash) AS (
              SELECT * FROM unnest(%s::integer[], %s::text[])
            )
            SELECT DISTINCT ON (
                     current_row.requirement_ordinal,
                     current_row.text_hash COLLATE "C"
                   )
                   current_row.requirement_ordinal,
                   current_row.text_hash,
                   current_row.observation_id
            FROM groundloop_m5_matching_observation_current AS current_row
            JOIN selected USING (requirement_ordinal, text_hash)
            WHERE current_row.group_version_id = %s
              AND NOT (current_row.observation_id = ANY(%s::text[]))
            ORDER BY current_row.requirement_ordinal,
                     current_row.text_hash COLLATE "C",
                     current_row.observation_id COLLATE "C"
            """,
            (
                [ordinal for ordinal, _ in ordered_pairs],
                [text_hash for _, text_hash in ordered_pairs],
                shape.group_version_id,
                sorted(removed_ids),
            ),
        ).fetchall()
        values = tuple(
            (int(row[0]), _sha256_text(row[1]), _text(row[2])) for row in rows
        )
    else:
        values_list: list[tuple[int, str, str]] = []
        for observation_id in intent.observation_ids:
            if observation_id in removed_ids:
                continue
            row = cursor.execute(
                """
                SELECT group_version_id, requirement_ordinal, text_hash
                FROM groundloop_m5_matching_observation_current
                WHERE observation_id = %s
                """,
                (observation_id,),
            ).fetchone()
            if row is not None and _text(row[0]) == shape.group_version_id:
                values_list.append((int(row[1]), _sha256_text(row[2]), observation_id))
        values = tuple(values_list)
    selected: list[tuple[int, str, str]] = []
    for ordinal, text_hash in sorted(pair_set):
        candidates = sorted(
            observation_id
            for candidate_ordinal, candidate_hash, observation_id in values
            if candidate_ordinal == ordinal
            and candidate_hash == text_hash
            and observation_id not in removed_ids
        )
        if not candidates:
            raise EventConflictError("document representative observation disappeared")
        selected.append((ordinal, text_hash, candidates[0]))
    return tuple(selected)


def _document_withdrawal_preview(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    before_epoch_id: int,
    before_revision: int,
    resulting_revision: int,
    decision_policy_version: str,
    source_id: str,
    update_kind: str,
    reserve_currency_targets: bool,
    lock_image_before_discovery: bool,
    intent: M5PersistedMatchingTransitionIntent | None = None,
) -> _DocumentWithdrawalPreview:
    """Derive the coalesced physical withdrawal from held D29 authority."""

    currency = _document_currency_before_images(
        cursor,
        epoch_id=epoch_id,
        before_epoch_id=before_epoch_id,
        resulting_revision=resulting_revision,
        source_id=source_id,
        update_kind=update_kind,
        reserve_targets=reserve_currency_targets,
    )
    removed = _document_removed_observations(
        cursor,
        before_epoch_id=before_epoch_id,
        before_revision=before_revision,
        currency_before_images=currency,
    )
    edges = _document_edge_transitions(cursor, removed)
    masks = _document_mask_transitions(cursor, edges)
    if lock_image_before_discovery:
        if intent is not None:
            raise ValidationError(
                "document locked-intent recomputation cannot reacquire its image"
            )
        _lock_structural_matching_image(
            cursor,
            epoch_id=epoch_id,
            before_epoch_id=before_epoch_id,
            before_revision=before_revision,
            decision_policy_version=decision_policy_version,
        )
    group_ids = tuple(sorted({row.group_version_id for row in removed}))
    shapes = _group_shapes_for_ids(cursor, group_ids)
    masks_by_group: dict[str, list[_DocumentMaskTransition]] = {}
    for transition in masks:
        masks_by_group.setdefault(transition.before.group_version_id, []).append(
            transition
        )
    removed_ids = frozenset(row.observation_id for row in removed)
    groups: list[_DocumentGroupPreview] = []
    for shape in shapes:
        row = cursor.execute(
            """
            SELECT group_version_id, requirement_count, mask_histogram,
                   neighbor_counts, deficiencies, maximum_deficiency,
                   matching_size, distinct_hash_count, installed_epoch_id,
                   installed_revision
            FROM groundloop_m5_matching_hall_current
            WHERE group_version_id = %s
            """,
            (shape.group_version_id,),
        ).fetchone()
        if row is None:
            raise EventConflictError("document withdrawal Hall state disappeared")
        hall_before = M5MatchingHallCurrent(
            M5MatchingLayer.CURRENT,
            _text(row[0]),
            int(row[1]),
            _tuple_ints(row[2]),
            _tuple_ints(row[3]),
            _tuple_ints(row[4]),
            int(row[5]),
            int(row[6]),
            int(row[7]),
            int(row[8]),
            int(row[9]),
        )
        old_hall = _hall_state_from_point(hall_before)
        if old_hall.requirement_count != shape.requirement_count:
            raise EventConflictError("document withdrawal Hall shape changed")
        transitions = tuple(
            HashMaskTransition(
                transition.before.text_hash,
                transition.before.mask,
                transition.after_mask,
            )
            for transition in sorted(
                masks_by_group.get(shape.group_version_id, ()),
                key=lambda value: value.before.text_hash,
            )
            if transition.before.mask != transition.after_mask
        )
        hall_result = apply_hash_mask_transitions(old_hall, transitions)
        candidates = _document_representative_candidates(
            cursor,
            epoch_id=epoch_id,
            shape=shape,
            hall_after=hall_result.state,
            masks=tuple(masks_by_group.get(shape.group_version_id, ())),
            intent=intent,
        )
        matching = affected_group_matching(shape.requirement_count, candidates)
        selected = _document_selected_observations(
            cursor,
            shape=shape,
            pairs=tuple(
                (pair.requirement_ordinal, pair.text_hash) for pair in matching.pairs
            )
            if matching.complete
            else (),
            removed_ids=removed_ids,
            intent=intent,
        )
        view = _BoundedCertificateView(
            epoch_id=epoch_id,
            revision=resulting_revision,
            decision_policy_version=decision_policy_version,
            group_version_id=shape.group_version_id,
            requirement_version_ids=tuple(
                requirement_id for _, requirement_id in shape.requirements
            ),
            candidates=candidates,
            selected_observations=selected,
            histogram=hall_result.state.mask_histogram,
        )
        reconstruction = reconstruct_certificate(view)
        if reconstruction.matching != matching:
            raise AssertionError("document matching reconstruction changed")
        groups.append(
            _DocumentGroupPreview(
                shape,
                hall_before,
                hall_result.state,
                hall_result.work,
                view,
                reconstruction.artifact,
            )
        )
    return _DocumentWithdrawalPreview(
        currency,
        removed,
        edges,
        masks,
        tuple(groups),
    )


def _document_direct_policy(
    cursor: Cursor[Any], decision_policy_version: str
) -> DecisionPolicy:
    row = cursor.execute(
        """
        SELECT policy_version, support_threshold, refute_threshold,
               tie_rule_version
        FROM groundloop_decision_policy
        WHERE policy_version = %s
        """,
        (decision_policy_version,),
    ).fetchone()
    if row is None:
        raise EventConflictError("document direct decision policy disappeared")
    try:
        policy = DecisionPolicy(
            _text(row[0]), float(row[1]), float(row[2]), _text(row[3])
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError("document direct decision policy changed") from error
    if policy.policy_version != decision_policy_version:
        raise EventConflictError("document direct decision policy changed")
    return policy


def _document_direct_plan_from_points(
    *,
    claim_points: tuple[_DocumentDirectClaimBefore, ...],
    answer_points: tuple[_DocumentDirectAnswerBefore, ...],
    observations: tuple[SemanticObservation, ...],
    observation_text_hashes: tuple[tuple[str, str], ...],
    deactivated_chunks: tuple[str, ...],
    policy: DecisionPolicy,
) -> _DocumentDirectPlan:
    """Derive direct M4 after-state from a bounded, exact point closure."""

    claim_ids = tuple(point.claim_id for point in claim_points)
    if claim_ids != tuple(sorted(set(claim_ids))):
        raise EventConflictError("document direct claim authority is not canonical")
    answer_ids = tuple(point.state.answer_version_id for point in answer_points)
    if answer_ids != tuple(sorted(set(answer_ids))):
        raise EventConflictError("document direct answer authority is not canonical")
    observation_ids = tuple(observation.observation_id for observation in observations)
    if observation_ids != tuple(sorted(set(observation_ids))):
        raise EventConflictError(
            "document direct observation authority is not canonical"
        )
    observation_by_id = {
        observation.observation_id: observation for observation in observations
    }
    if type(observation_text_hashes) is not tuple or any(
        type(item) is not tuple or len(item) != 2 for item in observation_text_hashes
    ):
        raise EventConflictError("document direct observation text hash changed")
    if observation_text_hashes != tuple(
        sorted(observation_text_hashes, key=lambda item: item[0])
    ) or len({item[0] for item in observation_text_hashes}) != len(
        observation_text_hashes
    ):
        raise EventConflictError(
            "document direct observation text hashes are not canonical"
        )
    text_hash_by_observation: dict[str, str] = {}
    for item in observation_text_hashes:
        observation_id, text_hash = item
        if type(observation_id) is not str or type(text_hash) is not str:
            raise EventConflictError("document direct observation text hash changed")
        try:
            normalized_hash = _sha256_text(text_hash)
        except ValidationError as error:
            raise EventConflictError(
                "document direct observation text hash changed"
            ) from error
        text_hash_by_observation[observation_id] = normalized_hash
    if set(text_hash_by_observation) != set(observation_by_id):
        raise EventConflictError(
            "document direct observation text hash closure changed"
        )
    named_observation_ids = {
        observation_id
        for point in claim_points
        for observation_id in (
            *point.supporting_observation_ids,
            *point.refuting_observation_ids,
        )
    }
    if not named_observation_ids.issubset(observation_by_id):
        raise EventConflictError("document direct observation closure changed")
    claim_id_set = set(claim_ids)
    for observation in observations:
        if (
            observation.subject_kind is not SubjectKind.CLAIM
            or observation.subject_id not in claim_id_set
        ):
            raise EventConflictError("document direct observation owner changed")

    deactivated = frozenset(deactivated_chunks)
    claim_plans: list[_DocumentDirectClaimPlan] = []
    for point in claim_points:
        support_ids = point.supporting_observation_ids
        refute_ids = point.refuting_observation_ids
        if (
            support_ids != tuple(sorted(set(support_ids)))
            or refute_ids != tuple(sorted(set(refute_ids)))
            or set(support_ids) & set(refute_ids)
            or point.support_count
            != len({text_hash_by_observation[value] for value in support_ids})
            or point.refute_count
            != len({text_hash_by_observation[value] for value in refute_ids})
        ):
            raise EventConflictError("document direct claim provenance changed")
        for observation_id in (*support_ids, *refute_ids):
            located_observation = observation_by_id.get(observation_id)
            if located_observation is None or (
                located_observation.subject_kind is not SubjectKind.CLAIM
                or located_observation.subject_id != point.claim_id
            ):
                raise EventConflictError("document direct observation owner changed")
            label = _direct_operational_label(
                support_score=located_observation.support_score,
                refute_score=located_observation.refute_score,
                neutral_score=located_observation.neutral_score,
                support_threshold=policy.support_threshold,
                refute_threshold=policy.refute_threshold,
            )
            expected_label = (
                VerificationLabel.SUPPORT
                if observation_id in support_ids
                else VerificationLabel.REFUTE
            )
            if label is not expected_label:
                raise EventConflictError(
                    "document direct observation policy label changed"
                )
        best_support = max(
            (observation_by_id[value].support_score for value in support_ids),
            default=None,
        )
        best_refute = max(
            (observation_by_id[value].refute_score for value in refute_ids),
            default=None,
        )
        status = _claim_status(supported=bool(support_ids), refuted=bool(refute_ids))
        certificate_digest = _stable_m4_digest(
            "m4-claim-certificate-v1",
            point.claim_id,
            support_ids[0] if support_ids else "",
            refute_ids[0] if refute_ids else "",
        )
        if (
            point.best_support_score != best_support
            or point.best_refute_score != best_refute
            or point.status is not status
            or point.certificate_digest != certificate_digest
        ):
            raise EventConflictError("document direct claim before-image changed")
        direct_ids = set((*support_ids, *refute_ids))
        claim_observations = tuple(
            observation
            for observation in observations
            if observation.subject_id == point.claim_id
        )
        for observation in claim_observations:
            if observation.observation_id in direct_ids:
                continue
            label = _direct_operational_label(
                support_score=observation.support_score,
                refute_score=observation.refute_score,
                neutral_score=observation.neutral_score,
                support_threshold=policy.support_threshold,
                refute_threshold=policy.refute_threshold,
            )
            if (
                observation.chunk_version_id not in deactivated
                or label is not VerificationLabel.NEUTRAL
            ):
                raise EventConflictError("document direct observation closure changed")
        withdrawn_ids = tuple(
            sorted(
                observation.observation_id
                for observation in claim_observations
                if observation.chunk_version_id in deactivated
            )
        )
        if not withdrawn_ids:
            continue
        withdrawn_set = set(withdrawn_ids)
        support_after = tuple(
            value for value in support_ids if value not in withdrawn_set
        )
        refute_after = tuple(
            value for value in refute_ids if value not in withdrawn_set
        )
        after_state = CombinedClaimState(
            point.claim_id,
            len({text_hash_by_observation[value] for value in support_after}),
            len({text_hash_by_observation[value] for value in refute_after}),
            max(
                (observation_by_id[value].support_score for value in support_after),
                default=None,
            ),
            max(
                (observation_by_id[value].refute_score for value in refute_after),
                default=None,
            ),
            support_after,
            refute_after,
            0,
            (),
            _claim_status(supported=bool(support_after), refuted=bool(refute_after)),
        )
        claim_plans.append(
            _DocumentDirectClaimPlan(
                point.claim_id,
                point.answer_version_id,
                point.required,
                CombinedClaimState(
                    point.claim_id,
                    point.support_count,
                    point.refute_count,
                    point.best_support_score,
                    point.best_refute_score,
                    support_ids,
                    refute_ids,
                    0,
                    (),
                    point.status,
                ),
                point.certificate_digest,
                after_state,
                _stable_m4_digest(
                    "m4-claim-certificate-v1",
                    point.claim_id,
                    support_after[0] if support_after else "",
                    refute_after[0] if refute_after else "",
                ),
                withdrawn_ids,
            )
        )

    answers_by_id = {
        point.state.answer_version_id: point.state for point in answer_points
    }
    affected_answer_ids = tuple(
        sorted({plan.answer_version_id for plan in claim_plans})
    )
    if answer_ids != affected_answer_ids:
        raise EventConflictError("document direct answer closure changed")
    answer_plans: list[_DocumentDirectAnswerPlan] = []
    for answer_id in affected_answer_ids:
        before = answers_by_id[answer_id]
        counts: Counter[ClaimStatus] = Counter(
            {
                ClaimStatus.SUPPORTED: before.supported_count,
                ClaimStatus.UNSUPPORTED: before.unsupported_count,
                ClaimStatus.REFUTED: before.refuted_count,
                ClaimStatus.CONFLICTED: before.conflicted_count,
            }
        )
        if (
            sum(counts.values()) != before.required_claim_count
            or _answer_status(counts, before.required_claim_count) is not before.status
        ):
            raise EventConflictError("document direct answer before-image changed")
        for claim in claim_plans:
            if claim.answer_version_id != answer_id or not claim.required:
                continue
            if claim.before_state.status is not claim.after_state.status:
                counts[claim.before_state.status] -= 1
                counts[claim.after_state.status] += 1
        if min(counts.values(), default=0) < 0:
            raise EventConflictError("document direct answer aggregation changed")
        after = CombinedAnswerState(
            answer_id,
            before.required_claim_count,
            counts[ClaimStatus.SUPPORTED],
            counts[ClaimStatus.UNSUPPORTED],
            counts[ClaimStatus.REFUTED],
            counts[ClaimStatus.CONFLICTED],
            _answer_status(counts, before.required_claim_count),
        )
        answer_plans.append(_DocumentDirectAnswerPlan(before, after))

    remaining_observation_ids = {
        observation_id
        for claim in claim_plans
        for observation_id in (
            *claim.after_state.supporting_observation_ids,
            *claim.after_state.refuting_observation_ids,
        )
    }
    withdrawn_observation_ids = {
        observation_id
        for claim in claim_plans
        for observation_id in claim.withdrawn_observation_ids
    }
    return _DocumentDirectPlan(
        claims=tuple(claim_plans),
        answers=tuple(answer_plans),
        withdrawn_observations=tuple(
            observation_by_id[value] for value in sorted(withdrawn_observation_ids)
        ),
        remaining_observations=tuple(
            observation_by_id[value] for value in sorted(remaining_observation_ids)
        ),
        observation_text_hashes=observation_text_hashes,
    )


def _document_direct_plan_from_database(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    before_epoch_id: int,
    source_id: str,
    update_kind: str,
    decision_policy_version: str,
    candidate_claim_ids: tuple[str, ...],
    expected_plan: _DocumentDirectPlan | None = None,
) -> _DocumentDirectPlan:
    """Point-read the exact published direct state named by a locator set."""

    if candidate_claim_ids != tuple(sorted(set(candidate_claim_ids))):
        raise ValidationError("document direct claim locator is not canonical")
    deactivated_chunks = _document_deactivated_chunks(
        cursor,
        epoch_id=epoch_id,
        source_id=source_id,
        update_kind=update_kind,
    )
    policy = _document_direct_policy(cursor, decision_policy_version)
    claim_points: list[_DocumentDirectClaimBefore] = []
    for claim_id in candidate_claim_ids:
        rows = cursor.execute(
            """
            SELECT claim.claim_id, claim.answer_version_id, claim.required,
                   state.support_count, state.refute_count,
                   state.best_support_score, state.best_refute_score,
                   state.supporting_observation_ids,
                   state.refuting_observation_ids, state.status,
                   state.certificate_digest
            FROM groundloop_claim AS claim
            JOIN groundloop_published_claim_state AS state USING (claim_id)
            WHERE claim.claim_id = %s
              AND state.valid_from_epoch <= %s
              AND (state.valid_to_epoch IS NULL OR %s < state.valid_to_epoch)
            """,
            (claim_id, before_epoch_id, before_epoch_id),
        ).fetchall()
        if len(rows) != 1:
            raise EventConflictError("document direct claim point changed")
        row = rows[0]
        claim_points.append(
            _DocumentDirectClaimBefore(
                _text(row[0]),
                _text(row[1]),
                bool(row[2]),
                int(row[3]),
                int(row[4]),
                None if row[5] is None else float(row[5]),
                None if row[6] is None else float(row[6]),
                tuple(_text(value) for value in row[7]),
                tuple(_text(value) for value in row[8]),
                ClaimStatus(_text(row[9])),
                _sha256_text(row[10]),
            )
        )
    named_observation_id_set = {
        observation_id
        for point in claim_points
        for observation_id in (
            *point.supporting_observation_ids,
            *point.refuting_observation_ids,
        )
    }
    if expected_plan is not None:
        named_observation_id_set.update(
            observation.observation_id
            for observation in (
                *expected_plan.withdrawn_observations,
                *expected_plan.remaining_observations,
            )
        )
    named_observation_ids = tuple(sorted(named_observation_id_set))
    observations: list[SemanticObservation] = []
    observation_text_hashes: list[tuple[str, str]] = []
    if named_observation_ids:
        rows = cursor.execute(
            """
            SELECT observation.observation_id,
                   observation.subject_kind::text, observation.subject_id,
                   observation.chunk_version_id, observation.task_type,
                   observation.support_score, observation.refute_score,
                   observation.neutral_score, observation.model_id,
                   observation.model_version, observation.prompt_version,
                   observation.input_hash, observation.eligible_for_currency,
                   chunk.text, chunk.text_hash
            FROM groundloop_semantic_observation AS observation
            JOIN groundloop_chunk_version AS chunk USING (chunk_version_id)
            WHERE observation.observation_id = ANY(%s)
            ORDER BY observation.observation_id COLLATE "C"
            """,
            (list(named_observation_ids),),
        ).fetchall()
        if tuple(_text(row[0]) for row in rows) != named_observation_ids:
            raise EventConflictError("document direct observation point changed")
        for row in rows:
            if row[12] is not True:
                raise EventConflictError("document direct observation is ineligible")
            try:
                text_hash = _sha256_text(row[14])
            except ValidationError as error:
                raise EventConflictError(
                    "document direct observation source changed"
                ) from error
            if text_hash != normalized_text_hash(_text(row[13])):
                raise EventConflictError("document direct observation source changed")
            try:
                observation = SemanticObservation(
                    observation_id=_text(row[0]),
                    subject_kind=SubjectKind(_text(row[1])),
                    subject_id=_text(row[2]),
                    chunk_version_id=_text(row[3]),
                    task_type=_text(row[4]),
                    support_score=float(row[5]),
                    refute_score=float(row[6]),
                    neutral_score=float(row[7]),
                    producer=ModelStamp(_text(row[8]), _text(row[9]), _text(row[10])),
                    input_hash=_sha256_text(row[11]),
                )
            except (TypeError, ValueError, ValidationError) as error:
                raise EventConflictError(
                    "document direct observation point changed"
                ) from error
            observation_text_hashes.append((observation.observation_id, text_hash))
            current = cursor.execute(
                """
                SELECT subject_kind::text, subject_id, chunk_version_id,
                       task_type, observation_id, installed_revision
                FROM groundloop_observation_currency
                WHERE subject_kind = %s AND subject_id = %s
                  AND chunk_version_id = %s AND task_type = %s
                """,
                observation.key,
            ).fetchall()
            published = cursor.execute(
                """
                SELECT subject_kind::text, subject_id, chunk_version_id,
                       task_type, observation_id, valid_from_epoch,
                       valid_to_epoch
                FROM groundloop_published_observation_currency
                WHERE subject_kind = %s AND subject_id = %s
                  AND chunk_version_id = %s AND task_type = %s
                  AND valid_from_epoch <= %s
                  AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
                """,
                (*observation.key, before_epoch_id, before_epoch_id),
            ).fetchall()
            expected_key = tuple(
                value.value if isinstance(value, SubjectKind) else value
                for value in observation.key
            )
            if (
                len(current) != 1
                or len(published) != 1
                or tuple(_text(value) for value in current[0][:4]) != expected_key
                or _text(current[0][4]) != observation.observation_id
                or isinstance(current[0][5], bool)
                or int(current[0][5]) < 0
                or tuple(_text(value) for value in published[0][:4]) != expected_key
                or _text(published[0][4]) != observation.observation_id
            ):
                raise EventConflictError("document direct observation currency changed")
            observations.append(observation)

    answer_ids = tuple(sorted({point.answer_version_id for point in claim_points}))
    answer_points: list[_DocumentDirectAnswerBefore] = []
    for answer_id in answer_ids:
        rows = cursor.execute(
            """
            SELECT required_claim_count, supported_count, unsupported_count,
                   refuted_count, conflicted_count, status
            FROM groundloop_published_answer_state
            WHERE answer_version_id = %s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            """,
            (answer_id, before_epoch_id, before_epoch_id),
        ).fetchall()
        if len(rows) != 1:
            raise EventConflictError("document direct answer point changed")
        row = rows[0]
        answer_points.append(
            _DocumentDirectAnswerBefore(
                CombinedAnswerState(
                    answer_id,
                    int(row[0]),
                    int(row[1]),
                    int(row[2]),
                    int(row[3]),
                    int(row[4]),
                    AnswerStatus(_text(row[5])),
                )
            )
        )
    direct_plan = _document_direct_plan_from_points(
        claim_points=tuple(claim_points),
        answer_points=tuple(answer_points),
        observations=tuple(observations),
        observation_text_hashes=tuple(observation_text_hashes),
        deactivated_chunks=deactivated_chunks,
        policy=policy,
    )
    if expected_plan is not None and direct_plan != expected_plan:
        raise EventConflictError("document direct matching authority changed")
    return direct_plan


def _document_direct_plan_from_d29_authority(
    cursor: Cursor[Any],
    *,
    authority: object,
    epoch_id: int,
    before_epoch_id: int,
    source_id: str,
    source_identity_hash: str,
    update_kind: str,
    decision_policy_version: str,
) -> _DocumentDirectPlan:
    """Validate and project the exact private D29 direct matching authority."""

    from groundloop.m5.runtime.postgres_withdrawal import (
        _D29DirectMatchingAuthority,
        _validate_d29_direct_matching_authority,
    )

    if type(authority) is not _D29DirectMatchingAuthority:
        raise ValidationError("document matching requires exact D29 direct authority")
    _validate_d29_direct_matching_authority(
        cursor,
        epoch_id=epoch_id,
        source_id=source_id,
        source_identity_hash=source_identity_hash,
        authority=authority,
    )
    try:
        claim_points = tuple(
            _DocumentDirectClaimBefore(
                image.claim_id,
                image.answer_version_id,
                image.required,
                image.support_count,
                image.refute_count,
                image.best_support_score,
                image.best_refute_score,
                image.supporting_observation_ids,
                image.refuting_observation_ids,
                ClaimStatus(image.status),
                image.certificate_digest,
            )
            for image in authority.claim_before_images
        )
        answer_points = tuple(
            _DocumentDirectAnswerBefore(
                CombinedAnswerState(
                    image.answer_version_id,
                    image.required_claim_count,
                    image.supported_count,
                    image.unsupported_count,
                    image.refuted_count,
                    image.conflicted_count,
                    AnswerStatus(image.status),
                )
            )
            for image in authority.answer_before_images
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError("document direct authority state changed") from error
    observations = tuple(
        sorted(
            (*authority.withdrawn_observations, *authority.remaining_observations),
            key=lambda observation: observation.observation_id,
        )
    )
    deactivated_chunks = _document_deactivated_chunks(
        cursor,
        epoch_id=epoch_id,
        source_id=source_id,
        update_kind=update_kind,
    )
    direct_plan = _document_direct_plan_from_points(
        claim_points=claim_points,
        answer_points=answer_points,
        observations=observations,
        observation_text_hashes=authority.observation_text_hashes,
        deactivated_chunks=deactivated_chunks,
        policy=_document_direct_policy(cursor, decision_policy_version),
    )
    claims_by_id = {claim.claim_id: claim for claim in direct_plan.claims}
    if set(claims_by_id) != {image.claim_id for image in authority.claim_before_images}:
        raise EventConflictError("document direct affected claims changed")
    for image in authority.claim_before_images:
        claim = claims_by_id[image.claim_id]
        if (
            claim.withdrawn_observation_ids != image.withdrawn_observation_ids
            or claim.after_state.supporting_observation_ids
            != image.remaining_support_observation_ids
            or claim.after_state.refuting_observation_ids
            != image.remaining_refute_observation_ids
        ):
            raise EventConflictError("document direct claim partition changed")
    if (
        direct_plan.withdrawn_observations != authority.withdrawn_observations
        or direct_plan.remaining_observations != authority.remaining_observations
        or direct_plan.observation_text_hashes != authority.observation_text_hashes
    ):
        raise EventConflictError("document direct observation partition changed")
    return _document_direct_plan_from_database(
        cursor,
        epoch_id=epoch_id,
        before_epoch_id=before_epoch_id,
        source_id=source_id,
        update_kind=update_kind,
        decision_policy_version=decision_policy_version,
        candidate_claim_ids=tuple(claim.claim_id for claim in direct_plan.claims),
        expected_plan=direct_plan,
    )


def _document_direct_expected_claim_images(
    intent: M5PersistedMatchingTransitionIntent,
    direct_plan: _DocumentDirectPlan,
) -> tuple[_DirectStageImage, ...]:
    return tuple(
        _DirectStageImage(
            _DirectStageCoordinate(
                "groundloop_m4_working_claim_state",
                ("epoch_id", "claim_id"),
                (intent.resulting_epoch_id, claim.claim_id),
            ),
            {
                "epoch_id": intent.resulting_epoch_id,
                "claim_id": claim.claim_id,
                "support_count": claim.after_state.support_count,
                "refute_count": claim.after_state.refute_count,
                "best_support_score": claim.after_state.best_support_score,
                "best_refute_score": claim.after_state.best_refute_score,
                "supporting_observation_ids": list(
                    claim.after_state.supporting_observation_ids
                ),
                "refuting_observation_ids": list(
                    claim.after_state.refuting_observation_ids
                ),
                "status": claim.after_state.status.value,
                "certificate_digest": claim.after_certificate_digest,
                "updated_revision": intent.resulting_revision,
            },
        )
        for claim in direct_plan.claims
    )


def _document_direct_expected_answer_images(
    intent: M5PersistedMatchingTransitionIntent,
    direct_plan: _DocumentDirectPlan,
) -> tuple[_DirectStageImage, ...]:
    return tuple(
        _DirectStageImage(
            _DirectStageCoordinate(
                "groundloop_m4_working_answer_state",
                ("epoch_id", "answer_version_id"),
                (intent.resulting_epoch_id, answer.after_state.answer_version_id),
            ),
            {
                "epoch_id": intent.resulting_epoch_id,
                "answer_version_id": answer.after_state.answer_version_id,
                "required_claim_count": answer.after_state.required_claim_count,
                "supported_count": answer.after_state.supported_count,
                "unsupported_count": answer.after_state.unsupported_count,
                "refuted_count": answer.after_state.refuted_count,
                "conflicted_count": answer.after_state.conflicted_count,
                "status": answer.after_state.status.value,
                "updated_revision": intent.resulting_revision,
            },
        )
        for answer in direct_plan.answers
    )


def _document_affected_projection(
    cursor: Cursor[Any],
    *,
    before_epoch_id: int,
    preview: _DocumentWithdrawalPreview,
    direct_claim_ids: tuple[str, ...] = (),
) -> dict[str, object]:
    """Build the complete transient D25 lock plan, including no-op reps."""

    if direct_claim_ids != tuple(sorted(set(direct_claim_ids))):
        raise ValidationError("document direct claim projection is not canonical")

    removed_ids = {row.observation_id for row in preview.removed_observations}
    selected_ids = {
        observation_id
        for group in preview.groups
        for _, _, observation_id in group.certificate_view.selected_observations
    }
    edge_keys = {
        (
            transition.before.group_version_id,
            transition.before.requirement_ordinal,
            transition.before.text_hash,
            transition.before.requirement_version_id,
        )
        for transition in preview.edge_transitions
    }
    shapes = {group.shape.group_version_id: group.shape for group in preview.groups}
    edge_keys.update(
        (
            group.shape.group_version_id,
            ordinal,
            text_hash,
            group.shape.requirements[ordinal][1],
        )
        for group in preview.groups
        for ordinal, text_hash, _ in group.certificate_view.selected_observations
    )
    mask_keys = {
        (transition.before.group_version_id, transition.before.text_hash)
        for transition in preview.mask_transitions
    }
    mask_keys.update(
        (group.shape.group_version_id, text_hash)
        for group in preview.groups
        for text_hash, _ in group.certificate_view.candidates
    )
    group_ids = tuple(sorted(shapes))
    owner_rows = cursor.execute(
        """
        SELECT validity.group_version_id, validity.claim_id,
               claim.answer_version_id, claim.required
        FROM groundloop_m5_group_validity AS validity
        JOIN groundloop_claim AS claim USING (claim_id)
        WHERE validity.group_version_id = ANY(%s)
          AND validity.valid_from_epoch <= %s
          AND (validity.valid_to_epoch IS NULL OR %s < validity.valid_to_epoch)
        ORDER BY validity.group_version_id COLLATE "C"
        """,
        (list(group_ids), before_epoch_id, before_epoch_id),
    ).fetchall()
    if (
        len(owner_rows) != len(group_ids)
        or tuple(_text(row[0]) for row in owner_rows) != group_ids
    ):
        raise EventConflictError("document withdrawal group owner changed")
    direct_owner_rows: tuple[tuple[object, ...], ...] = ()
    if direct_claim_ids:
        direct_owner_rows = tuple(
            tuple(row)
            for row in cursor.execute(
                """
                SELECT claim_id, answer_version_id, required
                FROM groundloop_claim
                WHERE claim_id = ANY(%s)
                ORDER BY claim_id COLLATE "C"
                """,
                (list(direct_claim_ids),),
            ).fetchall()
        )
        if tuple(_text(row[0]) for row in direct_owner_rows) != direct_claim_ids:
            raise EventConflictError("document direct claim ownership changed")
    claim_ids = tuple(
        sorted(
            {
                *(_text(row[1]) for row in owner_rows),
                *(_text(row[0]) for row in direct_owner_rows),
            }
        )
    )
    answer_ids = tuple(
        sorted(
            {
                *(_text(row[2]) for row in owner_rows),
                *(_text(row[1]) for row in direct_owner_rows),
            }
        )
    )
    complete_after = {
        group.shape.group_version_id: group.hall_after.complete
        for group in preview.groups
    }
    group_certificate_ids = set(group_ids)
    for claim_id in claim_ids:
        row = cursor.execute(
            """
            SELECT complete_group_ids
            FROM groundloop_m5_published_claim_state
            WHERE claim_id = %s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            """,
            (claim_id, before_epoch_id, before_epoch_id),
        ).fetchall()
        if len(row) != 1:
            raise EventConflictError("document withdrawal claim state changed")
        after_groups = set(_text(value) for value in row[0][0])
        for group_id, affected_claim_id, _, _ in owner_rows:
            if _text(affected_claim_id) != claim_id:
                continue
            checked_group_id = _text(group_id)
            if complete_after[checked_group_id]:
                after_groups.add(checked_group_id)
            else:
                after_groups.discard(checked_group_id)
        if after_groups:
            group_certificate_ids.add(min(after_groups))
    return {
        "group_shapes": tuple(shapes[group_id] for group_id in group_ids),
        "observation_ids": tuple(sorted(removed_ids | selected_ids)),
        "edge_keys": tuple(sorted(edge_keys)),
        "mask_keys": tuple(sorted(mask_keys)),
        "hall_group_ids": group_ids,
        "requirement_state_ids": tuple(
            sorted({row.requirement_version_id for row in preview.removed_observations})
        ),
        "group_state_ids": group_ids,
        "claim_state_ids": claim_ids,
        "answer_state_ids": answer_ids,
        "group_certificate_ids": tuple(sorted(group_certificate_ids)),
        "claim_certificate_ids": claim_ids,
    }


def _structural_logical_presence(
    cursor: Cursor[Any],
    *,
    before_epoch_id: int,
    affected: dict[str, object],
) -> _StructuralLogicalPresence:
    """Gather every tier-12--14 present/absence coordinate before tier 11."""

    def ids(name: str) -> tuple[str, ...]:
        values = affected[name]
        if not isinstance(values, tuple) or any(
            not isinstance(value, str) for value in values
        ):
            raise ValidationError("structural logical projection is malformed")
        return values

    requirement_ids = ids("requirement_state_ids")
    group_ids = ids("group_state_ids")
    group_certificate_ids = ids("group_certificate_ids")
    claim_ids = ids("claim_state_ids")
    claim_certificate_ids = ids("claim_certificate_ids")
    answer_ids = ids("answer_state_ids")

    def present(query: str, object_ids: tuple[str, ...]) -> tuple[str, ...]:
        if not object_ids:
            return ()
        rows = cursor.execute(
            query,
            (list(object_ids), before_epoch_id, before_epoch_id),
        ).fetchall()
        result = tuple(_text(row[0]) for row in rows)
        if any(value not in object_ids for value in result):
            raise EventConflictError(
                "structural logical presence returned an unplanned key"
            )
        return result

    return _StructuralLogicalPresence(
        requirement_state_ids=present(
            """
            SELECT requirement_version_id
            FROM groundloop_m5_published_requirement_state
            WHERE requirement_version_id = ANY(%s)
              AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            ORDER BY requirement_version_id COLLATE "C"
            """,
            requirement_ids,
        ),
        group_state_ids=present(
            """
            SELECT group_version_id
            FROM groundloop_m5_published_group_state
            WHERE group_version_id = ANY(%s)
              AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            ORDER BY group_version_id COLLATE "C"
            """,
            group_ids,
        ),
        group_certificate_ids=present(
            """
            SELECT group_version_id
            FROM groundloop_m5_published_group_certificate_binding
            WHERE group_version_id = ANY(%s)
              AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            ORDER BY group_version_id COLLATE "C"
            """,
            group_certificate_ids,
        ),
        claim_state_ids=present(
            """
            SELECT claim_id
            FROM groundloop_m5_published_claim_state
            WHERE claim_id = ANY(%s)
              AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            ORDER BY claim_id COLLATE "C"
            """,
            claim_ids,
        ),
        claim_certificate_ids=present(
            """
            SELECT claim_id
            FROM groundloop_m5_published_claim_certificate_binding
            WHERE claim_id = ANY(%s)
              AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            ORDER BY claim_id COLLATE "C"
            """,
            claim_certificate_ids,
        ),
        answer_state_ids=present(
            """
            SELECT answer_version_id
            FROM groundloop_m5_published_answer_state
            WHERE answer_version_id = ANY(%s)
              AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            ORDER BY answer_version_id COLLATE "C"
            """,
            answer_ids,
        ),
    )


def _lock_structural_logical_family(
    cursor: Cursor[Any],
    *,
    relation_name: str,
    object_ids: tuple[str, ...],
    present_ids: tuple[str, ...],
    before_epoch_id: int,
    query: str,
) -> None:
    present_set = set(present_ids)
    if len(present_set) != len(present_ids) or not present_set.issubset(object_ids):
        raise ValidationError("structural logical presence set is malformed")
    for object_id in object_ids:
        if object_id not in present_set:
            _reserve_matching_absence(
                cursor, relation_name, (object_id, before_epoch_id)
            )
        row = cursor.execute(
            query,
            (object_id, before_epoch_id, before_epoch_id),
        ).fetchone()
        if (row is not None) != (object_id in present_set) or (
            row is not None and _text(row[0]) != object_id
        ):
            raise EventConflictError(
                "structural logical present/absence coordinate changed"
            )


def _reserve_structural_working_absence(
    cursor: Cursor[Any],
    *,
    relation_name: str,
    key_parts: Sequence[object],
    query: str,
    parameters: Sequence[object],
) -> None:
    _reserve_matching_absence(cursor, relation_name, key_parts)
    if cursor.execute(query, parameters).fetchone() is not None:
        raise EventConflictError("structural working-state target already exists")


def _lock_structural_matching_image(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    before_epoch_id: int,
    before_revision: int,
    decision_policy_version: str,
) -> None:
    """Acquire the structural current/working image coordinates at tier 11b."""

    current = cursor.execute(
        """
        SELECT decision_policy_version, installed_epoch_id, installed_revision
        FROM groundloop_m5_matching_image_current
        WHERE singleton
        FOR UPDATE
        """
    ).fetchone()
    if current is None or (
        _text(current[0]) != decision_policy_version
        or int(current[1]) != before_epoch_id
        or int(current[2]) != before_revision
    ):
        raise EventConflictError("structural matching current image changed")
    _reserve_matching_absence(
        cursor,
        "groundloop_m5_matching_image_working",
        (epoch_id,),
    )
    working = cursor.execute(
        """
        SELECT epoch_id
        FROM groundloop_m5_matching_image_working
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if working is not None:
        raise EventConflictError("structural matching working image already exists")


def _document_direct_m4_target_ids(
    intent: M5PersistedMatchingTransitionIntent,
    images: tuple[_DirectStageImage, ...],
    *,
    relation_name: str,
    key_columns: tuple[str, str],
    label: str,
) -> tuple[str, ...]:
    """Pure-validate one exact direct-M4 target family before any SQL."""

    object_ids: list[str] = []
    for image in images:
        if type(image) is not _DirectStageImage:
            raise ValidationError(f"document direct {label} image has another key")
        coordinate = image.coordinate
        if (
            type(coordinate) is not _DirectStageCoordinate
            or coordinate.relation_name != relation_name
            or coordinate.key_columns != key_columns
            or type(coordinate.key_parts) is not tuple
            or len(coordinate.key_parts) != 2
            or type(coordinate.key_parts[0]) is not int
            or coordinate.key_parts[0] != intent.resulting_epoch_id
            or type(coordinate.key_parts[1]) is not str
            or not coordinate.key_parts[1].strip()
        ):
            raise ValidationError(f"document direct {label} image has another key")
        object_ids.append(coordinate.key_parts[1])
    result = tuple(object_ids)
    if result != tuple(sorted(set(result))):
        raise ValidationError(f"document direct {label} image repeats a key")
    return result


def _lock_expected_document_direct_m4_claims(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    images: tuple[_DirectStageImage, ...],
) -> None:
    """Acquire expected direct-M4 claim target absences at tier 13."""

    _document_direct_m4_target_ids(
        intent,
        images,
        relation_name="groundloop_m4_working_claim_state",
        key_columns=("epoch_id", "claim_id"),
        label="claim",
    )
    for image in images:
        coordinate = image.coordinate
        _reserve_structural_working_absence(
            cursor,
            relation_name=coordinate.relation_name,
            key_parts=coordinate.key_parts,
            query="""
                SELECT 1 FROM groundloop_m4_working_claim_state
                WHERE epoch_id = %s AND claim_id = %s
                FOR UPDATE
            """,
            parameters=coordinate.key_parts,
        )


def _lock_expected_document_direct_m4_answers(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    images: tuple[_DirectStageImage, ...],
) -> None:
    """Acquire expected direct-M4 answer target absences at tier 14."""

    _document_direct_m4_target_ids(
        intent,
        images,
        relation_name="groundloop_m4_working_answer_state",
        key_columns=("epoch_id", "answer_version_id"),
        label="answer",
    )
    for image in images:
        coordinate = image.coordinate
        _reserve_structural_working_absence(
            cursor,
            relation_name=coordinate.relation_name,
            key_parts=coordinate.key_parts,
            query="""
                SELECT 1 FROM groundloop_m4_working_answer_state
                WHERE epoch_id = %s AND answer_version_id = %s
                FOR UPDATE
            """,
            parameters=coordinate.key_parts,
        )


def _lock_structural_intent_rows(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    logical_presence: _StructuralLogicalPresence,
    update_kind: str,
    *,
    image_already_locked: bool = False,
    document_direct_plan: _DocumentDirectPlan | None = None,
    document_direct_authority: object | None = None,
) -> None:
    """Acquire structural D25 tiers 11b--14 from a pre-gathered intent."""

    if image_already_locked:
        current = cursor.execute(
            """
            SELECT decision_policy_version, installed_epoch_id, installed_revision
            FROM groundloop_m5_matching_image_current
            WHERE singleton
            """
        ).fetchone()
        working = cursor.execute(
            """
            SELECT epoch_id
            FROM groundloop_m5_matching_image_working
            WHERE epoch_id = %s
            """,
            (intent.resulting_epoch_id,),
        ).fetchone()
        if current is None or (
            _text(current[0]) != intent.decision_policy_version
            or int(current[1]) != intent.before_epoch_id
            or int(current[2]) != intent.before_revision
            or working is not None
        ):
            raise EventConflictError("structural matching held image changed")
    else:
        _lock_structural_matching_image(
            cursor,
            epoch_id=intent.resulting_epoch_id,
            before_epoch_id=intent.before_epoch_id,
            before_revision=intent.before_revision,
            decision_policy_version=intent.decision_policy_version,
        )

    for observation_id in intent.observation_ids:
        row = cursor.execute(
            """
            SELECT observation_id, requirement_version_id, group_version_id,
                   requirement_ordinal, text_hash
            FROM groundloop_m5_matching_observation_current
            WHERE observation_id = %s
            FOR UPDATE
            """,
            (observation_id,),
        ).fetchone()
        if row is None or _text(row[0]) != observation_id:
            raise EventConflictError("matching observation key changed")
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_matching_observation_working",
            (intent.resulting_epoch_id, observation_id),
        )
        shadow = cursor.execute(
            """
            SELECT observation_id
            FROM groundloop_m5_matching_observation_working
            WHERE epoch_id = %s AND observation_id = %s
            FOR UPDATE
            """,
            (intent.resulting_epoch_id, observation_id),
        ).fetchone()
        if shadow is not None:
            raise EventConflictError("matching observation shadow appeared")
    for group_id, ordinal, text_hash, requirement_id in intent.edge_keys:
        row = cursor.execute(
            """
            SELECT group_version_id, requirement_ordinal
            FROM groundloop_m5_matching_edge_current
            WHERE requirement_version_id = %s AND text_hash = %s
            FOR UPDATE
            """,
            (requirement_id, text_hash),
        ).fetchone()
        if row is None or (_text(row[0]), int(row[1])) != (group_id, ordinal):
            raise EventConflictError("matching edge key changed")
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_matching_edge_working",
            (intent.resulting_epoch_id, requirement_id, text_hash),
        )
        shadow = cursor.execute(
            """
            SELECT requirement_version_id
            FROM groundloop_m5_matching_edge_working
            WHERE epoch_id = %s AND requirement_version_id = %s
              AND text_hash = %s
            FOR UPDATE
            """,
            (intent.resulting_epoch_id, requirement_id, text_hash),
        ).fetchone()
        if shadow is not None:
            raise EventConflictError("matching edge shadow appeared")
    for group_id, text_hash in intent.mask_keys:
        row = cursor.execute(
            """
            SELECT group_version_id
            FROM groundloop_m5_matching_hash_mask_current
            WHERE group_version_id = %s AND text_hash = %s
            FOR UPDATE
            """,
            (group_id, text_hash),
        ).fetchone()
        if row is None:
            raise EventConflictError("matching mask key changed")
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_matching_hash_mask_working",
            (intent.resulting_epoch_id, group_id, text_hash),
        )
        shadow = cursor.execute(
            """
            SELECT group_version_id
            FROM groundloop_m5_matching_hash_mask_working
            WHERE epoch_id = %s AND group_version_id = %s AND text_hash = %s
            FOR UPDATE
            """,
            (intent.resulting_epoch_id, group_id, text_hash),
        ).fetchone()
        if shadow is not None:
            raise EventConflictError("matching mask shadow appeared")
    for group_id in intent.hall_group_ids:
        current_hall = cursor.execute(
            """
            SELECT group_version_id
            FROM groundloop_m5_matching_hall_current
            WHERE group_version_id = %s
            FOR UPDATE
            """,
            (group_id,),
        ).fetchone()
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_matching_hall_working",
            (intent.resulting_epoch_id, group_id),
        )
        shadow = cursor.execute(
            """
            SELECT group_version_id
            FROM groundloop_m5_matching_hall_working
            WHERE epoch_id = %s AND group_version_id = %s
            FOR UPDATE
            """,
            (intent.resulting_epoch_id, group_id),
        ).fetchone()
        if current_hall is None and shadow is not None:
            raise EventConflictError("matching Hall shadow appeared")

    _lock_structural_logical_family(
        cursor,
        relation_name="groundloop_m5_published_requirement_state",
        object_ids=intent.requirement_state_ids,
        present_ids=logical_presence.requirement_state_ids,
        before_epoch_id=intent.before_epoch_id,
        query="""
            SELECT requirement_version_id
            FROM groundloop_m5_published_requirement_state
            WHERE requirement_version_id = %s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            FOR UPDATE
        """,
    )
    for requirement_id in intent.requirement_state_ids:
        _reserve_structural_working_absence(
            cursor,
            relation_name="groundloop_m5_working_requirement_state",
            key_parts=(intent.resulting_epoch_id, requirement_id),
            query="""
                SELECT 1 FROM groundloop_m5_working_requirement_state
                WHERE epoch_id = %s AND requirement_version_id = %s
                FOR UPDATE
            """,
            parameters=(intent.resulting_epoch_id, requirement_id),
        )
    _lock_structural_logical_family(
        cursor,
        relation_name="groundloop_m5_published_group_state",
        object_ids=intent.group_state_ids,
        present_ids=logical_presence.group_state_ids,
        before_epoch_id=intent.before_epoch_id,
        query="""
            SELECT group_version_id
            FROM groundloop_m5_published_group_state
            WHERE group_version_id = %s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            FOR UPDATE
        """,
    )
    for group_id in intent.group_state_ids:
        _reserve_structural_working_absence(
            cursor,
            relation_name="groundloop_m5_working_group_state",
            key_parts=(intent.resulting_epoch_id, group_id),
            query="""
                SELECT 1 FROM groundloop_m5_working_group_state
                WHERE epoch_id = %s AND group_version_id = %s
                FOR UPDATE
            """,
            parameters=(intent.resulting_epoch_id, group_id),
        )
    _lock_structural_logical_family(
        cursor,
        relation_name="groundloop_m5_published_group_certificate_binding",
        object_ids=intent.group_certificate_ids,
        present_ids=logical_presence.group_certificate_ids,
        before_epoch_id=intent.before_epoch_id,
        query="""
            SELECT group_version_id
            FROM groundloop_m5_published_group_certificate_binding
            WHERE group_version_id = %s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            FOR UPDATE
        """,
    )
    for group_id in intent.group_certificate_ids:
        _reserve_structural_working_absence(
            cursor,
            relation_name="groundloop_m5_working_group_certificate_binding",
            key_parts=(
                intent.resulting_epoch_id,
                group_id,
                intent.resulting_revision,
            ),
            query="""
                SELECT 1
                FROM groundloop_m5_working_group_certificate_binding
                WHERE epoch_id = %s AND group_version_id = %s
                  AND (valid_from_revision = %s OR valid_to_revision IS NULL)
                FOR UPDATE
            """,
            parameters=(
                intent.resulting_epoch_id,
                group_id,
                intent.resulting_revision,
            ),
        )
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_working_group_certificate_binding#open",
            (intent.resulting_epoch_id, group_id),
        )

    preview_plan = _MatchingWritePlan()
    if update_kind in {"replace_group", "retire_group"}:
        preview_plan = _structural_retirement_plan(cursor, intent)[1]
    elif update_kind in {"document_delete", "document_replace"}:
        preview_plan = _document_first_application_plan(
            cursor,
            intent,
            update_kind=update_kind,
            document_direct_plan=document_direct_plan,
        )[1]
    planned_group_artifacts = {
        artifact.certificate_digest: artifact
        for artifact in preview_plan.group_certificate_artifact_rows
    }
    planned_group_binding_digests = {
        binding.certificate_digest for binding in preview_plan.group_binding_rows
    }
    for digest in sorted(planned_group_artifacts):
        if _lock_or_reserve_group_certificate_artifact(
            cursor, planned_group_artifacts[digest]
        ):
            raise EventConflictError(
                "matching planned group certificate insertion already exists"
            )
    for digest in sorted(planned_group_binding_digests - set(planned_group_artifacts)):
        artifact = _stored_group_certificate_artifact(cursor, digest, lock=True)
        if artifact is None or artifact.certificate_digest != digest:
            raise EventConflictError("matching group certificate authority changed")

    direct_answer_before_images: tuple[Any, ...] = ()
    if document_direct_authority is not None:
        direct_claim_target_ids = _document_direct_m4_target_ids(
            intent,
            preview_plan.expected_direct_m4_claim_after_images,
            relation_name="groundloop_m4_working_claim_state",
            key_columns=("epoch_id", "claim_id"),
            label="claim",
        )
        direct_answer_target_ids = _document_direct_m4_target_ids(
            intent,
            preview_plan.expected_direct_m4_answer_after_images,
            relation_name="groundloop_m4_working_answer_state",
            key_columns=("epoch_id", "answer_version_id"),
            label="answer",
        )
        from groundloop.m5.runtime.postgres_withdrawal import (
            _D29DirectMatchingAuthority,
            _lock_d29_direct_answer_before_images,
            _lock_d29_direct_claim_before_images,
            _validate_d29_direct_matching_authority,
        )

        if (
            type(document_direct_authority) is not _D29DirectMatchingAuthority
            or document_direct_plan is None
            or preview_plan.document_direct_plan is None
        ):
            raise ValidationError("document matching requires exact D29 authority")
        direct_claim_before_images = document_direct_authority.claim_before_images
        direct_answer_before_images = document_direct_authority.answer_before_images
        if (
            tuple(image.claim_id for image in direct_claim_before_images)
            != direct_claim_target_ids
            or direct_claim_target_ids
            != tuple(
                claim.claim_id for claim in preview_plan.document_direct_plan.claims
            )
            or tuple(image.answer_version_id for image in direct_answer_before_images)
            != direct_answer_target_ids
            or direct_answer_target_ids
            != tuple(
                answer.before_state.answer_version_id
                for answer in preview_plan.document_direct_plan.answers
            )
        ):
            raise EventConflictError("document direct M4 authority changed")
        _validate_d29_direct_matching_authority(
            cursor,
            epoch_id=intent.resulting_epoch_id,
            source_id=intent.source_id,
            source_identity_hash=intent.source_identity_hash,
            authority=document_direct_authority,
        )
        _lock_d29_direct_claim_before_images(
            cursor,
            direct_claim_before_images,
            predecessor_epoch_id=intent.before_epoch_id,
        )
    elif document_direct_plan is not None:
        raise ValidationError("document direct plan lacks D29 authority")

    _lock_expected_document_direct_m4_claims(
        cursor,
        intent,
        preview_plan.expected_direct_m4_claim_after_images,
    )

    _lock_structural_logical_family(
        cursor,
        relation_name="groundloop_m5_published_claim_state",
        object_ids=intent.claim_state_ids,
        present_ids=logical_presence.claim_state_ids,
        before_epoch_id=intent.before_epoch_id,
        query="""
            SELECT claim_id
            FROM groundloop_m5_published_claim_state
            WHERE claim_id = %s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            FOR UPDATE
        """,
    )
    for claim_id in intent.claim_state_ids:
        _reserve_structural_working_absence(
            cursor,
            relation_name="groundloop_m5_working_claim_state",
            key_parts=(intent.resulting_epoch_id, claim_id),
            query="""
                SELECT 1 FROM groundloop_m5_working_claim_state
                WHERE epoch_id = %s AND claim_id = %s
                FOR UPDATE
            """,
            parameters=(intent.resulting_epoch_id, claim_id),
        )
    _lock_structural_logical_family(
        cursor,
        relation_name="groundloop_m5_published_claim_certificate_binding",
        object_ids=intent.claim_certificate_ids,
        present_ids=logical_presence.claim_certificate_ids,
        before_epoch_id=intent.before_epoch_id,
        query="""
            SELECT claim_id
            FROM groundloop_m5_published_claim_certificate_binding
            WHERE claim_id = %s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            FOR UPDATE
        """,
    )
    for claim_id in intent.claim_certificate_ids:
        _reserve_structural_working_absence(
            cursor,
            relation_name="groundloop_m5_working_claim_certificate_binding",
            key_parts=(
                intent.resulting_epoch_id,
                claim_id,
                intent.resulting_revision,
            ),
            query="""
                SELECT 1
                FROM groundloop_m5_working_claim_certificate_binding
                WHERE epoch_id = %s AND claim_id = %s
                  AND (valid_from_revision = %s OR valid_to_revision IS NULL)
                FOR UPDATE
            """,
            parameters=(
                intent.resulting_epoch_id,
                claim_id,
                intent.resulting_revision,
            ),
        )
        _reserve_matching_absence(
            cursor,
            "groundloop_m5_working_claim_certificate_binding#open",
            (intent.resulting_epoch_id, claim_id),
        )

    planned_artifacts = {
        artifact.certificate_digest: artifact
        for artifact in preview_plan.claim_certificate_artifact_rows
    }
    planned_binding_digests = {
        binding.certificate_digest for binding in preview_plan.claim_binding_rows
    }
    for digest in sorted(planned_artifacts):
        _reserve_structural_working_absence(
            cursor,
            relation_name="groundloop_m5_claim_certificate_artifact",
            key_parts=(digest,),
            query="""
                SELECT 1 FROM groundloop_m5_claim_certificate_artifact
                WHERE certificate_digest = %s
                FOR UPDATE
            """,
            parameters=(digest,),
        )
    for digest in sorted(planned_binding_digests - set(planned_artifacts)):
        artifact_row = cursor.execute(
            """
            SELECT certificate_digest
            FROM groundloop_m5_claim_certificate_artifact
            WHERE certificate_digest = %s
            FOR UPDATE
            """,
            (digest,),
        ).fetchone()
        if artifact_row is None or _sha256_text(artifact_row[0]) != digest:
            raise EventConflictError("matching claim certificate authority changed")

    if document_direct_authority is not None:
        _lock_d29_direct_answer_before_images(
            cursor,
            direct_answer_before_images,
            predecessor_epoch_id=intent.before_epoch_id,
        )
    _lock_expected_document_direct_m4_answers(
        cursor,
        intent,
        preview_plan.expected_direct_m4_answer_after_images,
    )

    _lock_structural_logical_family(
        cursor,
        relation_name="groundloop_m5_published_answer_state",
        object_ids=intent.answer_state_ids,
        present_ids=logical_presence.answer_state_ids,
        before_epoch_id=intent.before_epoch_id,
        query="""
            SELECT answer_version_id
            FROM groundloop_m5_published_answer_state
            WHERE answer_version_id = %s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            FOR UPDATE
        """,
    )
    for answer_id in intent.answer_state_ids:
        _reserve_structural_working_absence(
            cursor,
            relation_name="groundloop_m5_working_answer_state",
            key_parts=(intent.resulting_epoch_id, answer_id),
            query="""
                SELECT 1 FROM groundloop_m5_working_answer_state
                WHERE epoch_id = %s AND answer_version_id = %s
                FOR UPDATE
            """,
            parameters=(intent.resulting_epoch_id, answer_id),
        )


def _require_structural_absence_plan(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    plan: _MatchingWritePlan,
) -> None:
    """Prove prepare still owns every earlier insert/absence coordinate."""

    coordinates: list[tuple[str, tuple[object, ...]]] = [
        (
            "groundloop_m5_matching_image_working",
            (intent.resulting_epoch_id,),
        )
    ]
    for currency_row in plan.observation_currency_rows:
        currency_key = (
            currency_row.epoch_id,
            currency_row.subject_kind,
            currency_row.subject_id,
            currency_row.chunk_version_id,
            currency_row.task_type,
        )
        coordinates.extend(
            (
                (
                    "groundloop_m5_working_currency_history",
                    (*currency_key, currency_row.valid_from_revision),
                ),
                (
                    "groundloop_m5_working_currency_history#open",
                    currency_key,
                ),
            )
        )
    coordinates.extend(
        (
            "groundloop_m5_matching_observation_working",
            (intent.resulting_epoch_id, observation_id),
        )
        for observation_id in intent.observation_ids
    )
    coordinates.extend(
        (
            "groundloop_m5_matching_edge_working",
            (intent.resulting_epoch_id, requirement_id, text_hash),
        )
        for _, _, text_hash, requirement_id in intent.edge_keys
    )
    coordinates.extend(
        (
            "groundloop_m5_matching_hash_mask_working",
            (intent.resulting_epoch_id, group_id, text_hash),
        )
        for group_id, text_hash in intent.mask_keys
    )
    coordinates.extend(
        (
            "groundloop_m5_matching_hall_working",
            (intent.resulting_epoch_id, group_id),
        )
        for group_id in intent.hall_group_ids
    )
    for relation_name, object_ids in (
        (
            "groundloop_m5_working_requirement_state",
            intent.requirement_state_ids,
        ),
        ("groundloop_m5_working_group_state", intent.group_state_ids),
        ("groundloop_m5_working_claim_state", intent.claim_state_ids),
        ("groundloop_m5_working_answer_state", intent.answer_state_ids),
    ):
        coordinates.extend(
            (relation_name, (intent.resulting_epoch_id, object_id))
            for object_id in object_ids
        )
    coordinates.extend(
        (image.coordinate.relation_name, image.coordinate.key_parts)
        for image in (
            *plan.expected_direct_m4_claim_after_images,
            *plan.expected_direct_m4_answer_after_images,
        )
    )
    for group_id in intent.group_certificate_ids:
        coordinates.extend(
            (
                (
                    "groundloop_m5_working_group_certificate_binding",
                    (
                        intent.resulting_epoch_id,
                        group_id,
                        intent.resulting_revision,
                    ),
                ),
                (
                    "groundloop_m5_working_group_certificate_binding#open",
                    (intent.resulting_epoch_id, group_id),
                ),
            )
        )
    for claim_id in intent.claim_certificate_ids:
        coordinates.extend(
            (
                (
                    "groundloop_m5_working_claim_certificate_binding",
                    (
                        intent.resulting_epoch_id,
                        claim_id,
                        intent.resulting_revision,
                    ),
                ),
                (
                    "groundloop_m5_working_claim_certificate_binding#open",
                    (intent.resulting_epoch_id, claim_id),
                ),
            )
        )
    coordinates.extend(
        (
            "groundloop_m5_claim_certificate_artifact",
            (artifact.certificate_digest,),
        )
        for artifact in plan.claim_certificate_artifact_rows
    )
    for artifact in plan.group_certificate_artifact_rows:
        coordinates.append(
            (
                "groundloop_m5_group_certificate_artifact",
                (artifact.certificate_digest,),
            )
        )
        coordinates.extend(
            (
                "groundloop_m5_group_certificate_artifact_row",
                (artifact.certificate_digest, row.requirement_ordinal),
            )
            for row in artifact.rows
        )

    affected: dict[str, object] = {
        "requirement_state_ids": intent.requirement_state_ids,
        "group_state_ids": intent.group_state_ids,
        "group_certificate_ids": intent.group_certificate_ids,
        "claim_state_ids": intent.claim_state_ids,
        "claim_certificate_ids": intent.claim_certificate_ids,
        "answer_state_ids": intent.answer_state_ids,
    }
    presence = _structural_logical_presence(
        cursor,
        before_epoch_id=intent.before_epoch_id,
        affected=affected,
    )
    for relation_name, object_ids, present_ids in (
        (
            "groundloop_m5_published_requirement_state",
            intent.requirement_state_ids,
            presence.requirement_state_ids,
        ),
        (
            "groundloop_m5_published_group_state",
            intent.group_state_ids,
            presence.group_state_ids,
        ),
        (
            "groundloop_m5_published_group_certificate_binding",
            intent.group_certificate_ids,
            presence.group_certificate_ids,
        ),
        (
            "groundloop_m5_published_claim_state",
            intent.claim_state_ids,
            presence.claim_state_ids,
        ),
        (
            "groundloop_m5_published_claim_certificate_binding",
            intent.claim_certificate_ids,
            presence.claim_certificate_ids,
        ),
        (
            "groundloop_m5_published_answer_state",
            intent.answer_state_ids,
            presence.answer_state_ids,
        ),
    ):
        present_set = set(present_ids)
        coordinates.extend(
            (relation_name, (object_id, intent.before_epoch_id))
            for object_id in object_ids
            if object_id not in present_set
        )
    if len(set(coordinates)) != len(coordinates):
        raise ValidationError("structural absence plan repeats a coordinate")
    for image in (
        *plan.expected_direct_m4_claim_after_images,
        *plan.expected_direct_m4_answer_after_images,
    ):
        if _direct_stage_row(cursor, image.coordinate, lock=False) is not None:
            raise EventConflictError("document direct M4 target appeared")
    for relation_name, key_parts in coordinates:
        _require_matching_absence_reservation(cursor, relation_name, key_parts)


def _candidate_retained_replay_intent(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_runtime_revision: int,
    resulting_revision: int,
    source_kind: M5PersistedMatchingSourceKind,
    source_id: str,
    expected_source_identity_hash: str | None,
) -> M5PersistedMatchingTransitionIntent | None:
    """Decode an unlocked replay candidate; authority is acquired by apply."""

    row = cursor.execute(
        """
        SELECT source_identity_hash, before_epoch_id, before_revision,
               resulting_revision, patch_digest
        FROM groundloop_m5_matching_work_contribution
        WHERE epoch_id = %s AND source_kind = %s AND source_id = %s
        """,
        (epoch_id, source_kind.value, source_id),
    ).fetchone()
    if row is None:
        return None
    source_identity_hash = _sha256_text(row[0])
    before_revision = int(row[2])
    expected_source_revision = (
        1
        if source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN
        else before_revision
    )
    if (
        int(row[3]) != resulting_revision
        or expected_runtime_revision != expected_source_revision
        or (
            expected_source_identity_hash is not None
            and expected_source_identity_hash != source_identity_hash
        )
    ):
        raise EventConflictError("matching replay locator differs")
    artifact_row = _matching_artifact_row(cursor, _sha256_text(row[4]), lock=False)
    if artifact_row is None:
        raise EventConflictError("matching replay lacks its retained patch artifact")
    retained = _decode_retained_matching_artifact(cursor, artifact_row)
    intent = retained.intent()
    if (
        intent.source_kind is not source_kind
        or intent.source_id != source_id
        or intent.source_identity_hash != source_identity_hash
        or intent.before_epoch_id != int(row[1])
        or intent.before_revision != before_revision
        or intent.resulting_epoch_id != epoch_id
        or intent.resulting_revision != resulting_revision
    ):
        raise EventConflictError("matching replay retained locator differs")
    return intent


def _derive_matching_transition_intent_core(
    cursor: Cursor[Any],
    epoch_id: int,
    expected_runtime_revision: int,
    resulting_revision: int,
    source_kind: M5PersistedMatchingSourceKind,
    source_id: str,
    expected_source_identity_hash: str | None = None,
    *,
    document_direct_authority: object | None,
) -> M5PersistedMatchingTransitionIntent:
    """Derive one source-present intent while retaining every frozen lock."""

    _require_positive_int("epoch_id", epoch_id)
    _require_positive_int("expected_runtime_revision", expected_runtime_revision)
    _require_positive_int("resulting_revision", resulting_revision)
    if not isinstance(source_kind, M5PersistedMatchingSourceKind):
        raise ValidationError("source_kind must be a persisted-matching source enum")
    _require_text("source_id", source_id)
    if expected_source_identity_hash is not None:
        _require_sha256("expected_source_identity_hash", expected_source_identity_hash)
    if (
        document_direct_authority is not None
        and source_kind is not M5PersistedMatchingSourceKind.STRUCTURAL_OPEN
    ):
        raise ValidationError("direct document authority requires structural_open")
    require_persisted_matching_bundle(cursor)
    transition_mode = cursor.execute(
        "SELECT current_setting('groundloop.m5_matching_mode', true)"
    ).fetchone()
    if transition_mode is None or transition_mode[0] != "transition":
        replay_intent = _candidate_retained_replay_intent(
            cursor,
            epoch_id=epoch_id,
            expected_runtime_revision=expected_runtime_revision,
            resulting_revision=resulting_revision,
            source_kind=source_kind,
            source_id=source_id,
            expected_source_identity_hash=expected_source_identity_hash,
        )
        if replay_intent is not None:
            return replay_intent
        raise ValidationError(
            "matching first application lacks transition authorization"
        )
    if source_kind is M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION:
        _require_checked_prefix(
            cursor,
            epoch_id=epoch_id,
            expected_revision=expected_runtime_revision,
            reauthorize=False,
        )
        requirement_source = _requirement_completion_source(
            cursor,
            epoch_id=epoch_id,
            expected_runtime_revision=expected_runtime_revision,
            resulting_revision=resulting_revision,
            proposed_attempt_id=source_id,
            expected_source_identity_hash=expected_source_identity_hash,
        )
        shapes = _group_shapes_for_ids(cursor, (requirement_source.group_version_id,))
        if len(shapes) != 1:
            raise EventConflictError("requirement matching group shape changed")
        preliminary_groups = _preliminary_complete_group_ids(cursor, requirement_source)
        currency_projection = _requirement_currency_projection(
            cursor, requirement_source
        )
        _capture_requirement_currency_before_image(
            cursor,
            requirement_source,
            currency_projection,
            lock=True,
        )
        _lock_requirement_matching_image(cursor, requirement_source)
        preview = _requirement_intent_preview(
            cursor,
            source=requirement_source,
            projection=currency_projection,
            shape=shapes[0],
            old_complete_groups=preliminary_groups,
        )
        requirement_values: dict[str, Any] = {
            "source_kind": source_kind,
            "source_id": source_id,
            "source_identity_hash": (requirement_source.attempt_result_artifact_hash),
            "before_epoch_id": epoch_id,
            "before_revision": expected_runtime_revision,
            "resulting_epoch_id": epoch_id,
            "resulting_revision": resulting_revision,
            "decision_policy_version": (requirement_source.decision_policy_version),
            **preview.affected,
        }
        requirement_digest_values = {
            **requirement_values,
            "group_shapes": tuple(
                shape.digest_row for shape in requirement_values["group_shapes"]
            ),
        }
        intent = M5PersistedMatchingTransitionIntent(
            **requirement_values,
            intent_digest=digests.persisted_matching_transition_intent_digest(
                **requirement_digest_values
            ),
        )
        _lock_requirement_intent_rows(
            cursor,
            source=requirement_source,
            intent=intent,
            preview=preview,
        )
        return intent
    if source_kind is not M5PersistedMatchingSourceKind.STRUCTURAL_OPEN:
        _fail_nonempty(source_kind.value)
    predecessor_row = cursor.execute(
        """
        SELECT typed_update.previous_published_epoch_id
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        WHERE epoch.epoch_id = %s
          AND epoch.event_id = %s
          AND runtime.structural_event_id = %s
        """,
        (epoch_id, source_id, source_id),
    ).fetchone()
    if predecessor_row is None:
        raise InvalidEventError("structural matching source authority is incomplete")
    predecessor_epoch_id = int(predecessor_row[0])
    _require_checked_prefix(
        cursor,
        epoch_id=epoch_id,
        expected_revision=expected_runtime_revision,
        reauthorize=False,
    )
    if expected_runtime_revision != 1 or resulting_revision != 1:
        raise EventConflictError("structural matching intent must use runtime point 1")

    source = cursor.execute(
        """
        SELECT epoch.payload_hash, typed_update.previous_published_epoch_id,
               predecessor.revision, typed_update.decision_policy_version,
               typed_update.update_kind, runtime.structural_event_id,
               runtime.revision, current_image.installed_epoch_id,
               current_image.installed_revision
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        JOIN groundloop_m5_candidate_policy AS typed_policy
          ON typed_policy.candidate_policy_id = runtime.candidate_policy_id
        LEFT JOIN groundloop_m4_update AS direct_update USING (epoch_id)
        LEFT JOIN groundloop_candidate_policy AS direct_policy
          ON direct_policy.candidate_policy_id = direct_update.candidate_policy_id
        JOIN groundloop_epoch AS predecessor
          ON predecessor.epoch_id = typed_update.previous_published_epoch_id
        JOIN groundloop_m4_publication_head AS m4_head
          ON m4_head.singleton
         AND m4_head.epoch_id = typed_update.previous_published_epoch_id
        JOIN groundloop_m5_publication_head AS m5_head
          ON m5_head.singleton
         AND m5_head.epoch_id = typed_update.previous_published_epoch_id
        JOIN groundloop_m5_matching_image_current AS current_image
          ON current_image.singleton
         AND current_image.installed_epoch_id = m5_head.epoch_id
         AND current_image.installed_revision = m5_head.sealed_revision
        JOIN groundloop_decision_policy AS current_policy
          ON current_policy.policy_version = current_image.decision_policy_version
        WHERE epoch.epoch_id = %s
          AND epoch.event_id = %s
          AND runtime.structural_event_id = %s
          AND typed_update.previous_published_epoch_id = %s
          AND runtime.expected_previous_published_epoch_id =
              typed_update.previous_published_epoch_id
          AND runtime.candidate_policy_manifest_hash =
              typed_policy.candidate_policy_manifest_hash
          AND typed_policy.decision_policy_version =
              typed_update.decision_policy_version
          AND (direct_update.epoch_id IS NULL OR (
            direct_update.previous_published_epoch_id =
                typed_update.previous_published_epoch_id
            AND direct_update.candidate_policy_id = runtime.candidate_policy_id
            AND direct_policy.decision_policy_version =
                typed_update.decision_policy_version))
          AND current_image.decision_policy_version =
              typed_update.decision_policy_version
          AND current_policy.valid_from_epoch <= current_image.installed_epoch_id
          AND (current_policy.valid_to_epoch IS NULL OR
               current_image.installed_epoch_id < current_policy.valid_to_epoch)
          AND 1 = (
              SELECT count(*)
              FROM groundloop_decision_policy AS strict_policy
              WHERE strict_policy.valid_from_epoch <=
                    current_image.installed_epoch_id
                AND (strict_policy.valid_to_epoch IS NULL OR
                     current_image.installed_epoch_id <
                     strict_policy.valid_to_epoch)
          )
          AND predecessor.revision = m5_head.sealed_revision
          AND predecessor.structural_status = 'committed'
          AND predecessor.semantic_status = 'sealed'
          AND predecessor.evaluation_state = 'complete'
          AND predecessor.sealed_at IS NOT NULL
        """,
        (epoch_id, source_id, source_id, predecessor_epoch_id),
    ).fetchone()
    if source is None:
        raise InvalidEventError("structural matching source authority is incomplete")
    source_identity_hash = _sha256_text(source[0])
    if expected_source_identity_hash is not None and (
        expected_source_identity_hash != source_identity_hash
    ):
        raise EventConflictError("structural matching source hash changed")
    if (
        int(source[1]) != predecessor_epoch_id
        or int(source[1]) != int(source[7])
        or int(source[2]) != int(source[8])
        or int(source[6]) != 1
    ):
        raise EventConflictError("structural matching predecessor point changed")
    update_kind = _text(source[4])
    if document_direct_authority is not None and update_kind not in {
        "document_delete",
        "document_replace",
    }:
        raise ValidationError("direct document authority requires delete or replace")
    affected_values: dict[str, object]
    structural_image_already_locked = False
    document_direct_plan: _DocumentDirectPlan | None = None
    if update_kind == "document_insert":
        affected_values = {
            "group_shapes": (),
            "observation_ids": (),
            "edge_keys": (),
            "mask_keys": (),
            "hall_group_ids": (),
            "requirement_state_ids": (),
            "group_state_ids": (),
            "claim_state_ids": (),
            "answer_state_ids": (),
            "group_certificate_ids": (),
            "claim_certificate_ids": (),
        }
    elif update_kind in {"register_group", "replace_group", "retire_group"}:
        affected_values = _structural_group_projection(
            cursor,
            epoch_id=epoch_id,
            source_id=source_id,
            update_kind=update_kind,
        )
    elif update_kind in {"document_delete", "document_replace"}:
        document_preview = _document_withdrawal_preview(
            cursor,
            epoch_id=epoch_id,
            before_epoch_id=int(source[1]),
            before_revision=int(source[2]),
            resulting_revision=resulting_revision,
            decision_policy_version=_text(source[3]),
            source_id=source_id,
            update_kind=update_kind,
            reserve_currency_targets=True,
            lock_image_before_discovery=True,
        )
        direct_claim_ids: tuple[str, ...] = ()
        if document_direct_authority is not None:
            document_direct_plan = _document_direct_plan_from_d29_authority(
                cursor,
                authority=document_direct_authority,
                epoch_id=epoch_id,
                before_epoch_id=int(source[1]),
                source_id=source_id,
                source_identity_hash=source_identity_hash,
                update_kind=update_kind,
                decision_policy_version=_text(source[3]),
            )
            direct_claim_ids = tuple(
                claim.claim_id for claim in document_direct_plan.claims
            )
        affected_values = _document_affected_projection(
            cursor,
            before_epoch_id=int(source[1]),
            preview=document_preview,
            direct_claim_ids=direct_claim_ids,
        )
        structural_image_already_locked = True
    else:
        _fail_nonempty(f"unsupported structural source {update_kind}")
    logical_presence = _structural_logical_presence(
        cursor,
        before_epoch_id=int(source[1]),
        affected=affected_values,
    )

    values: dict[str, Any] = {
        "source_kind": source_kind,
        "source_id": source_id,
        "source_identity_hash": source_identity_hash,
        "before_epoch_id": int(source[1]),
        "before_revision": int(source[2]),
        "resulting_epoch_id": epoch_id,
        "resulting_revision": resulting_revision,
        "decision_policy_version": _text(source[3]),
        **affected_values,
    }
    digest_values = {
        **values,
        "group_shapes": tuple(shape.digest_row for shape in values["group_shapes"]),
    }
    intent_digest = digests.persisted_matching_transition_intent_digest(**digest_values)
    intent = M5PersistedMatchingTransitionIntent(**values, intent_digest=intent_digest)
    _lock_structural_intent_rows(
        cursor,
        intent,
        logical_presence,
        update_kind,
        image_already_locked=structural_image_already_locked,
        document_direct_plan=document_direct_plan,
        document_direct_authority=document_direct_authority,
    )
    return intent


def derive_matching_transition_intent(
    cursor: Cursor[Any],
    epoch_id: int,
    expected_runtime_revision: int,
    resulting_revision: int,
    source_kind: M5PersistedMatchingSourceKind,
    source_id: str,
    expected_source_identity_hash: str | None = None,
) -> M5PersistedMatchingTransitionIntent:
    """Retained public derivation with no hidden document authority channel."""

    return _derive_matching_transition_intent_core(
        cursor,
        epoch_id,
        expected_runtime_revision,
        resulting_revision,
        source_kind,
        source_id,
        expected_source_identity_hash,
        document_direct_authority=None,
    )


def _derive_matching_transition_intent_with_document_authority(
    cursor: Cursor[Any],
    epoch_id: int,
    expected_runtime_revision: int,
    resulting_revision: int,
    source_kind: M5PersistedMatchingSourceKind,
    source_id: str,
    document_direct_authority: object,
    expected_source_identity_hash: str | None = None,
) -> M5PersistedMatchingTransitionIntent:
    """Private structural derivation bound to D29's consumed direct authority."""

    return _derive_matching_transition_intent_core(
        cursor,
        epoch_id,
        expected_runtime_revision,
        resulting_revision,
        source_kind,
        source_id,
        expected_source_identity_hash,
        document_direct_authority=document_direct_authority,
    )


def _overlay_work_from_values(values: Sequence[int]) -> M5OverlayWork:
    if len(values) != len(MATCHING_WORK_COUNTER_NAMES):
        raise ValidationError("persisted matching work has the wrong cardinality")
    checked = tuple(int(value) for value in values)
    if any(value < 0 for value in checked):
        raise ValidationError("persisted matching work cannot be negative")
    return M5OverlayWork(
        matching=MatchingWorkCounters(*checked[:31]),
        requirement_state_only_changes=checked[31],
        group_state_only_changes=checked[32],
        claim_state_only_changes=checked[33],
        group_certificate_only_changes=checked[34],
        claim_certificate_only_changes=checked[35],
        public_status_deltas=checked[36],
    )


def _matching_work_read_context(
    cursor: Cursor[Any], epoch_id: int, *, reauthorize: bool = True
) -> int:
    """Validate a retained accumulator's nonterminal, failed, or sealed envelope."""

    _, scoped_revision = _require_checked_prefix(
        cursor, epoch_id=epoch_id, reauthorize=reauthorize
    )
    current = cursor.execute(
        """
        SELECT decision_policy_version, installed_epoch_id, installed_revision
        FROM groundloop_m5_matching_image_current
        WHERE singleton
        FOR UPDATE
        """
    ).fetchone()
    if current is None:
        raise ValidationError("persisted matching current image is missing")
    current_policy = _text(current[0])
    current_epoch = int(current[1])
    current_revision = int(current[2])

    working = cursor.execute(
        """
        SELECT epoch_id, base_epoch_id, base_revision,
               decision_policy_version, updated_revision
        FROM groundloop_m5_matching_image_working
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if working is None:
        raise ValidationError("persisted matching working image is missing")
    working_epoch = int(working[0])
    working_base_epoch = int(working[1])
    working_base_revision = int(working[2])
    working_policy = _text(working[3])
    working_updated_revision = int(working[4])

    context = cursor.execute(
        """
        SELECT epoch.revision, epoch.structural_status,
               epoch.semantic_status, epoch.evaluation_state,
               epoch.publication_mode, epoch.sealed_at IS NOT NULL,
               runtime.revision, runtime.runtime_state,
               runtime.terminal_at IS NOT NULL,
               runtime.expected_previous_published_epoch_id,
               typed_update.previous_published_epoch_id,
               typed_update.decision_policy_version,
               typed_policy.candidate_policy_manifest_hash,
               runtime.candidate_policy_manifest_hash,
               typed_policy.decision_policy_version,
               predecessor.revision,
               predecessor.structural_status,
               predecessor.semantic_status,
               predecessor.evaluation_state,
               predecessor.publication_mode,
               predecessor.sealed_at IS NOT NULL,
               m4_head.epoch_id, m5_head.epoch_id,
               m5_head.sealed_revision, live_head.revision,
               live_head.structural_status,
               live_head.semantic_status,
               live_head.evaluation_state,
               live_head.publication_mode,
               live_head.sealed_at IS NOT NULL,
               (SELECT count(*)
                  FROM groundloop_decision_policy AS base_policy
                 WHERE base_policy.valid_from_epoch <= %s
                   AND (base_policy.valid_to_epoch IS NULL
                        OR %s < base_policy.valid_to_epoch)),
               EXISTS (
                 SELECT 1 FROM groundloop_decision_policy AS base_selected
                  WHERE base_selected.policy_version = %s
                    AND base_selected.valid_from_epoch <= %s
                    AND (base_selected.valid_to_epoch IS NULL
                         OR %s < base_selected.valid_to_epoch)
               ),
               (SELECT count(*)
                  FROM groundloop_decision_policy AS current_policy
                 WHERE current_policy.valid_from_epoch <= %s
                   AND (current_policy.valid_to_epoch IS NULL
                        OR %s < current_policy.valid_to_epoch)),
               EXISTS (
                 SELECT 1 FROM groundloop_decision_policy AS current_selected
                  WHERE current_selected.policy_version = %s
                    AND current_selected.valid_from_epoch <= %s
                    AND (current_selected.valid_to_epoch IS NULL
                         OR %s < current_selected.valid_to_epoch)
               ),
               direct_update.epoch_id,
               direct_update.previous_published_epoch_id,
               direct_update.candidate_policy_id,
               runtime.candidate_policy_id,
               direct_policy.decision_policy_version
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        JOIN groundloop_m5_candidate_policy AS typed_policy
          ON typed_policy.candidate_policy_id = runtime.candidate_policy_id
        JOIN groundloop_m4_publication_head AS m4_head ON m4_head.singleton
        JOIN groundloop_m5_publication_head AS m5_head ON m5_head.singleton
        JOIN groundloop_epoch AS live_head ON live_head.epoch_id = m5_head.epoch_id
        JOIN groundloop_epoch AS predecessor
          ON predecessor.epoch_id = typed_update.previous_published_epoch_id
        LEFT JOIN groundloop_m4_update AS direct_update
          ON direct_update.epoch_id = epoch.epoch_id
        LEFT JOIN groundloop_candidate_policy AS direct_policy
          ON direct_policy.candidate_policy_id = direct_update.candidate_policy_id
        WHERE epoch.epoch_id = %s
        """,
        (
            working_base_epoch,
            working_base_epoch,
            working_policy,
            working_base_epoch,
            working_base_epoch,
            current_epoch,
            current_epoch,
            current_policy,
            current_epoch,
            current_epoch,
            epoch_id,
        ),
    ).fetchone()
    if context is None:
        raise ValidationError("persisted matching work envelope is incomplete")
    runtime_state = _text(context[7])
    direct_present = context[34] is not None
    if (
        working_epoch != epoch_id
        or int(context[0]) != int(context[6])
        or int(context[6]) != scoped_revision
        or int(context[9]) != working_base_epoch
        or int(context[10]) != working_base_epoch
        or int(context[15]) != working_base_revision
        or _text(context[11]) != working_policy
        or _text(context[14]) != working_policy
        or _sha256_text(context[12]) != _sha256_text(context[13])
        or _text(context[16]) != "committed"
        or _text(context[17]) != "sealed"
        or _text(context[18]) != "complete"
        or _text(context[19]) != "strict"
        or not bool(context[20])
        or int(context[21]) != current_epoch
        or int(context[22]) != current_epoch
        or int(context[23]) != current_revision
        or int(context[24]) != current_revision
        or _text(context[25]) != "committed"
        or _text(context[26]) != "sealed"
        or _text(context[27]) != "complete"
        or _text(context[28]) != "strict"
        or not bool(context[29])
        or int(context[30]) != 1
        or not bool(context[31])
        or int(context[32]) != 1
        or not bool(context[33])
        or working_updated_revision > int(context[6])
        or (
            direct_present
            and (
                int(context[35]) != working_base_epoch
                or _text(context[36]) != _text(context[37])
                or _text(context[38]) != working_policy
            )
        )
    ):
        raise ValidationError("persisted matching work envelope is inconsistent")

    if runtime_state == "sealed":
        valid_terminal = (
            bool(context[8])
            and _text(context[1]) == "committed"
            and _text(context[2]) == "sealed"
            and _text(context[3]) == "complete"
            and _text(context[4]) == "strict"
            and bool(context[5])
            and current_epoch >= epoch_id
        )
    elif runtime_state == "failed":
        valid_terminal = (
            bool(context[8])
            and _text(context[1]) == "failed"
            and _text(context[2]) == "failed"
            and _text(context[3]) == "failed"
            and _text(context[4]) == "provisional"
            and not bool(context[5])
            and (current_epoch == working_base_epoch or current_epoch > epoch_id)
        )
    else:
        pending_shape = (
            runtime_state in {"structural_committed", "semantic_pending"}
            and _text(context[1]) == "committed"
            and _text(context[2]) == "pending"
            and _text(context[3]) == "pending"
        )
        complete_shape = (
            runtime_state == "semantic_complete"
            and _text(context[1]) == "committed"
            and _text(context[2]) == "complete"
            and _text(context[3]) == "complete"
        )
        valid_terminal = (
            (pending_shape or complete_shape)
            and not bool(context[8])
            and _text(context[4]) == "provisional"
            and not bool(context[5])
            and current_epoch == working_base_epoch
            and current_revision == working_base_revision
            and current_policy == working_policy
        )
    if not valid_terminal:
        raise ValidationError("persisted matching work terminal envelope is invalid")
    return working_updated_revision


def _read_matching_work_accumulator(
    cursor: Cursor[Any], epoch_id: int
) -> tuple[M5OverlayWork, int]:
    _require_positive_int("epoch_id", epoch_id)
    require_persisted_matching_bundle(cursor)
    working_updated_revision = _matching_work_read_context(cursor, epoch_id)
    return _read_locked_matching_work_accumulator(
        cursor, epoch_id, expected_revision=working_updated_revision
    )


def _read_locked_matching_work_accumulator(
    cursor: Cursor[Any], epoch_id: int, *, expected_revision: int
) -> tuple[M5OverlayWork, int]:
    """Read tier 15k after the caller has already locked/validated tier 11b."""

    columns = sql.SQL(", ").join(
        sql.Identifier(name) for name in MATCHING_WORK_COUNTER_NAMES
    )
    row = cursor.execute(
        sql.SQL(
            "SELECT {}, matching_work_digest, updated_revision FROM "
            "groundloop_m5_matching_work_accumulator WHERE epoch_id = %s FOR SHARE"
        ).format(columns),
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise InvalidEventError("typed epoch has no persisted matching work")
    work = _overlay_work_from_values(tuple(int(value) for value in row[:-2]))
    digest = _sha256_text(row[-2])
    updated_revision = int(row[-1])
    if digest != digests.matching_work_digest(m5_overlay_work_values(work)):
        raise ValidationError("persisted matching accumulator digest is inconsistent")
    if updated_revision != expected_revision:
        raise ValidationError(
            "persisted matching accumulator revision differs from its working image"
        )
    return work, updated_revision


def current_matching_work(cursor: Cursor[Any], epoch_id: int) -> M5OverlayWork:
    """Read checked retained work under the exact epoch and image lock scope."""

    return _read_matching_work_accumulator(cursor, epoch_id)[0]


def _empty_patch_artifact(
    intent: M5PersistedMatchingTransitionIntent,
) -> M5PersistedMatchingPatchArtifact:
    logical_output = digests.logical_output_preimage(())
    if (
        logical_output != _EMPTY_LOGICAL_OUTPUT_BYTES
        or len(logical_output) != _EMPTY_OUTPUT_BYTES
        or hashlib.sha256(logical_output).hexdigest() != _EMPTY_LOGICAL_OUTPUT_DIGEST
    ):
        raise ValidationError("empty logical-output golden image changed")
    logical_preimage = digests.logical_overlay_patch_preimage(
        (), (), _EMPTY_LOGICAL_OUTPUT_DIGEST, _EMPTY_OUTPUT_BYTES
    )
    logical_digest = digests.logical_overlay_patch_digest(
        (), (), _EMPTY_LOGICAL_OUTPUT_DIGEST, _EMPTY_OUTPUT_BYTES
    )
    logical_patch = M5PersistedLogicalOverlayPatch(
        resulting_revision=intent.resulting_revision,
        changes=(),
        binding_rows=(),
        output_records=(),
        logical_output_preimage=logical_output,
        logical_output_digest=_EMPTY_LOGICAL_OUTPUT_DIGEST,
        output_bytes=_EMPTY_OUTPUT_BYTES,
        patch_preimage=logical_preimage,
        patch_digest=logical_digest,
    )
    work = M5OverlayWork(matching=MatchingWorkCounters(output_bytes=71))
    work_digest = digests.matching_work_digest(m5_overlay_work_values(work))
    shapes: tuple[tuple[str, int, tuple[tuple[int, str], ...]], ...] = ()
    shape_digest = digests.matching_group_shape_set_digest(shapes)
    patch_values: dict[str, Any] = {
        "source_kind": intent.source_kind,
        "source_id": intent.source_id,
        "source_identity_hash": intent.source_identity_hash,
        "before_epoch_id": intent.before_epoch_id,
        "before_revision": intent.before_revision,
        "resulting_epoch_id": intent.resulting_epoch_id,
        "resulting_revision": intent.resulting_revision,
        "decision_policy_version": intent.decision_policy_version,
        "group_shape_set_digest": shape_digest,
        "observation_change_digests": (),
        "edge_change_digests": (),
        "mask_change_digests": (),
        "hall_change_digests": (),
        "logical_overlay_patch_digest_value": logical_digest,
        "matching_work_digest_value": work_digest,
    }
    patch_digest = digests.persisted_matching_patch_digest(**patch_values)
    patch = M5PersistedMatchingPatch(
        source_kind=intent.source_kind,
        source_id=intent.source_id,
        source_identity_hash=intent.source_identity_hash,
        before_epoch_id=intent.before_epoch_id,
        before_revision=intent.before_revision,
        resulting_epoch_id=intent.resulting_epoch_id,
        resulting_revision=intent.resulting_revision,
        decision_policy_version=intent.decision_policy_version,
        group_shape_set_digest=shape_digest,
        observation_change_digests=(),
        edge_change_digests=(),
        mask_change_digests=(),
        hall_change_digests=(),
        logical_overlay_patch_digest=logical_digest,
        matching_work_digest=work_digest,
        patch_digest=patch_digest,
    )
    return M5PersistedMatchingPatchArtifact(
        patch=patch,
        group_shapes=(),
        group_shape_set_preimage=digests.matching_group_shape_set_preimage(shapes),
        observation_changes=(),
        observation_change_preimages=(),
        edge_changes=(),
        edge_change_preimages=(),
        mask_changes=(),
        mask_change_preimages=(),
        hall_changes=(),
        hall_change_preimages=(),
        logical_patch=logical_patch,
        work=work,
        patch_preimage=digests.persisted_matching_patch_preimage(**patch_values),
    )


def _build_patch_artifact(
    intent: M5PersistedMatchingTransitionIntent,
    *,
    observation_changes: tuple[M5MatchingObservationChange, ...] = (),
    edge_changes: tuple[M5MatchingEdgeChange, ...] = (),
    mask_changes: tuple[M5MatchingMaskChange, ...] = (),
    hall_changes: tuple[M5MatchingHallChange, ...] = (),
    logical_changes: tuple[M5PersistedLogicalChange, ...] = (),
    binding_rows: tuple[M5PersistedCertificateBindingRow, ...] = (),
    output_records: tuple[tuple[str, str, object], ...] = (),
    work: M5OverlayWork,
) -> M5PersistedMatchingPatchArtifact:
    """Assemble the unchanged byte-total D25 artifact from store values."""

    logical_output = digests.logical_output_preimage(output_records)
    logical_output_digest = hashlib.sha256(logical_output).hexdigest()
    logical_rows = tuple(
        (row.kind, row.object_id, row.before_hash, row.after_hash)
        for row in logical_changes
    )
    binding_digests = tuple(row.binding_row_digest for row in binding_rows)
    logical_preimage = digests.logical_overlay_patch_preimage(
        logical_rows,
        binding_digests,
        logical_output_digest,
        len(logical_output),
    )
    logical_digest = digests.logical_overlay_patch_digest(
        logical_rows,
        binding_digests,
        logical_output_digest,
        len(logical_output),
    )
    logical_patch = M5PersistedLogicalOverlayPatch(
        resulting_revision=intent.resulting_revision,
        changes=logical_changes,
        binding_rows=binding_rows,
        output_records=output_records,
        logical_output_preimage=logical_output,
        logical_output_digest=logical_output_digest,
        output_bytes=len(logical_output),
        patch_preimage=logical_preimage,
        patch_digest=logical_digest,
    )
    observation_preimages = tuple(
        digests.matching_change_preimage(
            "m5-persisted-matching-observation-change-v1",
            (text_field(row.observation_id),),
            row.before,
            row.after,
        )
        for row in observation_changes
    )
    edge_preimages = tuple(
        digests.matching_change_preimage(
            "m5-persisted-matching-edge-change-v1",
            (text_field(row.requirement_version_id), hash_field(row.text_hash)),
            row.before,
            row.after,
        )
        for row in edge_changes
    )
    mask_preimages = tuple(
        digests.matching_change_preimage(
            "m5-persisted-matching-mask-change-v1",
            (text_field(row.group_version_id), hash_field(row.text_hash)),
            row.before,
            row.after,
        )
        for row in mask_changes
    )
    hall_preimages = tuple(
        digests.matching_change_preimage(
            "m5-persisted-matching-hall-change-v1",
            (text_field(row.group_version_id),),
            row.before,
            row.after,
        )
        for row in hall_changes
    )
    shapes = tuple(shape.digest_row for shape in intent.group_shapes)
    shape_digest = digests.matching_group_shape_set_digest(shapes)
    work_digest = digests.matching_work_digest(m5_overlay_work_values(work))
    patch_values: dict[str, Any] = {
        "source_kind": intent.source_kind,
        "source_id": intent.source_id,
        "source_identity_hash": intent.source_identity_hash,
        "before_epoch_id": intent.before_epoch_id,
        "before_revision": intent.before_revision,
        "resulting_epoch_id": intent.resulting_epoch_id,
        "resulting_revision": intent.resulting_revision,
        "decision_policy_version": intent.decision_policy_version,
        "group_shape_set_digest": shape_digest,
        "observation_change_digests": tuple(
            row.change_digest for row in observation_changes
        ),
        "edge_change_digests": tuple(row.change_digest for row in edge_changes),
        "mask_change_digests": tuple(row.change_digest for row in mask_changes),
        "hall_change_digests": tuple(row.change_digest for row in hall_changes),
        "logical_overlay_patch_digest_value": logical_digest,
        "matching_work_digest_value": work_digest,
    }
    patch = M5PersistedMatchingPatch(
        source_kind=intent.source_kind,
        source_id=intent.source_id,
        source_identity_hash=intent.source_identity_hash,
        before_epoch_id=intent.before_epoch_id,
        before_revision=intent.before_revision,
        resulting_epoch_id=intent.resulting_epoch_id,
        resulting_revision=intent.resulting_revision,
        decision_policy_version=intent.decision_policy_version,
        group_shape_set_digest=shape_digest,
        observation_change_digests=patch_values["observation_change_digests"],
        edge_change_digests=patch_values["edge_change_digests"],
        mask_change_digests=patch_values["mask_change_digests"],
        hall_change_digests=patch_values["hall_change_digests"],
        logical_overlay_patch_digest=logical_digest,
        matching_work_digest=work_digest,
        patch_digest=digests.persisted_matching_patch_digest(**patch_values),
    )
    return M5PersistedMatchingPatchArtifact(
        patch=patch,
        group_shapes=intent.group_shapes,
        group_shape_set_preimage=digests.matching_group_shape_set_preimage(shapes),
        observation_changes=observation_changes,
        observation_change_preimages=observation_preimages,
        edge_changes=edge_changes,
        edge_change_preimages=edge_preimages,
        mask_changes=mask_changes,
        mask_change_preimages=mask_preimages,
        hall_changes=hall_changes,
        hall_change_preimages=hall_preimages,
        logical_patch=logical_patch,
        work=work,
        patch_preimage=digests.persisted_matching_patch_preimage(**patch_values),
    )


def _matching_contribution(
    artifact: M5PersistedMatchingPatchArtifact,
) -> M5PersistedMatchingContribution:
    patch = artifact.patch
    contribution_digest = digests.matching_work_contribution_digest(
        epoch_id=patch.resulting_epoch_id,
        source_kind=patch.source_kind,
        source_id=patch.source_id,
        source_identity_hash=patch.source_identity_hash,
        before_epoch_id=patch.before_epoch_id,
        before_revision=patch.before_revision,
        resulting_revision=patch.resulting_revision,
        patch_digest=patch.patch_digest,
        matching_work_digest_value=patch.matching_work_digest,
    )
    return M5PersistedMatchingContribution(
        epoch_id=patch.resulting_epoch_id,
        source_kind=patch.source_kind,
        source_id=patch.source_id,
        source_identity_hash=patch.source_identity_hash,
        before_epoch_id=patch.before_epoch_id,
        before_revision=patch.before_revision,
        resulting_revision=patch.resulting_revision,
        patch_digest=patch.patch_digest,
        work=artifact.work,
        contribution_digest=contribution_digest,
    )


def _insert_matching_artifact(
    cursor: Cursor[Any], artifact: M5PersistedMatchingPatchArtifact
) -> None:
    patch = artifact.patch
    cursor.execute(
        """
        INSERT INTO groundloop_m5_matching_patch_artifact (
          patch_digest, source_kind, source_id, source_identity_hash,
          before_epoch_id, before_revision, resulting_epoch_id,
          resulting_revision, decision_policy_version,
          group_shape_set_digest, group_shape_set_preimage,
          observation_change_digests, observation_change_preimages,
          edge_change_digests, edge_change_preimages,
          mask_change_digests, mask_change_preimages,
          hall_change_digests, hall_change_preimages,
          logical_overlay_patch_digest, logical_overlay_patch_preimage,
          logical_output_preimage, matching_work_digest,
          canonical_patch_preimage
        ) VALUES (
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
          %s::char(64)[],%s::bytea[],%s::char(64)[],%s::bytea[],
          %s::char(64)[],%s::bytea[],%s::char(64)[],%s::bytea[],
          %s,%s,%s,%s,%s
        )
        ON CONFLICT (patch_digest) DO NOTHING
        """,
        (
            patch.patch_digest,
            patch.source_kind.value,
            patch.source_id,
            patch.source_identity_hash,
            patch.before_epoch_id,
            patch.before_revision,
            patch.resulting_epoch_id,
            patch.resulting_revision,
            patch.decision_policy_version,
            patch.group_shape_set_digest,
            artifact.group_shape_set_preimage,
            list(patch.observation_change_digests),
            list(artifact.observation_change_preimages),
            list(patch.edge_change_digests),
            list(artifact.edge_change_preimages),
            list(patch.mask_change_digests),
            list(artifact.mask_change_preimages),
            list(patch.hall_change_digests),
            list(artifact.hall_change_preimages),
            patch.logical_overlay_patch_digest,
            artifact.logical_patch.patch_preimage,
            artifact.logical_patch.logical_output_preimage,
            patch.matching_work_digest,
            artifact.patch_preimage,
        ),
    )


def _insert_contribution(
    cursor: Cursor[Any], contribution: M5PersistedMatchingContribution
) -> None:
    counter_columns = sql.SQL(", ").join(
        sql.Identifier(name) for name in MATCHING_WORK_COUNTER_NAMES
    )
    contribution_columns = sql.SQL(", ").join(
        (
            sql.SQL(
                "epoch_id, source_kind, source_id, source_identity_hash, "
                "before_epoch_id, before_revision, resulting_revision, patch_digest"
            ),
            counter_columns,
            sql.SQL("matching_work_digest, contribution_digest"),
        )
    )
    contribution_values = (
        contribution.epoch_id,
        contribution.source_kind.value,
        contribution.source_id,
        contribution.source_identity_hash,
        contribution.before_epoch_id,
        contribution.before_revision,
        contribution.resulting_revision,
        contribution.patch_digest,
        *m5_overlay_work_values(contribution.work),
        digests.matching_work_digest(m5_overlay_work_values(contribution.work)),
        contribution.contribution_digest,
    )
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_matching_work_contribution ({}) VALUES ({})"
        ).format(
            contribution_columns,
            sql.SQL(", ").join(sql.Placeholder() for _ in contribution_values),
        ),
        contribution_values,
    )


def _insert_or_advance_matching_accumulator(
    cursor: Cursor[Any],
    contribution: M5PersistedMatchingContribution,
    *,
    prewrite_matching_revision: int,
) -> M5OverlayWork:
    """Own tier 15k only: validate, add one vector, and advance once."""

    counter_columns = sql.SQL(", ").join(
        sql.Identifier(name) for name in MATCHING_WORK_COUNTER_NAMES
    )
    prior = cursor.execute(
        sql.SQL(
            "SELECT {}, matching_work_digest, updated_revision FROM "
            "groundloop_m5_matching_work_accumulator "
            "WHERE epoch_id = %s FOR UPDATE"
        ).format(counter_columns),
        (contribution.epoch_id,),
    ).fetchone()
    if contribution.source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN:
        if prior is not None:
            raise EventConflictError("structural matching accumulator already exists")
        accumulated = contribution.work
    else:
        if prior is None:
            raise EventConflictError("later matching transition lacks an accumulator")
        prior_work = _overlay_work_from_values(
            tuple(int(value) for value in prior[:-2])
        )
        if int(prior[-1]) != prewrite_matching_revision or _sha256_text(
            prior[-2]
        ) != digests.matching_work_digest(m5_overlay_work_values(prior_work)):
            raise EventConflictError("matching accumulator changed before finalization")
        accumulated = prior_work + contribution.work
    accumulated_digest = digests.matching_work_digest(
        m5_overlay_work_values(accumulated)
    )
    accumulator_columns = sql.SQL(", ").join(
        (
            sql.SQL("epoch_id"),
            counter_columns,
            sql.SQL("matching_work_digest, updated_revision"),
        )
    )
    accumulator_values = (
        contribution.epoch_id,
        *m5_overlay_work_values(accumulated),
        accumulated_digest,
        contribution.resulting_revision,
    )
    if prior is None:
        cursor.execute(
            sql.SQL(
                "INSERT INTO groundloop_m5_matching_work_accumulator ({}) VALUES ({})"
            ).format(
                accumulator_columns,
                sql.SQL(", ").join(sql.Placeholder() for _ in accumulator_values),
            ),
            accumulator_values,
        )
    else:
        assignments = sql.SQL(", ").join(
            sql.SQL("{} = %s").format(sql.Identifier(name))
            for name in MATCHING_WORK_COUNTER_NAMES
        )
        cursor.execute(
            sql.SQL(
                "UPDATE groundloop_m5_matching_work_accumulator SET {}, "
                "matching_work_digest = %s, updated_revision = %s "
                "WHERE epoch_id = %s"
            ).format(assignments),
            (
                *m5_overlay_work_values(accumulated),
                accumulated_digest,
                contribution.resulting_revision,
                contribution.epoch_id,
            ),
        )
    return accumulated


def _digest_array(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValidationError("persisted digest array has an invalid shape")
    return tuple(_sha256_text(item) for item in value)


def _bytea_array(value: object) -> tuple[bytes, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValidationError("persisted preimage array has an invalid shape")
    return tuple(bytes(item) for item in value)


@dataclass(frozen=True, slots=True)
class _RetainedMatchingArtifact:
    patch: M5PersistedMatchingPatch
    group_shapes: tuple[M5MatchingGroupShape, ...]
    observation_ids: tuple[str, ...]
    edge_keys: tuple[tuple[str, int, str, str], ...]
    mask_keys: tuple[tuple[str, str], ...]
    hall_group_ids: tuple[str, ...]
    requirement_state_ids: tuple[str, ...]
    group_state_ids: tuple[str, ...]
    claim_state_ids: tuple[str, ...]
    answer_state_ids: tuple[str, ...]
    group_certificate_ids: tuple[str, ...]
    claim_certificate_ids: tuple[str, ...]

    def intent(self) -> M5PersistedMatchingTransitionIntent:
        values: dict[str, Any] = {
            "source_kind": self.patch.source_kind,
            "source_id": self.patch.source_id,
            "source_identity_hash": self.patch.source_identity_hash,
            "before_epoch_id": self.patch.before_epoch_id,
            "before_revision": self.patch.before_revision,
            "resulting_epoch_id": self.patch.resulting_epoch_id,
            "resulting_revision": self.patch.resulting_revision,
            "decision_policy_version": self.patch.decision_policy_version,
            "group_shapes": self.group_shapes,
            "observation_ids": self.observation_ids,
            "edge_keys": self.edge_keys,
            "mask_keys": self.mask_keys,
            "hall_group_ids": self.hall_group_ids,
            "requirement_state_ids": self.requirement_state_ids,
            "group_state_ids": self.group_state_ids,
            "claim_state_ids": self.claim_state_ids,
            "answer_state_ids": self.answer_state_ids,
            "group_certificate_ids": self.group_certificate_ids,
            "claim_certificate_ids": self.claim_certificate_ids,
        }
        digest_values = {
            **values,
            "group_shapes": tuple(shape.digest_row for shape in self.group_shapes),
        }
        digest = digests.persisted_matching_transition_intent_digest(**digest_values)
        return M5PersistedMatchingTransitionIntent(**values, intent_digest=digest)


def _node_value(node: object, *, tag: str) -> str:
    if not isinstance(node, dict) or node.get("tag") != tag:
        raise ValidationError(f"retained matching node is not canonical {tag}")
    value = node.get("value")
    if not isinstance(value, str):
        raise ValidationError("retained matching node has no text value")
    return value


def _optional_hash_node(node: object) -> str | None:
    if isinstance(node, dict) and node.get("tag") == "null":
        return None
    value = _node_value(node, tag="sha256")
    _require_sha256("retained logical hash", value)
    return value


def _decode_group_shapes(value: object) -> tuple[M5MatchingGroupShape, ...]:
    if not isinstance(value, list):
        raise ValidationError("retained group-shape decoder returned another shape")
    result: list[M5MatchingGroupShape] = []
    for item in value:
        if not isinstance(item, dict) or not isinstance(item.get("requirements"), list):
            raise ValidationError("retained group-shape entry is malformed")
        requirements: list[tuple[int, str]] = []
        for requirement in item["requirements"]:
            if not isinstance(requirement, dict):
                raise ValidationError("retained group-shape requirement is malformed")
            children = requirement.get("children")
            if not isinstance(children, list) or len(children) != 2:
                raise ValidationError("retained group-shape requirement is malformed")
            ordinal = int(_node_value(children[0], tag="int"))
            requirement_id = _node_value(children[1], tag="text")
            requirements.append((ordinal, requirement_id))
        result.append(
            M5MatchingGroupShape(
                _text(item.get("group_id")),
                int(_text(item.get("requirement_count"))),
                tuple(requirements),
            )
        )
    return tuple(result)


def _decode_retained_matching_artifact(
    cursor: Cursor[Any], artifact_row: tuple[Any, ...]
) -> _RetainedMatchingArtifact:
    """Byte-validate one retained artifact and project only its changed keys."""

    source_kind = M5PersistedMatchingSourceKind(_text(artifact_row[1]))
    observation_digests = _digest_array(artifact_row[11])
    observation_preimages = _bytea_array(artifact_row[12])
    edge_digests = _digest_array(artifact_row[13])
    edge_preimages = _bytea_array(artifact_row[14])
    mask_digests = _digest_array(artifact_row[15])
    mask_preimages = _bytea_array(artifact_row[16])
    hall_digests = _digest_array(artifact_row[17])
    hall_preimages = _bytea_array(artifact_row[18])
    families = (
        ("observation", observation_digests, observation_preimages),
        ("edge", edge_digests, edge_preimages),
        ("mask", mask_digests, mask_preimages),
        ("hall", hall_digests, hall_preimages),
    )
    for _, declared, preimages in families:
        if len(declared) != len(preimages):
            raise ValidationError("retained matching child cardinality changed")
        if tuple(hashlib.sha256(value).hexdigest() for value in preimages) != declared:
            raise ValidationError("retained matching child digest changed")

    shape_preimage = bytes(artifact_row[10])
    logical_preimage = bytes(artifact_row[20])
    logical_output = bytes(artifact_row[21])
    canonical_patch = bytes(artifact_row[23])
    decoded = cursor.execute(
        """
        SELECT groundloop_m5_matching_validate_group_shapes(%s),
               groundloop_m5_matching_parse_typed_preimage(
                 %s, 'm5-persisted-logical-overlay-patch-v1'),
               groundloop_m5_matching_validate_logical_output(%s),
               groundloop_m5_matching_validate_logical_patch(
                 %s, %s, %s, %s, %s,
                 groundloop_m5_matching_validate_group_shapes(%s))
        """,
        (
            shape_preimage,
            logical_preimage,
            logical_output,
            logical_preimage,
            logical_output,
            int(artifact_row[6]),
            int(artifact_row[7]),
            _text(artifact_row[8]),
            shape_preimage,
        ),
    ).fetchone()
    if decoded is None or decoded[3] is not True:
        raise ValidationError("retained matching logical artifact is invalid")
    group_shapes = _decode_group_shapes(decoded[0])
    logical_root = decoded[1]
    outputs = decoded[2]
    if not isinstance(logical_root, dict) or not isinstance(outputs, list):
        raise ValidationError(
            "retained matching logical decoder returned another shape"
        )
    logical_children = logical_root.get("children")
    if not isinstance(logical_children, list) or len(logical_children) != 4:
        raise ValidationError("retained logical patch has another root shape")
    change_nodes = logical_children[0].get("children")
    binding_nodes = logical_children[1].get("children")
    if not isinstance(change_nodes, list) or not isinstance(binding_nodes, list):
        raise ValidationError("retained logical patch has malformed sequences")
    logical_changes: list[
        tuple[M5PersistedLogicalChangeKind, str, str | None, str | None]
    ] = []
    logical_ids: dict[M5PersistedLogicalChangeKind, list[str]] = {
        kind: [] for kind in M5PersistedLogicalChangeKind
    }
    for node in change_nodes:
        if not isinstance(node, dict) or not isinstance(node.get("children"), list):
            raise ValidationError("retained logical change is malformed")
        children = node["children"]
        if len(children) != 4:
            raise ValidationError("retained logical change is malformed")
        kind = M5PersistedLogicalChangeKind(_node_value(children[0], tag="enum"))
        object_id = _node_value(children[1], tag="text")
        before_hash = _optional_hash_node(children[2])
        after_hash = _optional_hash_node(children[3])
        logical_changes.append((kind, object_id, before_hash, after_hash))
        logical_ids[kind].append(object_id)
    binding_hashes = tuple(_node_value(node, tag="sha256") for node in binding_nodes)
    output_digest = _node_value(logical_children[2], tag="sha256")
    output_bytes = int(_node_value(logical_children[3], tag="int"))
    if (
        hashlib.sha256(logical_output).hexdigest() != output_digest
        or len(logical_output) != output_bytes
        or hashlib.sha256(logical_preimage).hexdigest()
        != _sha256_text(artifact_row[19])
        or digests.logical_overlay_patch_digest(
            tuple(logical_changes), binding_hashes, output_digest, output_bytes
        )
        != _sha256_text(artifact_row[19])
    ):
        raise ValidationError("retained logical patch digest changed")

    binding_ids: dict[str, list[str]] = {"group_binding": [], "claim_binding": []}
    for output in outputs:
        if not isinstance(output, dict):
            raise ValidationError("retained logical output is malformed")
        kind_value = output.get("kind")
        if kind_value in binding_ids:
            binding_ids[str(kind_value)].append(_text(output.get("object_id")))

    physical: dict[str, list[dict[str, object]]] = {
        "observation": [],
        "edge": [],
        "mask": [],
        "hall": [],
    }
    for family, _, preimages in families:
        prior_key: tuple[object, ...] | None = None
        for preimage in preimages:
            row = cursor.execute(
                """
                WITH decoded AS (
                  SELECT groundloop_m5_matching_validate_change(%s,%s) AS value
                ), shapes AS (
                  SELECT groundloop_m5_matching_validate_group_shapes(%s) AS value
                )
                SELECT decoded.value
                FROM decoded, shapes
                WHERE groundloop_m5_matching_validate_patch_change_point(
                        decoded.value, %s, %s, %s, %s, %s)
                  AND groundloop_m5_matching_validate_change_shape(
                        decoded.value, shapes.value, %s)
                """,
                (
                    preimage,
                    family,
                    shape_preimage,
                    source_kind.value,
                    int(artifact_row[4]),
                    int(artifact_row[5]),
                    int(artifact_row[6]),
                    int(artifact_row[7]),
                    family,
                ),
            ).fetchone()
            if row is None or not isinstance(row[0], dict):
                raise ValidationError("retained matching change did not validate")
            current = row[0]
            if current.get("after") is None:
                raise ValidationError("retained physical change has no after point")
            current_key: tuple[object, ...]
            if family in {"observation", "hall"}:
                current_key = (_text(current["outer_one"]),)
            elif family == "edge":
                current_key = (
                    _text(current["sort_group"]),
                    int(_text(current["sort_ordinal"])),
                    _sha256_text(current["outer_two"]),
                    _text(current["outer_one"]),
                )
            else:
                current_key = (
                    _text(current["outer_one"]),
                    _sha256_text(current["outer_two"]),
                )
            if prior_key is not None and current_key <= prior_key:
                raise ValidationError(
                    f"retained {family} changes are not sorted unique"
                )
            physical[family].append(current)
            prior_key = current_key

    patch_values: dict[str, Any] = {
        "source_kind": source_kind,
        "source_id": _text(artifact_row[2]),
        "source_identity_hash": _sha256_text(artifact_row[3]),
        "before_epoch_id": int(artifact_row[4]),
        "before_revision": int(artifact_row[5]),
        "resulting_epoch_id": int(artifact_row[6]),
        "resulting_revision": int(artifact_row[7]),
        "decision_policy_version": _text(artifact_row[8]),
        "group_shape_set_digest": _sha256_text(artifact_row[9]),
        "observation_change_digests": observation_digests,
        "edge_change_digests": edge_digests,
        "mask_change_digests": mask_digests,
        "hall_change_digests": hall_digests,
        "logical_overlay_patch_digest_value": _sha256_text(artifact_row[19]),
        "matching_work_digest_value": _sha256_text(artifact_row[22]),
    }
    if (
        hashlib.sha256(shape_preimage).hexdigest()
        != patch_values["group_shape_set_digest"]
        or digests.matching_group_shape_set_digest(
            tuple(shape.digest_row for shape in group_shapes)
        )
        != patch_values["group_shape_set_digest"]
    ):
        raise ValidationError("retained matching group-shape digest changed")
    expected_patch_preimage = digests.persisted_matching_patch_preimage(**patch_values)
    expected_patch_digest = digests.persisted_matching_patch_digest(**patch_values)
    if (
        canonical_patch != expected_patch_preimage
        or hashlib.sha256(canonical_patch).hexdigest() != expected_patch_digest
        or _sha256_text(artifact_row[0]) != expected_patch_digest
    ):
        raise ValidationError("retained matching outer patch digest changed")
    patch = M5PersistedMatchingPatch(
        source_kind=source_kind,
        source_id=patch_values["source_id"],
        source_identity_hash=patch_values["source_identity_hash"],
        before_epoch_id=patch_values["before_epoch_id"],
        before_revision=patch_values["before_revision"],
        resulting_epoch_id=patch_values["resulting_epoch_id"],
        resulting_revision=patch_values["resulting_revision"],
        decision_policy_version=patch_values["decision_policy_version"],
        group_shape_set_digest=patch_values["group_shape_set_digest"],
        observation_change_digests=observation_digests,
        edge_change_digests=edge_digests,
        mask_change_digests=mask_digests,
        hall_change_digests=hall_digests,
        logical_overlay_patch_digest=patch_values["logical_overlay_patch_digest_value"],
        matching_work_digest=patch_values["matching_work_digest_value"],
        patch_digest=expected_patch_digest,
    )
    group_certificates = set(
        logical_ids[M5PersistedLogicalChangeKind.GROUP_CERTIFICATE]
    )
    group_certificates.update(binding_ids["group_binding"])
    claim_certificates = set(
        logical_ids[M5PersistedLogicalChangeKind.CLAIM_CERTIFICATE]
    )
    claim_certificates.update(binding_ids["claim_binding"])
    return _RetainedMatchingArtifact(
        patch=patch,
        group_shapes=group_shapes,
        observation_ids=tuple(
            _text(item["outer_one"]) for item in physical["observation"]
        ),
        edge_keys=tuple(
            (
                _text(item["sort_group"]),
                int(_text(item["sort_ordinal"])),
                _sha256_text(item["outer_two"]),
                _text(item["requirement"]),
            )
            for item in physical["edge"]
        ),
        mask_keys=tuple(
            (_text(item["outer_one"]), _sha256_text(item["outer_two"]))
            for item in physical["mask"]
        ),
        hall_group_ids=tuple(_text(item["outer_one"]) for item in physical["hall"]),
        requirement_state_ids=tuple(
            logical_ids[M5PersistedLogicalChangeKind.REQUIREMENT_STATE]
        ),
        group_state_ids=tuple(logical_ids[M5PersistedLogicalChangeKind.GROUP_STATE]),
        claim_state_ids=tuple(logical_ids[M5PersistedLogicalChangeKind.CLAIM_STATE]),
        answer_state_ids=tuple(logical_ids[M5PersistedLogicalChangeKind.ANSWER_STATE]),
        group_certificate_ids=tuple(sorted(group_certificates)),
        claim_certificate_ids=tuple(sorted(claim_certificates)),
    )


def _lock_matching_transition_images(
    cursor: Cursor[Any], intent: M5PersistedMatchingTransitionIntent
) -> bool:
    """Lock tier 11b current then working and validate the transition point."""

    expected_runtime_revision = (
        1
        if intent.source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN
        else intent.before_revision
    )
    _require_checked_prefix(
        cursor,
        epoch_id=intent.resulting_epoch_id,
        expected_revision=expected_runtime_revision,
    )
    current = cursor.execute(
        """
        SELECT decision_policy_version, installed_epoch_id, installed_revision
        FROM groundloop_m5_matching_image_current
        WHERE singleton
        FOR UPDATE
        """
    ).fetchone()
    if current is None or (
        _text(current[0]) != intent.decision_policy_version
        or int(current[1]) != intent.before_epoch_id
        or int(current[2]) != intent.before_revision
    ):
        raise EventConflictError("matching transition current image point changed")
    working = cursor.execute(
        """
        SELECT epoch_id, base_epoch_id, base_revision,
               decision_policy_version, updated_revision
        FROM groundloop_m5_matching_image_working
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (intent.resulting_epoch_id,),
    ).fetchone()
    if working is None:
        return False
    if (
        int(working[0]) != intent.resulting_epoch_id
        or int(working[1]) != intent.before_epoch_id
        or int(working[2]) != intent.before_revision
        or _text(working[3]) != intent.decision_policy_version
        or int(working[4]) != intent.resulting_revision
    ):
        raise EventConflictError("matching replay differs from retained image bytes")
    return True


def _matching_artifact_row(
    cursor: Cursor[Any], patch_digest: str, *, lock: bool = True
) -> tuple[Any, ...] | None:
    row = cursor.execute(
        sql.SQL(
            """
        SELECT patch_digest, source_kind, source_id, source_identity_hash,
               before_epoch_id, before_revision, resulting_epoch_id,
               resulting_revision, decision_policy_version,
               group_shape_set_digest, group_shape_set_preimage,
               observation_change_digests, observation_change_preimages,
               edge_change_digests, edge_change_preimages,
               mask_change_digests, mask_change_preimages,
               hall_change_digests, hall_change_preimages,
               logical_overlay_patch_digest, logical_overlay_patch_preimage,
               logical_output_preimage, matching_work_digest,
               canonical_patch_preimage
        FROM groundloop_m5_matching_patch_artifact
        WHERE patch_digest = %s
        {}
        """
        ).format(sql.SQL("FOR SHARE") if lock else sql.SQL("")),
        (patch_digest,),
    ).fetchone()
    return None if row is None else tuple(row)


def _lock_matching_artifact_key(
    cursor: Cursor[Any], patch_digest: str
) -> tuple[Any, ...] | None:
    lock_key = int(patch_digest[:8], 16)
    if lock_key >= 2**31:
        lock_key -= 2**32
    cursor.execute(
        "SELECT pg_advisory_xact_lock(%s, %s)",
        (_MATCHING_ARTIFACT_LOCK_NAMESPACE, lock_key),
    )
    return _matching_artifact_row(cursor, patch_digest)


def _validate_matching_artifact_row(
    artifact_row: tuple[Any, ...], expected: M5PersistedMatchingPatchArtifact
) -> None:
    expected_patch = expected.patch
    if (
        _sha256_text(artifact_row[0]) != expected_patch.patch_digest
        or _text(artifact_row[1]) != expected_patch.source_kind.value
        or _text(artifact_row[2]) != expected_patch.source_id
        or _sha256_text(artifact_row[3]) != expected_patch.source_identity_hash
        or int(artifact_row[4]) != expected_patch.before_epoch_id
        or int(artifact_row[5]) != expected_patch.before_revision
        or int(artifact_row[6]) != expected_patch.resulting_epoch_id
        or int(artifact_row[7]) != expected_patch.resulting_revision
        or _text(artifact_row[8]) != expected_patch.decision_policy_version
        or _sha256_text(artifact_row[9]) != expected_patch.group_shape_set_digest
        or bytes(artifact_row[10]) != expected.group_shape_set_preimage
        or _digest_array(artifact_row[11]) != expected_patch.observation_change_digests
        or _bytea_array(artifact_row[12]) != expected.observation_change_preimages
        or _digest_array(artifact_row[13]) != expected_patch.edge_change_digests
        or _bytea_array(artifact_row[14]) != expected.edge_change_preimages
        or _digest_array(artifact_row[15]) != expected_patch.mask_change_digests
        or _bytea_array(artifact_row[16]) != expected.mask_change_preimages
        or _digest_array(artifact_row[17]) != expected_patch.hall_change_digests
        or _bytea_array(artifact_row[18]) != expected.hall_change_preimages
        or _sha256_text(artifact_row[19]) != expected_patch.logical_overlay_patch_digest
        or bytes(artifact_row[20]) != expected.logical_patch.patch_preimage
        or bytes(artifact_row[21]) != expected.logical_patch.logical_output_preimage
        or _sha256_text(artifact_row[22]) != expected_patch.matching_work_digest
        or bytes(artifact_row[23]) != expected.patch_preimage
    ):
        raise EventConflictError("matching replay differs from retained patch bytes")


def _insert_or_validate_matching_artifact(
    cursor: Cursor[Any], artifact: M5PersistedMatchingPatchArtifact
) -> None:
    """Serialize globally by digest, then insert or byte-validate at tier 15i."""

    existing = _lock_matching_artifact_key(cursor, artifact.patch.patch_digest)
    if existing is not None:
        _validate_matching_artifact_row(existing, artifact)
    _insert_matching_artifact(cursor, artifact)
    stored = _matching_artifact_row(cursor, artifact.patch.patch_digest)
    if stored is None:
        raise ValidationError("persisted matching artifact insert was not retained")
    _validate_matching_artifact_row(stored, artifact)


def _lock_matching_contribution(
    cursor: Cursor[Any], intent: M5PersistedMatchingTransitionIntent
) -> tuple[Any, ...] | None:
    counter_columns = sql.SQL(", ").join(
        sql.Identifier(name) for name in MATCHING_WORK_COUNTER_NAMES
    )
    source_row = cursor.execute(
        sql.SQL(
            """
            SELECT epoch_id, source_kind, source_id, source_identity_hash,
                   before_epoch_id, before_revision, resulting_revision,
                   patch_digest,
                   {},
                   matching_work_digest, contribution_digest
            FROM groundloop_m5_matching_work_contribution
            WHERE epoch_id = %s AND source_kind = %s AND source_id = %s
            FOR SHARE
            """
        ).format(counter_columns),
        (intent.resulting_epoch_id, intent.source_kind.value, intent.source_id),
    ).fetchone()
    revision_row = cursor.execute(
        """
        SELECT epoch_id, source_kind, source_id, resulting_revision
        FROM groundloop_m5_matching_work_contribution
        WHERE epoch_id = %s AND resulting_revision = %s
        FOR SHARE
        """,
        (intent.resulting_epoch_id, intent.resulting_revision),
    ).fetchone()
    if source_row is None and revision_row is None:
        return None
    if (
        source_row is None
        or revision_row is None
        or (
            int(source_row[0]),
            _text(source_row[1]),
            _text(source_row[2]),
            int(source_row[6]),
        )
        != (
            int(revision_row[0]),
            _text(revision_row[1]),
            _text(revision_row[2]),
            int(revision_row[3]),
        )
    ):
        raise EventConflictError("matching contribution keys name different sources")
    return tuple(source_row)


def _header_images(
    cursor: Cursor[Any], epoch_id: int
) -> tuple[_BaseHeaderAfterImage, _RuntimeHeaderAfterImage]:
    row = cursor.execute(
        """
        SELECT epoch.epoch_id, epoch.revision, epoch.structural_status,
               epoch.semantic_status, epoch.evaluation_state,
               epoch.publication_mode, epoch.sealed_at IS NOT NULL,
               runtime.epoch_id, runtime.revision, runtime.runtime_state,
               runtime.open_work_count, runtime.open_scope_count,
               runtime.blocking_failure_count,
               runtime.terminal_at IS NOT NULL,
               epoch.open_job_count, epoch.open_scope_count
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        WHERE epoch.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise InvalidEventError("matching transition header is missing")
    return (
        _BaseHeaderAfterImage(
            int(row[0]),
            int(row[1]),
            _text(row[2]),
            _text(row[3]),
            _text(row[4]),
            _text(row[5]),
            bool(row[6]),
            int(row[14]),
            int(row[15]),
        ),
        _RuntimeHeaderAfterImage(
            int(row[7]),
            int(row[8]),
            _text(row[9]),
            int(row[10]),
            int(row[11]),
            int(row[12]),
            bool(row[13]),
        ),
    )


def _validate_prepared_identity(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    prepared: _PreparedMatchingTransition,
    *,
    phase: _MatchingPhase,
) -> None:
    if type(prepared) is not _PreparedMatchingTransition:
        raise ValidationError("matching prepared authority has another concrete type")
    if prepared.prepared_identity != id(prepared):
        raise ValidationError("matching prepared authority was copied or replaced")
    snapshot = prepared.authority_snapshot
    if type(snapshot) is not _PreparedAuthoritySnapshot or (
        snapshot.snapshot_identity != id(snapshot)
    ):
        raise ValidationError("matching prepared authority snapshot was replaced")
    if prepared.prewrite_patch_artifact != snapshot.prewrite_patch_artifact:
        raise EventConflictError("matching prepared patch or held before image changed")
    if (
        prepared.physical_and_logical_write_plan
        != snapshot.physical_and_logical_write_plan
    ):
        raise EventConflictError("matching prepared write plan changed")
    if (
        prepared.base_header_after_image != snapshot.base_header_after_image
        or prepared.runtime_header_after_image != snapshot.runtime_header_after_image
    ):
        raise EventConflictError("matching header after-image changed")
    if (
        prepared.d24_owned_planned_write_counts
        != snapshot.d24_owned_planned_write_counts
    ):
        raise EventConflictError("matching planned write counts changed")
    if (
        prepared.binding != snapshot.binding
        or prepared.prepared_identity != snapshot.prepared_identity
        or prepared.official_intent != snapshot.official_intent
        or prepared.prewrite_matching_revision != snapshot.prewrite_matching_revision
        or prepared.matching_image_base_epoch_id
        != snapshot.matching_image_base_epoch_id
        or prepared.matching_image_base_revision
        != snapshot.matching_image_base_revision
        or prepared.stage_before_images != snapshot.stage_before_images
        or prepared.observation_currency_before_images
        != snapshot.observation_currency_before_images
        or prepared.d25_contribution_work != snapshot.d25_contribution_work
        or prepared.requirement_state_write_count_diagnostic
        != snapshot.requirement_state_write_count_diagnostic
        or prepared.expected_patch_digest != snapshot.expected_patch_digest
        or prepared.expected_work != snapshot.expected_work
        or prepared.direct_reservation_or_none != snapshot.direct_reservation_or_none
        or prepared.direct_m4_stage_evidence_or_none
        != snapshot.direct_m4_stage_evidence_or_none
    ):
        raise EventConflictError("matching prepared authority changed after sealing")
    if prepared.phase is not phase:
        raise EventConflictError("matching prepared authority is in another phase")
    if prepared.official_intent != intent:
        raise EventConflictError("matching prepared authority names another intent")
    _validate_cursor_binding(cursor, intent, prepared.binding)
    if intent.source_kind is M5PersistedMatchingSourceKind.DIRECT_TRANSITION:
        reservation = prepared.direct_reservation_or_none
        stage_result = prepared.direct_m4_stage_evidence_or_none
        if (
            type(reservation) is not _DirectMatchingReservation
            or type(stage_result) is not _DirectM4StageResult
            or reservation.phase != "consumed"
            or stage_result.phase != "consumed"
            or reservation.binding != prepared.binding
            or stage_result.reservation_identity != reservation.reservation_identity
        ):
            raise EventConflictError("direct prepared evidence changed")
    elif (
        prepared.direct_reservation_or_none is not None
        or prepared.direct_m4_stage_evidence_or_none is not None
    ):
        raise EventConflictError("non-direct prepared authority has direct evidence")


def _read_held_first_application_image(
    cursor: Cursor[Any], intent: M5PersistedMatchingTransitionIntent
) -> tuple[int, int, int]:
    """Revalidate held 11b rows and return their immutable coordinates."""

    current = cursor.execute(
        """
        SELECT decision_policy_version, installed_epoch_id, installed_revision
        FROM groundloop_m5_matching_image_current
        WHERE singleton
        """
    ).fetchone()
    if current is None or _text(current[0]) != intent.decision_policy_version:
        raise EventConflictError("matching current-image policy changed")
    working = cursor.execute(
        """
        SELECT base_epoch_id, base_revision, decision_policy_version,
               updated_revision
        FROM groundloop_m5_matching_image_working
        WHERE epoch_id = %s
        """,
        (intent.resulting_epoch_id,),
    ).fetchone()
    if intent.source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN:
        if (
            int(current[1]) != intent.before_epoch_id
            or int(current[2]) != intent.before_revision
            or working is not None
        ):
            raise EventConflictError("structural matching first image changed")
        return 0, intent.before_epoch_id, intent.before_revision
    if working is None:
        raise EventConflictError("later matching first application lacks working image")
    if (
        int(working[0]) != int(current[1])
        or int(working[1]) != int(current[2])
        or _text(working[2]) != intent.decision_policy_version
        or int(working[3]) > intent.before_revision
    ):
        raise EventConflictError("later matching prewrite image changed")
    return int(working[3]), int(working[0]), int(working[1])


def _claim_status(*, supported: bool, refuted: bool) -> ClaimStatus:
    if supported and refuted:
        return ClaimStatus.CONFLICTED
    if supported:
        return ClaimStatus.SUPPORTED
    if refuted:
        return ClaimStatus.REFUTED
    return ClaimStatus.UNSUPPORTED


def _answer_status(counts: Counter[ClaimStatus], required_count: int) -> AnswerStatus:
    if counts[ClaimStatus.REFUTED] > 0:
        return AnswerStatus.CONTRADICTED
    if counts[ClaimStatus.CONFLICTED] > 0:
        return AnswerStatus.CONFLICTED
    if counts[ClaimStatus.SUPPORTED] == required_count:
        return AnswerStatus.VALID
    if counts[ClaimStatus.SUPPORTED] > 0:
        return AnswerStatus.PARTIALLY_SUPPORTED
    return AnswerStatus.UNSUPPORTED


def _load_group_certificate_artifact(
    cursor: Cursor[Any],
    *,
    group_id: str,
    certificate_digest: str,
) -> GroupMatchingCertificateArtifact:
    header = cursor.execute(
        """
        SELECT decision_policy_version, certificate_version,
               group_version_id, requirement_count
        FROM groundloop_m5_group_certificate_artifact
        WHERE certificate_digest = %s
        """,
        (certificate_digest,),
    ).fetchone()
    rows = cursor.execute(
        """
        SELECT requirement_ordinal, requirement_version_id, text_hash,
               selected_observation_id
        FROM groundloop_m5_group_certificate_artifact_row
        WHERE certificate_digest = %s
        ORDER BY requirement_ordinal
        """,
        (certificate_digest,),
    ).fetchall()
    if header is None or (
        _text(header[1]) != "m5-group-certificate-v1"
        or _text(header[2]) != group_id
        or int(header[3]) != len(rows)
    ):
        raise EventConflictError("selected group certificate changed")
    return GroupMatchingCertificateArtifact(
        decision_policy_version=_text(header[0]),
        group_version_id=group_id,
        rows=tuple(
            GroupCertificateRow(
                int(row[0]),
                _text(row[1]),
                _sha256_text(row[2]),
                _text(row[3]),
            )
            for row in rows
        ),
        certificate_digest=certificate_digest,
    )


def _claim_certificate_from_state(
    cursor: Cursor[Any],
    state: CombinedClaimState,
    *,
    decision_policy_version: str,
    before_epoch_id: int,
) -> ClaimCertificateArtifact:
    selected_group_id: str | None = None
    selected_group_digest: str | None = None
    if state.supporting_observation_ids:
        support_kind = ClaimSupportKind.DIRECT
        direct_support = state.supporting_observation_ids[0]
    elif state.complete_group_ids:
        support_kind = ClaimSupportKind.GROUP
        direct_support = None
        selected_group_id = state.complete_group_ids[0]
        row = cursor.execute(
            """
            SELECT certificate_digest
            FROM groundloop_m5_published_group_certificate_binding
            WHERE group_version_id = %s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            """,
            (selected_group_id, before_epoch_id, before_epoch_id),
        ).fetchone()
        if row is None:
            raise EventConflictError(
                "selected complete group has no published certificate"
            )
        selected_group_digest = _sha256_text(row[0])
        selected = _load_group_certificate_artifact(
            cursor,
            group_id=selected_group_id,
            certificate_digest=selected_group_digest,
        )
        if selected.decision_policy_version != decision_policy_version:
            raise EventConflictError("selected group certificate policy changed")
    else:
        support_kind = ClaimSupportKind.NONE
        direct_support = None
    return ClaimCertificateArtifact(
        claim_id=state.claim_id,
        decision_policy_version=decision_policy_version,
        support_kind=support_kind,
        direct_support_observation_id=direct_support,
        group_version_id=selected_group_id,
        group_certificate_digest=selected_group_digest,
        direct_refute_observation_id=(
            state.refuting_observation_ids[0]
            if state.refuting_observation_ids
            else None
        ),
    )


def _load_claim_certificate_artifact(
    cursor: Cursor[Any], certificate_digest: str
) -> ClaimCertificateArtifact:
    row = cursor.execute(
        """
        SELECT certificate_version, claim_id, decision_policy_version,
               support_kind, direct_support_observation_id,
               group_version_id, group_certificate_digest,
               direct_refute_observation_id
        FROM groundloop_m5_claim_certificate_artifact
        WHERE certificate_digest = %s
        """,
        (certificate_digest,),
    ).fetchone()
    if row is None or _text(row[0]) != "m5-claim-certificate-v2":
        raise EventConflictError("published claim certificate artifact changed")
    return ClaimCertificateArtifact(
        claim_id=_text(row[1]),
        decision_policy_version=_text(row[2]),
        support_kind=ClaimSupportKind(_text(row[3])),
        direct_support_observation_id=(None if row[4] is None else _text(row[4])),
        group_version_id=None if row[5] is None else _text(row[5]),
        group_certificate_digest=(None if row[6] is None else _sha256_text(row[6])),
        direct_refute_observation_id=(None if row[7] is None else _text(row[7])),
        certificate_digest=certificate_digest,
    )


def _structural_registration_plan(
    intent: M5PersistedMatchingTransitionIntent,
) -> tuple[
    M5PersistedMatchingPatchArtifact,
    _MatchingWritePlan,
    _D24OwnedWriteCounts,
    int,
]:
    """Materialize one newly registered, initially empty bounded group."""

    if len(intent.group_shapes) != 1:
        raise EventConflictError("group registration shape changed")
    shape = intent.group_shapes[0]
    requirement_ids = tuple(
        sorted(requirement_id for _, requirement_id in shape.requirements)
    )
    if (
        intent.observation_ids
        or intent.edge_keys
        or intent.mask_keys
        or intent.hall_group_ids != (shape.group_version_id,)
        or intent.requirement_state_ids != requirement_ids
        or intent.group_state_ids != (shape.group_version_id,)
    ):
        raise EventConflictError("group registration physical plan changed")

    initialized = initialize_hall_mask_state(shape.requirement_count, ())
    hall = initialized.state
    hall_after = M5MatchingHallWorking(
        M5MatchingLayer.WORKING,
        intent.resulting_epoch_id,
        shape.group_version_id,
        True,
        hall.requirement_count,
        hall.mask_histogram,
        hall.neighbor_counts,
        hall.deficiencies,
        hall.maximum_deficiency,
        hall.matching_size,
        hall.distinct_hash_count,
        intent.resulting_revision,
    )
    hall_change = M5MatchingHallChange(
        shape.group_version_id,
        None,
        hall_after,
        digests.matching_change_digest(
            "m5-persisted-matching-hall-change-v1",
            (text_field(shape.group_version_id),),
            None,
            hall_after,
        ),
    )
    requirement_writes = tuple(
        _RequirementStateWrite(
            RequirementState(requirement_id, (), (), 0, False),
            intent.decision_policy_version,
        )
        for requirement_id in requirement_ids
    )
    group_state = GroupState(
        shape.group_version_id,
        shape.requirement_count,
        0,
        hall.matching_size,
        hall.complete,
    )
    if group_state.complete:
        raise EventConflictError("empty registered group unexpectedly completed")
    group_write = _GroupStateWrite(
        group_state,
        intent.decision_policy_version,
        None,
    )
    logical_changes: list[M5PersistedLogicalChange] = []
    output_records: list[tuple[str, str, object]] = []
    for write in requirement_writes:
        state = write.state
        after_hash = digests.requirement_state_artifact_digest(
            requirement_version_id=state.requirement_version_id,
            witness_hashes=state.witness_hashes,
            supporting_observation_ids=state.supporting_observation_ids,
            witness_count=state.witness_count,
            satisfied=state.satisfied,
            decision_policy_version=write.decision_policy_version,
        )
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.REQUIREMENT_STATE,
                state.requirement_version_id,
                None,
                after_hash,
            )
        )
        output_records.append(
            ("requirement_state", state.requirement_version_id, state)
        )
    group_after_hash = digests.group_state_artifact_digest(
        group_version_id=group_state.group_version_id,
        requirement_count=group_state.requirement_count,
        satisfied_count=group_state.satisfied_count,
        matching_size=group_state.matching_size,
        complete=group_state.complete,
        decision_policy_version=intent.decision_policy_version,
        certificate_digest=None,
    )
    logical_changes.append(
        M5PersistedLogicalChange(
            M5PersistedLogicalChangeKind.GROUP_STATE,
            group_state.group_version_id,
            None,
            group_after_hash,
        )
    )
    output_records.append(("group_state", group_state.group_version_id, group_state))
    logical_changes_tuple = tuple(
        sorted(logical_changes, key=lambda item: (item.kind.value, item.object_id))
    )
    output_records_tuple = tuple(
        sorted(
            output_records,
            key=lambda item: (
                0 if item[0] == "requirement_state" else 1,
                item[1],
            ),
        )
    )
    logical_output = digests.logical_output_preimage(output_records_tuple)
    work = M5OverlayWork(
        matching=(
            initialized.work
            + touched_state_work(
                groups_touched=1,
                output_bytes=len(logical_output),
            )
        )
    )
    patch_artifact = _build_patch_artifact(
        intent,
        hall_changes=(hall_change,),
        logical_changes=logical_changes_tuple,
        output_records=output_records_tuple,
        work=work,
    )
    plan = _MatchingWritePlan(
        hall_rows=(hall_after,),
        requirement_state_rows=requirement_writes,
        group_state_rows=(group_write,),
    )
    return (
        patch_artifact,
        plan,
        _D24OwnedWriteCounts(group_state_write_count=1),
        len(requirement_writes),
    )


def _structural_retirement_plan(
    cursor: Cursor[Any], intent: M5PersistedMatchingTransitionIntent
) -> tuple[
    M5PersistedMatchingPatchArtifact,
    _MatchingWritePlan,
    _D24OwnedWriteCounts,
    int,
]:
    """Build one bounded predecessor removal, with an optional empty successor."""

    deactivation = cursor.execute(
        """
        SELECT group_version_id, action, successor_group_version_id, event_id
        FROM groundloop_m5_group_deactivation
        WHERE epoch_id = %s
        """,
        (intent.resulting_epoch_id,),
    ).fetchall()
    if len(deactivation) != 1 or _text(deactivation[0][3]) != intent.source_id:
        raise EventConflictError("matching group-removal source changed")
    group_id = _text(deactivation[0][0])
    action = _text(deactivation[0][1])
    successor_id = None if deactivation[0][2] is None else _text(deactivation[0][2])
    if (action, successor_id is not None) not in {
        ("RETIRE", False),
        ("REPLACE", True),
    }:
        raise EventConflictError("matching group-removal action changed")
    shapes_by_id = {shape.group_version_id: shape for shape in intent.group_shapes}
    expected_shape_ids = {group_id}
    if successor_id is not None:
        expected_shape_ids.add(successor_id)
    if set(shapes_by_id) != expected_shape_ids:
        raise EventConflictError("matching group-removal shape set changed")
    shape = shapes_by_id[group_id]
    successor_shape = None if successor_id is None else shapes_by_id[successor_id]
    observation_rows = cursor.execute(
        """
        SELECT observation_id, requirement_version_id, group_version_id,
               requirement_ordinal, text_hash, installed_epoch_id,
               installed_revision
        FROM groundloop_m5_matching_observation_current
        WHERE group_version_id = %s
        ORDER BY observation_id COLLATE "C"
        """,
        (group_id,),
    ).fetchall()
    observation_changes: list[M5MatchingObservationChange] = []
    observation_after: list[M5MatchingObservationWorking] = []
    for row in observation_rows:
        observation_before = M5MatchingObservationCurrent(
            M5MatchingLayer.CURRENT,
            _text(row[0]),
            _text(row[1]),
            _text(row[2]),
            int(row[3]),
            _sha256_text(row[4]),
            int(row[5]),
            int(row[6]),
        )
        observation_result = M5MatchingObservationWorking(
            M5MatchingLayer.WORKING,
            intent.resulting_epoch_id,
            observation_before.observation_id,
            observation_before.requirement_version_id,
            observation_before.group_version_id,
            observation_before.requirement_ordinal,
            observation_before.text_hash,
            False,
            intent.resulting_revision,
        )
        observation_after.append(observation_result)
        observation_changes.append(
            M5MatchingObservationChange(
                observation_before.observation_id,
                observation_before,
                observation_result,
                digests.matching_change_digest(
                    "m5-persisted-matching-observation-change-v1",
                    (text_field(observation_before.observation_id),),
                    observation_before,
                    observation_result,
                ),
            )
        )
    edge_rows = cursor.execute(
        """
        SELECT requirement_version_id, text_hash, group_version_id,
               requirement_ordinal, refcount, installed_epoch_id,
               installed_revision
        FROM groundloop_m5_matching_edge_current
        WHERE group_version_id = %s
        ORDER BY group_version_id COLLATE "C", requirement_ordinal,
                 text_hash COLLATE "C", requirement_version_id COLLATE "C"
        """,
        (group_id,),
    ).fetchall()
    edge_changes: list[M5MatchingEdgeChange] = []
    edge_after: list[M5MatchingEdgeWorking] = []
    for row in edge_rows:
        edge_before = M5MatchingEdgeCurrent(
            M5MatchingLayer.CURRENT,
            _text(row[0]),
            _sha256_text(row[1]),
            _text(row[2]),
            int(row[3]),
            int(row[4]),
            int(row[5]),
            int(row[6]),
        )
        edge_result = M5MatchingEdgeWorking(
            M5MatchingLayer.WORKING,
            intent.resulting_epoch_id,
            edge_before.requirement_version_id,
            edge_before.text_hash,
            edge_before.group_version_id,
            edge_before.requirement_ordinal,
            0,
            intent.resulting_revision,
        )
        edge_after.append(edge_result)
        edge_changes.append(
            M5MatchingEdgeChange(
                edge_before.requirement_version_id,
                edge_before.text_hash,
                edge_before,
                edge_result,
                digests.matching_change_digest(
                    "m5-persisted-matching-edge-change-v1",
                    (
                        text_field(edge_before.requirement_version_id),
                        hash_field(edge_before.text_hash),
                    ),
                    edge_before,
                    edge_result,
                ),
            )
        )
    mask_rows = cursor.execute(
        """
        SELECT group_version_id, text_hash, mask, installed_epoch_id,
               installed_revision
        FROM groundloop_m5_matching_hash_mask_current
        WHERE group_version_id = %s
        ORDER BY group_version_id COLLATE "C", text_hash COLLATE "C"
        """,
        (group_id,),
    ).fetchall()
    mask_changes: list[M5MatchingMaskChange] = []
    mask_after: list[M5MatchingMaskWorking] = []
    neighbour_changes = 0
    subset_entries = 0
    crossing_count = 0
    for row in mask_rows:
        mask_before = M5MatchingMaskCurrent(
            M5MatchingLayer.CURRENT,
            _text(row[0]),
            _sha256_text(row[1]),
            int(row[2]),
            int(row[3]),
            int(row[4]),
        )
        mask_result = M5MatchingMaskWorking(
            M5MatchingLayer.WORKING,
            intent.resulting_epoch_id,
            mask_before.group_version_id,
            mask_before.text_hash,
            0,
            intent.resulting_revision,
        )
        mask_after.append(mask_result)
        mask_changes.append(
            M5MatchingMaskChange(
                mask_before.group_version_id,
                mask_before.text_hash,
                mask_before,
                mask_result,
                digests.matching_change_digest(
                    "m5-persisted-matching-mask-change-v1",
                    (
                        text_field(mask_before.group_version_id),
                        hash_field(mask_before.text_hash),
                    ),
                    mask_before,
                    mask_result,
                ),
            )
        )
        crossing_count += mask_before.mask.bit_count()
        subset_entries += (1 << shape.requirement_count) - 1
        neighbour_changes += sum(
            1
            for subset in range(1, 1 << shape.requirement_count)
            if mask_before.mask & subset
        )
    hall_row = cursor.execute(
        """
        SELECT group_version_id, requirement_count, mask_histogram,
               neighbor_counts, deficiencies, maximum_deficiency,
               matching_size, distinct_hash_count, installed_epoch_id,
               installed_revision
        FROM groundloop_m5_matching_hall_current
        WHERE group_version_id = %s
        """,
        (group_id,),
    ).fetchone()
    if hall_row is None:
        raise EventConflictError("matching retirement lacks its current Hall row")
    hall_before = M5MatchingHallCurrent(
        M5MatchingLayer.CURRENT,
        _text(hall_row[0]),
        int(hall_row[1]),
        _tuple_ints(hall_row[2]),
        _tuple_ints(hall_row[3]),
        _tuple_ints(hall_row[4]),
        int(hall_row[5]),
        int(hall_row[6]),
        int(hall_row[7]),
        int(hall_row[8]),
        int(hall_row[9]),
    )
    hall_after = M5MatchingHallWorking(
        M5MatchingLayer.WORKING,
        intent.resulting_epoch_id,
        group_id,
        False,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        intent.resulting_revision,
    )
    hall_change = M5MatchingHallChange(
        group_id,
        hall_before,
        hall_after,
        digests.matching_change_digest(
            "m5-persisted-matching-hall-change-v1",
            (text_field(group_id),),
            hall_before,
            hall_after,
        ),
    )
    hall_changes = [hall_change]
    hall_after_rows = [hall_after]
    successor_initialization_work = MatchingWorkCounters()
    if successor_shape is not None:
        initialized = initialize_hall_mask_state(successor_shape.requirement_count, ())
        successor_hall = initialized.state
        successor_hall_after = M5MatchingHallWorking(
            M5MatchingLayer.WORKING,
            intent.resulting_epoch_id,
            successor_shape.group_version_id,
            True,
            successor_hall.requirement_count,
            successor_hall.mask_histogram,
            successor_hall.neighbor_counts,
            successor_hall.deficiencies,
            successor_hall.maximum_deficiency,
            successor_hall.matching_size,
            successor_hall.distinct_hash_count,
            intent.resulting_revision,
        )
        hall_after_rows.append(successor_hall_after)
        hall_changes.append(
            M5MatchingHallChange(
                successor_shape.group_version_id,
                None,
                successor_hall_after,
                digests.matching_change_digest(
                    "m5-persisted-matching-hall-change-v1",
                    (text_field(successor_shape.group_version_id),),
                    None,
                    successor_hall_after,
                ),
            )
        )
        successor_initialization_work = initialized.work
    requirement_rows = cursor.execute(
        """
        SELECT state.requirement_version_id, state.witness_hashes,
               state.supporting_observation_ids, state.witness_count,
               state.satisfied, state.decision_policy_version
        FROM groundloop_m5_published_requirement_state AS state
        JOIN groundloop_m5_requirement_version AS requirement
          USING (requirement_version_id)
        WHERE requirement.group_version_id = %s
          AND state.valid_from_epoch <= %s
          AND (state.valid_to_epoch IS NULL OR %s < state.valid_to_epoch)
        ORDER BY state.requirement_version_id COLLATE "C"
        """,
        (group_id, intent.before_epoch_id, intent.before_epoch_id),
    ).fetchall()
    predecessor_requirement_ids = tuple(
        sorted(requirement_id for _, requirement_id in shape.requirements)
    )
    if tuple(_text(row[0]) for row in requirement_rows) != (
        predecessor_requirement_ids
    ):
        raise EventConflictError("matching retirement requirement state changed")
    logical_changes: list[M5PersistedLogicalChange] = []
    output_records: list[tuple[str, str, object]] = []
    requirement_state_writes: list[_RequirementStateWrite] = []
    for row in requirement_rows:
        requirement_id = _text(row[0])
        before_hash = digests.requirement_state_artifact_digest(
            requirement_version_id=requirement_id,
            witness_hashes=tuple(_text(value) for value in row[1]),
            supporting_observation_ids=tuple(_text(value) for value in row[2]),
            witness_count=int(row[3]),
            satisfied=bool(row[4]),
            decision_policy_version=_text(row[5]),
        )
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.REQUIREMENT_STATE,
                requirement_id,
                before_hash,
                None,
            )
        )
        output_records.append(("requirement_state", requirement_id, None))
    if successor_shape is not None:
        for _, requirement_id in successor_shape.requirements:
            state = RequirementState(requirement_id, (), (), 0, False)
            write = _RequirementStateWrite(state, intent.decision_policy_version)
            requirement_state_writes.append(write)
            logical_changes.append(
                M5PersistedLogicalChange(
                    M5PersistedLogicalChangeKind.REQUIREMENT_STATE,
                    requirement_id,
                    None,
                    digests.requirement_state_artifact_digest(
                        requirement_version_id=requirement_id,
                        witness_hashes=(),
                        supporting_observation_ids=(),
                        witness_count=0,
                        satisfied=False,
                        decision_policy_version=intent.decision_policy_version,
                    ),
                )
            )
            output_records.append(("requirement_state", requirement_id, state))
    group_state = cursor.execute(
        """
        SELECT requirement_count, satisfied_count, matching_size, complete,
               decision_policy_version, certificate_digest
        FROM groundloop_m5_published_group_state
        WHERE group_version_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (group_id, intent.before_epoch_id, intent.before_epoch_id),
    ).fetchone()
    if group_state is None:
        raise EventConflictError("matching retirement group state changed")
    group_before_hash = digests.group_state_artifact_digest(
        group_version_id=group_id,
        requirement_count=int(group_state[0]),
        satisfied_count=int(group_state[1]),
        matching_size=int(group_state[2]),
        complete=bool(group_state[3]),
        decision_policy_version=_text(group_state[4]),
        certificate_digest=(
            None if group_state[5] is None else _sha256_text(group_state[5])
        ),
    )
    logical_changes.append(
        M5PersistedLogicalChange(
            M5PersistedLogicalChangeKind.GROUP_STATE,
            group_id,
            group_before_hash,
            None,
        )
    )
    output_records.append(("group_state", group_id, None))
    group_state_writes: list[_GroupStateWrite] = []
    if successor_shape is not None:
        successor_hall_after = hall_after_rows[-1]
        assert successor_hall_after.matching_size is not None
        successor_group_state = GroupState(
            successor_shape.group_version_id,
            successor_shape.requirement_count,
            0,
            successor_hall_after.matching_size,
            False,
        )
        group_state_writes.append(
            _GroupStateWrite(
                successor_group_state,
                intent.decision_policy_version,
                None,
            )
        )
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.GROUP_STATE,
                successor_shape.group_version_id,
                None,
                digests.group_state_artifact_digest(
                    group_version_id=successor_shape.group_version_id,
                    requirement_count=successor_shape.requirement_count,
                    satisfied_count=0,
                    matching_size=successor_hall_after.matching_size,
                    complete=False,
                    decision_policy_version=intent.decision_policy_version,
                    certificate_digest=None,
                ),
            )
        )
        output_records.append(
            (
                "group_state",
                successor_shape.group_version_id,
                successor_group_state,
            )
        )
    certificate = cursor.execute(
        """
        SELECT certificate_digest
        FROM groundloop_m5_published_group_certificate_binding
        WHERE group_version_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (group_id, intent.before_epoch_id, intent.before_epoch_id),
    ).fetchone()
    if certificate is not None:
        certificate_digest = _sha256_text(certificate[0])
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.GROUP_CERTIFICATE,
                group_id,
                certificate_digest,
                None,
            )
        )
        output_records.append(("group_certificate", group_id, None))

    claim_state_writes: list[_ClaimStateWrite] = []
    answer_state_writes: list[CombinedAnswerState] = []
    claim_artifact_writes: list[ClaimCertificateArtifact] = []
    claim_binding_writes: list[WorkingClaimCertificateBinding] = []
    persisted_binding_rows: list[M5PersistedCertificateBindingRow] = []
    status_deltas: list[StatusDelta] = []
    claim_state_only_changes = 0
    owner = cursor.execute(
        """
        SELECT validity.claim_id, claim.answer_version_id, claim.required
        FROM groundloop_m5_group_validity AS validity
        JOIN groundloop_claim AS claim USING (claim_id)
        WHERE validity.group_version_id = %s
          AND validity.valid_from_epoch <= %s
          AND (validity.valid_to_epoch IS NULL OR %s < validity.valid_to_epoch)
        """,
        (group_id, intent.before_epoch_id, intent.before_epoch_id),
    ).fetchone()
    if owner is None:
        raise EventConflictError("matching retirement group owner changed")
    claim_id = _text(owner[0])
    answer_id = _text(owner[1])
    required_claim = bool(owner[2])
    group_was_complete = bool(group_state[3])
    claim_row = cursor.execute(
        """
        SELECT support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, complete_group_count,
               complete_group_ids, status, decision_policy_version,
               certificate_digest
        FROM groundloop_m5_published_claim_state
        WHERE claim_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (claim_id, intent.before_epoch_id, intent.before_epoch_id),
    ).fetchone()
    if claim_row is None:
        raise EventConflictError("matching retirement claim state changed")
    old_group_ids = tuple(_text(value) for value in claim_row[7])
    if (group_id in old_group_ids) != group_was_complete:
        raise EventConflictError("matching retirement group completeness index changed")
    if group_was_complete:
        old_claim_status = ClaimStatus(_text(claim_row[8]))
        new_group_ids = tuple(value for value in old_group_ids if value != group_id)
        new_claim_status = _claim_status(
            supported=bool(int(claim_row[0]) or new_group_ids),
            refuted=bool(int(claim_row[1])),
        )
        next_claim = CombinedClaimState(
            claim_id=claim_id,
            support_count=int(claim_row[0]),
            refute_count=int(claim_row[1]),
            best_support_score=(None if claim_row[2] is None else float(claim_row[2])),
            best_refute_score=(None if claim_row[3] is None else float(claim_row[3])),
            supporting_observation_ids=tuple(_text(value) for value in claim_row[4]),
            refuting_observation_ids=tuple(_text(value) for value in claim_row[5]),
            complete_group_count=len(new_group_ids),
            complete_group_ids=new_group_ids,
            status=new_claim_status,
        )
        prior_claim_digest = _sha256_text(claim_row[10])
        prior_claim_artifact = _load_claim_certificate_artifact(
            cursor, prior_claim_digest
        )
        if (
            prior_claim_artifact.claim_id != claim_id
            or _text(claim_row[9]) != intent.decision_policy_version
        ):
            raise EventConflictError("matching retirement claim authority changed")
        next_claim_artifact = _claim_certificate_from_state(
            cursor,
            next_claim,
            decision_policy_version=intent.decision_policy_version,
            before_epoch_id=intent.before_epoch_id,
        )
        before_claim_hash = digests.claim_state_artifact_digest(
            claim_id=claim_id,
            support_count=int(claim_row[0]),
            refute_count=int(claim_row[1]),
            best_support_score=(None if claim_row[2] is None else float(claim_row[2])),
            best_refute_score=(None if claim_row[3] is None else float(claim_row[3])),
            supporting_observation_ids=tuple(_text(value) for value in claim_row[4]),
            refuting_observation_ids=tuple(_text(value) for value in claim_row[5]),
            complete_group_count=int(claim_row[6]),
            complete_group_ids=old_group_ids,
            status=old_claim_status,
            decision_policy_version=_text(claim_row[9]),
            certificate_digest=prior_claim_digest,
        )
        after_claim_hash = digests.claim_state_artifact_digest(
            claim_id=claim_id,
            support_count=next_claim.support_count,
            refute_count=next_claim.refute_count,
            best_support_score=next_claim.best_support_score,
            best_refute_score=next_claim.best_refute_score,
            supporting_observation_ids=next_claim.supporting_observation_ids,
            refuting_observation_ids=next_claim.refuting_observation_ids,
            complete_group_count=next_claim.complete_group_count,
            complete_group_ids=next_claim.complete_group_ids,
            status=next_claim.status,
            decision_policy_version=intent.decision_policy_version,
            certificate_digest=next_claim_artifact.certificate_digest,
        )
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.CLAIM_STATE,
                claim_id,
                before_claim_hash,
                after_claim_hash,
            )
        )
        output_records.append(("claim_state", claim_id, next_claim))
        claim_state_writes.append(
            _ClaimStateWrite(
                next_claim,
                intent.decision_policy_version,
                next_claim_artifact.certificate_digest,
            )
        )
        if new_claim_status is old_claim_status:
            claim_state_only_changes = 1
        if next_claim_artifact.certificate_digest != prior_claim_digest:
            existing_artifact = cursor.execute(
                """
                SELECT 1 FROM groundloop_m5_claim_certificate_artifact
                WHERE certificate_digest = %s
                """,
                (next_claim_artifact.certificate_digest,),
            ).fetchone()
            if existing_artifact is None:
                claim_artifact_writes.append(next_claim_artifact)
            elif (
                _load_claim_certificate_artifact(
                    cursor, next_claim_artifact.certificate_digest
                )
                != next_claim_artifact
            ):
                raise EventConflictError("claim certificate digest collision")
            logical_changes.append(
                M5PersistedLogicalChange(
                    M5PersistedLogicalChangeKind.CLAIM_CERTIFICATE,
                    claim_id,
                    prior_claim_digest,
                    next_claim_artifact.certificate_digest,
                )
            )
            output_records.append(("claim_certificate", claim_id, next_claim_artifact))
            binding = WorkingClaimCertificateBinding(
                intent.resulting_epoch_id,
                claim_id,
                intent.resulting_revision,
                None,
                next_claim_artifact.certificate_digest,
            )
            claim_binding_writes.append(binding)
            persisted_binding_rows.append(
                M5PersistedCertificateBindingRow(
                    M5PersistedBindingKind.CLAIM,
                    binding.epoch_id,
                    binding.claim_id,
                    binding.valid_from_revision,
                    binding.valid_to_revision,
                    binding.certificate_digest,
                    digests.certificate_binding_row_digest(
                        M5PersistedBindingKind.CLAIM,
                        binding.epoch_id,
                        binding.claim_id,
                        binding.valid_from_revision,
                        binding.valid_to_revision,
                        binding.certificate_digest,
                    ),
                )
            )
            output_records.append(("claim_binding", claim_id, binding))

        if new_claim_status is not old_claim_status:
            operation = (
                "ReplaceGroupEvent"
                if successor_shape is not None
                else "RetireGroupEvent"
            )
            reason = f"event={intent.source_id} op={operation}"
            status_deltas.append(
                StatusDelta(
                    intent.source_id,
                    "claim",
                    claim_id,
                    old_claim_status.value,
                    new_claim_status.value,
                    reason,
                )
            )
            if required_claim:
                answer_row = cursor.execute(
                    """
                    SELECT required_claim_count, supported_count,
                           unsupported_count, refuted_count, conflicted_count,
                           status
                    FROM groundloop_m5_published_answer_state
                    WHERE answer_version_id = %s AND valid_from_epoch <= %s
                      AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
                    """,
                    (answer_id, intent.before_epoch_id, intent.before_epoch_id),
                ).fetchone()
                if answer_row is None:
                    raise EventConflictError("matching retirement answer state changed")
                answer_counts: Counter[ClaimStatus] = Counter(
                    {
                        ClaimStatus.SUPPORTED: int(answer_row[1]),
                        ClaimStatus.UNSUPPORTED: int(answer_row[2]),
                        ClaimStatus.REFUTED: int(answer_row[3]),
                        ClaimStatus.CONFLICTED: int(answer_row[4]),
                    }
                )
                answer_counts[old_claim_status] -= 1
                answer_counts[new_claim_status] += 1
                required_count = int(answer_row[0])
                next_answer_status = _answer_status(answer_counts, required_count)
                next_answer = CombinedAnswerState(
                    answer_id,
                    required_count,
                    answer_counts[ClaimStatus.SUPPORTED],
                    answer_counts[ClaimStatus.UNSUPPORTED],
                    answer_counts[ClaimStatus.REFUTED],
                    answer_counts[ClaimStatus.CONFLICTED],
                    next_answer_status,
                )
                old_answer_status = AnswerStatus(_text(answer_row[5]))
                before_answer_hash = digests.answer_state_artifact_digest(
                    answer_version_id=answer_id,
                    required_claim_count=required_count,
                    supported_count=int(answer_row[1]),
                    unsupported_count=int(answer_row[2]),
                    refuted_count=int(answer_row[3]),
                    conflicted_count=int(answer_row[4]),
                    status=old_answer_status,
                )
                after_answer_hash = digests.answer_state_artifact_digest(
                    answer_version_id=answer_id,
                    required_claim_count=required_count,
                    supported_count=next_answer.supported_count,
                    unsupported_count=next_answer.unsupported_count,
                    refuted_count=next_answer.refuted_count,
                    conflicted_count=next_answer.conflicted_count,
                    status=next_answer.status,
                )
                logical_changes.append(
                    M5PersistedLogicalChange(
                        M5PersistedLogicalChangeKind.ANSWER_STATE,
                        answer_id,
                        before_answer_hash,
                        after_answer_hash,
                    )
                )
                output_records.append(("answer_state", answer_id, next_answer))
                answer_state_writes.append(next_answer)
                if next_answer_status is not old_answer_status:
                    status_deltas.append(
                        StatusDelta(
                            intent.source_id,
                            "answer",
                            answer_id,
                            old_answer_status.value,
                            next_answer_status.value,
                            reason,
                        )
                    )

    output_records.extend(
        ("status_delta", delta.object_id, delta) for delta in status_deltas
    )
    logical_changes_tuple = tuple(
        sorted(logical_changes, key=lambda row: (row.kind.value, row.object_id))
    )
    output_rank = {
        "requirement_state": 0,
        "group_state": 1,
        "claim_state": 2,
        "answer_state": 3,
        "group_certificate": 4,
        "claim_certificate": 5,
        "claim_binding": 7,
        "status_delta": 8,
    }
    output_records_tuple = tuple(
        sorted(output_records, key=lambda row: (output_rank[row[0]], row[1]))
    )
    logical_output = digests.logical_output_preimage(output_records_tuple)
    matching_work = (
        MatchingWorkCounters(
            contribution_removals=len(observation_changes),
            requirement_observation_changes_processed=len(observation_changes),
            ordered_index_operations=(3 * len(observation_changes) + len(mask_changes)),
            edge_refcount_keys_updated=len(edge_changes),
            distinct_edge_crossings=crossing_count,
            hash_mask_transitions=len(mask_changes),
            hall_subset_entries_examined=subset_entries,
            hall_neighbor_entries_changed=neighbour_changes,
            hall_deficiency_entries_examined=subset_entries,
            group_local_state_operations=1,
            groups_touched=1 + int(successor_shape is not None),
            claims_touched=int(bool(claim_state_writes)),
            answers_touched=len(answer_state_writes),
            claim_status_changes=sum(
                delta.object_type == "claim" for delta in status_deltas
            ),
            answer_status_changes=sum(
                delta.object_type == "answer" for delta in status_deltas
            ),
            output_bytes=len(logical_output),
        )
        + successor_initialization_work
    )
    work = M5OverlayWork(
        matching=matching_work,
        claim_state_only_changes=claim_state_only_changes,
        public_status_deltas=len(status_deltas),
    )
    artifact = _build_patch_artifact(
        intent,
        observation_changes=tuple(observation_changes),
        edge_changes=tuple(edge_changes),
        mask_changes=tuple(mask_changes),
        hall_changes=tuple(
            sorted(hall_changes, key=lambda change: change.group_version_id)
        ),
        logical_changes=logical_changes_tuple,
        binding_rows=tuple(persisted_binding_rows),
        output_records=output_records_tuple,
        work=work,
    )
    plan = _MatchingWritePlan(
        observation_rows=tuple(observation_after),
        edge_rows=tuple(edge_after),
        mask_rows=tuple(mask_after),
        hall_rows=tuple(sorted(hall_after_rows, key=lambda row: row.group_version_id)),
        requirement_state_rows=tuple(
            sorted(
                requirement_state_writes,
                key=lambda write: write.state.requirement_version_id,
            )
        ),
        group_state_rows=tuple(
            sorted(
                group_state_writes,
                key=lambda write: write.state.group_version_id,
            )
        ),
        claim_state_rows=tuple(claim_state_writes),
        answer_state_rows=tuple(answer_state_writes),
        claim_certificate_artifact_rows=tuple(claim_artifact_writes),
        claim_binding_rows=tuple(claim_binding_writes),
    )
    write_counts = _D24OwnedWriteCounts(
        group_state_write_count=len(group_state_writes),
        claim_state_write_count=len(claim_state_writes),
        answer_state_write_count=len(answer_state_writes),
        certificate_binding_write_count=len(claim_binding_writes),
        public_delta_write_count=len(status_deltas),
    )
    return artifact, plan, write_counts, len(requirement_state_writes)


def _requirement_state_artifact_hash(write: _RequirementStateWrite) -> str:
    state = write.state
    return digests.requirement_state_artifact_digest(
        requirement_version_id=state.requirement_version_id,
        witness_hashes=state.witness_hashes,
        supporting_observation_ids=state.supporting_observation_ids,
        witness_count=state.witness_count,
        satisfied=state.satisfied,
        decision_policy_version=write.decision_policy_version,
    )


def _group_state_artifact_hash(write: _GroupStateWrite) -> str:
    state = write.state
    return digests.group_state_artifact_digest(
        group_version_id=state.group_version_id,
        requirement_count=state.requirement_count,
        satisfied_count=state.satisfied_count,
        matching_size=state.matching_size,
        complete=state.complete,
        decision_policy_version=write.decision_policy_version,
        certificate_digest=write.certificate_digest,
    )


def _claim_state_artifact_hash(write: _ClaimStateWrite) -> str:
    state = write.state
    return digests.claim_state_artifact_digest(
        claim_id=state.claim_id,
        support_count=state.support_count,
        refute_count=state.refute_count,
        best_support_score=state.best_support_score,
        best_refute_score=state.best_refute_score,
        supporting_observation_ids=state.supporting_observation_ids,
        refuting_observation_ids=state.refuting_observation_ids,
        complete_group_count=state.complete_group_count,
        complete_group_ids=state.complete_group_ids,
        status=state.status,
        decision_policy_version=write.decision_policy_version,
        certificate_digest=write.certificate_digest,
    )


def _answer_state_artifact_hash(state: CombinedAnswerState) -> str:
    return digests.answer_state_artifact_digest(
        answer_version_id=state.answer_version_id,
        required_claim_count=state.required_claim_count,
        supported_count=state.supported_count,
        unsupported_count=state.unsupported_count,
        refuted_count=state.refuted_count,
        conflicted_count=state.conflicted_count,
        status=state.status,
    )


def _document_requirement_state(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    requirement_id: str,
) -> _RequirementStateWrite:
    rows = cursor.execute(
        """
        SELECT witness_hashes, supporting_observation_ids, witness_count,
               satisfied, decision_policy_version
        FROM groundloop_m5_published_requirement_state
        WHERE requirement_version_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (requirement_id, intent.before_epoch_id, intent.before_epoch_id),
    ).fetchall()
    if len(rows) != 1 or _text(rows[0][4]) != intent.decision_policy_version:
        raise EventConflictError("document requirement state changed")
    state = RequirementState(
        requirement_id,
        tuple(_sha256_text(value) for value in rows[0][0]),
        tuple(_text(value) for value in rows[0][1]),
        int(rows[0][2]),
        bool(rows[0][3]),
    )
    if state.witness_count != len(state.witness_hashes) or state.satisfied != bool(
        state.witness_hashes
    ):
        raise EventConflictError("document requirement state is inconsistent")
    return _RequirementStateWrite(state, _text(rows[0][4]))


def _document_group_state(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    group_id: str,
) -> _GroupStateWrite:
    rows = cursor.execute(
        """
        SELECT requirement_count, satisfied_count, matching_size, complete,
               decision_policy_version, certificate_digest
        FROM groundloop_m5_published_group_state
        WHERE group_version_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (group_id, intent.before_epoch_id, intent.before_epoch_id),
    ).fetchall()
    if len(rows) != 1 or _text(rows[0][4]) != intent.decision_policy_version:
        raise EventConflictError("document group state changed")
    state = GroupState(
        group_id,
        int(rows[0][0]),
        int(rows[0][1]),
        int(rows[0][2]),
        bool(rows[0][3]),
    )
    digest = None if rows[0][5] is None else _sha256_text(rows[0][5])
    if state.complete != (digest is not None):
        raise EventConflictError("document group certificate pointer changed")
    return _GroupStateWrite(state, _text(rows[0][4]), digest)


def _document_claim_state(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    claim_id: str,
) -> _ClaimStateWrite:
    rows = cursor.execute(
        """
        SELECT support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, complete_group_count,
               complete_group_ids, status, decision_policy_version,
               certificate_digest
        FROM groundloop_m5_published_claim_state
        WHERE claim_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (claim_id, intent.before_epoch_id, intent.before_epoch_id),
    ).fetchall()
    if len(rows) != 1 or _text(rows[0][9]) != intent.decision_policy_version:
        raise EventConflictError("document claim state changed")
    groups = tuple(_text(value) for value in rows[0][7])
    state = CombinedClaimState(
        claim_id,
        int(rows[0][0]),
        int(rows[0][1]),
        None if rows[0][2] is None else float(rows[0][2]),
        None if rows[0][3] is None else float(rows[0][3]),
        tuple(_text(value) for value in rows[0][4]),
        tuple(_text(value) for value in rows[0][5]),
        int(rows[0][6]),
        groups,
        ClaimStatus(_text(rows[0][8])),
    )
    if state.complete_group_count != len(groups) or state.status is not _claim_status(
        supported=bool(state.support_count or groups),
        refuted=bool(state.refute_count),
    ):
        raise EventConflictError("document claim state is inconsistent")
    return _ClaimStateWrite(
        state,
        _text(rows[0][9]),
        _sha256_text(rows[0][10]),
    )


def _document_answer_state(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    answer_id: str,
) -> CombinedAnswerState:
    rows = cursor.execute(
        """
        SELECT required_claim_count, supported_count, unsupported_count,
               refuted_count, conflicted_count, status
        FROM groundloop_m5_published_answer_state
        WHERE answer_version_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (answer_id, intent.before_epoch_id, intent.before_epoch_id),
    ).fetchall()
    if len(rows) != 1:
        raise EventConflictError("document answer state changed")
    state = CombinedAnswerState(
        answer_id,
        int(rows[0][0]),
        int(rows[0][1]),
        int(rows[0][2]),
        int(rows[0][3]),
        int(rows[0][4]),
        AnswerStatus(_text(rows[0][5])),
    )
    counts = Counter(
        {
            ClaimStatus.SUPPORTED: state.supported_count,
            ClaimStatus.UNSUPPORTED: state.unsupported_count,
            ClaimStatus.REFUTED: state.refuted_count,
            ClaimStatus.CONFLICTED: state.conflicted_count,
        }
    )
    if state.status is not _answer_status(counts, state.required_claim_count):
        raise EventConflictError("document answer state is inconsistent")
    return state


def _document_group_binding(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    group_id: str,
) -> _EffectiveGroupBinding | None:
    rows = cursor.execute(
        """
        SELECT certificate_digest, sealed_revision
        FROM groundloop_m5_published_group_certificate_binding
        WHERE group_version_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (group_id, intent.before_epoch_id, intent.before_epoch_id),
    ).fetchall()
    if len(rows) > 1:
        raise EventConflictError("document group binding changed")
    if not rows:
        return None
    return _EffectiveGroupBinding(_sha256_text(rows[0][0]), int(rows[0][1]), False)


def _document_claim_binding(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    claim_id: str,
) -> _EffectiveClaimBinding:
    rows = cursor.execute(
        """
        SELECT certificate_digest, sealed_revision
        FROM groundloop_m5_published_claim_certificate_binding
        WHERE claim_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (claim_id, intent.before_epoch_id, intent.before_epoch_id),
    ).fetchall()
    if len(rows) != 1:
        raise EventConflictError("document claim binding changed")
    return _EffectiveClaimBinding(_sha256_text(rows[0][0]), int(rows[0][1]), False)


def _document_claim_artifact(
    cursor: Cursor[Any],
    *,
    intent: M5PersistedMatchingTransitionIntent,
    state: CombinedClaimState,
    group_artifacts: dict[str, GroupMatchingCertificateArtifact | None],
) -> ClaimCertificateArtifact:
    if state.supporting_observation_ids:
        support_kind = ClaimSupportKind.DIRECT
        direct_support = state.supporting_observation_ids[0]
        group_id = None
        group_digest = None
    elif state.complete_group_ids:
        support_kind = ClaimSupportKind.GROUP
        direct_support = None
        group_id = state.complete_group_ids[0]
        if group_id in group_artifacts:
            artifact = group_artifacts[group_id]
            if artifact is None:
                raise EventConflictError(
                    "document selected complete group lacks its certificate"
                )
            group_digest = artifact.certificate_digest
        else:
            binding = _document_group_binding(cursor, intent, group_id)
            if binding is None:
                raise EventConflictError("document selected group binding disappeared")
            group_digest = binding.certificate_digest
            stored = _stored_group_certificate_artifact(
                cursor, group_digest, lock=False
            )
            if stored is None or (
                stored.group_version_id != group_id
                or stored.decision_policy_version != intent.decision_policy_version
            ):
                raise EventConflictError("document selected group artifact changed")
    else:
        support_kind = ClaimSupportKind.NONE
        direct_support = None
        group_id = None
        group_digest = None
    return ClaimCertificateArtifact(
        claim_id=state.claim_id,
        decision_policy_version=intent.decision_policy_version,
        support_kind=support_kind,
        direct_support_observation_id=direct_support,
        group_version_id=group_id,
        group_certificate_digest=group_digest,
        direct_refute_observation_id=(
            state.refuting_observation_ids[0]
            if state.refuting_observation_ids
            else None
        ),
    )


def _persisted_binding_row(
    kind: M5PersistedBindingKind,
    binding: WorkingGroupCertificateBinding | WorkingClaimCertificateBinding,
) -> M5PersistedCertificateBindingRow:
    object_id = (
        binding.group_version_id
        if isinstance(binding, WorkingGroupCertificateBinding)
        else binding.claim_id
    )
    return M5PersistedCertificateBindingRow(
        kind,
        binding.epoch_id,
        object_id,
        binding.valid_from_revision,
        binding.valid_to_revision,
        binding.certificate_digest,
        digests.certificate_binding_row_digest(
            kind,
            binding.epoch_id,
            object_id,
            binding.valid_from_revision,
            binding.valid_to_revision,
            binding.certificate_digest,
        ),
    )


def _requirement_header_after_images(
    cursor: Cursor[Any], source: _RequirementCompletionSource
) -> tuple[_BaseHeaderAfterImage, _RuntimeHeaderAfterImage]:
    """Derive the complete N-to-N+1 headers while the source is preterminal."""

    base, runtime = _header_images(cursor, source.epoch_id)
    if (
        base.revision != source.expected_runtime_revision
        or runtime.revision != source.expected_runtime_revision
        or base.structural_status != "committed"
        or base.semantic_status != "pending"
        or base.evaluation_state != "pending"
        or base.sealed
        or runtime.runtime_state not in {"structural_committed", "semantic_pending"}
        or runtime.terminal
        or runtime.open_work_count < 1
    ):
        raise EventConflictError("requirement completion header changed")
    counts = cursor.execute(
        """
        SELECT count(*) FILTER (
                   WHERE job_state IN ('declared', 'running', 'retryable_failed')
               ),
               count(*) FILTER (WHERE job_state = 'terminal_failed'),
               (SELECT count(*)
                FROM groundloop_m5_discovery_scope
                WHERE epoch_id = %s
                  AND scope_state IN ('open', 'result_staged'))
        FROM groundloop_m5_semantic_job
        WHERE epoch_id = %s
        """,
        (source.epoch_id, source.epoch_id),
    ).fetchone()
    if counts is None or (
        int(counts[0]) != runtime.open_work_count
        or int(counts[1]) != runtime.blocking_failure_count
        or int(counts[2]) != runtime.open_scope_count
    ):
        raise EventConflictError("requirement completion runtime counts changed")
    return (
        _BaseHeaderAfterImage(
            source.epoch_id,
            source.resulting_revision,
            base.structural_status,
            base.semantic_status,
            base.evaluation_state,
            base.publication_mode,
            False,
            base.open_job_count,
            base.open_scope_count,
        ),
        _RuntimeHeaderAfterImage(
            source.epoch_id,
            source.resulting_revision,
            "semantic_pending",
            runtime.open_work_count - 1,
            runtime.open_scope_count,
            runtime.blocking_failure_count,
            False,
        ),
    )


def _requirement_first_application_plan(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    *,
    headers_installed: bool = False,
) -> tuple[
    M5PersistedMatchingPatchArtifact,
    _MatchingWritePlan,
    _D24OwnedWriteCounts,
    int,
    _ObservationCurrencyBeforeImage,
    _RequirementCompletionSource,
]:
    """Recompute a requirement completion solely from its held official keys."""

    if intent.source_kind is not M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION:
        raise InvalidEventError("requirement planner received another source kind")
    if (
        intent.before_epoch_id != intent.resulting_epoch_id
        or intent.resulting_revision != intent.before_revision + 1
    ):
        raise EventConflictError("requirement matching intent point changed")
    source = _requirement_completion_source(
        cursor,
        epoch_id=intent.resulting_epoch_id,
        expected_runtime_revision=intent.before_revision,
        resulting_revision=intent.resulting_revision,
        proposed_attempt_id=intent.source_id,
        expected_source_identity_hash=intent.source_identity_hash,
        headers_installed=headers_installed,
    )
    if source.decision_policy_version != intent.decision_policy_version:
        raise EventConflictError("requirement matching policy changed")
    projection = _requirement_currency_projection(cursor, source)
    currency_before = _capture_requirement_currency_before_image(
        cursor, source, projection, lock=False
    )
    preview = _requirement_preview_from_locked_intent(
        cursor,
        source=source,
        projection=projection,
        intent=intent,
    )

    requirement_before = _effective_requirement_state(cursor, source, lock=False)
    group_before = _effective_group_state(cursor, source, lock=False)
    claim_before = _effective_claim_state(cursor, source, lock=False)
    answer_before = _effective_answer_state(cursor, source, lock=False)
    group_bindings = {
        group_id: _effective_group_binding(cursor, source, group_id, lock=False)
        for group_id in intent.group_certificate_ids
    }
    source_group_binding = group_bindings.get(source.group_version_id)
    source_group_digest = (
        None
        if source_group_binding is None
        else source_group_binding.certificate_digest
    )
    if source_group_digest != group_before.certificate_digest:
        raise EventConflictError("requirement group binding/state changed")
    claim_binding = _effective_claim_binding(cursor, source, lock=False)
    if claim_binding is None or (
        claim_binding.certificate_digest != claim_before.certificate_digest
    ):
        raise EventConflictError("requirement claim binding/state changed")
    transition = _requirement_logical_transition(
        source=source,
        preview=preview,
        requirement_before=requirement_before,
        group_before=group_before,
        claim_before=claim_before,
        answer_before=answer_before,
        group_bindings=group_bindings,
    )

    if projection.base_observation_id == source.observation_id:
        raise EventConflictError("requirement currency replacement is unchanged")
    currency_rows: list[_ObservationCurrencyWrite] = []
    if currency_before.working_currency_row is not None:
        prior_currency = currency_before.working_currency_row
        prior_from = int(_text(prior_currency[6]))
        if prior_from >= source.resulting_revision or prior_currency[7] is not None:
            raise EventConflictError("requirement working currency changed")
        currency_rows.append(
            _ObservationCurrencyWrite(
                epoch_id=source.epoch_id,
                subject_id=source.requirement_version_id,
                chunk_version_id=source.chunk_version_id,
                observation_id=(
                    None if prior_currency[5] is None else _text(prior_currency[5])
                ),
                valid_from_revision=prior_from,
                valid_to_revision=source.resulting_revision,
            )
        )
    currency_rows.append(
        _ObservationCurrencyWrite(
            epoch_id=source.epoch_id,
            subject_id=source.requirement_version_id,
            chunk_version_id=source.chunk_version_id,
            observation_id=source.observation_id,
            valid_from_revision=source.resulting_revision,
            valid_to_revision=None,
        )
    )

    old_support = _requirement_label_supports(projection.base_operational_label)
    new_support = _requirement_label_supports(source.operational_label)
    old_observation_id = projection.base_observation_id
    observation_before = {
        point.observation_id: point for point in preview.observation_before
    }
    observation_rows: list[M5MatchingObservationWorking] = []
    observation_changes: list[M5MatchingObservationChange] = []
    if old_support:
        assert old_observation_id is not None
        old_after = M5MatchingObservationWorking(
            M5MatchingLayer.WORKING,
            source.epoch_id,
            old_observation_id,
            source.requirement_version_id,
            source.group_version_id,
            source.requirement_ordinal,
            source.text_hash,
            False,
            source.resulting_revision,
        )
        old_before = observation_before[old_observation_id]
        observation_rows.append(old_after)
        observation_changes.append(
            M5MatchingObservationChange(
                old_observation_id,
                old_before,
                old_after,
                digests.matching_change_digest(
                    "m5-persisted-matching-observation-change-v1",
                    (text_field(old_observation_id),),
                    old_before,
                    old_after,
                ),
            )
        )
    if new_support:
        new_after = M5MatchingObservationWorking(
            M5MatchingLayer.WORKING,
            source.epoch_id,
            source.observation_id,
            source.requirement_version_id,
            source.group_version_id,
            source.requirement_ordinal,
            source.text_hash,
            True,
            source.resulting_revision,
        )
        observation_rows.append(new_after)
        observation_changes.append(
            M5MatchingObservationChange(
                source.observation_id,
                None,
                new_after,
                digests.matching_change_digest(
                    "m5-persisted-matching-observation-change-v1",
                    (text_field(source.observation_id),),
                    None,
                    new_after,
                ),
            )
        )

    edge_rows: list[M5MatchingEdgeWorking] = []
    edge_changes: list[M5MatchingEdgeChange] = []
    old_refcount = 0 if preview.edge_before is None else preview.edge_before.refcount
    if old_refcount != preview.edge_after_refcount:
        edge_after = M5MatchingEdgeWorking(
            M5MatchingLayer.WORKING,
            source.epoch_id,
            source.requirement_version_id,
            source.text_hash,
            source.group_version_id,
            source.requirement_ordinal,
            preview.edge_after_refcount,
            source.resulting_revision,
        )
        edge_rows.append(edge_after)
        edge_changes.append(
            M5MatchingEdgeChange(
                source.requirement_version_id,
                source.text_hash,
                preview.edge_before,
                edge_after,
                digests.matching_change_digest(
                    "m5-persisted-matching-edge-change-v1",
                    (
                        text_field(source.requirement_version_id),
                        hash_field(source.text_hash),
                    ),
                    preview.edge_before,
                    edge_after,
                ),
            )
        )

    mask_rows: list[M5MatchingMaskWorking] = []
    mask_changes: list[M5MatchingMaskChange] = []
    old_mask = 0 if preview.mask_before is None else preview.mask_before.mask
    if old_mask != preview.mask_after:
        mask_after = M5MatchingMaskWorking(
            M5MatchingLayer.WORKING,
            source.epoch_id,
            source.group_version_id,
            source.text_hash,
            preview.mask_after,
            source.resulting_revision,
        )
        mask_rows.append(mask_after)
        mask_changes.append(
            M5MatchingMaskChange(
                source.group_version_id,
                source.text_hash,
                preview.mask_before,
                mask_after,
                digests.matching_change_digest(
                    "m5-persisted-matching-mask-change-v1",
                    (
                        text_field(source.group_version_id),
                        hash_field(source.text_hash),
                    ),
                    preview.mask_before,
                    mask_after,
                ),
            )
        )

    hall_rows: list[M5MatchingHallWorking] = []
    hall_changes: list[M5MatchingHallChange] = []
    before_hall = _hall_state_from_point(preview.hall_before)
    if before_hall != preview.hall_after:
        hall_after = M5MatchingHallWorking(
            M5MatchingLayer.WORKING,
            source.epoch_id,
            source.group_version_id,
            True,
            preview.hall_after.requirement_count,
            preview.hall_after.mask_histogram,
            preview.hall_after.neighbor_counts,
            preview.hall_after.deficiencies,
            preview.hall_after.maximum_deficiency,
            preview.hall_after.matching_size,
            preview.hall_after.distinct_hash_count,
            source.resulting_revision,
        )
        hall_rows.append(hall_after)
        hall_changes.append(
            M5MatchingHallChange(
                source.group_version_id,
                preview.hall_before,
                hall_after,
                digests.matching_change_digest(
                    "m5-persisted-matching-hall-change-v1",
                    (text_field(source.group_version_id),),
                    preview.hall_before,
                    hall_after,
                ),
            )
        )

    logical_changes: list[M5PersistedLogicalChange] = []
    output_records: list[tuple[str, str, object]] = []
    requirement_rows: list[_RequirementStateWrite] = []
    group_state_rows: list[_GroupStateWrite] = []
    claim_state_rows: list[_ClaimStateWrite] = []
    answer_state_rows: list[CombinedAnswerState] = []
    group_artifact_rows: list[GroupMatchingCertificateArtifact] = []
    claim_artifact_rows: list[ClaimCertificateArtifact] = []
    group_binding_rows: list[WorkingGroupCertificateBinding] = []
    claim_binding_rows: list[WorkingClaimCertificateBinding] = []
    persisted_binding_rows: list[M5PersistedCertificateBindingRow] = []

    if transition.requirement_after != transition.requirement_before:
        requirement_rows.append(transition.requirement_after)
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.REQUIREMENT_STATE,
                source.requirement_version_id,
                _requirement_state_artifact_hash(transition.requirement_before),
                _requirement_state_artifact_hash(transition.requirement_after),
            )
        )
        output_records.append(
            (
                "requirement_state",
                source.requirement_version_id,
                transition.requirement_after.state,
            )
        )
    if transition.group_after != transition.group_before:
        group_state_rows.append(transition.group_after)
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.GROUP_STATE,
                source.group_version_id,
                _group_state_artifact_hash(transition.group_before),
                _group_state_artifact_hash(transition.group_after),
            )
        )
        output_records.append(
            ("group_state", source.group_version_id, transition.group_after.state)
        )
    if transition.claim_after != transition.claim_before:
        claim_state_rows.append(transition.claim_after)
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.CLAIM_STATE,
                source.owner_claim_id,
                _claim_state_artifact_hash(transition.claim_before),
                _claim_state_artifact_hash(transition.claim_after),
            )
        )
        output_records.append(
            ("claim_state", source.owner_claim_id, transition.claim_after.state)
        )
    if transition.answer_after != transition.answer_before:
        answer_state_rows.append(transition.answer_after)
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.ANSWER_STATE,
                source.answer_version_id,
                _answer_state_artifact_hash(transition.answer_before),
                _answer_state_artifact_hash(transition.answer_after),
            )
        )
        output_records.append(
            ("answer_state", source.answer_version_id, transition.answer_after)
        )

    group_before_digest = transition.group_before.certificate_digest
    group_after_digest = transition.group_after.certificate_digest
    if group_before_digest != group_after_digest:
        group_artifact = preview.certificate_artifact
        if (group_artifact is None) != (group_after_digest is None):
            raise EventConflictError("requirement group certificate changed")
        if group_artifact is not None:
            stored_group_artifact = _stored_group_certificate_artifact(
                cursor, group_artifact.certificate_digest, lock=False
            )
            if stored_group_artifact is None:
                group_artifact_rows.append(group_artifact)
            elif stored_group_artifact != group_artifact:
                raise EventConflictError("matching group certificate digest collision")
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.GROUP_CERTIFICATE,
                source.group_version_id,
                group_before_digest,
                group_after_digest,
            )
        )
        output_records.append(
            ("group_certificate", source.group_version_id, group_artifact)
        )
        if source_group_binding is not None and source_group_binding.working:
            closed_group_binding = WorkingGroupCertificateBinding(
                source.epoch_id,
                source.group_version_id,
                source_group_binding.valid_from_revision,
                source.resulting_revision,
                source_group_binding.certificate_digest,
            )
            group_binding_rows.append(closed_group_binding)
            persisted_binding_rows.append(
                _persisted_binding_row(
                    M5PersistedBindingKind.GROUP, closed_group_binding
                )
            )
            output_records.append(
                ("group_binding", source.group_version_id, closed_group_binding)
            )
        if group_after_digest is not None:
            opened_group_binding = WorkingGroupCertificateBinding(
                source.epoch_id,
                source.group_version_id,
                source.resulting_revision,
                None,
                group_after_digest,
            )
            group_binding_rows.append(opened_group_binding)
            persisted_binding_rows.append(
                _persisted_binding_row(
                    M5PersistedBindingKind.GROUP, opened_group_binding
                )
            )
            output_records.append(
                ("group_binding", source.group_version_id, opened_group_binding)
            )

    claim_before_digest = transition.claim_before.certificate_digest
    claim_after_digest = transition.claim_after.certificate_digest
    if claim_before_digest != claim_after_digest:
        claim_artifact = transition.claim_artifact
        if claim_artifact.certificate_digest != claim_after_digest:
            raise EventConflictError("requirement claim certificate changed")
        stored_claim_artifact = _stored_claim_certificate_artifact(
            cursor, claim_artifact.certificate_digest, lock=False
        )
        if stored_claim_artifact is None:
            claim_artifact_rows.append(claim_artifact)
        elif stored_claim_artifact != claim_artifact:
            raise EventConflictError("matching claim certificate digest collision")
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.CLAIM_CERTIFICATE,
                source.owner_claim_id,
                claim_before_digest,
                claim_after_digest,
            )
        )
        output_records.append(
            ("claim_certificate", source.owner_claim_id, claim_artifact)
        )
        if claim_binding.working:
            closed_claim_binding = WorkingClaimCertificateBinding(
                source.epoch_id,
                source.owner_claim_id,
                claim_binding.valid_from_revision,
                source.resulting_revision,
                claim_binding.certificate_digest,
            )
            claim_binding_rows.append(closed_claim_binding)
            persisted_binding_rows.append(
                _persisted_binding_row(
                    M5PersistedBindingKind.CLAIM, closed_claim_binding
                )
            )
            output_records.append(
                ("claim_binding", source.owner_claim_id, closed_claim_binding)
            )
        opened_claim_binding = WorkingClaimCertificateBinding(
            source.epoch_id,
            source.owner_claim_id,
            source.resulting_revision,
            None,
            claim_after_digest,
        )
        claim_binding_rows.append(opened_claim_binding)
        persisted_binding_rows.append(
            _persisted_binding_row(M5PersistedBindingKind.CLAIM, opened_claim_binding)
        )
        output_records.append(
            ("claim_binding", source.owner_claim_id, opened_claim_binding)
        )
    elif (
        transition.claim_after != transition.claim_before and not claim_binding.working
    ):
        opened_claim_binding = WorkingClaimCertificateBinding(
            source.epoch_id,
            source.owner_claim_id,
            source.resulting_revision,
            None,
            claim_after_digest,
        )
        claim_binding_rows.append(opened_claim_binding)
        persisted_binding_rows.append(
            _persisted_binding_row(M5PersistedBindingKind.CLAIM, opened_claim_binding)
        )
        output_records.append(
            ("claim_binding", source.owner_claim_id, opened_claim_binding)
        )

    output_records.extend(
        ("status_delta", delta.object_id, delta) for delta in transition.status_deltas
    )
    output_rank = {
        "requirement_state": 0,
        "group_state": 1,
        "claim_state": 2,
        "answer_state": 3,
        "group_certificate": 4,
        "claim_certificate": 5,
        "group_binding": 6,
        "claim_binding": 7,
        "status_delta": 8,
    }
    output_records_tuple = tuple(
        sorted(
            output_records,
            key=lambda row: (
                output_rank[row[0]],
                row[1],
                getattr(row[2], "valid_from_revision", -1),
            ),
        )
    )
    logical_changes_tuple = tuple(
        sorted(logical_changes, key=lambda row: (row.kind.value, row.object_id))
    )
    logical_output = digests.logical_output_preimage(output_records_tuple)

    membership_count = int(old_support) + int(new_support)
    bucket_operations = (
        int(old_mask > 0) + int(preview.mask_after > 0)
        if old_mask != preview.mask_after
        else 0
    )
    matching_work = (
        MatchingWorkCounters(
            contribution_additions=int(new_support),
            contribution_removals=int(old_support),
            requirement_observation_changes_processed=(
                int(projection.base_observation_id is not None) + 1
            ),
            ordered_index_operations=(
                2 * (int(projection.base_observation_id is not None) + 1)
                + membership_count
                + bucket_operations
            ),
            edge_refcount_keys_updated=int(bool(membership_count)),
        )
        + preview.hall_work
    )
    if group_before_digest != group_after_digest and group_after_digest is not None:
        reconstruction = reconstruct_certificate(preview.certificate_view)
        if reconstruction.artifact != preview.certificate_artifact:
            raise EventConflictError("requirement certificate reconstruction changed")
        matching_work += reconstruction.work
    matching_work += MatchingWorkCounters(
        group_local_state_operations=len(
            {row.group_version_id for row in group_binding_rows}
        )
    )
    group_state_changed = transition.group_after != transition.group_before
    claim_state_changed = transition.claim_after != transition.claim_before
    answer_state_changed = transition.answer_after != transition.answer_before
    group_certificate_changed = group_before_digest != group_after_digest
    claim_certificate_changed = claim_before_digest != claim_after_digest
    matching_work += touched_state_work(
        groups_touched=int(group_state_changed or group_certificate_changed),
        claims_touched=int(claim_state_changed or claim_certificate_changed),
        answers_touched=int(answer_state_changed),
        claim_status_changes=sum(
            delta.object_type == "claim" for delta in transition.status_deltas
        ),
        answer_status_changes=sum(
            delta.object_type == "answer" for delta in transition.status_deltas
        ),
        output_bytes=len(logical_output),
    )
    work = M5OverlayWork(
        matching=matching_work,
        requirement_state_only_changes=int(
            transition.requirement_after != transition.requirement_before
            and transition.requirement_after.state.satisfied
            == transition.requirement_before.state.satisfied
        ),
        group_state_only_changes=int(
            group_state_changed
            and transition.group_after.state.complete
            == transition.group_before.state.complete
        ),
        claim_state_only_changes=int(
            claim_state_changed
            and transition.claim_after.state.status
            is transition.claim_before.state.status
        ),
        group_certificate_only_changes=int(
            group_certificate_changed and not group_state_changed
        ),
        claim_certificate_only_changes=int(
            claim_certificate_changed and not claim_state_changed
        ),
        public_status_deltas=len(transition.status_deltas),
    )
    work.assert_nonnegative()
    patch_artifact = _build_patch_artifact(
        intent,
        observation_changes=tuple(
            sorted(observation_changes, key=lambda row: row.observation_id)
        ),
        edge_changes=tuple(edge_changes),
        mask_changes=tuple(mask_changes),
        hall_changes=tuple(hall_changes),
        logical_changes=logical_changes_tuple,
        binding_rows=tuple(persisted_binding_rows),
        output_records=output_records_tuple,
        work=work,
    )
    plan = _MatchingWritePlan(
        observation_currency_rows=tuple(currency_rows),
        observation_rows=tuple(
            sorted(observation_rows, key=lambda row: row.observation_id)
        ),
        edge_rows=tuple(edge_rows),
        mask_rows=tuple(mask_rows),
        hall_rows=tuple(hall_rows),
        requirement_state_rows=tuple(requirement_rows),
        group_state_rows=tuple(group_state_rows),
        claim_state_rows=tuple(claim_state_rows),
        answer_state_rows=tuple(answer_state_rows),
        group_certificate_artifact_rows=tuple(group_artifact_rows),
        claim_certificate_artifact_rows=tuple(claim_artifact_rows),
        group_binding_rows=tuple(group_binding_rows),
        claim_binding_rows=tuple(claim_binding_rows),
    )
    write_counts = _D24OwnedWriteCounts(
        group_state_write_count=len(group_state_rows),
        claim_state_write_count=len(claim_state_rows),
        answer_state_write_count=len(answer_state_rows),
        certificate_binding_write_count=(
            len(group_binding_rows) + len(claim_binding_rows)
        ),
        public_delta_write_count=len(transition.status_deltas),
    )
    return (
        patch_artifact,
        plan,
        write_counts,
        len(requirement_rows),
        currency_before,
        source,
    )


def _document_first_application_plan(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    *,
    update_kind: str,
    document_direct_plan: _DocumentDirectPlan | None = None,
) -> tuple[
    M5PersistedMatchingPatchArtifact,
    _MatchingWritePlan,
    _D24OwnedWriteCounts,
    int,
]:
    """Materialize one coalesced document withdrawal from held lock-plan keys."""

    preview = _document_withdrawal_preview(
        cursor,
        epoch_id=intent.resulting_epoch_id,
        before_epoch_id=intent.before_epoch_id,
        before_revision=intent.before_revision,
        resulting_revision=intent.resulting_revision,
        decision_policy_version=intent.decision_policy_version,
        source_id=intent.source_id,
        update_kind=update_kind,
        reserve_currency_targets=False,
        lock_image_before_discovery=False,
        intent=intent,
    )
    direct_plan = (
        _DocumentDirectPlan()
        if document_direct_plan is None
        else _document_direct_plan_from_database(
            cursor,
            epoch_id=intent.resulting_epoch_id,
            before_epoch_id=intent.before_epoch_id,
            source_id=intent.source_id,
            update_kind=update_kind,
            decision_policy_version=intent.decision_policy_version,
            candidate_claim_ids=tuple(
                claim.claim_id for claim in document_direct_plan.claims
            ),
            expected_plan=document_direct_plan,
        )
    )
    projected = _document_affected_projection(
        cursor,
        before_epoch_id=intent.before_epoch_id,
        preview=preview,
        direct_claim_ids=tuple(claim.claim_id for claim in direct_plan.claims),
    )
    official = {
        "group_shapes": intent.group_shapes,
        "observation_ids": intent.observation_ids,
        "edge_keys": intent.edge_keys,
        "mask_keys": intent.mask_keys,
        "hall_group_ids": intent.hall_group_ids,
        "requirement_state_ids": intent.requirement_state_ids,
        "group_state_ids": intent.group_state_ids,
        "claim_state_ids": intent.claim_state_ids,
        "answer_state_ids": intent.answer_state_ids,
        "group_certificate_ids": intent.group_certificate_ids,
        "claim_certificate_ids": intent.claim_certificate_ids,
    }
    if projected != official:
        raise EventConflictError("document matching intent projection changed")

    currency_rows = tuple(
        _ObservationCurrencyWrite(
            epoch_id=intent.resulting_epoch_id,
            subject_id=before.key[2],
            chunk_version_id=before.key[3],
            task_type=before.key[4],
            observation_id=None,
            valid_from_revision=intent.resulting_revision,
            valid_to_revision=None,
        )
        for before in preview.currency_before_images
    )
    observation_rows: list[M5MatchingObservationWorking] = []
    observation_changes: list[M5MatchingObservationChange] = []
    for observation_before in preview.removed_observations:
        observation_after = M5MatchingObservationWorking(
            M5MatchingLayer.WORKING,
            intent.resulting_epoch_id,
            observation_before.observation_id,
            observation_before.requirement_version_id,
            observation_before.group_version_id,
            observation_before.requirement_ordinal,
            observation_before.text_hash,
            False,
            intent.resulting_revision,
        )
        observation_rows.append(observation_after)
        observation_changes.append(
            M5MatchingObservationChange(
                observation_before.observation_id,
                observation_before,
                observation_after,
                digests.matching_change_digest(
                    "m5-persisted-matching-observation-change-v1",
                    (text_field(observation_before.observation_id),),
                    observation_before,
                    observation_after,
                ),
            )
        )
    edge_rows: list[M5MatchingEdgeWorking] = []
    edge_changes: list[M5MatchingEdgeChange] = []
    for edge_transition in preview.edge_transitions:
        edge_before = edge_transition.before
        edge_after_row = M5MatchingEdgeWorking(
            M5MatchingLayer.WORKING,
            intent.resulting_epoch_id,
            edge_before.requirement_version_id,
            edge_before.text_hash,
            edge_before.group_version_id,
            edge_before.requirement_ordinal,
            edge_transition.after_refcount,
            intent.resulting_revision,
        )
        edge_rows.append(edge_after_row)
        edge_changes.append(
            M5MatchingEdgeChange(
                edge_before.requirement_version_id,
                edge_before.text_hash,
                edge_before,
                edge_after_row,
                digests.matching_change_digest(
                    "m5-persisted-matching-edge-change-v1",
                    (
                        text_field(edge_before.requirement_version_id),
                        hash_field(edge_before.text_hash),
                    ),
                    edge_before,
                    edge_after_row,
                ),
            )
        )
    mask_rows: list[M5MatchingMaskWorking] = []
    mask_changes: list[M5MatchingMaskChange] = []
    for mask_transition in preview.mask_transitions:
        mask_before = mask_transition.before
        if mask_before.mask == mask_transition.after_mask:
            continue
        mask_after = M5MatchingMaskWorking(
            M5MatchingLayer.WORKING,
            intent.resulting_epoch_id,
            mask_before.group_version_id,
            mask_before.text_hash,
            mask_transition.after_mask,
            intent.resulting_revision,
        )
        mask_rows.append(mask_after)
        mask_changes.append(
            M5MatchingMaskChange(
                mask_before.group_version_id,
                mask_before.text_hash,
                mask_before,
                mask_after,
                digests.matching_change_digest(
                    "m5-persisted-matching-mask-change-v1",
                    (
                        text_field(mask_before.group_version_id),
                        hash_field(mask_before.text_hash),
                    ),
                    mask_before,
                    mask_after,
                ),
            )
        )
    hall_rows: list[M5MatchingHallWorking] = []
    hall_changes: list[M5MatchingHallChange] = []
    for group in preview.groups:
        if _hall_state_from_point(group.hall_before) == group.hall_after:
            continue
        hall_after = M5MatchingHallWorking(
            M5MatchingLayer.WORKING,
            intent.resulting_epoch_id,
            group.shape.group_version_id,
            True,
            group.hall_after.requirement_count,
            group.hall_after.mask_histogram,
            group.hall_after.neighbor_counts,
            group.hall_after.deficiencies,
            group.hall_after.maximum_deficiency,
            group.hall_after.matching_size,
            group.hall_after.distinct_hash_count,
            intent.resulting_revision,
        )
        hall_rows.append(hall_after)
        hall_changes.append(
            M5MatchingHallChange(
                group.shape.group_version_id,
                group.hall_before,
                hall_after,
                digests.matching_change_digest(
                    "m5-persisted-matching-hall-change-v1",
                    (text_field(group.shape.group_version_id),),
                    group.hall_before,
                    hall_after,
                ),
            )
        )

    edge_after = {
        (row.before.requirement_version_id, row.before.text_hash): row.after_refcount
        for row in preview.edge_transitions
    }
    removed_by_requirement: dict[str, list[M5MatchingObservationCurrent]] = {}
    for row in preview.removed_observations:
        removed_by_requirement.setdefault(row.requirement_version_id, []).append(row)
    requirement_before: dict[str, _RequirementStateWrite] = {}
    requirement_after: dict[str, _RequirementStateWrite] = {}
    for requirement_id in intent.requirement_state_ids:
        requirement_prior = _document_requirement_state(cursor, intent, requirement_id)
        hashes = set(requirement_prior.state.witness_hashes)
        observations = set(requirement_prior.state.supporting_observation_ids)
        for removed in removed_by_requirement.get(requirement_id, ()):
            if removed.observation_id not in observations:
                raise EventConflictError(
                    "document requirement support provenance changed"
                )
            observations.remove(removed.observation_id)
            if edge_after[(requirement_id, removed.text_hash)] == 0:
                hashes.discard(removed.text_hash)
        for text_hash in requirement_prior.state.witness_hashes:
            changed = edge_after.get((requirement_id, text_hash))
            if changed is not None and (changed > 0) != (text_hash in hashes):
                raise EventConflictError(
                    "document requirement edge/state transition changed"
                )
        requirement_result_state = RequirementState(
            requirement_id,
            tuple(sorted(hashes)),
            tuple(sorted(observations)),
            len(hashes),
            bool(hashes),
        )
        requirement_before[requirement_id] = requirement_prior
        requirement_after[requirement_id] = _RequirementStateWrite(
            requirement_result_state, intent.decision_policy_version
        )

    group_before: dict[str, _GroupStateWrite] = {}
    group_after: dict[str, _GroupStateWrite] = {}
    group_artifacts: dict[str, GroupMatchingCertificateArtifact | None] = {}
    group_bindings = {
        group_id: _document_group_binding(cursor, intent, group_id)
        for group_id in intent.group_certificate_ids
    }
    for group in preview.groups:
        group_id = group.shape.group_version_id
        group_prior = _document_group_state(cursor, intent, group_id)
        group_binding = group_bindings.get(group_id)
        if (None if group_binding is None else group_binding.certificate_digest) != (
            group_prior.certificate_digest
        ):
            raise EventConflictError("document group binding/state changed")
        old_hall = _hall_state_from_point(group.hall_before)
        if (
            group_prior.state.requirement_count != group.shape.requirement_count
            or group_prior.state.matching_size != old_hall.matching_size
            or group_prior.state.complete != old_hall.complete
        ):
            raise EventConflictError("document group logical/Hall state changed")
        satisfied_count = group_prior.state.satisfied_count
        for _, requirement_id in group.shape.requirements:
            if requirement_id not in requirement_before:
                continue
            if (
                requirement_before[requirement_id].state.satisfied
                != requirement_after[requirement_id].state.satisfied
            ):
                satisfied_count += (
                    1 if requirement_after[requirement_id].state.satisfied else -1
                )
        group_certificate = group.certificate_artifact
        if group.hall_after.complete != (group_certificate is not None):
            raise EventConflictError(
                "document group certificate reconstruction changed"
            )
        group_result_state = GroupState(
            group_id,
            group.shape.requirement_count,
            satisfied_count,
            group.hall_after.matching_size,
            group.hall_after.complete,
        )
        group_before[group_id] = group_prior
        group_after[group_id] = _GroupStateWrite(
            group_result_state,
            intent.decision_policy_version,
            (
                None
                if group_certificate is None
                else group_certificate.certificate_digest
            ),
        )
        group_artifacts[group_id] = group_certificate

    owner_rows = cursor.execute(
        """
        SELECT validity.group_version_id, validity.claim_id,
               claim.answer_version_id, claim.required
        FROM groundloop_m5_group_validity AS validity
        JOIN groundloop_claim AS claim USING (claim_id)
        WHERE validity.group_version_id = ANY(%s)
          AND validity.valid_from_epoch <= %s
          AND (validity.valid_to_epoch IS NULL OR %s < validity.valid_to_epoch)
        ORDER BY validity.group_version_id COLLATE "C"
        """,
        (
            list(intent.group_state_ids),
            intent.before_epoch_id,
            intent.before_epoch_id,
        ),
    ).fetchall()
    owners = {
        _text(row[0]): (_text(row[1]), _text(row[2]), bool(row[3]))
        for row in owner_rows
    }
    if set(owners) != set(intent.group_state_ids):
        raise EventConflictError("document group ownership changed")
    direct_claims = {claim.claim_id: claim for claim in direct_plan.claims}
    affected_claim_owners: dict[str, tuple[str, bool]] = {}
    for owner_claim_id, owner_answer_id, owner_required in owners.values():
        existing_owner = affected_claim_owners.setdefault(
            owner_claim_id, (owner_answer_id, owner_required)
        )
        if existing_owner != (owner_answer_id, owner_required):
            raise EventConflictError("document claim ownership changed")
    for direct_claim in direct_plan.claims:
        existing_owner = affected_claim_owners.setdefault(
            direct_claim.claim_id,
            (direct_claim.answer_version_id, direct_claim.required),
        )
        if existing_owner != (
            direct_claim.answer_version_id,
            direct_claim.required,
        ):
            raise EventConflictError("document direct claim ownership changed")
    if set(affected_claim_owners) != set(intent.claim_state_ids) or {
        value[0] for value in affected_claim_owners.values()
    } != set(intent.answer_state_ids):
        raise EventConflictError("document affected owner projection changed")
    claim_before: dict[str, _ClaimStateWrite] = {}
    claim_after: dict[str, _ClaimStateWrite] = {}
    claim_artifacts: dict[str, ClaimCertificateArtifact] = {}
    claim_bindings = {
        claim_id: _document_claim_binding(cursor, intent, claim_id)
        for claim_id in intent.claim_certificate_ids
    }
    for claim_id in intent.claim_state_ids:
        claim_prior = _document_claim_state(cursor, intent, claim_id)
        claim_binding = claim_bindings.get(claim_id)
        if (
            claim_binding is None
            or claim_binding.certificate_digest != claim_prior.certificate_digest
        ):
            raise EventConflictError("document claim binding/state changed")
        direct_claim_plan = direct_claims.get(claim_id)
        if direct_claim_plan is not None and (
            claim_prior.state.support_count
            != direct_claim_plan.before_state.support_count
            or claim_prior.state.refute_count
            != direct_claim_plan.before_state.refute_count
            or claim_prior.state.best_support_score
            != direct_claim_plan.before_state.best_support_score
            or claim_prior.state.best_refute_score
            != direct_claim_plan.before_state.best_refute_score
            or claim_prior.state.supporting_observation_ids
            != direct_claim_plan.before_state.supporting_observation_ids
            or claim_prior.state.refuting_observation_ids
            != direct_claim_plan.before_state.refuting_observation_ids
        ):
            raise EventConflictError("document direct and combined claim states differ")
        complete_groups = set(claim_prior.state.complete_group_ids)
        for group_id, (owner_claim_id, _, _) in owners.items():
            if owner_claim_id != claim_id:
                continue
            if (group_id in complete_groups) != group_before[group_id].state.complete:
                raise EventConflictError(
                    "document claim/group completeness index changed"
                )
            if group_after[group_id].state.complete:
                complete_groups.add(group_id)
            else:
                complete_groups.discard(group_id)
        complete_group_ids = tuple(sorted(complete_groups))
        direct_after_state = (
            claim_prior.state
            if direct_claim_plan is None
            else direct_claim_plan.after_state
        )
        claim_result_status = _claim_status(
            supported=bool(direct_after_state.support_count or complete_group_ids),
            refuted=bool(direct_after_state.refute_count),
        )
        claim_result_state = CombinedClaimState(
            claim_id,
            direct_after_state.support_count,
            direct_after_state.refute_count,
            direct_after_state.best_support_score,
            direct_after_state.best_refute_score,
            direct_after_state.supporting_observation_ids,
            direct_after_state.refuting_observation_ids,
            len(complete_group_ids),
            complete_group_ids,
            claim_result_status,
        )
        claim_artifact = _document_claim_artifact(
            cursor,
            intent=intent,
            state=claim_result_state,
            group_artifacts=group_artifacts,
        )
        claim_before[claim_id] = claim_prior
        claim_after[claim_id] = _ClaimStateWrite(
            claim_result_state,
            intent.decision_policy_version,
            claim_artifact.certificate_digest,
        )
        claim_artifacts[claim_id] = claim_artifact

    answer_before = {
        answer_id: _document_answer_state(cursor, intent, answer_id)
        for answer_id in intent.answer_state_ids
    }
    answer_after: dict[str, CombinedAnswerState] = {}
    for answer_id, answer_prior in answer_before.items():
        status_counts: Counter[ClaimStatus] = Counter(
            {
                ClaimStatus.SUPPORTED: answer_prior.supported_count,
                ClaimStatus.UNSUPPORTED: answer_prior.unsupported_count,
                ClaimStatus.REFUTED: answer_prior.refuted_count,
                ClaimStatus.CONFLICTED: answer_prior.conflicted_count,
            }
        )
        for claim_id, (owner_answer_id, required) in sorted(
            affected_claim_owners.items()
        ):
            if owner_answer_id != answer_id or not required:
                continue
            old_status = claim_before[claim_id].state.status
            new_status = claim_after[claim_id].state.status
            if old_status is not new_status:
                status_counts[old_status] -= 1
                status_counts[new_status] += 1
        answer_result_status = _answer_status(
            status_counts, answer_prior.required_claim_count
        )
        answer_after[answer_id] = CombinedAnswerState(
            answer_id,
            answer_prior.required_claim_count,
            status_counts[ClaimStatus.SUPPORTED],
            status_counts[ClaimStatus.UNSUPPORTED],
            status_counts[ClaimStatus.REFUTED],
            status_counts[ClaimStatus.CONFLICTED],
            answer_result_status,
        )

    logical_changes: list[M5PersistedLogicalChange] = []
    output_records: list[tuple[str, str, object]] = []
    requirement_writes: list[_RequirementStateWrite] = []
    group_writes: list[_GroupStateWrite] = []
    claim_writes: list[_ClaimStateWrite] = []
    answer_writes: list[CombinedAnswerState] = []
    group_artifact_writes: list[GroupMatchingCertificateArtifact] = []
    claim_artifact_writes: list[ClaimCertificateArtifact] = []
    group_binding_writes: list[WorkingGroupCertificateBinding] = []
    claim_binding_writes: list[WorkingClaimCertificateBinding] = []
    binding_rows: list[M5PersistedCertificateBindingRow] = []
    status_deltas: list[StatusDelta] = []

    for requirement_id in intent.requirement_state_ids:
        requirement_prior = requirement_before[requirement_id]
        requirement_result = requirement_after[requirement_id]
        if requirement_result == requirement_prior:
            continue
        requirement_writes.append(requirement_result)
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.REQUIREMENT_STATE,
                requirement_id,
                _requirement_state_artifact_hash(requirement_prior),
                _requirement_state_artifact_hash(requirement_result),
            )
        )
        output_records.append(
            ("requirement_state", requirement_id, requirement_result.state)
        )
    for group_id in intent.group_state_ids:
        group_prior = group_before[group_id]
        group_result = group_after[group_id]
        group_digest_changed = (
            group_prior.certificate_digest != group_result.certificate_digest
        )
        if group_result != group_prior:
            group_writes.append(group_result)
            logical_changes.append(
                M5PersistedLogicalChange(
                    M5PersistedLogicalChangeKind.GROUP_STATE,
                    group_id,
                    _group_state_artifact_hash(group_prior),
                    _group_state_artifact_hash(group_result),
                )
            )
            output_records.append(("group_state", group_id, group_result.state))
        if group_digest_changed:
            group_certificate = group_artifacts[group_id]
            if group_certificate is not None:
                stored_group_certificate = _stored_group_certificate_artifact(
                    cursor, group_certificate.certificate_digest, lock=False
                )
                if stored_group_certificate is None:
                    group_artifact_writes.append(group_certificate)
                elif stored_group_certificate != group_certificate:
                    raise EventConflictError(
                        "matching group certificate digest collision"
                    )
            logical_changes.append(
                M5PersistedLogicalChange(
                    M5PersistedLogicalChangeKind.GROUP_CERTIFICATE,
                    group_id,
                    group_prior.certificate_digest,
                    group_result.certificate_digest,
                )
            )
            output_records.append(("group_certificate", group_id, group_certificate))
        if group_result != group_prior and group_result.certificate_digest is not None:
            group_binding_write = WorkingGroupCertificateBinding(
                intent.resulting_epoch_id,
                group_id,
                intent.resulting_revision,
                None,
                group_result.certificate_digest,
            )
            group_binding_writes.append(group_binding_write)
            binding_rows.append(
                _persisted_binding_row(
                    M5PersistedBindingKind.GROUP, group_binding_write
                )
            )
            output_records.append(("group_binding", group_id, group_binding_write))
    operation = (
        "DeleteDocumentVersionEvent"
        if update_kind == "document_delete"
        else "ReplaceDocumentVersionEvent"
    )
    reason = f"event={intent.source_id} op={operation}"
    for claim_id in intent.claim_state_ids:
        claim_prior = claim_before[claim_id]
        claim_result = claim_after[claim_id]
        claim_certificate = claim_artifacts[claim_id]
        claim_digest_changed = (
            claim_prior.certificate_digest != claim_result.certificate_digest
        )
        if claim_result != claim_prior:
            claim_writes.append(claim_result)
            logical_changes.append(
                M5PersistedLogicalChange(
                    M5PersistedLogicalChangeKind.CLAIM_STATE,
                    claim_id,
                    _claim_state_artifact_hash(claim_prior),
                    _claim_state_artifact_hash(claim_result),
                )
            )
            output_records.append(("claim_state", claim_id, claim_result.state))
        if claim_digest_changed:
            stored_claim_certificate = _stored_claim_certificate_artifact(
                cursor, claim_certificate.certificate_digest, lock=False
            )
            if stored_claim_certificate is None:
                claim_artifact_writes.append(claim_certificate)
            elif stored_claim_certificate != claim_certificate:
                raise EventConflictError("matching claim certificate digest collision")
            logical_changes.append(
                M5PersistedLogicalChange(
                    M5PersistedLogicalChangeKind.CLAIM_CERTIFICATE,
                    claim_id,
                    claim_prior.certificate_digest,
                    claim_result.certificate_digest,
                )
            )
            output_records.append(("claim_certificate", claim_id, claim_certificate))
        if claim_result != claim_prior:
            claim_binding_write = WorkingClaimCertificateBinding(
                intent.resulting_epoch_id,
                claim_id,
                intent.resulting_revision,
                None,
                claim_result.certificate_digest,
            )
            claim_binding_writes.append(claim_binding_write)
            binding_rows.append(
                _persisted_binding_row(
                    M5PersistedBindingKind.CLAIM, claim_binding_write
                )
            )
            output_records.append(("claim_binding", claim_id, claim_binding_write))
        if claim_result.state.status is not claim_prior.state.status:
            status_deltas.append(
                StatusDelta(
                    intent.source_id,
                    "claim",
                    claim_id,
                    claim_prior.state.status.value,
                    claim_result.state.status.value,
                    reason,
                )
            )
    for answer_id in intent.answer_state_ids:
        answer_prior = answer_before[answer_id]
        answer_result = answer_after[answer_id]
        if answer_result == answer_prior:
            continue
        answer_writes.append(answer_result)
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.ANSWER_STATE,
                answer_id,
                _answer_state_artifact_hash(answer_prior),
                _answer_state_artifact_hash(answer_result),
            )
        )
        output_records.append(("answer_state", answer_id, answer_result))
        if answer_result.status is not answer_prior.status:
            status_deltas.append(
                StatusDelta(
                    intent.source_id,
                    "answer",
                    answer_id,
                    answer_prior.status.value,
                    answer_result.status.value,
                    reason,
                )
            )
    output_records.extend(
        ("status_delta", delta.object_id, delta) for delta in status_deltas
    )
    output_rank = {
        "requirement_state": 0,
        "group_state": 1,
        "claim_state": 2,
        "answer_state": 3,
        "group_certificate": 4,
        "claim_certificate": 5,
        "group_binding": 6,
        "claim_binding": 7,
        "status_delta": 8,
    }
    output_records_tuple = tuple(
        sorted(
            output_records,
            key=lambda row: (
                output_rank[row[0]],
                row[1],
                getattr(row[2], "valid_from_revision", -1),
            ),
        )
    )
    logical_changes_tuple = tuple(
        sorted(logical_changes, key=lambda row: (row.kind.value, row.object_id))
    )
    logical_output = digests.logical_output_preimage(output_records_tuple)
    bucket_operations = sum(
        int(row.before.mask > 0) + int(row.after_mask > 0)
        for row in preview.mask_transitions
        if row.before.mask != row.after_mask
    )
    matching_work = MatchingWorkCounters(
        contribution_removals=len(preview.removed_observations),
        requirement_observation_changes_processed=len(preview.currency_before_images),
        ordered_index_operations=(
            2 * len(preview.currency_before_images)
            + len(preview.removed_observations)
            + bucket_operations
        ),
        edge_refcount_keys_updated=len(preview.edge_transitions),
    )
    for group in preview.groups:
        matching_work += group.hall_work
        before_digest = group_before[group.shape.group_version_id].certificate_digest
        after_digest = group_after[group.shape.group_version_id].certificate_digest
        if before_digest != after_digest:
            if group.certificate_artifact is not None:
                reconstruction = reconstruct_certificate(group.certificate_view)
                if reconstruction.artifact != group.certificate_artifact:
                    raise EventConflictError(
                        "document certificate reconstruction changed"
                    )
                matching_work += reconstruction.work
    matching_work += MatchingWorkCounters(
        group_local_state_operations=len(
            {row.group_version_id for row in group_binding_writes}
        )
    )
    group_state_changed = {
        group_id
        for group_id in intent.group_state_ids
        if group_after[group_id] != group_before[group_id]
    }
    group_certificate_changed = {
        group_id
        for group_id in intent.group_state_ids
        if group_after[group_id].certificate_digest
        != group_before[group_id].certificate_digest
    }
    claim_state_changed = {
        claim_id
        for claim_id in intent.claim_state_ids
        if claim_after[claim_id] != claim_before[claim_id]
    }
    claim_certificate_changed = {
        claim_id
        for claim_id in intent.claim_state_ids
        if claim_after[claim_id].certificate_digest
        != claim_before[claim_id].certificate_digest
    }
    answer_state_changed = {
        answer_id
        for answer_id in intent.answer_state_ids
        if answer_after[answer_id] != answer_before[answer_id]
    }
    matching_work += touched_state_work(
        groups_touched=len(group_state_changed | group_certificate_changed),
        claims_touched=len(claim_state_changed | claim_certificate_changed),
        answers_touched=len(answer_state_changed),
        claim_status_changes=sum(
            delta.object_type == "claim" for delta in status_deltas
        ),
        answer_status_changes=sum(
            delta.object_type == "answer" for delta in status_deltas
        ),
        output_bytes=len(logical_output),
    )
    work = M5OverlayWork(
        matching=matching_work,
        requirement_state_only_changes=sum(
            requirement_after[requirement_id] != requirement_before[requirement_id]
            and requirement_after[requirement_id].state.satisfied
            == requirement_before[requirement_id].state.satisfied
            for requirement_id in intent.requirement_state_ids
        ),
        group_state_only_changes=sum(
            group_id in group_state_changed
            and group_after[group_id].state.complete
            == group_before[group_id].state.complete
            for group_id in intent.group_state_ids
        ),
        claim_state_only_changes=sum(
            claim_id in claim_state_changed
            and claim_after[claim_id].state.status
            is claim_before[claim_id].state.status
            for claim_id in intent.claim_state_ids
        ),
        group_certificate_only_changes=len(
            group_certificate_changed - group_state_changed
        ),
        claim_certificate_only_changes=len(
            claim_certificate_changed - claim_state_changed
        ),
        public_status_deltas=len(status_deltas),
    )
    work.assert_nonnegative()
    document_patch_artifact = _build_patch_artifact(
        intent,
        observation_changes=tuple(observation_changes),
        edge_changes=tuple(edge_changes),
        mask_changes=tuple(mask_changes),
        hall_changes=tuple(hall_changes),
        logical_changes=logical_changes_tuple,
        binding_rows=tuple(binding_rows),
        output_records=output_records_tuple,
        work=work,
    )
    plan = _MatchingWritePlan(
        observation_currency_rows=currency_rows,
        observation_rows=tuple(observation_rows),
        edge_rows=tuple(edge_rows),
        mask_rows=tuple(mask_rows),
        hall_rows=tuple(hall_rows),
        requirement_state_rows=tuple(requirement_writes),
        group_state_rows=tuple(group_writes),
        claim_state_rows=tuple(claim_writes),
        answer_state_rows=tuple(answer_writes),
        group_certificate_artifact_rows=tuple(group_artifact_writes),
        claim_certificate_artifact_rows=tuple(claim_artifact_writes),
        group_binding_rows=tuple(group_binding_writes),
        claim_binding_rows=tuple(claim_binding_writes),
        document_direct_plan=(None if document_direct_plan is None else direct_plan),
        expected_direct_m4_claim_after_images=(
            ()
            if document_direct_plan is None
            else _document_direct_expected_claim_images(intent, direct_plan)
        ),
        expected_direct_m4_answer_after_images=(
            ()
            if document_direct_plan is None
            else _document_direct_expected_answer_images(intent, direct_plan)
        ),
    )
    document_write_counts = _D24OwnedWriteCounts(
        group_state_write_count=len(group_writes),
        claim_state_write_count=len(claim_writes),
        answer_state_write_count=len(answer_writes),
        certificate_binding_write_count=(
            len(group_binding_writes) + len(claim_binding_writes)
        ),
        public_delta_write_count=len(status_deltas),
    )
    return (
        document_patch_artifact,
        plan,
        document_write_counts,
        len(requirement_writes),
    )


def _document_currency_before_images_for_intent(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
) -> tuple[_ObservationCurrencyBeforeImage, ...]:
    """Revalidate the held document currency authority without rediscovery."""

    row = cursor.execute(
        """
        SELECT update_kind
        FROM groundloop_m5_update
        WHERE epoch_id = %s
        """,
        (intent.resulting_epoch_id,),
    ).fetchone()
    if row is None or _text(row[0]) not in {
        "document_delete",
        "document_replace",
    }:
        raise EventConflictError("document matching update kind changed")
    return _document_currency_before_images(
        cursor,
        epoch_id=intent.resulting_epoch_id,
        before_epoch_id=intent.before_epoch_id,
        resulting_revision=intent.resulting_revision,
        source_id=intent.source_id,
        update_kind=_text(row[0]),
        reserve_targets=False,
    )


def _structural_first_application_plan(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    *,
    document_direct_authority: object | None = None,
    expected_document_direct_plan: _DocumentDirectPlan | None = None,
) -> tuple[
    M5PersistedMatchingPatchArtifact,
    _MatchingWritePlan,
    _D24OwnedWriteCounts,
    int,
]:
    """Recompute one locked structural source into its complete D25 plan."""

    row = cursor.execute(
        """
        SELECT typed_update.update_kind, epoch.payload_hash,
               runtime.structural_event_id,
               typed_update.previous_published_epoch_id,
               typed_update.decision_policy_version,
               predecessor.revision
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        JOIN groundloop_epoch AS predecessor
          ON predecessor.epoch_id = typed_update.previous_published_epoch_id
        WHERE epoch.epoch_id = %s AND epoch.event_id = %s
        """,
        (intent.resulting_epoch_id, intent.source_id),
    ).fetchone()
    if row is None or (
        _sha256_text(row[1]) != intent.source_identity_hash
        or _text(row[2]) != intent.source_id
        or int(row[3]) != intent.before_epoch_id
        or _text(row[4]) != intent.decision_policy_version
        or int(row[5]) != intent.before_revision
    ):
        raise EventConflictError("structural matching source changed after intent")
    update_kind = _text(row[0])
    if update_kind in {"document_delete", "document_replace"}:
        if document_direct_authority is not None:
            if expected_document_direct_plan is not None:
                raise ValidationError("document direct authority is duplicated")
            expected_document_direct_plan = _document_direct_plan_from_d29_authority(
                cursor,
                authority=document_direct_authority,
                epoch_id=intent.resulting_epoch_id,
                before_epoch_id=intent.before_epoch_id,
                source_id=intent.source_id,
                source_identity_hash=intent.source_identity_hash,
                update_kind=update_kind,
                decision_policy_version=intent.decision_policy_version,
            )
        return _document_first_application_plan(
            cursor,
            intent,
            update_kind=update_kind,
            document_direct_plan=expected_document_direct_plan,
        )
    if (
        document_direct_authority is not None
        or expected_document_direct_plan is not None
    ):
        raise ValidationError("non-document matching received direct authority")
    if update_kind in {"register_group", "replace_group", "retire_group"}:
        expected_projection = _structural_group_projection(
            cursor,
            epoch_id=intent.resulting_epoch_id,
            source_id=intent.source_id,
            update_kind=update_kind,
        )
    else:
        expected_projection = {
            "group_shapes": (),
            "observation_ids": (),
            "edge_keys": (),
            "mask_keys": (),
            "hall_group_ids": (),
            "requirement_state_ids": (),
            "group_state_ids": (),
            "claim_state_ids": (),
            "answer_state_ids": (),
            "group_certificate_ids": (),
            "claim_certificate_ids": (),
        }
    if any(
        getattr(intent, name) != value for name, value in expected_projection.items()
    ):
        raise EventConflictError(
            "structural matching official intent changed under held locks"
        )
    affected = any(
        (
            intent.group_shapes,
            intent.observation_ids,
            intent.edge_keys,
            intent.mask_keys,
            intent.hall_group_ids,
            intent.requirement_state_ids,
            intent.group_state_ids,
            intent.claim_state_ids,
            intent.answer_state_ids,
            intent.group_certificate_ids,
            intent.claim_certificate_ids,
        )
    )
    if update_kind == "document_insert":
        if affected:
            raise EventConflictError(
                "empty structural source has a nonempty official intent"
            )
        return (
            _empty_patch_artifact(intent),
            _MatchingWritePlan(),
            (_D24OwnedWriteCounts()),
            0,
        )
    if update_kind == "register_group":
        return _structural_registration_plan(intent)
    if update_kind in {"replace_group", "retire_group"}:
        return _structural_retirement_plan(cursor, intent)
    raise InvalidEventError(
        f"structural source {update_kind} has no materialized write plan"
    )


def _matching_stage_coordinates(
    intent: M5PersistedMatchingTransitionIntent,
    plan: _MatchingWritePlan,
) -> tuple[_MatchingStageCoordinate, ...]:
    """Return the complete lower-tier journal key set in lexical stage order."""

    coordinates: list[_MatchingStageCoordinate] = []

    def add(
        relation_name: str,
        key_columns: tuple[str, ...],
        key_parts: tuple[object, ...],
    ) -> None:
        if relation_name not in _STAGE_JOURNAL_RELATIONS:
            raise ValidationError("matching stage coordinate names another relation")
        if len(key_columns) != len(key_parts) or not key_columns:
            raise ValidationError("matching stage coordinate has an invalid key")
        coordinates.append(
            _MatchingStageCoordinate(relation_name, key_columns, key_parts)
        )

    add(
        "groundloop_m5_matching_image_working",
        ("epoch_id",),
        (intent.resulting_epoch_id,),
    )
    for observation_row in plan.observation_rows:
        add(
            "groundloop_m5_matching_observation_working",
            ("epoch_id", "observation_id"),
            (observation_row.epoch_id, observation_row.observation_id),
        )
    for edge_row in plan.edge_rows:
        add(
            "groundloop_m5_matching_edge_working",
            ("epoch_id", "requirement_version_id", "text_hash"),
            (edge_row.epoch_id, edge_row.requirement_version_id, edge_row.text_hash),
        )
    for mask_row in plan.mask_rows:
        add(
            "groundloop_m5_matching_hash_mask_working",
            ("epoch_id", "group_version_id", "text_hash"),
            (mask_row.epoch_id, mask_row.group_version_id, mask_row.text_hash),
        )
    for hall_row in plan.hall_rows:
        add(
            "groundloop_m5_matching_hall_working",
            ("epoch_id", "group_version_id"),
            (hall_row.epoch_id, hall_row.group_version_id),
        )
    for requirement_write in plan.requirement_state_rows:
        add(
            "groundloop_m5_working_requirement_state",
            ("epoch_id", "requirement_version_id"),
            (
                intent.resulting_epoch_id,
                requirement_write.state.requirement_version_id,
            ),
        )
    for group_write in plan.group_state_rows:
        add(
            "groundloop_m5_working_group_state",
            ("epoch_id", "group_version_id"),
            (intent.resulting_epoch_id, group_write.state.group_version_id),
        )
    for claim_write in plan.claim_state_rows:
        add(
            "groundloop_m5_working_claim_state",
            ("epoch_id", "claim_id"),
            (intent.resulting_epoch_id, claim_write.state.claim_id),
        )
    for state in plan.answer_state_rows:
        add(
            "groundloop_m5_working_answer_state",
            ("epoch_id", "answer_version_id"),
            (intent.resulting_epoch_id, state.answer_version_id),
        )
    for group_artifact in plan.group_certificate_artifact_rows:
        add(
            "groundloop_m5_group_certificate_artifact",
            ("certificate_digest",),
            (group_artifact.certificate_digest,),
        )
        for certificate_row in group_artifact.rows:
            add(
                "groundloop_m5_group_certificate_artifact_row",
                ("certificate_digest", "requirement_ordinal"),
                (
                    group_artifact.certificate_digest,
                    certificate_row.requirement_ordinal,
                ),
            )
    for claim_artifact in plan.claim_certificate_artifact_rows:
        add(
            "groundloop_m5_claim_certificate_artifact",
            ("certificate_digest",),
            (claim_artifact.certificate_digest,),
        )
    for group_binding in plan.group_binding_rows:
        add(
            "groundloop_m5_working_group_certificate_binding",
            ("epoch_id", "group_version_id", "valid_from_revision"),
            (
                group_binding.epoch_id,
                group_binding.group_version_id,
                group_binding.valid_from_revision,
            ),
        )
    for claim_binding in plan.claim_binding_rows:
        add(
            "groundloop_m5_working_claim_certificate_binding",
            ("epoch_id", "claim_id", "valid_from_revision"),
            (
                claim_binding.epoch_id,
                claim_binding.claim_id,
                claim_binding.valid_from_revision,
            ),
        )
    family_order = {
        "groundloop_m5_matching_image_working": 0,
        "groundloop_m5_matching_observation_working": 1,
        "groundloop_m5_matching_edge_working": 2,
        "groundloop_m5_matching_hash_mask_working": 3,
        "groundloop_m5_matching_hall_working": 4,
        "groundloop_m5_working_requirement_state": 5,
        "groundloop_m5_working_group_state": 6,
        "groundloop_m5_group_certificate_artifact": 7,
        "groundloop_m5_group_certificate_artifact_row": 8,
        "groundloop_m5_working_group_certificate_binding": 9,
        "groundloop_m5_working_claim_state": 10,
        "groundloop_m5_claim_certificate_artifact": 11,
        "groundloop_m5_working_claim_certificate_binding": 12,
        "groundloop_m5_working_answer_state": 13,
    }
    coordinates.sort(
        key=lambda coordinate: (
            family_order[coordinate.relation_name],
            coordinate.journal_key[1],
        )
    )
    journal_keys = tuple(coordinate.journal_key for coordinate in coordinates)
    if len(set(journal_keys)) != len(journal_keys):
        raise ValidationError("matching stage plan repeats a journal key")
    return tuple(coordinates)


def _capture_matching_stage_before_images(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    plan: _MatchingWritePlan,
) -> tuple[_MatchingStageBeforeImage, ...]:
    """Capture exact held first-old rows before any lower-tier stage DML."""

    result: list[_MatchingStageBeforeImage] = []
    for coordinate in _matching_stage_coordinates(intent, plan):
        predicates = sql.SQL(" AND ").join(
            sql.SQL("{} = {}").format(sql.Identifier(column), sql.Placeholder())
            for column in coordinate.key_columns
        )
        row = cursor.execute(
            sql.SQL("SELECT to_jsonb(row_value) FROM {} AS row_value WHERE ").format(
                sql.Identifier(coordinate.relation_name)
            )
            + predicates,
            coordinate.key_parts,
        ).fetchone()
        result.append(
            _MatchingStageBeforeImage(
                coordinate,
                None if row is None else row[0],
            )
        )
    return tuple(result)


def _matching_stage_before_map(
    prepared: _PreparedMatchingTransition,
) -> dict[tuple[str, bytes], object | None]:
    before = {
        item.coordinate.journal_key: item.row_json
        for item in prepared.stage_before_images
    }
    if len(before) != len(prepared.stage_before_images):
        raise ValidationError("matching prepared before-image keys repeat")
    return before


def _prepare_matching_transition_core(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    expected_patch_digest: str | None = None,
    expected_work: M5OverlayWork | None = None,
    *,
    document_direct_authority: object | None,
) -> _PreparedMatchingTransition:
    """Bind a complete prewrite plan to the exact authorised cursor."""

    if type(intent) is not M5PersistedMatchingTransitionIntent:
        raise ValidationError("matching transition requires the exact intent DTO")
    if expected_patch_digest is not None:
        _require_sha256("expected_patch_digest", expected_patch_digest)
    if expected_work is not None and type(expected_work) is not M5OverlayWork:
        raise ValidationError("expected_work must be an exact M5OverlayWork")
    expected_revision = (
        1
        if intent.source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN
        else intent.before_revision
    )
    _require_checked_prefix(
        cursor,
        epoch_id=intent.resulting_epoch_id,
        expected_revision=expected_revision,
        reauthorize=False,
    )
    binding = _capture_cursor_binding(cursor, intent)
    prewrite_revision, image_base_epoch, image_base_revision = (
        _read_held_first_application_image(cursor, intent)
    )
    currency_before_images: tuple[_ObservationCurrencyBeforeImage, ...] = ()
    if intent.source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN:
        artifact, plan, d24_counts, requirement_count = (
            _structural_first_application_plan(
                cursor,
                intent,
                document_direct_authority=document_direct_authority,
            )
        )
        if plan.observation_currency_rows:
            currency_before_images = _document_currency_before_images_for_intent(
                cursor, intent
            )
        _require_structural_absence_plan(cursor, intent, plan)
        base_header, runtime_header = _header_images(cursor, intent.resulting_epoch_id)
    elif intent.source_kind is M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION:
        if document_direct_authority is not None:
            raise ValidationError("direct document authority requires structural_open")
        (
            artifact,
            plan,
            d24_counts,
            requirement_count,
            currency_before,
            requirement_source,
        ) = _requirement_first_application_plan(cursor, intent)
        currency_before_images = (currency_before,)
        base_header, runtime_header = _requirement_header_after_images(
            cursor, requirement_source
        )
    else:
        raise InvalidEventError(
            f"{intent.source_kind.value} has no materialized write plan"
        )
    if expected_patch_digest is not None and (
        expected_patch_digest != artifact.patch.patch_digest
    ):
        raise EventConflictError("computed matching patch digest differs")
    if expected_work is not None and expected_work != artifact.work:
        raise EventConflictError("computed matching work differs")
    stage_before_images = _capture_matching_stage_before_images(cursor, intent, plan)
    prepared = _PreparedMatchingTransition(
        binding=binding,
        prepared_identity=0,
        official_intent=intent,
        prewrite_matching_revision=prewrite_revision,
        matching_image_base_epoch_id=image_base_epoch,
        matching_image_base_revision=image_base_revision,
        prewrite_patch_artifact=artifact,
        physical_and_logical_write_plan=plan,
        stage_before_images=stage_before_images,
        observation_currency_before_images=currency_before_images,
        base_header_after_image=base_header,
        runtime_header_after_image=runtime_header,
        d25_contribution_work=artifact.work,
        d24_owned_planned_write_counts=d24_counts,
        requirement_state_write_count_diagnostic=requirement_count,
        expected_patch_digest=expected_patch_digest,
        expected_work=expected_work,
    )
    prepared.prepared_identity = id(prepared)
    _seal_prepared_authority(prepared)
    return prepared


def _prepare_matching_transition(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    expected_patch_digest: str | None = None,
    expected_work: M5OverlayWork | None = None,
) -> _PreparedMatchingTransition:
    """Retained prepare path with no hidden document authority channel."""

    return _prepare_matching_transition_core(
        cursor,
        intent,
        expected_patch_digest,
        expected_work,
        document_direct_authority=None,
    )


def _prepare_matching_transition_with_document_authority(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    document_direct_authority: object,
    expected_patch_digest: str | None = None,
    expected_work: M5OverlayWork | None = None,
) -> _PreparedMatchingTransition:
    """Private prepare path for one validated D29 direct authority."""

    return _prepare_matching_transition_core(
        cursor,
        intent,
        expected_patch_digest,
        expected_work,
        document_direct_authority=document_direct_authority,
    )


def _stage_before_row(
    before_images: dict[tuple[str, bytes], object | None],
    relation_name: str,
    key_parts: Sequence[object],
) -> object | None:
    key = (
        relation_name,
        _matching_journal_key_preimage(relation_name, key_parts),
    )
    if key not in before_images:
        raise ValidationError("matching stage mutation lacks a prepared before image")
    return before_images[key]


def _stage_before_revision(row_json: object) -> int:
    if not isinstance(row_json, dict):
        raise ValidationError("matching stage before image is not a JSON object")
    revision = row_json.get("updated_revision")
    if isinstance(revision, bool) or not isinstance(revision, int):
        raise ValidationError("matching stage before image lacks its revision")
    return revision


def _stage_observation_currency_plan(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    plan: _MatchingWritePlan,
    before_images: tuple[_ObservationCurrencyBeforeImage, ...],
) -> None:
    """Install only the held M5 tier-11a close/open history transition."""

    if not plan.observation_currency_rows:
        if before_images:
            raise EventConflictError(
                "matching currency authority exists without a write plan"
            )
        return
    if intent.source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN:
        before_by_key = {before.key: before for before in before_images}
        rows_by_key = {
            (
                row.epoch_id,
                row.subject_kind,
                row.subject_id,
                row.chunk_version_id,
                row.task_type,
            ): row
            for row in plan.observation_currency_rows
        }
        if (
            len(before_by_key) != len(before_images)
            or len(rows_by_key) != len(plan.observation_currency_rows)
            or set(before_by_key) != set(rows_by_key)
        ):
            raise EventConflictError(
                "document matching currency plan changed before stage"
            )
        for key in sorted(rows_by_key):
            before = before_by_key[key]
            opened = rows_by_key[key]
            if (
                before.working_currency_row is not None
                or before.current_currency_row is None
                or before.published_currency_row is None
                or len(before.semantic_observation_rows) != 1
                or opened.observation_id is not None
                or opened.valid_from_revision != intent.resulting_revision
                or opened.valid_to_revision is not None
            ):
                raise EventConflictError(
                    "document matching currency authority changed before stage"
                )
            inserted = cursor.execute(
                """
                INSERT INTO groundloop_m5_working_currency_history (
                  epoch_id, subject_kind, subject_id, chunk_version_id, task_type,
                  observation_id, valid_from_revision, valid_to_revision
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING valid_from_revision
                """,
                (
                    opened.epoch_id,
                    opened.subject_kind,
                    opened.subject_id,
                    opened.chunk_version_id,
                    opened.task_type,
                    opened.observation_id,
                    opened.valid_from_revision,
                    opened.valid_to_revision,
                ),
            ).fetchone()
            if inserted is None or int(inserted[0]) != opened.valid_from_revision:
                raise EventConflictError("document matching currency stage changed")
        return
    if (
        intent.source_kind is not M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION
        or len(before_images) != 1
    ):
        raise EventConflictError("matching currency plan has another source shape")
    before = before_images[0]
    rows = plan.observation_currency_rows
    close_rows = tuple(row for row in rows if row.valid_to_revision is not None)
    open_rows = tuple(row for row in rows if row.valid_to_revision is None)
    if len(close_rows) > 1 or len(open_rows) != 1:
        raise EventConflictError("matching currency plan is not one close/open chain")
    opened = open_rows[0]
    expected_key = (
        opened.epoch_id,
        opened.subject_kind,
        opened.subject_id,
        opened.chunk_version_id,
        opened.task_type,
    )
    if (
        expected_key != before.key
        or opened.valid_from_revision != intent.resulting_revision
        or opened.valid_to_revision is not None
        or any(
            (
                row.epoch_id,
                row.subject_kind,
                row.subject_id,
                row.chunk_version_id,
                row.task_type,
            )
            != expected_key
            for row in rows
        )
    ):
        raise EventConflictError("matching currency plan changed before stage")
    if before.working_currency_row is None:
        if close_rows:
            raise EventConflictError("matching currency close has no before image")
    else:
        if len(close_rows) != 1:
            raise EventConflictError("matching currency before image lacks its close")
        closed = close_rows[0]
        prior = before.working_currency_row
        if (
            closed.valid_from_revision != int(_text(prior[6]))
            or closed.valid_to_revision != intent.resulting_revision
            or closed.observation_id != (None if prior[5] is None else _text(prior[5]))
        ):
            raise EventConflictError("matching currency close changed before stage")
        updated = cursor.execute(
            """
            UPDATE groundloop_m5_working_currency_history
            SET valid_to_revision = %s
            WHERE epoch_id = %s AND subject_kind = %s AND subject_id = %s
              AND chunk_version_id = %s AND task_type = %s
              AND valid_from_revision = %s
              AND observation_id IS NOT DISTINCT FROM %s
              AND valid_to_revision IS NULL
            RETURNING valid_from_revision
            """,
            (
                intent.resulting_revision,
                closed.epoch_id,
                closed.subject_kind,
                closed.subject_id,
                closed.chunk_version_id,
                closed.task_type,
                closed.valid_from_revision,
                closed.observation_id,
            ),
        ).fetchone()
        if updated is None or int(updated[0]) != closed.valid_from_revision:
            raise EventConflictError("matching currency close lost its held row")
    inserted = cursor.execute(
        """
        INSERT INTO groundloop_m5_working_currency_history (
          epoch_id, subject_kind, subject_id, chunk_version_id, task_type,
          observation_id, valid_from_revision, valid_to_revision
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING valid_from_revision
        """,
        (
            opened.epoch_id,
            opened.subject_kind,
            opened.subject_id,
            opened.chunk_version_id,
            opened.task_type,
            opened.observation_id,
            opened.valid_from_revision,
            opened.valid_to_revision,
        ),
    ).fetchone()
    if inserted is None or int(inserted[0]) != opened.valid_from_revision:
        raise EventConflictError("matching currency open stage changed")


def _validate_staged_observation_currency(
    cursor: Cursor[Any], prepared: _PreparedMatchingTransition
) -> None:
    plan = prepared.physical_and_logical_write_plan
    if not plan.observation_currency_rows:
        if prepared.observation_currency_before_images:
            raise EventConflictError("matching staged currency authority has no plan")
        return
    if (
        prepared.official_intent.source_kind
        is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN
    ):
        before_by_key = {
            before.key: before for before in prepared.observation_currency_before_images
        }
        rows_by_key = {
            (
                row.epoch_id,
                row.subject_kind,
                row.subject_id,
                row.chunk_version_id,
                row.task_type,
            ): row
            for row in plan.observation_currency_rows
        }
        if (
            len(before_by_key) != len(prepared.observation_currency_before_images)
            or len(rows_by_key) != len(plan.observation_currency_rows)
            or set(before_by_key) != set(rows_by_key)
        ):
            raise EventConflictError("matching staged document currency changed")
        for key in sorted(rows_by_key):
            planned = rows_by_key[key]
            rows = cursor.execute(
                """
                SELECT epoch_id, subject_kind, subject_id, chunk_version_id,
                       task_type, observation_id, valid_from_revision,
                       valid_to_revision
                FROM groundloop_m5_working_currency_history
                WHERE epoch_id = %s AND subject_kind = %s AND subject_id = %s
                  AND chunk_version_id = %s AND task_type = %s
                  AND (valid_from_revision = %s OR valid_to_revision IS NULL)
                ORDER BY valid_from_revision
                """,
                (*key, planned.valid_from_revision),
            ).fetchall()
            expected_row = (
                planned.epoch_id,
                planned.subject_kind,
                planned.subject_id,
                planned.chunk_version_id,
                planned.task_type,
                planned.observation_id,
                planned.valid_from_revision,
                planned.valid_to_revision,
            )
            if tuple(tuple(row) for row in rows) != (expected_row,):
                raise EventConflictError(
                    "matching staged observation currency differs from its plan"
                )
        return
    if len(prepared.observation_currency_before_images) != 1:
        raise EventConflictError("matching staged currency authority changed")
    before = prepared.observation_currency_before_images[0]
    rows = cursor.execute(
        """
        SELECT epoch_id, subject_kind, subject_id, chunk_version_id,
               task_type, observation_id, valid_from_revision,
               valid_to_revision
        FROM groundloop_m5_working_currency_history
        WHERE epoch_id = %s AND subject_kind = %s AND subject_id = %s
          AND chunk_version_id = %s AND task_type = %s
          AND valid_from_revision = ANY(%s::bigint[])
        ORDER BY valid_from_revision
        """,
        (
            *before.key,
            [row.valid_from_revision for row in plan.observation_currency_rows],
        ),
    ).fetchall()
    expected = tuple(
        (
            row.epoch_id,
            row.subject_kind,
            row.subject_id,
            row.chunk_version_id,
            row.task_type,
            row.observation_id,
            row.valid_from_revision,
            row.valid_to_revision,
        )
        for row in sorted(
            plan.observation_currency_rows,
            key=lambda item: item.valid_from_revision,
        )
    )
    if tuple(tuple(row) for row in rows) != expected:
        raise EventConflictError(
            "matching staged observation currency differs from its plan"
        )


def _stage_matching_write_plan_fragment(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    plan: _MatchingWritePlan,
    before_images: dict[tuple[str, bytes], object | None],
) -> None:
    """Install only the precomputed lower-tier rows; acquire no new key set."""
    key_parts: tuple[object, ...]
    values: tuple[object, ...]
    for observation_row in plan.observation_rows:
        key_parts = (observation_row.epoch_id, observation_row.observation_id)
        before = _stage_before_row(
            before_images,
            "groundloop_m5_matching_observation_working",
            key_parts,
        )
        if before is None:
            inserted = cursor.execute(
                """
                INSERT INTO groundloop_m5_matching_observation_working (
                  epoch_id, observation_id, requirement_version_id,
                  group_version_id, requirement_ordinal, text_hash,
                  present, updated_revision
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING observation_id
                """,
                (
                    observation_row.epoch_id,
                    observation_row.observation_id,
                    observation_row.requirement_version_id,
                    observation_row.group_version_id,
                    observation_row.requirement_ordinal,
                    observation_row.text_hash,
                    observation_row.present,
                    observation_row.updated_revision,
                ),
            ).fetchone()
        else:
            inserted = cursor.execute(
                """
                UPDATE groundloop_m5_matching_observation_working
                SET present = %s, updated_revision = %s
                WHERE epoch_id = %s AND observation_id = %s
                  AND requirement_version_id = %s
                  AND group_version_id = %s
                  AND requirement_ordinal = %s AND text_hash = %s
                  AND updated_revision = %s
                RETURNING observation_id
                """,
                (
                    observation_row.present,
                    observation_row.updated_revision,
                    observation_row.epoch_id,
                    observation_row.observation_id,
                    observation_row.requirement_version_id,
                    observation_row.group_version_id,
                    observation_row.requirement_ordinal,
                    observation_row.text_hash,
                    _stage_before_revision(before),
                ),
            ).fetchone()
        if inserted is None or _text(inserted[0]) != observation_row.observation_id:
            raise EventConflictError("matching observation stage changed")
    for edge_row in plan.edge_rows:
        key_parts = (
            edge_row.epoch_id,
            edge_row.requirement_version_id,
            edge_row.text_hash,
        )
        before = _stage_before_row(
            before_images, "groundloop_m5_matching_edge_working", key_parts
        )
        if before is None:
            inserted = cursor.execute(
                """
                INSERT INTO groundloop_m5_matching_edge_working (
                  epoch_id, requirement_version_id, text_hash,
                  group_version_id, requirement_ordinal, refcount,
                  updated_revision
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                RETURNING requirement_version_id, text_hash
                """,
                (
                    edge_row.epoch_id,
                    edge_row.requirement_version_id,
                    edge_row.text_hash,
                    edge_row.group_version_id,
                    edge_row.requirement_ordinal,
                    edge_row.refcount,
                    edge_row.updated_revision,
                ),
            ).fetchone()
        else:
            inserted = cursor.execute(
                """
                UPDATE groundloop_m5_matching_edge_working
                SET refcount = %s, updated_revision = %s
                WHERE epoch_id = %s AND requirement_version_id = %s
                  AND text_hash = %s AND group_version_id = %s
                  AND requirement_ordinal = %s AND updated_revision = %s
                RETURNING requirement_version_id, text_hash
                """,
                (
                    edge_row.refcount,
                    edge_row.updated_revision,
                    edge_row.epoch_id,
                    edge_row.requirement_version_id,
                    edge_row.text_hash,
                    edge_row.group_version_id,
                    edge_row.requirement_ordinal,
                    _stage_before_revision(before),
                ),
            ).fetchone()
        if inserted is None or (
            _text(inserted[0]),
            _sha256_text(inserted[1]),
        ) != (edge_row.requirement_version_id, edge_row.text_hash):
            raise EventConflictError("matching edge stage changed")
    for mask_row in plan.mask_rows:
        key_parts = (mask_row.epoch_id, mask_row.group_version_id, mask_row.text_hash)
        before = _stage_before_row(
            before_images,
            "groundloop_m5_matching_hash_mask_working",
            key_parts,
        )
        if before is None:
            inserted = cursor.execute(
                """
                INSERT INTO groundloop_m5_matching_hash_mask_working (
                  epoch_id, group_version_id, text_hash, mask, updated_revision
                ) VALUES (%s,%s,%s,%s,%s)
                RETURNING group_version_id, text_hash
                """,
                (
                    mask_row.epoch_id,
                    mask_row.group_version_id,
                    mask_row.text_hash,
                    mask_row.mask,
                    mask_row.updated_revision,
                ),
            ).fetchone()
        else:
            inserted = cursor.execute(
                """
                UPDATE groundloop_m5_matching_hash_mask_working
                SET mask = %s, updated_revision = %s
                WHERE epoch_id = %s AND group_version_id = %s
                  AND text_hash = %s AND updated_revision = %s
                RETURNING group_version_id, text_hash
                """,
                (
                    mask_row.mask,
                    mask_row.updated_revision,
                    mask_row.epoch_id,
                    mask_row.group_version_id,
                    mask_row.text_hash,
                    _stage_before_revision(before),
                ),
            ).fetchone()
        if inserted is None or (
            _text(inserted[0]),
            _sha256_text(inserted[1]),
        ) != (mask_row.group_version_id, mask_row.text_hash):
            raise EventConflictError("matching mask stage changed")
    for hall_row in plan.hall_rows:
        key_parts = (hall_row.epoch_id, hall_row.group_version_id)
        before = _stage_before_row(
            before_images, "groundloop_m5_matching_hall_working", key_parts
        )
        parameters = (
            hall_row.present,
            hall_row.requirement_count,
            None if hall_row.mask_histogram is None else list(hall_row.mask_histogram),
            (
                None
                if hall_row.neighbor_counts is None
                else list(hall_row.neighbor_counts)
            ),
            None if hall_row.deficiencies is None else list(hall_row.deficiencies),
            hall_row.maximum_deficiency,
            hall_row.matching_size,
            hall_row.distinct_hash_count,
            hall_row.updated_revision,
        )
        if before is None:
            inserted = cursor.execute(
                """
            INSERT INTO groundloop_m5_matching_hall_working (
              epoch_id, group_version_id, present, requirement_count,
              mask_histogram, neighbor_counts, deficiencies,
              maximum_deficiency, matching_size, distinct_hash_count,
              updated_revision
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING group_version_id
            """,
                (
                    hall_row.epoch_id,
                    hall_row.group_version_id,
                    *parameters,
                ),
            ).fetchone()
        else:
            inserted = cursor.execute(
                """
                UPDATE groundloop_m5_matching_hall_working
                SET present = %s, requirement_count = %s,
                    mask_histogram = %s, neighbor_counts = %s,
                    deficiencies = %s, maximum_deficiency = %s,
                    matching_size = %s, distinct_hash_count = %s,
                    updated_revision = %s
                WHERE epoch_id = %s AND group_version_id = %s
                  AND updated_revision = %s
                RETURNING group_version_id
                """,
                (
                    *parameters,
                    hall_row.epoch_id,
                    hall_row.group_version_id,
                    _stage_before_revision(before),
                ),
            ).fetchone()
        if inserted is None or _text(inserted[0]) != hall_row.group_version_id:
            raise EventConflictError("matching Hall stage changed")
    for requirement_write in plan.requirement_state_rows:
        requirement_state = requirement_write.state
        key_parts = (
            intent.resulting_epoch_id,
            requirement_state.requirement_version_id,
        )
        before = _stage_before_row(
            before_images, "groundloop_m5_working_requirement_state", key_parts
        )
        values = (
            list(requirement_state.witness_hashes),
            list(requirement_state.supporting_observation_ids),
            requirement_state.witness_count,
            requirement_state.satisfied,
            requirement_write.decision_policy_version,
            intent.resulting_revision,
        )
        if before is None:
            inserted = cursor.execute(
                """
                INSERT INTO groundloop_m5_working_requirement_state (
                  epoch_id, requirement_version_id, witness_hashes,
                  supporting_observation_ids, witness_count, satisfied,
                  decision_policy_version, updated_revision
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING requirement_version_id
                """,
                (
                    intent.resulting_epoch_id,
                    requirement_state.requirement_version_id,
                    *values,
                ),
            ).fetchone()
        else:
            inserted = cursor.execute(
                """
                UPDATE groundloop_m5_working_requirement_state
                SET witness_hashes = %s, supporting_observation_ids = %s,
                    witness_count = %s, satisfied = %s,
                    decision_policy_version = %s, updated_revision = %s
                WHERE epoch_id = %s AND requirement_version_id = %s
                  AND updated_revision = %s
                RETURNING requirement_version_id
                """,
                (
                    *values,
                    intent.resulting_epoch_id,
                    requirement_state.requirement_version_id,
                    _stage_before_revision(before),
                ),
            ).fetchone()
        if (
            inserted is None
            or _text(inserted[0]) != requirement_state.requirement_version_id
        ):
            raise EventConflictError("matching requirement-state stage changed")
    for group_write in plan.group_state_rows:
        group_state = group_write.state
        key_parts = (intent.resulting_epoch_id, group_state.group_version_id)
        before = _stage_before_row(
            before_images, "groundloop_m5_working_group_state", key_parts
        )
        values = (
            group_state.requirement_count,
            group_state.satisfied_count,
            group_state.matching_size,
            group_state.complete,
            group_write.decision_policy_version,
            group_write.certificate_digest,
            intent.resulting_revision,
        )
        if before is None:
            inserted = cursor.execute(
                """
                INSERT INTO groundloop_m5_working_group_state (
                  epoch_id, group_version_id, requirement_count,
                  satisfied_count, matching_size, complete,
                  decision_policy_version, certificate_digest, updated_revision
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING group_version_id
                """,
                (
                    intent.resulting_epoch_id,
                    group_state.group_version_id,
                    *values,
                ),
            ).fetchone()
        else:
            inserted = cursor.execute(
                """
                UPDATE groundloop_m5_working_group_state
                SET requirement_count = %s, satisfied_count = %s,
                    matching_size = %s, complete = %s,
                    decision_policy_version = %s, certificate_digest = %s,
                    updated_revision = %s
                WHERE epoch_id = %s AND group_version_id = %s
                  AND updated_revision = %s
                RETURNING group_version_id
                """,
                (
                    *values,
                    intent.resulting_epoch_id,
                    group_state.group_version_id,
                    _stage_before_revision(before),
                ),
            ).fetchone()
        if inserted is None or _text(inserted[0]) != group_state.group_version_id:
            raise EventConflictError("matching group-state stage changed")
    for claim_write in plan.claim_state_rows:
        claim_state = claim_write.state
        key_parts = (intent.resulting_epoch_id, claim_state.claim_id)
        before = _stage_before_row(
            before_images, "groundloop_m5_working_claim_state", key_parts
        )
        values = (
            claim_state.support_count,
            claim_state.refute_count,
            claim_state.best_support_score,
            claim_state.best_refute_score,
            list(claim_state.supporting_observation_ids),
            list(claim_state.refuting_observation_ids),
            claim_state.complete_group_count,
            list(claim_state.complete_group_ids),
            claim_state.status.value,
            claim_write.decision_policy_version,
            claim_write.certificate_digest,
            intent.resulting_revision,
        )
        if before is None:
            inserted = cursor.execute(
                """
            INSERT INTO groundloop_m5_working_claim_state (
              epoch_id, claim_id, support_count, refute_count,
              best_support_score, best_refute_score,
              supporting_observation_ids, refuting_observation_ids,
              complete_group_count, complete_group_ids, status,
              decision_policy_version, certificate_digest, updated_revision
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING claim_id
            """,
                (
                    intent.resulting_epoch_id,
                    claim_state.claim_id,
                    *values,
                ),
            ).fetchone()
        else:
            inserted = cursor.execute(
                """
                UPDATE groundloop_m5_working_claim_state
                SET support_count = %s, refute_count = %s,
                    best_support_score = %s, best_refute_score = %s,
                    supporting_observation_ids = %s,
                    refuting_observation_ids = %s,
                    complete_group_count = %s, complete_group_ids = %s,
                    status = %s, decision_policy_version = %s,
                    certificate_digest = %s, updated_revision = %s
                WHERE epoch_id = %s AND claim_id = %s
                  AND updated_revision = %s
                RETURNING claim_id
                """,
                (
                    *values,
                    intent.resulting_epoch_id,
                    claim_state.claim_id,
                    _stage_before_revision(before),
                ),
            ).fetchone()
        if inserted is None or _text(inserted[0]) != claim_state.claim_id:
            raise EventConflictError("matching claim-state stage changed")
    for answer_state in plan.answer_state_rows:
        key_parts = (intent.resulting_epoch_id, answer_state.answer_version_id)
        before = _stage_before_row(
            before_images, "groundloop_m5_working_answer_state", key_parts
        )
        values = (
            answer_state.required_claim_count,
            answer_state.supported_count,
            answer_state.unsupported_count,
            answer_state.refuted_count,
            answer_state.conflicted_count,
            answer_state.status.value,
            intent.resulting_revision,
        )
        if before is None:
            inserted = cursor.execute(
                """
                INSERT INTO groundloop_m5_working_answer_state (
                  epoch_id, answer_version_id, required_claim_count,
                  supported_count, unsupported_count, refuted_count,
                  conflicted_count, status, updated_revision
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING answer_version_id
                """,
                (
                    intent.resulting_epoch_id,
                    answer_state.answer_version_id,
                    *values,
                ),
            ).fetchone()
        else:
            inserted = cursor.execute(
                """
                UPDATE groundloop_m5_working_answer_state
                SET required_claim_count = %s, supported_count = %s,
                    unsupported_count = %s, refuted_count = %s,
                    conflicted_count = %s, status = %s,
                    updated_revision = %s
                WHERE epoch_id = %s AND answer_version_id = %s
                  AND updated_revision = %s
                RETURNING answer_version_id
                """,
                (
                    *values,
                    intent.resulting_epoch_id,
                    answer_state.answer_version_id,
                    _stage_before_revision(before),
                ),
            ).fetchone()
        if inserted is None or _text(inserted[0]) != answer_state.answer_version_id:
            raise EventConflictError("matching answer-state stage changed")
    for group_artifact in plan.group_certificate_artifact_rows:
        if (
            _stage_before_row(
                before_images,
                "groundloop_m5_group_certificate_artifact",
                (group_artifact.certificate_digest,),
            )
            is not None
        ):
            raise EventConflictError(
                "matching group-certificate artifact already exists"
            )
        inserted = cursor.execute(
            """
            INSERT INTO groundloop_m5_group_certificate_artifact (
              certificate_digest, decision_policy_version,
              certificate_version, group_version_id, requirement_count
            ) VALUES (%s,%s,%s,%s,%s)
            RETURNING certificate_digest
            """,
            (
                group_artifact.certificate_digest,
                group_artifact.decision_policy_version,
                group_artifact.certificate_version,
                group_artifact.group_version_id,
                group_artifact.requirement_count,
            ),
        ).fetchone()
        if inserted is None or (
            _sha256_text(inserted[0]) != group_artifact.certificate_digest
        ):
            raise EventConflictError("matching group-certificate stage changed")
        for certificate_row in group_artifact.rows:
            if (
                _stage_before_row(
                    before_images,
                    "groundloop_m5_group_certificate_artifact_row",
                    (
                        group_artifact.certificate_digest,
                        certificate_row.requirement_ordinal,
                    ),
                )
                is not None
            ):
                raise EventConflictError(
                    "matching group-certificate artifact row already exists"
                )
            inserted_row = cursor.execute(
                """
                INSERT INTO groundloop_m5_group_certificate_artifact_row (
                  certificate_digest, requirement_ordinal,
                  requirement_version_id, text_hash,
                  selected_observation_id
                ) VALUES (%s,%s,%s,%s,%s)
                RETURNING certificate_digest, requirement_ordinal
                """,
                (
                    group_artifact.certificate_digest,
                    certificate_row.requirement_ordinal,
                    certificate_row.requirement_version_id,
                    certificate_row.text_hash,
                    certificate_row.selected_observation_id,
                ),
            ).fetchone()
            if inserted_row is None or (
                _sha256_text(inserted_row[0]),
                int(inserted_row[1]),
            ) != (
                group_artifact.certificate_digest,
                certificate_row.requirement_ordinal,
            ):
                raise EventConflictError("matching group-certificate row stage changed")
    for claim_artifact in plan.claim_certificate_artifact_rows:
        if (
            _stage_before_row(
                before_images,
                "groundloop_m5_claim_certificate_artifact",
                (claim_artifact.certificate_digest,),
            )
            is not None
        ):
            raise EventConflictError(
                "matching claim-certificate artifact already exists"
            )
        inserted = cursor.execute(
            """
            INSERT INTO groundloop_m5_claim_certificate_artifact (
              certificate_digest, certificate_version, claim_id,
              decision_policy_version, support_kind,
              direct_support_observation_id, group_version_id,
              group_certificate_digest, direct_refute_observation_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING certificate_digest
            """,
            (
                claim_artifact.certificate_digest,
                claim_artifact.certificate_version,
                claim_artifact.claim_id,
                claim_artifact.decision_policy_version,
                claim_artifact.support_kind.value,
                claim_artifact.direct_support_observation_id,
                claim_artifact.group_version_id,
                claim_artifact.group_certificate_digest,
                claim_artifact.direct_refute_observation_id,
            ),
        ).fetchone()
        if inserted is None or (
            _sha256_text(inserted[0]) != claim_artifact.certificate_digest
        ):
            raise EventConflictError("matching claim-certificate stage changed")
    for group_binding in plan.group_binding_rows:
        key_parts = (
            group_binding.epoch_id,
            group_binding.group_version_id,
            group_binding.valid_from_revision,
        )
        before = _stage_before_row(
            before_images,
            "groundloop_m5_working_group_certificate_binding",
            key_parts,
        )
        if before is None:
            inserted = cursor.execute(
                """
                INSERT INTO groundloop_m5_working_group_certificate_binding (
                  epoch_id, group_version_id, valid_from_revision,
                  valid_to_revision, certificate_digest
                ) VALUES (%s,%s,%s,%s,%s)
                RETURNING group_version_id, valid_from_revision
                """,
                (
                    group_binding.epoch_id,
                    group_binding.group_version_id,
                    group_binding.valid_from_revision,
                    group_binding.valid_to_revision,
                    group_binding.certificate_digest,
                ),
            ).fetchone()
        else:
            inserted = cursor.execute(
                """
                UPDATE groundloop_m5_working_group_certificate_binding
                SET valid_to_revision = %s
                WHERE epoch_id = %s AND group_version_id = %s
                  AND valid_from_revision = %s AND valid_to_revision IS NULL
                  AND certificate_digest = %s
                RETURNING group_version_id, valid_from_revision
                """,
                (
                    group_binding.valid_to_revision,
                    group_binding.epoch_id,
                    group_binding.group_version_id,
                    group_binding.valid_from_revision,
                    group_binding.certificate_digest,
                ),
            ).fetchone()
        if inserted is None or (
            _text(inserted[0]),
            int(inserted[1]),
        ) != (group_binding.group_version_id, group_binding.valid_from_revision):
            raise EventConflictError("matching group-binding stage changed")
    for claim_binding in plan.claim_binding_rows:
        key_parts = (
            claim_binding.epoch_id,
            claim_binding.claim_id,
            claim_binding.valid_from_revision,
        )
        before = _stage_before_row(
            before_images,
            "groundloop_m5_working_claim_certificate_binding",
            key_parts,
        )
        if before is None:
            inserted = cursor.execute(
                """
                INSERT INTO groundloop_m5_working_claim_certificate_binding (
                  epoch_id, claim_id, valid_from_revision,
                  valid_to_revision, certificate_digest
                ) VALUES (%s,%s,%s,%s,%s)
                RETURNING claim_id, valid_from_revision
                """,
                (
                    claim_binding.epoch_id,
                    claim_binding.claim_id,
                    claim_binding.valid_from_revision,
                    claim_binding.valid_to_revision,
                    claim_binding.certificate_digest,
                ),
            ).fetchone()
        else:
            inserted = cursor.execute(
                """
                UPDATE groundloop_m5_working_claim_certificate_binding
                SET valid_to_revision = %s
                WHERE epoch_id = %s AND claim_id = %s
                  AND valid_from_revision = %s AND valid_to_revision IS NULL
                  AND certificate_digest = %s
                RETURNING claim_id, valid_from_revision
                """,
                (
                    claim_binding.valid_to_revision,
                    claim_binding.epoch_id,
                    claim_binding.claim_id,
                    claim_binding.valid_from_revision,
                    claim_binding.certificate_digest,
                ),
            ).fetchone()
        if inserted is None or (
            _text(inserted[0]),
            int(inserted[1]),
        ) != (claim_binding.claim_id, claim_binding.valid_from_revision):
            raise EventConflictError("matching claim-binding stage changed")


def _matching_tier_12_write_plan(plan: _MatchingWritePlan) -> _MatchingWritePlan:
    """Project exactly the tier-11c--12b families from one sealed plan."""

    return _MatchingWritePlan(
        observation_rows=plan.observation_rows,
        edge_rows=plan.edge_rows,
        mask_rows=plan.mask_rows,
        hall_rows=plan.hall_rows,
        requirement_state_rows=plan.requirement_state_rows,
        group_state_rows=plan.group_state_rows,
        group_certificate_artifact_rows=plan.group_certificate_artifact_rows,
        group_binding_rows=plan.group_binding_rows,
    )


def _matching_tier_13_write_plan(plan: _MatchingWritePlan) -> _MatchingWritePlan:
    """Project exactly the tier-13 claim/certificate/binding families."""

    return _MatchingWritePlan(
        claim_state_rows=plan.claim_state_rows,
        claim_certificate_artifact_rows=plan.claim_certificate_artifact_rows,
        claim_binding_rows=plan.claim_binding_rows,
    )


def _matching_tier_14_write_plan(plan: _MatchingWritePlan) -> _MatchingWritePlan:
    """Project exactly the tier-14 answer-state family."""

    if plan.answer_state_rows and not plan.claim_state_rows:
        raise ValidationError("answer-state plan lacks its claim-state owner")
    return _MatchingWritePlan(answer_state_rows=plan.answer_state_rows)


def _stage_matching_write_plan(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    plan: _MatchingWritePlan,
    before_images: dict[tuple[str, bytes], object | None],
) -> None:
    """Retained monolithic wrapper over the exact tier-12/13/14 fragments."""

    _stage_matching_write_plan_fragment(
        cursor, intent, _matching_tier_12_write_plan(plan), before_images
    )
    _stage_matching_write_plan_fragment(
        cursor, intent, _matching_tier_13_write_plan(plan), before_images
    )
    _stage_matching_write_plan_fragment(
        cursor, intent, _matching_tier_14_write_plan(plan), before_images
    )


_STAGE_JOURNAL_RELATIONS = (
    "groundloop_m5_matching_image_working",
    "groundloop_m5_matching_observation_working",
    "groundloop_m5_matching_edge_working",
    "groundloop_m5_matching_hash_mask_working",
    "groundloop_m5_matching_hall_working",
    "groundloop_m5_working_requirement_state",
    "groundloop_m5_working_group_state",
    "groundloop_m5_working_claim_state",
    "groundloop_m5_working_answer_state",
    "groundloop_m5_group_certificate_artifact",
    "groundloop_m5_group_certificate_artifact_row",
    "groundloop_m5_claim_certificate_artifact",
    "groundloop_m5_working_group_certificate_binding",
    "groundloop_m5_working_claim_certificate_binding",
)


def _exact_stage_row_json(
    cursor: Cursor[Any],
    *,
    relation_name: str,
    key_parts: Sequence[object],
    query: str,
    parameters: Sequence[object],
) -> tuple[tuple[str, bytes], object]:
    row = cursor.execute(query, parameters).fetchone()
    if row is None:
        raise EventConflictError(
            "matching staged row differs from its prepared final after-image"
        )
    return (
        relation_name,
        _matching_journal_key_preimage(relation_name, key_parts),
    ), row[0]


def _expected_matching_stage_rows(
    cursor: Cursor[Any], prepared: _PreparedMatchingTransition
) -> dict[tuple[str, bytes], object]:
    """Load only exact planned rows and bind their complete live JSON bytes."""

    intent = prepared.official_intent
    plan = prepared.physical_and_logical_write_plan
    expected: dict[tuple[str, bytes], object] = {}

    def add(entry: tuple[tuple[str, bytes], object]) -> None:
        key, value = entry
        if key in expected:
            raise ValidationError("matching stage plan repeats a journal key")
        expected[key] = value

    add(
        _exact_stage_row_json(
            cursor,
            relation_name="groundloop_m5_matching_image_working",
            key_parts=(intent.resulting_epoch_id,),
            query="""
                SELECT to_jsonb(image)
                FROM groundloop_m5_matching_image_working AS image
                WHERE image.epoch_id = %s
                  AND image.base_epoch_id = %s
                  AND image.base_revision = %s
                  AND image.decision_policy_version = %s
                  AND image.updated_revision = %s
            """,
            parameters=(
                intent.resulting_epoch_id,
                prepared.matching_image_base_epoch_id,
                prepared.matching_image_base_revision,
                intent.decision_policy_version,
                intent.resulting_revision,
            ),
        )
    )
    for observation in plan.observation_rows:
        add(
            _exact_stage_row_json(
                cursor,
                relation_name="groundloop_m5_matching_observation_working",
                key_parts=(observation.epoch_id, observation.observation_id),
                query="""
                    SELECT to_jsonb(row_value)
                    FROM groundloop_m5_matching_observation_working AS row_value
                    WHERE row_value.epoch_id = %s
                      AND row_value.observation_id = %s
                      AND row_value.requirement_version_id = %s
                      AND row_value.group_version_id = %s
                      AND row_value.requirement_ordinal = %s
                      AND row_value.text_hash = %s
                      AND row_value.present = %s
                      AND row_value.updated_revision = %s
                """,
                parameters=(
                    observation.epoch_id,
                    observation.observation_id,
                    observation.requirement_version_id,
                    observation.group_version_id,
                    observation.requirement_ordinal,
                    observation.text_hash,
                    observation.present,
                    observation.updated_revision,
                ),
            )
        )
    for edge in plan.edge_rows:
        add(
            _exact_stage_row_json(
                cursor,
                relation_name="groundloop_m5_matching_edge_working",
                key_parts=(
                    edge.epoch_id,
                    edge.requirement_version_id,
                    edge.text_hash,
                ),
                query="""
                    SELECT to_jsonb(row_value)
                    FROM groundloop_m5_matching_edge_working AS row_value
                    WHERE row_value.epoch_id = %s
                      AND row_value.requirement_version_id = %s
                      AND row_value.text_hash = %s
                      AND row_value.group_version_id = %s
                      AND row_value.requirement_ordinal = %s
                      AND row_value.refcount = %s
                      AND row_value.updated_revision = %s
                """,
                parameters=(
                    edge.epoch_id,
                    edge.requirement_version_id,
                    edge.text_hash,
                    edge.group_version_id,
                    edge.requirement_ordinal,
                    edge.refcount,
                    edge.updated_revision,
                ),
            )
        )
    for mask in plan.mask_rows:
        add(
            _exact_stage_row_json(
                cursor,
                relation_name="groundloop_m5_matching_hash_mask_working",
                key_parts=(mask.epoch_id, mask.group_version_id, mask.text_hash),
                query="""
                    SELECT to_jsonb(row_value)
                    FROM groundloop_m5_matching_hash_mask_working AS row_value
                    WHERE row_value.epoch_id = %s
                      AND row_value.group_version_id = %s
                      AND row_value.text_hash = %s
                      AND row_value.mask = %s
                      AND row_value.updated_revision = %s
                """,
                parameters=(
                    mask.epoch_id,
                    mask.group_version_id,
                    mask.text_hash,
                    mask.mask,
                    mask.updated_revision,
                ),
            )
        )
    for hall in plan.hall_rows:
        add(
            _exact_stage_row_json(
                cursor,
                relation_name="groundloop_m5_matching_hall_working",
                key_parts=(hall.epoch_id, hall.group_version_id),
                query="""
                    SELECT to_jsonb(row_value)
                    FROM groundloop_m5_matching_hall_working AS row_value
                    WHERE row_value.epoch_id = %s
                      AND row_value.group_version_id = %s
                      AND row_value.present = %s
                      AND row_value.requirement_count IS NOT DISTINCT FROM %s
                      AND row_value.mask_histogram
                          IS NOT DISTINCT FROM %s::bigint[]
                      AND row_value.neighbor_counts
                          IS NOT DISTINCT FROM %s::bigint[]
                      AND row_value.deficiencies
                          IS NOT DISTINCT FROM %s::bigint[]
                      AND row_value.maximum_deficiency IS NOT DISTINCT FROM %s
                      AND row_value.matching_size IS NOT DISTINCT FROM %s
                      AND row_value.distinct_hash_count IS NOT DISTINCT FROM %s
                      AND row_value.updated_revision = %s
                """,
                parameters=(
                    hall.epoch_id,
                    hall.group_version_id,
                    hall.present,
                    hall.requirement_count,
                    (
                        None
                        if hall.mask_histogram is None
                        else list(hall.mask_histogram)
                    ),
                    (
                        None
                        if hall.neighbor_counts is None
                        else list(hall.neighbor_counts)
                    ),
                    (None if hall.deficiencies is None else list(hall.deficiencies)),
                    hall.maximum_deficiency,
                    hall.matching_size,
                    hall.distinct_hash_count,
                    hall.updated_revision,
                ),
            )
        )
    for requirement_write in plan.requirement_state_rows:
        requirement_state = requirement_write.state
        add(
            _exact_stage_row_json(
                cursor,
                relation_name="groundloop_m5_working_requirement_state",
                key_parts=(
                    intent.resulting_epoch_id,
                    requirement_state.requirement_version_id,
                ),
                query="""
                    SELECT to_jsonb(row_value)
                    FROM groundloop_m5_working_requirement_state AS row_value
                    WHERE row_value.epoch_id = %s
                      AND row_value.requirement_version_id = %s
                      AND row_value.witness_hashes = %s::text[]
                      AND row_value.supporting_observation_ids = %s::text[]
                      AND row_value.witness_count = %s
                      AND row_value.satisfied = %s
                      AND row_value.decision_policy_version = %s
                      AND row_value.updated_revision = %s
                """,
                parameters=(
                    intent.resulting_epoch_id,
                    requirement_state.requirement_version_id,
                    list(requirement_state.witness_hashes),
                    list(requirement_state.supporting_observation_ids),
                    requirement_state.witness_count,
                    requirement_state.satisfied,
                    requirement_write.decision_policy_version,
                    intent.resulting_revision,
                ),
            )
        )
    for group_write in plan.group_state_rows:
        group_state = group_write.state
        add(
            _exact_stage_row_json(
                cursor,
                relation_name="groundloop_m5_working_group_state",
                key_parts=(
                    intent.resulting_epoch_id,
                    group_state.group_version_id,
                ),
                query="""
                    SELECT to_jsonb(row_value)
                    FROM groundloop_m5_working_group_state AS row_value
                    WHERE row_value.epoch_id = %s
                      AND row_value.group_version_id = %s
                      AND row_value.requirement_count = %s
                      AND row_value.satisfied_count = %s
                      AND row_value.matching_size = %s
                      AND row_value.complete = %s
                      AND row_value.decision_policy_version = %s
                      AND row_value.certificate_digest IS NOT DISTINCT FROM %s
                      AND row_value.updated_revision = %s
                """,
                parameters=(
                    intent.resulting_epoch_id,
                    group_state.group_version_id,
                    group_state.requirement_count,
                    group_state.satisfied_count,
                    group_state.matching_size,
                    group_state.complete,
                    group_write.decision_policy_version,
                    group_write.certificate_digest,
                    intent.resulting_revision,
                ),
            )
        )
    for claim_write in plan.claim_state_rows:
        claim_state = claim_write.state
        add(
            _exact_stage_row_json(
                cursor,
                relation_name="groundloop_m5_working_claim_state",
                key_parts=(intent.resulting_epoch_id, claim_state.claim_id),
                query="""
                    SELECT to_jsonb(row_value)
                    FROM groundloop_m5_working_claim_state AS row_value
                    WHERE row_value.epoch_id = %s
                      AND row_value.claim_id = %s
                      AND row_value.support_count = %s
                      AND row_value.refute_count = %s
                      AND row_value.best_support_score IS NOT DISTINCT FROM %s
                      AND row_value.best_refute_score IS NOT DISTINCT FROM %s
                      AND row_value.supporting_observation_ids = %s::text[]
                      AND row_value.refuting_observation_ids = %s::text[]
                      AND row_value.complete_group_count = %s
                      AND row_value.complete_group_ids = %s::text[]
                      AND row_value.status = %s
                      AND row_value.decision_policy_version = %s
                      AND row_value.certificate_digest = %s
                      AND row_value.updated_revision = %s
                """,
                parameters=(
                    intent.resulting_epoch_id,
                    claim_state.claim_id,
                    claim_state.support_count,
                    claim_state.refute_count,
                    claim_state.best_support_score,
                    claim_state.best_refute_score,
                    list(claim_state.supporting_observation_ids),
                    list(claim_state.refuting_observation_ids),
                    claim_state.complete_group_count,
                    list(claim_state.complete_group_ids),
                    claim_state.status.value,
                    claim_write.decision_policy_version,
                    claim_write.certificate_digest,
                    intent.resulting_revision,
                ),
            )
        )
    for answer_state in plan.answer_state_rows:
        add(
            _exact_stage_row_json(
                cursor,
                relation_name="groundloop_m5_working_answer_state",
                key_parts=(
                    intent.resulting_epoch_id,
                    answer_state.answer_version_id,
                ),
                query="""
                    SELECT to_jsonb(row_value)
                    FROM groundloop_m5_working_answer_state AS row_value
                    WHERE row_value.epoch_id = %s
                      AND row_value.answer_version_id = %s
                      AND row_value.required_claim_count = %s
                      AND row_value.supported_count = %s
                      AND row_value.unsupported_count = %s
                      AND row_value.refuted_count = %s
                      AND row_value.conflicted_count = %s
                      AND row_value.status = %s
                      AND row_value.updated_revision = %s
                """,
                parameters=(
                    intent.resulting_epoch_id,
                    answer_state.answer_version_id,
                    answer_state.required_claim_count,
                    answer_state.supported_count,
                    answer_state.unsupported_count,
                    answer_state.refuted_count,
                    answer_state.conflicted_count,
                    answer_state.status.value,
                    intent.resulting_revision,
                ),
            )
        )
    for group_artifact in plan.group_certificate_artifact_rows:
        add(
            _exact_stage_row_json(
                cursor,
                relation_name="groundloop_m5_group_certificate_artifact",
                key_parts=(group_artifact.certificate_digest,),
                query="""
                    SELECT to_jsonb(row_value)
                    FROM groundloop_m5_group_certificate_artifact AS row_value
                    WHERE row_value.certificate_digest = %s
                      AND row_value.decision_policy_version = %s
                      AND row_value.certificate_version = %s
                      AND row_value.group_version_id = %s
                      AND row_value.requirement_count = %s
                """,
                parameters=(
                    group_artifact.certificate_digest,
                    group_artifact.decision_policy_version,
                    group_artifact.certificate_version,
                    group_artifact.group_version_id,
                    group_artifact.requirement_count,
                ),
            )
        )
        for certificate_row in group_artifact.rows:
            add(
                _exact_stage_row_json(
                    cursor,
                    relation_name=("groundloop_m5_group_certificate_artifact_row"),
                    key_parts=(
                        group_artifact.certificate_digest,
                        certificate_row.requirement_ordinal,
                    ),
                    query="""
                        SELECT to_jsonb(row_value)
                        FROM groundloop_m5_group_certificate_artifact_row
                          AS row_value
                        WHERE row_value.certificate_digest = %s
                          AND row_value.requirement_ordinal = %s
                          AND row_value.requirement_version_id = %s
                          AND row_value.text_hash = %s
                          AND row_value.selected_observation_id = %s
                    """,
                    parameters=(
                        group_artifact.certificate_digest,
                        certificate_row.requirement_ordinal,
                        certificate_row.requirement_version_id,
                        certificate_row.text_hash,
                        certificate_row.selected_observation_id,
                    ),
                )
            )
    for claim_artifact in plan.claim_certificate_artifact_rows:
        add(
            _exact_stage_row_json(
                cursor,
                relation_name="groundloop_m5_claim_certificate_artifact",
                key_parts=(claim_artifact.certificate_digest,),
                query="""
                    SELECT to_jsonb(row_value)
                    FROM groundloop_m5_claim_certificate_artifact AS row_value
                    WHERE row_value.certificate_digest = %s
                      AND row_value.certificate_version = %s
                      AND row_value.claim_id = %s
                      AND row_value.decision_policy_version = %s
                      AND row_value.support_kind = %s
                      AND row_value.direct_support_observation_id
                          IS NOT DISTINCT FROM %s
                      AND row_value.group_version_id IS NOT DISTINCT FROM %s
                      AND row_value.group_certificate_digest
                          IS NOT DISTINCT FROM %s
                      AND row_value.direct_refute_observation_id
                          IS NOT DISTINCT FROM %s
                """,
                parameters=(
                    claim_artifact.certificate_digest,
                    claim_artifact.certificate_version,
                    claim_artifact.claim_id,
                    claim_artifact.decision_policy_version,
                    claim_artifact.support_kind.value,
                    claim_artifact.direct_support_observation_id,
                    claim_artifact.group_version_id,
                    claim_artifact.group_certificate_digest,
                    claim_artifact.direct_refute_observation_id,
                ),
            )
        )
    for group_binding in plan.group_binding_rows:
        add(
            _exact_stage_row_json(
                cursor,
                relation_name=("groundloop_m5_working_group_certificate_binding"),
                key_parts=(
                    group_binding.epoch_id,
                    group_binding.group_version_id,
                    group_binding.valid_from_revision,
                ),
                query="""
                    SELECT to_jsonb(row_value)
                    FROM groundloop_m5_working_group_certificate_binding
                      AS row_value
                    WHERE row_value.epoch_id = %s
                      AND row_value.group_version_id = %s
                      AND row_value.valid_from_revision = %s
                      AND row_value.valid_to_revision IS NOT DISTINCT FROM %s
                      AND row_value.certificate_digest = %s
                """,
                parameters=(
                    group_binding.epoch_id,
                    group_binding.group_version_id,
                    group_binding.valid_from_revision,
                    group_binding.valid_to_revision,
                    group_binding.certificate_digest,
                ),
            )
        )
    for claim_binding in plan.claim_binding_rows:
        add(
            _exact_stage_row_json(
                cursor,
                relation_name=("groundloop_m5_working_claim_certificate_binding"),
                key_parts=(
                    claim_binding.epoch_id,
                    claim_binding.claim_id,
                    claim_binding.valid_from_revision,
                ),
                query="""
                    SELECT to_jsonb(row_value)
                    FROM groundloop_m5_working_claim_certificate_binding
                      AS row_value
                    WHERE row_value.epoch_id = %s
                      AND row_value.claim_id = %s
                      AND row_value.valid_from_revision = %s
                      AND row_value.valid_to_revision IS NOT DISTINCT FROM %s
                      AND row_value.certificate_digest = %s
                """,
                parameters=(
                    claim_binding.epoch_id,
                    claim_binding.claim_id,
                    claim_binding.valid_from_revision,
                    claim_binding.valid_to_revision,
                    claim_binding.certificate_digest,
                ),
            )
        )
    return expected


def _validate_matching_stage_journal(
    cursor: Cursor[Any], prepared: _PreparedMatchingTransition
) -> None:
    expected = _expected_matching_stage_rows(cursor, prepared)
    expected_old = _matching_stage_before_map(prepared)
    if set(expected_old) != set(expected):
        raise EventConflictError("matching prepared before/final key sets differ")
    rows = cursor.execute(
        """
        SELECT relation_name, key_preimage, first_old, final_new,
               first_operation, last_operation, mutation_count,
               saw_insert, saw_update, saw_delete
        FROM pg_temp.groundloop_m5_matching_change_journal
        WHERE relation_name = ANY(%s)
        """,
        (list(_STAGE_JOURNAL_RELATIONS),),
    ).fetchall()
    actual_keys = {(_text(row[0]), bytes(row[1])) for row in rows}
    if actual_keys != set(expected) or len(actual_keys) != len(rows):
        raise EventConflictError(
            "matching stage journal is not an exact planned-key bijection"
        )
    for row in rows:
        key = (_text(row[0]), bytes(row[1]))
        first_old = expected_old[key]
        operation = "INSERT" if first_old is None else "UPDATE"
        if (
            row[2] != first_old
            or row[3] != expected[key]
            or _text(row[4]) != operation
            or _text(row[5]) != operation
            or int(row[6]) != 1
            or bool(row[7]) != (operation == "INSERT")
            or bool(row[8]) != (operation == "UPDATE")
            or bool(row[9])
        ):
            raise EventConflictError(
                "matching stage journal operation or after-image differs"
            )


def _validate_prepared_matching_write_plan(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    prepared: _PreparedMatchingTransition,
) -> None:
    """Reject mutable prepared evidence before the first D25 stage write."""

    plan = prepared.physical_and_logical_write_plan
    artifact = prepared.prewrite_patch_artifact
    if intent.source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN:
        (
            recomputed_artifact,
            recomputed_plan,
            recomputed_counts,
            recomputed_requirement_count,
        ) = _structural_first_application_plan(
            cursor,
            intent,
            expected_document_direct_plan=plan.document_direct_plan,
        )
        if recomputed_artifact != artifact:
            raise EventConflictError(
                "matching prepared patch or held before image changed"
            )
        if recomputed_plan != plan:
            raise EventConflictError("matching prepared write plan changed")
        if recomputed_counts != prepared.d24_owned_planned_write_counts:
            raise EventConflictError("matching planned write counts changed")
        if (
            recomputed_requirement_count
            != prepared.requirement_state_write_count_diagnostic
        ):
            raise EventConflictError(
                "matching requirement-state write diagnostic changed"
            )
        _require_structural_absence_plan(cursor, intent, plan)
        if plan.observation_currency_rows:
            document_currency_before = _document_currency_before_images_for_intent(
                cursor, intent
            )
            if prepared.observation_currency_before_images != document_currency_before:
                raise EventConflictError("matching prepared document currency changed")
        elif prepared.observation_currency_before_images:
            raise EventConflictError(
                "structural matching prepared unexpected currency authority"
            )
    elif intent.source_kind is M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION:
        (
            recomputed_artifact,
            recomputed_plan,
            recomputed_counts,
            recomputed_requirement_count,
            requirement_currency_before,
            _,
        ) = _requirement_first_application_plan(cursor, intent, headers_installed=True)
        if recomputed_artifact != artifact:
            raise EventConflictError(
                "matching prepared patch or held before image changed"
            )
        if recomputed_plan != plan:
            raise EventConflictError("matching prepared write plan changed")
        if recomputed_counts != prepared.d24_owned_planned_write_counts:
            raise EventConflictError("matching planned write counts changed")
        if (
            recomputed_requirement_count
            != prepared.requirement_state_write_count_diagnostic
        ):
            raise EventConflictError(
                "matching requirement-state write diagnostic changed"
            )
        if prepared.observation_currency_before_images != (
            requirement_currency_before,
        ):
            raise EventConflictError("matching prepared observation currency changed")
    elif intent.source_kind is M5PersistedMatchingSourceKind.DIRECT_TRANSITION:
        reservation = prepared.direct_reservation_or_none
        stage_result = prepared.direct_m4_stage_evidence_or_none
        assert isinstance(reservation, _DirectMatchingReservation)
        assert isinstance(stage_result, _DirectM4StageResult)
        _validate_direct_stage_evidence(
            cursor,
            reservation,
            stage_result,
            expected_reservation_phase="consumed",
            expected_stage_phase="consumed",
        )
        official_projection, official_logical = _direct_after_stage_logical_plan(
            cursor, reservation.precursor
        )
        if (
            official_projection != reservation.d25_projection
            or official_logical != reservation.logical_plan
            or _direct_official_intent(reservation.precursor, official_projection)
            != intent
        ):
            raise EventConflictError("direct official intent changed")
        (
            recomputed_artifact,
            recomputed_plan,
            recomputed_counts,
            recomputed_requirement_count,
        ) = _direct_first_application_plan(intent, reservation)
        if recomputed_artifact != artifact:
            raise EventConflictError(
                "matching prepared patch or held before image changed"
            )
        if recomputed_plan != plan:
            raise EventConflictError("matching prepared write plan changed")
        if recomputed_counts != prepared.d24_owned_planned_write_counts:
            raise EventConflictError("matching planned write counts changed")
        if (
            recomputed_requirement_count
            != prepared.requirement_state_write_count_diagnostic
        ):
            raise EventConflictError(
                "matching requirement-state write diagnostic changed"
            )
        if prepared.observation_currency_before_images:
            raise EventConflictError(
                "direct matching prepared unexpected currency authority"
            )
    else:
        raise InvalidEventError(
            f"{intent.source_kind.value} has no materialized write plan"
        )
    recomputed_counts = _D24OwnedWriteCounts(
        group_state_write_count=len(plan.group_state_rows),
        claim_state_write_count=len(plan.claim_state_rows),
        answer_state_write_count=len(plan.answer_state_rows),
        certificate_binding_write_count=(
            len(plan.group_binding_rows) + len(plan.claim_binding_rows)
        ),
        public_delta_write_count=artifact.work.public_status_deltas,
    )
    if recomputed_counts != prepared.d24_owned_planned_write_counts:
        raise EventConflictError("matching planned write counts changed")
    if prepared.requirement_state_write_count_diagnostic != len(
        plan.requirement_state_rows
    ):
        raise EventConflictError("matching requirement-state write diagnostic changed")
    if prepared.d25_contribution_work != artifact.work:
        raise EventConflictError("matching contribution work changed")
    if prepared.expected_patch_digest is not None and (
        prepared.expected_patch_digest != artifact.patch.patch_digest
    ):
        raise EventConflictError("computed matching patch digest differs")
    if prepared.expected_work is not None and (prepared.expected_work != artifact.work):
        raise EventConflictError("computed matching work differs")
    if _capture_matching_stage_before_images(cursor, intent, plan) != (
        prepared.stage_before_images
    ):
        raise EventConflictError("matching staged before image changed")


def _stage_matching_image(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    prepared: _PreparedMatchingTransition,
) -> None:
    """Write the one tier-11b matching-image row from sealed authority."""

    before_images = _matching_stage_before_map(prepared)
    image_before = _stage_before_row(
        before_images,
        "groundloop_m5_matching_image_working",
        (intent.resulting_epoch_id,),
    )
    if intent.source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN:
        if image_before is not None:
            raise EventConflictError("structural matching image target appeared")
        cursor.execute(
            """
            INSERT INTO groundloop_m5_matching_image_working (
              epoch_id, base_epoch_id, base_revision,
              decision_policy_version, updated_revision
            ) VALUES (%s,%s,%s,%s,%s)
            """,
            (
                intent.resulting_epoch_id,
                intent.before_epoch_id,
                intent.before_revision,
                intent.decision_policy_version,
                intent.resulting_revision,
            ),
        )
    else:
        if image_before is None:
            raise EventConflictError("later matching image target disappeared")
        updated = cursor.execute(
            """
            UPDATE groundloop_m5_matching_image_working
            SET updated_revision = %s
            WHERE epoch_id = %s AND updated_revision = %s
            RETURNING epoch_id
            """,
            (
                intent.resulting_revision,
                intent.resulting_epoch_id,
                prepared.prewrite_matching_revision,
            ),
        ).fetchone()
        if updated is None:
            raise EventConflictError("matching image changed before stage")


def _validate_expected_document_direct_m4_images(
    cursor: Cursor[Any],
    images: tuple[_DirectStageImage, ...],
    *,
    relation_name: str,
    epoch_id: int,
) -> None:
    """Require one epoch-wide M4 family to equal its sealed after-images."""

    if relation_name == "groundloop_m4_working_claim_state":
        key_columns = ("epoch_id", "claim_id")
        rows = cursor.execute(
            """
            SELECT epoch_id, claim_id, to_jsonb(row_value)
            FROM groundloop_m4_working_claim_state AS row_value
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchall()
    elif relation_name == "groundloop_m4_working_answer_state":
        key_columns = ("epoch_id", "answer_version_id")
        rows = cursor.execute(
            """
            SELECT epoch_id, answer_version_id, to_jsonb(row_value)
            FROM groundloop_m4_working_answer_state AS row_value
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchall()
    else:
        raise EventConflictError("document direct M4 after-image changed")

    actual: dict[tuple[object, ...], object] = {}
    for row in rows:
        actual_key = (int(row[0]), _text(row[1]))
        if actual_key in actual:
            raise EventConflictError("document direct M4 staged keys repeat")
        actual[actual_key] = row[2]

    expected: dict[tuple[object, ...], object] = {}
    for image in images:
        if (
            type(image) is not _DirectStageImage
            or type(image.coordinate) is not _DirectStageCoordinate
            or image.coordinate.relation_name != relation_name
            or image.coordinate.key_columns != key_columns
            or len(image.coordinate.key_parts) != 2
            or image.coordinate.key_parts[0] != epoch_id
            or not isinstance(image.row_json, dict)
        ):
            raise EventConflictError("document direct M4 after-image changed")
        expected_key = image.coordinate.key_parts
        if expected_key in expected:
            raise EventConflictError("document direct M4 after-image keys repeat")
        expected[expected_key] = image.row_json

    if set(actual) != set(expected):
        raise EventConflictError("document direct M4 staged keys differ")
    if actual != expected:
        raise EventConflictError("document direct M4 staged row differs")


def _stage_prepared_matching_transition_through_tier_12(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    prepared: _PreparedMatchingTransition,
) -> _PreparedMatchingTransition:
    """Validate originals once, then write exact D25 tiers 11a--12b."""

    _validate_prepared_identity(
        cursor, intent, prepared, phase=_MatchingPhase.READY_TO_ADVANCE
    )
    _validate_prepared_matching_write_plan(cursor, intent, prepared)
    if _header_images(cursor, intent.resulting_epoch_id) != (
        prepared.base_header_after_image,
        prepared.runtime_header_after_image,
    ):
        raise EventConflictError("matching header after-image changed before stage")
    _stage_observation_currency_plan(
        cursor,
        intent,
        prepared.physical_and_logical_write_plan,
        prepared.observation_currency_before_images,
    )
    _stage_matching_image(cursor, intent, prepared)
    _stage_matching_write_plan_fragment(
        cursor,
        intent,
        _matching_tier_12_write_plan(prepared.physical_and_logical_write_plan),
        _matching_stage_before_map(prepared),
    )
    prepared.phase = _MatchingPhase.STAGED_THROUGH_TIER_12
    return prepared


def _stage_prepared_matching_transition_through_tier_13(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    prepared: _PreparedMatchingTransition,
) -> _PreparedMatchingTransition:
    """Write only exact D25 tier-13 claim/certificate/binding rows."""

    _validate_prepared_identity(
        cursor, intent, prepared, phase=_MatchingPhase.STAGED_THROUGH_TIER_12
    )
    plan = prepared.physical_and_logical_write_plan
    if plan.document_direct_plan is not None:
        _validate_expected_document_direct_m4_images(
            cursor,
            plan.expected_direct_m4_claim_after_images,
            relation_name="groundloop_m4_working_claim_state",
            epoch_id=intent.resulting_epoch_id,
        )
    _stage_matching_write_plan_fragment(
        cursor,
        intent,
        _matching_tier_13_write_plan(plan),
        _matching_stage_before_map(prepared),
    )
    prepared.phase = _MatchingPhase.STAGED_THROUGH_TIER_13
    return prepared


def _stage_prepared_matching_transition_through_tier_14(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    prepared: _PreparedMatchingTransition,
) -> _PreparedMatchingTransition:
    """Write only exact D25 tier-14 answer rows and expose STAGED."""

    _validate_prepared_identity(
        cursor, intent, prepared, phase=_MatchingPhase.STAGED_THROUGH_TIER_13
    )
    plan = prepared.physical_and_logical_write_plan
    if plan.document_direct_plan is not None:
        _validate_expected_document_direct_m4_images(
            cursor,
            plan.expected_direct_m4_answer_after_images,
            relation_name="groundloop_m4_working_answer_state",
            epoch_id=intent.resulting_epoch_id,
        )
    _stage_matching_write_plan_fragment(
        cursor,
        intent,
        _matching_tier_14_write_plan(plan),
        _matching_stage_before_map(prepared),
    )
    prepared.phase = _MatchingPhase.STAGED
    return prepared


def _stage_prepared_matching_transition(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    prepared: _PreparedMatchingTransition,
) -> _PreparedMatchingTransition:
    """Retained wrapper that advances the exact D25 tier phases in order."""

    _stage_prepared_matching_transition_through_tier_12(cursor, intent, prepared)
    _stage_prepared_matching_transition_through_tier_13(cursor, intent, prepared)
    return _stage_prepared_matching_transition_through_tier_14(cursor, intent, prepared)


def _finalize_prepared_matching_transition(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    prepared: _PreparedMatchingTransition,
    expected_patch_digest: str | None = None,
    expected_work: M5OverlayWork | None = None,
) -> M5PersistedMatchingPatchReceipt:
    """Consume prepared authority and write only D25 tiers 15i--15k."""

    if expected_patch_digest is not None:
        _require_sha256("expected_patch_digest", expected_patch_digest)
    if expected_work is not None and type(expected_work) is not M5OverlayWork:
        raise ValidationError("expected_work must be an exact M5OverlayWork")
    _validate_prepared_identity(cursor, intent, prepared, phase=_MatchingPhase.STAGED)
    if intent.source_kind is M5PersistedMatchingSourceKind.DIRECT_TRANSITION:
        _validate_direct_finalizer_authority(cursor, intent, prepared)
    artifact = prepared.prewrite_patch_artifact
    if (
        expected_patch_digest != prepared.expected_patch_digest
        or expected_work != prepared.expected_work
    ):
        raise EventConflictError("matching compare-only expectation changed")
    if expected_patch_digest is not None and (
        expected_patch_digest != artifact.patch.patch_digest
    ):
        raise EventConflictError("computed matching patch digest differs")
    if expected_work is not None and expected_work != artifact.work:
        raise EventConflictError("computed matching work differs")
    if _header_images(cursor, intent.resulting_epoch_id) != (
        prepared.base_header_after_image,
        prepared.runtime_header_after_image,
    ):
        raise EventConflictError("matching installed header after-image changed")
    working = cursor.execute(
        """
        SELECT base_epoch_id, base_revision, decision_policy_version,
               updated_revision
        FROM groundloop_m5_matching_image_working
        WHERE epoch_id = %s
        """,
        (intent.resulting_epoch_id,),
    ).fetchone()
    if working is None or (
        int(working[0]) != prepared.matching_image_base_epoch_id
        or int(working[1]) != prepared.matching_image_base_revision
        or _text(working[2]) != intent.decision_policy_version
        or int(working[3]) != intent.resulting_revision
    ):
        raise EventConflictError("matching staged image changed before finalization")
    _validate_staged_observation_currency(cursor, prepared)
    _validate_matching_stage_journal(cursor, prepared)
    _insert_or_validate_matching_artifact(cursor, artifact)
    if _lock_matching_contribution(cursor, intent) is not None:
        raise EventConflictError("matching contribution appeared before finalization")
    contribution = _matching_contribution(artifact)
    _insert_contribution(cursor, contribution)
    accumulated = _insert_or_advance_matching_accumulator(
        cursor,
        contribution,
        prewrite_matching_revision=prepared.prewrite_matching_revision,
    )
    prepared.phase = _MatchingPhase.CONSUMED
    return M5PersistedMatchingPatchReceipt(
        artifact.patch,
        contribution.contribution_digest,
        accumulated,
        intent.resulting_revision,
        False,
    )


def _stable_m4_digest(*parts: str) -> str:
    """Mirror the frozen M4 length-framed UTF-8 digest locally."""

    digest = hashlib.sha256()
    for part in parts:
        encoded = part.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _direct_transition_payload_hash(
    *,
    expected_revision: int,
    scope_delta: int,
    claim_job_deltas: Sequence[tuple[str, int]],
) -> str:
    payload = {
        "kind": "delta",
        "expected_revision": expected_revision,
        "scope_delta": scope_delta,
        "claim_job_deltas": [
            {"claim_id": claim_id, "delta": delta}
            for claim_id, delta in claim_job_deltas
        ],
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_direct_deltas(
    values: Sequence[tuple[str, int]],
) -> tuple[tuple[str, int], ...]:
    totals: dict[str, int] = {}
    for object_id, delta in values:
        _require_text("direct delta object_id", object_id)
        if isinstance(delta, bool) or not isinstance(delta, int) or delta == 0:
            raise ValidationError("direct counter delta must be a nonzero integer")
        totals[object_id] = totals.get(object_id, 0) + delta
    return tuple(
        (object_id, delta) for object_id, delta in sorted(totals.items()) if delta != 0
    )


def _direct_candidate_policy(
    cursor: Cursor[Any], candidate_policy_id: str, *, lock: bool
) -> M4CandidatePolicyManifest:
    """Validate the complete immutable M4 policy row and JSON identity."""

    suffix = " FOR UPDATE" if lock else ""
    row = cursor.execute(
        """
        SELECT candidate_policy_id, policy_hash,
               embedding_model_artifact_id, decision_policy_version,
               claim_role_template_hash, chunk_role_template_hash,
               vector_method_version, vector_index_kind,
               vector_index_build_config_hash, vector_search_config_hash,
               lexical_method_version, lexical_config_hash,
               lexical_postgres_version, lexical_regconfig_identity,
               claim_registry_snapshot_id, claim_count, fusion_version,
               approximate_cap_per_inserted_chunk, frontier_depth, manifest
        FROM groundloop_candidate_policy
        WHERE candidate_policy_id = %s
        """
        + suffix,
        (candidate_policy_id,),
    ).fetchone()
    if row is None or type(row[19]) is not dict:
        raise EventConflictError("direct candidate-policy manifest is absent")
    payload = row[19]
    expected_keys = {
        "policy_id",
        "policy_hash",
        "embedding_model_artifact_id",
        "claim_role_template_hash",
        "chunk_role_template_hash",
        "vector_method_version",
        "vector_index_kind",
        "vector_index_build_config_hash",
        "vector_search_config_hash",
        "lexical_method_version",
        "lexical_config_hash",
        "lexical_postgres_version",
        "lexical_regconfig_identity",
        "claim_registry_snapshot_id",
        "claim_count",
        "fusion_version",
        "approximate_cap_per_inserted_chunk",
        "frontier_depth",
        "verifier_execution_spec_hash",
        "decision_policy_version",
        "lineage_safety_override",
    }
    integer_fields = (
        "claim_count",
        "approximate_cap_per_inserted_chunk",
        "frontier_depth",
    )
    if set(payload) != expected_keys or (
        any(type(payload[field]) is not int for field in integer_fields)
        or type(payload["lineage_safety_override"]) is not bool
    ):
        raise EventConflictError("direct candidate-policy manifest changed")
    try:
        policy = M4CandidatePolicyManifest(
            policy_id=_text(payload["policy_id"]),
            policy_hash=_sha256_text(payload["policy_hash"]),
            embedding_model_artifact_id=_text(payload["embedding_model_artifact_id"]),
            claim_role_template_hash=_sha256_text(payload["claim_role_template_hash"]),
            chunk_role_template_hash=_sha256_text(payload["chunk_role_template_hash"]),
            vector_method_version=_text(payload["vector_method_version"]),
            vector_index_kind=VectorIndexKind(_text(payload["vector_index_kind"])),
            vector_index_build_config_hash=_sha256_text(
                payload["vector_index_build_config_hash"]
            ),
            vector_search_config_hash=_sha256_text(
                payload["vector_search_config_hash"]
            ),
            lexical_method_version=_text(payload["lexical_method_version"]),
            lexical_config_hash=_sha256_text(payload["lexical_config_hash"]),
            lexical_postgres_version=_text(payload["lexical_postgres_version"]),
            lexical_regconfig_identity=_text(payload["lexical_regconfig_identity"]),
            claim_registry_snapshot_id=_text(payload["claim_registry_snapshot_id"]),
            claim_count=int(payload["claim_count"]),
            fusion_version=_text(payload["fusion_version"]),
            approximate_cap_per_inserted_chunk=int(
                payload["approximate_cap_per_inserted_chunk"]
            ),
            frontier_depth=int(payload["frontier_depth"]),
            verifier_execution_spec_hash=_sha256_text(
                payload["verifier_execution_spec_hash"]
            ),
            decision_policy_version=_text(payload["decision_policy_version"]),
            lineage_safety_override=payload["lineage_safety_override"],
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError(
            "direct candidate-policy manifest is invalid"
        ) from error
    relational_identity = (
        _text(row[0]),
        _sha256_text(row[1]),
        _text(row[2]),
        _text(row[3]),
        _sha256_text(row[4]),
        _sha256_text(row[5]),
        _text(row[6]),
        _text(row[7]),
        _sha256_text(row[8]),
        _sha256_text(row[9]),
        _text(row[10]),
        _sha256_text(row[11]),
        _text(row[12]),
        _text(row[13]),
        _text(row[14]),
        int(row[15]),
        _text(row[16]),
        int(row[17]),
        int(row[18]),
    )
    manifest_identity = (
        policy.policy_id,
        policy.policy_hash,
        policy.embedding_model_artifact_id,
        policy.decision_policy_version,
        policy.claim_role_template_hash,
        policy.chunk_role_template_hash,
        policy.vector_method_version,
        policy.vector_index_kind.value,
        policy.vector_index_build_config_hash,
        policy.vector_search_config_hash,
        policy.lexical_method_version,
        policy.lexical_config_hash,
        policy.lexical_postgres_version,
        policy.lexical_regconfig_identity,
        policy.claim_registry_snapshot_id,
        policy.claim_count,
        policy.fusion_version,
        policy.approximate_cap_per_inserted_chunk,
        policy.frontier_depth,
    )
    if relational_identity != manifest_identity:
        raise EventConflictError("direct candidate-policy columns changed")
    return policy


def _direct_verifier_artifact_from_observation(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    job_id: str,
    claim_id: str,
    chunk_version_id: str,
    decision_policy: DecisionPolicy,
    execution_row: Sequence[Any],
    observation_row: Sequence[Any],
    model_authority: Sequence[Any],
    prompt_authority: Sequence[Any],
) -> tuple[PairVerificationArtifact, PairVerificationInput]:
    """Derive the exact artifact locator from persisted verifier output rows."""

    input_row = cursor.execute(
        """
        SELECT claim.text, claim.required, claim.answer_version_id,
               chunk.document_version_id, chunk.chunk_index, chunk.text,
               chunk.text_hash, provenance.chunker_artifact_id
        FROM groundloop_claim AS claim
        CROSS JOIN groundloop_chunk_version AS chunk
        JOIN groundloop_chunk_provenance AS provenance
          ON provenance.chunk_version_id = chunk.chunk_version_id
        WHERE claim.claim_id = %s AND chunk.chunk_version_id = %s
        """,
        (claim_id, chunk_version_id),
    ).fetchone()
    if input_row is None:
        raise EventConflictError("direct verifier pair input is incomplete")
    citations = tuple(
        _text(row[0])
        for row in cursor.execute(
            """
            SELECT chunk_version_id
            FROM groundloop_answer_citation
            WHERE answer_version_id = %s
            ORDER BY citation_ordinal
            """,
            (_text(input_row[2]),),
        ).fetchall()
    )
    try:
        pair = PairKey(claim_id, chunk_version_id)
        pair_input = PairVerificationInput(
            pair=pair,
            claim_text=_text(input_row[0]),
            claim_required=bool(input_row[1]),
            claim_cited_chunk_version_ids=citations,
            document_version_id=_text(input_row[3]),
            chunk_index=int(input_row[4]),
            chunk_text=_text(input_row[5]),
            chunk_text_hash=_sha256_text(input_row[6]),
            chunker_artifact_id=_text(input_row[7]),
        )
        raw_logits_value = execution_row[10]
        if not isinstance(raw_logits_value, (list, tuple)):
            raise ValidationError("direct verifier raw logits changed")
        raw_logits = tuple(float(value) for value in raw_logits_value)
        if len(raw_logits) != 3:
            raise ValidationError("direct verifier raw logits changed")
        calibrated = logits_to_score_triple(raw_logits, float(execution_row[9]))
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError("direct verifier pair input is malformed") from error

    try:
        observation = SemanticObservation(
            observation_id=_text(observation_row[0]),
            subject_kind=SubjectKind(_text(observation_row[1])),
            subject_id=_text(observation_row[2]),
            chunk_version_id=_text(observation_row[3]),
            task_type=_text(observation_row[4]),
            support_score=float(observation_row[5]),
            refute_score=float(observation_row[6]),
            neutral_score=float(observation_row[7]),
            producer=ModelStamp(
                _text(observation_row[8]),
                _text(observation_row[9]),
                _text(observation_row[10]),
            ),
            input_hash=_sha256_text(observation_row[11]),
        )
        scores = ScoreTriple(
            observation.support_score,
            observation.refute_score,
            observation.neutral_score,
        )
        for recorded, expected in zip(
            (scores.support, scores.refute, scores.neutral),
            (calibrated.support, calibrated.refute, calibrated.neutral),
            strict=True,
        ):
            if not math.isclose(recorded, expected, rel_tol=1e-12, abs_tol=1e-12):
                raise ValidationError("direct verifier calibrated scores changed")
        result = AIVerificationResult(
            claim_id=claim_id,
            chunk_version_id=chunk_version_id,
            candidate_id=stable_ai_digest(
                "verification-candidate-v1", claim_id, chunk_version_id
            ),
            model_artifact_id=_text(execution_row[3]),
            prompt_artifact_id=_text(execution_row[4]),
            calibration_version=_text(execution_row[7]),
            temperature=float(execution_row[9]),
            scores=scores,
            input_hash=observation.input_hash,
            raw_output_hash=_sha256_text(execution_row[11]),
            raw_logits=(raw_logits[0], raw_logits[1], raw_logits[2]),
        )
        operational_label = derive_operational_label(result, decision_policy)
        artifact_id = PairVerificationArtifact.build_artifact_id(
            pair=pair,
            pair_input_hash=pair_input.input_hash,
            execution_spec_hash=_sha256_text(execution_row[5]),
            result=result,
            decision_policy_hash=decision_policy_hash(decision_policy),
            operational_label=operational_label,
        )
        artifact = PairVerificationArtifact(
            artifact_id=artifact_id,
            pair=pair,
            pair_input_hash=pair_input.input_hash,
            execution_spec_hash=_sha256_text(execution_row[5]),
            decision_policy_version=decision_policy.policy_version,
            decision_policy_hash=decision_policy_hash(decision_policy),
            operational_label=operational_label,
            result=result,
        )
        expected_observation = artifact.to_semantic_observation(
            model_id=_text(model_authority[2]),
            model_revision=_text(model_authority[3]),
            prompt_version=_text(prompt_authority[2]),
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError(
            "direct verifier output closure is malformed"
        ) from error
    if (
        observation != expected_observation
        or len(observation_row) != 15
        or int(observation_row[12]) != epoch_id
        or _sha256_text(observation_row[13]) != artifact.result.raw_output_hash
        or observation_row[14] is not True
        or _text(execution_row[0]) != observation.observation_id
        or _text(execution_row[1]) != job_id
        or execution_row[12] is not None
        or artifact.result.model_artifact_id != _text(model_authority[0])
        or artifact.result.prompt_artifact_id != _text(prompt_authority[0])
        or _text(model_authority[1]) != "verification"
        or _text(prompt_authority[1]) != "verification"
        or pair_input.input_hash != _sha256_text(execution_row[6])
    ):
        raise EventConflictError("direct verifier output identity changed")
    return artifact, pair_input


def _validate_direct_verifier_judgment(
    artifact: PairVerificationArtifact,
    *,
    candidate_policy_id: str,
    judgment_row: Sequence[Any] | None,
) -> None:
    """Validate the one exact persisted judgment that names the artifact."""

    if judgment_row is None or len(judgment_row) != 13:
        raise EventConflictError("direct verifier judgment is absent")
    result = artifact.result
    if (
        _text(judgment_row[0])
        != _stable_m4_digest(
            "m4-pair-judgment-v1",
            artifact.artifact_id,
            _text(judgment_row[11]),
        )
        or _text(judgment_row[1]) != artifact.pair.claim_id
        or _text(judgment_row[2]) != artifact.pair.chunk_version_id
        or _text(judgment_row[3]) != "model"
        or _text(judgment_row[4]) != artifact.artifact_id
        or _text(judgment_row[5]) != artifact.decision_policy_version
        or _text(judgment_row[6]) != artifact.operational_label.value
        or float(judgment_row[7]) != result.scores.support
        or float(judgment_row[8]) != result.scores.refute
        or float(judgment_row[9]) != result.scores.neutral
        or _sha256_text(judgment_row[10]) != result.input_hash
        or not _text(judgment_row[11]).strip()
        or _text(judgment_row[12]) != candidate_policy_id
    ):
        raise EventConflictError("direct verifier judgment identity changed")


def _direct_precursor(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    resulting_revision: int,
    proposed_source_id: str,
    job_id: str,
    attempt_id: str,
) -> _DirectPrecursor:
    """Gather first, then lock the exact direct precursor in tiers 7--10."""

    # These reads discover only keys.  They intentionally take no row lock so
    # that every tier-7/8 authority and every tier-9 job can be acquired before
    # the first tier-10 attempt/result lock.
    job_hint = cursor.execute(
        """
        SELECT job_id, epoch_id, parent_job_id, job_kind, candidate_policy_id,
               payload_hash, execution_spec_hash, claim_id, chunk_version_id,
               expandable, job_state, child_closed, child_set_hash,
               completion_digest, result_artifact_id, result_artifact_hash,
               created_revision, completed_revision, created_at, completed_at
        FROM groundloop_semantic_job
        WHERE epoch_id = %s AND job_id = %s
        """,
        (epoch_id, job_id),
    ).fetchone()
    if job_hint is None:
        raise EventConflictError("direct matching precursor job is absent")
    job_kind = _text(job_hint[3])
    candidate_policy_id = _text(job_hint[4])
    parent_job_id = None if job_hint[2] is None else _text(job_hint[2])
    claim_id = None if job_hint[7] is None else _text(job_hint[7])
    chunk_version_id = None if job_hint[8] is None else _text(job_hint[8])
    expandable = bool(job_hint[9])

    update_hint = cursor.execute(
        """
        SELECT candidate_policy_id, previous_published_epoch_id,
               registry_snapshot_id
        FROM groundloop_m4_update
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    policy_hint = cursor.execute(
        """
        SELECT decision_policy_version, claim_registry_snapshot_id
        FROM groundloop_candidate_policy
        WHERE candidate_policy_id = %s
        """,
        (candidate_policy_id,),
    ).fetchone()
    if update_hint is None or policy_hint is None:
        raise EventConflictError("direct matching policy authority is absent")
    decision_policy_version = _text(policy_hint[0])

    target_dependencies_hint = cursor.execute(
        """
        SELECT child_job_id
        FROM groundloop_semantic_job_dependency
        WHERE epoch_id = %s AND parent_job_id = %s
        ORDER BY child_job_id COLLATE "C"
        """,
        (epoch_id, job_id),
    ).fetchall()
    target_child_ids = tuple(_text(row[0]) for row in target_dependencies_hint)
    parent_child_ids: tuple[str, ...] = ()
    if parent_job_id is not None:
        parent_child_ids = tuple(
            _text(row[0])
            for row in cursor.execute(
                """
                SELECT child_job_id
                FROM groundloop_semantic_job_dependency
                WHERE epoch_id = %s AND parent_job_id = %s
                ORDER BY child_job_id COLLATE "C"
                """,
                (epoch_id, parent_job_id),
            ).fetchall()
        )
    hinted_job_ids = tuple(
        sorted(
            {job_id, *target_child_ids, *parent_child_ids}
            | ({parent_job_id} if parent_job_id is not None else set())
        )
    )
    hinted_jobs = cursor.execute(
        """
        SELECT job_id, epoch_id, parent_job_id, job_kind, candidate_policy_id,
               payload_hash, execution_spec_hash, claim_id, chunk_version_id,
               expandable, job_state, child_closed, child_set_hash,
               completion_digest, result_artifact_id, result_artifact_hash,
               created_revision, completed_revision, created_at, completed_at
        FROM groundloop_semantic_job
        WHERE epoch_id = %s AND job_id = ANY(%s)
        ORDER BY job_id COLLATE "C"
        """,
        (epoch_id, list(hinted_job_ids)),
    ).fetchall()
    if tuple(_text(row[0]) for row in hinted_jobs) != hinted_job_ids:
        raise EventConflictError("direct matching precursor job closure is incomplete")
    hinted_by_id = {_text(row[0]): tuple(row) for row in hinted_jobs}

    execution_hint = None
    observation_hint = None
    admitted_pair_id: str | None = None
    if job_kind == "verify_pair":
        execution_hint = cursor.execute(
            """
            SELECT observation_id, job_id, admitted_pair_id,
                   model_artifact_id, prompt_artifact_id,
                   execution_spec_hash, pair_input_hash,
                   calibration_version, calibration_artifact_sha256,
                   temperature, raw_logits, raw_output_hash,
                   reused_from_observation_id
            FROM groundloop_m4_verification_execution
            WHERE job_id = %s
            """,
            (job_id,),
        ).fetchone()
        if execution_hint is None:
            raise EventConflictError("direct verifier execution is absent")
        admitted_pair_id = _text(execution_hint[2])
        observation_hint = cursor.execute(
            """
            SELECT observation_id, subject_kind::text, subject_id,
                   chunk_version_id, task_type, support_score, refute_score,
                   neutral_score, model_id, model_version, prompt_version,
                   input_hash, produced_epoch, raw_output_hash,
                   eligible_for_currency
            FROM groundloop_semantic_observation
            WHERE observation_id = %s
            """,
            (_text(execution_hint[0]),),
        ).fetchone()
        if observation_hint is None:
            raise EventConflictError("direct verifier observation is absent")

    hinted_claim_ids = {_text(row[7]) for row in hinted_jobs if row[7] is not None}
    if claim_id is not None:
        hinted_claim_ids.add(claim_id)
    ordered_claim_ids = tuple(sorted(hinted_claim_ids))
    hinted_chunk_ids = tuple(
        sorted({_text(row[8]) for row in hinted_jobs if row[8] is not None})
    )

    # Tiers 7 and 8: update/policy, immutable structural inputs, registry
    # membership, and claim ownership.  Nothing below this point may acquire
    # one of these rows for the first time.
    update_row = cursor.execute(
        """
        SELECT candidate_policy_id, previous_published_epoch_id,
               registry_snapshot_id, manifest
        FROM groundloop_m4_update
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if update_row is None:
        raise EventConflictError("direct matching precursor update is absent")
    typed_update_row = cursor.execute(
        """
        SELECT update_kind, previous_published_epoch_id,
               decision_policy_version
        FROM groundloop_m5_update
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if typed_update_row is None or (
        _text(typed_update_row[0])
        not in {"document_insert", "document_delete", "document_replace"}
        or int(typed_update_row[1]) != int(update_row[1])
        or _text(typed_update_row[2]) != decision_policy_version
    ):
        raise EventConflictError("direct matching typed update authority changed")
    chunk_rows = cursor.execute(
        """
        SELECT chunk_version_id, document_version_id, chunk_index, text,
               text_hash, chunker_version, valid_from_epoch, valid_to_epoch
        FROM groundloop_chunk_version
        WHERE chunk_version_id = ANY(%s)
        ORDER BY chunk_version_id COLLATE "C"
        FOR UPDATE
        """,
        (list(hinted_chunk_ids),),
    ).fetchall()
    if tuple(_text(row[0]) for row in chunk_rows) != hinted_chunk_ids:
        raise EventConflictError("direct matching chunk authority changed")
    provenance_rows = cursor.execute(
        """
        SELECT chunk_version_id, chunker_artifact_id, input_hash
        FROM groundloop_chunk_provenance
        WHERE chunk_version_id = ANY(%s)
        ORDER BY chunk_version_id COLLATE "C"
        FOR UPDATE
        """,
        (list(hinted_chunk_ids),),
    ).fetchall()
    if tuple(_text(row[0]) for row in provenance_rows) != hinted_chunk_ids:
        raise EventConflictError("direct matching chunk provenance changed")
    effective_chunk_rows = cursor.execute(
        """
        SELECT chunk_version_id
        FROM groundloop_m4_effective_chunk_version
        WHERE epoch_id = %s AND chunk_version_id = ANY(%s)
        ORDER BY chunk_version_id COLLATE "C"
        """,
        (epoch_id, list(hinted_chunk_ids)),
    ).fetchall()
    if tuple(_text(row[0]) for row in effective_chunk_rows) != hinted_chunk_ids:
        raise InvalidEventError("inactive direct completion is matching-inert")
    direct_policy = _direct_candidate_policy(cursor, candidate_policy_id, lock=True)
    decision_policy = cursor.execute(
        """
        SELECT support_threshold, refute_threshold, tie_rule_version
        FROM groundloop_decision_policy
        WHERE policy_version = %s
        FOR UPDATE
        """,
        (decision_policy_version,),
    ).fetchone()
    if decision_policy is None or _text(decision_policy[2]) != "v1":
        raise EventConflictError("direct decision policy changed")
    registry_snapshot_id = _text(update_row[2])
    snapshot_header = cursor.execute(
        """
        SELECT claim_count, claim_set_hash
        FROM groundloop_m4_claim_registry_snapshot
        WHERE claim_registry_snapshot_id = %s
        FOR UPDATE
        """,
        (registry_snapshot_id,),
    ).fetchone()
    if snapshot_header is None:
        raise EventConflictError("direct claim-registry snapshot authority changed")
    snapshot_identity = (int(snapshot_header[0]), _sha256_text(snapshot_header[1]))
    if snapshot_identity[0] != direct_policy.claim_count:
        raise EventConflictError("direct claim-registry snapshot authority changed")
    member_rows = cursor.execute(
        """
        SELECT claim_id
        FROM groundloop_m4_claim_registry_member
        WHERE claim_registry_snapshot_id = %s AND claim_id = ANY(%s)
        ORDER BY claim_id COLLATE "C"
        FOR UPDATE
        """,
        (registry_snapshot_id, list(ordered_claim_ids)),
    ).fetchall()
    claim_rows = cursor.execute(
        """
        SELECT claim_id, answer_version_id, text, required
        FROM groundloop_claim
        WHERE claim_id = ANY(%s)
        ORDER BY claim_id COLLATE "C"
        FOR UPDATE
        """,
        (list(ordered_claim_ids),),
    ).fetchall()
    if (
        tuple(_text(row[0]) for row in member_rows) != ordered_claim_ids
        or tuple(_text(row[0]) for row in claim_rows) != ordered_claim_ids
    ):
        raise EventConflictError("direct evaluation claim registry changed")
    claims_by_id = {_text(row[0]): tuple(row) for row in claim_rows}
    model_authority: tuple[object, ...] | None = None
    prompt_authority: tuple[object, ...] | None = None
    if execution_hint is not None:
        model = cursor.execute(
            """
            SELECT model_artifact_id, task, model_id, immutable_revision,
                   tokenizer_revision, config_hash, artifact_sha256
            FROM groundloop_model_artifact
            WHERE model_artifact_id = %s
            FOR UPDATE
            """,
            (_text(execution_hint[3]),),
        ).fetchone()
        prompt = cursor.execute(
            """
            SELECT prompt_artifact_id, task, version, template_hash,
                   decoding_config_hash
            FROM groundloop_prompt_artifact
            WHERE prompt_artifact_id = %s
            FOR UPDATE
            """,
            (_text(execution_hint[4]),),
        ).fetchone()
        if (
            model is None
            or prompt is None
            or (_text(model[1]) != "verification" or _text(prompt[1]) != "verification")
        ):
            raise EventConflictError("direct verifier model authority changed")
        model_authority = tuple(model)
        prompt_authority = tuple(prompt)
        owner = claims_by_id.get(claim_id or "")
        if owner is None:
            raise EventConflictError("direct verifier claim owner changed")
        cursor.execute(
            """
            SELECT answer_version_id, citation_ordinal, chunk_version_id
            FROM groundloop_answer_citation
            WHERE answer_version_id = %s
            ORDER BY citation_ordinal
            FOR UPDATE
            """,
            (_text(owner[1]),),
        ).fetchall()

    # Tier 9: lock the complete pre-gathered parent/child job set once, in
    # lexical order.  In particular no child job is first locked after an
    # attempt or dependency row.
    locked_jobs = cursor.execute(
        """
        SELECT job_id, epoch_id, parent_job_id, job_kind, candidate_policy_id,
               payload_hash, execution_spec_hash, claim_id, chunk_version_id,
               expandable, job_state, child_closed, child_set_hash,
               completion_digest, result_artifact_id, result_artifact_hash,
               created_revision, completed_revision, created_at, completed_at
        FROM groundloop_semantic_job
        WHERE epoch_id = %s AND job_id = ANY(%s)
        ORDER BY job_id COLLATE "C"
        FOR UPDATE
        """,
        (epoch_id, list(hinted_job_ids)),
    ).fetchall()
    if tuple(tuple(row) for row in locked_jobs) != tuple(
        hinted_by_id[item] for item in hinted_job_ids
    ):
        raise EventConflictError("direct matching precursor jobs changed")
    jobs_by_id = {_text(row[0]): tuple(row) for row in locked_jobs}
    job = jobs_by_id[job_id]

    # Tier 10 begins with the entire dense attempt history.  The cited attempt
    # must be the sole latest leased attempt and every predecessor must already
    # be a finished failure/expiry.
    all_attempts = cursor.execute(
        """
        SELECT attempt_id, job_id, execution_spec_hash, attempt_ordinal,
               lease_token_hash, attempt_state, lease_expires_at,
               started_at, finished_at
        FROM groundloop_semantic_job_attempt
        WHERE job_id = ANY(%s)
        ORDER BY job_id COLLATE "C", attempt_ordinal
        FOR UPDATE
        """,
        (list(hinted_job_ids),),
    ).fetchall()
    attempts = tuple(row for row in all_attempts if _text(row[1]) == job_id)
    for related_job_id in hinted_job_ids:
        related_attempts = tuple(
            row for row in all_attempts if _text(row[1]) == related_job_id
        )
        related_job = jobs_by_id[related_job_id]
        for ordinal, related_attempt in enumerate(related_attempts, start=1):
            if (
                int(related_attempt[3]) != ordinal
                or _text(related_attempt[0])
                != _stable_m4_digest("m4-job-attempt-v1", related_job_id, str(ordinal))
                or _sha256_text(related_attempt[2]) != _sha256_text(related_job[6])
                or _sha256_text(related_attempt[4])
                != _stable_m4_digest("m4-lease-token-v1", related_job_id, str(ordinal))
                or related_attempt[6] is None
                or related_attempt[7] is None
            ):
                raise EventConflictError("direct related attempt history changed")
    if not attempts:
        raise EventConflictError("direct matching precursor attempt is absent")
    for expected_ordinal, raw_attempt in enumerate(attempts, start=1):
        ordinal = int(raw_attempt[3])
        if (
            ordinal != expected_ordinal
            or _text(raw_attempt[0])
            != _stable_m4_digest("m4-job-attempt-v1", job_id, str(ordinal))
            or _text(raw_attempt[1]) != job_id
            or _sha256_text(raw_attempt[2]) != _sha256_text(job[6])
            or _sha256_text(raw_attempt[4])
            != _stable_m4_digest("m4-lease-token-v1", job_id, str(ordinal))
            or raw_attempt[6] is None
            or raw_attempt[7] is None
        ):
            raise EventConflictError("direct matching attempt history changed")
        state = _text(raw_attempt[5])
        if expected_ordinal < len(attempts):
            if state not in {"failed", "expired"} or raw_attempt[8] is None:
                raise EventConflictError("direct matching attempt history changed")
        elif (
            _text(raw_attempt[0]) != attempt_id
            or state != "leased"
            or raw_attempt[8] is not None
        ):
            raise EventConflictError("direct matching precursor is not preterminal")
    attempt_ordinal = int(attempts[-1][3])

    locked_execution: tuple[object, ...] | None = None
    verifier_artifact: PairVerificationArtifact | None = None
    if job_kind == "verify_pair":
        if (
            execution_hint is None
            or observation_hint is None
            or claim_id is None
            or chunk_version_id is None
            or model_authority is None
            or prompt_authority is None
        ):
            raise EventConflictError("direct verifier precursor is incomplete")
        execution = cursor.execute(
            """
            SELECT observation_id, job_id, admitted_pair_id,
                   model_artifact_id, prompt_artifact_id,
                   execution_spec_hash, pair_input_hash,
                   calibration_version, calibration_artifact_sha256,
                   temperature, raw_logits, raw_output_hash,
                   reused_from_observation_id
            FROM groundloop_m4_verification_execution
            WHERE job_id = %s
            FOR UPDATE
            """,
            (job_id,),
        ).fetchone()
        if execution is None or tuple(execution) != tuple(execution_hint):
            raise EventConflictError("direct verifier execution changed")
        locked_execution = tuple(execution)
        verifier_artifact, _ = _direct_verifier_artifact_from_observation(
            cursor,
            epoch_id=epoch_id,
            job_id=job_id,
            claim_id=claim_id,
            chunk_version_id=chunk_version_id,
            decision_policy=DecisionPolicy(
                decision_policy_version,
                float(decision_policy[0]),
                float(decision_policy[1]),
                _text(decision_policy[2]),
            ),
            execution_row=locked_execution,
            observation_row=tuple(observation_hint),
            model_authority=model_authority,
            prompt_authority=prompt_authority,
        )
        judgment = cursor.execute(
            """
            SELECT judgment_id, claim_id, chunk_version_id, source_kind,
                   source_artifact_id, decision_policy_or_guideline_id,
                   derived_label, support_score, refute_score,
                   neutral_score, input_hash, split_id, manifest_id
            FROM groundloop_pair_judgment
            WHERE claim_id = %s AND chunk_version_id = %s
              AND source_kind = 'model' AND source_artifact_id = %s
              AND decision_policy_or_guideline_id = %s AND input_hash = %s
            FOR UPDATE
            """,
            (
                claim_id,
                chunk_version_id,
                verifier_artifact.artifact_id,
                decision_policy_version,
                verifier_artifact.result.input_hash,
            ),
        ).fetchone()
        _validate_direct_verifier_judgment(
            verifier_artifact,
            candidate_policy_id=candidate_policy_id,
            judgment_row=judgment,
        )

    dependency_parents = tuple(
        sorted(
            {job_id if expandable else ""}
            | ({parent_job_id} if parent_job_id is not None else set()) - {""}
        )
    )
    dependency_rows = cursor.execute(
        """
        SELECT epoch_id, parent_job_id, child_job_id
        FROM groundloop_semantic_job_dependency
        WHERE epoch_id = %s AND parent_job_id = ANY(%s)
        ORDER BY parent_job_id COLLATE "C", child_job_id COLLATE "C"
        FOR UPDATE
        """,
        (epoch_id, list(dependency_parents)),
    ).fetchall()
    actual_dependency_map: dict[str, tuple[str, ...]] = {}
    for parent in dependency_parents:
        actual_dependency_map[parent] = tuple(
            _text(row[2]) for row in dependency_rows if _text(row[1]) == parent
        )
    if expandable and actual_dependency_map.get(job_id, ()) != target_child_ids:
        raise EventConflictError("direct discovery child closure changed")
    if parent_job_id is not None and (
        actual_dependency_map.get(parent_job_id, ()) != parent_child_ids
        or job_id not in parent_child_ids
    ):
        raise EventConflictError("direct verifier parent closure changed")

    raw_claim_deltas: list[tuple[str, int]] = []
    scope_delta = 0
    child_job_ids: tuple[str, ...] = ()
    observation_id: str | None = None
    result_artifact_id: str | None = None
    result_artifact_hash: str | None = None
    child_set_hash: str | None = None
    if expandable:
        child_job_ids = target_child_ids
        children = tuple(jobs_by_id[item] for item in child_job_ids)
        if any(
            _text(row[2]) != job_id
            or _text(row[3]) != "verify_pair"
            or _text(row[4]) != candidate_policy_id
            or row[7] is None
            or _text(row[10]) != "declared"
            for row in children
        ):
            raise EventConflictError("direct discovery child rows changed")
        if job_kind == "impact_discovery":
            scope = cursor.execute(
                """
                SELECT epoch_id, registry_snapshot_id, scope_kind,
                       explicit_claim_ids, closed_revision
                FROM groundloop_discovery_scope
                WHERE root_job_id = %s
                FOR UPDATE
                """,
                (job_id,),
            ).fetchone()
            if scope is None or (
                int(scope[0]) != epoch_id
                or _text(scope[1]) != registry_snapshot_id
                or _text(scope[2]) != "all_registered_claims"
                or scope[3] is not None
                or scope[4] is not None
            ):
                raise EventConflictError("direct discovery scope changed")
        discovery = cursor.execute(
            """
            SELECT root_job_id, epoch_id, result_artifact_id,
                   result_artifact_hash, fallback_satisfied,
                   channel_hit_count, admitted_pair_count,
                   channel_set_hash, admitted_pair_set_hash
            FROM groundloop_m4_discovery_result
            WHERE root_job_id = %s AND epoch_id = %s
            FOR UPDATE
            """,
            (job_id, epoch_id),
        ).fetchone()
        if discovery is None:
            raise EventConflictError("direct discovery precursor changed")
        scope_predicate = (
            "chunk_version_id = %s"
            if job_kind == "impact_discovery"
            else "claim_id = %s"
        )
        scope_value = chunk_version_id if job_kind == "impact_discovery" else claim_id
        channel_rows = cursor.execute(
            "SELECT epoch_id, chunk_version_id, claim_id, candidate_policy_id, "
            "channel, rank, score, channel_artifact_hash "
            "FROM groundloop_impact_channel_hit WHERE epoch_id = %s "
            "AND candidate_policy_id = %s AND "
            + scope_predicate
            + ' ORDER BY channel COLLATE "C", rank, claim_id COLLATE "C", '
            'chunk_version_id COLLATE "C" FOR UPDATE',
            (epoch_id, candidate_policy_id, scope_value),
        ).fetchall()
        admitted_rows = cursor.execute(
            "SELECT admitted_pair_id, epoch_id, chunk_version_id, claim_id, "
            "candidate_policy_id, fused_rank, reasons, mandatory_lineage "
            "FROM groundloop_admitted_pair WHERE epoch_id = %s "
            "AND candidate_policy_id = %s AND "
            + scope_predicate
            + ' ORDER BY fused_rank, claim_id COLLATE "C", '
            'chunk_version_id COLLATE "C" FOR UPDATE',
            (epoch_id, candidate_policy_id, scope_value),
        ).fetchall()
        channel_identities = tuple(
            sorted(
                _stable_m4_digest(
                    "m4-discovery-channel-v1",
                    str(int(row[0])),
                    _text(row[2]),
                    _text(row[1]),
                    _text(row[3]),
                    _text(row[4]),
                    str(int(row[5])),
                    "" if row[6] is None else format(float(row[6]), ".17g"),
                    _sha256_text(row[7]),
                )
                for row in channel_rows
            )
        )
        admitted_ids = tuple(sorted(_text(row[0]) for row in admitted_rows))
        if (
            _text(discovery[0]) != job_id
            or int(discovery[1]) != epoch_id
            or int(discovery[5]) != len(channel_rows)
            or int(discovery[6]) != len(admitted_rows)
            or _sha256_text(discovery[7])
            != _stable_m4_digest("m4-discovery-channel-set-v1", *channel_identities)
            or _sha256_text(discovery[8])
            != _stable_m4_digest("m4-discovery-admitted-set-v1", *admitted_ids)
        ):
            raise EventConflictError("direct discovery artifact closure changed")
        result_artifact_id = _text(discovery[2])
        result_artifact_hash = _sha256_text(discovery[3])
        child_set_hash = _stable_m4_digest("m4-child-set-v1", *child_job_ids)
        raw_claim_deltas.extend((_text(row[7]), 1) for row in children)
        if job_kind == "frontier_retrieve":
            if claim_id is None:
                raise EventConflictError("direct frontier precursor lost its claim")
            raw_claim_deltas.append((claim_id, -1))
        elif job_kind == "impact_discovery":
            scope_delta = -1
        else:
            raise EventConflictError("direct expandable job kind changed")
    else:
        if (
            job_kind != "verify_pair"
            or claim_id is None
            or chunk_version_id is None
            or parent_job_id is None
            or job[12] is not None
            or execution_hint is None
            or admitted_pair_id is None
        ):
            raise EventConflictError("direct verifier precursor shape changed")
        parent_job_row = jobs_by_id[parent_job_id]
        parent_attempts = tuple(
            row for row in all_attempts if _text(row[1]) == parent_job_id
        )
        if not parent_attempts:
            raise EventConflictError("direct verifier parent attempt history is absent")
        for ordinal, parent_attempt in enumerate(parent_attempts, start=1):
            state = _text(parent_attempt[5])
            if int(parent_attempt[3]) != ordinal or (
                ordinal < len(parent_attempts)
                and (state not in {"failed", "expired"} or parent_attempt[8] is None)
            ):
                raise EventConflictError(
                    "direct verifier parent attempt history changed"
                )
        if (
            _text(parent_attempts[-1][5]) != "completed"
            or parent_attempts[-1][8] is None
            or _sha256_text(parent_attempts[-1][2]) != _sha256_text(parent_job_row[6])
        ):
            raise EventConflictError("direct verifier parent attempt is not completed")
        parent_kind = _text(parent_job_row[3])
        parent_children = actual_dependency_map.get(parent_job_id, ())
        if any(parent_job_row[index] is None for index in (12, 13, 14, 15)):
            raise EventConflictError("direct verifier parent closure changed")
        expected_parent_child_hash = _stable_m4_digest(
            "m4-child-set-v1", *parent_children
        )
        expected_parent_completion = _stable_m4_digest(
            "m4-job-completion-v1",
            parent_job_id,
            _sha256_text(parent_job_row[5]),
            _sha256_text(parent_job_row[6]),
            _text(parent_job_row[14]),
            _sha256_text(parent_job_row[15]),
            "completed_active",
            expected_parent_child_hash,
        )
        if (
            parent_kind not in {"impact_discovery", "frontier_retrieve"}
            or not bool(parent_job_row[9])
            or _text(parent_job_row[10]) != "completed_active"
            or not bool(parent_job_row[11])
            or _sha256_text(parent_job_row[12]) != expected_parent_child_hash
            or _sha256_text(parent_job_row[13]) != expected_parent_completion
            or parent_job_row[17] is None
            or parent_job_row[19] is None
        ):
            raise EventConflictError("direct verifier parent closure changed")
        if parent_kind == "impact_discovery":
            parent_scope = cursor.execute(
                """
                SELECT epoch_id, registry_snapshot_id, scope_kind,
                       explicit_claim_ids, closed_revision
                FROM groundloop_discovery_scope
                WHERE root_job_id = %s
                FOR UPDATE
                """,
                (parent_job_id,),
            ).fetchone()
            if parent_scope is None or (
                int(parent_scope[0]) != epoch_id
                or _text(parent_scope[1]) != registry_snapshot_id
                or _text(parent_scope[2]) != "all_registered_claims"
                or parent_scope[3] is not None
                or int(parent_scope[4]) != int(parent_job_row[17])
            ):
                raise EventConflictError("direct verifier parent scope changed")
        parent_result = cursor.execute(
            """
            SELECT result_artifact_id, result_artifact_hash,
                   channel_hit_count, admitted_pair_count,
                   channel_set_hash, admitted_pair_set_hash
            FROM groundloop_m4_discovery_result
            WHERE root_job_id = %s AND epoch_id = %s
            FOR UPDATE
            """,
            (parent_job_id, epoch_id),
        ).fetchone()
        parent_scope_column = (
            "chunk_version_id" if parent_kind == "impact_discovery" else "claim_id"
        )
        parent_scope_value = (
            parent_job_row[8]
            if parent_kind == "impact_discovery"
            else parent_job_row[7]
        )
        if parent_scope_value is None:
            raise EventConflictError("direct verifier parent scope key changed")
        parent_channels = cursor.execute(
            "SELECT epoch_id, chunk_version_id, claim_id, candidate_policy_id, "
            "channel, rank, score, channel_artifact_hash "
            "FROM groundloop_impact_channel_hit WHERE epoch_id = %s "
            "AND candidate_policy_id = %s AND "
            + parent_scope_column
            + ' = %s ORDER BY channel COLLATE "C", rank, '
            'claim_id COLLATE "C", chunk_version_id COLLATE "C" FOR UPDATE',
            (epoch_id, candidate_policy_id, parent_scope_value),
        ).fetchall()
        parent_admitted = cursor.execute(
            "SELECT admitted_pair_id FROM groundloop_admitted_pair "
            "WHERE epoch_id = %s AND candidate_policy_id = %s AND "
            + parent_scope_column
            + ' = %s ORDER BY fused_rank, claim_id COLLATE "C", '
            'chunk_version_id COLLATE "C" FOR UPDATE',
            (epoch_id, candidate_policy_id, parent_scope_value),
        ).fetchall()
        parent_channel_ids = tuple(
            sorted(
                _stable_m4_digest(
                    "m4-discovery-channel-v1",
                    str(int(row[0])),
                    _text(row[2]),
                    _text(row[1]),
                    _text(row[3]),
                    _text(row[4]),
                    str(int(row[5])),
                    "" if row[6] is None else format(float(row[6]), ".17g"),
                    _sha256_text(row[7]),
                )
                for row in parent_channels
            )
        )
        parent_admitted_ids = tuple(sorted(_text(row[0]) for row in parent_admitted))
        if parent_result is None or (
            _text(parent_result[0]) != _text(parent_job_row[14])
            or _sha256_text(parent_result[1]) != _sha256_text(parent_job_row[15])
            or int(parent_result[2]) != len(parent_channels)
            or int(parent_result[3]) != len(parent_admitted)
            or _sha256_text(parent_result[4])
            != _stable_m4_digest("m4-discovery-channel-set-v1", *parent_channel_ids)
            or _sha256_text(parent_result[5])
            != _stable_m4_digest("m4-discovery-admitted-set-v1", *parent_admitted_ids)
            or admitted_pair_id not in parent_admitted_ids
        ):
            raise EventConflictError("direct verifier parent result changed")
        execution = locked_execution
        admitted = cursor.execute(
            """
            SELECT admitted_pair_id, epoch_id, chunk_version_id, claim_id,
                   candidate_policy_id, fused_rank, reasons, mandatory_lineage
            FROM groundloop_admitted_pair
            WHERE admitted_pair_id = %s
            FOR UPDATE
            """,
            (admitted_pair_id,),
        ).fetchone()
        frontier = cursor.execute(
            """
            SELECT frontier_state, rank, retrieval_score,
                   candidate_artifact_hash, valid_to_epoch
            FROM groundloop_candidate_frontier
            WHERE claim_id = %s AND chunk_version_id = %s
              AND candidate_policy_id = %s AND valid_from_epoch = %s
            FOR UPDATE
            """,
            (claim_id, chunk_version_id, candidate_policy_id, epoch_id),
        ).fetchone()
        expected_admitted_id = _stable_m4_digest(
            "m4-admitted-pair-v1",
            str(epoch_id),
            claim_id,
            chunk_version_id,
            candidate_policy_id,
        )
        if (
            execution is None
            or admitted is None
            or frontier is None
            or (
                tuple(execution) != tuple(execution_hint)
                or _text(execution[0]) == ""
                or _text(execution[1]) != job_id
                or _text(execution[2]) != expected_admitted_id
                or _sha256_text(execution[5]) != _sha256_text(job[6])
                or _text(admitted[0]) != expected_admitted_id
                or int(admitted[1]) != epoch_id
                or _text(admitted[2]) != chunk_version_id
                or _text(admitted[3]) != claim_id
                or _text(admitted[4]) != candidate_policy_id
                or _text(frontier[0]) != "queued"
                or frontier[4] is not None
            )
        ):
            raise EventConflictError("direct verifier persisted closure changed")
        observation_id = _text(execution[0])
        if verifier_artifact is None:
            raise EventConflictError("direct verifier result artifact is absent")
        result_artifact_id = verifier_artifact.artifact_id
        result_artifact_hash = verification_artifact_payload_hash(verifier_artifact)
        raw_claim_deltas.append((claim_id, -1))

    payload_hash = _sha256_text(job[5])
    execution_spec_hash = _sha256_text(job[6])
    if (
        job_kind not in {"impact_discovery", "frontier_retrieve", "verify_pair"}
        or _text(job[0]) != job_id
        or int(job[1]) != epoch_id
        or _text(job[4]) != candidate_policy_id
        or _text(job[10]) != "running"
        or bool(job[11])
        or any(job[index] is not None for index in (12, 13, 14, 15))
        or result_artifact_id is None
        or result_artifact_hash is None
        or int(job[16]) > expected_revision
        or job[17] is not None
        or job[19] is not None
        or _text(update_row[0]) != candidate_policy_id
        or update_row[1] is None
        or registry_snapshot_id != direct_policy.claim_registry_snapshot_id
        or direct_policy.decision_policy_version != decision_policy_version
        or (
            job_kind == "verify_pair"
            and execution_spec_hash != direct_policy.verifier_execution_spec_hash
        )
    ):
        raise EventConflictError("direct matching precursor is not preterminal")
    expected_completion_digest = _stable_m4_digest(
        "m4-job-completion-v1",
        job_id,
        payload_hash,
        execution_spec_hash,
        result_artifact_id,
        result_artifact_hash,
        "completed_active",
        "" if child_set_hash is None else child_set_hash,
    )
    completion_digest = expected_completion_digest
    if proposed_source_id != completion_digest:
        raise EventConflictError("direct proposed source differs from precursor")

    claim_deltas = _canonical_direct_deltas(raw_claim_deltas)
    source_identity_hash = _direct_transition_payload_hash(
        expected_revision=expected_revision,
        scope_delta=scope_delta,
        claim_job_deltas=claim_deltas,
    )
    if tuple(claim_id for claim_id, _ in claim_deltas) != tuple(
        sorted(claim_id for claim_id, _ in claim_deltas)
    ):
        raise EventConflictError("direct evaluation claim order changed")
    answer_deltas = _canonical_direct_deltas(
        tuple(
            (_text(claims_by_id[object_id][1]), delta)
            for object_id, delta in claim_deltas
            if bool(claims_by_id[object_id][3])
        )
    )
    answer_version_id: str | None = None
    owner_claim_required: bool | None = None
    if claim_id is not None:
        owner = claims_by_id.get(claim_id)
        if owner is None:
            raise EventConflictError("direct verifier claim owner changed")
        answer_version_id = _text(owner[1])
        owner_claim_required = bool(owner[3])
    return _DirectPrecursor(
        epoch_id=epoch_id,
        expected_revision=expected_revision,
        resulting_revision=resulting_revision,
        job_id=job_id,
        attempt_id=attempt_id,
        parent_job_id=parent_job_id,
        job_kind=job_kind,
        candidate_policy_id=candidate_policy_id,
        candidate_policy=direct_policy,
        registry_snapshot_id=registry_snapshot_id,
        decision_policy_version=decision_policy_version,
        support_threshold=float(decision_policy[0]),
        refute_threshold=float(decision_policy[1]),
        payload_hash=payload_hash,
        execution_spec_hash=execution_spec_hash,
        result_artifact_id=result_artifact_id,
        result_artifact_hash=result_artifact_hash,
        attempt_ordinal=attempt_ordinal,
        source_id=completion_digest,
        source_identity_hash=source_identity_hash,
        scope_delta=scope_delta,
        claim_job_deltas=claim_deltas,
        answer_job_deltas=answer_deltas,
        child_job_ids=child_job_ids,
        claim_id=claim_id,
        answer_version_id=answer_version_id,
        owner_claim_required=owner_claim_required,
        chunk_version_id=chunk_version_id,
        observation_id=observation_id,
        admitted_pair_id=admitted_pair_id,
        model_authority=model_authority,
        prompt_authority=prompt_authority,
    )


_DIRECT_STAGE_RELATIONS = frozenset(
    {
        "groundloop_semantic_job",
        "groundloop_semantic_job_attempt",
        "groundloop_discovery_scope",
        "groundloop_candidate_frontier",
        "groundloop_working_observation_delta",
        "groundloop_m4_working_claim_state",
        "groundloop_m4_working_answer_state",
        "groundloop_working_transition",
        "groundloop_m5_owner_pending_counter",
        "groundloop_m5_answer_pending_counter",
        "groundloop_m4_evaluation_epoch_counter",
        "groundloop_m4_evaluation_override_counter",
        "groundloop_m4_evaluation_counter_transition",
    }
)


def _direct_stage_row(
    cursor: Cursor[Any],
    coordinate: _DirectStageCoordinate,
    *,
    lock: bool,
) -> object | None:
    if coordinate.relation_name not in _DIRECT_STAGE_RELATIONS:
        raise ValidationError("direct stage coordinate names another relation")
    if not coordinate.key_columns or len(coordinate.key_columns) != len(
        coordinate.key_parts
    ):
        raise ValidationError("direct stage coordinate has a malformed key")
    predicates = sql.SQL(" AND ").join(
        sql.SQL("{} = {}").format(sql.Identifier(column), sql.Placeholder())
        for column in coordinate.key_columns
    )
    statement = (
        sql.SQL("SELECT to_jsonb(row_value) FROM {} AS row_value WHERE ").format(
            sql.Identifier(coordinate.relation_name)
        )
        + predicates
    )
    if lock:
        statement += sql.SQL(" FOR UPDATE")
    rows = cursor.execute(statement, coordinate.key_parts).fetchall()
    if len(rows) > 1:
        raise EventConflictError("direct stage coordinate is not point-unique")
    return None if not rows else rows[0][0]


def _direct_observation_coordinate(
    cursor: Cursor[Any], precursor: _DirectPrecursor
) -> _DirectStageCoordinate | None:
    if precursor.observation_id is None:
        return None
    row = cursor.execute(
        """
        SELECT subject_kind, subject_id, chunk_version_id, task_type
        FROM groundloop_semantic_observation
        WHERE observation_id = %s
        """,
        (precursor.observation_id,),
    ).fetchone()
    if row is None or (
        _text(row[0]) != "claim"
        or precursor.claim_id is None
        or _text(row[1]) != precursor.claim_id
    ):
        raise EventConflictError("direct verifier observation key changed")
    return _DirectStageCoordinate(
        "groundloop_working_observation_delta",
        (
            "epoch_id",
            "subject_kind",
            "subject_id",
            "chunk_version_id",
            "task_type",
        ),
        (
            precursor.epoch_id,
            _text(row[0]),
            _text(row[1]),
            _text(row[2]),
            _text(row[3]),
        ),
    )


def _validate_direct_verifier_closure(
    cursor: Cursor[Any], precursor: _DirectPrecursor
) -> None:
    """Byte-validate the persisted verifier result and provenance closure."""

    if precursor.job_kind != "verify_pair":
        return
    if (
        precursor.claim_id is None
        or precursor.chunk_version_id is None
        or precursor.observation_id is None
        or precursor.admitted_pair_id is None
    ):
        raise EventConflictError("direct verifier precursor is incomplete")
    execution_row = cursor.execute(
        """
        SELECT observation_id, job_id, btrim(admitted_pair_id),
               model_artifact_id, prompt_artifact_id,
               btrim(execution_spec_hash), btrim(pair_input_hash),
               calibration_version, btrim(calibration_artifact_sha256),
               temperature, raw_logits, btrim(raw_output_hash),
               reused_from_observation_id
        FROM groundloop_m4_verification_execution
        WHERE job_id = %s
        """,
        (precursor.job_id,),
    ).fetchone()
    admitted_row = cursor.execute(
        """
        SELECT admitted_pair_id, epoch_id, claim_id, chunk_version_id,
               candidate_policy_id, fused_rank, reasons, mandatory_lineage
        FROM groundloop_admitted_pair
        WHERE admitted_pair_id = %s
        """,
        (precursor.admitted_pair_id,),
    ).fetchone()
    input_row = cursor.execute(
        """
        SELECT claim.text, claim.required, claim.answer_version_id,
               chunk.document_version_id, chunk.chunk_index, chunk.text,
               chunk.text_hash, provenance.chunker_artifact_id
        FROM groundloop_claim AS claim
        CROSS JOIN groundloop_chunk_version AS chunk
        JOIN groundloop_chunk_provenance AS provenance
          ON provenance.chunk_version_id = chunk.chunk_version_id
        WHERE claim.claim_id = %s AND chunk.chunk_version_id = %s
        """,
        (precursor.claim_id, precursor.chunk_version_id),
    ).fetchone()
    observation_row = cursor.execute(
        """
        SELECT observation_id, subject_kind::text, subject_id,
               chunk_version_id, task_type, support_score, refute_score,
               neutral_score, model_id, model_version, prompt_version,
               input_hash, produced_epoch, raw_output_hash,
               eligible_for_currency
        FROM groundloop_semantic_observation
        WHERE observation_id = %s
        """,
        (precursor.observation_id,),
    ).fetchone()
    if (
        execution_row is None
        or admitted_row is None
        or input_row is None
        or observation_row is None
    ):
        raise EventConflictError("direct verifier result closure is incomplete")
    citations = tuple(
        _text(row[0])
        for row in cursor.execute(
            """
            SELECT chunk_version_id
            FROM groundloop_answer_citation
            WHERE answer_version_id = %s
            ORDER BY citation_ordinal
            """,
            (_text(input_row[2]),),
        ).fetchall()
    )
    model = cursor.execute(
        """
        SELECT model_artifact_id, task, model_id, immutable_revision,
               tokenizer_revision, config_hash, artifact_sha256
        FROM groundloop_model_artifact
        WHERE model_artifact_id = %s
        """,
        (_text(execution_row[3]),),
    ).fetchone()
    prompt = cursor.execute(
        """
        SELECT prompt_artifact_id, task, version, template_hash,
               decoding_config_hash
        FROM groundloop_prompt_artifact
        WHERE prompt_artifact_id = %s
        """,
        (_text(execution_row[4]),),
    ).fetchone()
    direct_policy = _direct_candidate_policy(
        cursor, precursor.candidate_policy_id, lock=False
    )
    decision_row = cursor.execute(
        """
        SELECT policy_version, support_threshold, refute_threshold,
               tie_rule_version
        FROM groundloop_decision_policy
        WHERE policy_version = %s
        """,
        (precursor.decision_policy_version,),
    ).fetchone()
    if (
        model is None
        or prompt is None
        or decision_row is None
        or tuple(model) != precursor.model_authority
        or tuple(prompt) != precursor.prompt_authority
    ):
        raise EventConflictError("direct verifier authority is incomplete")
    try:
        pair = PairKey(precursor.claim_id, precursor.chunk_version_id)
        pair_input = PairVerificationInput(
            pair=pair,
            claim_text=_text(input_row[0]),
            claim_required=bool(input_row[1]),
            claim_cited_chunk_version_ids=citations,
            document_version_id=_text(input_row[3]),
            chunk_index=int(input_row[4]),
            chunk_text=_text(input_row[5]),
            chunk_text_hash=_sha256_text(input_row[6]),
            chunker_artifact_id=_text(input_row[7]),
        )
        raw_logits_values = execution_row[10]
        if not isinstance(raw_logits_values, (list, tuple)):
            raise ValidationError("direct verifier raw logits changed")
        raw_logits = tuple(float(value) for value in raw_logits_values)
        if len(raw_logits) != 3:
            raise ValidationError("direct verifier raw logits changed")
        execution = M5TypedDirectVerificationExecution(
            observation_id=_text(execution_row[0]),
            job_id=_text(execution_row[1]),
            admitted_pair_id=_text(execution_row[2]),
            model_artifact_id=_text(execution_row[3]),
            prompt_artifact_id=_text(execution_row[4]),
            execution_spec_hash=_sha256_text(execution_row[5]),
            pair_input_hash=_sha256_text(execution_row[6]),
            calibration_version=_text(execution_row[7]),
            calibration_artifact_sha256=_sha256_text(execution_row[8]),
            temperature=float(execution_row[9]),
            raw_logits=(raw_logits[0], raw_logits[1], raw_logits[2]),
            raw_output_hash=_sha256_text(execution_row[11]),
            reused_from_observation_id=(
                None if execution_row[12] is None else _text(execution_row[12])
            ),
        )
        admitted = AdmittedPair(
            epoch_id=int(admitted_row[1]),
            pair=PairKey(_text(admitted_row[2]), _text(admitted_row[3])),
            candidate_policy_id=_text(admitted_row[4]),
            fused_rank=int(admitted_row[5]),
            reasons=tuple(AdmissionChannel(_text(value)) for value in admitted_row[6]),
            mandatory_lineage=bool(admitted_row[7]),
        )
        decision_policy = DecisionPolicy(
            _text(decision_row[0]),
            float(decision_row[1]),
            float(decision_row[2]),
            _text(decision_row[3]),
        )
        observation = SemanticObservation(
            observation_id=_text(observation_row[0]),
            subject_kind=SubjectKind(_text(observation_row[1])),
            subject_id=_text(observation_row[2]),
            chunk_version_id=_text(observation_row[3]),
            task_type=_text(observation_row[4]),
            support_score=float(observation_row[5]),
            refute_score=float(observation_row[6]),
            neutral_score=float(observation_row[7]),
            producer=ModelStamp(
                _text(observation_row[8]),
                _text(observation_row[9]),
                _text(observation_row[10]),
            ),
            input_hash=_sha256_text(observation_row[11]),
        )
        result = AIVerificationResult(
            claim_id=pair.claim_id,
            chunk_version_id=pair.chunk_version_id,
            candidate_id=stable_ai_digest(
                "verification-candidate-v1", pair.claim_id, pair.chunk_version_id
            ),
            model_artifact_id=execution.model_artifact_id,
            prompt_artifact_id=execution.prompt_artifact_id,
            calibration_version=execution.calibration_version,
            temperature=execution.temperature,
            scores=ScoreTriple(
                observation.support_score,
                observation.refute_score,
                observation.neutral_score,
            ),
            input_hash=observation.input_hash,
            raw_output_hash=execution.raw_output_hash,
            raw_logits=execution.raw_logits,
        )
        artifact = PairVerificationArtifact(
            artifact_id=precursor.result_artifact_id,
            pair=pair,
            pair_input_hash=pair_input.input_hash,
            execution_spec_hash=execution.execution_spec_hash,
            decision_policy_version=decision_policy.policy_version,
            decision_policy_hash=decision_policy_hash(decision_policy),
            operational_label=derive_operational_label(result, decision_policy),
            result=result,
        )
        expected_observation = artifact.to_semantic_observation(
            model_id=_text(model[2]),
            model_revision=_text(model[3]),
            prompt_version=_text(prompt[2]),
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError(
            "direct verifier result closure is malformed"
        ) from error
    derived_artifact, _ = _direct_verifier_artifact_from_observation(
        cursor,
        epoch_id=precursor.epoch_id,
        job_id=precursor.job_id,
        claim_id=precursor.claim_id,
        chunk_version_id=precursor.chunk_version_id,
        decision_policy=decision_policy,
        execution_row=tuple(execution_row),
        observation_row=tuple(observation_row),
        model_authority=tuple(model),
        prompt_authority=tuple(prompt),
    )
    judgment_row = cursor.execute(
        """
        SELECT judgment_id, claim_id, chunk_version_id, source_kind,
               source_artifact_id, decision_policy_or_guideline_id,
               derived_label, support_score, refute_score, neutral_score,
               input_hash, split_id, manifest_id
        FROM groundloop_pair_judgment
        WHERE claim_id = %s AND chunk_version_id = %s
          AND source_kind = 'model' AND source_artifact_id = %s
          AND decision_policy_or_guideline_id = %s AND input_hash = %s
        """,
        (
            precursor.claim_id,
            precursor.chunk_version_id,
            derived_artifact.artifact_id,
            precursor.decision_policy_version,
            derived_artifact.result.input_hash,
        ),
    ).fetchone()
    _validate_direct_verifier_judgment(
        derived_artifact,
        candidate_policy_id=precursor.candidate_policy_id,
        judgment_row=judgment_row,
    )
    expected_admitted_id = _stable_m4_digest(
        "m4-admitted-pair-v1",
        str(admitted.epoch_id),
        admitted.pair.claim_id,
        admitted.pair.chunk_version_id,
        admitted.candidate_policy_id,
    )
    if (
        _text(admitted_row[0]) != expected_admitted_id
        or execution.admitted_pair_id != expected_admitted_id
        or execution.job_id != precursor.job_id
        or execution.observation_id != precursor.observation_id
        or execution.execution_spec_hash != precursor.execution_spec_hash
        or execution.reused_from_observation_id is not None
        or execution.pair_input_hash != pair_input.input_hash
        or admitted.epoch_id != precursor.epoch_id
        or admitted.pair != pair
        or admitted.candidate_policy_id != precursor.candidate_policy_id
        or direct_policy != precursor.candidate_policy
        or direct_policy.verifier_execution_spec_hash != precursor.execution_spec_hash
        or direct_policy.decision_policy_version != precursor.decision_policy_version
        or direct_policy.claim_registry_snapshot_id != precursor.registry_snapshot_id
        or tuple(model[:2]) != (execution.model_artifact_id, "verification")
        or tuple(prompt[:2]) != (execution.prompt_artifact_id, "verification")
        or observation.task_type != "verify"
        or observation != expected_observation
        or int(observation_row[12]) != precursor.epoch_id
        or _sha256_text(observation_row[13]) != execution.raw_output_hash
        or observation_row[14] is not True
        or artifact.artifact_id != precursor.result_artifact_id
        or artifact != derived_artifact
        or verification_artifact_payload_hash(artifact)
        != precursor.result_artifact_hash
    ):
        raise EventConflictError("direct verifier result/provenance identity changed")


def _lock_direct_tier11a(
    cursor: Cursor[Any], precursor: _DirectPrecursor
) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
    """Lock every verifier observation and its currency key before image locks."""

    if precursor.observation_id is None:
        return None
    if precursor.claim_id is None or precursor.chunk_version_id is None:
        raise EventConflictError("direct verifier observation owner changed")
    observation = cursor.execute(
        """
        SELECT subject_kind, subject_id, chunk_version_id, task_type
        FROM groundloop_semantic_observation
        WHERE observation_id = %s
        """,
        (precursor.observation_id,),
    ).fetchone()
    if observation is None or (
        _text(observation[0]) != "claim"
        or _text(observation[1]) != precursor.claim_id
        or _text(observation[2]) != precursor.chunk_version_id
    ):
        raise EventConflictError("direct verifier observation key changed")
    task_type = _text(observation[3])
    base = cursor.execute(
        """
        SELECT previous_published_epoch_id
        FROM groundloop_m4_update
        WHERE epoch_id = %s
        """,
        (precursor.epoch_id,),
    ).fetchone()
    if base is None or base[0] is None:
        raise EventConflictError("direct verifier currency base changed")
    base_epoch_id = int(base[0])
    published_claim = cursor.execute(
        """
        SELECT supporting_observation_ids, refuting_observation_ids
        FROM groundloop_published_claim_state
        WHERE claim_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (precursor.claim_id, base_epoch_id, base_epoch_id),
    ).fetchall()
    working_claim = cursor.execute(
        """
        SELECT supporting_observation_ids, refuting_observation_ids
        FROM groundloop_m4_working_claim_state
        WHERE epoch_id = %s AND claim_id = %s
        """,
        (precursor.epoch_id, precursor.claim_id),
    ).fetchone()
    if len(published_claim) != 1:
        raise EventConflictError("direct M4 published claim state changed")
    claim_row = published_claim[0] if working_claim is None else working_claim
    support_values = claim_row[0]
    refute_values = claim_row[1]
    if not isinstance(support_values, (list, tuple)) or not isinstance(
        refute_values, (list, tuple)
    ):
        raise EventConflictError("direct M4 claim provenance changed")
    support_ids = tuple(_text(value) for value in support_values)
    refute_ids = tuple(_text(value) for value in refute_values)
    if support_ids != tuple(sorted(set(support_ids))) or refute_ids != tuple(
        sorted(set(refute_ids))
    ):
        raise EventConflictError("direct M4 claim provenance changed")
    published_currency = cursor.execute(
        """
        SELECT observation_id, valid_from_epoch
        FROM groundloop_published_observation_currency
        WHERE subject_kind = 'claim' AND subject_id = %s
          AND chunk_version_id = %s AND task_type = %s
          AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (
            precursor.claim_id,
            precursor.chunk_version_id,
            task_type,
            base_epoch_id,
            base_epoch_id,
        ),
    ).fetchall()
    working_currency = cursor.execute(
        """
        SELECT base_observation_id, working_observation_id, installed_revision
        FROM groundloop_working_observation_delta
        WHERE epoch_id = %s AND subject_kind = 'claim' AND subject_id = %s
          AND chunk_version_id = %s AND task_type = %s
        """,
        (
            precursor.epoch_id,
            precursor.claim_id,
            precursor.chunk_version_id,
            task_type,
        ),
    ).fetchone()
    if len(published_currency) > 1 or (
        working_currency is not None
        and int(working_currency[2]) > precursor.expected_revision
    ):
        raise EventConflictError("direct observation currency changed")
    observation_ids = {
        precursor.observation_id,
        *support_ids,
        *refute_ids,
    }
    if published_currency:
        observation_ids.add(_text(published_currency[0][0]))
    if working_currency is not None:
        if working_currency[0] is not None:
            observation_ids.add(_text(working_currency[0]))
        if working_currency[1] is not None:
            observation_ids.add(_text(working_currency[1]))
    ordered_observation_ids = tuple(sorted(observation_ids))
    locked_observations = cursor.execute(
        """
        SELECT observation_id
        FROM groundloop_semantic_observation
        WHERE observation_id = ANY(%s)
        ORDER BY observation_id COLLATE "C"
        FOR UPDATE
        """,
        (list(ordered_observation_ids),),
    ).fetchall()
    if tuple(_text(row[0]) for row in locked_observations) != (ordered_observation_ids):
        raise EventConflictError("direct claim provenance observations changed")
    locked_published_currency = cursor.execute(
        """
        SELECT observation_id, valid_from_epoch
        FROM groundloop_published_observation_currency
        WHERE subject_kind = 'claim' AND subject_id = %s
          AND chunk_version_id = %s AND task_type = %s
          AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        ORDER BY valid_from_epoch
        FOR UPDATE
        """,
        (
            precursor.claim_id,
            precursor.chunk_version_id,
            task_type,
            base_epoch_id,
            base_epoch_id,
        ),
    ).fetchall()
    if tuple(tuple(row) for row in locked_published_currency) != tuple(
        tuple(row) for row in published_currency
    ):
        raise EventConflictError("direct published observation currency changed")
    locked_working_currency = cursor.execute(
        """
        SELECT base_observation_id, working_observation_id, installed_revision
        FROM groundloop_working_observation_delta
        WHERE epoch_id = %s AND subject_kind = 'claim' AND subject_id = %s
          AND chunk_version_id = %s AND task_type = %s
        FOR UPDATE
        """,
        (
            precursor.epoch_id,
            precursor.claim_id,
            precursor.chunk_version_id,
            task_type,
        ),
    ).fetchone()
    if (
        None if locked_working_currency is None else tuple(locked_working_currency)
    ) != (None if working_currency is None else tuple(working_currency)):
        raise EventConflictError("direct working observation currency changed")
    if locked_working_currency is None:
        _reserve_matching_absence(
            cursor,
            "groundloop_working_observation_delta",
            (
                precursor.epoch_id,
                "claim",
                precursor.claim_id,
                precursor.chunk_version_id,
                task_type,
            ),
        )
    _validate_direct_verifier_closure(cursor, precursor)
    return support_ids, refute_ids


def _direct_lower_stage_coordinates(
    cursor: Cursor[Any], precursor: _DirectPrecursor
) -> tuple[_DirectStageCoordinate, ...]:
    """Reserve the exact lower-tier M4 mutations before D25 tier 11b+."""

    coordinates: list[_DirectStageCoordinate] = [
        _DirectStageCoordinate(
            "groundloop_semantic_job", ("job_id",), (precursor.job_id,)
        ),
        _DirectStageCoordinate(
            "groundloop_semantic_job_attempt",
            ("attempt_id",),
            (precursor.attempt_id,),
        ),
    ]
    observation_coordinate = _direct_observation_coordinate(cursor, precursor)
    if observation_coordinate is not None:
        coordinates.append(observation_coordinate)
        assert precursor.claim_id is not None
        coordinates.append(
            _DirectStageCoordinate(
                "groundloop_m4_working_claim_state",
                ("epoch_id", "claim_id"),
                (precursor.epoch_id, precursor.claim_id),
            )
        )
        if precursor.answer_version_id is None:
            raise EventConflictError("direct verifier lost its answer owner")
        coordinates.append(
            _DirectStageCoordinate(
                "groundloop_m4_working_answer_state",
                ("epoch_id", "answer_version_id"),
                (precursor.epoch_id, precursor.answer_version_id),
            )
        )
        if precursor.chunk_version_id is None:
            raise EventConflictError("direct verifier lost its frontier key")
        coordinates.append(
            _DirectStageCoordinate(
                "groundloop_candidate_frontier",
                (
                    "claim_id",
                    "chunk_version_id",
                    "candidate_policy_id",
                    "valid_from_epoch",
                ),
                (
                    precursor.claim_id,
                    precursor.chunk_version_id,
                    precursor.candidate_policy_id,
                    precursor.epoch_id,
                ),
            )
        )
    elif precursor.job_kind == "impact_discovery":
        coordinates.append(
            _DirectStageCoordinate(
                "groundloop_discovery_scope",
                ("root_job_id",),
                (precursor.job_id,),
            )
        )

    keys = tuple(
        (coordinate.relation_name, coordinate.key_columns, coordinate.key_parts)
        for coordinate in coordinates
    )
    if len(set(keys)) != len(keys):
        raise ValidationError("direct lower-stage plan repeats a key")
    return tuple(coordinates)


def _direct_override_keys(
    precursor: _DirectPrecursor,
) -> tuple[tuple[str, str], ...]:
    """Return the one frozen UTF-8/C order for all tier-15c overrides."""

    values = tuple(
        ("claim", claim_id) for claim_id, _ in precursor.claim_job_deltas
    ) + tuple(("answer", answer_id) for answer_id, _ in precursor.answer_job_deltas)
    ordered = tuple(
        sorted(values, key=lambda item: (item[0].encode(), item[1].encode()))
    )
    if len(set(ordered)) != len(ordered):
        raise ValidationError("direct override plan repeats a key")
    return ordered


def _direct_late_stage_coordinates(
    cursor: Cursor[Any], precursor: _DirectPrecursor
) -> tuple[_DirectStageCoordinate, ...]:
    """Reserve every applicable tier-15a, 15b, and 15c coordinate."""

    coordinates: list[_DirectStageCoordinate] = []
    owner_pending = cursor.execute(
        """
        SELECT owner_claim_id
        FROM groundloop_m5_owner_pending_counter
        WHERE epoch_id = %s
        ORDER BY owner_claim_id COLLATE "C"
        FOR UPDATE
        """,
        (precursor.epoch_id,),
    ).fetchall()
    coordinates.extend(
        _DirectStageCoordinate(
            "groundloop_m5_owner_pending_counter",
            ("epoch_id", "owner_claim_id"),
            (precursor.epoch_id, _text(row[0])),
        )
        for row in owner_pending
    )
    answer_pending = cursor.execute(
        """
        SELECT answer_version_id
        FROM groundloop_m5_answer_pending_counter
        WHERE epoch_id = %s
        ORDER BY answer_version_id COLLATE "C"
        FOR UPDATE
        """,
        (precursor.epoch_id,),
    ).fetchall()
    coordinates.extend(
        _DirectStageCoordinate(
            "groundloop_m5_answer_pending_counter",
            ("epoch_id", "answer_version_id"),
            (precursor.epoch_id, _text(row[0])),
        )
        for row in answer_pending
    )
    coordinates.append(
        _DirectStageCoordinate(
            "groundloop_m4_evaluation_epoch_counter",
            ("epoch_id",),
            (precursor.epoch_id,),
        )
    )
    for object_type, object_id in _direct_override_keys(precursor):
        coordinates.append(
            _DirectStageCoordinate(
                "groundloop_m4_evaluation_override_counter",
                ("epoch_id", "object_type", "object_id"),
                (precursor.epoch_id, object_type, object_id),
            )
        )
    coordinates.append(
        _DirectStageCoordinate(
            "groundloop_m4_evaluation_counter_transition",
            ("epoch_id", "transition_id"),
            (precursor.epoch_id, precursor.source_id),
        )
    )
    keys = tuple(
        (coordinate.relation_name, coordinate.key_columns, coordinate.key_parts)
        for coordinate in coordinates
    )
    if len(set(keys)) != len(keys):
        raise ValidationError("direct late-stage plan repeats a key")
    for coordinate in coordinates:
        before = _direct_stage_row(cursor, coordinate, lock=True)
        if before is None:
            _reserve_matching_absence(
                cursor, coordinate.relation_name, coordinate.key_parts
            )
    _reserve_matching_absence(
        cursor,
        "groundloop_m4_evaluation_counter_transition_to_revision",
        (precursor.epoch_id, precursor.resulting_revision),
    )
    conflict = cursor.execute(
        """
        SELECT transition_id
        FROM groundloop_m4_evaluation_counter_transition
        WHERE epoch_id = %s AND to_revision = %s
        FOR UPDATE
        """,
        (precursor.epoch_id, precursor.resulting_revision),
    ).fetchone()
    if conflict is not None:
        raise EventConflictError("direct transition revision is already occupied")
    return tuple(coordinates)


def _capture_direct_stage_before_images(
    cursor: Cursor[Any], coordinates: Sequence[_DirectStageCoordinate]
) -> tuple[_DirectStageImage, ...]:
    return tuple(
        _DirectStageImage(
            coordinate,
            _direct_stage_row(cursor, coordinate, lock=False),
        )
        for coordinate in coordinates
    )


def _freeze_direct_authority_value(value: object) -> object:
    """Make driver containers immutable without repr or hash-based identity."""

    if isinstance(value, dict):
        return tuple(
            (
                _text(key),
                _freeze_direct_authority_value(item),
            )
            for key, item in sorted(
                value.items(), key=lambda pair: _text(pair[0]).encode()
            )
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_direct_authority_value(item) for item in value)
    if isinstance(value, memoryview):
        return bytes(value)
    return value


def _direct_authority_images(
    cursor: Cursor[Any],
    precursor: _DirectPrecursor,
    logical_plan: _DirectLogicalPlan,
) -> tuple[_DirectAuthorityImage, ...]:
    """Capture every unmutated authority row in deterministic query order."""

    images: list[_DirectAuthorityImage] = []

    def add(
        authority_name: str,
        key_parts: tuple[object, ...],
        statement: str,
        parameters: tuple[object, ...],
    ) -> None:
        rows = cursor.execute(statement, parameters).fetchall()
        images.append(
            _DirectAuthorityImage(
                authority_name,
                key_parts,
                tuple(_freeze_direct_authority_value(tuple(row)) for row in rows),
            )
        )

    epoch_id = precursor.epoch_id
    parent_key = "" if precursor.parent_job_id is None else precursor.parent_job_id
    bases = cursor.execute(
        """
        SELECT m4.previous_published_epoch_id, m5.previous_published_epoch_id
        FROM groundloop_m4_update AS m4
        JOIN groundloop_m5_update AS m5 USING (epoch_id)
        WHERE m4.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    if bases is None or bases[0] is None or bases[1] is None:
        raise EventConflictError("direct authority base selector changed")
    m4_base_epoch = int(bases[0])
    m5_base_epoch = int(bases[1])
    authority_claim_ids = tuple(
        sorted(
            {
                *(claim_id for claim_id, _ in precursor.claim_job_deltas),
                *((precursor.claim_id,) if precursor.claim_id is not None else ()),
            }
        )
    )
    add(
        "m4_update",
        (epoch_id,),
        "SELECT * FROM groundloop_m4_update WHERE epoch_id = %s",
        (epoch_id,),
    )
    add(
        "m5_update",
        (epoch_id,),
        "SELECT * FROM groundloop_m5_update WHERE epoch_id = %s",
        (epoch_id,),
    )
    add(
        "candidate_policy",
        (precursor.candidate_policy_id,),
        "SELECT * FROM groundloop_candidate_policy WHERE candidate_policy_id = %s",
        (precursor.candidate_policy_id,),
    )
    add(
        "decision_policy",
        (precursor.decision_policy_version,),
        "SELECT * FROM groundloop_decision_policy WHERE policy_version = %s",
        (precursor.decision_policy_version,),
    )
    add(
        "registry_header",
        (precursor.registry_snapshot_id,),
        "SELECT claim_registry_snapshot_id, claim_count, claim_set_hash "
        "FROM groundloop_m4_claim_registry_snapshot "
        "WHERE claim_registry_snapshot_id = %s",
        (precursor.registry_snapshot_id,),
    )
    add(
        "registry_members",
        (precursor.registry_snapshot_id, *authority_claim_ids),
        "SELECT * FROM groundloop_m4_claim_registry_member "
        "WHERE claim_registry_snapshot_id = %s AND claim_id = ANY(%s) "
        'ORDER BY member_ordinal, claim_id COLLATE "C"',
        (precursor.registry_snapshot_id, list(authority_claim_ids)),
    )
    add(
        "registry_claims",
        (precursor.registry_snapshot_id, *authority_claim_ids),
        "SELECT claim.* FROM groundloop_m4_claim_registry_member AS member "
        "JOIN groundloop_claim AS claim USING (claim_id) "
        "WHERE member.claim_registry_snapshot_id = %s "
        "AND member.claim_id = ANY(%s) "
        'ORDER BY member.member_ordinal, claim.claim_id COLLATE "C"',
        (precursor.registry_snapshot_id, list(authority_claim_ids)),
    )
    add(
        "job_target_immutable",
        (precursor.job_id,),
        "SELECT job_id, epoch_id, parent_job_id, job_kind, "
        "candidate_policy_id, payload_hash, execution_spec_hash, claim_id, "
        "chunk_version_id, expandable, created_revision, created_at "
        "FROM groundloop_semantic_job WHERE job_id = %s AND epoch_id = %s",
        (precursor.job_id, epoch_id),
    )
    add(
        "job_related_rows",
        (epoch_id, precursor.job_id, parent_key),
        "SELECT * FROM groundloop_semantic_job WHERE epoch_id = %s "
        "AND job_id <> %s AND (parent_job_id = %s OR job_id = %s "
        "OR parent_job_id = %s) "
        'ORDER BY job_id COLLATE "C"',
        (epoch_id, precursor.job_id, precursor.job_id, parent_key, parent_key),
    )
    add(
        "target_attempt_immutable",
        (precursor.attempt_id,),
        "SELECT attempt_id, job_id, execution_spec_hash, attempt_ordinal, "
        "lease_token_hash, lease_expires_at, started_at "
        "FROM groundloop_semantic_job_attempt WHERE attempt_id = %s",
        (precursor.attempt_id,),
    )
    add(
        "other_attempt_rows",
        (epoch_id, precursor.job_id, parent_key),
        "SELECT attempt.* FROM groundloop_semantic_job_attempt AS attempt "
        "JOIN groundloop_semantic_job AS job USING (job_id) "
        "WHERE job.epoch_id = %s AND attempt.attempt_id <> %s "
        "AND (job.job_id = %s OR job.parent_job_id = %s OR job.job_id = %s "
        "OR job.parent_job_id = %s) "
        'ORDER BY job.job_id COLLATE "C", attempt.attempt_ordinal',
        (
            epoch_id,
            precursor.attempt_id,
            precursor.job_id,
            precursor.job_id,
            parent_key,
            parent_key,
        ),
    )
    add(
        "job_dependencies",
        (epoch_id, precursor.job_id, parent_key),
        "SELECT * FROM groundloop_semantic_job_dependency WHERE epoch_id = %s "
        "AND parent_job_id IN (%s,%s) "
        'ORDER BY parent_job_id COLLATE "C", child_job_id COLLATE "C"',
        (epoch_id, precursor.job_id, parent_key),
    )
    add(
        "job_chunks",
        (epoch_id, precursor.job_id, parent_key),
        "SELECT chunk.* FROM groundloop_chunk_version AS chunk "
        "WHERE chunk.chunk_version_id IN ("
        "SELECT job.chunk_version_id FROM groundloop_semantic_job AS job "
        "WHERE job.epoch_id = %s AND job.chunk_version_id IS NOT NULL "
        "AND (job.job_id = %s OR job.parent_job_id = %s OR job.job_id = %s "
        "OR job.parent_job_id = %s)) "
        'ORDER BY chunk.chunk_version_id COLLATE "C"',
        (epoch_id, precursor.job_id, precursor.job_id, parent_key, parent_key),
    )
    add(
        "job_chunk_provenance",
        (epoch_id, precursor.job_id, parent_key),
        "SELECT provenance.* FROM groundloop_chunk_provenance AS provenance "
        "WHERE provenance.chunk_version_id IN ("
        "SELECT job.chunk_version_id FROM groundloop_semantic_job AS job "
        "WHERE job.epoch_id = %s AND job.chunk_version_id IS NOT NULL "
        "AND (job.job_id = %s OR job.parent_job_id = %s OR job.job_id = %s "
        "OR job.parent_job_id = %s)) "
        'ORDER BY provenance.chunk_version_id COLLATE "C"',
        (epoch_id, precursor.job_id, precursor.job_id, parent_key, parent_key),
    )
    add(
        "effective_job_chunks",
        (epoch_id, precursor.job_id, parent_key),
        "SELECT effective.* FROM groundloop_m4_effective_chunk_version AS effective "
        "WHERE effective.epoch_id = %s AND effective.chunk_version_id IN ("
        "SELECT job.chunk_version_id FROM groundloop_semantic_job AS job "
        "WHERE job.epoch_id = %s AND job.chunk_version_id IS NOT NULL "
        "AND (job.job_id = %s OR job.parent_job_id = %s OR job.job_id = %s "
        "OR job.parent_job_id = %s)) "
        'ORDER BY effective.chunk_version_id COLLATE "C"',
        (
            epoch_id,
            epoch_id,
            precursor.job_id,
            precursor.job_id,
            parent_key,
            parent_key,
        ),
    )
    add(
        "discovery_results",
        (epoch_id, precursor.job_id, parent_key),
        "SELECT * FROM groundloop_m4_discovery_result WHERE epoch_id = %s "
        'AND root_job_id IN (%s,%s) ORDER BY root_job_id COLLATE "C"',
        (epoch_id, precursor.job_id, parent_key),
    )
    add(
        "discovery_scope_immutable",
        (epoch_id, precursor.job_id, parent_key),
        "SELECT root_job_id, epoch_id, registry_snapshot_id, scope_kind, "
        "explicit_claim_ids FROM groundloop_discovery_scope WHERE epoch_id = %s "
        'AND root_job_id IN (%s,%s) ORDER BY root_job_id COLLATE "C"',
        (epoch_id, precursor.job_id, parent_key),
    )
    if precursor.job_kind == "verify_pair" and precursor.parent_job_id is not None:
        add(
            "parent_discovery_scope_closed_revision",
            (precursor.parent_job_id,),
            "SELECT closed_revision FROM groundloop_discovery_scope "
            "WHERE epoch_id = %s AND root_job_id = %s",
            (epoch_id, precursor.parent_job_id),
        )
    scope_roots = (
        (precursor.job_id,)
        if precursor.job_kind in {"impact_discovery", "frontier_retrieve"}
        else ((precursor.parent_job_id,) if precursor.parent_job_id is not None else ())
    )
    for root_job_id in scope_roots:
        scope_job = cursor.execute(
            """
            SELECT job_kind, claim_id, chunk_version_id
            FROM groundloop_semantic_job
            WHERE epoch_id = %s AND job_id = %s
            """,
            (epoch_id, root_job_id),
        ).fetchone()
        if scope_job is None or _text(scope_job[0]) not in {
            "impact_discovery",
            "frontier_retrieve",
        }:
            raise EventConflictError("direct discovery range root changed")
        scope_column = (
            "chunk_version_id"
            if _text(scope_job[0]) == "impact_discovery"
            else "claim_id"
        )
        scope_value = (
            scope_job[2] if scope_column == "chunk_version_id" else scope_job[1]
        )
        if scope_value is None:
            raise EventConflictError("direct discovery range key changed")
        add(
            "discovery_channel_range",
            (root_job_id, scope_column, _text(scope_value)),
            "SELECT epoch_id, chunk_version_id, claim_id, "
            "candidate_policy_id, channel, rank, score, channel_artifact_hash "
            "FROM groundloop_impact_channel_hit WHERE epoch_id = %s "
            "AND candidate_policy_id = %s AND "
            + scope_column
            + ' = %s ORDER BY channel COLLATE "C", rank, '
            'claim_id COLLATE "C", chunk_version_id COLLATE "C"',
            (epoch_id, precursor.candidate_policy_id, scope_value),
        )
        add(
            "admitted_pair_range",
            (root_job_id, scope_column, _text(scope_value)),
            "SELECT admitted_pair_id, epoch_id, chunk_version_id, claim_id, "
            "candidate_policy_id, fused_rank, reasons, mandatory_lineage "
            "FROM groundloop_admitted_pair WHERE epoch_id = %s "
            "AND candidate_policy_id = %s AND "
            + scope_column
            + ' = %s ORDER BY fused_rank, claim_id COLLATE "C", '
            'chunk_version_id COLLATE "C"',
            (epoch_id, precursor.candidate_policy_id, scope_value),
        )
    if precursor.job_kind == "verify_pair":
        add(
            "verification_execution",
            (precursor.job_id,),
            "SELECT * FROM groundloop_m4_verification_execution WHERE job_id = %s",
            (precursor.job_id,),
        )
        add(
            "pair_judgment",
            (
                precursor.claim_id,
                precursor.chunk_version_id,
                precursor.result_artifact_id,
                precursor.decision_policy_version,
                precursor.observation_id,
            ),
            "SELECT judgment_id, claim_id, chunk_version_id, source_kind, "
            "source_artifact_id, decision_policy_or_guideline_id, "
            "derived_label, support_score, refute_score, neutral_score, "
            "input_hash, split_id, manifest_id FROM groundloop_pair_judgment "
            "WHERE claim_id = %s AND chunk_version_id = %s "
            "AND source_kind = 'model' AND source_artifact_id = %s "
            "AND decision_policy_or_guideline_id = %s "
            "AND input_hash = (SELECT input_hash "
            "FROM groundloop_semantic_observation WHERE observation_id = %s) "
            'ORDER BY judgment_id COLLATE "C"',
            (
                precursor.claim_id,
                precursor.chunk_version_id,
                precursor.result_artifact_id,
                precursor.decision_policy_version,
                precursor.observation_id,
            ),
        )
        if precursor.claim_id is None or precursor.chunk_version_id is None:
            raise EventConflictError("direct verifier frontier authority changed")
        add(
            "frontier_target_immutable",
            (epoch_id, precursor.claim_id, precursor.chunk_version_id),
            "SELECT claim_id, chunk_version_id, candidate_policy_id, "
            "valid_from_epoch, rank, retrieval_score, candidate_artifact_hash, "
            "valid_to_epoch FROM groundloop_candidate_frontier "
            "WHERE claim_id = %s AND chunk_version_id = %s "
            "AND candidate_policy_id = %s AND valid_from_epoch = %s",
            (
                precursor.claim_id,
                precursor.chunk_version_id,
                precursor.candidate_policy_id,
                epoch_id,
            ),
        )
    if precursor.model_authority is not None:
        add(
            "model_authority",
            (precursor.model_authority[0],),
            "SELECT * FROM groundloop_model_artifact WHERE model_artifact_id = %s",
            (precursor.model_authority[0],),
        )
    if precursor.prompt_authority is not None:
        add(
            "prompt_authority",
            (precursor.prompt_authority[0],),
            "SELECT * FROM groundloop_prompt_artifact WHERE prompt_artifact_id = %s",
            (precursor.prompt_authority[0],),
        )
    if precursor.job_kind == "verify_pair" and precursor.answer_version_id is not None:
        add(
            "owner_citations",
            (precursor.answer_version_id,),
            "SELECT * FROM groundloop_answer_citation WHERE answer_version_id = %s "
            "ORDER BY citation_ordinal",
            (precursor.answer_version_id,),
        )
    observation_ids: set[str] = set()
    if precursor.observation_id is not None:
        observation_ids.add(precursor.observation_id)
    if logical_plan.base_observation_id is not None:
        observation_ids.add(logical_plan.base_observation_id)
    if logical_plan.claim_before is not None:
        observation_ids.update(
            logical_plan.claim_before.state.supporting_observation_ids
        )
        observation_ids.update(logical_plan.claim_before.state.refuting_observation_ids)
    if logical_plan.claim_after is not None:
        observation_ids.update(
            logical_plan.claim_after.state.supporting_observation_ids
        )
        observation_ids.update(logical_plan.claim_after.state.refuting_observation_ids)
    ordered_observations = tuple(sorted(observation_ids))
    add(
        "semantic_observations",
        ordered_observations,
        "SELECT * FROM groundloop_semantic_observation "
        'WHERE observation_id = ANY(%s) ORDER BY observation_id COLLATE "C"',
        (list(ordered_observations),),
    )
    if (
        precursor.observation_id is not None
        and precursor.claim_id is not None
        and precursor.chunk_version_id is not None
    ):
        add(
            "published_observation_currency",
            (precursor.claim_id, precursor.chunk_version_id or ""),
            "SELECT subject_kind, subject_id, chunk_version_id, task_type, "
            "observation_id, valid_from_epoch, valid_to_epoch "
            "FROM groundloop_published_observation_currency "
            "WHERE subject_kind = 'claim' AND subject_id = %s "
            "AND chunk_version_id = %s AND task_type = (SELECT task_type "
            "FROM groundloop_semantic_observation WHERE observation_id = %s) "
            "AND valid_from_epoch <= %s "
            "AND (valid_to_epoch IS NULL OR %s < valid_to_epoch) "
            "ORDER BY valid_from_epoch",
            (
                precursor.claim_id,
                precursor.chunk_version_id,
                precursor.observation_id,
                m4_base_epoch,
                m4_base_epoch,
            ),
        )
    add(
        "matching_image_current",
        (),
        "SELECT * FROM groundloop_m5_matching_image_current WHERE singleton",
        (),
    )
    add(
        "matching_image_working",
        (epoch_id,),
        "SELECT * FROM groundloop_m5_matching_image_working WHERE epoch_id = %s",
        (epoch_id,),
    )
    if logical_plan.claim_before is not None and precursor.claim_id is not None:
        add(
            "m5_published_claim",
            (precursor.claim_id,),
            "SELECT * FROM groundloop_m5_published_claim_state "
            "WHERE claim_id = %s AND valid_from_epoch <= %s "
            "AND (valid_to_epoch IS NULL OR %s < valid_to_epoch) "
            "ORDER BY valid_from_epoch",
            (precursor.claim_id, m5_base_epoch, m5_base_epoch),
        )
        add(
            "m5_working_claim",
            (epoch_id, precursor.claim_id),
            "SELECT * FROM groundloop_m5_working_claim_state "
            "WHERE epoch_id = %s AND claim_id = %s",
            (epoch_id, precursor.claim_id),
        )
        add(
            "m4_published_claim",
            (precursor.claim_id,),
            "SELECT * FROM groundloop_published_claim_state "
            "WHERE claim_id = %s AND valid_from_epoch <= %s "
            "AND (valid_to_epoch IS NULL OR %s < valid_to_epoch) "
            "ORDER BY valid_from_epoch",
            (precursor.claim_id, m4_base_epoch, m4_base_epoch),
        )
        add(
            "published_claim_bindings",
            (precursor.claim_id,),
            "SELECT * FROM groundloop_m5_published_claim_certificate_binding "
            "WHERE claim_id = %s AND valid_from_epoch <= %s "
            "AND (valid_to_epoch IS NULL OR %s < valid_to_epoch) "
            "ORDER BY valid_from_epoch",
            (precursor.claim_id, m5_base_epoch, m5_base_epoch),
        )
        add(
            "working_claim_bindings",
            (epoch_id, precursor.claim_id),
            "SELECT * FROM groundloop_m5_working_claim_certificate_binding "
            "WHERE epoch_id = %s AND claim_id = %s "
            "AND valid_to_revision IS NULL ORDER BY valid_from_revision",
            (epoch_id, precursor.claim_id),
        )
    if (
        logical_plan.answer_before is not None
        and precursor.answer_version_id is not None
    ):
        add(
            "m5_published_answer",
            (precursor.answer_version_id,),
            "SELECT * FROM groundloop_m5_published_answer_state "
            "WHERE answer_version_id = %s AND valid_from_epoch <= %s "
            "AND (valid_to_epoch IS NULL OR %s < valid_to_epoch) "
            "ORDER BY valid_from_epoch",
            (precursor.answer_version_id, m5_base_epoch, m5_base_epoch),
        )
        add(
            "m5_working_answer",
            (epoch_id, precursor.answer_version_id),
            "SELECT * FROM groundloop_m5_working_answer_state "
            "WHERE epoch_id = %s AND answer_version_id = %s",
            (epoch_id, precursor.answer_version_id),
        )
        add(
            "m4_published_answer",
            (precursor.answer_version_id,),
            "SELECT * FROM groundloop_published_answer_state "
            "WHERE answer_version_id = %s AND valid_from_epoch <= %s "
            "AND (valid_to_epoch IS NULL OR %s < valid_to_epoch) "
            "ORDER BY valid_from_epoch",
            (precursor.answer_version_id, m4_base_epoch, m4_base_epoch),
        )
    group_ids: set[str] = set()
    if logical_plan.claim_before is not None:
        group_ids.update(logical_plan.claim_before.state.complete_group_ids)
    if logical_plan.claim_after is not None:
        group_ids.update(logical_plan.claim_after.state.complete_group_ids)
    ordered_group_ids = tuple(sorted(group_ids))
    add(
        "published_group_bindings",
        ordered_group_ids,
        "SELECT * FROM groundloop_m5_published_group_certificate_binding "
        "WHERE group_version_id = ANY(%s) AND valid_from_epoch <= %s "
        "AND (valid_to_epoch IS NULL OR %s < valid_to_epoch) "
        'ORDER BY group_version_id COLLATE "C", valid_from_epoch',
        (list(ordered_group_ids), m5_base_epoch, m5_base_epoch),
    )
    add(
        "working_group_bindings",
        (epoch_id, *ordered_group_ids),
        "SELECT * FROM groundloop_m5_working_group_certificate_binding "
        "WHERE epoch_id = %s AND group_version_id = ANY(%s) "
        "AND valid_to_revision IS NULL "
        'ORDER BY group_version_id COLLATE "C", valid_from_revision',
        (epoch_id, list(ordered_group_ids)),
    )
    published_group_digests = cursor.execute(
        """
        SELECT group_version_id, certificate_digest
        FROM groundloop_m5_published_group_certificate_binding
        WHERE group_version_id = ANY(%s) AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        ORDER BY group_version_id COLLATE "C"
        """,
        (list(ordered_group_ids), m5_base_epoch, m5_base_epoch),
    ).fetchall()
    working_group_digests = cursor.execute(
        """
        SELECT group_version_id, certificate_digest
        FROM groundloop_m5_working_group_certificate_binding
        WHERE epoch_id = %s AND group_version_id = ANY(%s)
          AND valid_to_revision IS NULL
        ORDER BY group_version_id COLLATE "C"
        """,
        (epoch_id, list(ordered_group_ids)),
    ).fetchall()
    published_digest_by_group = {
        _text(row[0]): _sha256_text(row[1]) for row in published_group_digests
    }
    working_digest_by_group = {
        _text(row[0]): _sha256_text(row[1]) for row in working_group_digests
    }
    effective_group_digests: list[str] = []
    for group_id in ordered_group_ids:
        digest = working_digest_by_group.get(group_id)
        if digest is None:
            digest = published_digest_by_group.get(group_id)
        if digest is None:
            raise EventConflictError("direct selected group binding changed")
        effective_group_digests.append(digest)
    group_digests = tuple(sorted(effective_group_digests))
    add(
        "group_artifacts",
        group_digests,
        "SELECT artifact.* FROM groundloop_m5_group_certificate_artifact "
        "AS artifact WHERE artifact.certificate_digest = ANY(%s) "
        "ORDER BY artifact.certificate_digest",
        (list(group_digests),),
    )
    add(
        "group_artifact_rows",
        group_digests,
        "SELECT artifact_row.* "
        "FROM groundloop_m5_group_certificate_artifact_row AS artifact_row "
        "WHERE artifact_row.certificate_digest = ANY(%s) "
        "ORDER BY artifact_row.certificate_digest, "
        "artifact_row.requirement_ordinal",
        (list(group_digests),),
    )
    claim_digests = tuple(
        sorted(
            {
                *(
                    ()
                    if logical_plan.claim_before is None
                    else (logical_plan.claim_before.certificate_digest,)
                ),
                *(
                    ()
                    if logical_plan.claim_after is None
                    else (logical_plan.claim_after.certificate_digest,)
                ),
            }
        )
    )
    add(
        "claim_artifacts",
        claim_digests,
        "SELECT * FROM groundloop_m5_claim_certificate_artifact "
        "WHERE certificate_digest = ANY(%s) ORDER BY certificate_digest",
        (list(claim_digests),),
    )
    return tuple(images)


def _direct_conflict_images(
    cursor: Cursor[Any],
    coordinates: tuple[_DirectStageCoordinate, ...],
) -> tuple[_DirectAuthorityImage, ...]:
    """Capture exact D25/advisory/unique-conflict rows without new locks."""

    images: list[_DirectAuthorityImage] = []
    for coordinate in coordinates:
        relation = coordinate.relation_name
        if relation == "groundloop_m5_working_claim_certificate_binding":
            rows = cursor.execute(
                """
                SELECT epoch_id, claim_id, valid_from_revision,
                       valid_to_revision, certificate_digest
                FROM groundloop_m5_working_claim_certificate_binding
                WHERE epoch_id = %s AND claim_id = %s
                  AND valid_from_revision = %s
                """,
                coordinate.key_parts,
            ).fetchall()
        elif relation == "groundloop_m5_working_claim_certificate_binding_open":
            rows = cursor.execute(
                """
                SELECT epoch_id, claim_id, valid_from_revision,
                       valid_to_revision, certificate_digest
                FROM groundloop_m5_working_claim_certificate_binding
                WHERE epoch_id = %s AND claim_id = %s
                  AND valid_to_revision IS NULL
                ORDER BY valid_from_revision
                """,
                coordinate.key_parts,
            ).fetchall()
        elif relation == ("groundloop_m4_evaluation_counter_transition_to_revision"):
            rows = cursor.execute(
                """
                SELECT transition_id
                FROM groundloop_m4_evaluation_counter_transition
                WHERE epoch_id = %s AND to_revision = %s
                """,
                coordinate.key_parts,
            ).fetchall()
        else:
            raise ValidationError("direct conflict coordinate is unknown")
        images.append(
            _DirectAuthorityImage(
                relation,
                coordinate.key_parts,
                tuple(_freeze_direct_authority_value(tuple(row)) for row in rows),
            )
        )
    return tuple(images)


def _empty_direct_logical_plan() -> _DirectLogicalPlan:
    return _DirectLogicalPlan(None, None, None, None, None, None, False, None, ())


def _direct_operational_label(
    *,
    support_score: float,
    refute_score: float,
    neutral_score: float,
    support_threshold: float,
    refute_threshold: float,
) -> VerificationLabel:
    if (
        refute_score >= refute_threshold
        and refute_score >= support_score
        and refute_score >= neutral_score
    ):
        return VerificationLabel.REFUTE
    if (
        support_score >= support_threshold
        and support_score > refute_score
        and support_score > neutral_score
    ):
        return VerificationLabel.SUPPORT
    return VerificationLabel.NEUTRAL


def _lock_direct_group_support_authority(
    cursor: Cursor[Any], precursor: _DirectPrecursor
) -> tuple[str, ...]:
    """Lock every possible tier-12b group support before tier-13 claims."""

    if precursor.job_kind != "verify_pair" or precursor.claim_id is None:
        return ()
    base = cursor.execute(
        """
        SELECT previous_published_epoch_id
        FROM groundloop_m5_update
        WHERE epoch_id = %s
        """,
        (precursor.epoch_id,),
    ).fetchone()
    if base is None:
        raise EventConflictError("direct verifier lacks its M5 base")
    base_epoch_id = int(base[0])
    published = cursor.execute(
        """
        SELECT complete_group_ids
        FROM groundloop_m5_published_claim_state
        WHERE claim_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (precursor.claim_id, base_epoch_id, base_epoch_id),
    ).fetchall()
    working = cursor.execute(
        """
        SELECT complete_group_ids
        FROM groundloop_m5_working_claim_state
        WHERE epoch_id = %s AND claim_id = %s
        """,
        (precursor.epoch_id, precursor.claim_id),
    ).fetchone()
    if len(published) != 1:
        raise EventConflictError("direct published claim state changed")
    values = published[0][0] if working is None else working[0]
    if not isinstance(values, (list, tuple)):
        raise EventConflictError("direct complete-group authority changed")
    group_ids = tuple(sorted(_text(value) for value in values))
    if group_ids != tuple(sorted(set(group_ids))):
        raise EventConflictError("direct complete-group authority changed")
    if not group_ids:
        return ()
    published_bindings = cursor.execute(
        """
        SELECT group_version_id, valid_from_epoch, certificate_digest
        FROM groundloop_m5_published_group_certificate_binding
        WHERE group_version_id = ANY(%s) AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        ORDER BY group_version_id COLLATE "C", valid_from_epoch
        FOR UPDATE
        """,
        (list(group_ids), base_epoch_id, base_epoch_id),
    ).fetchall()
    working_bindings = cursor.execute(
        """
        SELECT group_version_id, valid_from_revision, certificate_digest
        FROM groundloop_m5_working_group_certificate_binding
        WHERE epoch_id = %s AND group_version_id = ANY(%s)
          AND valid_to_revision IS NULL
        ORDER BY group_version_id COLLATE "C", valid_from_revision
        FOR UPDATE
        """,
        (precursor.epoch_id, list(group_ids)),
    ).fetchall()
    published_by_group = {_text(row[0]): row for row in published_bindings}
    working_by_group = {_text(row[0]): row for row in working_bindings}
    if len(published_by_group) != len(published_bindings) or len(
        working_by_group
    ) != len(working_bindings):
        raise EventConflictError("direct selected group binding changed")
    digests_to_lock: set[str] = set()
    for group_id in group_ids:
        row = working_by_group.get(group_id, published_by_group.get(group_id))
        if row is None:
            raise EventConflictError("direct selected group binding changed")
        digests_to_lock.add(_sha256_text(row[2]))
    for certificate_digest in sorted(digests_to_lock):
        stored = _stored_group_certificate_artifact(
            cursor, certificate_digest, lock=True
        )
        if stored is None or stored.group_version_id not in group_ids:
            raise EventConflictError("direct selected group artifact changed")
    return group_ids


def _direct_claim_points(
    cursor: Cursor[Any], precursor: _DirectPrecursor
) -> tuple[_ClaimStateWrite, tuple[object, ...]]:
    """Lock and validate all tier-13 claim before images."""

    claim_id = precursor.claim_id
    if claim_id is None or precursor.answer_version_id is None:
        raise EventConflictError("direct verifier logical owner changed")
    base = cursor.execute(
        """
        SELECT previous_published_epoch_id
        FROM groundloop_m5_update
        WHERE epoch_id = %s
        """,
        (precursor.epoch_id,),
    ).fetchone()
    if base is None:
        raise EventConflictError("direct verifier lacks its M5 base")
    base_epoch_id = int(base[0])
    published_claims = cursor.execute(
        """
        SELECT support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, complete_group_count,
               complete_group_ids, status, decision_policy_version,
               certificate_digest
        FROM groundloop_m5_published_claim_state
        WHERE claim_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        FOR UPDATE
        """,
        (claim_id, base_epoch_id, base_epoch_id),
    ).fetchall()
    if len(published_claims) != 1:
        raise EventConflictError("direct published claim state changed")
    _reserve_matching_absence(
        cursor,
        "groundloop_m5_working_claim_state",
        (precursor.epoch_id, claim_id),
    )
    working_claim = cursor.execute(
        """
        SELECT support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, complete_group_count,
               complete_group_ids, status, decision_policy_version,
               certificate_digest, updated_revision
        FROM groundloop_m5_working_claim_state
        WHERE epoch_id = %s AND claim_id = %s
        FOR UPDATE
        """,
        (precursor.epoch_id, claim_id),
    ).fetchone()
    claim_row = published_claims[0] if working_claim is None else working_claim
    if _text(claim_row[9]) != precursor.decision_policy_version or (
        working_claim is not None
        and int(working_claim[11]) > precursor.expected_revision
    ):
        raise EventConflictError("direct combined claim state changed")
    groups = tuple(_text(value) for value in claim_row[7])
    combined_claim = CombinedClaimState(
        claim_id,
        int(claim_row[0]),
        int(claim_row[1]),
        None if claim_row[2] is None else float(claim_row[2]),
        None if claim_row[3] is None else float(claim_row[3]),
        tuple(_text(value) for value in claim_row[4]),
        tuple(_text(value) for value in claim_row[5]),
        int(claim_row[6]),
        groups,
        ClaimStatus(_text(claim_row[8])),
    )
    if combined_claim.complete_group_count != len(
        groups
    ) or combined_claim.status is not _claim_status(
        supported=bool(combined_claim.support_count or groups),
        refuted=bool(combined_claim.refute_count),
    ):
        raise EventConflictError("direct combined claim state is inconsistent")
    claim_before = _ClaimStateWrite(
        combined_claim,
        _text(claim_row[9]),
        _sha256_text(claim_row[10]),
    )

    m4_published_claims = cursor.execute(
        """
        SELECT support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, status, certificate_digest
        FROM groundloop_published_claim_state
        WHERE claim_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        FOR UPDATE
        """,
        (claim_id, base_epoch_id, base_epoch_id),
    ).fetchall()
    if len(m4_published_claims) != 1:
        raise EventConflictError("direct M4 published claim state changed")
    working_m4_claim = cursor.execute(
        """
        SELECT support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, status, certificate_digest,
               updated_revision
        FROM groundloop_m4_working_claim_state
        WHERE epoch_id = %s AND claim_id = %s
        FOR UPDATE
        """,
        (precursor.epoch_id, claim_id),
    ).fetchone()
    m4_claim_row = (
        m4_published_claims[0] if working_m4_claim is None else working_m4_claim
    )
    if working_m4_claim is not None and int(working_m4_claim[8]) > (
        precursor.expected_revision
    ):
        raise EventConflictError("direct M4 claim state revision changed")
    direct_claim_projection = (
        int(m4_claim_row[0]),
        int(m4_claim_row[1]),
        None if m4_claim_row[2] is None else float(m4_claim_row[2]),
        None if m4_claim_row[3] is None else float(m4_claim_row[3]),
        tuple(_text(value) for value in m4_claim_row[4]),
        tuple(_text(value) for value in m4_claim_row[5]),
        ClaimStatus(_text(m4_claim_row[6])),
        _sha256_text(m4_claim_row[7]),
    )
    if direct_claim_projection[:6] != (
        combined_claim.support_count,
        combined_claim.refute_count,
        combined_claim.best_support_score,
        combined_claim.best_refute_score,
        combined_claim.supporting_observation_ids,
        combined_claim.refuting_observation_ids,
    ):
        raise EventConflictError("direct M4 and combined claim states differ")

    return claim_before, tuple(m4_claim_row)


def _direct_answer_points(
    cursor: Cursor[Any], precursor: _DirectPrecursor
) -> CombinedAnswerState:
    """Lock and validate all tier-14 answer before images."""

    answer_id = precursor.answer_version_id
    if answer_id is None:
        raise EventConflictError("direct verifier answer owner changed")
    base = cursor.execute(
        """
        SELECT previous_published_epoch_id
        FROM groundloop_m5_update
        WHERE epoch_id = %s
        """,
        (precursor.epoch_id,),
    ).fetchone()
    if base is None:
        raise EventConflictError("direct verifier lacks its M5 base")
    base_epoch_id = int(base[0])
    published_answers = cursor.execute(
        """
        SELECT required_claim_count, supported_count, unsupported_count,
               refuted_count, conflicted_count, status
        FROM groundloop_m5_published_answer_state
        WHERE answer_version_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        FOR UPDATE
        """,
        (answer_id, base_epoch_id, base_epoch_id),
    ).fetchall()
    if len(published_answers) != 1:
        raise EventConflictError("direct published answer state changed")
    _reserve_matching_absence(
        cursor,
        "groundloop_m5_working_answer_state",
        (precursor.epoch_id, answer_id),
    )
    working_answer = cursor.execute(
        """
        SELECT required_claim_count, supported_count, unsupported_count,
               refuted_count, conflicted_count, status, updated_revision
        FROM groundloop_m5_working_answer_state
        WHERE epoch_id = %s AND answer_version_id = %s
        FOR UPDATE
        """,
        (precursor.epoch_id, answer_id),
    ).fetchone()
    answer_row = published_answers[0] if working_answer is None else working_answer
    if working_answer is not None and int(working_answer[6]) > (
        precursor.expected_revision
    ):
        raise EventConflictError("direct combined answer state changed")
    answer_before = CombinedAnswerState(
        answer_id,
        int(answer_row[0]),
        int(answer_row[1]),
        int(answer_row[2]),
        int(answer_row[3]),
        int(answer_row[4]),
        AnswerStatus(_text(answer_row[5])),
    )
    answer_counts: Counter[ClaimStatus] = Counter(
        {
            ClaimStatus.SUPPORTED: answer_before.supported_count,
            ClaimStatus.UNSUPPORTED: answer_before.unsupported_count,
            ClaimStatus.REFUTED: answer_before.refuted_count,
            ClaimStatus.CONFLICTED: answer_before.conflicted_count,
        }
    )
    if _answer_status(answer_counts, answer_before.required_claim_count) is not (
        answer_before.status
    ):
        raise EventConflictError("direct combined answer state is inconsistent")
    m4_published_answers = cursor.execute(
        """
        SELECT required_claim_count, supported_count, unsupported_count,
               refuted_count, conflicted_count, status
        FROM groundloop_published_answer_state
        WHERE answer_version_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        FOR UPDATE
        """,
        (answer_id, base_epoch_id, base_epoch_id),
    ).fetchall()
    if len(m4_published_answers) != 1:
        raise EventConflictError("direct M4 published answer state changed")
    working_m4_answer = cursor.execute(
        """
        SELECT required_claim_count, supported_count, unsupported_count,
               refuted_count, conflicted_count, status, updated_revision
        FROM groundloop_m4_working_answer_state
        WHERE epoch_id = %s AND answer_version_id = %s
        FOR UPDATE
        """,
        (precursor.epoch_id, answer_id),
    ).fetchone()
    m4_answer_row = (
        m4_published_answers[0] if working_m4_answer is None else working_m4_answer
    )
    if working_m4_answer is not None and int(working_m4_answer[6]) > (
        precursor.expected_revision
    ):
        raise EventConflictError("direct M4 answer state revision changed")
    direct_answer = CombinedAnswerState(
        answer_id,
        int(m4_answer_row[0]),
        int(m4_answer_row[1]),
        int(m4_answer_row[2]),
        int(m4_answer_row[3]),
        int(m4_answer_row[4]),
        AnswerStatus(_text(m4_answer_row[5])),
    )
    if direct_answer != answer_before:
        raise EventConflictError("direct M4 and combined answer states differ")
    return answer_before


def _direct_claim_binding(
    cursor: Cursor[Any],
    precursor: _DirectPrecursor,
    claim_before: _ClaimStateWrite,
) -> _EffectiveClaimBinding:
    """Lock the effective tier-13 claim-certificate binding before answers."""

    if precursor.claim_id is None:
        raise EventConflictError("direct verifier claim owner changed")
    published_bindings = cursor.execute(
        """
        SELECT certificate_digest, sealed_revision
        FROM groundloop_m5_published_claim_certificate_binding
        WHERE claim_id = %s AND valid_from_epoch <= (
          SELECT previous_published_epoch_id
          FROM groundloop_m5_update WHERE epoch_id = %s
        ) AND (valid_to_epoch IS NULL OR (
          SELECT previous_published_epoch_id
          FROM groundloop_m5_update WHERE epoch_id = %s
        ) < valid_to_epoch)
        FOR UPDATE
        """,
        (precursor.claim_id, precursor.epoch_id, precursor.epoch_id),
    ).fetchall()
    working_bindings = cursor.execute(
        """
        SELECT certificate_digest, valid_from_revision
        FROM groundloop_m5_working_claim_certificate_binding
        WHERE epoch_id = %s AND claim_id = %s
          AND valid_to_revision IS NULL
        FOR UPDATE
        """,
        (precursor.epoch_id, precursor.claim_id),
    ).fetchall()
    if (
        len(published_bindings) > 1
        or len(working_bindings) > 1
        or (not published_bindings and not working_bindings)
    ):
        raise EventConflictError("direct claim certificate binding changed")
    claim_binding = (
        _EffectiveClaimBinding(
            _sha256_text(working_bindings[0][0]),
            int(working_bindings[0][1]),
            True,
        )
        if working_bindings
        else _EffectiveClaimBinding(
            _sha256_text(published_bindings[0][0]),
            int(published_bindings[0][1]),
            False,
        )
    )
    if claim_binding.certificate_digest != claim_before.certificate_digest:
        raise EventConflictError("direct claim state/binding changed")
    return claim_binding


def _direct_claim_artifact(
    cursor: Cursor[Any],
    *,
    precursor: _DirectPrecursor,
    state: CombinedClaimState,
    lock: bool = True,
) -> tuple[ClaimCertificateArtifact, tuple[str, ...]]:
    selected_group_id: str | None = None
    selected_group_digest: str | None = None
    group_ids: tuple[str, ...] = ()
    if state.supporting_observation_ids:
        support_kind = ClaimSupportKind.DIRECT
        direct_support = state.supporting_observation_ids[0]
    elif state.complete_group_ids:
        support_kind = ClaimSupportKind.GROUP
        direct_support = None
        selected_group_id = state.complete_group_ids[0]
        group_ids = (selected_group_id,)
        suffix = " FOR UPDATE" if lock else ""
        published = cursor.execute(
            """
            SELECT certificate_digest
            FROM groundloop_m5_published_group_certificate_binding
            WHERE group_version_id = %s AND valid_from_epoch <= (
              SELECT previous_published_epoch_id
              FROM groundloop_m5_update WHERE epoch_id = %s
            ) AND (valid_to_epoch IS NULL OR (
              SELECT previous_published_epoch_id
              FROM groundloop_m5_update WHERE epoch_id = %s
            ) < valid_to_epoch)
            """
            + suffix,
            (selected_group_id, precursor.epoch_id, precursor.epoch_id),
        ).fetchall()
        working = cursor.execute(
            """
            SELECT certificate_digest
            FROM groundloop_m5_working_group_certificate_binding
            WHERE epoch_id = %s AND group_version_id = %s
              AND valid_to_revision IS NULL
            """
            + suffix,
            (precursor.epoch_id, selected_group_id),
        ).fetchall()
        if len(published) > 1 or len(working) > 1 or (not published and not working):
            raise EventConflictError("direct selected group binding changed")
        selected_group_digest = _sha256_text(
            working[0][0] if working else published[0][0]
        )
        stored = _stored_group_certificate_artifact(
            cursor, selected_group_digest, lock=lock
        )
        if stored is None or (
            stored.group_version_id != selected_group_id
            or stored.decision_policy_version != precursor.decision_policy_version
        ):
            raise EventConflictError("direct selected group artifact changed")
    else:
        support_kind = ClaimSupportKind.NONE
        direct_support = None
    artifact = ClaimCertificateArtifact(
        claim_id=state.claim_id,
        decision_policy_version=precursor.decision_policy_version,
        support_kind=support_kind,
        direct_support_observation_id=direct_support,
        group_version_id=selected_group_id,
        group_certificate_digest=selected_group_digest,
        direct_refute_observation_id=(
            state.refuting_observation_ids[0]
            if state.refuting_observation_ids
            else None
        ),
    )
    return artifact, group_ids


def _direct_logical_plan(
    cursor: Cursor[Any],
    precursor: _DirectPrecursor,
    tier11a_provenance: tuple[tuple[str, ...], tuple[str, ...]] | None,
    group_support_ids: tuple[str, ...],
) -> tuple[
    _DirectAffectedProjection,
    _DirectLogicalPlan,
    tuple[_DirectStageCoordinate, ...],
]:
    if precursor.job_kind != "verify_pair":
        if tier11a_provenance is not None or group_support_ids:
            raise EventConflictError("direct expansion gathered verifier authority")
        return _DirectAffectedProjection(), _empty_direct_logical_plan(), ()
    if precursor.observation_id is None or precursor.claim_id is None:
        raise EventConflictError("direct verifier precursor lost its observation")
    if tier11a_provenance is None:
        raise EventConflictError("direct verifier lacks tier-11a authority")
    claim_before, m4_claim_row = _direct_claim_points(cursor, precursor)
    if claim_before.state.complete_group_ids != group_support_ids:
        raise EventConflictError("direct pre-gathered claim authority changed")
    observation = cursor.execute(
        """
        SELECT support_score, refute_score, neutral_score, subject_id,
               chunk_version_id, task_type
        FROM groundloop_semantic_observation
        WHERE observation_id = %s
        """,
        (precursor.observation_id,),
    ).fetchone()
    if observation is None or _text(observation[3]) != precursor.claim_id:
        raise EventConflictError("direct semantic observation changed")
    new_label = _direct_operational_label(
        support_score=float(observation[0]),
        refute_score=float(observation[1]),
        neutral_score=float(observation[2]),
        support_threshold=precursor.support_threshold,
        refute_threshold=precursor.refute_threshold,
    )
    observation_coordinate = _direct_observation_coordinate(cursor, precursor)
    assert observation_coordinate is not None
    if _direct_stage_row(cursor, observation_coordinate, lock=False) is not None:
        raise EventConflictError("direct observation currency key was already written")
    base_currency = cursor.execute(
        """
        SELECT observation_id
        FROM groundloop_published_observation_currency
        WHERE subject_kind = %s AND subject_id = %s
          AND chunk_version_id = %s AND task_type = %s
          AND valid_from_epoch <= (
            SELECT previous_published_epoch_id
            FROM groundloop_m4_update WHERE epoch_id = %s
          ) AND (valid_to_epoch IS NULL OR (
            SELECT previous_published_epoch_id
            FROM groundloop_m4_update WHERE epoch_id = %s
          ) < valid_to_epoch)
        """,
        (
            observation_coordinate.key_parts[1],
            observation_coordinate.key_parts[2],
            observation_coordinate.key_parts[3],
            observation_coordinate.key_parts[4],
            precursor.epoch_id,
            precursor.epoch_id,
        ),
    ).fetchall()
    if len(base_currency) > 1:
        raise EventConflictError("direct published observation currency changed")
    old_observation_id = None if not base_currency else _text(base_currency[0][0])
    support_values = m4_claim_row[4]
    refute_values = m4_claim_row[5]
    if not isinstance(support_values, (list, tuple)) or not isinstance(
        refute_values, (list, tuple)
    ):
        raise EventConflictError("direct M4 claim provenance changed")
    if (
        tuple(_text(value) for value in support_values) != tier11a_provenance[0]
        or tuple(_text(value) for value in refute_values) != tier11a_provenance[1]
    ):
        raise EventConflictError("direct pre-gathered claim authority changed")
    support_ids = set(_text(value) for value in support_values)
    refute_ids = set(_text(value) for value in refute_values)
    if old_observation_id is not None:
        old = cursor.execute(
            """
            SELECT support_score, refute_score, neutral_score
            FROM groundloop_semantic_observation
            WHERE observation_id = %s
            """,
            (old_observation_id,),
        ).fetchone()
        if old is None:
            raise EventConflictError("direct prior observation disappeared")
        old_label = _direct_operational_label(
            support_score=float(old[0]),
            refute_score=float(old[1]),
            neutral_score=float(old[2]),
            support_threshold=precursor.support_threshold,
            refute_threshold=precursor.refute_threshold,
        )
        if old_label is VerificationLabel.SUPPORT:
            if old_observation_id not in support_ids:
                raise EventConflictError("direct support provenance changed")
            support_ids.remove(old_observation_id)
        elif old_label is VerificationLabel.REFUTE:
            if old_observation_id not in refute_ids:
                raise EventConflictError("direct refute provenance changed")
            refute_ids.remove(old_observation_id)
    if new_label is VerificationLabel.SUPPORT:
        support_ids.add(precursor.observation_id)
    elif new_label is VerificationLabel.REFUTE:
        refute_ids.add(precursor.observation_id)
    score_rows = cursor.execute(
        """
        SELECT observation_id, support_score, refute_score
        FROM groundloop_semantic_observation
        WHERE observation_id = ANY(%s)
        ORDER BY observation_id COLLATE "C"
        """,
        (list(sorted(support_ids | refute_ids)),),
    ).fetchall()
    scores = {_text(row[0]): (float(row[1]), float(row[2])) for row in score_rows}
    if set(scores) != support_ids | refute_ids:
        raise EventConflictError("direct claim provenance observations changed")
    support_tuple = tuple(sorted(support_ids))
    refute_tuple = tuple(sorted(refute_ids))
    combined_after_state = CombinedClaimState(
        precursor.claim_id,
        len(support_tuple),
        len(refute_tuple),
        max((scores[item][0] for item in support_tuple), default=None),
        max((scores[item][1] for item in refute_tuple), default=None),
        support_tuple,
        refute_tuple,
        claim_before.state.complete_group_count,
        claim_before.state.complete_group_ids,
        _claim_status(
            supported=bool(support_tuple or claim_before.state.complete_group_ids),
            refuted=bool(refute_tuple),
        ),
    )
    artifact, group_ids = _direct_claim_artifact(
        cursor, precursor=precursor, state=combined_after_state, lock=False
    )
    artifact_stored = _lock_or_reserve_claim_certificate_artifact(cursor, artifact)
    claim_after = _ClaimStateWrite(
        combined_after_state,
        precursor.decision_policy_version,
        artifact.certificate_digest,
    )
    claim_binding = _direct_claim_binding(cursor, precursor, claim_before)
    provisional = _DirectLogicalPlan(
        claim_before,
        claim_after,
        None,
        None,
        artifact,
        claim_binding,
        artifact_stored,
        old_observation_id,
        (),
    )
    d25_conflict_coordinates = _reserve_direct_d25_write_targets(
        cursor, precursor, provisional
    )

    # Tier 14 follows every group/claim/certificate/binding authority above.
    answer_before = _direct_answer_points(cursor, precursor)
    answer_after = answer_before
    status_deltas: list[StatusDelta] = []
    reason = f"completion={precursor.source_id} op=DirectTransition"
    if claim_after.state.status is not claim_before.state.status:
        status_deltas.append(
            StatusDelta(
                precursor.source_id,
                "claim",
                precursor.claim_id,
                claim_before.state.status.value,
                claim_after.state.status.value,
                reason,
            )
        )
        if precursor.owner_claim_required:
            counts: Counter[ClaimStatus] = Counter(
                {
                    ClaimStatus.SUPPORTED: answer_before.supported_count,
                    ClaimStatus.UNSUPPORTED: answer_before.unsupported_count,
                    ClaimStatus.REFUTED: answer_before.refuted_count,
                    ClaimStatus.CONFLICTED: answer_before.conflicted_count,
                }
            )
            counts[claim_before.state.status] -= 1
            counts[claim_after.state.status] += 1
            answer_status = _answer_status(counts, answer_before.required_claim_count)
            answer_after = CombinedAnswerState(
                answer_before.answer_version_id,
                answer_before.required_claim_count,
                counts[ClaimStatus.SUPPORTED],
                counts[ClaimStatus.UNSUPPORTED],
                counts[ClaimStatus.REFUTED],
                counts[ClaimStatus.CONFLICTED],
                answer_status,
            )
            if answer_status is not answer_before.status:
                status_deltas.append(
                    StatusDelta(
                        precursor.source_id,
                        "answer",
                        answer_before.answer_version_id,
                        answer_before.status.value,
                        answer_status.value,
                        reason,
                    )
                )
    affected_group_ids = tuple(
        sorted({*claim_before.state.complete_group_ids, *group_ids})
    )
    projection = _DirectAffectedProjection(
        claim_state_ids=(precursor.claim_id,),
        answer_state_ids=(answer_before.answer_version_id,),
        group_certificate_ids=affected_group_ids,
        claim_certificate_ids=(precursor.claim_id,),
    )
    return (
        projection,
        _DirectLogicalPlan(
            claim_before,
            claim_after,
            answer_before,
            answer_after,
            artifact,
            claim_binding,
            artifact_stored,
            old_observation_id,
            tuple(status_deltas),
        ),
        d25_conflict_coordinates,
    )


def _direct_status_stage_coordinates(
    cursor: Cursor[Any],
    precursor: _DirectPrecursor,
    logical_plan: _DirectLogicalPlan,
) -> tuple[_DirectStageCoordinate, ...]:
    """Reserve exact legacy M4 status-transition inserts at tier 14."""

    coordinates = tuple(
        _DirectStageCoordinate(
            "groundloop_working_transition",
            (
                "epoch_id",
                "revision",
                "object_type",
                "object_id",
                "old_status",
                "new_status",
            ),
            (
                precursor.epoch_id,
                precursor.resulting_revision,
                delta.object_type,
                delta.object_id,
                delta.old_status,
                delta.new_status,
            ),
        )
        for delta in logical_plan.status_deltas
    )
    keys = tuple(
        (coordinate.relation_name, coordinate.key_columns, coordinate.key_parts)
        for coordinate in coordinates
    )
    if len(set(keys)) != len(keys):
        raise ValidationError("direct status-transition plan repeats a key")
    for coordinate in coordinates:
        if _direct_stage_row(cursor, coordinate, lock=True) is not None:
            raise EventConflictError("direct status-transition key is occupied")
        _reserve_matching_absence(
            cursor, coordinate.relation_name, coordinate.key_parts
        )
    return coordinates


def _reserve_direct_d25_write_targets(
    cursor: Cursor[Any],
    precursor: _DirectPrecursor,
    logical_plan: _DirectLogicalPlan,
) -> tuple[_DirectStageCoordinate, ...]:
    """Reserve every direct D25 insert/open-binding conflict before tier 15."""

    before = logical_plan.claim_before
    after = logical_plan.claim_after
    binding = logical_plan.claim_binding_before
    if before is None:
        return ()
    if after is None or binding is None:
        raise EventConflictError("direct verifier binding plan is incomplete")
    state_changed = after != before
    digest_changed = after.certificate_digest != before.certificate_digest
    opens_binding = digest_changed or (state_changed and not binding.working)
    if not opens_binding:
        return ()
    key = (precursor.epoch_id, after.state.claim_id, precursor.resulting_revision)
    _reserve_matching_absence(
        cursor, "groundloop_m5_working_claim_certificate_binding", key
    )
    occupied = cursor.execute(
        """
        SELECT certificate_digest, valid_to_revision
        FROM groundloop_m5_working_claim_certificate_binding
        WHERE epoch_id = %s AND claim_id = %s AND valid_from_revision = %s
        FOR UPDATE
        """,
        key,
    ).fetchone()
    if occupied is not None:
        raise EventConflictError("direct claim binding target is occupied")
    _reserve_matching_absence(
        cursor,
        "groundloop_m5_working_claim_certificate_binding_open",
        (precursor.epoch_id, after.state.claim_id),
    )
    return (
        _DirectStageCoordinate(
            "groundloop_m5_working_claim_certificate_binding",
            ("epoch_id", "claim_id", "valid_from_revision"),
            key,
        ),
        _DirectStageCoordinate(
            "groundloop_m5_working_claim_certificate_binding_open",
            ("epoch_id", "claim_id"),
            (precursor.epoch_id, after.state.claim_id),
        ),
    )


def _reserve_direct_matching_transition(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_runtime_revision: int,
    resulting_revision: int,
    proposed_source_id: str,
    job_id: str,
    attempt_id: str,
) -> _DirectMatchingReservation:
    """Create the explicit pre-source direct reservation without D25 DML."""

    _require_positive_int("epoch_id", epoch_id)
    _require_positive_int("expected_runtime_revision", expected_runtime_revision)
    if resulting_revision != expected_runtime_revision + 1:
        raise EventConflictError("direct matching reservation must advance once")
    _require_text("job_id", job_id)
    _require_text("attempt_id", attempt_id)
    proposed_source_id = _sha256_text(proposed_source_id)
    _authorize_matching_transition(
        cursor,
        epoch_id=epoch_id,
        expected_runtime_revision=expected_runtime_revision,
        resulting_revision=resulting_revision,
        source_kind=M5PersistedMatchingSourceKind.DIRECT_TRANSITION,
        source_id=proposed_source_id,
    )
    precursor = _direct_precursor(
        cursor,
        epoch_id=epoch_id,
        expected_revision=expected_runtime_revision,
        resulting_revision=resulting_revision,
        proposed_source_id=proposed_source_id,
        job_id=job_id,
        attempt_id=attempt_id,
    )
    binding = _capture_direct_context_binding(
        cursor,
        epoch_id=epoch_id,
        expected_revision=expected_runtime_revision,
        resulting_revision=resulting_revision,
        source_id=precursor.source_id,
        source_identity_hash=precursor.source_identity_hash,
    )
    base_header_before, runtime_header_before = _header_images(cursor, epoch_id)
    if (
        base_header_before.revision != expected_runtime_revision
        or base_header_before.structural_status != "committed"
        or base_header_before.semantic_status != "pending"
        or base_header_before.evaluation_state != "pending"
        or base_header_before.sealed
        or base_header_before.open_job_count < 1
        or runtime_header_before.revision != expected_runtime_revision
        or runtime_header_before.runtime_state
        not in {"structural_committed", "semantic_pending"}
        or runtime_header_before.terminal
    ):
        raise EventConflictError("direct completion header changed")
    tier11a_provenance = _lock_direct_tier11a(cursor, precursor)
    lower_coordinates = _direct_lower_stage_coordinates(cursor, precursor)
    current = cursor.execute(
        """
        SELECT decision_policy_version, installed_epoch_id, installed_revision
        FROM groundloop_m5_matching_image_current
        WHERE singleton
        FOR UPDATE
        """
    ).fetchone()
    working = cursor.execute(
        """
        SELECT base_epoch_id, base_revision, decision_policy_version,
               updated_revision
        FROM groundloop_m5_matching_image_working
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if (
        current is None
        or working is None
        or (
            int(working[0]) != int(current[1])
            or int(working[1]) != int(current[2])
            or _text(working[2]) != _text(current[0])
            or _text(working[2]) != precursor.decision_policy_version
            or int(working[3]) > expected_runtime_revision
        )
    ):
        raise EventConflictError("direct matching reservation image changed")
    group_support_ids = _lock_direct_group_support_authority(cursor, precursor)
    (
        d25_projection,
        logical_plan,
        d25_conflict_coordinates,
    ) = _direct_logical_plan(
        cursor,
        precursor,
        tier11a_provenance,
        group_support_ids,
    )
    status_coordinates = _direct_status_stage_coordinates(
        cursor, precursor, logical_plan
    )
    late_coordinates = _direct_late_stage_coordinates(cursor, precursor)
    stage_coordinates = lower_coordinates + status_coordinates + late_coordinates
    unique_conflict = _DirectStageCoordinate(
        "groundloop_m4_evaluation_counter_transition_to_revision",
        ("epoch_id", "to_revision"),
        (precursor.epoch_id, precursor.resulting_revision),
    )
    before_images = _capture_direct_stage_before_images(cursor, stage_coordinates)
    before_image_authority = tuple(
        _freeze_direct_authority_value(image.row_json) for image in before_images
    )
    conflict_coordinates = d25_conflict_coordinates + (unique_conflict,)
    conflict_before_images = _direct_conflict_images(cursor, conflict_coordinates)
    if any(image.rows for image in conflict_before_images):
        # The open-binding pseudo-coordinate deliberately captures the held
        # predecessor and is the sole non-absence exception.
        nonempty = tuple(
            image
            for image in conflict_before_images
            if image.rows
            and image.authority_name
            != "groundloop_m5_working_claim_certificate_binding_open"
        )
        if nonempty:
            raise EventConflictError("direct conflict target changed before stage")
    authority_before_images = _direct_authority_images(cursor, precursor, logical_plan)
    reservation = _DirectMatchingReservation(
        binding=binding,
        reservation_identity=0,
        precursor=precursor,
        d25_projection=d25_projection,
        logical_plan=logical_plan,
        stage_coordinates=stage_coordinates,
        remainder_coordinates=stage_coordinates + conflict_coordinates,
        before_images=before_images,
        before_image_authority=before_image_authority,
        conflict_before_images=conflict_before_images,
        authority_before_images=authority_before_images,
        base_header_before_image=base_header_before,
        runtime_header_before_image=runtime_header_before,
    )
    object.__setattr__(reservation, "reservation_identity", id(reservation))
    return reservation


def _mint_direct_m4_stage_result_for_phase(
    cursor: Cursor[Any],
    reservation: _DirectMatchingReservation,
    mutation_records: tuple[_DirectM4MutationRecord, ...],
    *,
    expected_reservation_phase: Literal["reserved", "consumed"],
) -> _DirectM4StageResult:
    """Validate and mint evidence at one exact reservation phase."""

    if type(reservation) is not _DirectMatchingReservation or (
        reservation.reservation_identity != id(reservation)
    ):
        raise ValidationError("direct matching reservation was copied or replaced")
    if reservation.phase != expected_reservation_phase:
        raise EventConflictError("direct matching reservation is in another phase")
    _validate_direct_context_binding(cursor, reservation.binding)
    precursor = reservation.precursor
    if (
        reservation.binding.source_id != precursor.source_id
        or reservation.binding.source_identity_hash != precursor.source_identity_hash
    ):
        raise EventConflictError("direct reservation source projection changed")
    if (
        tuple(item.coordinate for item in reservation.before_images)
        != (reservation.stage_coordinates)
        or tuple(
            _freeze_direct_authority_value(item.row_json)
            for item in reservation.before_images
        )
        != reservation.before_image_authority
    ):
        raise EventConflictError("direct reservation coordinate plan changed")
    if type(mutation_records) is not tuple or any(
        type(record) is not _DirectM4MutationRecord for record in mutation_records
    ):
        raise ValidationError("direct M4 mutation evidence must be an exact tuple")
    if tuple(record.coordinate for record in mutation_records) != (
        reservation.stage_coordinates
    ):
        raise EventConflictError("direct M4 mutation order differs from reservation")
    if tuple(record.first_old for record in mutation_records) != tuple(
        item.row_json for item in reservation.before_images
    ):
        raise EventConflictError("direct M4 first-old evidence changed")
    if any(
        record.statement_rowcount != 1 or record.first_old == record.final_new
        for record in mutation_records
    ):
        raise EventConflictError("direct M4 mutation rowcount or image changed")
    final_images = tuple(
        _DirectStageImage(record.coordinate, record.final_new)
        for record in mutation_records
    )
    live_images = tuple(
        _DirectStageImage(coordinate, _direct_stage_row(cursor, coordinate, lock=False))
        for coordinate in reservation.stage_coordinates
    )
    if live_images != final_images:
        raise EventConflictError("direct M4 live rows differ from mutation evidence")
    by_relation: Counter[str] = Counter()
    for record in mutation_records:
        by_relation[record.coordinate.relation_name] += record.statement_rowcount
    evidence_counts = tuple(sorted(by_relation.items()))

    before_by_coordinate = {
        item.coordinate: item.row_json for item in reservation.before_images
    }
    after_by_coordinate = {item.coordinate: item.row_json for item in final_images}
    if len(before_by_coordinate) != len(reservation.before_images) or len(
        after_by_coordinate
    ) != len(final_images):
        raise ValidationError("direct M4 stage evidence repeats a coordinate")

    def image(
        relation_name: str, key_parts: tuple[object, ...]
    ) -> tuple[object | None, object | None]:
        matches = tuple(
            coordinate
            for coordinate in reservation.stage_coordinates
            if coordinate.relation_name == relation_name
            and coordinate.key_parts == key_parts
        )
        if len(matches) != 1:
            raise EventConflictError("direct M4 stage coordinate changed")
        coordinate = matches[0]
        return before_by_coordinate[coordinate], after_by_coordinate[coordinate]

    def exact_text(row: dict[str, object], key: str) -> str:
        value = row.get(key)
        if not isinstance(value, str):
            raise EventConflictError("direct M4 stage text image changed shape")
        return value.strip()

    job_before, job_after = image("groundloop_semantic_job", (precursor.job_id,))
    attempt_before, attempt_after = image(
        "groundloop_semantic_job_attempt", (precursor.attempt_id,)
    )
    if not isinstance(job_before, dict) or not isinstance(job_after, dict):
        raise EventConflictError("direct M4 job stage differs from reservation")
    expected_job = dict(job_before)
    expected_child_set_hash = (
        _stable_m4_digest("m4-child-set-v1", *precursor.child_job_ids)
        if precursor.job_kind != "verify_pair"
        else None
    )
    expected_job.update(
        {
            "job_state": "completed_active",
            "child_closed": precursor.job_kind != "verify_pair",
            "child_set_hash": expected_child_set_hash,
            "completion_digest": precursor.source_id,
            "result_artifact_id": precursor.result_artifact_id,
            "result_artifact_hash": precursor.result_artifact_hash,
            "completed_revision": precursor.resulting_revision,
            "completed_at": job_after.get("completed_at"),
        }
    )
    if (
        job_before.get("job_state") != "running"
        or bool(job_before.get("child_closed"))
        or any(
            job_before.get(name) is not None
            for name in (
                "child_set_hash",
                "completion_digest",
                "result_artifact_id",
                "result_artifact_hash",
            )
        )
        or job_before.get("completed_revision") is not None
        or job_before.get("completed_at") is not None
        or job_after.get("completed_at") is None
        or exact_text(job_after, "completion_digest") != precursor.source_id
        or job_after != expected_job
    ):
        raise EventConflictError("direct M4 job stage differs from reservation")
    if not isinstance(attempt_before, dict) or not isinstance(attempt_after, dict):
        raise EventConflictError("direct M4 attempt stage differs from reservation")
    expected_attempt = dict(attempt_before)
    expected_attempt.update(
        {
            "attempt_state": "completed",
            "finished_at": attempt_after.get("finished_at"),
        }
    )
    if (
        attempt_before.get("attempt_state") != "leased"
        or attempt_before.get("finished_at") is not None
        or attempt_after.get("finished_at") is None
        or attempt_after != expected_attempt
    ):
        raise EventConflictError("direct M4 attempt stage differs from reservation")
    if precursor.job_kind == "impact_discovery":
        scope_before, scope_after = image(
            "groundloop_discovery_scope", (precursor.job_id,)
        )
        if not isinstance(scope_before, dict) or not isinstance(scope_after, dict):
            raise EventConflictError("direct discovery scope stage changed shape")
        expected_scope = dict(scope_before)
        expected_scope["closed_revision"] = precursor.resulting_revision
        if scope_before.get("closed_revision") is not None or (
            scope_after != expected_scope
        ):
            raise EventConflictError("direct discovery scope stage differs")
    if precursor.observation_id is not None:
        observation_coordinate = _direct_observation_coordinate(cursor, precursor)
        assert observation_coordinate is not None
        observation_before = before_by_coordinate[observation_coordinate]
        observation_after = after_by_coordinate[observation_coordinate]
        expected_observation = {
            "epoch_id": precursor.epoch_id,
            "subject_kind": "claim",
            "subject_id": precursor.claim_id,
            "chunk_version_id": precursor.chunk_version_id,
            "task_type": observation_coordinate.key_parts[4],
            "base_observation_id": reservation.logical_plan.base_observation_id,
            "working_observation_id": precursor.observation_id,
            "installed_revision": precursor.resulting_revision,
        }
        if observation_before is not None or observation_after != expected_observation:
            raise EventConflictError(
                "direct M4 observation currency stage differs from reservation"
            )
        assert precursor.claim_id is not None
        claim_before, claim_after = image(
            "groundloop_m4_working_claim_state",
            (precursor.epoch_id, precursor.claim_id),
        )
        claim_plan = reservation.logical_plan.claim_after
        if claim_plan is None:
            raise EventConflictError("direct M4 claim stage lacks its logical plan")
        claim_state = claim_plan.state
        expected_claim = {
            "epoch_id": precursor.epoch_id,
            "claim_id": precursor.claim_id,
            "support_count": claim_state.support_count,
            "refute_count": claim_state.refute_count,
            "best_support_score": claim_state.best_support_score,
            "best_refute_score": claim_state.best_refute_score,
            "supporting_observation_ids": list(claim_state.supporting_observation_ids),
            "refuting_observation_ids": list(claim_state.refuting_observation_ids),
            "status": claim_state.status.value,
            "certificate_digest": _stable_m4_digest(
                "m4-claim-certificate-v1",
                precursor.claim_id,
                (
                    claim_state.supporting_observation_ids[0]
                    if claim_state.supporting_observation_ids
                    else ""
                ),
                (
                    claim_state.refuting_observation_ids[0]
                    if claim_state.refuting_observation_ids
                    else ""
                ),
            ),
            "updated_revision": precursor.resulting_revision,
        }
        if claim_after != expected_claim:
            raise EventConflictError("direct M4 claim stage differs from reservation")
        if claim_before is not None and not isinstance(claim_before, dict):
            raise EventConflictError("direct M4 claim before image changed")
        assert precursor.answer_version_id is not None
        answer_before, answer_after = image(
            "groundloop_m4_working_answer_state",
            (precursor.epoch_id, precursor.answer_version_id),
        )
        answer_plan = reservation.logical_plan.answer_after
        if answer_plan is None:
            raise EventConflictError("direct M4 answer stage lacks its logical plan")
        expected_answer = {
            "epoch_id": precursor.epoch_id,
            "answer_version_id": precursor.answer_version_id,
            "required_claim_count": answer_plan.required_claim_count,
            "supported_count": answer_plan.supported_count,
            "unsupported_count": answer_plan.unsupported_count,
            "refuted_count": answer_plan.refuted_count,
            "conflicted_count": answer_plan.conflicted_count,
            "status": answer_plan.status.value,
            "updated_revision": precursor.resulting_revision,
        }
        if answer_after != expected_answer:
            raise EventConflictError("direct M4 answer stage differs from reservation")
        if answer_before is not None and not isinstance(answer_before, dict):
            raise EventConflictError("direct M4 answer before image changed")
        assert precursor.chunk_version_id is not None
        frontier_before, frontier_after = image(
            "groundloop_candidate_frontier",
            (
                precursor.claim_id,
                precursor.chunk_version_id,
                precursor.candidate_policy_id,
                precursor.epoch_id,
            ),
        )
        if not isinstance(frontier_before, dict) or not isinstance(
            frontier_after, dict
        ):
            raise EventConflictError("direct frontier stage changed shape")
        expected_frontier = dict(frontier_before)
        expected_frontier["frontier_state"] = "verified_current"
        if frontier_before.get("frontier_state") != "queued" or (
            frontier_after != expected_frontier
        ):
            raise EventConflictError("direct frontier stage differs")

    for delta in reservation.logical_plan.status_deltas:
        transition_before, transition_after = image(
            "groundloop_working_transition",
            (
                precursor.epoch_id,
                precursor.resulting_revision,
                delta.object_type,
                delta.object_id,
                delta.old_status,
                delta.new_status,
            ),
        )
        if transition_before is not None or not isinstance(transition_after, dict):
            raise EventConflictError("direct status-transition stage changed shape")
        if (
            transition_after.get("transition_id") is None
            or transition_after.get("epoch_id") != precursor.epoch_id
            or transition_after.get("revision") != precursor.resulting_revision
            or transition_after.get("object_type") != delta.object_type
            or transition_after.get("object_id") != delta.object_id
            or transition_after.get("old_status") != delta.old_status
            or transition_after.get("new_status") != delta.new_status
            or exact_text(transition_after, "causative_completion_digest")
            != precursor.source_id
        ):
            raise EventConflictError("direct status-transition stage differs")

    for item in final_images:
        if item.coordinate.relation_name not in {
            "groundloop_m5_owner_pending_counter",
            "groundloop_m5_answer_pending_counter",
        }:
            continue
        before = before_by_coordinate[item.coordinate]
        after = item.row_json
        if not isinstance(before, dict) or not isinstance(after, dict):
            raise EventConflictError("direct pending counter stage changed shape")
        expected = dict(before)
        expected["updated_revision"] = precursor.resulting_revision
        if (
            int(before.get("updated_revision", -1)) != precursor.expected_revision
            or after != expected
        ):
            raise EventConflictError("direct pending counter stage changed content")

    evaluation_before, evaluation_after = image(
        "groundloop_m4_evaluation_epoch_counter", (precursor.epoch_id,)
    )
    if not isinstance(evaluation_before, dict) or not isinstance(
        evaluation_after, dict
    ):
        raise EventConflictError("direct evaluation epoch stage changed shape")
    expected_evaluation = dict(evaluation_before)
    next_scope_count = int(evaluation_before["open_discovery_scope_count"]) + (
        precursor.scope_delta
    )
    if next_scope_count < 0:
        raise EventConflictError("direct evaluation scope count became negative")
    expected_evaluation["open_discovery_scope_count"] = next_scope_count
    expected_evaluation["default_evaluation_state"] = (
        "pending" if next_scope_count else "complete"
    )
    expected_evaluation["revision"] = precursor.resulting_revision
    if (
        evaluation_before.get("lifecycle_state") != "active"
        or int(evaluation_before.get("revision", -1)) != precursor.expected_revision
        or evaluation_after != expected_evaluation
    ):
        raise EventConflictError("direct evaluation epoch stage differs")

    delta_by_key = {
        ("claim", object_id): delta for object_id, delta in precursor.claim_job_deltas
    } | {
        ("answer", object_id): delta for object_id, delta in precursor.answer_job_deltas
    }
    override_write_count = 0
    for object_type, object_id in _direct_override_keys(precursor):
        counter_delta = delta_by_key[(object_type, object_id)]
        before, after = image(
            "groundloop_m4_evaluation_override_counter",
            (precursor.epoch_id, object_type, object_id),
        )
        if before is not None and not isinstance(before, dict):
            raise EventConflictError("direct override before image changed shape")
        old_count = 0 if before is None else int(before["open_required_job_count"])
        new_count = old_count + counter_delta
        if new_count < 0:
            raise EventConflictError("direct override count became negative")
        if new_count == 0:
            if after is not None:
                raise EventConflictError("direct override deletion differs")
        elif not isinstance(after, dict) or (
            int(after.get("open_required_job_count", -1)) != new_count
            or int(after.get("counter_updated_revision", -1))
            != precursor.resulting_revision
        ):
            raise EventConflictError("direct override stage differs")
        override_write_count += 1

    transition_before, transition_after = image(
        "groundloop_m4_evaluation_counter_transition",
        (precursor.epoch_id, precursor.source_id),
    )
    if (
        transition_before is not None
        or not isinstance(transition_after, dict)
        or (
            transition_after.get("payload_hash") != precursor.source_identity_hash
            or transition_after.get("transition_kind") != "delta"
            or transition_after.get("from_revision") != precursor.expected_revision
            or transition_after.get("to_revision") != precursor.resulting_revision
            or transition_after.get("override_rows_written") != override_write_count
        )
    ):
        raise EventConflictError("direct immutable source stage differs")
    result = _DirectM4StageResult(
        cursor_object_identity=id(cursor),
        backend_identity=reservation.binding.backend_identity,
        transaction_identity=reservation.binding.transaction_identity,
        matching_context_identity=reservation.binding.matching_context_identity,
        matching_journal_identity=reservation.binding.matching_journal_identity,
        matching_expected_identity=reservation.binding.matching_expected_identity,
        reservation_identity=reservation.reservation_identity,
        stage_result_identity=0,
        source_id=precursor.source_id,
        source_identity_hash=precursor.source_identity_hash,
        mutation_records=mutation_records,
        first_old_by_key=reservation.before_images,
        final_new_by_key=final_images,
        actual_write_counts=evidence_counts,
    )
    object.__setattr__(result, "stage_result_identity", id(result))
    return result


def _mint_direct_m4_stage_result(
    cursor: Cursor[Any],
    reservation: _DirectMatchingReservation,
    mutation_records: tuple[_DirectM4MutationRecord, ...],
) -> _DirectM4StageResult:
    """Mint evidence from Lane M's ordered per-statement mutation journal."""

    return _mint_direct_m4_stage_result_for_phase(
        cursor,
        reservation,
        mutation_records,
        expected_reservation_phase="reserved",
    )


def _direct_stage_result_authority(
    result: _DirectM4StageResult,
) -> tuple[object, ...]:
    """Return every stage-evidence field except self identity and lifecycle phase."""

    return (
        result.cursor_object_identity,
        result.backend_identity,
        result.transaction_identity,
        result.matching_context_identity,
        result.matching_journal_identity,
        result.matching_expected_identity,
        result.reservation_identity,
        result.source_id,
        result.source_identity_hash,
        result.mutation_records,
        result.first_old_by_key,
        result.final_new_by_key,
        result.actual_write_counts,
    )


def _direct_after_stage_logical_plan(
    cursor: Cursor[Any], precursor: _DirectPrecursor
) -> tuple[_DirectAffectedProjection, _DirectLogicalPlan]:
    """Independently derive the official D25 projection from persisted after-state."""

    if precursor.job_kind != "verify_pair":
        return _DirectAffectedProjection(), _empty_direct_logical_plan()
    if (
        precursor.claim_id is None
        or precursor.answer_version_id is None
        or precursor.observation_id is None
        or precursor.chunk_version_id is None
    ):
        raise EventConflictError("direct verifier precursor lost its owner")
    base_row = cursor.execute(
        """
        SELECT previous_published_epoch_id
        FROM groundloop_m5_update
        WHERE epoch_id = %s
        """,
        (precursor.epoch_id,),
    ).fetchone()
    if base_row is None:
        raise EventConflictError("direct verifier lacks its M5 base")
    base_epoch_id = int(base_row[0])
    published_claim = cursor.execute(
        """
        SELECT support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, complete_group_count,
               complete_group_ids, status, decision_policy_version,
               certificate_digest
        FROM groundloop_m5_published_claim_state
        WHERE claim_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (precursor.claim_id, base_epoch_id, base_epoch_id),
    ).fetchall()
    working_claim = cursor.execute(
        """
        SELECT support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, complete_group_count,
               complete_group_ids, status, decision_policy_version,
               certificate_digest, updated_revision
        FROM groundloop_m5_working_claim_state
        WHERE epoch_id = %s AND claim_id = %s
        """,
        (precursor.epoch_id, precursor.claim_id),
    ).fetchone()
    if len(published_claim) != 1:
        raise EventConflictError("direct published claim state changed")
    claim_row = published_claim[0] if working_claim is None else working_claim
    if _text(claim_row[9]) != precursor.decision_policy_version or (
        working_claim is not None
        and int(working_claim[11]) > precursor.expected_revision
    ):
        raise EventConflictError("direct combined claim state changed")
    complete_groups = tuple(_text(value) for value in claim_row[7])
    claim_before_state = CombinedClaimState(
        precursor.claim_id,
        int(claim_row[0]),
        int(claim_row[1]),
        None if claim_row[2] is None else float(claim_row[2]),
        None if claim_row[3] is None else float(claim_row[3]),
        tuple(_text(value) for value in claim_row[4]),
        tuple(_text(value) for value in claim_row[5]),
        int(claim_row[6]),
        complete_groups,
        ClaimStatus(_text(claim_row[8])),
    )
    claim_before = _ClaimStateWrite(
        claim_before_state,
        _text(claim_row[9]),
        _sha256_text(claim_row[10]),
    )
    if claim_before_state.complete_group_count != len(
        complete_groups
    ) or claim_before_state.status is not _claim_status(
        supported=bool(claim_before_state.support_count or complete_groups),
        refuted=bool(claim_before_state.refute_count),
    ):
        raise EventConflictError("direct combined claim state is inconsistent")

    direct_claim = cursor.execute(
        """
        SELECT support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, status, updated_revision
        FROM groundloop_m4_working_claim_state
        WHERE epoch_id = %s AND claim_id = %s
        """,
        (precursor.epoch_id, precursor.claim_id),
    ).fetchone()
    if direct_claim is None or int(direct_claim[7]) != precursor.resulting_revision:
        raise EventConflictError("direct M4 claim after-image changed")
    claim_after_state = CombinedClaimState(
        precursor.claim_id,
        int(direct_claim[0]),
        int(direct_claim[1]),
        None if direct_claim[2] is None else float(direct_claim[2]),
        None if direct_claim[3] is None else float(direct_claim[3]),
        tuple(_text(value) for value in direct_claim[4]),
        tuple(_text(value) for value in direct_claim[5]),
        claim_before_state.complete_group_count,
        claim_before_state.complete_group_ids,
        _claim_status(
            supported=bool(direct_claim[0] or complete_groups),
            refuted=bool(direct_claim[1]),
        ),
    )
    claim_artifact, group_ids = _direct_claim_artifact(
        cursor,
        precursor=precursor,
        state=claim_after_state,
        lock=False,
    )
    claim_after = _ClaimStateWrite(
        claim_after_state,
        precursor.decision_policy_version,
        claim_artifact.certificate_digest,
    )

    published_answer = cursor.execute(
        """
        SELECT required_claim_count, supported_count, unsupported_count,
               refuted_count, conflicted_count, status
        FROM groundloop_m5_published_answer_state
        WHERE answer_version_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (precursor.answer_version_id, base_epoch_id, base_epoch_id),
    ).fetchall()
    working_answer = cursor.execute(
        """
        SELECT required_claim_count, supported_count, unsupported_count,
               refuted_count, conflicted_count, status, updated_revision
        FROM groundloop_m5_working_answer_state
        WHERE epoch_id = %s AND answer_version_id = %s
        """,
        (precursor.epoch_id, precursor.answer_version_id),
    ).fetchone()
    if len(published_answer) != 1:
        raise EventConflictError("direct published answer state changed")
    answer_row = published_answer[0] if working_answer is None else working_answer
    if working_answer is not None and int(working_answer[6]) > (
        precursor.expected_revision
    ):
        raise EventConflictError("direct combined answer state changed")
    answer_before = CombinedAnswerState(
        precursor.answer_version_id,
        int(answer_row[0]),
        int(answer_row[1]),
        int(answer_row[2]),
        int(answer_row[3]),
        int(answer_row[4]),
        AnswerStatus(_text(answer_row[5])),
    )
    answer_after = answer_before
    status_deltas: list[StatusDelta] = []
    reason = f"completion={precursor.source_id} op=DirectTransition"
    if claim_after.state.status is not claim_before.state.status:
        status_deltas.append(
            StatusDelta(
                precursor.source_id,
                "claim",
                precursor.claim_id,
                claim_before.state.status.value,
                claim_after.state.status.value,
                reason,
            )
        )
        if precursor.owner_claim_required:
            counts: Counter[ClaimStatus] = Counter(
                {
                    ClaimStatus.SUPPORTED: answer_before.supported_count,
                    ClaimStatus.UNSUPPORTED: answer_before.unsupported_count,
                    ClaimStatus.REFUTED: answer_before.refuted_count,
                    ClaimStatus.CONFLICTED: answer_before.conflicted_count,
                }
            )
            counts[claim_before.state.status] -= 1
            counts[claim_after.state.status] += 1
            next_status = _answer_status(counts, answer_before.required_claim_count)
            answer_after = CombinedAnswerState(
                answer_before.answer_version_id,
                answer_before.required_claim_count,
                counts[ClaimStatus.SUPPORTED],
                counts[ClaimStatus.UNSUPPORTED],
                counts[ClaimStatus.REFUTED],
                counts[ClaimStatus.CONFLICTED],
                next_status,
            )
            if next_status is not answer_before.status:
                status_deltas.append(
                    StatusDelta(
                        precursor.source_id,
                        "answer",
                        answer_before.answer_version_id,
                        answer_before.status.value,
                        next_status.value,
                        reason,
                    )
                )

    published_bindings = cursor.execute(
        """
        SELECT certificate_digest, sealed_revision
        FROM groundloop_m5_published_claim_certificate_binding
        WHERE claim_id = %s AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (precursor.claim_id, base_epoch_id, base_epoch_id),
    ).fetchall()
    working_bindings = cursor.execute(
        """
        SELECT certificate_digest, valid_from_revision
        FROM groundloop_m5_working_claim_certificate_binding
        WHERE epoch_id = %s AND claim_id = %s
          AND valid_to_revision IS NULL
        """,
        (precursor.epoch_id, precursor.claim_id),
    ).fetchall()
    if (
        len(published_bindings) > 1
        or len(working_bindings) > 1
        or (not published_bindings and not working_bindings)
    ):
        raise EventConflictError("direct claim certificate binding changed")
    claim_binding = (
        _EffectiveClaimBinding(
            _sha256_text(working_bindings[0][0]),
            int(working_bindings[0][1]),
            True,
        )
        if working_bindings
        else _EffectiveClaimBinding(
            _sha256_text(published_bindings[0][0]),
            int(published_bindings[0][1]),
            False,
        )
    )
    if claim_binding.certificate_digest != claim_before.certificate_digest:
        raise EventConflictError("direct claim state/binding changed")
    stored_artifact = _stored_claim_certificate_artifact(
        cursor, claim_artifact.certificate_digest, lock=False
    )
    if stored_artifact is not None and stored_artifact != claim_artifact:
        raise EventConflictError("matching claim certificate digest collision")
    base_currency = cursor.execute(
        """
        SELECT observation_id
        FROM groundloop_published_observation_currency
        WHERE subject_kind = 'claim' AND subject_id = %s
          AND chunk_version_id = %s AND task_type = (
            SELECT task_type FROM groundloop_semantic_observation
            WHERE observation_id = %s
          )
          AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        """,
        (
            precursor.claim_id,
            precursor.chunk_version_id,
            precursor.observation_id,
            base_epoch_id,
            base_epoch_id,
        ),
    ).fetchall()
    if len(base_currency) > 1:
        raise EventConflictError("direct published observation currency changed")
    base_observation_id = None if not base_currency else _text(base_currency[0][0])
    affected_group_ids = tuple(
        sorted({*claim_before.state.complete_group_ids, *group_ids})
    )
    projection = _DirectAffectedProjection(
        claim_state_ids=(precursor.claim_id,),
        answer_state_ids=(precursor.answer_version_id,),
        group_certificate_ids=affected_group_ids,
        claim_certificate_ids=(precursor.claim_id,),
    )
    return projection, _DirectLogicalPlan(
        claim_before,
        claim_after,
        answer_before,
        answer_after,
        claim_artifact,
        claim_binding,
        stored_artifact is not None,
        base_observation_id,
        tuple(status_deltas),
    )


def _direct_remainder_after_stage(
    cursor: Cursor[Any],
    precursor: _DirectPrecursor,
    logical_plan: _DirectLogicalPlan,
) -> tuple[tuple[_DirectStageCoordinate, ...], tuple[_DirectStageCoordinate, ...]]:
    """Rederive the ordered stage and conflict coordinates after source insert."""

    stage: list[_DirectStageCoordinate] = [
        _DirectStageCoordinate(
            "groundloop_semantic_job", ("job_id",), (precursor.job_id,)
        ),
        _DirectStageCoordinate(
            "groundloop_semantic_job_attempt",
            ("attempt_id",),
            (precursor.attempt_id,),
        ),
    ]
    observation_coordinate = _direct_observation_coordinate(cursor, precursor)
    if observation_coordinate is not None:
        stage.append(observation_coordinate)
        assert precursor.claim_id is not None
        assert precursor.answer_version_id is not None
        assert precursor.chunk_version_id is not None
        stage.extend(
            (
                _DirectStageCoordinate(
                    "groundloop_m4_working_claim_state",
                    ("epoch_id", "claim_id"),
                    (precursor.epoch_id, precursor.claim_id),
                ),
                _DirectStageCoordinate(
                    "groundloop_m4_working_answer_state",
                    ("epoch_id", "answer_version_id"),
                    (precursor.epoch_id, precursor.answer_version_id),
                ),
                _DirectStageCoordinate(
                    "groundloop_candidate_frontier",
                    (
                        "claim_id",
                        "chunk_version_id",
                        "candidate_policy_id",
                        "valid_from_epoch",
                    ),
                    (
                        precursor.claim_id,
                        precursor.chunk_version_id,
                        precursor.candidate_policy_id,
                        precursor.epoch_id,
                    ),
                ),
            )
        )
    elif precursor.job_kind == "impact_discovery":
        stage.append(
            _DirectStageCoordinate(
                "groundloop_discovery_scope",
                ("root_job_id",),
                (precursor.job_id,),
            )
        )
    stage.extend(
        _DirectStageCoordinate(
            "groundloop_working_transition",
            (
                "epoch_id",
                "revision",
                "object_type",
                "object_id",
                "old_status",
                "new_status",
            ),
            (
                precursor.epoch_id,
                precursor.resulting_revision,
                delta.object_type,
                delta.object_id,
                delta.old_status,
                delta.new_status,
            ),
        )
        for delta in logical_plan.status_deltas
    )
    owner_pending = cursor.execute(
        """
        SELECT owner_claim_id
        FROM groundloop_m5_owner_pending_counter
        WHERE epoch_id = %s
        ORDER BY owner_claim_id COLLATE "C"
        """,
        (precursor.epoch_id,),
    ).fetchall()
    stage.extend(
        _DirectStageCoordinate(
            "groundloop_m5_owner_pending_counter",
            ("epoch_id", "owner_claim_id"),
            (precursor.epoch_id, _text(row[0])),
        )
        for row in owner_pending
    )
    answer_pending = cursor.execute(
        """
        SELECT answer_version_id
        FROM groundloop_m5_answer_pending_counter
        WHERE epoch_id = %s
        ORDER BY answer_version_id COLLATE "C"
        """,
        (precursor.epoch_id,),
    ).fetchall()
    stage.extend(
        _DirectStageCoordinate(
            "groundloop_m5_answer_pending_counter",
            ("epoch_id", "answer_version_id"),
            (precursor.epoch_id, _text(row[0])),
        )
        for row in answer_pending
    )
    stage.append(
        _DirectStageCoordinate(
            "groundloop_m4_evaluation_epoch_counter",
            ("epoch_id",),
            (precursor.epoch_id,),
        )
    )
    stage.extend(
        _DirectStageCoordinate(
            "groundloop_m4_evaluation_override_counter",
            ("epoch_id", "object_type", "object_id"),
            (precursor.epoch_id, object_type, object_id),
        )
        for object_type, object_id in _direct_override_keys(precursor)
    )
    stage.append(
        _DirectStageCoordinate(
            "groundloop_m4_evaluation_counter_transition",
            ("epoch_id", "transition_id"),
            (precursor.epoch_id, precursor.source_id),
        )
    )

    conflicts: list[_DirectStageCoordinate] = []
    before = logical_plan.claim_before
    after = logical_plan.claim_after
    binding = logical_plan.claim_binding_before
    if before is not None:
        if after is None or binding is None:
            raise EventConflictError("direct verifier binding plan is incomplete")
        state_changed = after != before
        digest_changed = after.certificate_digest != before.certificate_digest
        if digest_changed or (state_changed and not binding.working):
            conflicts.extend(
                (
                    _DirectStageCoordinate(
                        "groundloop_m5_working_claim_certificate_binding",
                        ("epoch_id", "claim_id", "valid_from_revision"),
                        (
                            precursor.epoch_id,
                            after.state.claim_id,
                            precursor.resulting_revision,
                        ),
                    ),
                    _DirectStageCoordinate(
                        "groundloop_m5_working_claim_certificate_binding_open",
                        ("epoch_id", "claim_id"),
                        (precursor.epoch_id, after.state.claim_id),
                    ),
                )
            )
    conflicts.append(
        _DirectStageCoordinate(
            "groundloop_m4_evaluation_counter_transition_to_revision",
            ("epoch_id", "to_revision"),
            (precursor.epoch_id, precursor.resulting_revision),
        )
    )
    stage_tuple = tuple(stage)
    remainder = stage_tuple + tuple(conflicts)
    keys = tuple(
        (item.relation_name, item.key_columns, item.key_parts) for item in remainder
    )
    if len(set(keys)) != len(keys):
        raise EventConflictError("direct official remainder repeats a coordinate")
    return stage_tuple, remainder


def _direct_projection_from_logical_plan(
    precursor: _DirectPrecursor,
    logical_plan: _DirectLogicalPlan,
) -> _DirectAffectedProjection:
    """Rebuild the public affected-key projection from frozen logical authority."""

    if precursor.job_kind != "verify_pair":
        if logical_plan != _empty_direct_logical_plan():
            raise EventConflictError("direct non-verifier logical plan changed")
        return _DirectAffectedProjection()
    before = logical_plan.claim_before
    after = logical_plan.claim_after
    answer_before = logical_plan.answer_before
    answer_after = logical_plan.answer_after
    artifact = logical_plan.claim_artifact
    binding = logical_plan.claim_binding_before
    if (
        precursor.claim_id is None
        or precursor.answer_version_id is None
        or before is None
        or after is None
        or answer_before is None
        or answer_after is None
        or artifact is None
        or binding is None
        or before.state.claim_id != precursor.claim_id
        or after.state.claim_id != precursor.claim_id
        or answer_before.answer_version_id != precursor.answer_version_id
        or answer_after.answer_version_id != precursor.answer_version_id
        or artifact.claim_id != precursor.claim_id
        or artifact.certificate_digest != after.certificate_digest
        or binding.certificate_digest != before.certificate_digest
    ):
        raise EventConflictError("direct verifier logical authority changed")
    affected_groups = tuple(
        sorted(
            {
                *before.state.complete_group_ids,
                *after.state.complete_group_ids,
            }
        )
    )
    return _DirectAffectedProjection(
        claim_state_ids=(precursor.claim_id,),
        answer_state_ids=(precursor.answer_version_id,),
        group_certificate_ids=affected_groups,
        claim_certificate_ids=(precursor.claim_id,),
    )


def _direct_official_intent(
    precursor: _DirectPrecursor,
    projection: _DirectAffectedProjection,
) -> M5PersistedMatchingTransitionIntent:
    """Build the frozen public DTO from the validated persisted direct source."""

    values: dict[str, Any] = {
        "source_kind": M5PersistedMatchingSourceKind.DIRECT_TRANSITION,
        "source_id": precursor.source_id,
        "source_identity_hash": precursor.source_identity_hash,
        "before_epoch_id": precursor.epoch_id,
        "before_revision": precursor.expected_revision,
        "resulting_epoch_id": precursor.epoch_id,
        "resulting_revision": precursor.resulting_revision,
        "decision_policy_version": precursor.decision_policy_version,
        "group_shapes": projection.group_shapes,
        "observation_ids": projection.observation_ids,
        "edge_keys": projection.edge_keys,
        "mask_keys": projection.mask_keys,
        "hall_group_ids": projection.hall_group_ids,
        "requirement_state_ids": projection.requirement_state_ids,
        "group_state_ids": projection.group_state_ids,
        "claim_state_ids": projection.claim_state_ids,
        "answer_state_ids": projection.answer_state_ids,
        "group_certificate_ids": projection.group_certificate_ids,
        "claim_certificate_ids": projection.claim_certificate_ids,
    }
    digest_values = {
        **values,
        "group_shapes": tuple(shape.digest_row for shape in projection.group_shapes),
    }
    return M5PersistedMatchingTransitionIntent(
        **values,
        intent_digest=digests.persisted_matching_transition_intent_digest(
            **digest_values
        ),
    )


def _validate_direct_precursor_after_stage(
    cursor: Cursor[Any], precursor: _DirectPrecursor
) -> None:
    """Rederive direct source/projection fields from immutable/live store rows."""

    direct_policy = _direct_candidate_policy(
        cursor, precursor.candidate_policy_id, lock=False
    )
    authority = cursor.execute(
        """
        SELECT update_row.candidate_policy_id, update_row.registry_snapshot_id,
               policy.decision_policy_version,
               policy.claim_registry_snapshot_id,
               decision.support_threshold, decision.refute_threshold,
               decision.tie_rule_version
        FROM groundloop_m4_update AS update_row
        JOIN groundloop_candidate_policy AS policy
          ON policy.candidate_policy_id = update_row.candidate_policy_id
        JOIN groundloop_decision_policy AS decision
          ON decision.policy_version = policy.decision_policy_version
        WHERE update_row.epoch_id = %s
        """,
        (precursor.epoch_id,),
    ).fetchone()
    if authority is None or (
        _text(authority[0]) != precursor.candidate_policy_id
        or _text(authority[1]) != precursor.registry_snapshot_id
        or _text(authority[2]) != precursor.decision_policy_version
        or _text(authority[3]) != precursor.registry_snapshot_id
        or float(authority[4]) != precursor.support_threshold
        or float(authority[5]) != precursor.refute_threshold
        or _text(authority[6]) != "v1"
        or direct_policy != precursor.candidate_policy
    ):
        raise EventConflictError("direct precursor policy authority changed")

    raw_claim_deltas: list[tuple[str, int]] = []
    scope_delta = 0
    if precursor.job_kind in {"impact_discovery", "frontier_retrieve"}:
        discovery = cursor.execute(
            """
            SELECT result.result_artifact_id, result.result_artifact_hash,
                   job.result_artifact_id, job.result_artifact_hash
            FROM groundloop_m4_discovery_result AS result
            JOIN groundloop_semantic_job AS job
              ON job.job_id = result.root_job_id
             AND job.epoch_id = result.epoch_id
            WHERE result.root_job_id = %s AND result.epoch_id = %s
            """,
            (precursor.job_id, precursor.epoch_id),
        ).fetchone()
        dependencies = cursor.execute(
            """
            SELECT dependency.child_job_id, child.parent_job_id,
                   child.job_kind, child.candidate_policy_id,
                   child.claim_id, child.job_state
            FROM groundloop_semantic_job_dependency AS dependency
            JOIN groundloop_semantic_job AS child
              ON child.epoch_id = dependency.epoch_id
             AND child.job_id = dependency.child_job_id
            WHERE dependency.epoch_id = %s
              AND dependency.parent_job_id = %s
            ORDER BY dependency.child_job_id COLLATE "C"
            """,
            (precursor.epoch_id, precursor.job_id),
        ).fetchall()
        child_ids = tuple(_text(row[0]) for row in dependencies)
        if discovery is None or (
            _text(discovery[0]) != _text(discovery[2])
            or _sha256_text(discovery[1]) != _sha256_text(discovery[3])
            or child_ids != precursor.child_job_ids
            or any(
                _text(row[1]) != precursor.job_id
                or _text(row[2]) != "verify_pair"
                or _text(row[3]) != precursor.candidate_policy_id
                or row[4] is None
                or _text(row[5]) != "declared"
                for row in dependencies
            )
        ):
            raise EventConflictError("direct discovery precursor changed after stage")
        raw_claim_deltas.extend((_text(row[4]), 1) for row in dependencies)
        if precursor.job_kind == "frontier_retrieve":
            if precursor.claim_id is None:
                raise EventConflictError("direct frontier precursor lost its claim")
            raw_claim_deltas.append((precursor.claim_id, -1))
        else:
            scope_delta = -1
    else:
        execution = cursor.execute(
            """
            SELECT execution.observation_id, execution.execution_spec_hash,
                   observation.subject_kind, observation.subject_id,
                   observation.chunk_version_id, observation.produced_epoch
            FROM groundloop_m4_verification_execution AS execution
            JOIN groundloop_semantic_observation AS observation
              USING (observation_id)
            WHERE execution.job_id = %s
            """,
            (precursor.job_id,),
        ).fetchone()
        if execution is None or (
            precursor.observation_id is None
            or _text(execution[0]) != precursor.observation_id
            or _text(execution[2]) != "claim"
            or precursor.claim_id is None
            or _text(execution[3]) != precursor.claim_id
            or precursor.chunk_version_id is None
            or _text(execution[4]) != precursor.chunk_version_id
            or int(execution[5]) != precursor.epoch_id
        ):
            raise EventConflictError("direct verifier precursor changed after stage")
        raw_claim_deltas.append((precursor.claim_id, -1))
    claim_deltas = _canonical_direct_deltas(raw_claim_deltas)
    if (
        claim_deltas != precursor.claim_job_deltas
        or scope_delta != precursor.scope_delta
    ):
        raise EventConflictError("direct precursor counter projection changed")

    delta_claim_ids = [claim_id for claim_id, _ in claim_deltas]
    claim_rows = (
        cursor.execute(
            """
            SELECT member.claim_id, claim.answer_version_id, claim.required
            FROM groundloop_m4_claim_registry_member AS member
            JOIN groundloop_claim AS claim USING (claim_id)
            WHERE member.claim_registry_snapshot_id = %s
              AND member.claim_id = ANY(%s)
            ORDER BY member.claim_id COLLATE "C"
            """,
            (precursor.registry_snapshot_id, delta_claim_ids),
        ).fetchall()
        if delta_claim_ids
        else ()
    )
    if tuple(_text(row[0]) for row in claim_rows) != tuple(delta_claim_ids):
        raise EventConflictError("direct precursor registry projection changed")
    delta_by_claim = dict(claim_deltas)
    answer_deltas = _canonical_direct_deltas(
        tuple(
            (_text(row[1]), delta_by_claim[_text(row[0])])
            for row in claim_rows
            if bool(row[2])
        )
    )
    if answer_deltas != precursor.answer_job_deltas:
        raise EventConflictError("direct precursor answer projection changed")
    source_hash = _direct_transition_payload_hash(
        expected_revision=precursor.expected_revision,
        scope_delta=scope_delta,
        claim_job_deltas=claim_deltas,
    )
    if source_hash != precursor.source_identity_hash:
        raise EventConflictError("direct precursor source hash changed")
    _validate_direct_verifier_closure(cursor, precursor)


def _validate_direct_stage_evidence(
    cursor: Cursor[Any],
    reservation: _DirectMatchingReservation,
    stage_result: _DirectM4StageResult,
    *,
    expected_reservation_phase: Literal["reserved", "consumed"],
    expected_stage_phase: Literal["staged", "consumed"],
    matching_stage_installed: bool = False,
) -> None:
    """Revalidate exact lexical evidence and its live after-images without locks."""

    if type(reservation) is not _DirectMatchingReservation or (
        reservation.reservation_identity != id(reservation)
    ):
        raise ValidationError("direct matching reservation was copied or replaced")
    if type(stage_result) is not _DirectM4StageResult or (
        stage_result.stage_result_identity != id(stage_result)
    ):
        raise ValidationError("direct M4 stage evidence was copied or replaced")
    if reservation.phase != expected_reservation_phase or (
        stage_result.phase != expected_stage_phase
    ):
        raise EventConflictError("direct matching evidence is in another phase")
    _validate_direct_context_binding(cursor, reservation.binding)
    _validate_direct_precursor_after_stage(cursor, reservation.precursor)
    current_authority = _direct_authority_images(
        cursor, reservation.precursor, reservation.logical_plan
    )
    d25_plan: _MatchingWritePlan | None = None
    if matching_stage_installed:
        direct_intent = _direct_official_intent(
            reservation.precursor, reservation.d25_projection
        )
        _, d25_plan, _, _ = _direct_first_application_plan(direct_intent, reservation)
        mutable_authority = {"matching_image_working"}
        if d25_plan.claim_state_rows:
            mutable_authority.add("m5_working_claim")
        if d25_plan.answer_state_rows:
            mutable_authority.add("m5_working_answer")
        if d25_plan.claim_binding_rows:
            mutable_authority.add("working_claim_bindings")
        if d25_plan.claim_certificate_artifact_rows:
            mutable_authority.add("claim_artifacts")
        before_unmutated = tuple(
            image
            for image in reservation.authority_before_images
            if image.authority_name not in mutable_authority
        )
        current_unmutated = tuple(
            image
            for image in current_authority
            if image.authority_name not in mutable_authority
        )
    else:
        before_unmutated = reservation.authority_before_images
        current_unmutated = current_authority
    if current_unmutated != before_unmutated:
        raise EventConflictError("direct unmutated remainder authority changed")
    official_stage, official_remainder = _direct_remainder_after_stage(
        cursor, reservation.precursor, reservation.logical_plan
    )
    if official_stage != reservation.stage_coordinates or (
        official_remainder != reservation.remainder_coordinates
    ):
        raise EventConflictError("direct official remainder changed")
    conflict_coordinates = reservation.remainder_coordinates[
        len(reservation.stage_coordinates) :
    ]
    current_conflicts = _direct_conflict_images(cursor, conflict_coordinates)
    if len(current_conflicts) != len(reservation.conflict_before_images):
        raise EventConflictError("direct conflict remainder changed shape")
    for before, current in zip(
        reservation.conflict_before_images, current_conflicts, strict=True
    ):
        if (
            before.authority_name != current.authority_name
            or before.key_parts != current.key_parts
        ):
            raise EventConflictError("direct conflict remainder changed identity")
        if before.authority_name == (
            "groundloop_m4_evaluation_counter_transition_to_revision"
        ):
            if before.rows or current.rows != ((reservation.precursor.source_id,),):
                raise EventConflictError("direct transition revision changed")
        elif matching_stage_installed:
            if d25_plan is None:
                raise EventConflictError("direct matching write plan disappeared")
            planned_rows = tuple(
                row
                for row in d25_plan.claim_binding_rows
                if row.epoch_id == reservation.precursor.epoch_id
                and row.claim_id == before.key_parts[1]
                and (
                    before.authority_name
                    == "groundloop_m5_working_claim_certificate_binding_open"
                    and row.valid_to_revision is None
                    or before.authority_name
                    == "groundloop_m5_working_claim_certificate_binding"
                    and row.valid_from_revision == before.key_parts[2]
                )
            )
            expected_rows = tuple(
                (
                    row.epoch_id,
                    row.claim_id,
                    row.valid_from_revision,
                    row.valid_to_revision,
                    row.certificate_digest,
                )
                for row in sorted(planned_rows, key=lambda row: row.valid_from_revision)
            )
            if current.rows != expected_rows:
                raise EventConflictError("direct D25 conflict authority changed")
        elif current.rows != before.rows:
            raise EventConflictError("direct D25 conflict authority changed")
    binding = reservation.binding
    if (
        stage_result.cursor_object_identity != id(cursor)
        or stage_result.backend_identity != binding.backend_identity
        or stage_result.transaction_identity != binding.transaction_identity
        or stage_result.matching_context_identity != binding.matching_context_identity
        or stage_result.matching_journal_identity != binding.matching_journal_identity
        or stage_result.matching_expected_identity != binding.matching_expected_identity
        or stage_result.reservation_identity != reservation.reservation_identity
        or stage_result.source_id != binding.source_id
        or stage_result.source_identity_hash != binding.source_identity_hash
        or tuple(record.coordinate for record in stage_result.mutation_records)
        != reservation.stage_coordinates
        or tuple(record.first_old for record in stage_result.mutation_records)
        != tuple(item.row_json for item in reservation.before_images)
        or tuple(
            _freeze_direct_authority_value(record.first_old)
            for record in stage_result.mutation_records
        )
        != reservation.before_image_authority
        or tuple(record.final_new for record in stage_result.mutation_records)
        != tuple(item.row_json for item in stage_result.final_new_by_key)
        or stage_result.first_old_by_key != reservation.before_images
        or tuple(item.coordinate for item in reservation.before_images)
        != reservation.stage_coordinates
        or tuple(item.coordinate for item in stage_result.final_new_by_key)
        != reservation.stage_coordinates
    ):
        raise EventConflictError("direct M4 stage evidence changed identity")
    live_after = tuple(
        _DirectStageImage(
            coordinate,
            _direct_stage_row(cursor, coordinate, lock=False),
        )
        for coordinate in reservation.stage_coordinates
    )
    if live_after != stage_result.final_new_by_key:
        raise EventConflictError("direct M4 stage live after-images changed")
    mutation_counts: Counter[str] = Counter()
    for record in stage_result.mutation_records:
        if record.statement_rowcount != 1 or record.first_old == record.final_new:
            raise EventConflictError("direct M4 mutation evidence changed")
        mutation_counts[record.coordinate.relation_name] += record.statement_rowcount
    expected_counts = tuple(
        sorted(
            Counter(
                item.relation_name for item in reservation.stage_coordinates
            ).items()
        )
    )
    if (
        tuple(sorted(mutation_counts.items())) != stage_result.actual_write_counts
        or expected_counts != stage_result.actual_write_counts
    ):
        raise EventConflictError("direct M4 stage actual counts changed")
    source = cursor.execute(
        """
        SELECT payload_hash, transition_kind, from_revision, to_revision,
               override_rows_written
        FROM groundloop_m4_evaluation_counter_transition
        WHERE epoch_id = %s AND transition_id = %s
        """,
        (binding.epoch_id, binding.source_id),
    ).fetchone()
    expected_override_count = len(reservation.precursor.claim_job_deltas) + len(
        reservation.precursor.answer_job_deltas
    )
    if source is None or (
        _sha256_text(source[0]) != binding.source_identity_hash
        or _text(source[1]) != "delta"
        or int(source[2]) != binding.expected_revision
        or int(source[3]) != binding.resulting_revision
        or int(source[4]) != expected_override_count
    ):
        raise EventConflictError("direct matching source differs from reservation")
    if matching_stage_installed:
        reminted = _mint_direct_m4_stage_result_for_phase(
            cursor,
            reservation,
            stage_result.mutation_records,
            expected_reservation_phase=expected_reservation_phase,
        )
        if _direct_stage_result_authority(reminted) != (
            _direct_stage_result_authority(stage_result)
        ):
            raise EventConflictError("direct M4 stage semantic evidence changed")


def _direct_first_application_plan(
    intent: M5PersistedMatchingTransitionIntent,
    reservation: _DirectMatchingReservation,
) -> tuple[
    M5PersistedMatchingPatchArtifact,
    _MatchingWritePlan,
    _D24OwnedWriteCounts,
    int,
]:
    """Materialize the D25 half exclusively from reserved prewrite authority."""

    if intent != _direct_official_intent(
        reservation.precursor, reservation.d25_projection
    ):
        raise EventConflictError("direct official intent differs from reservation")
    logical = reservation.logical_plan
    if logical.claim_before is None:
        if logical != _empty_direct_logical_plan():
            raise EventConflictError("direct expansion has a logical write plan")
        return (
            _empty_patch_artifact(intent),
            _MatchingWritePlan(),
            _D24OwnedWriteCounts(),
            0,
        )
    if (
        logical.claim_after is None
        or logical.answer_before is None
        or logical.answer_after is None
        or logical.claim_artifact is None
        or logical.claim_binding_before is None
    ):
        raise EventConflictError("direct verifier logical plan is incomplete")

    claim_before = logical.claim_before
    claim_after = logical.claim_after
    answer_before = logical.answer_before
    answer_after = logical.answer_after
    claim_binding = logical.claim_binding_before
    claim_state_changed = claim_after != claim_before
    answer_state_changed = answer_after != answer_before
    claim_before_digest = claim_before.certificate_digest
    claim_after_digest = claim_after.certificate_digest
    claim_certificate_changed = claim_before_digest != claim_after_digest

    logical_changes: list[M5PersistedLogicalChange] = []
    output_records: list[tuple[str, str, object]] = []
    claim_rows: list[_ClaimStateWrite] = []
    answer_rows: list[CombinedAnswerState] = []
    artifact_rows: list[ClaimCertificateArtifact] = []
    binding_rows: list[WorkingClaimCertificateBinding] = []
    persisted_bindings: list[M5PersistedCertificateBindingRow] = []
    claim_id = claim_after.state.claim_id
    if claim_state_changed:
        claim_rows.append(claim_after)
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.CLAIM_STATE,
                claim_id,
                _claim_state_artifact_hash(claim_before),
                _claim_state_artifact_hash(claim_after),
            )
        )
        output_records.append(("claim_state", claim_id, claim_after.state))
    if answer_state_changed:
        answer_rows.append(answer_after)
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.ANSWER_STATE,
                answer_after.answer_version_id,
                _answer_state_artifact_hash(answer_before),
                _answer_state_artifact_hash(answer_after),
            )
        )
        output_records.append(
            ("answer_state", answer_after.answer_version_id, answer_after)
        )
    if claim_certificate_changed:
        if logical.claim_artifact.certificate_digest != claim_after_digest:
            raise EventConflictError("direct claim certificate plan changed")
        if not logical.claim_artifact_already_stored:
            artifact_rows.append(logical.claim_artifact)
        logical_changes.append(
            M5PersistedLogicalChange(
                M5PersistedLogicalChangeKind.CLAIM_CERTIFICATE,
                claim_id,
                claim_before_digest,
                claim_after_digest,
            )
        )
        output_records.append(("claim_certificate", claim_id, logical.claim_artifact))
        if claim_binding.working:
            closed = WorkingClaimCertificateBinding(
                intent.resulting_epoch_id,
                claim_id,
                claim_binding.valid_from_revision,
                intent.resulting_revision,
                claim_binding.certificate_digest,
            )
            binding_rows.append(closed)
            persisted_bindings.append(
                _persisted_binding_row(M5PersistedBindingKind.CLAIM, closed)
            )
            output_records.append(("claim_binding", claim_id, closed))
        opened = WorkingClaimCertificateBinding(
            intent.resulting_epoch_id,
            claim_id,
            intent.resulting_revision,
            None,
            claim_after_digest,
        )
        binding_rows.append(opened)
        persisted_bindings.append(
            _persisted_binding_row(M5PersistedBindingKind.CLAIM, opened)
        )
        output_records.append(("claim_binding", claim_id, opened))
    elif claim_state_changed and not claim_binding.working:
        opened = WorkingClaimCertificateBinding(
            intent.resulting_epoch_id,
            claim_id,
            intent.resulting_revision,
            None,
            claim_after_digest,
        )
        binding_rows.append(opened)
        persisted_bindings.append(
            _persisted_binding_row(M5PersistedBindingKind.CLAIM, opened)
        )
        output_records.append(("claim_binding", claim_id, opened))

    output_records.extend(
        ("status_delta", delta.object_id, delta) for delta in logical.status_deltas
    )
    output_rank = {
        "claim_state": 0,
        "answer_state": 1,
        "claim_certificate": 2,
        "claim_binding": 3,
        "status_delta": 4,
    }
    ordered_output = tuple(
        sorted(
            output_records,
            key=lambda row: (
                output_rank[row[0]],
                row[1],
                getattr(row[2], "valid_from_revision", -1),
            ),
        )
    )
    output_bytes = len(digests.logical_output_preimage(ordered_output))
    matching_work = touched_state_work(
        claims_touched=int(claim_state_changed or claim_certificate_changed),
        answers_touched=int(answer_state_changed),
        claim_status_changes=sum(
            delta.object_type == "claim" for delta in logical.status_deltas
        ),
        answer_status_changes=sum(
            delta.object_type == "answer" for delta in logical.status_deltas
        ),
        output_bytes=output_bytes,
    )
    work = M5OverlayWork(
        matching=matching_work,
        claim_state_only_changes=int(
            claim_state_changed
            and claim_after.state.status is claim_before.state.status
        ),
        claim_certificate_only_changes=int(
            claim_certificate_changed and not claim_state_changed
        ),
        public_status_deltas=len(logical.status_deltas),
    )
    work.assert_nonnegative()
    artifact = _build_patch_artifact(
        intent,
        logical_changes=tuple(
            sorted(logical_changes, key=lambda row: (row.kind.value, row.object_id))
        ),
        binding_rows=tuple(persisted_bindings),
        output_records=ordered_output,
        work=work,
    )
    plan = _MatchingWritePlan(
        claim_state_rows=tuple(claim_rows),
        answer_state_rows=tuple(answer_rows),
        claim_certificate_artifact_rows=tuple(artifact_rows),
        claim_binding_rows=tuple(binding_rows),
    )
    counts = _D24OwnedWriteCounts(
        claim_state_write_count=len(claim_rows),
        answer_state_write_count=len(answer_rows),
        certificate_binding_write_count=len(binding_rows),
        public_delta_write_count=len(logical.status_deltas),
    )
    return artifact, plan, counts, 0


def _project_direct_header_after_images(
    reservation: _DirectMatchingReservation,
) -> tuple[_BaseHeaderAfterImage, _RuntimeHeaderAfterImage]:
    """Purely project the complete N+1 headers from captured N counters."""

    precursor = reservation.precursor
    base_before = reservation.base_header_before_image
    runtime_before = reservation.runtime_header_before_image
    projected_job_count = base_before.open_job_count - 1
    projected_scope_count = base_before.open_scope_count + precursor.scope_delta
    if projected_job_count < 0 or projected_scope_count < 0:
        raise EventConflictError("direct completion counter projection underflowed")
    complete = (
        projected_job_count == 0
        and projected_scope_count == 0
        and runtime_before.open_work_count == 0
        and runtime_before.open_scope_count == 0
        and runtime_before.blocking_failure_count == 0
    )
    semantic_state = "complete" if complete else "pending"
    return (
        _BaseHeaderAfterImage(
            precursor.epoch_id,
            precursor.resulting_revision,
            "committed",
            semantic_state,
            semantic_state,
            base_before.publication_mode,
            False,
            projected_job_count,
            projected_scope_count,
        ),
        _RuntimeHeaderAfterImage(
            precursor.epoch_id,
            precursor.resulting_revision,
            "semantic_complete" if complete else "semantic_pending",
            runtime_before.open_work_count,
            runtime_before.open_scope_count,
            runtime_before.blocking_failure_count,
            False,
        ),
    )


def _direct_header_after_images(
    cursor: Cursor[Any], reservation: _DirectMatchingReservation
) -> tuple[_BaseHeaderAfterImage, _RuntimeHeaderAfterImage]:
    """Project headers from captured pre-stage counters and exact live deltas."""

    precursor = reservation.precursor
    base_before = reservation.base_header_before_image
    runtime_before = reservation.runtime_header_before_image
    projected, projected_runtime = _project_direct_header_after_images(reservation)
    live_base, live_runtime = _header_images(cursor, precursor.epoch_id)
    expected_live_base = _BaseHeaderAfterImage(
        precursor.epoch_id,
        precursor.expected_revision,
        base_before.structural_status,
        base_before.semantic_status,
        base_before.evaluation_state,
        base_before.publication_mode,
        False,
        projected.open_job_count,
        projected.open_scope_count,
    )
    if live_base != expected_live_base or live_runtime != runtime_before:
        raise EventConflictError("direct completion header counters changed")
    return projected, projected_runtime


def _validate_direct_finalizer_authority(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    prepared: _PreparedMatchingTransition,
) -> None:
    """Deeply rebind direct authority after D25 stage and before tier 15i."""

    reservation = prepared.direct_reservation_or_none
    stage_result = prepared.direct_m4_stage_evidence_or_none
    if type(reservation) is not _DirectMatchingReservation or (
        type(stage_result) is not _DirectM4StageResult
    ):
        raise EventConflictError("direct finalizer evidence disappeared")
    _validate_direct_stage_evidence(
        cursor,
        reservation,
        stage_result,
        expected_reservation_phase="consumed",
        expected_stage_phase="consumed",
        matching_stage_installed=True,
    )
    official_projection = _direct_projection_from_logical_plan(
        reservation.precursor, reservation.logical_plan
    )
    if (
        official_projection != reservation.d25_projection
        or _direct_official_intent(reservation.precursor, official_projection) != intent
    ):
        raise EventConflictError("direct official intent changed before finalization")
    official_stage, official_remainder = _direct_remainder_after_stage(
        cursor, reservation.precursor, reservation.logical_plan
    )
    if official_stage != reservation.stage_coordinates or (
        official_remainder != reservation.remainder_coordinates
    ):
        raise EventConflictError(
            "direct official remainder changed before finalization"
        )
    (
        artifact,
        plan,
        d24_counts,
        requirement_count,
    ) = _direct_first_application_plan(intent, reservation)
    projected_headers = _project_direct_header_after_images(reservation)
    if (
        artifact != prepared.prewrite_patch_artifact
        or plan != prepared.physical_and_logical_write_plan
        or artifact.work != prepared.d25_contribution_work
        or d24_counts != prepared.d24_owned_planned_write_counts
        or requirement_count != prepared.requirement_state_write_count_diagnostic
        or projected_headers
        != (
            prepared.base_header_after_image,
            prepared.runtime_header_after_image,
        )
        or prepared.observation_currency_before_images
    ):
        raise EventConflictError("direct prepared plan changed before finalization")
    if prepared.expected_patch_digest is not None and (
        prepared.expected_patch_digest != artifact.patch.patch_digest
    ):
        raise EventConflictError("computed matching patch digest differs")
    if prepared.expected_work is not None and prepared.expected_work != artifact.work:
        raise EventConflictError("computed matching work differs")


def _complete_reserved_direct_matching_transition(
    cursor: Cursor[Any],
    reservation: _DirectMatchingReservation,
    stage_result: _DirectM4StageResult,
    expected_patch_digest: str | None = None,
    expected_work: M5OverlayWork | None = None,
) -> tuple[M5PersistedMatchingTransitionIntent, _PreparedMatchingTransition]:
    """Consume explicit direct evidence and return ready authority without D25 DML."""

    if expected_patch_digest is not None:
        _require_sha256("expected_patch_digest", expected_patch_digest)
    if expected_work is not None and type(expected_work) is not M5OverlayWork:
        raise ValidationError("expected_work must be an exact M5OverlayWork")
    _validate_direct_stage_evidence(
        cursor,
        reservation,
        stage_result,
        expected_reservation_phase="reserved",
        expected_stage_phase="staged",
    )
    official_projection, official_logical = _direct_after_stage_logical_plan(
        cursor, reservation.precursor
    )
    if (
        official_projection != reservation.d25_projection
        or official_logical != reservation.logical_plan
    ):
        raise EventConflictError("direct official projection differs from reservation")
    official_stage, official_remainder = _direct_remainder_after_stage(
        cursor, reservation.precursor, official_logical
    )
    if official_stage != reservation.stage_coordinates or (
        official_remainder != reservation.remainder_coordinates
    ):
        raise EventConflictError("direct official remainder differs from reservation")
    intent = _direct_official_intent(reservation.precursor, official_projection)
    binding = _capture_cursor_binding(cursor, intent)
    if binding != reservation.binding:
        raise EventConflictError("direct official intent changed context binding")
    (
        artifact,
        plan,
        d24_counts,
        requirement_count,
    ) = _direct_first_application_plan(intent, reservation)
    if expected_patch_digest is not None and (
        expected_patch_digest != artifact.patch.patch_digest
    ):
        raise EventConflictError("computed matching patch digest differs")
    if expected_work is not None and expected_work != artifact.work:
        raise EventConflictError("computed matching work differs")
    prewrite_revision, image_base_epoch, image_base_revision = (
        _read_held_first_application_image(cursor, intent)
    )
    base_header, runtime_header = _direct_header_after_images(cursor, reservation)
    stage_before_images = _capture_matching_stage_before_images(cursor, intent, plan)
    prepared = _PreparedMatchingTransition(
        binding=binding,
        prepared_identity=0,
        official_intent=intent,
        prewrite_matching_revision=prewrite_revision,
        matching_image_base_epoch_id=image_base_epoch,
        matching_image_base_revision=image_base_revision,
        prewrite_patch_artifact=artifact,
        physical_and_logical_write_plan=plan,
        stage_before_images=stage_before_images,
        observation_currency_before_images=(),
        base_header_after_image=base_header,
        runtime_header_after_image=runtime_header,
        d25_contribution_work=artifact.work,
        d24_owned_planned_write_counts=d24_counts,
        requirement_state_write_count_diagnostic=requirement_count,
        expected_patch_digest=expected_patch_digest,
        expected_work=expected_work,
        direct_reservation_or_none=reservation,
        direct_m4_stage_evidence_or_none=stage_result,
    )
    prepared.prepared_identity = id(prepared)
    _seal_prepared_authority(prepared, direct_consumed=True)
    object.__setattr__(reservation, "phase", "consumed")
    object.__setattr__(stage_result, "phase", "consumed")
    return intent, prepared


def _matching_contribution_hint(
    cursor: Cursor[Any], intent: M5PersistedMatchingTransitionIntent
) -> tuple[Any, ...] | None:
    """Read one non-authoritative replay locator without taking a 15j lock."""

    counter_columns = sql.SQL(", ").join(
        sql.Identifier(name) for name in MATCHING_WORK_COUNTER_NAMES
    )
    row = cursor.execute(
        sql.SQL(
            """
            SELECT epoch_id, source_kind, source_id, source_identity_hash,
                   before_epoch_id, before_revision, resulting_revision,
                   patch_digest, {}, matching_work_digest, contribution_digest
            FROM groundloop_m5_matching_work_contribution
            WHERE epoch_id = %s AND source_kind = %s AND source_id = %s
            """
        ).format(counter_columns),
        (intent.resulting_epoch_id, intent.source_kind.value, intent.source_id),
    ).fetchone()
    return None if row is None else tuple(row)


def _current_runtime_revision_hint(cursor: Cursor[Any], epoch_id: int) -> int:
    row = cursor.execute(
        "SELECT revision FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s",
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise InvalidEventError("matching replay names an unknown typed epoch")
    return int(row[0])


def _lock_structural_replay_source(
    cursor: Cursor[Any], intent: M5PersistedMatchingTransitionIntent
) -> int:
    """Lock the structural source/predecessor tier before the tier-6 CAS."""

    predecessor = cursor.execute(
        """
        SELECT previous_published_epoch_id
        FROM groundloop_m5_update
        WHERE epoch_id = %s
        """,
        (intent.resulting_epoch_id,),
    ).fetchone()
    if predecessor is None:
        raise InvalidEventError("structural matching replay source is incomplete")
    predecessor_epoch_id = int(predecessor[0])
    rows = cursor.execute(
        """
        SELECT epoch_id
        FROM groundloop_epoch
        WHERE epoch_id IN (%s,%s)
        ORDER BY epoch_id
        FOR UPDATE
        """,
        (predecessor_epoch_id, intent.resulting_epoch_id),
    ).fetchall()
    if tuple(int(row[0]) for row in rows) != tuple(
        sorted((predecessor_epoch_id, intent.resulting_epoch_id))
    ):
        raise InvalidEventError("structural matching replay epoch prefix is incomplete")
    row = cursor.execute(
        """
        SELECT epoch.payload_hash, runtime.structural_event_id,
               runtime.revision, typed_update.previous_published_epoch_id,
               typed_update.decision_policy_version, predecessor.revision,
               predecessor.structural_status, predecessor.semantic_status,
               predecessor.evaluation_state,
               predecessor.sealed_at IS NOT NULL
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        JOIN groundloop_epoch AS predecessor
          ON predecessor.epoch_id = typed_update.previous_published_epoch_id
        WHERE epoch.epoch_id = %s AND epoch.event_id = %s
        """,
        (intent.resulting_epoch_id, intent.source_id),
    ).fetchone()
    if row is None or (
        _sha256_text(row[0]) != intent.source_identity_hash
        or _text(row[1]) != intent.source_id
        or int(row[3]) != intent.before_epoch_id
        or _text(row[4]) != intent.decision_policy_version
        or int(row[5]) != intent.before_revision
        or _text(row[6]) != "committed"
        or _text(row[7]) != "sealed"
        or _text(row[8]) != "complete"
        or not bool(row[9])
    ):
        raise EventConflictError("structural matching replay source changed")
    return int(row[2])


def _lock_requirement_replay_source(
    cursor: Cursor[Any], intent: M5PersistedMatchingTransitionIntent
) -> None:
    """Lock and byte-check the tier-9/10 requirement source closure."""

    job_hint = cursor.execute(
        """
        SELECT logical_job_id
        FROM groundloop_m5_job_attempt
        WHERE attempt_id = %s
        """,
        (intent.source_id,),
    ).fetchone()
    if job_hint is None:
        raise EventConflictError("requirement matching replay attempt disappeared")
    job = cursor.execute(
        """
        SELECT job.logical_job_id, job.epoch_id, job.job_kind, job.job_state,
               job.result_artifact_id, job.result_artifact_hash,
               job.completed_revision, job.payload_hash,
               job.execution_spec_hash, job.semantic_pair_digest,
               job.subject_kind, job.subject_id, job.chunk_version_id
        FROM groundloop_m5_semantic_job AS job
        WHERE job.logical_job_id = %s AND job.epoch_id = %s
        FOR UPDATE
        """,
        (_text(job_hint[0]), intent.resulting_epoch_id),
    ).fetchone()
    if job is None or (
        _text(job[2]) != "verify_requirement_pair"
        or _text(job[3]) != "completed_active"
        or int(job[6]) != intent.resulting_revision
    ):
        raise EventConflictError("requirement matching replay job changed")
    attempt = cursor.execute(
        """
        SELECT logical_job_id, attempt_state, attempt_output_digest,
               execution_spec_hash
        FROM groundloop_m5_job_attempt
        WHERE attempt_id = %s AND logical_job_id = %s
        FOR UPDATE
        """,
        (intent.source_id, _text(job[0])),
    ).fetchone()
    if attempt is None:
        raise EventConflictError("requirement matching replay attempt changed")
    artifact = cursor.execute(
        """
        SELECT attempt_result_artifact_hash, disposition, logical_job_id,
               job_epoch_id, attempt_output_digest, execution_spec_hash,
               payload_hash, job_state_after, result_artifact_id,
               result_artifact_hash
        FROM groundloop_m5_attempt_result_artifact
        WHERE attempt_id = %s AND job_epoch_id = %s
        FOR UPDATE
        """,
        (intent.source_id, intent.resulting_epoch_id),
    ).fetchone()
    if artifact is None:
        raise EventConflictError("requirement matching replay result changed")
    execution = cursor.execute(
        """
        SELECT logical_job_id, attempt_id, artifact_id, artifact_hash,
               pair_input_hash, produced_epoch_id
        FROM groundloop_m5_requirement_verifier_execution
        WHERE logical_job_id = %s AND attempt_id = %s
          AND artifact_id = %s AND artifact_hash = %s
        FOR UPDATE
        """,
        (
            _text(job[0]),
            intent.source_id,
            _text(artifact[8]),
            _sha256_text(artifact[9]),
        ),
    ).fetchone()
    if execution is None:
        raise EventConflictError("requirement matching replay execution changed")
    verifier = cursor.execute(
        """
        SELECT artifact_id, artifact_hash, execution_spec_hash,
               semantic_pair_digest, subject_kind, subject_id,
               chunk_version_id, pair_input_hash
        FROM groundloop_m5_requirement_verifier_artifact
        WHERE artifact_id = %s AND artifact_hash = %s
        FOR UPDATE
        """,
        (_text(execution[2]), _sha256_text(execution[3])),
    ).fetchone()
    if verifier is None or (
        _sha256_text(artifact[0]) != intent.source_identity_hash
        or _text(artifact[1]) != "verifier_completed_active"
        or _text(artifact[2]) != _text(job[0])
        or int(artifact[3]) != intent.resulting_epoch_id
        or _sha256_text(artifact[4]) != _sha256_text(attempt[2])
        or _sha256_text(artifact[5]) != _sha256_text(attempt[3])
        or _sha256_text(artifact[6]) != _sha256_text(job[7])
        or _text(artifact[7]) != _text(job[3])
        or _text(artifact[8]) != _text(job[4])
        or _sha256_text(artifact[9]) != _sha256_text(job[5])
        or _text(attempt[0]) != _text(job[0])
        or _text(attempt[1]) != "completed"
        or _sha256_text(attempt[3]) != _sha256_text(job[8])
        or int(job[6]) != intent.resulting_revision
        or _text(execution[0]) != _text(job[0])
        or _text(execution[1]) != intent.source_id
        or _text(execution[2]) != _text(verifier[0])
        or _sha256_text(execution[3]) != _sha256_text(verifier[1])
        or _sha256_text(execution[4]) != _sha256_text(verifier[7])
        or int(execution[5]) != intent.resulting_epoch_id
        or _sha256_text(verifier[2]) != _sha256_text(job[8])
        or _sha256_text(verifier[3]) != _sha256_text(job[9])
        or _text(verifier[4]) != _text(job[10])
        or _text(verifier[5]) != _text(job[11])
        or _text(verifier[6]) != _text(job[12])
    ):
        raise EventConflictError("requirement matching replay closure changed")


def _lock_direct_replay_source(
    cursor: Cursor[Any], intent: M5PersistedMatchingTransitionIntent
) -> None:
    row = cursor.execute(
        """
        SELECT payload_hash, transition_kind, from_revision, to_revision
        FROM groundloop_m4_evaluation_counter_transition
        WHERE epoch_id = %s AND transition_id = %s
        FOR UPDATE
        """,
        (intent.resulting_epoch_id, intent.source_id),
    ).fetchone()
    if row is None or (
        _sha256_text(row[0]) != intent.source_identity_hash
        or _text(row[1]) != "delta"
        or int(row[2]) != intent.before_revision
        or int(row[3]) != intent.resulting_revision
    ):
        raise EventConflictError("direct matching replay source changed")


def _lock_replay_envelope(
    cursor: Cursor[Any], intent: M5PersistedMatchingTransitionIntent
) -> int:
    """Acquire the frozen source/CAS/image order and return image revision."""

    if intent.source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN:
        scoped_revision = _lock_structural_replay_source(cursor, intent)
    else:
        scoped_revision = _current_runtime_revision_hint(
            cursor, intent.resulting_epoch_id
        )
    _authorize_checked_prefix(
        cursor,
        epoch_id=intent.resulting_epoch_id,
        expected_revision=scoped_revision,
    )
    if intent.source_kind is M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION:
        _lock_requirement_replay_source(cursor, intent)
    retained_image_revision = _matching_work_read_context(
        cursor, intent.resulting_epoch_id, reauthorize=False
    )
    if retained_image_revision < intent.resulting_revision:
        raise EventConflictError("matching replay revision is newer than its image")
    if intent.source_kind is M5PersistedMatchingSourceKind.DIRECT_TRANSITION:
        _lock_direct_replay_source(cursor, intent)
    return retained_image_revision


def _read_matching_transition_replay(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    contribution_hint: tuple[Any, ...],
    *,
    expected_patch_digest: str | None,
    expected_work: M5OverlayWork | None,
) -> M5PersistedMatchingPatchReceipt:
    retained_image_revision = _lock_replay_envelope(cursor, intent)
    candidate_patch_digest = _sha256_text(contribution_hint[7])
    artifact_row = _lock_matching_artifact_key(cursor, candidate_patch_digest)
    if artifact_row is None:
        raise EventConflictError("matching replay lacks its retained patch artifact")
    retained = _decode_retained_matching_artifact(cursor, tuple(artifact_row))
    retained_intent = retained.intent()
    if retained_intent != intent:
        raise EventConflictError(
            "matching replay intent differs from retained changed-key projection"
        )
    contribution_row = _lock_matching_contribution(cursor, intent)
    if contribution_row is None:
        raise EventConflictError("matching replay lost its retained contribution")
    if tuple(contribution_row) != contribution_hint:
        raise EventConflictError("matching replay contribution changed after its hint")
    counter_start = 8
    counter_end = counter_start + len(MATCHING_WORK_COUNTER_NAMES)
    stored_work = _overlay_work_from_values(
        tuple(int(value) for value in contribution_row[counter_start:counter_end])
    )
    stored_work_digest = _sha256_text(contribution_row[counter_end])
    contribution_digest = _sha256_text(contribution_row[counter_end + 1])
    retained_patch = retained.patch
    expected_contribution_digest = digests.matching_work_contribution_digest(
        epoch_id=retained_patch.resulting_epoch_id,
        source_kind=retained_patch.source_kind,
        source_id=retained_patch.source_id,
        source_identity_hash=retained_patch.source_identity_hash,
        before_epoch_id=retained_patch.before_epoch_id,
        before_revision=retained_patch.before_revision,
        resulting_revision=retained_patch.resulting_revision,
        patch_digest=retained_patch.patch_digest,
        matching_work_digest_value=retained_patch.matching_work_digest,
    )
    if (
        int(contribution_row[0]) != intent.resulting_epoch_id
        or _text(contribution_row[1]) != intent.source_kind.value
        or _text(contribution_row[2]) != intent.source_id
        or _sha256_text(contribution_row[3]) != intent.source_identity_hash
        or int(contribution_row[4]) != intent.before_epoch_id
        or int(contribution_row[5]) != intent.before_revision
        or int(contribution_row[6]) != intent.resulting_revision
        or _sha256_text(contribution_row[7]) != retained_patch.patch_digest
        or stored_work_digest != retained_patch.matching_work_digest
        or stored_work_digest
        != digests.matching_work_digest(m5_overlay_work_values(stored_work))
        or contribution_digest != expected_contribution_digest
    ):
        raise EventConflictError(
            "matching replay differs from retained contribution bytes"
        )

    if expected_patch_digest is not None and (
        expected_patch_digest != retained_patch.patch_digest
    ):
        raise EventConflictError("matching replay differs from expected patch digest")
    if expected_work is not None and expected_work != stored_work:
        raise EventConflictError("matching replay differs from expected work")
    accumulated, accumulator_revision = _read_locked_matching_work_accumulator(
        cursor,
        intent.resulting_epoch_id,
        expected_revision=retained_image_revision,
    )
    if accumulator_revision < intent.resulting_revision or any(
        current < historical
        for current, historical in zip(
            m5_overlay_work_values(accumulated),
            m5_overlay_work_values(stored_work),
            strict=True,
        )
    ):
        raise ValidationError("matching accumulator does not cover replay contribution")
    return M5PersistedMatchingPatchReceipt(
        retained_patch,
        contribution_digest,
        accumulated,
        intent.resulting_revision,
        True,
    )


def apply_matching_transition(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    expected_patch_digest: str | None = None,
    expected_work: M5OverlayWork | None = None,
) -> M5PersistedMatchingPatchReceipt:
    """Exactly replay one retained D25 transition without writing any row."""

    if type(intent) is not M5PersistedMatchingTransitionIntent:
        raise ValidationError("matching transition requires the exact intent DTO")
    if expected_patch_digest is not None:
        _require_sha256("expected_patch_digest", expected_patch_digest)
    if expected_work is not None and type(expected_work) is not M5OverlayWork:
        raise ValidationError("expected_work must be an exact M5OverlayWork")
    require_persisted_matching_bundle(cursor)
    contribution_hint = _matching_contribution_hint(cursor, intent)
    if contribution_hint is None:
        raise EventConflictError(
            "matching first application requires explicit prepare/stage/finalize"
        )
    return _read_matching_transition_replay(
        cursor,
        intent,
        contribution_hint,
        expected_patch_digest=expected_patch_digest,
        expected_work=expected_work,
    )


__all__ = [
    "apply_matching_transition",
    "current_matching_work",
    "derive_matching_transition_intent",
    "effective_matching_edge",
    "effective_matching_hall",
    "effective_matching_image",
    "effective_matching_mask",
    "effective_matching_observation",
    "least_effective_observation",
    "representative_effective_hashes",
    "require_persisted_matching_bundle",
    "resolved_matching_edge_point",
    "resolved_matching_hall_point",
    "resolved_matching_mask_point",
    "resolved_matching_observation_point",
]
