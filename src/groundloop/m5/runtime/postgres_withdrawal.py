"""Private PostgreSQL planning for bounded M5 document withdrawal.

This module is deliberately transaction-neutral.  Its callers own the
preview, structural-open, or continuation transaction and pass the cursor
whose snapshot/locks are authority.  No object in this module survives that
call, and none of the helpers starts, commits, or rolls back a transaction.
"""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from enum import StrEnum
from types import UnionType
from typing import Any, TypeVar, cast, get_args, get_origin, get_type_hints

from psycopg import Cursor

from groundloop.ai.contracts import (
    PipelineRunManifest,
    PipelineRunStatus,
    QueryKind,
    ScoreTriple,
)
from groundloop.ai.contracts import (
    VerificationResult as AIVerificationResult,
)
from groundloop.ai.contracts import stable_digest as stable_ai_digest
from groundloop.ai.manifest import manifest_from_dict, manifest_to_dict
from groundloop.domain import (
    ChunkVersion,
    DecisionPolicy,
    DocumentVersion,
    ModelStamp,
    SemanticObservation,
    StatusDelta,
    SubjectKind,
    VerificationLabel,
    normalized_text_hash,
)
from groundloop.errors import EventConflictError, ValidationError
from groundloop.events import (
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
    ReplaceDocumentVersionEvent,
)
from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    OpenEventReceipt,
    PublicationReceipt,
    StructuralWithdrawal,
)
from groundloop.m4.application import (
    DiscoveryResult as M4DiscoveryResult,
)
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    PairKey,
    UpdateKind,
    VectorIndexKind,
    stable_m4_digest,
)
from groundloop.m4.contracts import (
    CandidatePolicyManifest as M4CandidatePolicyManifest,
)
from groundloop.m4.contracts import (
    ChannelHit as M4ChannelHit,
)
from groundloop.m4.contracts import (
    ChildClosure as M4ChildClosure,
)
from groundloop.m4.contracts import (
    DiscoveryScope as M4DiscoveryScope,
)
from groundloop.m4.contracts import (
    FrontierEntry as M4FrontierEntry,
)
from groundloop.m4.contracts import (
    FrontierState as M4FrontierState,
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
from groundloop.m4.models.contracts import (
    PairVerificationArtifact,
    PairVerificationInput,
    decision_policy_hash,
    derive_operational_label,
)
from groundloop.m4.models.ports import verification_artifact_payload_hash
from groundloop.m4.pipeline import InsertedDocument, StructuralPayload
from groundloop.m4.runtime.withdrawal import (
    CandidateDependency,
    ObservationDependency,
    ReverseDependencyIndex,
    WithdrawalPlan,
    plan_withdrawal,
)
from groundloop.m5.digests import normalize_text_v1, normalized_text_hash_v1
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.application import (
    M5DirectOpenPlan,
    M5RequirementRootDeclaration,
    _M5D29HydrationTerminalCutoff,
)
from groundloop.m5.runtime.contracts import (
    M5AttemptArchiveReason,
    M5AttemptDisposition,
    M5AttemptExecutionEvidence,
    M5AttemptOutput,
    M5AttemptResultArtifact,
    M5CandidatePolicyManifest,
    M5ChangedStateReference,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5DispatchRecord,
    M5EventRunResult,
    M5ExecutionEvidenceDisposition,
    M5ExpiredAttemptReturn,
    M5JobAttempt,
    M5JobCompletion,
    M5JobKind,
    M5JobState,
    M5LogicalJobSpec,
    M5ReplayedOutcome,
    M5RequirementAdmissionChannel,
    M5RequirementAdmittedPair,
    M5RequirementAdmittedPairSource,
    M5RequirementChannelHit,
    M5RequirementDiscoveryResult,
    M5RequirementFallbackKey,
    M5RequirementPairInput,
    M5RequirementScopeSelection,
    M5RequirementVerifierArtifact,
    M5RequirementWithdrawalPlan,
    M5RetrievalTermination,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeSubgraph,
    M5RuntimeTiming,
    M5RuntimeTimingCoverage,
    M5RuntimeTimingObservation,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5ScopeState,
    M5StateReferenceKind,
    M5TerminalReason,
    M5TypedDirectLateReturnEnvelope,
    M5TypedDirectReturnKind,
    M5TypedDirectScopeKind,
    M5TypedDirectTerminalProjection,
    M5TypedDirectVerificationExecution,
    M5TypedEventPlan,
    SemanticPairKey,
)
from groundloop.m5.runtime.frontier import (
    M5WithdrawnCandidateEdge,
    M5WithdrawnObservationEdge,
    plan_requirement_withdrawal,
)
from groundloop.m5.runtime.postgres_direct_recovery import _envelope_json_bindings
from groundloop.postgres.migrations import (
    _verify_m5_bounded_document_withdrawal_route_authority,
)

_DOCUMENT_DECLARATION_KEY = "m5_d29_document_declaration_v1"
_DOCUMENT_EVENT_TYPES = (
    InsertDocumentEvent,
    DeleteDocumentVersionEvent,
    ReplaceDocumentVersionEvent,
)
_M4_RUNTIME_MANIFEST_KEY = "_groundloop_m4_runtime_v1"
_ALL_JOB_STATES = (
    "declared",
    "running",
    "completed_active",
    "completed_inactive",
    "retryable_failed",
    "terminal_failed",
    "cancelled",
)
_NONTERMINAL_JOB_STATES = ("declared", "running", "retryable_failed")
_D29_SCOPE_RESERVATION_NAMESPACE = 1_146_242_387  # signed int32 for b"D29S"
_D29_JOB_RESERVATION_NAMESPACE = 1_146_242_378  # signed int32 for b"D29J"
_D29_NORMALIZER_ID = "m5-normalize-text-v1"
_D29_NORMALIZER_PROVENANCE_HASH = (
    "d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb"
)


class _DocumentOpenPhase(StrEnum):
    LOCATORS_GATHERED = "locators_gathered"
    CONSUMED = "consumed"


def _c_key(value: str) -> bytes:
    """Match PostgreSQL UTF-8 ``COLLATE \"C\"`` text order."""

    return value.encode("utf-8")


def _strip(value: object) -> str:
    return str(value).rstrip(" ")


def _exact_text(name: str, value: object) -> str:
    if type(value) is not str or not value.strip():
        raise ValidationError(f"{name} must be exact nonempty text")
    return value


def _exact_digest(name: str, value: object) -> str:
    checked = _exact_text(name, value)
    if len(checked) != 64 or any(
        character not in "0123456789abcdef" for character in checked
    ):
        raise ValidationError(f"{name} must be an exact lowercase SHA-256")
    return checked


def _canonical_texts(name: str, values: Iterable[object]) -> tuple[str, ...]:
    checked = tuple(_exact_text(f"{name} item", value) for value in values)
    canonical = tuple(sorted(set(checked), key=_c_key))
    if checked != canonical:
        raise ValidationError(f"{name} must be C-sorted and unique")
    return checked


def _document_event(event: M5TypedEventPlan) -> M5TypedEventPlan:
    if (
        type(event) is not M5TypedEventPlan
        or type(event.event) not in _DOCUMENT_EVENT_TYPES
    ):
        raise ValidationError("D29 planning requires an exact typed document event")
    if event.direct_plan is None:
        raise ValidationError("D29 document event is missing its direct plan")
    if (
        type(event.structural_event_id) is not str
        or type(event.payload_hash) is not str
        or event.event.event_id != event.structural_event_id
        or event.direct_plan.update.event_id != event.structural_event_id
        or event.direct_plan.update.payload_hash != event.payload_hash
    ):
        raise ValidationError("D29 document event identity is not exact")
    concrete = event.event
    direct = event.direct_plan
    if type(concrete) is InsertDocumentEvent:
        expected_kind = UpdateKind.INSERT
        expected_inserted = tuple(
            sorted((chunk.chunk_version_id for chunk in concrete.chunks), key=_c_key)
        )
        expected_deactivated: tuple[str, ...] | None = ()
    elif type(concrete) is DeleteDocumentVersionEvent:
        expected_kind = UpdateKind.DELETE
        expected_inserted = ()
        expected_deactivated = None
    elif type(concrete) is ReplaceDocumentVersionEvent:
        expected_kind = UpdateKind.REPLACE
        expected_inserted = tuple(
            sorted((chunk.chunk_version_id for chunk in concrete.chunks), key=_c_key)
        )
        expected_deactivated = None
    else:
        raise AssertionError("validated document event changed concrete type")
    if (
        direct.update.update_kind is not expected_kind
        or direct.inserted_chunk_version_ids != expected_inserted
        or (
            expected_deactivated is not None
            and direct.deactivated_chunk_version_ids != expected_deactivated
        )
    ):
        raise ValidationError("D29 direct plan differs from its concrete event")
    return event


def _structural_payload_from_event(event: M5TypedEventPlan) -> StructuralPayload:
    checked = _document_event(event)
    concrete = checked.event
    if type(concrete) is DeleteDocumentVersionEvent:
        return StructuralPayload(
            deactivated_document_version_id=concrete.document_version_id
        )
    if type(concrete) is InsertDocumentEvent:
        version_id = concrete.document_version_id
        document_id = concrete.document_id
        content_hash = concrete.content_hash
        deactivated = None
    elif type(concrete) is ReplaceDocumentVersionEvent:
        version_id = concrete.new_document_version_id
        document_id = concrete.document_id
        content_hash = concrete.content_hash
        deactivated = concrete.old_document_version_id
    else:
        raise AssertionError("validated document event changed concrete type")
    chunks = tuple(
        sorted(
            (
                ChunkVersion(
                    item.chunk_version_id,
                    version_id,
                    item.chunk_index,
                    item.text,
                )
                for item in concrete.chunks
            ),
            key=lambda item: _c_key(item.chunk_version_id),
        )
    )
    return StructuralPayload(
        inserted=InsertedDocument(
            version=DocumentVersion(version_id, document_id, content_hash),
            chunks=chunks,
        ),
        deactivated_document_version_id=deactivated,
    )


def _require_d29_document_route_authority(cursor: Cursor[Any]) -> None:
    _verify_m5_bounded_document_withdrawal_route_authority(cursor)


def _document_declaration_manifest(
    direct_open: M5DirectOpenPlan,
) -> dict[str, object]:
    """Build the exact typed D29 declaration commitment (never a hash input)."""

    if type(direct_open) is not M5DirectOpenPlan:
        raise ValidationError("D29 manifest requires an exact direct-open plan")
    root_ids = _canonical_texts(
        "direct root job IDs", (job.job_id for job in direct_open.root_jobs)
    )
    scope_ids = _canonical_texts(
        "direct scope root job IDs",
        (scope.root_job_id for scope in direct_open.discovery_scopes),
    )
    fallback_values = (
        ()
        if direct_open.withdrawal is None
        else direct_open.withdrawal.fallback_claim_ids
    )
    fallback_ids = _canonical_texts("direct fallback claim IDs", fallback_values)
    return {
        _DOCUMENT_DECLARATION_KEY: {
            "direct_root_job_ids": list(root_ids),
            "direct_scope_root_job_ids": list(scope_ids),
            "direct_fallback_claim_ids": list(fallback_ids),
        }
    }


def _validate_document_manifest(
    value: object,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    if type(value) is not dict or tuple(value) != (_DOCUMENT_DECLARATION_KEY,):
        raise EventConflictError("retained D29 declaration manifest has wrong keys")
    nested = value[_DOCUMENT_DECLARATION_KEY]
    expected_keys = {
        "direct_root_job_ids",
        "direct_scope_root_job_ids",
        "direct_fallback_claim_ids",
    }
    if type(nested) is not dict or set(nested) != expected_keys:
        raise EventConflictError("retained D29 declaration value has wrong keys")
    arrays: list[tuple[str, ...]] = []
    for key in (
        "direct_root_job_ids",
        "direct_scope_root_job_ids",
        "direct_fallback_claim_ids",
    ):
        raw = nested[key]
        if type(raw) is not list:
            raise EventConflictError("retained D29 declaration member is not an array")
        try:
            arrays.append(_canonical_texts(key, raw))
        except ValidationError as error:
            raise EventConflictError(
                "retained D29 declaration member is not typed canonical text"
            ) from error
    return arrays[0], arrays[1], arrays[2]


def _validate_direct_update_manifest(
    value: object,
    event: M5TypedEventPlan,
    *,
    direct_scope_root_job_ids: tuple[str, ...],
    runtime_state: str,
) -> None:
    """Validate the exact typed M4 metadata object without serializing it."""

    if type(value) is not dict or tuple(value) != (_M4_RUNTIME_MANIFEST_KEY,):
        raise EventConflictError("retained direct update manifest has wrong keys")
    runtime = value[_M4_RUNTIME_MANIFEST_KEY]
    expected_keys = {"event_manifest", "scope_claim_ids", "failure_reason"}
    if type(runtime) is not dict or set(runtime) != expected_keys:
        raise EventConflictError("retained direct runtime metadata has wrong keys")
    expected_event_manifest = _structural_payload_from_event(event).manifest
    direct = event.direct_plan
    assert direct is not None
    expected_scopes = {
        scope_id: list(direct.registered_claim_ids)
        for scope_id in direct_scope_root_job_ids
    }
    failure_reason = runtime["failure_reason"]
    if (
        runtime["event_manifest"] != expected_event_manifest
        or runtime["scope_claim_ids"] != expected_scopes
        or (
            runtime_state == "failed"
            and (type(failure_reason) is not str or not failure_reason.strip())
        )
        or (runtime_state != "failed" and failure_reason is not None)
    ):
        raise EventConflictError("retained direct runtime metadata changed")


@dataclass(frozen=True, slots=True)
class _DocumentClosure:
    epoch_id: int
    epoch_revision: int
    update_kind: str
    previous_epoch_id: int
    runtime_revision: int
    runtime_state: str
    open_work_count: int
    open_scope_count: int
    blocking_failure_count: int
    candidate_policy: M5CandidatePolicyManifest
    requirement_snapshot_digest: str
    chunk_snapshot_digest: str
    requirement_root_set_hash: str
    direct_root_job_ids: tuple[str, ...]
    direct_scope_root_job_ids: tuple[str, ...]
    direct_fallback_claim_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _D29DocumentOpenBinding:
    """Identity of the one cursor and outer transaction owning D29 authority."""

    cursor_object_identity: int
    backend_identity: int
    transaction_identity: int
    session_role: str
    epoch_id: int
    structural_event_id: str
    source_identity_hash: str
    predecessor_epoch_id: int
    source_chunks: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _D29DirectClaimBeforeImage:
    """Pre-tier-8 point image for one claim with withdrawn current evidence."""

    claim_id: str
    answer_version_id: str
    required: bool
    support_count: int
    refute_count: int
    best_support_score: float | None
    best_refute_score: float | None
    supporting_observation_ids: tuple[str, ...]
    refuting_observation_ids: tuple[str, ...]
    status: str
    certificate_digest: str
    state_valid_from_epoch: int
    materialized_updated_epoch: int
    materialized_updated_revision: int
    certificate_support_observation_id: str | None
    certificate_refute_observation_id: str | None
    certificate_repaired_epoch: int
    certificate_repaired_revision: int
    withdrawn_observation_ids: tuple[str, ...]
    remaining_support_observation_ids: tuple[str, ...]
    remaining_refute_observation_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _D29DirectAnswerBeforeImage:
    """Pre-tier-8 point image for one affected direct claim's answer."""

    answer_version_id: str
    required_claim_count: int
    supported_count: int
    unsupported_count: int
    refuted_count: int
    conflicted_count: int
    status: str


@dataclass(frozen=True, slots=True)
class _D29DirectMatchingAuthoritySnapshot:
    """Deep immutable self-image of cursor-local direct matching authority."""

    snapshot_identity: int
    binding: _D29DocumentOpenBinding
    authority_identity: int
    claim_before_images: tuple[_D29DirectClaimBeforeImage, ...]
    answer_before_images: tuple[_D29DirectAnswerBeforeImage, ...]
    withdrawn_observations: tuple[SemanticObservation, ...]
    remaining_observations: tuple[SemanticObservation, ...]
    observation_text_hashes: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class _D29DirectMatchingAuthority:
    """Locked D29/D30 direct-state evidence passed only to private D25 code."""

    binding: _D29DocumentOpenBinding
    authority_identity: int
    claim_before_images: tuple[_D29DirectClaimBeforeImage, ...]
    answer_before_images: tuple[_D29DirectAnswerBeforeImage, ...]
    withdrawn_observations: tuple[SemanticObservation, ...]
    remaining_observations: tuple[SemanticObservation, ...]
    observation_text_hashes: tuple[tuple[str, str], ...]
    authority_snapshot: _D29DirectMatchingAuthoritySnapshot | None = None


def _load_candidate_policy(row: tuple[object, ...]) -> M5CandidatePolicyManifest:
    return M5CandidatePolicyManifest(
        candidate_policy_id=_strip(row[0]),
        embedding_model_artifact_id=str(row[2]),
        requirement_role_template_hash=_strip(row[3]),
        chunk_role_template_hash=_strip(row[4]),
        vector_method_version=str(row[5]),
        vector_index_kind=VectorIndexKind(str(row[6])),
        vector_index_build_config_hash=_strip(row[7]),
        vector_search_config_hash=_strip(row[8]),
        lexical_method_version=str(row[9]),
        lexical_config_hash=_strip(row[10]),
        lexical_postgres_version=str(row[11]),
        lexical_regconfig_identity=str(row[12]),
        fusion_version=str(row[13]),
        reverse_budget_per_inserted_chunk=int(str(row[14])),
        forward_budget_per_requirement=int(str(row[15])),
        verifier_execution_spec_hash=_strip(row[16]),
        decision_policy_version=str(row[17]),
        lineage_safety_override=bool(row[18]),
    )


def _load_direct_candidate_policy(
    cursor: Cursor[Any], candidate_policy_id: str
) -> M4CandidatePolicyManifest:
    """Validate every immutable M4 policy column against its JSON manifest."""

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
        """,
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
    if set(payload) != expected_keys:
        raise EventConflictError("direct candidate-policy manifest keys changed")
    integer_fields = (
        "claim_count",
        "approximate_cap_per_inserted_chunk",
        "frontier_depth",
    )
    if (
        any(type(payload[field]) is not int for field in integer_fields)
        or type(payload["lineage_safety_override"]) is not bool
    ):
        raise EventConflictError("direct candidate-policy manifest types changed")
    try:
        policy = M4CandidatePolicyManifest(
            policy_id=_exact_text("direct policy ID", payload["policy_id"]),
            policy_hash=_exact_digest("direct policy hash", payload["policy_hash"]),
            embedding_model_artifact_id=_exact_text(
                "direct embedding artifact", payload["embedding_model_artifact_id"]
            ),
            claim_role_template_hash=_exact_digest(
                "direct claim template", payload["claim_role_template_hash"]
            ),
            chunk_role_template_hash=_exact_digest(
                "direct chunk template", payload["chunk_role_template_hash"]
            ),
            vector_method_version=_exact_text(
                "direct vector method", payload["vector_method_version"]
            ),
            vector_index_kind=VectorIndexKind(
                _exact_text("direct vector index", payload["vector_index_kind"])
            ),
            vector_index_build_config_hash=_exact_digest(
                "direct vector build config",
                payload["vector_index_build_config_hash"],
            ),
            vector_search_config_hash=_exact_digest(
                "direct vector search config", payload["vector_search_config_hash"]
            ),
            lexical_method_version=_exact_text(
                "direct lexical method", payload["lexical_method_version"]
            ),
            lexical_config_hash=_exact_digest(
                "direct lexical config", payload["lexical_config_hash"]
            ),
            lexical_postgres_version=_exact_text(
                "direct PostgreSQL version", payload["lexical_postgres_version"]
            ),
            lexical_regconfig_identity=_exact_text(
                "direct regconfig", payload["lexical_regconfig_identity"]
            ),
            claim_registry_snapshot_id=_exact_text(
                "direct registry", payload["claim_registry_snapshot_id"]
            ),
            claim_count=int(payload["claim_count"]),
            fusion_version=_exact_text(
                "direct fusion version", payload["fusion_version"]
            ),
            approximate_cap_per_inserted_chunk=int(
                payload["approximate_cap_per_inserted_chunk"]
            ),
            frontier_depth=int(payload["frontier_depth"]),
            verifier_execution_spec_hash=_exact_digest(
                "direct verifier execution spec",
                payload["verifier_execution_spec_hash"],
            ),
            decision_policy_version=_exact_text(
                "direct decision policy", payload["decision_policy_version"]
            ),
            lineage_safety_override=payload["lineage_safety_override"],
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError(
            "direct candidate-policy manifest is invalid"
        ) from error
    relational_identity = (
        str(row[0]),
        _strip(row[1]),
        str(row[2]),
        str(row[3]),
        _strip(row[4]),
        _strip(row[5]),
        str(row[6]),
        str(row[7]),
        _strip(row[8]),
        _strip(row[9]),
        str(row[10]),
        _strip(row[11]),
        str(row[12]),
        str(row[13]),
        str(row[14]),
        int(str(row[15])),
        str(row[16]),
        int(str(row[17])),
        int(str(row[18])),
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


def _read_existing_document_closure(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    *,
    allow_missing_event_snapshots: bool = False,
) -> _DocumentClosure | None:
    """Read only event-local closure; never reconstruct the legacy payload."""

    if type(allow_missing_event_snapshots) is not bool:
        raise ValidationError("snapshot-read mode must be an exact boolean")
    checked = _document_event(event)
    row = cursor.execute(
        """
        SELECT epoch.epoch_id, epoch.payload_hash, epoch.revision,
               epoch.structural_status, epoch.semantic_status,
               epoch.evaluation_state,
               typed_update.update_kind,
               typed_update.previous_published_epoch_id,
               typed_update.decision_policy_version,
               typed_update.manifest,
               direct_update.update_kind,
               direct_update.previous_published_epoch_id,
               direct_update.candidate_policy_id,
               direct_update.registry_snapshot_id,
               runtime.structural_event_id,
               runtime.candidate_policy_id,
               runtime.candidate_policy_manifest_hash,
               runtime.requirement_registry_snapshot_digest,
               runtime.active_chunk_snapshot_digest,
               runtime.expected_previous_published_epoch_id,
               runtime.requirement_root_set_hash,
               runtime.runtime_state, runtime.revision,
               runtime.open_work_count, runtime.open_scope_count,
               runtime.blocking_failure_count,
               policy.candidate_policy_id,
               policy.candidate_policy_manifest_hash,
               policy.embedding_model_artifact_id,
               policy.requirement_role_template_hash,
               policy.chunk_role_template_hash,
               policy.vector_method_version, policy.vector_index_kind,
               policy.vector_index_build_config_hash,
               policy.vector_search_config_hash,
               policy.lexical_method_version, policy.lexical_config_hash,
               policy.lexical_postgres_version,
               policy.lexical_regconfig_identity, policy.fusion_version,
               policy.reverse_budget_per_inserted_chunk,
               policy.forward_budget_per_requirement,
               policy.verifier_execution_spec_hash,
               policy.decision_policy_version,
               policy.lineage_safety_override,
               direct_policy.decision_policy_version,
               registry.requirement_count, chunks.chunk_count,
               epoch.publication_mode, epoch.sealed_at,
               runtime.terminal_at, result.outcome, direct_update.manifest
        FROM groundloop_epoch AS epoch
        LEFT JOIN groundloop_m5_update AS typed_update
          ON typed_update.epoch_id = epoch.epoch_id
        LEFT JOIN groundloop_m4_update AS direct_update
          ON direct_update.epoch_id = epoch.epoch_id
        LEFT JOIN groundloop_m5_runtime_epoch AS runtime
          ON runtime.epoch_id = epoch.epoch_id
        LEFT JOIN groundloop_m5_candidate_policy AS policy
          ON policy.candidate_policy_id = runtime.candidate_policy_id
        LEFT JOIN groundloop_candidate_policy AS direct_policy
          ON direct_policy.candidate_policy_id = direct_update.candidate_policy_id
        LEFT JOIN groundloop_m5_requirement_registry_snapshot AS registry
          ON registry.requirement_registry_snapshot_digest =
             runtime.requirement_registry_snapshot_digest
        LEFT JOIN groundloop_m5_active_chunk_snapshot AS chunks
          ON chunks.active_chunk_snapshot_digest =
             runtime.active_chunk_snapshot_digest
        LEFT JOIN groundloop_m5_event_result AS result
          ON result.epoch_id = epoch.epoch_id
        WHERE epoch.event_id = %s
        """,
        (checked.structural_event_id,),
    ).fetchone()
    if row is None:
        return None
    if _strip(row[1]) != checked.payload_hash:
        raise EventConflictError(
            "document event ID is already bound to another locked payload hash"
        )
    # LEFT JOINs are intentional: every sidecar except the two content-addressed
    # snapshot headers must already exist.  The private D29 prepare phase alone
    # runs immediately before C1's tier-8 snapshot step, so it may tolerate
    # either header being absent.  Every retained caller remains strict.
    if any(row[index] is None for index in range(6, 46)) or (
        not allow_missing_event_snapshots
        and any(row[index] is None for index in (46, 47))
    ):
        raise EventConflictError("existing document event lacks its typed closure")
    expected_kinds = {
        InsertDocumentEvent: ("document_insert", "insert"),
        DeleteDocumentVersionEvent: ("document_delete", "delete"),
        ReplaceDocumentVersionEvent: ("document_replace", "replace"),
    }
    typed_kind, direct_kind = expected_kinds[type(checked.event)]
    policy = _load_candidate_policy(tuple(row[26:45]))
    direct_policy = _load_direct_candidate_policy(cursor, _strip(row[12]))
    manifest_roots, manifest_scopes, manifest_fallback = _validate_document_manifest(
        row[9]
    )
    direct = checked.direct_plan
    assert direct is not None
    expected_registry_count = len(checked.requirement_registry_snapshot.entries)
    expected_chunk_count = len(checked.active_chunk_snapshot.entries)
    registry_count_matches = row[46] is not None and (
        int(row[46]) == expected_registry_count
    )
    chunk_count_matches = row[47] is not None and (int(row[47]) == expected_chunk_count)
    if (
        row[3] not in {"committed", "failed"}
        or row[6] != typed_kind
        or row[10] != direct_kind
        or int(row[7]) != checked.expected_previous_published_epoch_id
        or int(row[11]) != checked.expected_previous_published_epoch_id
        or int(row[19]) != checked.expected_previous_published_epoch_id
        or str(row[8]) != policy.decision_policy_version
        or _strip(row[12]) != checked.candidate_policy_id
        or str(row[13]) != direct.claim_registry_snapshot_id
        or str(row[14]) != checked.structural_event_id
        or _strip(row[15]) != checked.candidate_policy_id
        or _strip(row[16]) != checked.candidate_policy_manifest_hash
        or policy.candidate_policy_id != checked.candidate_policy_id
        or _strip(row[27]) != policy.manifest_hash
        or policy.manifest_hash != checked.candidate_policy_manifest_hash
        or str(row[45]) != policy.decision_policy_version
        or direct_policy.policy_id != checked.candidate_policy_id
        or direct_policy.decision_policy_version != policy.decision_policy_version
        or direct_policy.verifier_execution_spec_hash
        != policy.verifier_execution_spec_hash
        or direct_policy.claim_registry_snapshot_id != direct.claim_registry_snapshot_id
        or _strip(row[17])
        != checked.requirement_registry_snapshot.requirement_registry_snapshot_digest
        or _strip(row[18]) != checked.active_chunk_snapshot.active_chunk_snapshot_digest
        or (
            not registry_count_matches
            and not (allow_missing_event_snapshots and row[46] is None)
        )
        or (
            not chunk_count_matches
            and not (allow_missing_event_snapshots and row[47] is None)
        )
    ):
        raise EventConflictError("existing document typed closure is inconsistent")
    runtime_state = str(row[21])
    _validate_direct_update_manifest(
        row[52],
        checked,
        direct_scope_root_job_ids=manifest_scopes,
        runtime_state=runtime_state,
    )
    epoch_state = (
        str(row[3]),
        str(row[4]),
        str(row[5]),
        str(row[48]),
        row[49] is not None,
    )
    expected_epoch_state = {
        "structural_committed": (
            "committed",
            "pending",
            "pending",
            "provisional",
            False,
        ),
        "semantic_pending": (
            "committed",
            "pending",
            "pending",
            "provisional",
            False,
        ),
        "semantic_complete": (
            "committed",
            "complete",
            "complete",
            "provisional",
            False,
        ),
        "sealed": ("committed", "sealed", "complete", "strict", True),
        "failed": ("failed", "failed", "failed", "provisional", False),
    }.get(runtime_state)
    if (
        type(row[2]) is not int
        or type(row[22]) is not int
        or int(row[2]) != int(row[22])
        or int(row[22]) < 1
        or expected_epoch_state is None
        or epoch_state != expected_epoch_state
        or (runtime_state in {"sealed", "failed"}) != (row[50] is not None)
        or (runtime_state in {"sealed", "failed"}) != (row[51] is not None)
        or (runtime_state == "sealed" and tuple(row[23:26]) != (0, 0, 0))
        or any(
            type(row[index]) is not int or int(row[index]) < 0 for index in (23, 24, 25)
        )
    ):
        raise EventConflictError("existing document runtime header is malformed")
    return _DocumentClosure(
        epoch_id=int(row[0]),
        epoch_revision=int(row[2]),
        update_kind=typed_kind,
        previous_epoch_id=int(row[7]),
        runtime_revision=int(row[22]),
        runtime_state=runtime_state,
        open_work_count=int(row[23]),
        open_scope_count=int(row[24]),
        blocking_failure_count=int(row[25]),
        candidate_policy=policy,
        requirement_snapshot_digest=_strip(row[17]),
        chunk_snapshot_digest=_strip(row[18]),
        requirement_root_set_hash=_strip(row[20]),
        direct_root_job_ids=manifest_roots,
        direct_scope_root_job_ids=manifest_scopes,
        direct_fallback_claim_ids=manifest_fallback,
    )


def _validate_d29_event_snapshot_image(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    closure: _DocumentClosure,
) -> None:
    """Validate both exact new-event snapshot images before tier-8 locking."""

    requirement = event.requirement_registry_snapshot
    requirement_header = cursor.execute(
        """
        SELECT requirement_registry_snapshot_digest, requirement_count,
               created_epoch_id, created_at
        FROM groundloop_m5_requirement_registry_snapshot
        WHERE requirement_registry_snapshot_digest = %s
        """,
        (requirement.requirement_registry_snapshot_digest,),
    ).fetchone()
    if (
        requirement_header is None
        or _strip(requirement_header[0])
        != requirement.requirement_registry_snapshot_digest
        or type(requirement_header[1]) is not int
        or int(requirement_header[1]) != requirement.requirement_count
        or type(requirement_header[2]) is not int
        or not 0 < int(requirement_header[2]) <= closure.epoch_id
        or not isinstance(requirement_header[3], datetime)
    ):
        raise EventConflictError("D29 requirement snapshot header changed")
    requirement_rows = tuple(
        (
            int(row[0]),
            str(row[1]),
            str(row[2]),
            str(row[3]),
            str(row[4]),
            str(row[5]),
            _strip(row[6]),
        )
        for row in cursor.execute(
            """
            SELECT member_ordinal, requirement_version_id, group_version_id,
                   group_family_id, owner_claim_id,
                   normalized_requirement_text, requirement_text_hash
            FROM groundloop_m5_requirement_registry_snapshot_member
            WHERE requirement_registry_snapshot_digest = %s
            ORDER BY member_ordinal
            """,
            (requirement.requirement_registry_snapshot_digest,),
        ).fetchall()
    )
    expected_requirement_rows = tuple(
        (
            ordinal,
            entry.requirement_version_id,
            entry.group_version_id,
            entry.group_family_id,
            entry.owner_claim_id,
            entry.normalized_requirement_text,
            entry.requirement_text_hash,
        )
        for ordinal, entry in enumerate(requirement.entries)
    )
    if requirement_rows != expected_requirement_rows:
        raise EventConflictError("D29 requirement snapshot members changed")

    chunks = event.active_chunk_snapshot
    chunk_header = cursor.execute(
        """
        SELECT active_chunk_snapshot_digest, chunk_count, created_epoch_id,
               normalizer_id, normalizer_provenance_hash, created_at
        FROM groundloop_m5_active_chunk_snapshot
        WHERE active_chunk_snapshot_digest = %s
        """,
        (chunks.active_chunk_snapshot_digest,),
    ).fetchone()
    if (
        chunk_header is None
        or _strip(chunk_header[0]) != chunks.active_chunk_snapshot_digest
        or type(chunk_header[1]) is not int
        or int(chunk_header[1]) != chunks.chunk_count
        or type(chunk_header[2]) is not int
        or not 0 < int(chunk_header[2]) <= closure.epoch_id
        or str(chunk_header[3]) != _D29_NORMALIZER_ID
        or _strip(chunk_header[4]) != _D29_NORMALIZER_PROVENANCE_HASH
        or not isinstance(chunk_header[5], datetime)
    ):
        raise EventConflictError("D29 active-chunk snapshot header changed")
    chunk_rows = tuple(
        (int(row[0]), str(row[1]), _strip(row[2]))
        for row in cursor.execute(
            """
            SELECT member_ordinal, chunk_version_id, text_hash
            FROM groundloop_m5_active_chunk_snapshot_member
            WHERE active_chunk_snapshot_digest = %s
            ORDER BY member_ordinal
            """,
            (chunks.active_chunk_snapshot_digest,),
        ).fetchall()
    )
    expected_chunk_rows = tuple(
        (ordinal, entry.chunk_version_id, entry.text_hash)
        for ordinal, entry in enumerate(chunks.entries)
    )
    if chunk_rows != expected_chunk_rows:
        raise EventConflictError("D29 active-chunk snapshot members changed")


def _raise_if_terminal(
    cursor: Cursor[Any], event: M5TypedEventPlan, closure: _DocumentClosure
) -> None:
    result = _load_canonical_terminal_result(
        cursor,
        structural_event_id=event.structural_event_id,
        payload_hash=event.payload_hash,
    )
    if result is None:
        return
    if result.epoch_id != closure.epoch_id:
        raise EventConflictError("document terminal result identity is inconsistent")
    raise _M5D29HydrationTerminalCutoff()


def _optional_int(value: object) -> int | None:
    return None if value is None else int(str(value))


def _load_terminal_work(
    cursor: Cursor[Any],
    *,
    structural_event_id: str,
    epoch_id: int,
    work_kind: str,
) -> M5RuntimeWork | None:
    counter_names = M5RuntimeWork.counter_names()
    columns = ", ".join(counter_names)
    row = cursor.execute(
        f"""
        SELECT epoch_id, work_digest, {columns}
        FROM groundloop_m5_runtime_work
        WHERE structural_event_id = %s AND work_kind = %s
        """,
        (structural_event_id, work_kind),
    ).fetchone()
    if row is None:
        return None
    if int(row[0]) != epoch_id:
        raise EventConflictError("terminal work belongs to another epoch")
    counters = {
        name: int(value) for name, value in zip(counter_names, row[2:], strict=True)
    }
    return M5RuntimeWork(**counters, work_digest=_strip(row[1]))


def _load_canonical_terminal_result(
    cursor: Cursor[Any], *, structural_event_id: str, payload_hash: str
) -> M5EventRunResult | None:
    """Cursor-local C5 reader used only after declaration validation."""

    row = cursor.execute(
        """
        SELECT base.epoch_id, base.payload_hash,
               runtime.structural_event_id,
               result.outcome,
               result.original_open_receipt_binding_hash,
               result.publication_id,
               result.original_publication_receipt_binding_hash,
               result.event_work_digest,
               result.combined_status_delta_set_hash,
               result.changed_state_set_hash,
               result.failure_reason,
               result.logical_result_hash,
               result.delta_count,
               result.state_reference_count,
               result.coordinator_non_db_non_neural_ns,
               result.neural_wall_ns,
               result.postgres_roundtrip_wall_ns,
               result.external_io_wall_ns,
               result.end_to_end_wall_ns,
               result.postgres_server_execution_ns,
               result.postgres_lock_wait_ns,
               result.postgres_wal_bytes,
               result.postgres_shared_block_reads
        FROM groundloop_epoch AS base
        LEFT JOIN groundloop_m5_runtime_epoch AS runtime
          ON runtime.epoch_id = base.epoch_id
        LEFT JOIN groundloop_m5_event_result AS result
          ON result.epoch_id = base.epoch_id
        WHERE base.event_id = %s
        """,
        (structural_event_id,),
    ).fetchone()
    if row is None:
        return None
    if _strip(row[1]) != payload_hash:
        raise EventConflictError("terminal event payload identity changed")
    if row[2] is None or str(row[2]) != structural_event_id:
        raise EventConflictError("terminal event lacks its typed runtime binding")
    if row[3] is None:
        return None
    epoch_id = int(row[0])
    try:
        outcome = M5ReplayedOutcome(str(row[3]))
        failure_reason = None if row[10] is None else M5RunFailureReason(str(row[10]))
    except ValueError as error:
        raise EventConflictError("terminal event outcome is malformed") from error
    event_work = _load_terminal_work(
        cursor,
        structural_event_id=structural_event_id,
        epoch_id=epoch_id,
        work_kind="event",
    )
    call_work = _load_terminal_work(
        cursor,
        structural_event_id=structural_event_id,
        epoch_id=epoch_id,
        work_kind="call",
    )
    if event_work is None or call_work is None:
        raise EventConflictError("terminal event lacks its durable work rows")
    delta_rows = cursor.execute(
        """
        SELECT delta_ordinal, object_type, object_id,
               old_status, new_status, reason
        FROM groundloop_m5_event_result_delta
        WHERE structural_event_id = %s
        ORDER BY delta_ordinal
        """,
        (structural_event_id,),
    ).fetchall()
    if tuple(int(item[0]) for item in delta_rows) != tuple(range(len(delta_rows))):
        raise EventConflictError("terminal event delta ordinals are not dense")
    deltas = tuple(
        StatusDelta(
            event_id=structural_event_id,
            object_type=str(item[1]),
            object_id=str(item[2]),
            old_status=str(item[3]),
            new_status=str(item[4]),
            reason=str(item[5]),
        )
        for item in delta_rows
    )
    reference_rows = cursor.execute(
        """
        SELECT reference_ordinal, kind, object_id, epoch_id, revision,
               state_artifact_hash, reference_digest
        FROM groundloop_m5_event_result_state_reference
        WHERE structural_event_id = %s
        ORDER BY reference_ordinal
        """,
        (structural_event_id,),
    ).fetchall()
    if tuple(int(item[0]) for item in reference_rows) != tuple(
        range(len(reference_rows))
    ):
        raise EventConflictError("terminal event reference ordinals are not dense")
    try:
        references = tuple(
            M5ChangedStateReference(
                kind=M5StateReferenceKind(str(item[1])),
                object_id=str(item[2]),
                epoch_id=int(item[3]),
                revision=int(item[4]),
                state_artifact_hash=_strip(item[5]),
                reference_digest=_strip(item[6]),
            )
            for item in reference_rows
        )
    except (ValueError, ValidationError) as error:
        raise EventConflictError("terminal state reference is malformed") from error
    if (
        len(deltas) != int(row[12])
        or len(references) != int(row[13])
        or event_work.work_digest != _strip(row[7])
        or digests.combined_status_delta_set_digest(deltas) != _strip(row[8])
        or digests.changed_state_set_digest(
            reference.reference_digest for reference in references
        )
        != _strip(row[9])
    ):
        raise EventConflictError("terminal event child or work digest is invalid")
    expected_open_binding = digests.open_event_receipt_binding_digest(
        epoch_id=epoch_id,
        replayed=False,
        already_sealed=False,
        publication_id=None,
        already_failed=False,
        failure_reason=None,
    )
    if expected_open_binding != _strip(row[4]):
        raise EventConflictError("terminal event open binding is invalid")
    publication: PublicationReceipt | None = None
    if outcome is M5ReplayedOutcome.SEALED:
        if row[5] is None or row[6] is None or failure_reason is not None:
            raise EventConflictError("sealed terminal event has malformed shape")
        publication_id = str(row[5])
        if digests.publication_receipt_binding_digest(
            epoch_id=epoch_id,
            publication_id=publication_id,
            replayed=False,
        ) != _strip(row[6]):
            raise EventConflictError("terminal publication binding is invalid")
        open_receipt = OpenEventReceipt(
            epoch_id=epoch_id,
            replayed=True,
            already_sealed=True,
            publication_id=publication_id,
        )
        publication = PublicationReceipt(
            epoch_id=epoch_id,
            publication_id=publication_id,
            replayed=True,
        )
    else:
        if row[5] is not None or row[6] is not None or failure_reason is None:
            raise EventConflictError("failed terminal event has malformed shape")
        open_receipt = OpenEventReceipt(
            epoch_id=epoch_id,
            replayed=True,
            already_sealed=False,
            already_failed=True,
            failure_reason=failure_reason.value,
        )
    timing = M5RuntimeTiming(
        coordinator_non_db_non_neural_ns=int(row[14]),
        neural_wall_ns=int(row[15]),
        postgres_roundtrip_wall_ns=int(row[16]),
        external_io_wall_ns=int(row[17]),
        end_to_end_wall_ns=int(row[18]),
        postgres_server_execution_ns=_optional_int(row[19]),
        postgres_lock_wait_ns=_optional_int(row[20]),
        postgres_wal_bytes=_optional_int(row[21]),
        postgres_shared_block_reads=_optional_int(row[22]),
    )
    coverage_row = cursor.execute(
        """
        SELECT required_expected_count, required_observed_count,
               required_missing_count,
               postgres_server_execution_expected_count,
               postgres_server_execution_observed_count,
               postgres_server_execution_missing_count,
               postgres_lock_wait_expected_count,
               postgres_lock_wait_observed_count,
               postgres_lock_wait_missing_count,
               postgres_wal_bytes_expected_count,
               postgres_wal_bytes_observed_count,
               postgres_wal_bytes_missing_count,
               postgres_shared_block_reads_expected_count,
               postgres_shared_block_reads_observed_count,
               postgres_shared_block_reads_missing_count,
               terminal_client_roundtrip_included
        FROM groundloop_m5_event_timing_coverage
        WHERE structural_event_id = %s AND epoch_id = %s
        """,
        (structural_event_id, epoch_id),
    ).fetchone()
    if coverage_row is None:
        raise EventConflictError("terminal event lacks timing coverage")
    coverage_values = tuple(coverage_row)
    coverage = M5RuntimeTimingCoverage(
        required_expected_count=int(coverage_values[0]),
        required_observed_count=int(coverage_values[1]),
        required_missing_count=int(coverage_values[2]),
        postgres_server_execution_expected_count=int(coverage_values[3]),
        postgres_server_execution_observed_count=int(coverage_values[4]),
        postgres_server_execution_missing_count=int(coverage_values[5]),
        postgres_lock_wait_expected_count=int(coverage_values[6]),
        postgres_lock_wait_observed_count=int(coverage_values[7]),
        postgres_lock_wait_missing_count=int(coverage_values[8]),
        postgres_wal_bytes_expected_count=int(coverage_values[9]),
        postgres_wal_bytes_observed_count=int(coverage_values[10]),
        postgres_wal_bytes_missing_count=int(coverage_values[11]),
        postgres_shared_block_reads_expected_count=int(coverage_values[12]),
        postgres_shared_block_reads_observed_count=int(coverage_values[13]),
        postgres_shared_block_reads_missing_count=int(coverage_values[14]),
        terminal_client_roundtrip_included=bool(coverage_values[15]),
    )
    coverage.validate_aggregate(timing)
    result = M5EventRunResult.build(
        event_id=structural_event_id,
        payload_hash=payload_hash,
        epoch_id=epoch_id,
        state=M5RunState.REPLAYED,
        replayed_outcome=outcome,
        open_receipt=open_receipt,
        publication_receipt=publication,
        event_work=event_work,
        call_work=M5RuntimeWork(),
        event_timing=timing,
        call_timing=M5RuntimeTiming(),
        combined_deltas=deltas,
        changed_state_references=references,
        failure_reason=failure_reason,
        event_timing_coverage=coverage,
        call_timing_coverage=M5RuntimeTimingCoverage.single_point(
            None, terminal_client_roundtrip_included=False
        ),
    )
    if result.logical_result_hash != _strip(row[11]):
        raise EventConflictError("terminal event logical hash is invalid")
    return result


def _policy_by_id(
    cursor: Cursor[Any], candidate_policy_id: str
) -> M5CandidatePolicyManifest:
    row = cursor.execute(
        """
        SELECT candidate_policy_id, candidate_policy_manifest_hash,
               embedding_model_artifact_id, requirement_role_template_hash,
               chunk_role_template_hash, vector_method_version,
               vector_index_kind, vector_index_build_config_hash,
               vector_search_config_hash, lexical_method_version,
               lexical_config_hash, lexical_postgres_version,
               lexical_regconfig_identity, fusion_version,
               reverse_budget_per_inserted_chunk,
               forward_budget_per_requirement, verifier_execution_spec_hash,
               decision_policy_version, lineage_safety_override
        FROM groundloop_m5_candidate_policy
        WHERE candidate_policy_id = %s
        """,
        (candidate_policy_id,),
    ).fetchone()
    if row is None:
        raise EventConflictError("document event candidate policy is missing")
    policy = _load_candidate_policy(tuple(row))
    if _strip(row[1]) != policy.manifest_hash:
        raise EventConflictError("document event candidate policy hash changed")
    return policy


def _predecessor_snapshot_ids(
    cursor: Cursor[Any], predecessor_epoch_id: int
) -> tuple[str, str] | None:
    head = cursor.execute(
        """
        SELECT m4_head.epoch_id, m5_head.epoch_id, m5_head.sealed_revision,
               epoch.revision, epoch.structural_status,
               epoch.semantic_status, epoch.evaluation_state,
               epoch.publication_mode, epoch.sealed_at,
               epoch.event_id, epoch.payload_hash
        FROM groundloop_m4_publication_head AS m4_head
        JOIN groundloop_m5_publication_head AS m5_head
          ON m5_head.singleton
        JOIN groundloop_epoch AS epoch
          ON epoch.epoch_id = m5_head.epoch_id
        WHERE m4_head.singleton
        """
    ).fetchone()
    if (
        head is None
        or int(str(head[0])) != predecessor_epoch_id
        or int(str(head[1])) != predecessor_epoch_id
        or type(head[2]) is not int
        or type(head[3]) is not int
        or int(head[2]) != int(head[3])
        or tuple(head[4:8]) != ("committed", "sealed", "complete", "strict")
        or head[8] is None
    ):
        raise EventConflictError("document predecessor head authority changed")
    predecessor_revision = int(head[3])
    try:
        predecessor_event_id = _exact_text("predecessor event", head[9])
        predecessor_payload_hash = _exact_digest("predecessor payload", head[10])
    except ValidationError as error:
        raise EventConflictError(
            "document predecessor identity is malformed"
        ) from error
    row = cursor.execute(
        """
        SELECT structural_event_id, requirement_registry_snapshot_digest,
               active_chunk_snapshot_digest, runtime_state,
               open_work_count, open_scope_count, blocking_failure_count,
               revision, terminal_at
        FROM groundloop_m5_runtime_epoch
        WHERE epoch_id = %s
        """,
        (predecessor_epoch_id,),
    ).fetchone()
    if row is not None:
        if (
            str(row[0]) != predecessor_event_id
            or row[3] != "sealed"
            or tuple(int(value) for value in row[4:7]) != (0, 0, 0)
            or type(row[7]) is not int
            or int(row[7]) != predecessor_revision
            or row[8] is None
        ):
            raise EventConflictError("document predecessor is not terminal and idle")
        result = _load_canonical_terminal_result(
            cursor,
            structural_event_id=predecessor_event_id,
            payload_hash=predecessor_payload_hash,
        )
        if (
            result is None
            or result.epoch_id != predecessor_epoch_id
            or result.replayed_outcome is not M5ReplayedOutcome.SEALED
        ):
            raise EventConflictError(
                "document predecessor canonical sealed result changed"
            )
        registry_digest = _strip(row[1])
        chunk_digest = _strip(row[2])
        registry = cursor.execute(
            """
            SELECT requirement_registry_snapshot_digest,
                   requirement_count, created_epoch_id, created_at
            FROM groundloop_m5_requirement_registry_snapshot
            WHERE requirement_registry_snapshot_digest = %s
            """,
            (registry_digest,),
        ).fetchone()
        chunks = cursor.execute(
            """
            SELECT active_chunk_snapshot_digest, chunk_count,
                   created_epoch_id, normalizer_id,
                   normalizer_provenance_hash, created_at
            FROM groundloop_m5_active_chunk_snapshot
            WHERE active_chunk_snapshot_digest = %s
            """,
            (chunk_digest,),
        ).fetchone()
        if (
            registry is None
            or _strip(registry[0]) != registry_digest
            or type(registry[1]) is not int
            or int(registry[1]) < 0
            or type(registry[2]) is not int
            or not 1 <= int(registry[2]) <= predecessor_epoch_id
            or registry[3] is None
            or chunks is None
            or _strip(chunks[0]) != chunk_digest
            or type(chunks[1]) is not int
            or int(chunks[1]) < 0
            or type(chunks[2]) is not int
            or not 1 <= int(chunks[2]) <= predecessor_epoch_id
            or str(chunks[3]) != "m5-normalize-text-v1"
            or _strip(chunks[4])
            != "d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb"
            or chunks[5] is None
        ):
            raise EventConflictError("document predecessor snapshots changed")
        return registry_digest, chunk_digest
    activation = cursor.execute(
        """
        SELECT activation_id, payload_hash, base_m4_epoch_id, activated_at
        FROM groundloop_m5_activation
        WHERE singleton
        """
    ).fetchone()
    if (
        activation is None
        or not str(activation[0]).strip()
        or len(_strip(activation[1])) != 64
        or int(str(activation[2])) != predecessor_epoch_id
        or activation[3] is None
    ):
        raise EventConflictError("document predecessor has no snapshot authority")
    return None


def _validated_structural_source_chunks(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    closure: _DocumentClosure,
    *,
    lock_authority: bool,
    event_local_only: bool = False,
) -> tuple[str, ...]:
    """Derive D from retained document sidecars, never from the preview."""

    payload = _structural_payload_from_event(event)
    old_version_id = payload.deactivated_document_version_id
    preliminary_old: tuple[tuple[object, ...], ...] = ()
    if old_version_id is not None:
        old_where = (
            "version.document_version_id = %s"
            if event_local_only
            else """
                version.document_version_id = %s
                AND version.valid_from_epoch <= %s
                AND (version.valid_to_epoch IS NULL OR %s < version.valid_to_epoch)
                AND chunk.valid_from_epoch <= %s
                AND (chunk.valid_to_epoch IS NULL OR %s < chunk.valid_to_epoch)
            """
        )
        old_parameters: tuple[object, ...] = (
            (old_version_id,)
            if event_local_only
            else (
                old_version_id,
                closure.previous_epoch_id,
                closure.previous_epoch_id,
                closure.previous_epoch_id,
                closure.previous_epoch_id,
            )
        )
        preliminary_old = tuple(
            tuple(row)
            for row in cursor.execute(
                f"""
                SELECT version.document_version_id, version.document_id,
                       version.content_hash, version.valid_from_epoch,
                       version.valid_to_epoch, chunk.chunk_version_id,
                       chunk.chunk_index, chunk.text, chunk.text_hash,
                       chunk.chunker_version, chunk.valid_from_epoch,
                       chunk.valid_to_epoch
                FROM groundloop_document_version AS version
                JOIN groundloop_chunk_version AS chunk
                  ON chunk.document_version_id = version.document_version_id
                WHERE {old_where}
                ORDER BY chunk.chunk_version_id COLLATE "C"
                """,
                old_parameters,
            ).fetchall()
        )
        if not preliminary_old:
            raise EventConflictError("deactivated document version is not active")
    inserted = payload.inserted
    document_ids = {str(row[1]) for row in preliminary_old} | (
        {inserted.version.document_id} if inserted is not None else set()
    )
    version_ids = ({old_version_id} if old_version_id is not None else set()) | (
        {inserted.version.document_version_id} if inserted is not None else set()
    )
    chunk_ids = {str(row[5]) for row in preliminary_old} | (
        {chunk.chunk_version_id for chunk in inserted.chunks}
        if inserted is not None
        else set()
    )
    if lock_authority:
        for document_id in sorted(document_ids, key=_c_key):
            if (
                cursor.execute(
                    """
                SELECT document_id, source_uri, authority_class
                FROM groundloop_document
                WHERE document_id = %s
                FOR UPDATE
                """,
                    (document_id,),
                ).fetchone()
                is None
            ):
                raise EventConflictError("document source identity disappeared")
        cursor.execute(
            """
            SELECT epoch_id, document_id, source_uri, authority_class
            FROM groundloop_m4_document_metadata_overlay
            WHERE epoch_id = %s
            ORDER BY document_id COLLATE "C"
            FOR UPDATE
            """,
            (closure.epoch_id,),
        ).fetchall()
        for version_id in sorted(version_ids, key=_c_key):
            if (
                cursor.execute(
                    """
                SELECT document_version_id, document_id, content_hash,
                       valid_from_epoch, valid_to_epoch
                FROM groundloop_document_version
                WHERE document_version_id = %s
                FOR UPDATE
                """,
                    (version_id,),
                ).fetchone()
                is None
            ):
                raise EventConflictError("document-version source disappeared")
        for chunk_id in sorted(chunk_ids, key=_c_key):
            if (
                cursor.execute(
                    """
                SELECT chunk_version_id, document_version_id, chunk_index,
                       text, text_hash, chunker_version,
                       valid_from_epoch, valid_to_epoch
                FROM groundloop_chunk_version
                WHERE chunk_version_id = %s
                FOR UPDATE
                """,
                    (chunk_id,),
                ).fetchone()
                is None
            ):
                raise EventConflictError("document chunk source disappeared")
            cursor.execute(
                """
                SELECT chunk_version_id, chunker_artifact_id, input_hash
                FROM groundloop_chunk_provenance
                WHERE chunk_version_id = %s
                FOR UPDATE
                """,
                (chunk_id,),
            ).fetchall()
        cursor.execute(
            """
            SELECT epoch_id, document_version_id
            FROM groundloop_m4_structural_deactivation
            WHERE epoch_id = %s
            ORDER BY document_version_id COLLATE "C"
            FOR UPDATE
            """,
            (closure.epoch_id,),
        ).fetchall()
    deactivation_rows = cursor.execute(
        """
        SELECT epoch_id, document_version_id
        FROM groundloop_m4_structural_deactivation
        WHERE epoch_id = %s
        ORDER BY document_version_id COLLATE "C"
        """,
        (closure.epoch_id,),
    ).fetchall()
    expected_deactivation = (
        () if old_version_id is None else ((closure.epoch_id, old_version_id),)
    )
    if tuple((int(row[0]), str(row[1])) for row in deactivation_rows) != (
        expected_deactivation
    ):
        raise EventConflictError("document structural deactivation is inconsistent")
    old_rows: tuple[tuple[object, ...], ...] = ()
    if old_version_id is not None:
        old_rows = tuple(
            tuple(row)
            for row in cursor.execute(
                f"""
                SELECT version.document_version_id, version.document_id,
                       version.content_hash, version.valid_from_epoch,
                       version.valid_to_epoch, chunk.chunk_version_id,
                       chunk.chunk_index, chunk.text, chunk.text_hash,
                       chunk.chunker_version, chunk.valid_from_epoch,
                       chunk.valid_to_epoch
                FROM groundloop_document_version AS version
                JOIN groundloop_chunk_version AS chunk
                  ON chunk.document_version_id = version.document_version_id
                WHERE {old_where}
                ORDER BY chunk.chunk_version_id COLLATE "C"
                """,
                old_parameters,
            ).fetchall()
        )
        if old_rows != preliminary_old:
            raise EventConflictError("deactivated document source changed before lock")
        if any(
            type(row[3]) is not int
            or not 1 <= int(row[3]) <= closure.previous_epoch_id
            or type(row[10]) is not int
            or not 1 <= int(row[10]) <= closure.previous_epoch_id
            or int(row[3]) != int(row[10])
            or type(row[4]) is not int
            or int(row[4]) != closure.epoch_id
            or type(row[11]) is not int
            or int(row[11]) != closure.epoch_id
            for row in old_rows
        ):
            raise EventConflictError(
                "deactivated document intervals do not close at the event"
            )
        if type(event.event) is ReplaceDocumentVersionEvent and any(
            str(row[1]) != event.event.document_id for row in old_rows
        ):
            raise EventConflictError("replaced version belongs to another document")
        old_chunks: list[ChunkVersion] = []
        old_provenance: list[tuple[str, str, str]] = []
        try:
            old_version = DocumentVersion(
                document_version_id=_exact_text(
                    "deactivated document version", old_rows[0][0]
                ),
                document_id=_exact_text("deactivated document", old_rows[0][1]),
                content_hash=_exact_digest(
                    "deactivated document content hash", old_rows[0][2]
                ),
            )
        except ValidationError as error:
            raise EventConflictError(
                "deactivated document identity is malformed"
            ) from error
        for row in old_rows:
            try:
                if (
                    str(row[0]) != old_version.document_version_id
                    or str(row[1]) != old_version.document_id
                    or _exact_digest("deactivated document content hash", row[2])
                    != old_version.content_hash
                ):
                    raise ValidationError(
                        "deactivated chunks disagree on document identity"
                    )
                old_chunks.append(
                    ChunkVersion(
                        chunk_version_id=_exact_text(
                            "deactivated chunk version", row[5]
                        ),
                        document_version_id=old_version.document_version_id,
                        chunk_index=int(str(row[6])),
                        text=str(row[7]),
                        text_hash=_strip(row[8]),
                    )
                )
                chunker_version = _exact_text("deactivated chunker version", row[9])
            except (TypeError, ValueError, ValidationError) as error:
                raise EventConflictError(
                    "deactivated document chunk identity is malformed"
                ) from error
            provenance = cursor.execute(
                """
                SELECT provenance.chunker_artifact_id,
                       provenance.input_hash,
                       artifact.chunker_version
                FROM groundloop_chunk_provenance AS provenance
                JOIN groundloop_chunker_artifact AS artifact
                  ON artifact.chunker_artifact_id = provenance.chunker_artifact_id
                WHERE provenance.chunk_version_id = %s
                """,
                (str(row[5]),),
            ).fetchall()
            if len(provenance) != 1 or str(provenance[0][2]) != str(row[9]):
                raise EventConflictError(
                    "deactivated document chunk provenance is malformed"
                )
            try:
                old_provenance.append(
                    (
                        _exact_text("deactivated chunker artifact", provenance[0][0]),
                        _exact_digest(
                            "deactivated chunker input hash", provenance[0][1]
                        ),
                        chunker_version,
                    )
                )
            except ValidationError as error:
                raise EventConflictError(
                    "deactivated document chunk provenance is malformed"
                ) from error
        if len(set(old_provenance)) != 1:
            raise EventConflictError(
                "deactivated document chunks have inconsistent provenance"
            )
        artifact_id, input_hash, chunker_version = old_provenance[0]
        try:
            InsertedDocument(
                version=old_version,
                chunks=tuple(old_chunks),
                chunker_version=chunker_version,
                chunker_artifact_id=artifact_id,
                chunker_input_hash=input_hash,
            )
        except ValidationError as error:
            raise EventConflictError(
                "deactivated document source closure is malformed"
            ) from error
    chunks = tuple(str(row[5]) for row in old_rows)
    direct = event.direct_plan
    assert direct is not None
    if chunks != tuple(sorted(direct.deactivated_chunk_version_ids, key=_c_key)):
        raise EventConflictError("caller deactivation differs from locked source")
    metadata_rows = cursor.execute(
        """
        SELECT epoch_id, document_id, source_uri, authority_class
        FROM groundloop_m4_document_metadata_overlay
        WHERE epoch_id = %s
        ORDER BY document_id COLLATE "C"
        """,
        (closure.epoch_id,),
    ).fetchall()
    inserted_rows = cursor.execute(
        """
        SELECT version.document_version_id, version.document_id,
               version.content_hash, version.valid_from_epoch,
               version.valid_to_epoch, chunk.chunk_version_id,
               chunk.chunk_index, chunk.text, chunk.text_hash,
               chunk.chunker_version, chunk.valid_from_epoch,
               chunk.valid_to_epoch
        FROM groundloop_document_version AS version
        LEFT JOIN groundloop_chunk_version AS chunk
          ON chunk.document_version_id = version.document_version_id
         AND chunk.valid_from_epoch = version.valid_from_epoch
        WHERE version.valid_from_epoch = %s
        ORDER BY chunk.chunk_version_id COLLATE "C"
        """,
        (closure.epoch_id,),
    ).fetchall()
    if inserted is None:
        if inserted_rows or metadata_rows:
            raise EventConflictError("delete event contains inserted document rows")
        return chunks
    if metadata_rows != [
        (
            closure.epoch_id,
            inserted.version.document_id,
            inserted.source_uri,
            inserted.authority_class,
        )
    ] or len(inserted_rows) != len(inserted.chunks):
        raise EventConflictError("inserted document chunk closure is incomplete")
    document_row = cursor.execute(
        """
        SELECT document_id, source_uri, authority_class
        FROM groundloop_document
        WHERE document_id = %s
        """,
        (inserted.version.document_id,),
    ).fetchone()
    expected_document_metadata = (
        (inserted.source_uri, inserted.authority_class)
        if old_version_id is not None or closure.runtime_state == "sealed"
        else (None, "unclassified")
    )
    if document_row is None or (
        str(document_row[0]),
        document_row[1],
        str(document_row[2]),
    ) != (
        inserted.version.document_id,
        expected_document_metadata[0],
        expected_document_metadata[1],
    ):
        raise EventConflictError("inserted document identity is inconsistent")
    expected_chunks = {chunk.chunk_version_id: chunk for chunk in inserted.chunks}
    for row in inserted_rows:
        chunk_id = str(row[5])
        expected = expected_chunks.get(chunk_id)
        if expected is None or (
            str(row[0]) != inserted.version.document_version_id
            or str(row[1]) != inserted.version.document_id
            or str(row[2]) != inserted.version.content_hash
            or int(row[3]) != closure.epoch_id
            or row[4] is not None
            or int(row[6]) != expected.chunk_index
            or str(row[7]) != expected.text
            or _strip(row[8]) != expected.text_hash
            or str(row[9]) != inserted.chunker_version
            or int(row[10]) != closure.epoch_id
            or row[11] is not None
        ):
            raise EventConflictError("inserted document sidecar closure changed")
        provenance_rows = cursor.execute(
            """
            SELECT chunker_artifact_id, input_hash
            FROM groundloop_chunk_provenance
            WHERE chunk_version_id = %s
            """,
            (chunk_id,),
        ).fetchall()
        expected_provenance = (
            []
            if inserted.chunker_artifact_id is None
            else [(inserted.chunker_artifact_id, inserted.chunker_input_hash)]
        )
        if [
            (str(item[0]), _strip(item[1])) for item in provenance_rows
        ] != expected_provenance:
            raise EventConflictError("inserted chunk provenance changed")
    return chunks


def _require_active_membership(
    cursor: Cursor[Any],
    *,
    predecessor_epoch_id: int,
    snapshots: tuple[str, str] | None,
    requirement_version_id: str | None = None,
    chunk_version_id: str | None = None,
) -> bool:
    if requirement_version_id is None and chunk_version_id is None:
        raise AssertionError("membership requires a touched key")
    if snapshots is not None:
        registry_digest, chunk_digest = snapshots
        if requirement_version_id is not None:
            row = cursor.execute(
                """
                WITH member_point AS MATERIALIZED (
                    SELECT requirement_registry_snapshot_digest,
                           requirement_version_id
                    FROM groundloop_m5_requirement_registry_snapshot_member
                    WHERE ROW(
                        requirement_registry_snapshot_digest,
                        requirement_version_id
                    ) >= ROW(%s, %s)
                    ORDER BY requirement_registry_snapshot_digest,
                             requirement_version_id
                    LIMIT 1
                )
                SELECT 1
                FROM member_point
                WHERE requirement_registry_snapshot_digest = %s
                  AND requirement_version_id = %s
                """,
                (
                    registry_digest,
                    requirement_version_id,
                    registry_digest,
                    requirement_version_id,
                ),
            ).fetchone()
            if row is None:
                return False
        if chunk_version_id is not None:
            row = cursor.execute(
                """
                WITH member_point AS MATERIALIZED (
                    SELECT active_chunk_snapshot_digest, chunk_version_id
                    FROM groundloop_m5_active_chunk_snapshot_member
                    WHERE ROW(
                        active_chunk_snapshot_digest, chunk_version_id
                    ) >= ROW(%s, %s)
                    ORDER BY active_chunk_snapshot_digest, chunk_version_id
                    LIMIT 1
                )
                SELECT 1
                FROM member_point
                WHERE active_chunk_snapshot_digest = %s
                  AND chunk_version_id = %s
                """,
                (chunk_digest, chunk_version_id, chunk_digest, chunk_version_id),
            ).fetchone()
            if row is None:
                return False
        return True
    if requirement_version_id is not None:
        row = cursor.execute(
            """
            SELECT 1
            FROM groundloop_m5_requirement_version AS requirement
            JOIN groundloop_m5_group_version AS group_version
              ON group_version.group_version_id = requirement.group_version_id
            JOIN groundloop_m5_group_validity AS validity
              ON validity.group_version_id = requirement.group_version_id
            WHERE requirement.requirement_version_id = %s
              AND requirement.lifecycle_state = 'PUBLISHED'
              AND group_version.lifecycle_state = 'PUBLISHED'
              AND validity.valid_from_epoch <= %s
              AND (
                  validity.valid_to_epoch IS NULL
                  OR %s < validity.valid_to_epoch
              )
            """,
            (
                requirement_version_id,
                predecessor_epoch_id,
                predecessor_epoch_id,
            ),
        ).fetchone()
        if row is None:
            return False
    if chunk_version_id is not None:
        row = cursor.execute(
            """
            SELECT 1
            FROM groundloop_chunk_version AS chunk
            JOIN groundloop_document_version AS version
              ON version.document_version_id = chunk.document_version_id
            JOIN groundloop_epoch AS creator
              ON creator.epoch_id = chunk.valid_from_epoch
            WHERE chunk.chunk_version_id = %s
              AND chunk.valid_from_epoch <= %s
              AND (chunk.valid_to_epoch IS NULL OR %s < chunk.valid_to_epoch)
              AND version.valid_from_epoch <= %s
              AND (version.valid_to_epoch IS NULL OR %s < version.valid_to_epoch)
              AND creator.structural_status = 'committed'
              AND creator.semantic_status = 'sealed'
              AND creator.evaluation_state = 'complete'
              AND creator.publication_mode IN ('provisional', 'strict')
              AND creator.sealed_at IS NOT NULL
            """,
            (
                chunk_version_id,
                predecessor_epoch_id,
                predecessor_epoch_id,
                predecessor_epoch_id,
                predecessor_epoch_id,
            ),
        ).fetchone()
        if row is None:
            return False
    return True


def _is_sealed_lineage_owner(
    cursor: Cursor[Any], owner_epoch_id: int, predecessor_epoch_id: int
) -> bool:
    row = cursor.execute(
        """
        SELECT epoch.event_id, epoch.payload_hash, epoch.revision,
               epoch.structural_status, epoch.semantic_status,
               epoch.evaluation_state, epoch.publication_mode, epoch.sealed_at,
               runtime.structural_event_id, runtime.runtime_state,
               runtime.revision, runtime.open_work_count,
               runtime.open_scope_count, runtime.blocking_failure_count,
               runtime.terminal_at,
               result.structural_event_id, result.payload_hash,
               result.epoch_id, result.outcome
        FROM groundloop_epoch AS epoch
        LEFT JOIN groundloop_m5_runtime_epoch AS runtime
          ON runtime.epoch_id = epoch.epoch_id
        LEFT JOIN groundloop_m5_event_result AS result
          ON result.epoch_id = epoch.epoch_id
        WHERE epoch.epoch_id = %s
        """,
        (owner_epoch_id,),
    ).fetchone()
    if row is None:
        raise EventConflictError("withdrawal history names a missing owner epoch")
    try:
        event_id = _exact_text("withdrawal owner event", row[0])
        payload_hash = _exact_digest("withdrawal owner payload", row[1])
    except ValidationError as error:
        raise EventConflictError("withdrawal owner identity is malformed") from error
    runtime_state = None if row[9] is None else str(row[9])
    if (
        type(row[2]) is not int
        or int(row[2]) < 1
        or row[8] is None
        or runtime_state is None
        or str(row[8]) != event_id
        or type(row[10]) is not int
        or int(row[10]) != int(row[2])
        or any(
            type(row[index]) is not int or int(row[index]) < 0 for index in (11, 12, 13)
        )
    ):
        raise EventConflictError("withdrawal owner runtime header is malformed")
    epoch_shape = (
        str(row[3]),
        str(row[4]),
        str(row[5]),
        str(row[6]),
        row[7] is not None,
    )
    expected_shape = {
        "structural_committed": (
            "committed",
            "pending",
            "pending",
            "provisional",
            False,
        ),
        "semantic_pending": (
            "committed",
            "pending",
            "pending",
            "provisional",
            False,
        ),
        "semantic_complete": (
            "committed",
            "complete",
            "complete",
            "provisional",
            False,
        ),
        "sealed": ("committed", "sealed", "complete", "strict", True),
        "failed": ("failed", "failed", "failed", "provisional", False),
    }.get(runtime_state)
    terminal = runtime_state in {"sealed", "failed"}
    if (
        expected_shape is None
        or epoch_shape != expected_shape
        or terminal != (row[14] is not None)
        or (
            runtime_state in {"semantic_complete", "sealed"}
            and tuple(row[11:14]) != (0, 0, 0)
        )
        or (runtime_state == "failed" and tuple(row[11:13]) != (0, 0))
    ):
        raise EventConflictError("withdrawal owner state shape is malformed")
    result_values = tuple(row[15:19])
    result_present = any(value is not None for value in result_values)
    if result_present != terminal or (
        result_present
        and (
            any(value is None for value in result_values)
            or str(row[15]) != event_id
            or _strip(row[16]) != payload_hash
            or int(str(row[17])) != owner_epoch_id
            or str(row[18]) != runtime_state
        )
    ):
        raise EventConflictError("withdrawal owner terminal identity is malformed")
    if not terminal:
        return False
    result = _load_canonical_terminal_result(
        cursor,
        structural_event_id=event_id,
        payload_hash=payload_hash,
    )
    if result is None or result.epoch_id != owner_epoch_id:
        raise EventConflictError("withdrawal owner lost its terminal result")
    if runtime_state == "failed":
        if result.replayed_outcome is not M5ReplayedOutcome.FAILED:
            raise EventConflictError("failed withdrawal owner result changed")
        return False
    if result.replayed_outcome is not M5ReplayedOutcome.SEALED:
        raise EventConflictError("sealed withdrawal owner result changed")
    if owner_epoch_id > predecessor_epoch_id:
        raise EventConflictError("withdrawal owner is sealed after the predecessor")
    return True


def _is_sealed_direct_owner(
    cursor: Cursor[Any],
    owner_epoch_id: int,
    predecessor_epoch_id: int,
    candidate_policy_id: str,
) -> bool:
    row = cursor.execute(
        """
        SELECT epoch.structural_status, epoch.semantic_status,
               epoch.evaluation_state, epoch.publication_mode,
               epoch.sealed_at, update.candidate_policy_id
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m4_update AS update ON update.epoch_id = epoch.epoch_id
        WHERE epoch.epoch_id = %s
        """,
        (owner_epoch_id,),
    ).fetchone()
    if row is None:
        raise EventConflictError("direct verifier owner closure disappeared")
    sealed = (
        tuple(row[:4]) == ("committed", "sealed", "complete", "strict")
        and row[4] is not None
        and str(row[5]) == candidate_policy_id
    )
    if not sealed:
        raise EventConflictError("direct verifier owner is not a sealed M4 epoch")
    if owner_epoch_id > predecessor_epoch_id:
        raise EventConflictError("direct verifier owner is after the predecessor")
    return True


def _candidate_locator_classification_coordinates(
    row: tuple[object, ...],
) -> tuple[int, str]:
    """Extract only the two coordinates needed for §5 classification."""

    try:
        owner_epoch_id = int(str(row[0]))
        requirement_id = _exact_text("candidate locator requirement", row[2])
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError(
            "candidate locator classification coordinates are malformed"
        ) from error
    if owner_epoch_id < 1:
        raise EventConflictError(
            "candidate locator classification coordinates are malformed"
        )
    return owner_epoch_id, requirement_id


def _validate_candidate_locator_admitted_row(
    *,
    located_chunk: str,
    admitted_digest: str,
    row: tuple[object, ...],
) -> tuple[int, str]:
    """Validate a qualifying admitted row after lineage/activity filtering."""

    try:
        owner_epoch_id, requirement_id = _candidate_locator_classification_coordinates(
            row
        )
        pair = SemanticPairKey(SubjectKind(str(row[1])), str(row[2]), str(row[3]))
        historical_policy = _exact_digest("candidate locator policy", row[5])
        _exact_digest("candidate locator owner root", row[6])
        _exact_digest("candidate locator digest", admitted_digest)
        raw_reasons = row[7]
        if not isinstance(raw_reasons, (list, tuple)):
            raise ValidationError("candidate locator reasons are not an array")
        reasons = tuple(
            M5RequirementAdmissionChannel(str(reason)) for reason in raw_reasons
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError("candidate locator row is malformed") from error
    if (
        owner_epoch_id < 1
        or pair.subject_id != requirement_id
        or pair.subject_kind is not SubjectKind.REQUIREMENT
        or pair.chunk_version_id != located_chunk
        or _strip(row[4]) != pair.semantic_pair_digest
        or not reasons
        or type(row[8]) is not bool
        or not historical_policy
    ):
        raise EventConflictError("candidate locator row is malformed")
    return owner_epoch_id, pair.subject_id


def _candidate_edges(
    cursor: Cursor[Any],
    *,
    chunks: tuple[str, ...],
    predecessor_epoch_id: int,
    snapshots: tuple[str, str] | None,
    candidate_policy_id: str,
    lock_authority: bool,
    located_rows: tuple[tuple[str, str], ...] | None = None,
    qualifying_digests: frozenset[str] | None = None,
) -> tuple[M5WithdrawnCandidateEdge, ...]:
    if located_rows is None:
        locator_values: list[tuple[str, str]] = []
        for chunk_id in chunks:
            rows = cursor.execute(
                """
                SELECT chunk_version_id, admitted_pair_digest
                FROM groundloop_m5_requirement_admitted_pair
                WHERE chunk_version_id COLLATE "C" = %s
                ORDER BY admitted_pair_digest COLLATE "C"
                """,
                (chunk_id,),
            ).fetchall()
            locator_values.extend((str(row[0]), _strip(row[1])) for row in rows)
        locator_values.sort(key=lambda item: (_c_key(item[0]), _c_key(item[1])))
        locator_rows = tuple(locator_values)
    else:
        locator_rows = located_rows
    evidence: list[M5WithdrawnCandidateEdge] = []
    for located_chunk, admitted_digest in locator_rows:
        if qualifying_digests is not None and admitted_digest not in qualifying_digests:
            continue
        row = cursor.execute(
            """
            SELECT epoch_id, subject_kind::text, subject_id,
                   chunk_version_id, semantic_pair_digest,
                   candidate_policy_id, owner_root_job_id,
                   reasons, mandatory_lineage
            FROM groundloop_m5_requirement_admitted_pair
            WHERE admitted_pair_digest = %s
            """,
            (admitted_digest,),
        ).fetchone()
        if row is None:
            raise EventConflictError("candidate locator changed before validation")
        owner_epoch_id, requirement_id = _candidate_locator_classification_coordinates(
            tuple(row)
        )
        if qualifying_digests is None:
            lineage = _is_sealed_lineage_owner(
                cursor, owner_epoch_id, predecessor_epoch_id
            )
            active = _require_active_membership(
                cursor,
                predecessor_epoch_id=predecessor_epoch_id,
                snapshots=snapshots,
                requirement_version_id=requirement_id,
                chunk_version_id=located_chunk,
            )
            if not lineage or not active:
                continue
        validated_owner, validated_requirement = (
            _validate_candidate_locator_admitted_row(
                located_chunk=located_chunk,
                admitted_digest=admitted_digest,
                row=tuple(row),
            )
        )
        if validated_owner != owner_epoch_id or validated_requirement != requirement_id:
            raise EventConflictError("candidate locator coordinates changed")
        historical_policy = _strip(row[5])
        pair = SemanticPairKey(SubjectKind.REQUIREMENT, requirement_id, located_chunk)
        source_rows = cursor.execute(
            """
            SELECT source.root_job_id, source.scope_contract_digest,
                   source.selection_digest, selection.subject_kind::text,
                   selection.subject_id, selection.chunk_version_id,
                   selection.semantic_pair_digest,
                   selection.fused_rank, selection.reasons,
                   selection.mandatory_lineage,
                   scope.epoch_id, scope.scope_state,
                   scope.candidate_policy_id, scope.scope_contract_digest,
                   job.candidate_policy_id, job.job_kind, job.job_state,
                   job.parent_job_id, job.scope_contract_digest,
                   job.result_artifact_id, job.result_artifact_hash,
                   job.completion_digest
            FROM groundloop_m5_requirement_admitted_pair_source AS source
            JOIN groundloop_m5_requirement_scope_selection AS selection
              ON selection.selection_digest = source.selection_digest
             AND selection.root_job_id = source.root_job_id
             AND selection.scope_contract_digest = source.scope_contract_digest
            JOIN groundloop_m5_discovery_scope AS scope
              ON scope.root_job_id = source.root_job_id
             AND scope.scope_contract_digest = source.scope_contract_digest
            JOIN groundloop_m5_semantic_job AS job
              ON job.logical_job_id = source.root_job_id
             AND job.epoch_id = scope.epoch_id
            WHERE source.admitted_pair_digest = %s
            ORDER BY source.root_job_id COLLATE "C"
            """,
            (admitted_digest,),
        ).fetchall()
        if lock_authority:
            locked_row = cursor.execute(
                """
                SELECT epoch_id, subject_kind::text, subject_id,
                       chunk_version_id, semantic_pair_digest,
                       candidate_policy_id, owner_root_job_id,
                       reasons, mandatory_lineage
                FROM groundloop_m5_requirement_admitted_pair
                WHERE admitted_pair_digest = %s
                FOR UPDATE
                """,
                (admitted_digest,),
            ).fetchone()
            if locked_row is None or tuple(locked_row) != tuple(row):
                raise EventConflictError(
                    "candidate admission changed before authority lock"
                )
            locked_sources = cursor.execute(
                """
                SELECT source.root_job_id, source.scope_contract_digest,
                       source.selection_digest, selection.subject_kind::text,
                       selection.subject_id, selection.chunk_version_id,
                       selection.semantic_pair_digest,
                       selection.fused_rank, selection.reasons,
                       selection.mandatory_lineage,
                       scope.epoch_id, scope.scope_state,
                       scope.candidate_policy_id, scope.scope_contract_digest,
                       job.candidate_policy_id, job.job_kind, job.job_state,
                       job.parent_job_id, job.scope_contract_digest,
                       job.result_artifact_id, job.result_artifact_hash,
                       job.completion_digest
                FROM groundloop_m5_requirement_admitted_pair_source AS source
                JOIN groundloop_m5_requirement_scope_selection AS selection
                  ON selection.selection_digest = source.selection_digest
                 AND selection.root_job_id = source.root_job_id
                 AND selection.scope_contract_digest =
                     source.scope_contract_digest
                JOIN groundloop_m5_discovery_scope AS scope
                  ON scope.root_job_id = source.root_job_id
                 AND scope.scope_contract_digest = source.scope_contract_digest
                JOIN groundloop_m5_semantic_job AS job
                  ON job.logical_job_id = source.root_job_id
                 AND job.epoch_id = scope.epoch_id
                WHERE source.admitted_pair_digest = %s
                ORDER BY source.root_job_id COLLATE "C"
                FOR UPDATE OF source
                """,
                (admitted_digest,),
            ).fetchall()
            if tuple(tuple(item) for item in locked_sources) != tuple(
                tuple(item) for item in source_rows
            ):
                raise EventConflictError(
                    "candidate source changed before authority lock"
                )
        sources: list[M5RequirementAdmittedPairSource] = []
        for source_row in source_rows:
            if (
                source_row[3] != "requirement"
                or str(source_row[4]) != requirement_id
                or str(source_row[5]) != located_chunk
                or _strip(source_row[6]) != pair.semantic_pair_digest
                or int(source_row[10]) != owner_epoch_id
                or source_row[11] != "closed_active"
                or _strip(source_row[12]) != historical_policy
                or _strip(source_row[13]) != _strip(source_row[1])
                or _strip(source_row[14]) != historical_policy
                or source_row[15]
                not in {
                    "forward_requirement_retrieval",
                    "reverse_requirement_discovery",
                }
                or source_row[16] != "completed_active"
                or source_row[17] is not None
                or _strip(source_row[18]) != _strip(source_row[1])
                or source_row[19] is None
                or source_row[20] is None
                or source_row[21] is None
            ):
                raise EventConflictError("candidate source closure is inconsistent")
            try:
                M5RequirementScopeSelection(
                    root_job_id=_strip(source_row[0]),
                    scope_contract_digest=_strip(source_row[1]),
                    pair=pair,
                    semantic_pair_digest=_strip(source_row[6]),
                    fused_rank=int(source_row[7]),
                    reasons=tuple(
                        M5RequirementAdmissionChannel(str(reason))
                        for reason in source_row[8]
                    ),
                    mandatory_lineage=bool(source_row[9]),
                    selection_digest=_strip(source_row[2]),
                )
            except (TypeError, ValueError, ValidationError) as error:
                raise EventConflictError(
                    "candidate source selection digest is invalid"
                ) from error
            sources.append(
                M5RequirementAdmittedPairSource(
                    _strip(source_row[0]),
                    _strip(source_row[1]),
                    _strip(source_row[2]),
                )
            )
        try:
            admitted = M5RequirementAdmittedPair(
                epoch_id=owner_epoch_id,
                pair=pair,
                semantic_pair_digest=_strip(row[4]),
                candidate_policy_id=historical_policy,
                owner_root_job_id=_strip(row[6]),
                sources=tuple(sources),
                reasons=tuple(
                    M5RequirementAdmissionChannel(str(reason)) for reason in row[7]
                ),
                mandatory_lineage=bool(row[8]),
                admitted_pair_digest=admitted_digest,
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError("candidate admission digest is invalid") from error
        if admitted.candidate_policy_id != candidate_policy_id:
            raise EventConflictError(
                "cross-policy candidate withdrawal is not authorized"
            )
        evidence.append(
            M5WithdrawnCandidateEdge(
                pair.semantic_pair_digest,
                requirement_id,
                located_chunk,
                candidate_policy_id,
            )
        )
    return tuple(
        sorted(
            set(evidence),
            key=lambda edge: (
                _c_key(edge.semantic_pair_digest),
                _c_key(edge.requirement_version_id),
                _c_key(edge.chunk_version_id),
                _c_key(edge.candidate_policy_id),
            ),
        )
    )


def _observation_edges(
    cursor: Cursor[Any],
    *,
    chunks: tuple[str, ...],
    predecessor_epoch_id: int,
    snapshots: tuple[str, str] | None,
    candidate_policy_id: str,
    lock_authority: bool,
) -> tuple[M5WithdrawnObservationEdge, ...]:
    locators: list[tuple[str, str, str, str]] = []
    for chunk_id in chunks:
        rows = cursor.execute(
            """
            SELECT subject_id, chunk_version_id, task_type, observation_id
            FROM groundloop_observation_currency
            WHERE subject_kind = 'requirement'
              AND chunk_version_id = %s
            """,
            (chunk_id,),
        ).fetchall()
        locators.extend(
            (str(row[0]), str(row[1]), str(row[2]), str(row[3])) for row in rows
        )
    locators.sort(key=lambda row: tuple(_c_key(value) for value in row))
    edges: list[M5WithdrawnObservationEdge] = []
    for requirement_id, chunk_id, task_type, observation_id in locators:
        observation_lock: tuple[object, ...] | None = None
        if lock_authority:
            locked = cursor.execute(
                """
                SELECT subject_kind::text, subject_id, chunk_version_id,
                       task_type, input_hash, produced_epoch,
                       eligible_for_currency
                FROM groundloop_semantic_observation
                WHERE observation_id = %s
                FOR UPDATE
                """,
                (observation_id,),
            ).fetchone()
            if locked is None:
                raise EventConflictError("current observation disappeared")
            observation_lock = tuple(locked)
        lock_clause = " FOR UPDATE OF currency, published" if lock_authority else ""
        rows = cursor.execute(
            """
            SELECT currency.observation_id, currency.installed_revision,
                   observation.subject_kind::text, observation.subject_id,
                   observation.chunk_version_id, observation.task_type,
                   observation.input_hash, observation.produced_epoch,
                   observation.eligible_for_currency,
                   published.valid_from_epoch, published.valid_to_epoch
            FROM groundloop_observation_currency AS currency
            JOIN groundloop_semantic_observation AS observation
              ON observation.observation_id = currency.observation_id
            JOIN groundloop_published_observation_currency AS published
              ON published.subject_kind = currency.subject_kind
             AND published.subject_id = currency.subject_id
             AND published.chunk_version_id = currency.chunk_version_id
             AND published.task_type = currency.task_type
             AND published.observation_id = currency.observation_id
             AND published.valid_to_epoch IS NULL
            WHERE currency.subject_kind = 'requirement'
              AND currency.subject_id = %s
              AND currency.chunk_version_id = %s
              AND currency.task_type = %s
              AND currency.observation_id = %s
            """
            + lock_clause,
            (
                requirement_id,
                chunk_id,
                task_type,
                observation_id,
            ),
        ).fetchall()
        if len(rows) != 1:
            raise EventConflictError("current requirement currency changed")
        row = rows[0]
        if (
            str(row[0]) != observation_id
            or type(row[1]) is not int
            or int(row[1]) < 0
            or row[2] != "requirement"
            or str(row[3]) != requirement_id
            or str(row[4]) != chunk_id
            or str(row[5]) != task_type
            or row[8] is not True
            or int(str(row[9])) > predecessor_epoch_id
            or row[10] is not None
        ):
            raise EventConflictError("current requirement observation is malformed")
        if observation_lock is not None and observation_lock != (
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
        ):
            raise EventConflictError("current observation changed before currency")
        if not _require_active_membership(
            cursor,
            predecessor_epoch_id=predecessor_epoch_id,
            snapshots=snapshots,
            requirement_version_id=requirement_id,
            chunk_version_id=chunk_id,
        ):
            raise EventConflictError("current observation is inactive at predecessor")
        verifier_rows = cursor.execute(
            """
            SELECT pair_input.subject_kind::text, pair_input.subject_id,
                   pair_input.chunk_version_id,
                   pair_input.semantic_pair_digest,
                   pair_input.candidate_policy_id,
                   execution.produced_epoch_id,
                   execution.eligible_for_currency,
                   artifact.subject_kind::text, artifact.subject_id,
                   artifact.chunk_version_id, artifact.semantic_pair_digest,
                   artifact.pair_input_hash, execution.pair_input_hash,
                   job.epoch_id, job.job_kind, job.job_state,
                   job.candidate_policy_id, job.semantic_pair_digest,
                   job.result_artifact_id, execution.artifact_id,
                   job.result_artifact_hash, execution.artifact_hash
            FROM groundloop_m5_requirement_verifier_execution AS execution
            JOIN groundloop_m5_requirement_pair_input AS pair_input
              ON pair_input.pair_input_hash = execution.pair_input_hash
            JOIN groundloop_m5_requirement_verifier_artifact AS artifact
              ON artifact.artifact_id = execution.artifact_id
             AND artifact.artifact_hash = execution.artifact_hash
            JOIN groundloop_m5_semantic_job AS job
              ON job.logical_job_id = execution.logical_job_id
            WHERE execution.observation_id = %s
            """,
            (observation_id,),
        ).fetchall()
        if len(verifier_rows) > 1:
            raise EventConflictError("observation has ambiguous verifier provenance")
        produced_epoch = int(row[7])
        if verifier_rows:
            provenance = verifier_rows[0]
            pair = SemanticPairKey(SubjectKind.REQUIREMENT, requirement_id, chunk_id)
            if (
                provenance[0] != "requirement"
                or str(provenance[1]) != requirement_id
                or str(provenance[2]) != chunk_id
                or _strip(provenance[3]) != pair.semantic_pair_digest
                or _strip(provenance[4]) != candidate_policy_id
                or int(provenance[5]) != produced_epoch
                or provenance[6] is not True
                or provenance[7] != "requirement"
                or str(provenance[8]) != requirement_id
                or str(provenance[9]) != chunk_id
                or _strip(provenance[10]) != pair.semantic_pair_digest
                or _strip(provenance[11]) != _strip(provenance[12])
                or int(provenance[13]) != produced_epoch
                or provenance[14] != "verify_requirement_pair"
                or provenance[15] != "completed_active"
                or _strip(provenance[16]) != candidate_policy_id
                or _strip(provenance[17]) != pair.semantic_pair_digest
                or _strip(provenance[18]) != _strip(provenance[19])
                or _strip(provenance[20]) != _strip(provenance[21])
                or not _is_sealed_lineage_owner(
                    cursor, produced_epoch, predecessor_epoch_id
                )
            ):
                raise EventConflictError(
                    "current observation verifier provenance is inconsistent"
                )
        else:
            typed_producer = cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM groundloop_m5_runtime_epoch
                    WHERE epoch_id = %s
                ) OR EXISTS (
                    SELECT 1 FROM groundloop_m5_update WHERE epoch_id = %s
                )
                """,
                (produced_epoch, produced_epoch),
            ).fetchone()
            activation = cursor.execute(
                """
                SELECT activation.base_m4_epoch_id, base.revision,
                       base.structural_status, base.semantic_status,
                       base.evaluation_state, base.publication_mode,
                       base.sealed_at
                FROM groundloop_m5_activation AS activation
                JOIN groundloop_epoch AS base
                  ON base.epoch_id = activation.base_m4_epoch_id
                WHERE activation.singleton
                """
            ).fetchone()
            if (
                typed_producer != (False,)
                or activation is None
                or produced_epoch > int(activation[0])
                or int(row[9]) > int(activation[0])
                or predecessor_epoch_id < int(activation[0])
                or tuple(activation[2:6])
                != ("committed", "sealed", "complete", "strict")
                or activation[6] is None
                or not _require_active_membership(
                    cursor,
                    predecessor_epoch_id=int(activation[0]),
                    snapshots=None,
                    requirement_version_id=requirement_id,
                    chunk_version_id=chunk_id,
                )
            ):
                raise EventConflictError(
                    "current observation lacks supported exact provenance"
                )
        edges.append(
            M5WithdrawnObservationEdge(
                observation_id,
                requirement_id,
                chunk_id,
                candidate_policy_id,
            )
        )
    return tuple(
        sorted(
            set(edges),
            key=lambda edge: (
                _c_key(edge.observation_id),
                _c_key(edge.requirement_version_id),
                _c_key(edge.chunk_version_id),
                _c_key(edge.candidate_policy_id),
            ),
        )
    )


def _assert_zero_cancellation_authority(
    cursor: Cursor[Any], predecessor_epoch_id: int
) -> None:
    probes = (
        (
            """
            SELECT 1 FROM groundloop_semantic_job
            WHERE epoch_id = %s
              AND job_state = ANY(%s)
            LIMIT 1
            """,
            (predecessor_epoch_id, list(_NONTERMINAL_JOB_STATES)),
        ),
        (
            """
            SELECT 1
            FROM groundloop_semantic_job AS job
            JOIN groundloop_discovery_scope AS scope
              ON scope.root_job_id = job.job_id
             AND scope.epoch_id = job.epoch_id
            WHERE job.epoch_id = %s
              AND scope.closed_revision IS NULL
            LIMIT 1
            """,
            (predecessor_epoch_id,),
        ),
        (
            """
            SELECT 1 FROM groundloop_m5_semantic_job
            WHERE epoch_id = %s
              AND job_state = ANY(%s)
            LIMIT 1
            """,
            (predecessor_epoch_id, list(_NONTERMINAL_JOB_STATES)),
        ),
        (
            """
            SELECT 1 FROM groundloop_m5_discovery_scope
            WHERE epoch_id = %s
              AND scope_state IN ('open', 'result_staged')
            LIMIT 1
            """,
            (predecessor_epoch_id,),
        ),
        (
            """
            SELECT 1 FROM groundloop_m5_owner_pending_counter
            WHERE epoch_id = %s AND pending_multiplicity > 0
            LIMIT 1
            """,
            (predecessor_epoch_id,),
        ),
        (
            """
            SELECT 1 FROM groundloop_m5_answer_pending_counter
            WHERE epoch_id = %s AND pending_multiplicity > 0
            LIMIT 1
            """,
            (predecessor_epoch_id,),
        ),
    )
    if any(
        cursor.execute(statement, parameters).fetchone() is not None
        for statement, parameters in probes
    ):
        raise EventConflictError("document predecessor retains nonterminal authority")


@dataclass(frozen=True, slots=True)
class _D29LocatorAuthority:
    predecessor_snapshots: tuple[str, str] | None
    admitted_locator_keys: tuple[tuple[str, str], ...]
    qualifying_admitted_pair_digests: tuple[str, ...]
    candidate_root_job_ids: tuple[str, ...]
    candidate_root_job_coordinates: tuple[tuple[int, str], ...]
    candidate_dependency_coordinates: tuple[tuple[int, str, str], ...]
    direct_job_ids: tuple[str, ...]
    direct_job_coordinates: tuple[tuple[int, str], ...]
    direct_dependency_coordinates: tuple[tuple[int, str, str], ...]
    direct_admitted_pair_ids: tuple[str, ...]
    direct_scope_root_job_ids: tuple[str, ...]
    direct_verifier_observation_ids: tuple[str, ...]
    verifier_job_ids: tuple[str, ...]
    verifier_root_job_ids: tuple[str, ...]
    verifier_observation_ids: tuple[str, ...]
    verifier_dependency_coordinates: tuple[tuple[int, str, str], ...]
    verifier_artifact_ids: tuple[str, ...]
    verifier_pair_input_hashes: tuple[str, ...]
    bootstrap_observation_coordinates: tuple[tuple[str, int], ...]
    direct_bootstrap_observation_coordinates: tuple[tuple[str, int], ...]
    requirement_currency_keys: tuple[tuple[str, str, str, str], ...]
    direct_currency_keys: tuple[tuple[str, str, str, str], ...]
    direct_frontier_keys: tuple[tuple[str, str, str, int], ...]
    touched_requirement_ids: tuple[str, ...]
    prospective_coordinates: _DocumentDeclarationCoordinates
    d30_claims: _D30ClaimLocatorAuthority
    direct_claim_before_images: tuple[_D29DirectClaimBeforeImage, ...] = ()
    direct_answer_before_images: tuple[_D29DirectAnswerBeforeImage, ...] = ()
    direct_remaining_observation_rows: tuple[tuple[object, ...], ...] = ()
    direct_observation_source_rows: tuple[tuple[str, str, str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class _D30CurrencyRow:
    subject_kind: str
    subject_id: str
    chunk_version_id: str
    task_type: str
    observation_id: str
    installed_revision: int

    @property
    def full_key(self) -> tuple[str, str, str, str]:
        return (
            self.subject_kind,
            self.subject_id,
            self.chunk_version_id,
            self.task_type,
        )


@dataclass(frozen=True, slots=True)
class _D30D24AttemptLocator:
    attempt_id: str
    job_id: str
    attempt_state: str
    dispatch_row: tuple[object, ...]
    evidence_row: tuple[object, ...] | None
    timing_row: tuple[object, ...] | None
    acquisition_contribution_row: tuple[object, ...]
    attempt_contribution_row: tuple[object, ...] | None
    transition_contribution_row: tuple[object, ...] | None
    transition_timing_rows: tuple[tuple[object, ...], ...]
    m4_transition_row: tuple[object, ...] | None
    late_envelope_row: tuple[object, ...] | None
    expired_return_row: tuple[object, ...] | None
    postterminal_timing_row: tuple[object, ...] | None
    postterminal_audit_row: tuple[object, ...] | None
    preterminal_late_contribution_row: tuple[object, ...] | None


@dataclass(frozen=True, slots=True)
class _D30D24TerminalLocator:
    event_result_row: tuple[object, ...]
    work_rows: tuple[tuple[object, ...], ...]
    timing_coverage_row: tuple[object, ...]
    delta_rows: tuple[tuple[object, ...], ...]
    reference_rows: tuple[tuple[object, ...], ...]


@dataclass(frozen=True, slots=True)
class _D30D24OwnerLocator:
    attempts: tuple[_D30D24AttemptLocator, ...]
    work_accumulator_row: tuple[object, ...]
    timing_accumulator_row: tuple[object, ...]
    seal_contribution_row: tuple[object, ...]
    terminal_rows: _D30D24TerminalLocator
    terminal_call_work: M5RuntimeWork
    terminal_result: M5EventRunResult


@dataclass(frozen=True, slots=True)
class _D30OwnerLocator:
    epoch_id: int
    epoch_row: tuple[object, ...]
    update_row: tuple[object, ...]
    runtime_row: tuple[object, ...] | None
    jobs: tuple[tuple[object, ...], ...]
    dependencies: tuple[tuple[object, ...], ...]
    scopes: tuple[tuple[str, tuple[object, ...] | None], ...]
    attempts: tuple[tuple[str, tuple[tuple[object, ...], ...]], ...]
    discovery_results: tuple[tuple[str, tuple[object, ...] | None], ...]
    projections: tuple[tuple[str, tuple[object, ...] | None], ...]
    d24: _D30D24OwnerLocator | None = None


@dataclass(frozen=True, slots=True)
class _D30DynamicClaimLocator:
    currency: _D30CurrencyRow
    observation_row: tuple[object, ...]
    delta_row: tuple[object, ...]
    predecessor_epoch_id: int | None
    predecessor_candidate: tuple[object, ...] | None
    owner_epoch_id: int
    child_job_id: str
    parent_job_id: str
    admitted_pair_id: str
    admitted_pair_row: tuple[object, ...]
    parent_result_row: tuple[object, ...]
    execution_rows: tuple[
        tuple[object, ...] | None,
        tuple[object, ...] | None,
        tuple[object, ...] | None,
    ]
    model_row: tuple[object, ...] | None
    prompt_row: tuple[object, ...] | None


@dataclass(frozen=True, slots=True)
class _D30M3ClaimLocator:
    currency: _D30CurrencyRow
    observation_row: tuple[object, ...]
    epoch_row: tuple[object, ...]
    execution_row: tuple[object, ...]
    run_row: tuple[object, ...]
    candidate_row: tuple[object, ...]
    model_row: tuple[object, ...]
    prompt_row: tuple[object, ...]
    embedding_model_row: tuple[object, ...]
    chunk_embedding_row: tuple[object, ...]
    artifact_use_rows: tuple[tuple[object, ...], ...]
    image_rows: tuple[tuple[object, ...], ...]


@dataclass(frozen=True, slots=True)
class _D30ClaimLocatorAuthority:
    currency_rows: tuple[_D30CurrencyRow, ...]
    dynamic: tuple[_D30DynamicClaimLocator, ...]
    bootstrap: tuple[_D30M3ClaimLocator, ...]
    owners: tuple[_D30OwnerLocator, ...]
    activation_row: tuple[object, ...] | None


@dataclass(frozen=True, slots=True)
class _RequirementVerifierAuthority:
    observation: SemanticObservation
    raw_output_hash: str
    produced_epoch_id: int
    installed_revision: int
    eligible_for_currency: bool
    candidate_policy_id: str


@dataclass(frozen=True, slots=True)
class _BootstrapObservationAuthority:
    base_epoch_id: int
    base_revision: int


@dataclass(frozen=True, slots=True)
class _DirectVerifierAuthority:
    execution: M5TypedDirectVerificationExecution
    pair_input: PairVerificationInput
    pair: PairKey
    candidate_policy_id: str
    produced_epoch_id: int
    installed_revision: int
    result_artifact_id: str
    result_artifact_hash: str
    decision_policy: DecisionPolicy
    model_id: str
    model_revision: str
    prompt_version: str


@dataclass(frozen=True, slots=True)
class _DirectAttemptAuthority:
    attempts: tuple[M4JobAttempt, ...]
    states: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _DirectScopeAuthority:
    scope: M4DiscoveryScope
    epoch_id: int
    candidate_policy_id: str
    scope_kind: str
    closed_revision: int


@dataclass(frozen=True, slots=True)
class _PreparedDocumentOpenSnapshot:
    """Deep immutable self-image of a pre-tier-8 D29 locator authority."""

    snapshot_identity: int
    binding: _D29DocumentOpenBinding
    prepared_identity: int
    event: M5TypedEventPlan
    execution_policy: ApplicationExecutionPolicy
    closure: _DocumentClosure
    source_chunks: tuple[str, ...]
    locator: _D29LocatorAuthority
    located_observation_edges: tuple[M5WithdrawnObservationEdge, ...]
    bootstrap_authority: tuple[tuple[str, _BootstrapObservationAuthority], ...]


@dataclass(slots=True)
class _PreparedDocumentOpen:
    """Single-use cursor-local D29 authority at the pre-tier-8 cut."""

    binding: _D29DocumentOpenBinding
    prepared_identity: int
    event: M5TypedEventPlan
    execution_policy: ApplicationExecutionPolicy
    closure: _DocumentClosure
    source_chunks: tuple[str, ...]
    locator: _D29LocatorAuthority
    located_observation_edges: tuple[M5WithdrawnObservationEdge, ...]
    bootstrap_authority: tuple[tuple[str, _BootstrapObservationAuthority], ...]
    authority_snapshot: _PreparedDocumentOpenSnapshot | None = None
    phase: _DocumentOpenPhase = _DocumentOpenPhase.LOCATORS_GATHERED


@dataclass(frozen=True, slots=True)
class _LockedDocumentOpenResult:
    """Private continuation result; the retained wrapper projects four fields."""

    direct_open: M5DirectOpenPlan
    requirement_withdrawal: M5RequirementWithdrawalPlan
    requirement_roots: tuple[M5RequirementRootDeclaration, ...]
    requirement_root_set_hash: str
    direct_matching_authority: _D29DirectMatchingAuthority


def _prospective_requirement_withdrawal(
    event: M5TypedEventPlan,
    *,
    source_chunks: tuple[str, ...],
    requirement_ids: Iterable[str],
) -> M5RequirementWithdrawalPlan:
    fallback = tuple(
        M5RequirementFallbackKey(requirement_id, event.candidate_policy_id)
        for requirement_id in sorted(set(requirement_ids), key=_c_key)
    )
    return M5RequirementWithdrawalPlan(
        event_id=event.structural_event_id,
        deactivated_chunk_version_ids=source_chunks,
        withdrawn_candidate_pair_digests=(),
        withdrawn_observation_ids=(),
        cancelled_job_ids=(),
        fallback_keys=fallback,
        plan_digest=digests.requirement_withdrawal_plan_digest(
            event_id=event.structural_event_id,
            deactivated_chunk_version_ids=source_chunks,
            withdrawn_candidate_pair_digests=(),
            withdrawn_observation_ids=(),
            cancelled_job_ids=(),
            fallback_keys=(
                (key.requirement_version_id, key.candidate_policy_id)
                for key in fallback
            ),
        ),
    )


def _locate_d29_m5_dependency_edges(
    cursor: Cursor[Any], *, epoch_id: int, parent_job_id: str
) -> tuple[tuple[int, str, str], ...]:
    """Enumerate one M5 dependency prefix through bounded PK successor points."""

    edges: list[tuple[int, str, str]] = []
    previous_child_id = ""
    while True:
        row = cursor.execute(
            """
            SELECT epoch_id, parent_job_id, child_job_id
            FROM groundloop_m5_job_dependency
            WHERE ROW(epoch_id, parent_job_id, child_job_id)
                  > ROW(%s, %s, %s)
            ORDER BY epoch_id, parent_job_id, child_job_id
            LIMIT 1
            """,
            (epoch_id, parent_job_id, previous_child_id),
        ).fetchone()
        if row is None:
            break
        coordinate = (int(str(row[0])), _strip(row[1]), _strip(row[2]))
        if coordinate[:2] != (epoch_id, parent_job_id):
            break
        if _c_key(coordinate[2]) <= _c_key(previous_child_id):
            raise EventConflictError("M5 dependency locator did not advance")
        edges.append(coordinate)
        previous_child_id = coordinate[2]
    return tuple(edges)


def _lock_d29_m5_dependency_edge(
    cursor: Cursor[Any], coordinate: tuple[int, str, str]
) -> None:
    """Lock one exact gathered dependency via a single composite-PK seek."""

    epoch_id, parent_job_id, child_job_id = coordinate
    row = cursor.execute(
        """
        WITH edge_point AS MATERIALIZED (
            SELECT epoch_id, parent_job_id, child_job_id
            FROM groundloop_m5_job_dependency
            WHERE ROW(epoch_id, parent_job_id, child_job_id)
                  >= ROW(%s, %s, %s)
            ORDER BY epoch_id, parent_job_id, child_job_id
            LIMIT 1
            FOR UPDATE
        )
        SELECT epoch_id, parent_job_id, child_job_id
        FROM edge_point
        WHERE epoch_id = %s AND parent_job_id = %s AND child_job_id = %s
        """,
        (
            epoch_id,
            parent_job_id,
            child_job_id,
            epoch_id,
            parent_job_id,
            child_job_id,
        ),
    ).fetchone()
    if (
        row is None
        or (
            int(str(row[0])),
            _strip(row[1]),
            _strip(row[2]),
        )
        != coordinate
    ):
        raise EventConflictError("M5 dependency disappeared before authority lock")


def _require_d30_read_committed(cursor: Cursor[Any]) -> None:
    """Reject unsupported snapshots before the first D30 authority locator."""

    row = cursor.execute("SELECT current_setting('transaction_isolation')").fetchone()
    if row is None or str(row[0]).lower() != "read committed":
        raise EventConflictError("D30 planning requires PostgreSQL READ COMMITTED")


def _d30_observation_row(
    cursor: Cursor[Any], observation_id: str
) -> tuple[object, ...]:
    row = cursor.execute(
        """
        SELECT observation_id, subject_kind::text, subject_id,
               chunk_version_id, task_type, support_score, refute_score,
               neutral_score, model_id, model_version, prompt_version,
               input_hash, produced_epoch, raw_output_hash,
               eligible_for_currency
        FROM groundloop_semantic_observation
        WHERE observation_id = %s
        """,
        (observation_id,),
    ).fetchone()
    if row is None:
        raise EventConflictError("D30 claim observation disappeared during location")
    return tuple(row)


def _probe_d30_predecessor_currency(
    cursor: Cursor[Any],
    currency: _D30CurrencyRow,
    predecessor_epoch_id: int | None,
    *,
    lock_authority: bool = False,
) -> tuple[object, ...] | None:
    """Return at most the latest exact-key interval candidate at a predecessor."""

    if predecessor_epoch_id is None:
        return None
    lock_clause = " FOR UPDATE" if lock_authority else ""
    row = cursor.execute(
        """
        SELECT subject_kind::text, subject_id, chunk_version_id, task_type,
               observation_id, valid_from_epoch, valid_to_epoch
        FROM groundloop_published_observation_currency
        WHERE subject_kind = %s
          AND subject_id = %s
          AND chunk_version_id = %s
          AND task_type = %s
          AND valid_from_epoch <= %s
        ORDER BY valid_from_epoch DESC
        LIMIT 1
        """
        + lock_clause,
        (*currency.full_key, predecessor_epoch_id),
    ).fetchone()
    return None if row is None else tuple(row)


_D30_WORK_COUNTERS = M5RuntimeWork.counter_names()
_D30PointKey = TypeVar("_D30PointKey")
_D30_DIRECT_TRANSITION_COUNTERS = frozenset(
    {
        "direct_observation_artifact_count",
        "direct_effective_observation_count",
        "direct_inactive_completion_count",
        "group_state_write_count",
        "claim_state_write_count",
        "answer_state_write_count",
        "certificate_binding_write_count",
        "bytes_hashed",
        "bytes_serialized",
    }
)
_D30_SEAL_COUNTERS = frozenset(
    {
        "group_state_write_count",
        "claim_state_write_count",
        "answer_state_write_count",
        "certificate_binding_write_count",
        "public_delta_count",
        "bytes_hashed",
        "bytes_serialized",
    }
)


def _d30_work_columns(prefix: str) -> str:
    return ", ".join(f"{prefix}{name}" for name in _D30_WORK_COUNTERS)


def _validate_d30_work_counter_subset(
    work: M5RuntimeWork,
    *,
    allowed: frozenset[str],
    label: str,
) -> None:
    unexpected = tuple(
        name
        for name, value in zip(_D30_WORK_COUNTERS, work.counter_values(), strict=True)
        if value != 0 and name not in allowed
    )
    if unexpected:
        raise EventConflictError(f"D30 {label} work matrix changed")


def _remember_d30_point(
    points: dict[_D30PointKey, tuple[object, ...]],
    key: _D30PointKey,
    expected: tuple[object, ...],
    *,
    label: str,
) -> None:
    previous = points.setdefault(key, expected)
    if previous != expected:
        raise EventConflictError(f"D30 {label} is ambiguous")


def _d30_contribution_point(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    contribution_kind: str,
    source_id: str,
    lock_authority: bool = False,
) -> tuple[object, ...] | None:
    lock_clause = " FOR UPDATE" if lock_authority else ""
    row = cursor.execute(
        f"""
        SELECT epoch_id, {_d30_work_columns("")}, work_digest,
               contribution_kind, source_id, source_identity_hash,
               contribution_key_digest, applied_revision
        FROM groundloop_m5_runtime_work_contribution
        WHERE epoch_id = %s AND contribution_kind = %s AND source_id = %s
        """
        + lock_clause,
        (epoch_id, contribution_kind, source_id),
    ).fetchone()
    return None if row is None else tuple(row)


def _d30_transition_timing_point(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    contribution_kind: str,
    source_id: str,
    anchor_revision: int,
    lock_authority: bool = False,
) -> tuple[object, ...] | None:
    lock_clause = " FOR UPDATE" if lock_authority else ""
    row = cursor.execute(
        """
        SELECT epoch_id, contribution_kind, source_id,
               contribution_key_digest, anchor_revision,
               required_interval_observed,
               coordinator_non_db_non_neural_ns, neural_wall_ns,
               postgres_roundtrip_wall_ns, external_io_wall_ns,
               end_to_end_wall_ns, postgres_server_execution_ns,
               postgres_lock_wait_ns, postgres_wal_bytes,
               postgres_shared_block_reads, observation_digest,
               transition_timing_digest
        FROM groundloop_m5_transition_call_timing
        WHERE epoch_id = %s AND contribution_kind = %s
          AND source_id = %s AND anchor_revision = %s
        """
        + lock_clause,
        (epoch_id, contribution_kind, source_id, anchor_revision),
    ).fetchone()
    return None if row is None else tuple(row)


def _d30_late_attempt_points(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    attempt_id: str,
    lock_authority: bool = False,
) -> tuple[
    tuple[object, ...] | None,
    tuple[object, ...] | None,
    tuple[object, ...] | None,
    tuple[object, ...] | None,
]:
    """Read the four exact D24 late-return points for one direct attempt."""

    lock_clause = " FOR UPDATE" if lock_authority else ""
    envelope = cursor.execute(
        """
        SELECT epoch_id, return_kind, job_id, attempt_id,
               result_artifact_id, btrim(result_artifact_hash),
               verification_execution_present,
               observation_eligible_for_currency,
               requested_make_effective, job_binding, attempt_binding,
               completion_binding, discovery_binding, scope_binding,
               verifier_binding, btrim(job_binding_digest),
               btrim(attempt_binding_digest),
               btrim(completion_binding_digest),
               btrim(discovery_binding_digest), btrim(scope_binding_digest),
               btrim(verifier_binding_digest), btrim(envelope_digest)
        FROM groundloop_m5_typed_direct_late_return_envelope
        WHERE epoch_id = %s AND attempt_id = %s
        """
        + lock_clause,
        (epoch_id, attempt_id),
    ).fetchone()
    expired = cursor.execute(
        """
        SELECT epoch_id, subgraph, attempt_id, logical_job_id,
               btrim(original_lease_token_hash), original_lease_expires_at,
               btrim(worker_output_digest), btrim(worker_artifact_hash),
               activity_snapshot_epoch_id, activity_snapshot_revision,
               cancellation_attribution, archive_reason,
               btrim(execution_evidence_digest), received_after_terminal,
               btrim(expired_return_digest)
        FROM groundloop_m5_expired_attempt_return
        WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
        """
        + lock_clause,
        (epoch_id, attempt_id),
    ).fetchone()
    postterminal_timing = cursor.execute(
        """
        SELECT epoch_id, subgraph, attempt_id, required_interval_observed,
               coordinator_non_db_non_neural_ns, neural_wall_ns,
               postgres_roundtrip_wall_ns, external_io_wall_ns,
               end_to_end_wall_ns, postgres_server_execution_ns,
               postgres_lock_wait_ns, postgres_wal_bytes,
               postgres_shared_block_reads, btrim(observation_digest),
               btrim(attempt_timing_digest)
        FROM groundloop_m5_post_terminal_attempt_timing
        WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
        """
        + lock_clause,
        (epoch_id, attempt_id),
    ).fetchone()
    postterminal_audit = cursor.execute(
        """
        SELECT epoch_id, subgraph, attempt_id, return_kind,
               btrim(return_artifact_digest),
               btrim(execution_evidence_digest), btrim(work_digest),
               btrim(timing_digest), btrim(terminal_logical_result_hash)
        FROM groundloop_m5_post_terminal_attempt_audit
        WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
        """
        + lock_clause,
        (epoch_id, attempt_id),
    ).fetchone()
    return (
        None if envelope is None else tuple(envelope),
        None if expired is None else tuple(expired),
        None if postterminal_timing is None else tuple(postterminal_timing),
        None if postterminal_audit is None else tuple(postterminal_audit),
    )


def _d30_terminal_rows(
    cursor: Cursor[Any],
    *,
    structural_event_id: str,
    epoch_id: int,
    lock_authority: bool = False,
) -> _D30D24TerminalLocator | None:
    """Read the exact immutable terminal-result bytes for one typed owner."""

    lock_clause = " FOR UPDATE" if lock_authority else ""
    result = cursor.execute(
        """
        SELECT structural_event_id, payload_hash, epoch_id, outcome,
               original_open_receipt_binding_hash, publication_id,
               original_publication_receipt_binding_hash, event_work_kind,
               event_work_digest, combined_status_delta_set_hash,
               changed_state_set_hash, failure_reason, logical_result_hash,
               delta_count, state_reference_count,
               coordinator_non_db_non_neural_ns, neural_wall_ns,
               postgres_roundtrip_wall_ns, external_io_wall_ns,
               end_to_end_wall_ns, postgres_server_execution_ns,
               postgres_lock_wait_ns, postgres_wal_bytes,
               postgres_shared_block_reads
        FROM groundloop_m5_event_result
        WHERE structural_event_id = %s AND epoch_id = %s
        """
        + lock_clause,
        (structural_event_id, epoch_id),
    ).fetchone()
    if result is None:
        return None
    work_rows = tuple(
        tuple(row)
        for row in cursor.execute(
            f"""
            SELECT epoch_id, work_digest, {_d30_work_columns("")},
                   structural_event_id, work_kind
            FROM groundloop_m5_runtime_work
            WHERE structural_event_id = %s AND epoch_id = %s
            ORDER BY work_kind COLLATE "C"
            """
            + lock_clause,
            (structural_event_id, epoch_id),
        ).fetchall()
    )
    coverage = cursor.execute(
        """
        SELECT structural_event_id, epoch_id,
               required_expected_count, required_observed_count,
               required_missing_count,
               postgres_server_execution_expected_count,
               postgres_server_execution_observed_count,
               postgres_server_execution_missing_count,
               postgres_lock_wait_expected_count,
               postgres_lock_wait_observed_count,
               postgres_lock_wait_missing_count,
               postgres_wal_bytes_expected_count,
               postgres_wal_bytes_observed_count,
               postgres_wal_bytes_missing_count,
               postgres_shared_block_reads_expected_count,
               postgres_shared_block_reads_observed_count,
               postgres_shared_block_reads_missing_count,
               terminal_client_roundtrip_included
        FROM groundloop_m5_event_timing_coverage
        WHERE structural_event_id = %s AND epoch_id = %s
        """
        + lock_clause,
        (structural_event_id, epoch_id),
    ).fetchone()
    delta_rows = tuple(
        tuple(row)
        for row in cursor.execute(
            """
            SELECT structural_event_id, delta_ordinal, object_type, object_id,
                   old_status, new_status, reason
            FROM groundloop_m5_event_result_delta
            WHERE structural_event_id = %s
            ORDER BY delta_ordinal
            """
            + lock_clause,
            (structural_event_id,),
        ).fetchall()
    )
    reference_rows = tuple(
        tuple(row)
        for row in cursor.execute(
            """
            SELECT structural_event_id, reference_ordinal, kind, object_id,
                   epoch_id, revision, state_artifact_hash, reference_digest
            FROM groundloop_m5_event_result_state_reference
            WHERE structural_event_id = %s
            ORDER BY reference_ordinal
            """
            + lock_clause,
            (structural_event_id,),
        ).fetchall()
    )
    if coverage is None:
        return None
    return _D30D24TerminalLocator(
        event_result_row=tuple(result),
        work_rows=work_rows,
        timing_coverage_row=tuple(coverage),
        delta_rows=delta_rows,
        reference_rows=reference_rows,
    )


def _d30_d24_owner_locator(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    epoch_row: tuple[object, ...],
    jobs: tuple[tuple[object, ...], ...],
    attempts: tuple[tuple[str, tuple[tuple[object, ...], ...]], ...],
) -> _D30D24OwnerLocator:
    """Gather every exact D24 point needed by one typed sealed owner."""

    jobs_by_id = {str(row[0]): row for row in jobs}
    attempt_locators: list[_D30D24AttemptLocator] = []
    dispatch_projection = _d30_work_columns("maximum_")
    evidence_projection = _d30_work_columns("attempt_")
    for job_id, rows in attempts:
        job = jobs_by_id[job_id]
        for attempt in rows:
            attempt_id = str(attempt[0])
            dispatch = cursor.execute(
                f"""
                SELECT epoch_id, {dispatch_projection}, maximum_work_digest,
                       subgraph, attempt_id, logical_job_id, attempt_ordinal,
                       job_kind, fallback_required, dispatched_revision,
                       lease_expires_at, record_digest
                FROM groundloop_m5_dispatch_record
                WHERE epoch_id = %s AND subgraph = 'direct'
                  AND attempt_id = %s
                """,
                (epoch_id, attempt_id),
            ).fetchone()
            if dispatch is None:
                raise EventConflictError("D30 typed attempt lacks its dispatch")
            dispatch_row = tuple(dispatch)
            dispatch_digest = _strip(dispatch_row[len(_D30_WORK_COUNTERS) + 10])
            evidence = cursor.execute(
                f"""
                SELECT epoch_id, {evidence_projection}, attempt_work_digest,
                       subgraph, attempt_id, disposition,
                       result_or_error_hash, attempt_timing_digest,
                       evidence_digest
                FROM groundloop_m5_attempt_execution_evidence
                WHERE epoch_id = %s AND subgraph = 'direct'
                  AND attempt_id = %s
                """,
                (epoch_id, attempt_id),
            ).fetchone()
            evidence_row = None if evidence is None else tuple(evidence)
            timing = cursor.execute(
                """
                SELECT epoch_id, subgraph, attempt_id,
                       execution_evidence_digest, required_interval_observed,
                       coordinator_non_db_non_neural_ns, neural_wall_ns,
                       postgres_roundtrip_wall_ns, external_io_wall_ns,
                       end_to_end_wall_ns, postgres_server_execution_ns,
                       postgres_lock_wait_ns, postgres_wal_bytes,
                       postgres_shared_block_reads, observation_digest,
                       attempt_timing_digest
                FROM groundloop_m5_runtime_timing_contribution
                WHERE epoch_id = %s AND subgraph = 'direct'
                  AND attempt_id = %s
                """,
                (epoch_id, attempt_id),
            ).fetchone()
            (
                late_envelope,
                expired_return,
                postterminal_timing,
                postterminal_audit,
            ) = _d30_late_attempt_points(
                cursor,
                epoch_id=epoch_id,
                attempt_id=attempt_id,
            )
            acquisition = _d30_contribution_point(
                cursor,
                epoch_id=epoch_id,
                contribution_kind=(
                    M5RuntimeWorkContributionKind.DIRECT_ACQUISITION.value
                ),
                source_id=dispatch_digest,
            )
            if acquisition is None:
                raise EventConflictError(
                    "D30 typed dispatch lacks acquisition contribution"
                )
            attempt_contribution = (
                None
                if evidence_row is None
                else _d30_contribution_point(
                    cursor,
                    epoch_id=epoch_id,
                    contribution_kind=(
                        M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION.value
                    ),
                    source_id=attempt_id,
                )
            )
            preterminal_late_contribution = _d30_contribution_point(
                cursor,
                epoch_id=epoch_id,
                contribution_kind=(
                    M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN.value
                ),
                source_id=attempt_id,
            )
            transition_contribution: tuple[object, ...] | None = None
            m4_transition: tuple[object, ...] | None = None
            timing_rows: list[tuple[object, ...]] = []
            acquisition_timing = _d30_transition_timing_point(
                cursor,
                epoch_id=epoch_id,
                contribution_kind=(
                    M5RuntimeWorkContributionKind.DIRECT_ACQUISITION.value
                ),
                source_id=dispatch_digest,
                anchor_revision=int(str(dispatch_row[len(_D30_WORK_COUNTERS) + 8])),
            )
            if acquisition_timing is not None:
                timing_rows.append(acquisition_timing)
            if str(attempt[5]) == "completed":
                transition_id = _strip(job[13])
                transition_contribution = _d30_contribution_point(
                    cursor,
                    epoch_id=epoch_id,
                    contribution_kind=(
                        M5RuntimeWorkContributionKind.DIRECT_TRANSITION.value
                    ),
                    source_id=transition_id,
                )
                transition = cursor.execute(
                    """
                    SELECT epoch_id, transition_id, payload_hash,
                           transition_kind, from_revision, to_revision,
                           override_rows_written
                    FROM groundloop_m4_evaluation_counter_transition
                    WHERE epoch_id = %s AND transition_id = %s
                    """,
                    (epoch_id, transition_id),
                ).fetchone()
                m4_transition = None if transition is None else tuple(transition)
                if transition_contribution is not None:
                    transition_timing = _d30_transition_timing_point(
                        cursor,
                        epoch_id=epoch_id,
                        contribution_kind=(
                            M5RuntimeWorkContributionKind.DIRECT_TRANSITION.value
                        ),
                        source_id=transition_id,
                        anchor_revision=int(str(job[17])),
                    )
                    if transition_timing is not None:
                        timing_rows.append(transition_timing)
            elif (
                str(attempt[5]) == "failed"
                and evidence_row is not None
                and attempt_contribution is not None
            ):
                attempt_timing = _d30_transition_timing_point(
                    cursor,
                    epoch_id=epoch_id,
                    contribution_kind=(
                        M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION.value
                    ),
                    source_id=attempt_id,
                    anchor_revision=int(
                        str(attempt_contribution[len(_D30_WORK_COUNTERS) + 6])
                    ),
                )
                if attempt_timing is not None:
                    timing_rows.append(attempt_timing)
            elif (
                str(attempt[5]) == "expired"
                and evidence_row is not None
                and expired_return is not None
                and expired_return[13] is False
            ):
                late_timing = _d30_transition_timing_point(
                    cursor,
                    epoch_id=epoch_id,
                    contribution_kind=(
                        M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN.value
                    ),
                    source_id=attempt_id,
                    anchor_revision=int(str(expired_return[9])),
                )
                if late_timing is not None:
                    timing_rows.append(late_timing)
            attempt_locators.append(
                _D30D24AttemptLocator(
                    attempt_id=attempt_id,
                    job_id=job_id,
                    attempt_state=str(attempt[5]),
                    dispatch_row=dispatch_row,
                    evidence_row=evidence_row,
                    timing_row=None if timing is None else tuple(timing),
                    acquisition_contribution_row=acquisition,
                    attempt_contribution_row=attempt_contribution,
                    transition_contribution_row=transition_contribution,
                    transition_timing_rows=tuple(timing_rows),
                    m4_transition_row=m4_transition,
                    late_envelope_row=late_envelope,
                    expired_return_row=expired_return,
                    postterminal_timing_row=postterminal_timing,
                    postterminal_audit_row=postterminal_audit,
                    preterminal_late_contribution_row=(preterminal_late_contribution),
                )
            )

    work_accumulator = cursor.execute(
        f"""
        SELECT epoch_id, {_d30_work_columns("")}, work_digest,
               updated_revision, terminalized
        FROM groundloop_m5_runtime_work_accumulator WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    timing_accumulator = cursor.execute(
        """
        SELECT epoch_id,
               coordinator_non_db_non_neural_ns, neural_wall_ns,
               postgres_roundtrip_wall_ns, external_io_wall_ns,
               end_to_end_wall_ns, postgres_server_execution_ns,
               postgres_lock_wait_ns, postgres_wal_bytes,
               postgres_shared_block_reads,
               required_expected_count, required_observed_count,
               required_missing_count,
               postgres_server_execution_expected_count,
               postgres_server_execution_observed_count,
               postgres_server_execution_missing_count,
               postgres_lock_wait_expected_count,
               postgres_lock_wait_observed_count,
               postgres_lock_wait_missing_count,
               postgres_wal_bytes_expected_count,
               postgres_wal_bytes_observed_count,
               postgres_wal_bytes_missing_count,
               postgres_shared_block_reads_expected_count,
               postgres_shared_block_reads_observed_count,
               postgres_shared_block_reads_missing_count,
               pending_contribution_kind, pending_source_id,
               pending_contribution_key_digest, pending_anchor_revision,
               updated_revision, terminalized
        FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    terminal_rows = _d30_terminal_rows(
        cursor,
        structural_event_id=str(epoch_row[1]),
        epoch_id=epoch_id,
    )
    terminal_result = _load_canonical_terminal_result(
        cursor,
        structural_event_id=str(epoch_row[1]),
        payload_hash=_strip(epoch_row[2]),
    )
    terminal_call_work = _load_terminal_work(
        cursor,
        structural_event_id=str(epoch_row[1]),
        epoch_id=epoch_id,
        work_kind="call",
    )
    if (
        work_accumulator is None
        or timing_accumulator is None
        or terminal_rows is None
        or terminal_result is None
        or terminal_call_work is None
        or terminal_result.replayed_outcome is not M5ReplayedOutcome.SEALED
    ):
        raise EventConflictError("D30 typed owner terminal closure is incomplete")
    seal = _d30_contribution_point(
        cursor,
        epoch_id=epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.SEAL.value,
        source_id=str(epoch_row[1]),
    )
    if seal is None:
        raise EventConflictError("D30 typed owner lacks its seal contribution")
    return _D30D24OwnerLocator(
        attempts=tuple(attempt_locators),
        work_accumulator_row=tuple(work_accumulator),
        timing_accumulator_row=tuple(timing_accumulator),
        seal_contribution_row=seal,
        terminal_rows=terminal_rows,
        terminal_call_work=terminal_call_work,
        terminal_result=terminal_result,
    )


def _d30_owner_locator(cursor: Cursor[Any], epoch_id: int) -> _D30OwnerLocator:
    """Gather one complete direct-owner epoch before any D30 authority lock."""

    epoch = cursor.execute(
        """
        SELECT epoch_id, event_id, payload_hash, revision, structural_status,
               semantic_status, evaluation_state, publication_mode, sealed_at
        FROM groundloop_epoch WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    update = cursor.execute(
        """
        SELECT epoch_id, update_kind, candidate_policy_id,
               previous_published_epoch_id, registry_snapshot_id, manifest
        FROM groundloop_m4_update WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    if epoch is None or update is None:
        raise EventConflictError("D30 dynamic owner header is incomplete")
    jobs = tuple(
        tuple(row)
        for row in cursor.execute(
            """
            SELECT job_id, epoch_id, parent_job_id, job_kind,
                   candidate_policy_id, payload_hash, execution_spec_hash,
                   claim_id, chunk_version_id, expandable, job_state,
                   child_closed, child_set_hash, completion_digest,
                   result_artifact_id, result_artifact_hash,
                   created_revision, completed_revision, created_at, completed_at
            FROM groundloop_semantic_job
            WHERE epoch_id = %s
            ORDER BY job_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )
    dependencies = tuple(
        tuple(row)
        for row in cursor.execute(
            """
            SELECT epoch_id, parent_job_id, child_job_id
            FROM groundloop_semantic_job_dependency
            WHERE epoch_id = %s
            ORDER BY parent_job_id COLLATE "C", child_job_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )
    scopes: list[tuple[str, tuple[object, ...] | None]] = []
    attempts: list[tuple[str, tuple[tuple[object, ...], ...]]] = []
    discovery_results: list[tuple[str, tuple[object, ...] | None]] = []
    projections: list[tuple[str, tuple[object, ...] | None]] = []
    for job in jobs:
        job_id = str(job[0])
        scope = cursor.execute(
            """
            SELECT root_job_id, epoch_id, registry_snapshot_id, scope_kind,
                   explicit_claim_ids, closed_revision
            FROM groundloop_discovery_scope WHERE root_job_id = %s
            """,
            (job_id,),
        ).fetchone()
        scopes.append((job_id, None if scope is None else tuple(scope)))
        attempt_rows = cursor.execute(
            """
            SELECT attempt_id, job_id, execution_spec_hash, attempt_ordinal,
                   lease_token_hash, attempt_state, lease_expires_at,
                   started_at, finished_at
            FROM groundloop_semantic_job_attempt
            WHERE job_id = %s
            ORDER BY attempt_ordinal
            """,
            (job_id,),
        ).fetchall()
        attempts.append((job_id, tuple(tuple(row) for row in attempt_rows)))
        result = cursor.execute(
            """
            SELECT root_job_id, epoch_id, result_artifact_id,
                   result_artifact_hash, fallback_satisfied,
                   channel_hit_count, admitted_pair_count,
                   channel_set_hash, admitted_pair_set_hash
            FROM groundloop_m4_discovery_result WHERE root_job_id = %s
            """,
            (job_id,),
        ).fetchone()
        discovery_results.append((job_id, None if result is None else tuple(result)))
        projection = cursor.execute(
            """
            SELECT epoch_id, job_id, terminal_state, terminal_reason,
                   m4_completion_digest, completed_revision,
                   terminal_identity_hash
            FROM groundloop_m5_direct_terminal_projection
            WHERE epoch_id = %s AND job_id = %s
            """,
            (epoch_id, job_id),
        ).fetchone()
        projections.append((job_id, None if projection is None else tuple(projection)))
    runtime = cursor.execute(
        """
        SELECT epoch_id, structural_event_id, candidate_policy_id,
               candidate_policy_manifest_hash,
               requirement_registry_snapshot_digest,
               active_chunk_snapshot_digest,
               expected_previous_published_epoch_id,
               requirement_root_set_hash, runtime_state, revision,
               open_work_count, open_scope_count, blocking_failure_count,
               terminal_at
        FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    d24 = (
        None
        if runtime is None
        else _d30_d24_owner_locator(
            cursor,
            epoch_id=epoch_id,
            epoch_row=tuple(epoch),
            jobs=jobs,
            attempts=tuple(attempts),
        )
    )
    return _D30OwnerLocator(
        epoch_id=epoch_id,
        epoch_row=tuple(epoch),
        update_row=tuple(update),
        runtime_row=None if runtime is None else tuple(runtime),
        jobs=jobs,
        dependencies=dependencies,
        scopes=tuple(scopes),
        attempts=tuple(attempts),
        discovery_results=tuple(discovery_results),
        projections=tuple(projections),
        d24=d24,
    )


_D30_M4_EXECUTION_SELECT = """
    SELECT observation_id, job_id, btrim(admitted_pair_id),
           model_artifact_id, prompt_artifact_id,
           btrim(execution_spec_hash), btrim(pair_input_hash),
           calibration_version, btrim(calibration_artifact_sha256),
           temperature, raw_logits, btrim(raw_output_hash),
           reused_from_observation_id
    FROM groundloop_m4_verification_execution
"""


def _d30_execution_coordinates(
    cursor: Cursor[Any],
    *,
    observation_id: str,
    job_id: str,
    admitted_pair_id: str,
    lock_authority: bool = False,
) -> tuple[
    tuple[object, ...] | None,
    tuple[object, ...] | None,
    tuple[object, ...] | None,
]:
    lock_clause = " FOR UPDATE" if lock_authority else ""
    rows: list[tuple[object, ...] | None] = []
    for column, value in (
        ("observation_id", observation_id),
        ("job_id", job_id),
        ("admitted_pair_id", admitted_pair_id),
    ):
        row = cursor.execute(
            _D30_M4_EXECUTION_SELECT + f" WHERE {column} = %s" + lock_clause,
            (value,),
        ).fetchone()
        rows.append(None if row is None else tuple(row))
    return (rows[0], rows[1], rows[2])


def _validate_d30_locked_execution_coordinates(
    *,
    preliminary_rows: tuple[
        tuple[object, ...] | None,
        tuple[object, ...] | None,
        tuple[object, ...] | None,
    ],
    locked_rows: tuple[
        tuple[object, ...] | None,
        tuple[object, ...] | None,
        tuple[object, ...] | None,
    ],
) -> None:
    """Revalidate every selected execution byte after authority is locked."""

    if locked_rows != preliminary_rows:
        raise EventConflictError("D30 optional execution coordinates changed")
    present = tuple(row for row in locked_rows if row is not None)
    if present and (len(present) != 3 or not all(row == present[0] for row in present)):
        raise EventConflictError("D30 optional execution coordinates disagree")


def _d30_m3_image_point(
    cursor: Cursor[Any],
    *,
    image_epoch_id: int,
    claim_id: str,
    chunk_version_id: str,
    lock_registry: bool = False,
) -> tuple[object, ...] | None:
    """Read one exact activation/predecessor claim-and-chunk image point."""

    lock_clause = (
        " FOR KEY SHARE OF update_row, snapshot, member, claim_row"
        if lock_registry
        else ""
    )
    row = cursor.execute(
        """
        SELECT update_row.epoch_id, update_row.registry_snapshot_id,
               snapshot.claim_registry_snapshot_id, snapshot.claim_count,
               btrim(snapshot.claim_set_hash),
               member.claim_registry_snapshot_id, member.claim_id,
               member.member_ordinal,
               claim_row.claim_id, claim_row.answer_version_id, claim_row.text,
               claim_row.extractor_model_id,
               claim_row.extractor_model_version,
               claim_row.extractor_prompt_version, claim_row.required,
               chunk.chunk_version_id, chunk.document_version_id,
               chunk.chunk_index, chunk.text, btrim(chunk.text_hash),
               chunk.chunker_version, chunk.valid_from_epoch,
               chunk.valid_to_epoch,
               version.document_version_id, version.document_id,
               version.content_hash, version.valid_from_epoch,
               version.valid_to_epoch
        FROM groundloop_m4_update AS update_row
        JOIN groundloop_m4_claim_registry_snapshot AS snapshot
          ON snapshot.claim_registry_snapshot_id = update_row.registry_snapshot_id
        JOIN groundloop_m4_claim_registry_member AS member
          ON member.claim_registry_snapshot_id = update_row.registry_snapshot_id
         AND member.claim_id = %s
        JOIN groundloop_claim AS claim_row ON claim_row.claim_id = member.claim_id
        JOIN groundloop_chunk_version AS chunk
          ON chunk.chunk_version_id = %s
        JOIN groundloop_document_version AS version
          ON version.document_version_id = chunk.document_version_id
        WHERE update_row.epoch_id = %s
        """
        + lock_clause,
        (claim_id, chunk_version_id, image_epoch_id),
    ).fetchone()
    return None if row is None else tuple(row)


def _lock_d30_m3_image_membership(
    cursor: Cursor[Any],
    locator: _D29LocatorAuthority,
) -> None:
    """Lock every preliminary M3 base/head claim-image point exactly once."""

    points: dict[tuple[int, str, str], tuple[object, ...]] = {}
    try:
        for claim in locator.d30_claims.bootstrap:
            for expected in claim.image_rows:
                coordinate = (
                    int(str(expected[0])),
                    str(expected[6]),
                    str(expected[15]),
                )
                _remember_d30_point(
                    points,
                    coordinate,
                    expected,
                    label="activation-base M3 image coordinate",
                )
    except (IndexError, TypeError, ValueError) as error:
        raise EventConflictError(
            "D30 activation-base M3 image coordinate is malformed"
        ) from error

    for image_epoch_id, claim_id, chunk_version_id in sorted(
        points,
        key=lambda item: (item[0], _c_key(item[1]), _c_key(item[2])),
    ):
        locked = _d30_m3_image_point(
            cursor,
            image_epoch_id=image_epoch_id,
            claim_id=claim_id,
            chunk_version_id=chunk_version_id,
            lock_registry=True,
        )
        if (
            locked is None
            or locked != points[(image_epoch_id, claim_id, chunk_version_id)]
        ):
            raise EventConflictError("D30 activation-base M3 image changed")


def _d30_m3_claim_locator(
    cursor: Cursor[Any],
    *,
    currency: _D30CurrencyRow,
    observation_row: tuple[object, ...],
) -> _D30M3ClaimLocator:
    observation_id = currency.observation_id
    execution = cursor.execute(
        """
        SELECT observation_id, run_id, candidate_id, model_artifact_id,
               prompt_artifact_id, calibration_version, temperature,
               raw_logits, raw_output_hash, reused_from_observation_id
        FROM groundloop_verification_execution
        WHERE observation_id = %s
        """,
        (observation_id,),
    ).fetchone()
    if execution is None:
        raise EventConflictError("D30 activation-base claim lacks M3 execution")
    run = cursor.execute(
        """
        SELECT run_id, schema_version, status, config_hash, input_hash,
               corpus_hash, question_id, answer_version_id,
               semantic_epoch_id, manifest, failure_code, started_at,
               completed_at
        FROM groundloop_pipeline_run WHERE run_id = %s
        """,
        (str(execution[1]),),
    ).fetchone()
    candidate = cursor.execute(
        """
        SELECT candidate_id, run_id, query_kind, query_id, claim_id,
               chunk_version_id, embedding_model_artifact_id,
               method_version, score, rank
        FROM groundloop_retrieval_candidate WHERE candidate_id = %s
        """,
        (str(execution[2]),),
    ).fetchone()
    epoch = cursor.execute(
        """
        SELECT epoch_id, event_id, payload_hash, revision, structural_status,
               semantic_status, evaluation_state, publication_mode, sealed_at
        FROM groundloop_epoch WHERE epoch_id = %s
        """,
        (int(str(observation_row[12])),),
    ).fetchone()
    if run is None or candidate is None or epoch is None:
        raise EventConflictError("D30 activation-base M3 closure is incomplete")

    def model_point(artifact_id: str) -> tuple[object, ...]:
        row = cursor.execute(
            """
            SELECT model_artifact_id, task, provider, model_id,
                   immutable_revision, tokenizer_revision, license_id,
                   config_hash, artifact_sha256
            FROM groundloop_model_artifact WHERE model_artifact_id = %s
            """,
            (artifact_id,),
        ).fetchone()
        if row is None:
            raise EventConflictError("D30 activation-base model point is absent")
        return tuple(row)

    model = model_point(str(execution[3]))
    embedding_model = model_point(str(candidate[6]))
    prompt = cursor.execute(
        """
        SELECT prompt_artifact_id, task, version, template, template_hash,
               decoding_config_hash
        FROM groundloop_prompt_artifact WHERE prompt_artifact_id = %s
        """,
        (str(execution[4]),),
    ).fetchone()
    if prompt is None:
        raise EventConflictError("D30 activation-base prompt point is absent")
    chunk_embedding = cursor.execute(
        """
        SELECT chunk_version_id, model_artifact_id, embedding::text,
               input_hash
        FROM groundloop_chunk_embedding
        WHERE chunk_version_id = %s AND model_artifact_id = %s
        """,
        (currency.chunk_version_id, str(candidate[6])),
    ).fetchone()
    if chunk_embedding is None:
        raise EventConflictError("D30 activation-base chunk embedding is absent")
    image = cursor.execute(
        """
        SELECT activation.base_m4_epoch_id, m4_head.epoch_id, m5_head.epoch_id
        FROM groundloop_m5_activation AS activation
        JOIN groundloop_m4_publication_head AS m4_head ON m4_head.singleton
        JOIN groundloop_m5_publication_head AS m5_head ON m5_head.singleton
        WHERE activation.singleton
        """
    ).fetchone()
    if image is None or int(str(image[1])) != int(str(image[2])):
        raise EventConflictError("D30 activation-base claim/chunk image changed")
    image_epochs = tuple(dict.fromkeys((int(str(image[0])), int(str(image[1])))))
    image_rows = tuple(
        _d30_m3_image_point(
            cursor,
            image_epoch_id=image_epoch_id,
            claim_id=currency.subject_id,
            chunk_version_id=currency.chunk_version_id,
        )
        for image_epoch_id in image_epochs
    )
    if any(row is None for row in image_rows):
        raise EventConflictError("D30 activation-base claim/chunk image changed")
    embedding_use_id = f"embedding:{currency.chunk_version_id}:{str(candidate[6])}"
    artifact_coordinates = tuple(
        sorted(
            {
                ("model", str(execution[3])),
                ("prompt", str(execution[4])),
                ("model", str(candidate[6])),
                ("embedding", embedding_use_id),
                ("retrieval", str(candidate[0])),
                ("verification", observation_id),
            },
            key=lambda item: (_c_key(item[0]), _c_key(item[1])),
        )
    )
    artifact_uses: list[tuple[object, ...]] = []
    for artifact_kind, artifact_id in artifact_coordinates:
        row = cursor.execute(
            """
            SELECT run_id, artifact_kind, artifact_id, reused
            FROM groundloop_pipeline_artifact_use
            WHERE run_id = %s AND artifact_kind = %s AND artifact_id = %s
            """,
            (str(execution[1]), artifact_kind, artifact_id),
        ).fetchone()
        if row is None:
            raise EventConflictError("D30 activation-base artifact use is absent")
        artifact_uses.append(tuple(row))
    return _D30M3ClaimLocator(
        currency=currency,
        observation_row=observation_row,
        epoch_row=tuple(epoch),
        execution_row=tuple(execution),
        run_row=tuple(run),
        candidate_row=tuple(candidate),
        model_row=model,
        prompt_row=tuple(prompt),
        embedding_model_row=embedding_model,
        chunk_embedding_row=tuple(chunk_embedding),
        artifact_use_rows=tuple(artifact_uses),
        image_rows=tuple(cast(tuple[object, ...], row) for row in image_rows),
    )


def _gather_d30_claim_authority(
    cursor: Cursor[Any],
    currency_rows: Iterable[_D30CurrencyRow],
) -> _D30ClaimLocatorAuthority:
    """Classify claim holders only through positive D30 authority."""

    ordered_currency = tuple(
        sorted(
            currency_rows,
            key=lambda row: (
                _c_key(row.subject_kind),
                _c_key(row.subject_id),
                _c_key(row.chunk_version_id),
                _c_key(row.task_type),
                _c_key(row.observation_id),
            ),
        )
    )
    activation: tuple[object, ...] | None = None
    if ordered_currency:
        raw_activation = cursor.execute(
            """
            SELECT activation.activation_id, activation.payload_hash,
                   activation.base_m4_epoch_id, activation.activated_at,
                   mode.mode, mode.mode_revision,
                   m4_head.epoch_id, m5_head.epoch_id,
                   base.revision, base.structural_status,
                   base.semantic_status, base.evaluation_state,
                   base.publication_mode, base.sealed_at
            FROM groundloop_m5_activation AS activation
            JOIN groundloop_runtime_mode AS mode ON mode.singleton
            JOIN groundloop_m4_publication_head AS m4_head ON m4_head.singleton
            JOIN groundloop_m5_publication_head AS m5_head ON m5_head.singleton
            JOIN groundloop_epoch AS base
              ON base.epoch_id = activation.base_m4_epoch_id
            WHERE activation.singleton
            """
        ).fetchone()
        if raw_activation is None:
            raise EventConflictError("D30 claim authority lacks activation")
        activation = tuple(raw_activation)
    bootstrap: list[_D30M3ClaimLocator] = []
    pending_dynamic: list[
        tuple[
            _D30CurrencyRow,
            tuple[object, ...],
            tuple[object, ...],
            int | None,
            tuple[object, ...] | None,
            int,
        ]
    ] = []
    source_epochs: set[int] = set()
    for currency in ordered_currency:
        if currency.subject_kind != "claim":
            raise EventConflictError("D30 claim locator received another subject kind")
        observation = _d30_observation_row(cursor, currency.observation_id)
        if currency.installed_revision == 0:
            bootstrap.append(
                _d30_m3_claim_locator(
                    cursor,
                    currency=currency,
                    observation_row=observation,
                )
            )
            continue
        source_epoch = int(str(observation[12]))
        delta = cursor.execute(
            """
            SELECT epoch_id, subject_kind::text, subject_id,
                   chunk_version_id, task_type, base_observation_id,
                   working_observation_id, installed_revision
            FROM groundloop_working_observation_delta
            WHERE epoch_id = %s AND subject_kind = %s
              AND subject_id = %s AND chunk_version_id = %s
              AND task_type = %s
            """,
            (source_epoch, *currency.full_key),
        ).fetchone()
        update = cursor.execute(
            """
            SELECT previous_published_epoch_id
            FROM groundloop_m4_update WHERE epoch_id = %s
            """,
            (source_epoch,),
        ).fetchone()
        if delta is None or update is None:
            raise EventConflictError("D30 dynamic claim lacks its working delta")
        previous_epoch = None if update[0] is None else int(str(update[0]))
        predecessor = _probe_d30_predecessor_currency(cursor, currency, previous_epoch)
        pending_dynamic.append(
            (
                currency,
                observation,
                tuple(delta),
                previous_epoch,
                predecessor,
                source_epoch,
            )
        )
        source_epochs.add(source_epoch)

    owners = tuple(
        _d30_owner_locator(cursor, epoch_id) for epoch_id in sorted(source_epochs)
    )
    owner_by_epoch = {owner.epoch_id: owner for owner in owners}
    dynamic: list[_D30DynamicClaimLocator] = []
    for (
        currency,
        observation,
        delta,
        previous_epoch,
        predecessor,
        source_epoch,
    ) in pending_dynamic:
        owner = owner_by_epoch[source_epoch]
        delta_installed_revision = delta[7]
        if type(delta_installed_revision) is not int:
            raise EventConflictError("D30 dynamic delta revision is malformed")
        revision_matches = tuple(
            row
            for row in owner.jobs
            if row[17] is not None
            and type(row[17]) is int
            and int(row[17]) == delta_installed_revision
        )
        if len(revision_matches) != 1:
            raise EventConflictError(
                "D30 dynamic revision does not select exactly one owner child"
            )
        child = revision_matches[0]
        if (
            str(child[3]) != M4JobKind.VERIFY_PAIR.value
            or str(child[10]) != M4JobState.COMPLETED_ACTIVE.value
            or child[2] is None
            or str(child[7]) != currency.subject_id
            or str(child[8]) != currency.chunk_version_id
        ):
            raise EventConflictError("D30 dynamic revision selected another job")
        child_job_id = str(child[0])
        parent_job_id = str(child[2])
        admitted_pair_id = stable_m4_digest(
            "m4-admitted-pair-v1",
            str(source_epoch),
            currency.subject_id,
            currency.chunk_version_id,
            str(child[4]),
        )
        admitted = cursor.execute(
            """
            SELECT admitted_pair_id, epoch_id, claim_id, chunk_version_id,
                   candidate_policy_id, fused_rank, reasons,
                   mandatory_lineage
            FROM groundloop_admitted_pair WHERE admitted_pair_id = %s
            """,
            (admitted_pair_id,),
        ).fetchone()
        result = dict(owner.discovery_results).get(parent_job_id)
        if admitted is None or result is None:
            raise EventConflictError("D30 dynamic child closure is incomplete")
        executions = _d30_execution_coordinates(
            cursor,
            observation_id=currency.observation_id,
            job_id=child_job_id,
            admitted_pair_id=admitted_pair_id,
        )
        present = tuple(row for row in executions if row is not None)
        if present and (
            len(present) != 3 or not all(row == present[0] for row in present)
        ):
            raise EventConflictError("D30 optional execution coordinates disagree")
        model: tuple[object, ...] | None = None
        prompt: tuple[object, ...] | None = None
        if present:
            execution = present[0]
            model_row = cursor.execute(
                """
                SELECT model_artifact_id, task, provider, model_id,
                       immutable_revision, tokenizer_revision, license_id,
                       config_hash, artifact_sha256
                FROM groundloop_model_artifact WHERE model_artifact_id = %s
                """,
                (str(execution[3]),),
            ).fetchone()
            prompt_row = cursor.execute(
                """
                SELECT prompt_artifact_id, task, version, template,
                       template_hash, decoding_config_hash
                FROM groundloop_prompt_artifact WHERE prompt_artifact_id = %s
                """,
                (str(execution[4]),),
            ).fetchone()
            model = None if model_row is None else tuple(model_row)
            prompt = None if prompt_row is None else tuple(prompt_row)
        dynamic.append(
            _D30DynamicClaimLocator(
                currency=currency,
                observation_row=observation,
                delta_row=delta,
                predecessor_epoch_id=previous_epoch,
                predecessor_candidate=predecessor,
                owner_epoch_id=source_epoch,
                child_job_id=child_job_id,
                parent_job_id=parent_job_id,
                admitted_pair_id=admitted_pair_id,
                admitted_pair_row=tuple(admitted),
                parent_result_row=result,
                execution_rows=executions,
                model_row=model,
                prompt_row=prompt,
            )
        )
    return _D30ClaimLocatorAuthority(
        currency_rows=ordered_currency,
        dynamic=tuple(dynamic),
        bootstrap=tuple(bootstrap),
        owners=owners,
        activation_row=activation,
    )


def _semantic_observation_from_row(
    row: tuple[object, ...], *, label: str
) -> SemanticObservation:
    """Construct the immutable semantic object from one exact point row."""

    if len(row) != 15:
        raise EventConflictError(f"{label} has the wrong row width")
    try:
        return SemanticObservation(
            observation_id=str(row[0]),
            subject_kind=SubjectKind(str(row[1])),
            subject_id=str(row[2]),
            chunk_version_id=str(row[3]),
            task_type=str(row[4]),
            support_score=float(str(row[5])),
            refute_score=float(str(row[6])),
            neutral_score=float(str(row[7])),
            producer=ModelStamp(str(row[8]), str(row[9]), str(row[10])),
            input_hash=_exact_digest(f"{label} input", row[11]),
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError(f"{label} bytes are malformed") from error


def _direct_claim_status(*, supported: bool, refuted: bool) -> str:
    if supported and refuted:
        return "conflicted"
    if supported:
        return "supported"
    if refuted:
        return "refuted"
    return "unsupported"


def _direct_answer_status(
    *,
    required_claim_count: int,
    supported_count: int,
    refuted_count: int,
    conflicted_count: int,
) -> str:
    if refuted_count > 0:
        return "contradicted"
    if conflicted_count > 0:
        return "conflicted"
    if required_claim_count > 0 and supported_count == required_claim_count:
        return "valid"
    if supported_count > 0:
        return "partially_supported"
    return "unsupported"


def _gather_d29_direct_state_locators(
    cursor: Cursor[Any],
    d30_claims: _D30ClaimLocatorAuthority,
    *,
    predecessor_epoch_id: int,
) -> tuple[
    tuple[_D29DirectClaimBeforeImage, ...],
    tuple[_D29DirectAnswerBeforeImage, ...],
    tuple[tuple[object, ...], ...],
    tuple[tuple[str, str, str, str], ...],
    tuple[_D30CurrencyRow, ...],
]:
    """Gather bounded direct state and every remaining current-currency point."""

    withdrawn_by_claim: dict[str, set[str]] = {}
    preliminary_withdrawn_rows = {
        item.currency.observation_id: item.observation_row
        for item in d30_claims.dynamic
    }
    preliminary_withdrawn_rows.update(
        {
            item.currency.observation_id: item.observation_row
            for item in d30_claims.bootstrap
        }
    )
    for currency in d30_claims.currency_rows:
        withdrawn_by_claim.setdefault(currency.subject_id, set()).add(
            currency.observation_id
        )
    if set(preliminary_withdrawn_rows) != {
        row.observation_id for row in d30_claims.currency_rows
    }:
        raise EventConflictError("D29 direct current-observation locators changed")

    claim_images: list[_D29DirectClaimBeforeImage] = []
    answer_images: dict[str, _D29DirectAnswerBeforeImage] = {}
    remaining_rows: dict[str, tuple[object, ...]] = {}
    for claim_id in sorted(withdrawn_by_claim, key=_c_key):
        owners = cursor.execute(
            """
            SELECT claim_id, answer_version_id, required
            FROM groundloop_claim
            WHERE claim_id = %s
            """,
            (claim_id,),
        ).fetchall()
        states = cursor.execute(
            """
            SELECT support_count, refute_count, best_support_score,
                   best_refute_score, supporting_observation_ids,
                   refuting_observation_ids, status, certificate_digest,
                   valid_from_epoch
            FROM groundloop_published_claim_state
            WHERE claim_id = %s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            """,
            (claim_id, predecessor_epoch_id, predecessor_epoch_id),
        ).fetchall()
        materialized_rows = cursor.execute(
            """
            SELECT support_count, refute_count, best_support_score,
                   best_refute_score, supporting_observation_ids,
                   refuting_observation_ids, status, updated_epoch,
                   updated_revision
            FROM groundloop_claim_state_materialized
            WHERE claim_id = %s
            """,
            (claim_id,),
        ).fetchall()
        certificate_rows = cursor.execute(
            """
            SELECT support_observation_id, refute_observation_id,
                   repaired_epoch, repaired_revision
            FROM groundloop_claim_certificate
            WHERE claim_id = %s
            """,
            (claim_id,),
        ).fetchall()
        if (
            len(owners) != 1
            or len(states) != 1
            or len(materialized_rows) != 1
            or len(certificate_rows) != 1
        ):
            raise EventConflictError(
                "D29 direct claim materialized certificate before image is not unique"
            )
        owner = owners[0]
        state = states[0]
        materialized = materialized_rows[0]
        certificate = certificate_rows[0]
        if str(owner[0]) != claim_id or type(owner[2]) is not bool:
            raise EventConflictError("D29 direct claim ownership changed")
        support_values = state[4]
        refute_values = state[5]
        if not isinstance(support_values, (list, tuple)) or not isinstance(
            refute_values, (list, tuple)
        ):
            raise EventConflictError("D29 direct claim provenance is malformed")
        supporting_ids = tuple(str(value) for value in support_values)
        refuting_ids = tuple(str(value) for value in refute_values)
        materialized_support_values = materialized[4]
        materialized_refute_values = materialized[5]
        if (
            supporting_ids != tuple(sorted(set(supporting_ids), key=_c_key))
            or refuting_ids != tuple(sorted(set(refuting_ids), key=_c_key))
            or set(supporting_ids) & set(refuting_ids)
            or type(state[0]) is not int
            or type(state[1]) is not int
            or int(state[0]) < 0
            or int(state[1]) < 0
            or type(state[8]) is not int
            or int(state[8]) <= 0
            or not isinstance(materialized_support_values, (list, tuple))
            or not isinstance(materialized_refute_values, (list, tuple))
            or type(materialized[0]) is not int
            or type(materialized[1]) is not int
            or type(materialized[7]) is not int
            or type(materialized[8]) is not int
            or type(certificate[2]) is not int
            or type(certificate[3]) is not int
        ):
            raise EventConflictError("D29 direct claim provenance changed")
        withdrawn_ids = tuple(sorted(withdrawn_by_claim[claim_id], key=_c_key))
        remaining_support = tuple(
            item for item in supporting_ids if item not in withdrawn_by_claim[claim_id]
        )
        remaining_refute = tuple(
            item for item in refuting_ids if item not in withdrawn_by_claim[claim_id]
        )
        named_rows: dict[str, tuple[object, ...]] = {}
        for observation_id in sorted(
            set(supporting_ids) | set(refuting_ids), key=_c_key
        ):
            row = preliminary_withdrawn_rows.get(observation_id)
            if row is None:
                row = _d30_observation_row(cursor, observation_id)
                existing = remaining_rows.get(observation_id)
                if existing is not None and existing != row:
                    raise EventConflictError(
                        "D29 remaining direct observation locator changed"
                    )
                remaining_rows[observation_id] = row
            if (
                str(row[0]) != observation_id
                or str(row[1]) != "claim"
                or str(row[2]) != claim_id
                or row[14] is not True
            ):
                raise EventConflictError(
                    "D29 direct claim observation ownership changed"
                )
            named_rows[observation_id] = row
        support_best = (
            None
            if not supporting_ids
            else max(float(str(named_rows[item][5])) for item in supporting_ids)
        )
        refute_best = (
            None
            if not refuting_ids
            else max(float(str(named_rows[item][6])) for item in refuting_ids)
        )
        best_support = None if state[2] is None else float(str(state[2]))
        best_refute = None if state[3] is None else float(str(state[3]))
        expected_status = _direct_claim_status(
            supported=int(state[0]) > 0, refuted=int(state[1]) > 0
        )
        expected_certificate = stable_m4_digest(
            "m4-claim-certificate-v1",
            claim_id,
            supporting_ids[0] if supporting_ids else "",
            refuting_ids[0] if refuting_ids else "",
        )
        if (
            best_support != support_best
            or best_refute != refute_best
            or str(state[6]) != expected_status
            or _strip(state[7]) != expected_certificate
        ):
            raise EventConflictError("D29 direct claim state is inconsistent")
        materialized_supporting_ids = tuple(
            str(value) for value in materialized_support_values
        )
        materialized_refuting_ids = tuple(
            str(value) for value in materialized_refute_values
        )
        materialized_best_support = (
            None if materialized[2] is None else float(str(materialized[2]))
        )
        materialized_best_refute = (
            None if materialized[3] is None else float(str(materialized[3]))
        )
        certificate_support_id = None if certificate[0] is None else str(certificate[0])
        certificate_refute_id = None if certificate[1] is None else str(certificate[1])
        expected_certificate_support = supporting_ids[0] if supporting_ids else None
        expected_certificate_refute = refuting_ids[0] if refuting_ids else None
        if (
            int(materialized[0]) != int(state[0])
            or int(materialized[1]) != int(state[1])
            or materialized_best_support != best_support
            or materialized_best_refute != best_refute
            or materialized_supporting_ids != supporting_ids
            or materialized_refuting_ids != refuting_ids
            or str(materialized[6]) != str(state[6])
            or int(materialized[7]) != int(state[8])
            or int(materialized[8]) < 0
            or certificate_support_id != expected_certificate_support
            or certificate_refute_id != expected_certificate_refute
            or int(certificate[2]) != int(materialized[7])
            or int(certificate[3]) != int(materialized[8])
        ):
            raise EventConflictError(
                "D29 direct claim materialized certificate closure changed"
            )
        answer_id = str(owner[1])
        claim_images.append(
            _D29DirectClaimBeforeImage(
                claim_id=claim_id,
                answer_version_id=answer_id,
                required=owner[2],
                support_count=int(state[0]),
                refute_count=int(state[1]),
                best_support_score=best_support,
                best_refute_score=best_refute,
                supporting_observation_ids=supporting_ids,
                refuting_observation_ids=refuting_ids,
                status=str(state[6]),
                certificate_digest=_strip(state[7]),
                state_valid_from_epoch=int(state[8]),
                materialized_updated_epoch=int(materialized[7]),
                materialized_updated_revision=int(materialized[8]),
                certificate_support_observation_id=certificate_support_id,
                certificate_refute_observation_id=certificate_refute_id,
                certificate_repaired_epoch=int(certificate[2]),
                certificate_repaired_revision=int(certificate[3]),
                withdrawn_observation_ids=withdrawn_ids,
                remaining_support_observation_ids=remaining_support,
                remaining_refute_observation_ids=remaining_refute,
            )
        )
        if answer_id in answer_images:
            continue
        answers = cursor.execute(
            """
            SELECT required_claim_count, supported_count, unsupported_count,
                   refuted_count, conflicted_count, status
            FROM groundloop_published_answer_state
            WHERE answer_version_id = %s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            """,
            (answer_id, predecessor_epoch_id, predecessor_epoch_id),
        ).fetchall()
        if len(answers) != 1 or any(
            type(answers[0][index]) is not int for index in range(5)
        ):
            raise EventConflictError("D29 direct answer before image is not unique")
        answer = answers[0]
        (
            required_count,
            supported_count,
            unsupported_count,
            refuted_count,
            conflicted_count,
        ) = (int(answer[index]) for index in range(5))
        if (
            min(
                required_count,
                supported_count,
                unsupported_count,
                refuted_count,
                conflicted_count,
            )
            < 0
            or (
                supported_count + unsupported_count + refuted_count + conflicted_count
                != required_count
            )
            or str(answer[5])
            != _direct_answer_status(
                required_claim_count=required_count,
                supported_count=supported_count,
                refuted_count=refuted_count,
                conflicted_count=conflicted_count,
            )
        ):
            raise EventConflictError("D29 direct answer state is inconsistent")
        answer_images[answer_id] = _D29DirectAnswerBeforeImage(
            answer_version_id=answer_id,
            required_claim_count=required_count,
            supported_count=supported_count,
            unsupported_count=unsupported_count,
            refuted_count=refuted_count,
            conflicted_count=conflicted_count,
            status=str(answer[5]),
        )

    remaining_currency_rows: dict[str, _D30CurrencyRow] = {}
    for observation_id, observation in sorted(
        remaining_rows.items(), key=lambda item: _c_key(item[0])
    ):
        key = tuple(str(value) for value in observation[1:5])
        current = cursor.execute(
            """
            SELECT subject_kind::text, subject_id, chunk_version_id,
                   task_type, observation_id, installed_revision
            FROM groundloop_observation_currency
            WHERE subject_kind = %s AND subject_id = %s
              AND chunk_version_id = %s AND task_type = %s
            """,
            key,
        ).fetchone()
        if (
            current is None
            or tuple(str(value) for value in current[:5]) != (*key, observation_id)
            or type(current[5]) is not int
            or int(current[5]) < 0
        ):
            raise EventConflictError("D29 remaining direct observation is not current")
        remaining_currency_rows[observation_id] = _D30CurrencyRow(
            subject_kind=key[0],
            subject_id=key[1],
            chunk_version_id=key[2],
            task_type=key[3],
            observation_id=observation_id,
            installed_revision=int(current[5]),
        )

    all_observation_rows = {**preliminary_withdrawn_rows, **remaining_rows}
    source_by_chunk: dict[str, tuple[str, str, str]] = {}
    for chunk_id in sorted(
        {str(row[3]) for row in all_observation_rows.values()}, key=_c_key
    ):
        row = cursor.execute(
            """
            SELECT chunk.chunk_version_id, chunk.text, chunk.text_hash,
                   chunk.document_version_id, chunk.valid_from_epoch,
                   chunk.valid_to_epoch, version.document_version_id,
                   version.valid_from_epoch, version.valid_to_epoch
            FROM groundloop_chunk_version AS chunk
            JOIN groundloop_document_version AS version
              ON version.document_version_id = chunk.document_version_id
            WHERE chunk.chunk_version_id = %s
            """,
            (chunk_id,),
        ).fetchone()
        if (
            row is None
            or str(row[0]) != chunk_id
            or _strip(row[2]) != normalized_text_hash(str(row[1]))
            or str(row[3]) != str(row[6])
            or int(str(row[4])) > predecessor_epoch_id
            or (row[5] is not None and predecessor_epoch_id >= int(str(row[5])))
            or int(str(row[7])) > predecessor_epoch_id
            or (row[8] is not None and predecessor_epoch_id >= int(str(row[8])))
        ):
            raise EventConflictError("D29 direct observation source is inactive")
        source_by_chunk[chunk_id] = (chunk_id, str(row[1]), _strip(row[2]))
    observation_source_rows = tuple(
        (
            observation_id,
            source_by_chunk[str(row[3])][0],
            source_by_chunk[str(row[3])][1],
            source_by_chunk[str(row[3])][2],
        )
        for observation_id, row in sorted(
            all_observation_rows.items(), key=lambda item: _c_key(item[0])
        )
    )
    text_hash_by_observation = {
        observation_id: text_hash
        for observation_id, _chunk_id, _text, text_hash in observation_source_rows
    }
    for claim in claim_images:
        if claim.support_count != len(
            {
                text_hash_by_observation[observation_id]
                for observation_id in claim.supporting_observation_ids
            }
        ) or claim.refute_count != len(
            {
                text_hash_by_observation[observation_id]
                for observation_id in claim.refuting_observation_ids
            }
        ):
            raise EventConflictError("D29 direct claim distinct-evidence count changed")

    return (
        tuple(claim_images),
        tuple(answer_images[key] for key in sorted(answer_images, key=_c_key)),
        tuple(remaining_rows[key] for key in sorted(remaining_rows, key=_c_key)),
        observation_source_rows,
        tuple(
            remaining_currency_rows[key]
            for key in sorted(remaining_currency_rows, key=_c_key)
        ),
    )


def _gather_d29_locator_authority(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    closure: _DocumentClosure,
    *,
    execution_policy: ApplicationExecutionPolicy,
    source_chunks: tuple[str, ...],
) -> _D29LocatorAuthority:
    """Gather nonauthoritative locator keys before any tier-8/9 acquisition."""

    requirement_ids: set[str] = set()
    prospective_requirement_ids: set[str] = set()
    admitted_locator_keys: set[tuple[str, str]] = set()
    qualifying_admitted_pair_digests: set[str] = set()
    candidate_roots: set[str] = set()
    candidate_root_coordinates: set[tuple[int, str]] = set()
    candidate_dependencies: set[tuple[int, str, str]] = set()
    direct_jobs: set[str] = set()
    direct_job_coordinates: set[tuple[int, str]] = set()
    direct_dependencies: set[tuple[int, str, str]] = set()
    direct_admitted_pair_ids: set[str] = set()
    direct_scopes: set[str] = set()
    direct_verifier_observations: set[str] = set()
    verifier_jobs: set[str] = set()
    verifier_roots: set[str] = set()
    verifier_observations: set[str] = set()
    verifier_dependencies: set[tuple[int, str, str]] = set()
    verifier_artifact_ids: set[str] = set()
    verifier_pair_input_hashes: set[str] = set()
    bootstrap_observations: set[tuple[str, int]] = set()
    direct_bootstrap_observations: set[tuple[str, int]] = set()
    direct_claim_ids: set[str] = set()
    requirement_currency_keys: set[tuple[str, str, str, str]] = set()
    direct_currency_keys: set[tuple[str, str, str, str]] = set()
    direct_frontier_keys: set[tuple[str, str, str, int]] = set()
    d30_claim_currency_rows: list[_D30CurrencyRow] = []
    snapshots = _predecessor_snapshot_ids(cursor, closure.previous_epoch_id)
    for chunk_id in source_chunks:
        digest_rows = cursor.execute(
            """
            SELECT chunk_version_id, admitted_pair_digest
            FROM groundloop_m5_requirement_admitted_pair
            WHERE chunk_version_id COLLATE "C" = %s
            ORDER BY admitted_pair_digest COLLATE "C"
            """,
            (chunk_id,),
        ).fetchall()
        for located_chunk, admitted_digest in digest_rows:
            if str(located_chunk) != chunk_id:
                raise EventConflictError("admitted-pair locator escaped its chunk")
            admitted_digest_text = _strip(admitted_digest)
            admitted_locator_keys.add((chunk_id, admitted_digest_text))
            admitted = cursor.execute(
                """
                SELECT epoch_id, subject_kind::text, subject_id,
                       chunk_version_id, semantic_pair_digest,
                       candidate_policy_id, owner_root_job_id,
                       reasons, mandatory_lineage
                FROM groundloop_m5_requirement_admitted_pair
                WHERE admitted_pair_digest = %s
                """,
                (admitted_digest_text,),
            ).fetchone()
            if admitted is None:
                raise EventConflictError("admitted-pair locator lost its point row")
            owner_epoch_id, requirement_id = (
                _candidate_locator_classification_coordinates(tuple(admitted))
            )
            prospective_requirement_ids.add(requirement_id)
            if not _is_sealed_lineage_owner(
                cursor, owner_epoch_id, closure.previous_epoch_id
            ) or not _require_active_membership(
                cursor,
                predecessor_epoch_id=closure.previous_epoch_id,
                snapshots=snapshots,
                requirement_version_id=requirement_id,
                chunk_version_id=chunk_id,
            ):
                continue
            validated_owner, validated_requirement = (
                _validate_candidate_locator_admitted_row(
                    located_chunk=chunk_id,
                    admitted_digest=admitted_digest_text,
                    row=tuple(admitted),
                )
            )
            if (
                validated_owner != owner_epoch_id
                or validated_requirement != requirement_id
            ):
                raise EventConflictError("admitted-pair locator coordinates changed")
            requirement_ids.add(requirement_id)
            qualifying_admitted_pair_digests.add(admitted_digest_text)
            owner_root_id = _strip(admitted[6])
            candidate_roots.add(owner_root_id)
            candidate_root_coordinates.add((owner_epoch_id, owner_root_id))
            source_rows = cursor.execute(
                """
                SELECT root_job_id
                FROM groundloop_m5_requirement_admitted_pair_source
                WHERE admitted_pair_digest = %s
                ORDER BY root_job_id COLLATE "C"
                """,
                (admitted_digest_text,),
            ).fetchall()
            source_root_ids = tuple(_strip(row[0]) for row in source_rows)
            candidate_roots.update(source_root_ids)
            candidate_root_coordinates.update(
                (owner_epoch_id, root_id) for root_id in source_root_ids
            )
            for source_row in source_rows:
                source_root_id = _strip(source_row[0])
                dependency_rows = _locate_d29_m5_dependency_edges(
                    cursor,
                    epoch_id=owner_epoch_id,
                    parent_job_id=source_root_id,
                )
                for dependency_row in dependency_rows:
                    child_id = dependency_row[2]
                    verifier_jobs.add(child_id)
                    candidate_dependencies.add(dependency_row)
        current_rows = cursor.execute(
            """
            SELECT subject_kind::text, subject_id, chunk_version_id,
                   task_type, observation_id, installed_revision
            FROM groundloop_observation_currency
            WHERE chunk_version_id = %s
            ORDER BY subject_kind::text COLLATE "C",
                     subject_id COLLATE "C", chunk_version_id COLLATE "C",
                     task_type COLLATE "C",
                     observation_id COLLATE "C"
            """,
            (chunk_id,),
        ).fetchall()
        for raw_currency in current_rows:
            subject_kind = str(raw_currency[0])
            subject_id = str(raw_currency[1])
            located_chunk_id = str(raw_currency[2])
            task_type = str(raw_currency[3])
            observation_id = str(raw_currency[4])
            installed_revision = raw_currency[5]
            if located_chunk_id != chunk_id or type(installed_revision) is not int:
                raise EventConflictError("total current-currency locator changed")
            currency = _D30CurrencyRow(
                subject_kind=subject_kind,
                subject_id=subject_id,
                chunk_version_id=located_chunk_id,
                task_type=task_type,
                observation_id=observation_id,
                installed_revision=installed_revision,
            )
            if subject_kind == "claim":
                claim_id = subject_id
                observation_text = observation_id
                direct_claim_ids.add(claim_id)
                direct_currency_keys.add(
                    (claim_id, chunk_id, task_type, observation_text)
                )
                d30_claim_currency_rows.append(currency)
                continue
            if subject_kind != "requirement":
                raise EventConflictError("unsupported current observation subject kind")
            requirement_id = subject_id
            requirement_text = str(requirement_id)
            prospective_requirement_ids.add(requirement_text)
            if not _require_active_membership(
                cursor,
                predecessor_epoch_id=closure.previous_epoch_id,
                snapshots=snapshots,
                requirement_version_id=requirement_text,
                chunk_version_id=chunk_id,
            ):
                raise EventConflictError(
                    "current observation is inactive at predecessor"
                )
            requirement_ids.add(requirement_text)
            observation_text = str(observation_id)
            requirement_currency_keys.add(
                (requirement_text, chunk_id, task_type, observation_text)
            )
            executions = cursor.execute(
                """
                SELECT execution.logical_job_id, job.parent_job_id,
                       job.epoch_id, execution.artifact_id,
                       execution.pair_input_hash
                FROM groundloop_m5_requirement_verifier_execution AS execution
                JOIN groundloop_m5_semantic_job AS job
                  ON job.logical_job_id = execution.logical_job_id
                WHERE execution.observation_id = %s
                """,
                (observation_text,),
            ).fetchall()
            if len(executions) > 1:
                raise EventConflictError(
                    "observation locator has ambiguous verifier provenance"
                )
            if executions:
                verifier_observations.add(observation_text)
                verifier_job_id = _strip(executions[0][0])
                verifier_jobs.add(verifier_job_id)
                if executions[0][1] is None:
                    raise EventConflictError("verifier provenance has no root job")
                verifier_root_id = _strip(executions[0][1])
                verifier_roots.add(verifier_root_id)
                verifier_dependencies.add(
                    (int(str(executions[0][2])), verifier_root_id, verifier_job_id)
                )
                verifier_artifact_ids.add(_strip(executions[0][3]))
                verifier_pair_input_hashes.add(_strip(executions[0][4]))
            else:
                produced = cursor.execute(
                    """
                    SELECT produced_epoch
                    FROM groundloop_semantic_observation
                    WHERE observation_id = %s
                    """,
                    (observation_text,),
                ).fetchone()
                if produced is None:
                    raise EventConflictError(
                        "bootstrap observation locator lost its source row"
                    )
                bootstrap_observations.add((observation_text, int(str(produced[0]))))
        direct_candidate_rows = cursor.execute(
            """
            SELECT claim_id, candidate_policy_id, valid_from_epoch
            FROM groundloop_candidate_frontier
            WHERE chunk_version_id = %s
              AND valid_to_epoch IS NULL
            ORDER BY claim_id COLLATE "C", candidate_policy_id COLLATE "C",
                     valid_from_epoch
            """,
            (chunk_id,),
        ).fetchall()
        for row in direct_candidate_rows:
            claim_id = str(row[0])
            direct_claim_ids.add(claim_id)
            direct_frontier_keys.add((claim_id, chunk_id, str(row[1]), int(row[2])))
    withdrawn_d30_claims = _gather_d30_claim_authority(cursor, d30_claim_currency_rows)
    (
        direct_claim_before_images,
        direct_answer_before_images,
        direct_remaining_observation_rows,
        direct_observation_source_rows,
        direct_remaining_currency_rows,
    ) = _gather_d29_direct_state_locators(
        cursor,
        withdrawn_d30_claims,
        predecessor_epoch_id=closure.previous_epoch_id,
    )
    d30_claims = withdrawn_d30_claims
    if direct_remaining_currency_rows:
        all_direct_currency_rows = tuple(
            sorted(
                (*d30_claim_currency_rows, *direct_remaining_currency_rows),
                key=lambda row: (
                    _c_key(row.subject_kind),
                    _c_key(row.subject_id),
                    _c_key(row.chunk_version_id),
                    _c_key(row.task_type),
                    _c_key(row.observation_id),
                ),
            )
        )
        if len({row.full_key for row in all_direct_currency_rows}) != len(
            all_direct_currency_rows
        ) or len({row.observation_id for row in all_direct_currency_rows}) != len(
            all_direct_currency_rows
        ):
            raise EventConflictError("D29 direct state currency locators overlap")
        d30_claims = _gather_d30_claim_authority(cursor, all_direct_currency_rows)
        gathered_observations = {
            item.currency.observation_id: item.observation_row
            for item in d30_claims.dynamic
        }
        gathered_observations.update(
            {
                item.currency.observation_id: item.observation_row
                for item in d30_claims.bootstrap
            }
        )
        expected_observations = {
            item.currency.observation_id: item.observation_row
            for item in withdrawn_d30_claims.dynamic
        }
        expected_observations.update(
            {
                item.currency.observation_id: item.observation_row
                for item in withdrawn_d30_claims.bootstrap
            }
        )
        expected_observations.update(
            {str(row[0]): row for row in direct_remaining_observation_rows}
        )
        if gathered_observations != expected_observations:
            raise EventConflictError("D29 direct observation locators changed")
    for owner in d30_claims.owners:
        for row in owner.jobs:
            job_id = str(row[0])
            direct_jobs.add(job_id)
            direct_job_coordinates.add((owner.epoch_id, job_id))
            if row[2] is None and str(row[3]) == M4JobKind.IMPACT_DISCOVERY.value:
                direct_scopes.add(job_id)
        direct_dependencies.update(
            (int(str(row[0])), str(row[1]), str(row[2])) for row in owner.dependencies
        )
    for claim in d30_claims.dynamic:
        direct_admitted_pair_ids.add(claim.admitted_pair_id)
        if claim.execution_rows[0] is not None:
            direct_verifier_observations.add(claim.currency.observation_id)
    verifier_root_coordinates = {
        (epoch_id, parent_id)
        for epoch_id, parent_id, _child_id in verifier_dependencies
    }
    for owner_epoch_id, root_id in sorted(
        verifier_root_coordinates, key=lambda item: (item[0], _c_key(item[1]))
    ):
        dependency_rows = _locate_d29_m5_dependency_edges(
            cursor, epoch_id=owner_epoch_id, parent_job_id=root_id
        )
        for dependency_row in dependency_rows:
            child_id = dependency_row[2]
            verifier_jobs.add(child_id)
            verifier_dependencies.add(dependency_row)
    prospective_requirement = _prospective_requirement_withdrawal(
        event,
        source_chunks=source_chunks,
        requirement_ids=prospective_requirement_ids,
    )
    prospective_roots = _document_requirement_roots(
        event, closure.candidate_policy, prospective_requirement
    )
    prospective_direct_withdrawal = StructuralWithdrawal(
        WithdrawalPlan(
            deactivated_chunk_ids=source_chunks,
            observation_ids=(),
            candidate_edge_ids=(),
            affected_pairs=(),
            affected_claim_ids=(),
            chunk_lookups=len(source_chunks),
            observation_edge_visits=0,
            candidate_edge_visits=0,
        ),
        tuple(sorted(direct_claim_ids, key=_c_key)),
    )
    prospective_direct = _direct_open_from_withdrawal(
        event, execution_policy, prospective_direct_withdrawal
    )
    return _D29LocatorAuthority(
        predecessor_snapshots=snapshots,
        admitted_locator_keys=tuple(
            sorted(
                admitted_locator_keys,
                key=lambda item: (_c_key(item[0]), _c_key(item[1])),
            )
        ),
        qualifying_admitted_pair_digests=tuple(
            sorted(qualifying_admitted_pair_digests, key=_c_key)
        ),
        candidate_root_job_ids=tuple(sorted(candidate_roots, key=_c_key)),
        candidate_root_job_coordinates=tuple(
            sorted(
                candidate_root_coordinates,
                key=lambda item: (item[0], _c_key(item[1])),
            )
        ),
        candidate_dependency_coordinates=tuple(
            sorted(
                candidate_dependencies,
                key=lambda item: (item[0], _c_key(item[1]), _c_key(item[2])),
            )
        ),
        direct_job_ids=tuple(sorted(direct_jobs, key=_c_key)),
        direct_job_coordinates=tuple(
            sorted(
                direct_job_coordinates,
                key=lambda item: (item[0], _c_key(item[1])),
            )
        ),
        direct_dependency_coordinates=tuple(
            sorted(
                direct_dependencies,
                key=lambda item: (item[0], _c_key(item[1]), _c_key(item[2])),
            )
        ),
        direct_admitted_pair_ids=tuple(sorted(direct_admitted_pair_ids, key=_c_key)),
        direct_scope_root_job_ids=tuple(sorted(direct_scopes, key=_c_key)),
        direct_verifier_observation_ids=tuple(
            sorted(direct_verifier_observations, key=_c_key)
        ),
        verifier_job_ids=tuple(sorted(verifier_jobs, key=_c_key)),
        verifier_root_job_ids=tuple(sorted(verifier_roots, key=_c_key)),
        verifier_observation_ids=tuple(sorted(verifier_observations, key=_c_key)),
        verifier_dependency_coordinates=tuple(
            sorted(
                verifier_dependencies,
                key=lambda item: (item[0], _c_key(item[1]), _c_key(item[2])),
            )
        ),
        verifier_artifact_ids=tuple(sorted(verifier_artifact_ids, key=_c_key)),
        verifier_pair_input_hashes=tuple(
            sorted(verifier_pair_input_hashes, key=_c_key)
        ),
        bootstrap_observation_coordinates=tuple(
            sorted(
                bootstrap_observations,
                key=lambda item: (_c_key(item[0]), item[1]),
            )
        ),
        direct_bootstrap_observation_coordinates=tuple(
            sorted(
                direct_bootstrap_observations,
                key=lambda item: (_c_key(item[0]), item[1]),
            )
        ),
        requirement_currency_keys=tuple(
            sorted(
                requirement_currency_keys,
                key=lambda item: tuple(_c_key(value) for value in item),
            )
        ),
        direct_currency_keys=tuple(
            sorted(
                direct_currency_keys,
                key=lambda item: (
                    _c_key(item[0]),
                    _c_key(item[1]),
                    _c_key(item[2]),
                    _c_key(item[3]),
                ),
            )
        ),
        direct_frontier_keys=tuple(
            sorted(
                direct_frontier_keys,
                key=lambda item: (
                    _c_key(item[0]),
                    _c_key(item[1]),
                    _c_key(item[2]),
                    item[3],
                ),
            )
        ),
        touched_requirement_ids=tuple(sorted(requirement_ids, key=_c_key)),
        prospective_coordinates=_document_declaration_coordinates(
            prospective_direct, prospective_roots
        ),
        d30_claims=d30_claims,
        direct_claim_before_images=direct_claim_before_images,
        direct_answer_before_images=direct_answer_before_images,
        direct_remaining_observation_rows=direct_remaining_observation_rows,
        direct_observation_source_rows=direct_observation_source_rows,
    )


def _reservation_key(kind: str, object_id: str) -> int:
    value = hashlib.sha256(f"m5-d29:{kind}:{object_id}".encode()).digest()
    key = int.from_bytes(value[:4], "big", signed=False)
    return key if key < 2**31 else key - 2**32


def _lock_d29_bootstrap_provenance(
    cursor: Cursor[Any],
    locator: _D29LocatorAuthority,
    *,
    predecessor_epoch_id: int,
) -> dict[str, _BootstrapObservationAuthority]:
    """Bind the exact immutable activation image before tier-8 acquisition."""

    # Claim bootstrap provenance is the positive frozen-M3 closure introduced
    # by D30.  This retained D29 helper remains requirement-only and therefore
    # must not infer claim provenance from the absence of typed/M4 rows.
    coordinates = locator.bootstrap_observation_coordinates
    if not coordinates:
        return {}
    activation = cursor.execute(
        """
        SELECT activation.activation_id, activation.payload_hash,
               activation.base_m4_epoch_id, activation.activated_at,
               base.revision, base.structural_status, base.semantic_status,
               base.evaluation_state, base.publication_mode, base.sealed_at
        FROM groundloop_m5_activation AS activation
        JOIN groundloop_epoch AS base
          ON base.epoch_id = activation.base_m4_epoch_id
        WHERE activation.singleton
        """
    ).fetchone()
    if (
        activation is None
        or not str(activation[0]).strip()
        or len(_strip(activation[1])) != 64
        or int(str(activation[2])) > predecessor_epoch_id
        or type(activation[4]) is not int
        or int(activation[4]) < 0
        or tuple(activation[5:9]) != ("committed", "sealed", "complete", "strict")
        or activation[9] is None
    ):
        raise EventConflictError("activation bootstrap authority is malformed")
    base_epoch_id = int(str(activation[2]))
    base_revision = int(activation[4])
    authority: dict[str, _BootstrapObservationAuthority] = {}
    for observation_id, produced_epoch_id in coordinates:
        typed = cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
            ), EXISTS (
                SELECT 1 FROM groundloop_m5_update WHERE epoch_id = %s
            ), EXISTS (
                SELECT 1
                FROM groundloop_m5_requirement_verifier_execution
                WHERE observation_id = %s
            ), EXISTS (
                SELECT 1
                FROM groundloop_m4_verification_execution
                WHERE observation_id = %s
            )
            """,
            (
                produced_epoch_id,
                produced_epoch_id,
                observation_id,
                observation_id,
            ),
        ).fetchone()
        if typed != (False, False, False, False) or produced_epoch_id > base_epoch_id:
            raise EventConflictError(
                "bootstrap observation has typed or post-activation provenance"
            )
        requirement_key = next(
            (
                key
                for key in locator.requirement_currency_keys
                if key[3] == observation_id
            ),
            None,
        )
        member_active = requirement_key is not None and _require_active_membership(
            cursor,
            predecessor_epoch_id=base_epoch_id,
            snapshots=None,
            requirement_version_id=requirement_key[0],
            chunk_version_id=requirement_key[1],
        )
        if not member_active:
            raise EventConflictError(
                "bootstrap observation is absent from the activation image"
            )
        authority[observation_id] = _BootstrapObservationAuthority(
            base_epoch_id, base_revision
        )
    if len(authority) != len(coordinates):
        raise EventConflictError("bootstrap observation locator is ambiguous")
    return authority


def _reserve_d29_coordinate(
    cursor: Cursor[Any], *, namespace: int, kind: str, object_id: str
) -> None:
    cursor.execute(
        "SELECT pg_advisory_xact_lock(%s, %s)",
        (namespace, _reservation_key(kind, object_id)),
    )


def _lock_d29_touched_membership(
    cursor: Cursor[Any],
    locator: _D29LocatorAuthority,
    *,
    predecessor_epoch_id: int,
    snapshots: tuple[str, str] | None,
    source_chunks: tuple[str, ...],
) -> None:
    """Point-lock only touched predecessor members before tier-8 scopes."""

    if snapshots is not None:
        registry_digest, chunk_digest = snapshots
        for requirement_id in locator.touched_requirement_ids:
            row = cursor.execute(
                """
                WITH member_point AS MATERIALIZED (
                    SELECT member.requirement_registry_snapshot_digest,
                           member.member_ordinal,
                           member.requirement_version_id,
                           member.group_version_id, member.group_family_id,
                           member.owner_claim_id,
                           member.normalized_requirement_text,
                           member.requirement_text_hash
                    FROM groundloop_m5_requirement_registry_snapshot_member AS member
                    WHERE ROW(
                        member.requirement_registry_snapshot_digest,
                        member.requirement_version_id
                    ) >= ROW(%s, %s)
                    ORDER BY member.requirement_registry_snapshot_digest,
                             member.requirement_version_id
                    LIMIT 1
                    FOR KEY SHARE OF member
                )
                SELECT member.requirement_registry_snapshot_digest,
                       member.member_ordinal,
                       member.requirement_version_id,
                       member.group_version_id, member.group_family_id,
                       member.owner_claim_id,
                       member.normalized_requirement_text,
                       member.requirement_text_hash,
                       requirement.group_version_id,
                       requirement.lifecycle_state,
                       requirement.requirement_text,
                       requirement.requirement_text_hash,
                       group_version.group_family_id,
                       group_version.lifecycle_state,
                       group_version.semantic_structure_hash,
                       family.claim_id, family.lifecycle_state,
                       validity.group_family_id, validity.claim_id,
                       validity.semantic_structure_hash,
                       validity.valid_from_epoch, validity.valid_to_epoch
                FROM member_point AS member
                JOIN groundloop_m5_requirement_version AS requirement
                  ON requirement.requirement_version_id =
                     member.requirement_version_id
                JOIN groundloop_m5_group_version AS group_version
                  ON group_version.group_version_id = member.group_version_id
                JOIN groundloop_m5_group_family AS family
                  ON family.group_family_id = member.group_family_id
                JOIN groundloop_m5_group_validity AS validity
                  ON validity.group_version_id = member.group_version_id
                WHERE member.requirement_registry_snapshot_digest = %s
                  AND member.requirement_version_id = %s
                FOR KEY SHARE OF requirement, group_version, family, validity
                """,
                (registry_digest, requirement_id, registry_digest, requirement_id),
            ).fetchone()
            if (
                row is None
                or _strip(row[0]) != registry_digest
                or type(row[1]) is not int
                or int(row[1]) < 0
                or str(row[2]) != requirement_id
                or str(row[3]) != str(row[8])
                or str(row[4]) != str(row[12])
                or str(row[4]) != str(row[17])
                or str(row[5]) != str(row[15])
                or str(row[5]) != str(row[18])
                or str(row[6]) != str(row[10])
                or str(row[6]) != normalize_text_v1(str(row[6]))
                or _strip(row[7]) != normalized_text_hash_v1(str(row[6]))
                or _strip(row[7]) != _strip(row[11])
                or str(row[9]) != "PUBLISHED"
                or str(row[13]) != "PUBLISHED"
                or str(row[16]) != "PUBLISHED"
                or _strip(row[14]) != _strip(row[19])
                or int(str(row[20])) > predecessor_epoch_id
                or (row[21] is not None and predecessor_epoch_id >= int(str(row[21])))
            ):
                raise EventConflictError(
                    "touched requirement snapshot authority changed"
                )
        for chunk_id in source_chunks:
            row = cursor.execute(
                """
                WITH member_point AS MATERIALIZED (
                    SELECT member.active_chunk_snapshot_digest,
                           member.member_ordinal, member.chunk_version_id,
                           member.text_hash
                    FROM groundloop_m5_active_chunk_snapshot_member AS member
                    WHERE ROW(
                        member.active_chunk_snapshot_digest,
                        member.chunk_version_id
                    ) >= ROW(%s, %s)
                    ORDER BY member.active_chunk_snapshot_digest,
                             member.chunk_version_id
                    LIMIT 1
                    FOR KEY SHARE OF member
                )
                SELECT member.active_chunk_snapshot_digest,
                       member.member_ordinal, member.chunk_version_id,
                       member.text_hash,
                       chunk.chunk_version_id, chunk.document_version_id,
                       chunk.chunk_index, chunk.text, chunk.text_hash,
                       chunk.chunker_version,
                       chunk.valid_from_epoch, chunk.valid_to_epoch,
                       version.document_version_id,
                       version.valid_from_epoch, version.valid_to_epoch,
                       creator.structural_status, creator.semantic_status,
                       creator.evaluation_state, creator.publication_mode,
                       creator.sealed_at
                FROM member_point AS member
                JOIN groundloop_chunk_version AS chunk
                  ON chunk.chunk_version_id = member.chunk_version_id
                JOIN groundloop_document_version AS version
                  ON version.document_version_id = chunk.document_version_id
                JOIN groundloop_epoch AS creator
                  ON creator.epoch_id = chunk.valid_from_epoch
                WHERE member.active_chunk_snapshot_digest = %s
                  AND member.chunk_version_id = %s
                FOR KEY SHARE OF chunk, version
                """,
                (chunk_digest, chunk_id, chunk_digest, chunk_id),
            ).fetchone()
            try:
                chunk = (
                    None
                    if row is None
                    else ChunkVersion(
                        chunk_version_id=str(row[4]),
                        document_version_id=str(row[5]),
                        chunk_index=int(str(row[6])),
                        text=str(row[7]),
                        text_hash=_strip(row[8]),
                    )
                )
            except (TypeError, ValueError, ValidationError):
                chunk = None
            if (
                row is None
                or chunk is None
                or _strip(row[0]) != chunk_digest
                or type(row[1]) is not int
                or int(row[1]) < 0
                or str(row[2]) != chunk_id
                or str(row[4]) != chunk_id
                or _strip(row[3]) != normalized_text_hash_v1(str(row[7]))
                or type(row[9]) is not str
                or not str(row[9]).strip()
                or int(str(row[10])) > predecessor_epoch_id
                or (row[11] is not None and predecessor_epoch_id >= int(str(row[11])))
                or str(row[5]) != str(row[12])
                or int(str(row[13])) > predecessor_epoch_id
                or (row[14] is not None and predecessor_epoch_id >= int(str(row[14])))
                or tuple(row[15:18]) != ("committed", "sealed", "complete")
                or str(row[18]) not in {"provisional", "strict"}
                or row[19] is None
            ):
                raise EventConflictError("deactivated chunk snapshot authority changed")
        _lock_d30_m3_image_membership(cursor, locator)
        return

    for requirement_id in locator.touched_requirement_ids:
        row = cursor.execute(
            """
            SELECT requirement.requirement_version_id,
                   requirement.group_version_id,
                   requirement.lifecycle_state,
                   requirement.requirement_text,
                   requirement.requirement_text_hash,
                   group_version.group_version_id,
                   group_version.group_family_id,
                   group_version.lifecycle_state,
                   group_version.semantic_structure_hash,
                   family.group_family_id, family.claim_id,
                   family.lifecycle_state,
                   validity.group_version_id,
                   validity.group_family_id, validity.claim_id,
                   validity.semantic_structure_hash,
                   validity.valid_from_epoch, validity.valid_to_epoch
            FROM groundloop_m5_requirement_version AS requirement
            JOIN groundloop_m5_group_version AS group_version
              ON group_version.group_version_id = requirement.group_version_id
            JOIN groundloop_m5_group_family AS family
              ON family.group_family_id = group_version.group_family_id
            JOIN groundloop_m5_group_validity AS validity
              ON validity.group_version_id = group_version.group_version_id
            WHERE requirement.requirement_version_id = %s
            FOR KEY SHARE OF requirement, group_version, family, validity
            """,
            (requirement_id,),
        ).fetchone()
        if (
            row is None
            or str(row[0]) != requirement_id
            or str(row[1]) != str(row[5])
            or str(row[2]) != "PUBLISHED"
            or str(row[3]) != normalize_text_v1(str(row[3]))
            or _strip(row[4]) != normalized_text_hash_v1(str(row[3]))
            or str(row[5]) != str(row[12])
            or str(row[6]) != str(row[9])
            or str(row[6]) != str(row[13])
            or str(row[7]) != "PUBLISHED"
            or _strip(row[8]) != _strip(row[15])
            or str(row[9]) != str(row[13])
            or str(row[10]) != str(row[14])
            or str(row[11]) != "PUBLISHED"
            or int(str(row[16])) > predecessor_epoch_id
            or (row[17] is not None and predecessor_epoch_id >= int(str(row[17])))
        ):
            raise EventConflictError("touched requirement is absent at activation base")
    for chunk_id in source_chunks:
        row = cursor.execute(
            """
            SELECT chunk.chunk_version_id, chunk.document_version_id,
                   chunk.chunk_index, chunk.text, chunk.text_hash,
                   chunk.chunker_version,
                   chunk.valid_from_epoch, chunk.valid_to_epoch,
                   version.document_version_id,
                   version.valid_from_epoch, version.valid_to_epoch,
                   creator.structural_status, creator.semantic_status,
                   creator.evaluation_state, creator.publication_mode,
                   creator.sealed_at
            FROM groundloop_chunk_version AS chunk
            JOIN groundloop_document_version AS version
              ON version.document_version_id = chunk.document_version_id
            JOIN groundloop_epoch AS creator
              ON creator.epoch_id = chunk.valid_from_epoch
            WHERE chunk.chunk_version_id = %s
            FOR KEY SHARE OF chunk, version
            """,
            (chunk_id,),
        ).fetchone()
        try:
            chunk = (
                None
                if row is None
                else ChunkVersion(
                    chunk_version_id=str(row[0]),
                    document_version_id=str(row[1]),
                    chunk_index=int(str(row[2])),
                    text=str(row[3]),
                    text_hash=_strip(row[4]),
                )
            )
        except (TypeError, ValueError, ValidationError):
            chunk = None
        if (
            row is None
            or chunk is None
            or str(row[0]) != chunk_id
            or type(row[5]) is not str
            or not str(row[5]).strip()
            or int(str(row[6])) > predecessor_epoch_id
            or (row[7] is not None and predecessor_epoch_id >= int(str(row[7])))
            or str(row[1]) != str(row[8])
            or int(str(row[9])) > predecessor_epoch_id
            or (row[10] is not None and predecessor_epoch_id >= int(str(row[10])))
            or tuple(row[11:14]) != ("committed", "sealed", "complete")
            or str(row[14]) not in {"provisional", "strict"}
            or row[15] is None
        ):
            raise EventConflictError("deactivated chunk is absent at activation base")
    _lock_d30_m3_image_membership(cursor, locator)


def _lock_d29_scopes_and_reserve(
    cursor: Cursor[Any], locator: _D29LocatorAuthority
) -> dict[str, _DirectScopeAuthority]:
    direct_authority: dict[str, _DirectScopeAuthority] = {}
    expected_direct_scopes = set(locator.direct_scope_root_job_ids)
    preliminary_direct_scopes = {
        job_id: row
        for owner in locator.d30_claims.owners
        for job_id, row in owner.scopes
    }
    for job_id in locator.direct_job_ids:
        row = cursor.execute(
            """
            SELECT scope.root_job_id, scope.epoch_id,
                   scope.registry_snapshot_id, scope.scope_kind,
                   scope.explicit_claim_ids, scope.closed_revision,
                   update.registry_snapshot_id, update.candidate_policy_id,
                   update.manifest
            FROM groundloop_discovery_scope AS scope
            JOIN groundloop_m4_update AS update
              ON update.epoch_id = scope.epoch_id
            WHERE scope.root_job_id = %s
            FOR UPDATE OF scope
            """,
            (job_id,),
        ).fetchone()
        preliminary = preliminary_direct_scopes.get(job_id)
        locked_scope = None if row is None else tuple(row[:6])
        if job_id not in preliminary_direct_scopes or locked_scope != preliminary:
            raise EventConflictError("D30 direct scope point changed before tier 8")
        if job_id not in expected_direct_scopes:
            if row is not None:
                raise EventConflictError("direct child/frontier job owns a scope")
            continue
        if row is None:
            raise EventConflictError("D29 direct locator scope disappeared")
        manifest = row[8]
        if type(manifest) is not dict or tuple(manifest) != (_M4_RUNTIME_MANIFEST_KEY,):
            raise EventConflictError("direct scope runtime manifest is malformed")
        runtime = manifest[_M4_RUNTIME_MANIFEST_KEY]
        if type(runtime) is not dict or set(runtime) != {
            "event_manifest",
            "scope_claim_ids",
            "failure_reason",
        }:
            raise EventConflictError("direct scope runtime metadata is malformed")
        scope_claim_ids = runtime["scope_claim_ids"]
        if type(scope_claim_ids) is not dict or job_id not in scope_claim_ids:
            raise EventConflictError("direct scope membership is absent")
        raw_members = scope_claim_ids[job_id]
        if type(raw_members) is not list:
            raise EventConflictError("direct scope membership is untyped")
        try:
            registered_claim_ids = _canonical_texts(
                "direct scope registered claims", raw_members
            )
            scope = M4DiscoveryScope(
                root_job_id=str(row[0]),
                registry_snapshot_id=str(row[2]),
                registered_claim_ids=registered_claim_ids,
                closed=row[5] is not None,
            )
            policy = _load_direct_candidate_policy(cursor, str(row[7]))
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError("direct scope identity is malformed") from error
        if (
            scope.root_job_id != job_id
            or int(str(row[1])) < 1
            or scope.registry_snapshot_id != str(row[6])
            or scope.registry_snapshot_id != policy.claim_registry_snapshot_id
            or str(row[3]) != "all_registered_claims"
            or row[4] is not None
            or raw_members != list(registered_claim_ids)
            or not scope.closed
            or type(row[5]) is not int
            or int(row[5]) < 1
            or runtime["failure_reason"] is not None
        ):
            raise EventConflictError("direct scope declaration changed")
        direct_authority[job_id] = _DirectScopeAuthority(
            scope=scope,
            epoch_id=int(str(row[1])),
            candidate_policy_id=str(row[7]),
            scope_kind=str(row[3]),
            closed_revision=int(row[5]),
        )
    existing_ids = tuple(
        sorted(
            set(locator.candidate_root_job_ids) | set(locator.verifier_root_job_ids),
            key=_c_key,
        )
    )
    for root_id in existing_ids:
        if (
            cursor.execute(
                """
            SELECT root_job_id, epoch_id, direction,
                   requirement_version_id, inserted_chunk_version_id,
                   candidate_policy_id,
                   requirement_registry_snapshot_digest,
                   active_chunk_snapshot_digest, scope_contract_digest,
                   scope_state, staged_result_artifact_hash,
                   scope_closure_digest, child_set_hash, completion_digest,
                   created_revision, staged_revision, closed_revision,
                   created_at, closed_at
            FROM groundloop_m5_discovery_scope
            WHERE root_job_id = %s
            FOR UPDATE
            """,
                (root_id,),
            ).fetchone()
            is None
        ):
            raise EventConflictError("D29 locator root scope disappeared")
    for root_id in locator.prospective_coordinates.direct_scope_root_job_ids:
        _reserve_d29_coordinate(
            cursor,
            namespace=_D29_SCOPE_RESERVATION_NAMESPACE,
            kind="direct-scope",
            object_id=root_id,
        )
        if (
            cursor.execute(
                "SELECT 1 FROM groundloop_discovery_scope WHERE root_job_id = %s",
                (root_id,),
            ).fetchone()
            is not None
        ):
            raise EventConflictError("prospective direct scope already exists")
    for root_id in locator.prospective_coordinates.requirement_scope_root_job_ids:
        _reserve_d29_coordinate(
            cursor,
            namespace=_D29_SCOPE_RESERVATION_NAMESPACE,
            kind="requirement-scope",
            object_id=root_id,
        )
        if (
            cursor.execute(
                "SELECT 1 FROM groundloop_m5_discovery_scope WHERE root_job_id = %s",
                (root_id,),
            ).fetchone()
            is not None
        ):
            raise EventConflictError("prospective requirement scope already exists")
    return direct_authority


def _lock_d29_jobs_and_reserve(
    cursor: Cursor[Any], locator: _D29LocatorAuthority
) -> None:
    preliminary_jobs: dict[str, tuple[object, ...]] = {}
    for owner in locator.d30_claims.owners:
        for job in owner.jobs:
            job_id = str(job[0])
            if job_id in preliminary_jobs:
                raise EventConflictError("D30 owner maps repeat a direct job")
            preliminary_jobs[job_id] = job
    for job_id in locator.direct_job_ids:
        row = cursor.execute(
            """
            SELECT job_id, epoch_id, parent_job_id, job_kind,
                   candidate_policy_id, payload_hash, execution_spec_hash,
                   claim_id, chunk_version_id, expandable, job_state,
                   child_closed, child_set_hash, completion_digest,
                   result_artifact_id, result_artifact_hash,
                   created_revision, completed_revision, created_at, completed_at
            FROM groundloop_semantic_job
            WHERE job_id = %s
            FOR UPDATE
            """,
            (job_id,),
        ).fetchone()
        if (
            row is None
            or str(row[0]) != job_id
            or job_id not in preliminary_jobs
            or tuple(row) != preliminary_jobs[job_id]
        ):
            raise EventConflictError("D29 direct locator job disappeared")
    for owner in locator.d30_claims.owners:
        rerun = tuple(
            tuple(row)
            for row in cursor.execute(
                """
                SELECT job_id, epoch_id, parent_job_id, job_kind,
                       candidate_policy_id, payload_hash, execution_spec_hash,
                       claim_id, chunk_version_id, expandable, job_state,
                       child_closed, child_set_hash, completion_digest,
                       result_artifact_id, result_artifact_hash,
                       created_revision, completed_revision, created_at,
                       completed_at
                FROM groundloop_semantic_job
                WHERE epoch_id = %s
                ORDER BY job_id COLLATE "C"
                """,
                (owner.epoch_id,),
            ).fetchall()
        )
        if rerun != owner.jobs:
            raise EventConflictError("D30 direct owner job range changed")
        for job_id, preliminary_scope in owner.scopes:
            scope = cursor.execute(
                """
                SELECT root_job_id, epoch_id, registry_snapshot_id, scope_kind,
                       explicit_claim_ids, closed_revision
                FROM groundloop_discovery_scope WHERE root_job_id = %s
                """,
                (job_id,),
            ).fetchone()
            if (None if scope is None else tuple(scope)) != preliminary_scope:
                raise EventConflictError("D30 direct scope point changed after jobs")
    for job_id in locator.prospective_coordinates.direct_job_ids:
        _reserve_d29_coordinate(
            cursor,
            namespace=_D29_JOB_RESERVATION_NAMESPACE,
            kind="direct-job",
            object_id=job_id,
        )
        if (
            cursor.execute(
                "SELECT 1 FROM groundloop_semantic_job WHERE job_id = %s",
                (job_id,),
            ).fetchone()
            is not None
        ):
            raise EventConflictError("prospective direct job already exists")
    existing_m5_ids = tuple(
        sorted(
            set(locator.candidate_root_job_ids)
            | set(locator.verifier_root_job_ids)
            | set(locator.verifier_job_ids),
            key=_c_key,
        )
    )
    for job_id in existing_m5_ids:
        if (
            cursor.execute(
                """
            SELECT logical_job_id, epoch_id, structural_event_id, job_kind,
                   candidate_policy_id, candidate_policy_manifest_hash,
                   parent_job_id, subject_kind::text, subject_id,
                   chunk_version_id, semantic_pair_digest,
                   admitted_pair_digest, scope_contract_digest,
                   requirement_registry_snapshot_digest,
                   active_chunk_snapshot_digest, role_template_hash,
                   execution_spec_hash, expandable, payload_hash, job_state,
                   result_artifact_id, result_artifact_hash,
                   scope_closure_digest, child_set_hash, archive_reason,
                   completion_digest, cancelled_by_event_id,
                   cancelled_by_epoch_id, cancellation_reason,
                   created_revision, completed_revision, created_at, completed_at
            FROM groundloop_m5_semantic_job
            WHERE logical_job_id = %s
            FOR UPDATE
            """,
                (job_id,),
            ).fetchone()
            is None
        ):
            raise EventConflictError("D29 locator job disappeared")
    for job_id in locator.prospective_coordinates.requirement_job_ids:
        _reserve_d29_coordinate(
            cursor,
            namespace=_D29_JOB_RESERVATION_NAMESPACE,
            kind="requirement-job",
            object_id=job_id,
        )
        if (
            cursor.execute(
                "SELECT 1 FROM groundloop_m5_semantic_job WHERE logical_job_id = %s",
                (job_id,),
            ).fetchone()
            is not None
        ):
            raise EventConflictError("prospective requirement job already exists")


def _lock_d29_direct_attempts(
    cursor: Cursor[Any], locator: _D29LocatorAuthority
) -> dict[str, _DirectAttemptAuthority]:
    authority: dict[str, _DirectAttemptAuthority] = {}
    preliminary_attempts = {
        job_id: rows
        for owner in locator.d30_claims.owners
        for job_id, rows in owner.attempts
    }
    for job_id in locator.direct_job_ids:
        job = cursor.execute(
            """
            SELECT execution_spec_hash, job_state
            FROM groundloop_semantic_job
            WHERE job_id = %s
            """,
            (job_id,),
        ).fetchone()
        if job is None:
            raise EventConflictError("direct attempt job disappeared after lock")
        rows = cursor.execute(
            """
            SELECT attempt_id, job_id, execution_spec_hash, attempt_ordinal,
                   lease_token_hash, attempt_state, lease_expires_at,
                   started_at, finished_at
            FROM groundloop_semantic_job_attempt
            WHERE job_id = %s
            ORDER BY attempt_ordinal
            FOR UPDATE
            """,
            (job_id,),
        ).fetchall()
        if (
            job_id not in preliminary_attempts
            or tuple(tuple(row) for row in rows) != preliminary_attempts[job_id]
        ):
            raise EventConflictError("D30 direct attempt history changed before lock")
        attempts: list[M4JobAttempt] = []
        states: list[str] = []
        for expected_ordinal, row in enumerate(rows, start=1):
            try:
                attempt = M4JobAttempt(
                    attempt_id=str(row[0]),
                    job_id=str(row[1]),
                    execution_spec_hash=_strip(row[2]),
                    attempt_ordinal=int(str(row[3])),
                    lease_token_hash=_strip(row[4]),
                )
            except (TypeError, ValueError, ValidationError) as error:
                raise EventConflictError(
                    "direct attempt identity is malformed"
                ) from error
            state = str(row[5])
            expected_attempt_id = stable_m4_digest(
                "m4-job-attempt-v1", job_id, str(expected_ordinal)
            )
            expected_lease_token_hash = stable_m4_digest(
                "m4-lease-token-v1", job_id, str(expected_ordinal)
            )
            if (
                attempt.job_id != job_id
                or attempt.execution_spec_hash != _strip(job[0])
                or attempt.attempt_ordinal != expected_ordinal
                or attempt.attempt_id != expected_attempt_id
                or attempt.lease_token_hash != expected_lease_token_hash
                or state not in {"leased", "completed", "failed", "expired"}
                or row[6] is None
                or row[7] is None
                or ((state == "leased") != (row[8] is None))
            ):
                raise EventConflictError("direct attempt closure changed")
            try:
                if row[6] < row[7]:
                    raise EventConflictError(
                        "direct attempt lease expires before start"
                    )
                if row[8] is not None and row[8] < row[7]:
                    raise EventConflictError("direct attempt finished before it began")
                if state == "expired" and row[8] < row[6]:
                    raise EventConflictError("direct attempt expired before its lease")
            except TypeError as error:
                raise EventConflictError(
                    "direct attempt timestamps are malformed"
                ) from error
            attempts.append(attempt)
            states.append(state)
        job_state = M4JobState(str(job[1]))
        latest = None if not states else states[-1]
        if (
            (job_state is M4JobState.DECLARED and states)
            or (job_state is M4JobState.RUNNING and latest != "leased")
            or (
                job_state is M4JobState.RETRYABLE_FAILED
                and latest not in {"failed", "expired"}
            )
            or (
                job_state
                in {M4JobState.COMPLETED_ACTIVE, M4JobState.COMPLETED_INACTIVE}
                and latest != "completed"
            )
            or (job_state is M4JobState.TERMINAL_FAILED and latest != "failed")
            or (
                job_state is not M4JobState.RUNNING
                and job_state is not M4JobState.CANCELLED
                and "leased" in states
            )
            or states.count("leased") > 1
            or "leased" in states[:-1]
            or (job_state is M4JobState.CANCELLED and "completed" in states)
            or states.count("completed") > 1
            or ("completed" in states and latest != "completed")
        ):
            raise EventConflictError("direct attempt state disagrees with its job")
        authority[job_id] = _DirectAttemptAuthority(tuple(attempts), tuple(states))
    return authority


def _direct_channel_set_hash(hits: tuple[M4ChannelHit, ...]) -> str:
    identities = sorted(
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
        for hit in hits
    )
    return stable_m4_digest("m4-discovery-channel-set-v1", *identities)


def _direct_admitted_pair_id(admitted: AdmittedPair) -> str:
    return stable_m4_digest(
        "m4-admitted-pair-v1",
        str(admitted.epoch_id),
        admitted.pair.claim_id,
        admitted.pair.chunk_version_id,
        admitted.candidate_policy_id,
    )


def _validate_direct_discovery_closure(
    cursor: Cursor[Any],
    spec: M4LogicalJobSpec,
    row: tuple[object, ...],
    child_specs: tuple[M4LogicalJobSpec, ...],
    *,
    lock_authority: bool,
) -> None:
    state = M4JobState(str(row[10]))
    lock_clause = " FOR UPDATE" if lock_authority else ""
    result = cursor.execute(
        """
        SELECT root_job_id, epoch_id, result_artifact_id,
               result_artifact_hash, fallback_satisfied,
               channel_hit_count, admitted_pair_count,
               channel_set_hash, admitted_pair_set_hash
        FROM groundloop_m4_discovery_result
        WHERE root_job_id = %s
        """
        + lock_clause,
        (spec.job_id,),
    ).fetchone()
    if state not in {M4JobState.COMPLETED_ACTIVE, M4JobState.COMPLETED_INACTIVE}:
        if result is not None:
            raise EventConflictError("noncompleted direct root has a discovery result")
        return
    if result is None:
        raise EventConflictError("completed direct root lacks its discovery result")
    hit_rows = cursor.execute(
        """
        SELECT epoch_id, claim_id, chunk_version_id, candidate_policy_id,
               channel, rank, score, channel_artifact_hash
        FROM groundloop_impact_channel_hit
        WHERE epoch_id = %s AND candidate_policy_id = %s
          AND ((%s = 'impact_discovery' AND chunk_version_id = %s)
               OR (%s = 'frontier_retrieve' AND claim_id = %s))
        ORDER BY channel, rank, claim_id, chunk_version_id,
                 candidate_policy_id, channel_artifact_hash
        """
        + lock_clause,
        (
            int(str(row[1])),
            spec.candidate_policy_id,
            spec.kind.value,
            spec.target_chunk_version_id,
            spec.kind.value,
            spec.target_claim_id,
        ),
    ).fetchall()
    admitted_rows = cursor.execute(
        """
        SELECT admitted_pair_id, epoch_id, claim_id, chunk_version_id,
               candidate_policy_id, fused_rank, reasons, mandatory_lineage
        FROM groundloop_admitted_pair
        WHERE epoch_id = %s AND candidate_policy_id = %s
          AND ((%s = 'impact_discovery' AND chunk_version_id = %s)
               OR (%s = 'frontier_retrieve' AND claim_id = %s))
        ORDER BY fused_rank, claim_id, chunk_version_id, candidate_policy_id
        """
        + lock_clause,
        (
            int(str(row[1])),
            spec.candidate_policy_id,
            spec.kind.value,
            spec.target_chunk_version_id,
            spec.kind.value,
            spec.target_claim_id,
        ),
    ).fetchall()
    try:
        hits = tuple(
            M4ChannelHit(
                epoch_id=int(str(item[0])),
                pair=PairKey(str(item[1]), str(item[2])),
                candidate_policy_id=str(item[3]),
                channel=AdmissionChannel(str(item[4])),
                rank=int(str(item[5])),
                score=None if item[6] is None else float(str(item[6])),
                channel_artifact_hash=_strip(item[7]),
            )
            for item in hit_rows
        )
        admitted = tuple(
            AdmittedPair(
                epoch_id=int(str(item[1])),
                pair=PairKey(str(item[2]), str(item[3])),
                candidate_policy_id=str(item[4]),
                fused_rank=int(str(item[5])),
                reasons=tuple(AdmissionChannel(str(reason)) for reason in item[6]),
                mandatory_lineage=bool(item[7]),
            )
            for item in admitted_rows
        )
        discovery = M4DiscoveryResult(
            root_job_id=str(result[0]),
            result_artifact_id=str(result[2]),
            result_artifact_hash=_strip(result[3]),
            admitted_pairs=admitted,
            fallback_satisfied=bool(result[4]),
            channel_hits=hits,
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError("direct discovery closure is malformed") from error
    if any(
        _strip(item[0]) != _direct_admitted_pair_id(value)
        for item, value in zip(admitted_rows, admitted, strict=True)
    ):
        raise EventConflictError("direct admitted-pair identity changed")
    expected_child_pairs = tuple(
        sorted(
            (child.pair for child in child_specs if child.pair is not None),
            key=lambda pair: (_c_key(pair.claim_id), _c_key(pair.chunk_version_id)),
        )
    )
    admitted_pairs = tuple(
        sorted(
            (item.pair for item in admitted),
            key=lambda pair: (_c_key(pair.claim_id), _c_key(pair.chunk_version_id)),
        )
    )
    if (
        discovery.root_job_id != spec.job_id
        or int(str(result[1])) != int(str(row[1]))
        or discovery.result_artifact_id != str(row[14])
        or discovery.result_artifact_hash != _strip(row[15])
        or type(result[5]) is not int
        or int(result[5]) != len(hits)
        or type(result[6]) is not int
        or int(result[6]) != len(admitted)
        or _strip(result[7]) != _direct_channel_set_hash(hits)
        or _strip(result[8])
        != stable_m4_digest(
            "m4-discovery-admitted-set-v1",
            *sorted(_direct_admitted_pair_id(item) for item in admitted),
        )
        or (
            spec.kind is M4JobKind.IMPACT_DISCOVERY and not discovery.fallback_satisfied
        )
        or (
            state is M4JobState.COMPLETED_ACTIVE
            and expected_child_pairs != admitted_pairs
        )
    ):
        raise EventConflictError("direct discovery-result closure changed")


def _lock_d29_direct_job_closure(
    cursor: Cursor[Any],
    locator: _D29LocatorAuthority,
    attempts: dict[str, _DirectAttemptAuthority],
    scopes: dict[str, _DirectScopeAuthority],
) -> dict[str, _DirectVerifierAuthority]:
    """Revalidate the complete direct-M4 provenance named by locators."""

    rows: dict[str, tuple[object, ...]] = {}
    for job_id in locator.direct_job_ids:
        row = cursor.execute(
            """
            SELECT job_id, epoch_id, parent_job_id, job_kind,
                   candidate_policy_id, payload_hash, execution_spec_hash,
                   claim_id, chunk_version_id, expandable, job_state,
                   child_closed, child_set_hash, completion_digest,
                   result_artifact_id, result_artifact_hash,
                   created_revision, completed_revision, created_at, completed_at
            FROM groundloop_semantic_job
            WHERE job_id = %s
            """,
            (job_id,),
        ).fetchone()
        if row is None:
            raise EventConflictError("direct locator job disappeared after lock")
        rows[job_id] = tuple(row)

    dependencies: set[tuple[int, str, str]] = set()
    for job_id, row in rows.items():
        epoch_id = int(str(row[1]))
        dependency_rows = cursor.execute(
            """
            SELECT epoch_id, parent_job_id, child_job_id
            FROM groundloop_semantic_job_dependency
            WHERE epoch_id = %s AND parent_job_id = %s
            ORDER BY child_job_id COLLATE "C"
            """,
            (epoch_id, job_id),
        ).fetchall()
        dependencies.update(
            (int(item[0]), str(item[1]), str(item[2])) for item in dependency_rows
        )
    expected_dependencies = {
        (int(str(row[1])), str(row[2]), job_id)
        for job_id, row in rows.items()
        if row[2] is not None
    }
    if dependencies != expected_dependencies:
        raise EventConflictError("direct locator dependency closure changed")

    children: dict[str, list[str]] = {}
    for _epoch_id, parent_id, child_id in dependencies:
        children.setdefault(parent_id, []).append(child_id)
    specs: dict[str, M4LogicalJobSpec] = {}
    for job_id, row in rows.items():
        try:
            kind = M4JobKind(str(row[3]))
            claim_id = None if row[7] is None else str(row[7])
            chunk_id = None if row[8] is None else str(row[8])
            pair = (
                PairKey(claim_id, chunk_id)
                if kind is M4JobKind.VERIFY_PAIR
                and claim_id is not None
                and chunk_id is not None
                else None
            )
            epoch = cursor.execute(
                "SELECT event_id FROM groundloop_epoch WHERE epoch_id = %s",
                (int(str(row[1])),),
            ).fetchone()
            if epoch is None:
                raise EventConflictError("direct locator job epoch disappeared")
            spec = M4LogicalJobSpec(
                job_id=job_id,
                event_id=str(epoch[0]),
                kind=kind,
                candidate_policy_id=str(row[4]),
                payload_hash=_strip(row[5]),
                execution_spec_hash=_strip(row[6]),
                parent_job_id=None if row[2] is None else str(row[2]),
                pair=pair,
                target_claim_id=(
                    claim_id if kind is M4JobKind.FRONTIER_RETRIEVE else None
                ),
                target_chunk_version_id=(
                    chunk_id if kind is M4JobKind.IMPACT_DISCOVERY else None
                ),
                expandable=bool(row[9]),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError(
                "direct locator job identity is malformed"
            ) from error
        specs[job_id] = spec
    if set(attempts) != set(specs):
        raise EventConflictError("direct attempt authority escaped locator jobs")
    for job_id, row in rows.items():
        spec = specs[job_id]
        kind = spec.kind
        child_ids = tuple(sorted(children.get(job_id, ()), key=_c_key))
        if spec.parent_job_id is None:
            if kind not in {
                M4JobKind.IMPACT_DISCOVERY,
                M4JobKind.FRONTIER_RETRIEVE,
            }:
                raise EventConflictError("direct locator has a parentless verifier")
        elif (
            kind is not M4JobKind.VERIFY_PAIR
            or spec.parent_job_id not in specs
            or specs[spec.parent_job_id].parent_job_id is not None
            or specs[spec.parent_job_id].kind
            not in {
                M4JobKind.IMPACT_DISCOVERY,
                M4JobKind.FRONTIER_RETRIEVE,
            }
        ):
            raise EventConflictError("direct verifier parent topology changed")
        expected_scope = (
            spec.parent_job_id is None and kind is M4JobKind.IMPACT_DISCOVERY
        )
        if expected_scope != (job_id in scopes):
            raise EventConflictError("direct root/scope bijection changed")
        if expected_scope:
            scope_authority = scopes[job_id]
            if (
                scope_authority.epoch_id != int(str(row[1]))
                or scope_authority.candidate_policy_id != spec.candidate_policy_id
                or type(row[17]) is not int
                or scope_authority.closed_revision != int(row[17])
            ):
                raise EventConflictError("direct root/scope closure changed")
        if spec.parent_job_id is not None:
            parent_scope = scopes.get(spec.parent_job_id)
            if (
                parent_scope is not None
                and spec.pair is not None
                and spec.pair.claim_id not in parent_scope.scope.registered_claim_ids
            ):
                raise EventConflictError("direct verifier escaped root scope")
        _validate_m4_job_state(spec, row, child_ids)
        if kind in {M4JobKind.IMPACT_DISCOVERY, M4JobKind.FRONTIER_RETRIEVE}:
            _validate_direct_discovery_closure(
                cursor,
                spec,
                row,
                tuple(specs[child_id] for child_id in child_ids),
                lock_authority=False,
            )
    authority: dict[str, _DirectVerifierAuthority] = {}
    for observation_id in locator.direct_verifier_observation_ids:
        execution = cursor.execute(
            """
            SELECT observation_id, job_id, btrim(admitted_pair_id),
                   model_artifact_id, prompt_artifact_id,
                   btrim(execution_spec_hash), btrim(pair_input_hash),
                   calibration_version, btrim(calibration_artifact_sha256),
                   temperature, raw_logits, btrim(raw_output_hash),
                   reused_from_observation_id
            FROM groundloop_m4_verification_execution
            WHERE observation_id = %s
            """,
            (observation_id,),
        ).fetchone()
        if execution is None or str(execution[1]) not in rows:
            raise EventConflictError("direct verifier provenance escaped locators")
        job_row = rows[str(execution[1])]
        attempt_authority = attempts[str(execution[1])]
        admitted_row = cursor.execute(
            """
            SELECT admitted_pair_id, epoch_id, chunk_version_id, claim_id,
                   candidate_policy_id, fused_rank, reasons,
                   mandatory_lineage
            FROM groundloop_admitted_pair
            WHERE admitted_pair_id = %s
            """,
            (_strip(execution[2]),),
        ).fetchone()
        if admitted_row is None:
            raise EventConflictError("direct verifier admitted pair disappeared")
        pair = PairKey(str(admitted_row[3]), str(admitted_row[2]))
        try:
            admitted = AdmittedPair(
                epoch_id=int(str(admitted_row[1])),
                pair=pair,
                candidate_policy_id=str(admitted_row[4]),
                fused_rank=int(str(admitted_row[5])),
                reasons=tuple(
                    AdmissionChannel(str(reason)) for reason in admitted_row[6]
                ),
                mandatory_lineage=bool(admitted_row[7]),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError(
                "direct verifier admitted pair is malformed"
            ) from error
        expected_admitted_id = stable_m4_digest(
            "m4-admitted-pair-v1",
            str(admitted.epoch_id),
            admitted.pair.claim_id,
            admitted.pair.chunk_version_id,
            admitted.candidate_policy_id,
        )
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
            (pair.claim_id, pair.chunk_version_id),
        ).fetchone()
        if input_row is None:
            raise EventConflictError("direct verifier pair input disappeared")
        citation_rows = cursor.execute(
            """
            SELECT chunk_version_id
            FROM groundloop_answer_citation
            WHERE answer_version_id = %s
            ORDER BY citation_ordinal
            """,
            (str(input_row[2]),),
        ).fetchall()
        try:
            pair_input = PairVerificationInput(
                pair=pair,
                claim_text=str(input_row[0]),
                claim_required=bool(input_row[1]),
                claim_cited_chunk_version_ids=tuple(
                    str(item[0]) for item in citation_rows
                ),
                document_version_id=str(input_row[3]),
                chunk_index=int(str(input_row[4])),
                chunk_text=str(input_row[5]),
                chunk_text_hash=_strip(input_row[6]),
                chunker_artifact_id=str(input_row[7]),
            )
            raw_logits = tuple(float(str(value)) for value in execution[10])
            if len(raw_logits) != 3:
                raise ValidationError("direct verifier raw logits changed")
            checked_execution = M5TypedDirectVerificationExecution(
                observation_id=str(execution[0]),
                job_id=str(execution[1]),
                admitted_pair_id=_strip(execution[2]),
                model_artifact_id=str(execution[3]),
                prompt_artifact_id=str(execution[4]),
                execution_spec_hash=_strip(execution[5]),
                pair_input_hash=_strip(execution[6]),
                calibration_version=str(execution[7]),
                calibration_artifact_sha256=_strip(execution[8]),
                temperature=float(str(execution[9])),
                raw_logits=(raw_logits[0], raw_logits[1], raw_logits[2]),
                raw_output_hash=_strip(execution[11]),
                reused_from_observation_id=(
                    None if execution[12] is None else str(execution[12])
                ),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError(
                "direct verifier execution/input is malformed"
            ) from error
        model = cursor.execute(
            """
            SELECT model_artifact_id, task, model_id, immutable_revision
            FROM groundloop_model_artifact WHERE model_artifact_id = %s
            """,
            (checked_execution.model_artifact_id,),
        ).fetchone()
        prompt = cursor.execute(
            """
            SELECT prompt_artifact_id, task, version
            FROM groundloop_prompt_artifact WHERE prompt_artifact_id = %s
            """,
            (checked_execution.prompt_artifact_id,),
        ).fetchone()
        direct_policy = _load_direct_candidate_policy(cursor, str(job_row[4]))
        decision_row = cursor.execute(
            """
            SELECT policy_version, support_threshold, refute_threshold,
                   tie_rule_version
            FROM groundloop_decision_policy
            WHERE policy_version = %s
            """,
            (direct_policy.decision_policy_version,),
        ).fetchone()
        try:
            if decision_row is None:
                raise ValidationError("direct decision policy disappeared")
            decision_policy = DecisionPolicy(
                str(decision_row[0]),
                float(decision_row[1]),
                float(decision_row[2]),
                str(decision_row[3]),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError("direct decision policy changed") from error
        if (
            _strip(admitted_row[0]) != expected_admitted_id
            or checked_execution.admitted_pair_id != expected_admitted_id
            or checked_execution.pair_input_hash != pair_input.input_hash
            or job_row[3] != M4JobKind.VERIFY_PAIR.value
            or str(job_row[7]) != pair.claim_id
            or str(job_row[8]) != pair.chunk_version_id
            or str(job_row[4]) != admitted.candidate_policy_id
            or int(str(job_row[1])) != admitted.epoch_id
            or checked_execution.execution_spec_hash != _strip(job_row[6])
            or checked_execution.execution_spec_hash
            != direct_policy.verifier_execution_spec_hash
            or M4JobState(str(job_row[10])) is not M4JobState.COMPLETED_ACTIVE
            or not attempt_authority.states
            or attempt_authority.states[-1] != "completed"
            or type(job_row[17]) is not int
            or job_row[14] is None
            or job_row[15] is None
            or model is None
            or str(model[0]) != checked_execution.model_artifact_id
            or str(model[1]) != "verification"
            or prompt is None
            or str(prompt[0]) != checked_execution.prompt_artifact_id
            or str(prompt[1]) != "verification"
        ):
            raise EventConflictError("direct verifier provenance changed")
        authority[observation_id] = _DirectVerifierAuthority(
            execution=checked_execution,
            pair_input=pair_input,
            pair=pair,
            candidate_policy_id=admitted.candidate_policy_id,
            produced_epoch_id=int(str(job_row[1])),
            installed_revision=int(job_row[17]),
            result_artifact_id=str(job_row[14]),
            result_artifact_hash=_strip(job_row[15]),
            decision_policy=decision_policy,
            model_id=str(model[2]),
            model_revision=str(model[3]),
            prompt_version=str(prompt[2]),
        )
    if len(authority) != len(locator.direct_verifier_observation_ids):
        raise EventConflictError("direct verifier provenance is ambiguous")
    return authority


def _lock_d29_attempt_outputs(
    cursor: Cursor[Any], locator: _D29LocatorAuthority
) -> None:
    for observation_id in locator.verifier_observation_ids:
        execution = cursor.execute(
            """
            SELECT logical_job_id, attempt_id, artifact_id, artifact_hash,
                   pair_input_hash, produced_epoch_id
            FROM groundloop_m5_requirement_verifier_execution
            WHERE observation_id = %s
            """,
            (observation_id,),
        ).fetchone()
        if execution is None:
            raise EventConflictError("verifier locator execution disappeared")
        job_id = _strip(execution[0])
        attempt_id = _strip(execution[1])
        job_raw = cursor.execute(
            """
            SELECT logical_job_id, epoch_id, structural_event_id, job_kind,
                   candidate_policy_id, candidate_policy_manifest_hash,
                   parent_job_id, subject_kind::text, subject_id,
                   chunk_version_id, semantic_pair_digest,
                   admitted_pair_digest, scope_contract_digest,
                   requirement_registry_snapshot_digest,
                   active_chunk_snapshot_digest, role_template_hash,
                   execution_spec_hash, expandable, payload_hash, job_state,
                   result_artifact_id, result_artifact_hash,
                   scope_closure_digest, child_set_hash, archive_reason,
                   completion_digest, cancelled_by_event_id,
                   cancelled_by_epoch_id, cancellation_reason,
                   created_revision, completed_revision, created_at, completed_at
            FROM groundloop_m5_semantic_job
            WHERE logical_job_id = %s
            """,
            (job_id,),
        ).fetchone()
        attempt_rows = cursor.execute(
            """
            SELECT attempt_id, logical_job_id, attempt_ordinal,
                   execution_spec_hash, lease_token_hash, attempt_state,
                   attempt_output_digest, error_hash, dispatched_at,
                   finished_at, lease_expires_at, attempt_work_digest
            FROM groundloop_m5_job_attempt
            WHERE logical_job_id = %s
            ORDER BY attempt_ordinal
            """,
            (job_id,),
        ).fetchall()
        output = cursor.execute(
            """
            SELECT attempt_result_artifact_id, attempt_result_artifact_hash,
                   attempt_output_digest, attempt_id, logical_job_id,
                   job_epoch_id, payload_hash, execution_spec_hash,
                   result_artifact_id, result_artifact_hash,
                   job_state_at_receipt, job_state_after, disposition,
                   activity_snapshot_epoch_id, activity_snapshot_revision,
                   epoch_active, chunk_active, requirement_active,
                   group_active, archive_reason, cancelled_by_event_id,
                   cancelled_by_epoch_id, cancellation_reason
            FROM groundloop_m5_attempt_result_artifact
            WHERE attempt_id = %s
            """,
            (attempt_id,),
        ).fetchone()
        if job_raw is None or not attempt_rows or output is None:
            raise EventConflictError("verifier attempt/result closure is incomplete")
        job_row = tuple(job_raw)
        job = _m5_job_spec_from_row(job_row, context="verifier attempt")
        _validate_m5_job_state(job, job_row, ())
        checked_attempts: list[tuple[M5JobAttempt, str, str | None]] = []
        previous: M5JobAttempt | None = None
        try:
            for raw in attempt_rows:
                row = tuple(raw)
                state = str(row[5])
                output_digest = None if row[6] is None else _strip(row[6])
                error_hash = None if row[7] is None else _strip(row[7])
                dispatched_at = row[8]
                finished_at = row[9]
                if not isinstance(dispatched_at, datetime):
                    raise ValidationError("attempt dispatch time is invalid")
                attempt = M5JobAttempt(
                    attempt_id=_strip(row[0]),
                    logical_job_id=_strip(row[1]),
                    attempt_ordinal=int(str(row[2])),
                    execution_spec_hash=_strip(row[3]),
                    lease_token_hash=_strip(row[4]),
                    lease_expires_at=row[10],
                    attempt_work_digest=_strip(row[11]),
                )
                attempt.validate_previous(previous)
                previous = attempt
                valid_shape = (
                    (
                        state == "dispatched"
                        and output_digest is None
                        and error_hash is None
                        and finished_at is None
                    )
                    or (
                        state == "result_reserved"
                        and output_digest is not None
                        and error_hash is None
                        and finished_at is None
                    )
                    or (
                        state == "completed"
                        and output_digest is not None
                        and error_hash is None
                        and isinstance(finished_at, datetime)
                    )
                    or (
                        state == "failed"
                        and output_digest is None
                        and error_hash is not None
                        and isinstance(finished_at, datetime)
                    )
                    or (
                        state == "expired"
                        and output_digest is None
                        and error_hash is None
                        and isinstance(finished_at, datetime)
                    )
                )
                if (
                    not valid_shape
                    or attempt.logical_job_id != job.logical_job_id
                    or attempt.execution_spec_hash != job.execution_spec_hash
                    or attempt.lease_expires_at is None
                    or attempt.lease_expires_at <= dispatched_at
                    or (
                        isinstance(finished_at, datetime)
                        and finished_at < dispatched_at
                    )
                ):
                    raise ValidationError("attempt history row is malformed")
                checked_attempts.append((attempt, state, output_digest))
            cited_attempt, cited_state, cited_output_digest = checked_attempts[-1]
            checked_output = M5AttemptOutput(
                attempt_id=_strip(output[3]),
                logical_job_id=_strip(output[4]),
                job_epoch_id=int(str(output[5])),
                payload_hash=_strip(output[6]),
                execution_spec_hash=_strip(output[7]),
                result_artifact_id=_strip(output[8]),
                result_artifact_hash=_strip(output[9]),
                attempt_output_digest=_strip(output[2]),
            )
            checked_artifact = M5AttemptResultArtifact(
                attempt_result_artifact_id=_strip(output[0]),
                attempt_result_artifact_hash=_strip(output[1]),
                attempt_output_digest=_strip(output[2]),
                attempt_id=_strip(output[3]),
                logical_job_id=_strip(output[4]),
                job_epoch_id=int(str(output[5])),
                job_state_at_receipt=M5JobState(str(output[10])),
                job_state_after=M5JobState(str(output[11])),
                disposition=M5AttemptDisposition(str(output[12])),
                activity_snapshot_epoch_id=int(str(output[13])),
                activity_snapshot_revision=int(str(output[14])),
                epoch_active=bool(output[15]),
                chunk_active=None if output[16] is None else bool(output[16]),
                requirement_active=(None if output[17] is None else bool(output[17])),
                group_active=None if output[18] is None else bool(output[18]),
                archive_reason=(
                    None
                    if output[19] is None
                    else M5AttemptArchiveReason(str(output[19]))
                ),
                cancelled_by_event_id=(None if output[20] is None else str(output[20])),
                cancelled_by_epoch_id=(
                    None if output[21] is None else int(str(output[21]))
                ),
                cancellation_reason=(
                    None if output[22] is None else M5TerminalReason(str(output[22]))
                ),
            )
            checked_artifact.validate_job_shape(job.job_kind)
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError(
                "verifier attempt/result history is invalid"
            ) from error
        if (
            job.job_kind is not M5JobKind.VERIFY_REQUIREMENT_PAIR
            or M5JobState(str(job_row[19])) is not M5JobState.COMPLETED_ACTIVE
            or int(str(job_row[1])) != int(str(execution[5]))
            or _strip(job_row[20]) != _strip(execution[2])
            or _strip(job_row[21]) != _strip(execution[3])
            or cited_attempt.attempt_id != attempt_id
            or cited_state != "completed"
            or cited_output_digest != checked_output.attempt_output_digest
            or checked_output.logical_job_id != job_id
            or checked_output.payload_hash != job.payload_hash
            or checked_output.execution_spec_hash != job.execution_spec_hash
            or checked_output.result_artifact_id != _strip(execution[2])
            or checked_output.result_artifact_hash != _strip(execution[3])
            or checked_output.job_epoch_id != int(str(execution[5]))
            or checked_artifact.attempt_output_digest
            != checked_output.attempt_output_digest
            or checked_artifact.attempt_id != cited_attempt.attempt_id
            or checked_artifact.logical_job_id != job_id
            or checked_artifact.job_epoch_id != int(str(execution[5]))
            or checked_artifact.job_state_after is not M5JobState.COMPLETED_ACTIVE
            or checked_artifact.disposition
            is not M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE
            or any(
                state not in {"failed", "expired"}
                for _, state, _ in checked_attempts[:-1]
            )
        ):
            raise EventConflictError("verifier attempt/result closure changed")


def _m5_job_spec_from_row(row: tuple[object, ...], *, context: str) -> M5LogicalJobSpec:
    try:
        kind = M5JobKind(str(row[3]))
        pair = None
        if kind is M5JobKind.VERIFY_REQUIREMENT_PAIR:
            if row[7] != "requirement" or row[8] is None or row[9] is None:
                raise ValidationError("verifier job lacks its exact pair")
            pair = SemanticPairKey(SubjectKind.REQUIREMENT, str(row[8]), str(row[9]))
        return M5LogicalJobSpec(
            logical_job_id=_strip(row[0]),
            structural_event_id=str(row[2]),
            job_kind=kind,
            candidate_policy_id=_strip(row[4]),
            candidate_policy_manifest_hash=_strip(row[5]),
            parent_job_id=None if row[6] is None else _strip(row[6]),
            pair=pair,
            semantic_pair_digest=None if row[10] is None else _strip(row[10]),
            scope_contract_digest=_strip(row[12]),
            requirement_registry_snapshot_digest=_strip(row[13]),
            active_chunk_snapshot_digest=_strip(row[14]),
            role_template_hash=_strip(row[15]),
            execution_spec_hash=_strip(row[16]),
            expandable=bool(row[17]),
            payload_hash=_strip(row[18]),
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError(f"{context} job identity is invalid") from error


def _lock_d29_candidate_source_closure(
    cursor: Cursor[Any], locator: _D29LocatorAuthority
) -> None:
    root_epochs = {
        root_id: epoch_id
        for epoch_id, root_id in locator.candidate_root_job_coordinates
    }
    for root_id in locator.candidate_root_job_ids:
        root_epoch_id = root_epochs.get(root_id)
        if root_epoch_id is None:
            raise EventConflictError("candidate root lacks its epoch coordinate")
        dependency_rows = _locate_d29_m5_dependency_edges(
            cursor, epoch_id=root_epoch_id, parent_job_id=root_id
        )
        result_row = cursor.execute(
            """
            SELECT result_artifact_id, result_artifact_hash, root_job_id,
                   scope_contract_digest, termination, channel_hit_count,
                   selection_count, approximate_selection_count,
                   mandatory_lineage_only_count, staged_epoch_id,
                   staged_revision
            FROM groundloop_m5_requirement_discovery_result
            WHERE root_job_id = %s
            """,
            (root_id,),
        ).fetchone()
        if result_row is None:
            raise EventConflictError("candidate source lacks discovery result")
        hit_rows = cursor.execute(
            """
            SELECT hit_digest, epoch_id, root_job_id, scope_contract_digest,
                   candidate_policy_id, subject_kind::text, subject_id,
                   chunk_version_id, semantic_pair_digest, channel, rank,
                   score, channel_artifact_hash
            FROM groundloop_m5_requirement_channel_hit
            WHERE root_job_id = %s
            ORDER BY channel COLLATE "C", rank, semantic_pair_digest COLLATE "C"
            """,
            (root_id,),
        ).fetchall()
        selection_rows = cursor.execute(
            """
            SELECT selection_digest, root_job_id, scope_contract_digest,
                   subject_kind::text, subject_id, chunk_version_id,
                   semantic_pair_digest, fused_rank, reasons,
                   mandatory_lineage
            FROM groundloop_m5_requirement_scope_selection
            WHERE root_job_id = %s
            ORDER BY fused_rank, semantic_pair_digest COLLATE "C"
            """,
            (root_id,),
        ).fetchall()
        root_raw = cursor.execute(
            """
            SELECT logical_job_id, epoch_id, structural_event_id, job_kind,
                   candidate_policy_id, candidate_policy_manifest_hash,
                   parent_job_id, subject_kind::text, subject_id,
                   chunk_version_id, semantic_pair_digest,
                   admitted_pair_digest, scope_contract_digest,
                   requirement_registry_snapshot_digest,
                   active_chunk_snapshot_digest, role_template_hash,
                   execution_spec_hash, expandable, payload_hash, job_state,
                   result_artifact_id, result_artifact_hash,
                   scope_closure_digest, child_set_hash, archive_reason,
                   completion_digest, cancelled_by_event_id,
                   cancelled_by_epoch_id, cancellation_reason,
                   created_revision, completed_revision, created_at, completed_at
            FROM groundloop_m5_semantic_job
            WHERE logical_job_id = %s
            """,
            (root_id,),
        ).fetchone()
        scope_raw = cursor.execute(
            """
            SELECT root_job_id, epoch_id, direction,
                   requirement_version_id, inserted_chunk_version_id,
                   candidate_policy_id,
                   requirement_registry_snapshot_digest,
                   active_chunk_snapshot_digest, scope_contract_digest,
                   scope_state, created_revision,
                   staged_result_artifact_hash, scope_closure_digest,
                   child_set_hash, completion_digest, staged_revision,
                   closed_revision, created_at, closed_at
            FROM groundloop_m5_discovery_scope
            WHERE root_job_id = %s
            """,
            (root_id,),
        ).fetchone()
        if root_raw is None or scope_raw is None:
            raise EventConflictError("candidate root/scope closure disappeared")
        root_row = tuple(root_raw)
        scope_row = tuple(scope_raw)
        root = _m5_job_spec_from_row(root_row, context="candidate source")
        try:
            scope = M5DiscoveryScopeContract(
                direction=M5DiscoveryDirection(str(scope_row[2])),
                requirement_version_id=(
                    None if scope_row[3] is None else str(scope_row[3])
                ),
                inserted_chunk_version_id=(
                    None if scope_row[4] is None else str(scope_row[4])
                ),
                candidate_policy_id=_strip(scope_row[5]),
                requirement_registry_snapshot_digest=_strip(scope_row[6]),
                active_chunk_snapshot_digest=_strip(scope_row[7]),
                scope_contract_digest=_strip(scope_row[8]),
            )
        except (ValueError, ValidationError) as error:
            raise EventConflictError("candidate scope identity is invalid") from error
        manifest = _policy_by_id(cursor, scope.candidate_policy_id)
        if (
            root.logical_job_id != root_id
            or root.parent_job_id is not None
            or root.scope_contract_digest != scope.scope_contract_digest
            or root.candidate_policy_id != scope.candidate_policy_id
            or root.candidate_policy_manifest_hash != manifest.manifest_hash
            or root.requirement_registry_snapshot_digest
            != scope.requirement_registry_snapshot_digest
            or root.active_chunk_snapshot_digest != scope.active_chunk_snapshot_digest
            or int(scope_row[1]) != int(root_row[1])
        ):
            raise EventConflictError("candidate root/scope manifest closure changed")
        try:
            root.validate_manifest_and_scope(manifest, scope)
        except ValidationError as error:
            raise EventConflictError(
                "candidate root manifest/scope binding changed"
            ) from error
        child_ids = tuple(str(row[2]) for row in dependency_rows)
        for dependency in dependency_rows:
            if (
                int(dependency[0]) != int(root_row[1])
                or str(dependency[1]) != root_id
                or str(dependency[2]) not in locator.verifier_job_ids
            ):
                raise EventConflictError("candidate dependency closure changed")
        for child_id in child_ids:
            child_raw = cursor.execute(
                """
                SELECT logical_job_id, epoch_id, structural_event_id, job_kind,
                       candidate_policy_id, candidate_policy_manifest_hash,
                       parent_job_id, subject_kind::text, subject_id,
                       chunk_version_id, semantic_pair_digest,
                       admitted_pair_digest, scope_contract_digest,
                       requirement_registry_snapshot_digest,
                       active_chunk_snapshot_digest, role_template_hash,
                       execution_spec_hash, expandable, payload_hash, job_state,
                       result_artifact_id, result_artifact_hash,
                       scope_closure_digest, child_set_hash, archive_reason,
                       completion_digest, cancelled_by_event_id,
                       cancelled_by_epoch_id, cancellation_reason,
                       created_revision, completed_revision, created_at, completed_at
                FROM groundloop_m5_semantic_job
                WHERE logical_job_id = %s
                """,
                (child_id,),
            ).fetchone()
            if child_raw is None:
                raise EventConflictError("candidate verifier child disappeared")
            child_row = tuple(child_raw)
            child = _m5_job_spec_from_row(child_row, context="candidate verifier")
            if (
                child.parent_job_id != root_id
                or int(child_row[1]) != int(root_row[1])
                or child.scope_contract_digest != root.scope_contract_digest
                or child.candidate_policy_id != root.candidate_policy_id
                or child.candidate_policy_manifest_hash
                != root.candidate_policy_manifest_hash
                or child.requirement_registry_snapshot_digest
                != root.requirement_registry_snapshot_digest
                or child.active_chunk_snapshot_digest
                != root.active_chunk_snapshot_digest
            ):
                raise EventConflictError("candidate verifier child closure changed")
            try:
                child.validate_manifest_and_scope(manifest, scope)
            except ValidationError as error:
                raise EventConflictError(
                    "candidate verifier manifest/scope binding changed"
                ) from error
            _validate_m5_job_state(child, child_row, ())
        _validate_m5_job_state(root, root_row, child_ids)
        _validate_m5_scope_state(root, root_row, scope_row)
        try:
            hits = tuple(
                M5RequirementChannelHit(
                    epoch_id=int(row[1]),
                    root_job_id=_strip(row[2]),
                    scope_contract_digest=_strip(row[3]),
                    pair=SemanticPairKey(
                        SubjectKind(str(row[5])), str(row[6]), str(row[7])
                    ),
                    semantic_pair_digest=_strip(row[8]),
                    candidate_policy_id=_strip(row[4]),
                    channel=M5RequirementAdmissionChannel(str(row[9])),
                    rank=int(row[10]),
                    score=None if row[11] is None else float(row[11]),
                    channel_artifact_hash=_strip(row[12]),
                    hit_digest=_strip(row[0]),
                )
                for row in hit_rows
            )
            selections = tuple(
                M5RequirementScopeSelection(
                    root_job_id=_strip(row[1]),
                    scope_contract_digest=_strip(row[2]),
                    pair=SemanticPairKey(
                        SubjectKind(str(row[3])), str(row[4]), str(row[5])
                    ),
                    semantic_pair_digest=_strip(row[6]),
                    fused_rank=int(row[7]),
                    reasons=tuple(
                        M5RequirementAdmissionChannel(str(reason)) for reason in row[8]
                    ),
                    mandatory_lineage=bool(row[9]),
                    selection_digest=_strip(row[0]),
                )
                for row in selection_rows
            )
            result = M5RequirementDiscoveryResult(
                root_job_id=_strip(result_row[2]),
                scope_contract_digest=_strip(result_row[3]),
                termination=M5RetrievalTermination(str(result_row[4])),
                channel_hits=hits,
                selections=selections,
                approximate_selection_count=int(result_row[7]),
                mandatory_lineage_only_count=int(result_row[8]),
                result_artifact_hash=_strip(result_row[1]),
                result_artifact_id=_strip(result_row[0]),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError(
                "candidate discovery closure digest is invalid"
            ) from error
        if (
            len(hits) != int(result_row[5])
            or len(selections) != int(result_row[6])
            or result.root_job_id != root_id
            or result.scope_contract_digest != scope.scope_contract_digest
            or int(result_row[9]) != int(root_row[1])
            or result.result_artifact_id != _strip(root_row[20])
            or result.result_artifact_hash != _strip(root_row[21])
            or result.result_artifact_hash != _strip(scope_row[11])
        ):
            raise EventConflictError("candidate discovery closure changed")


def _lock_d29_verifier_provenance(
    cursor: Cursor[Any], locator: _D29LocatorAuthority
) -> dict[str, _RequirementVerifierAuthority]:
    authority: dict[str, _RequirementVerifierAuthority] = {}
    for observation_id in locator.verifier_observation_ids:
        execution = cursor.execute(
            """
            SELECT observation_id, artifact_id, artifact_hash,
                   logical_job_id, attempt_id, pair_input_hash,
                   decision_policy_version, decision_policy_hash,
                   eligible_for_currency, produced_epoch_id
            FROM groundloop_m5_requirement_verifier_execution
            WHERE observation_id = %s
            """,
            (observation_id,),
        ).fetchone()
        if execution is None:
            raise EventConflictError("verifier execution disappeared")
        artifact = cursor.execute(
            """
            SELECT artifact_id, artifact_hash, subject_kind::text, subject_id,
                   chunk_version_id, semantic_pair_digest, pair_input_hash,
                   execution_spec_hash, model_artifact_id, model_id,
                   model_revision, prompt_artifact_id, prompt_version,
                   calibration_version, calibration_artifact_hash,
                   temperature, decision_policy_version,
                   decision_policy_hash, support_score, refute_score,
                   neutral_score, raw_logit_contradiction,
                   raw_logit_entailment, raw_logit_neutral,
                   raw_output_hash, operational_label
            FROM groundloop_m5_requirement_verifier_artifact
            WHERE artifact_id = %s
            """,
            (_strip(execution[1]),),
        ).fetchone()
        pair_input = cursor.execute(
            """
            SELECT pair_input_hash, subject_kind::text, subject_id,
                   chunk_version_id, semantic_pair_digest,
                   scope_contract_digest, candidate_policy_id,
                   owner_claim_id, group_version_id, group_family_id,
                   requirement_ordinal, normalized_requirement_text,
                   requirement_text_hash, document_version_id, chunk_index,
                   chunk_text, stored_chunk_text_hash, m5_chunk_text_hash,
                   chunker_artifact_id, normalizer_id,
                   normalizer_provenance_hash
            FROM groundloop_m5_requirement_pair_input
            WHERE pair_input_hash = %s
            """,
            (_strip(execution[5]),),
        ).fetchone()
        if artifact is None or pair_input is None:
            raise EventConflictError("verifier artifact/input closure is incomplete")
        try:
            pair = SemanticPairKey(
                SubjectKind(str(pair_input[1])),
                str(pair_input[2]),
                str(pair_input[3]),
            )
            checked_input = M5RequirementPairInput(
                pair=pair,
                semantic_pair_digest=_strip(pair_input[4]),
                scope_contract_digest=_strip(pair_input[5]),
                candidate_policy_id=_strip(pair_input[6]),
                owner_claim_id=str(pair_input[7]),
                group_version_id=str(pair_input[8]),
                group_family_id=str(pair_input[9]),
                requirement_ordinal=int(pair_input[10]),
                normalized_requirement_text=str(pair_input[11]),
                requirement_text_hash=_strip(pair_input[12]),
                document_version_id=str(pair_input[13]),
                chunk_index=int(pair_input[14]),
                chunk_text=str(pair_input[15]),
                stored_chunk_text_hash=_strip(pair_input[16]),
                m5_chunk_text_hash=_strip(pair_input[17]),
                chunker_artifact_id=str(pair_input[18]),
                normalizer_id=str(pair_input[19]),
                normalizer_provenance_hash=_strip(pair_input[20]),
                pair_input_hash=_strip(pair_input[0]),
            )
            checked_artifact = M5RequirementVerifierArtifact(
                artifact_id=_strip(artifact[0]),
                artifact_hash=_strip(artifact[1]),
                pair=pair,
                semantic_pair_digest=_strip(artifact[5]),
                pair_input_hash=_strip(artifact[6]),
                execution_spec_hash=_strip(artifact[7]),
                model_artifact_id=str(artifact[8]),
                model_id=str(artifact[9]),
                model_revision=str(artifact[10]),
                prompt_artifact_id=str(artifact[11]),
                prompt_version=str(artifact[12]),
                calibration_version=str(artifact[13]),
                calibration_artifact_hash=(
                    None if artifact[14] is None else _strip(artifact[14])
                ),
                temperature=float(artifact[15]),
                decision_policy_version=str(artifact[16]),
                decision_policy_hash=_strip(artifact[17]),
                support_score=float(artifact[18]),
                refute_score=float(artifact[19]),
                neutral_score=float(artifact[20]),
                raw_logits=(
                    float(artifact[21]),
                    float(artifact[22]),
                    float(artifact[23]),
                ),
                raw_output_hash=_strip(artifact[24]),
                operational_label=VerificationLabel(str(artifact[25])),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError(
                "verifier artifact/input digest closure is invalid"
            ) from error
        job = cursor.execute(
            """
            SELECT logical_job_id, epoch_id, candidate_policy_id,
                   semantic_pair_digest, scope_contract_digest,
                   execution_spec_hash, result_artifact_id,
                   result_artifact_hash, job_state, completed_revision
            FROM groundloop_m5_semantic_job
            WHERE logical_job_id = %s
            """,
            (_strip(execution[3]),),
        ).fetchone()
        model = cursor.execute(
            """
            SELECT model_artifact_id, task, model_id, immutable_revision
            FROM groundloop_model_artifact
            WHERE model_artifact_id = %s
            """,
            (checked_artifact.model_artifact_id,),
        ).fetchone()
        prompt = cursor.execute(
            """
            SELECT prompt_artifact_id, task, version
            FROM groundloop_prompt_artifact
            WHERE prompt_artifact_id = %s
            """,
            (checked_artifact.prompt_artifact_id,),
        ).fetchone()
        policy_row = cursor.execute(
            """
            SELECT policy_version, support_threshold, refute_threshold,
                   tie_rule_version
            FROM groundloop_decision_policy
            WHERE policy_version = %s
            """,
            (checked_artifact.decision_policy_version,),
        ).fetchone()
        try:
            if policy_row is None:
                raise ValidationError("verifier decision policy disappeared")
            decision_policy = DecisionPolicy(
                str(policy_row[0]),
                float(policy_row[1]),
                float(policy_row[2]),
                str(policy_row[3]),
            )
            checked_artifact.validate_decision_policy(decision_policy)
            expected_observation = checked_artifact.to_semantic_observation()
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError(
                "verifier immutable provenance is invalid"
            ) from error
        if job is None or (
            checked_artifact.artifact_id != _strip(execution[1])
            or checked_artifact.artifact_hash != _strip(execution[2])
            or checked_input.pair_input_hash != _strip(execution[5])
            or checked_artifact.pair_input_hash != checked_input.pair_input_hash
            or checked_artifact.decision_policy_version != str(execution[6])
            or checked_artifact.decision_policy_hash != _strip(execution[7])
            or execution[8] is not True
            or _strip(job[0]) not in locator.verifier_job_ids
            or int(job[1]) != int(execution[9])
            or _strip(job[2]) != checked_input.candidate_policy_id
            or _strip(job[3]) != pair.semantic_pair_digest
            or _strip(job[4]) != checked_input.scope_contract_digest
            or _strip(job[5]) != checked_artifact.execution_spec_hash
            or _strip(job[6]) != checked_artifact.artifact_id
            or _strip(job[7]) != checked_artifact.artifact_hash
            or job[8] != "completed_active"
            or type(job[9]) is not int
            or int(job[9]) < 1
            or expected_observation.observation_id != observation_id
            or model is None
            or tuple(model)
            != (
                checked_artifact.model_artifact_id,
                "verification",
                checked_artifact.model_id,
                checked_artifact.model_revision,
            )
            or prompt is None
            or tuple(prompt)
            != (
                checked_artifact.prompt_artifact_id,
                "verification",
                checked_artifact.prompt_version,
            )
        ):
            raise EventConflictError("verifier execution/artifact/input changed")
        authority[observation_id] = _RequirementVerifierAuthority(
            observation=expected_observation,
            raw_output_hash=checked_artifact.raw_output_hash,
            produced_epoch_id=int(str(execution[9])),
            installed_revision=int(job[9]),
            eligible_for_currency=bool(execution[8]),
            candidate_policy_id=checked_input.candidate_policy_id,
        )
    if len(authority) != len(locator.verifier_observation_ids):
        raise EventConflictError("verifier observation provenance is ambiguous")
    return authority


def _lock_d29_m5_attempt_and_output_rows(
    cursor: Cursor[Any], locator: _D29LocatorAuthority
) -> None:
    """Acquire every gathered M5 attempt before every attempt output."""

    job_ids = tuple(
        sorted(
            set(locator.candidate_root_job_ids)
            | set(locator.verifier_root_job_ids)
            | set(locator.verifier_job_ids),
            key=_c_key,
        )
    )
    for job_id in job_ids:
        cursor.execute(
            """
            SELECT attempt_id
            FROM groundloop_m5_job_attempt
            WHERE logical_job_id = %s
            ORDER BY attempt_ordinal, attempt_id
            FOR UPDATE
            """,
            (job_id,),
        ).fetchall()
    for job_id in job_ids:
        cursor.execute(
            """
            SELECT attempt_result_artifact_id
            FROM groundloop_m5_attempt_result_artifact
            WHERE logical_job_id = %s
            ORDER BY attempt_id
            FOR UPDATE
            """,
            (job_id,),
        ).fetchall()


def _direct_root_lock_coordinates(
    locator: _D29LocatorAuthority,
) -> tuple[tuple[int, str], ...]:
    return tuple(
        sorted(
            {
                (epoch_id, parent_id)
                for epoch_id, parent_id, _child_id in (
                    locator.direct_dependency_coordinates
                )
            },
            key=lambda item: (item[0], _c_key(item[1])),
        )
    )


def _lock_d30_typed_owner_evidence(
    cursor: Cursor[Any], locator: _D29LocatorAuthority
) -> None:
    """Lock and byte-revalidate every preliminary D24 typed-owner point."""

    typed = tuple(owner for owner in locator.d30_claims.owners if owner.d24 is not None)
    dispatch_projection = _d30_work_columns("maximum_")
    evidence_projection = _d30_work_columns("attempt_")
    contribution_points: dict[tuple[int, str, str], tuple[object, ...]] = {}
    timing_points: dict[tuple[int, str, str, int], tuple[object, ...]] = {}
    transition_points: dict[tuple[int, str], tuple[object, ...]] = {}
    for owner in typed:
        assert owner.d24 is not None
        for located in sorted(
            owner.d24.attempts,
            key=lambda item: (
                _c_key(item.job_id),
                int(str(item.dispatch_row[len(_D30_WORK_COUNTERS) + 5])),
                _c_key(item.attempt_id),
            ),
        ):
            dispatch = cursor.execute(
                f"""
                SELECT epoch_id, {dispatch_projection}, maximum_work_digest,
                       subgraph, attempt_id, logical_job_id, attempt_ordinal,
                       job_kind, fallback_required, dispatched_revision,
                       lease_expires_at, record_digest
                FROM groundloop_m5_dispatch_record
                WHERE epoch_id = %s AND subgraph = 'direct'
                  AND attempt_id = %s
                FOR UPDATE
                """,
                (owner.epoch_id, located.attempt_id),
            ).fetchone()
            evidence = cursor.execute(
                f"""
                SELECT epoch_id, {evidence_projection}, attempt_work_digest,
                       subgraph, attempt_id, disposition,
                       result_or_error_hash, attempt_timing_digest,
                       evidence_digest
                FROM groundloop_m5_attempt_execution_evidence
                WHERE epoch_id = %s AND subgraph = 'direct'
                  AND attempt_id = %s
                FOR UPDATE
                """,
                (owner.epoch_id, located.attempt_id),
            ).fetchone()
            timing = cursor.execute(
                """
                SELECT epoch_id, subgraph, attempt_id,
                       execution_evidence_digest, required_interval_observed,
                       coordinator_non_db_non_neural_ns, neural_wall_ns,
                       postgres_roundtrip_wall_ns, external_io_wall_ns,
                       end_to_end_wall_ns, postgres_server_execution_ns,
                       postgres_lock_wait_ns, postgres_wal_bytes,
                       postgres_shared_block_reads, observation_digest,
                       attempt_timing_digest
                FROM groundloop_m5_runtime_timing_contribution
                WHERE epoch_id = %s AND subgraph = 'direct'
                  AND attempt_id = %s
                FOR UPDATE
                """,
                (owner.epoch_id, located.attempt_id),
            ).fetchone()
            late_points = _d30_late_attempt_points(
                cursor,
                epoch_id=owner.epoch_id,
                attempt_id=located.attempt_id,
                lock_authority=True,
            )
            if (
                dispatch is None
                or tuple(dispatch) != located.dispatch_row
                or (None if evidence is None else tuple(evidence))
                != located.evidence_row
                or (None if timing is None else tuple(timing)) != located.timing_row
                or late_points
                != (
                    located.late_envelope_row,
                    located.expired_return_row,
                    located.postterminal_timing_row,
                    located.postterminal_audit_row,
                )
            ):
                raise EventConflictError("D30 typed attempt evidence changed")
            for contribution in (
                located.acquisition_contribution_row,
                located.attempt_contribution_row,
                located.transition_contribution_row,
                located.preterminal_late_contribution_row,
            ):
                if contribution is None:
                    continue
                offset = len(_D30_WORK_COUNTERS)
                contribution_key = (
                    int(str(contribution[0])),
                    str(contribution[offset + 2]),
                    str(contribution[offset + 3]),
                )
                previous = contribution_points.setdefault(
                    contribution_key, contribution
                )
                if previous != contribution:
                    raise EventConflictError("D30 contribution locator is ambiguous")
            for timing_row in located.transition_timing_rows:
                timing_key = (
                    int(str(timing_row[0])),
                    str(timing_row[1]),
                    str(timing_row[2]),
                    int(str(timing_row[4])),
                )
                previous = timing_points.setdefault(timing_key, timing_row)
                if previous != timing_row:
                    raise EventConflictError("D30 timing locator is ambiguous")
            if located.m4_transition_row is not None:
                transition_key = (
                    int(str(located.m4_transition_row[0])),
                    str(located.m4_transition_row[1]),
                )
                previous = transition_points.setdefault(
                    transition_key, located.m4_transition_row
                )
                if previous != located.m4_transition_row:
                    raise EventConflictError("D30 M4 transition locator is ambiguous")
        seal = owner.d24.seal_contribution_row
        offset = len(_D30_WORK_COUNTERS)
        contribution_points[
            (int(str(seal[0])), str(seal[offset + 2]), str(seal[offset + 3]))
        ] = seal

    for contribution_key in sorted(
        contribution_points,
        key=lambda item: (item[0], _c_key(item[1]), _c_key(item[2])),
    ):
        row = _d30_contribution_point(
            cursor,
            epoch_id=contribution_key[0],
            contribution_kind=contribution_key[1],
            source_id=contribution_key[2],
            lock_authority=True,
        )
        if row != contribution_points[contribution_key]:
            raise EventConflictError("D30 typed work contribution changed")
    for timing_key in sorted(
        timing_points,
        key=lambda item: (item[0], _c_key(item[1]), _c_key(item[2]), item[3]),
    ):
        row = _d30_transition_timing_point(
            cursor,
            epoch_id=timing_key[0],
            contribution_kind=timing_key[1],
            source_id=timing_key[2],
            anchor_revision=timing_key[3],
            lock_authority=True,
        )
        if row != timing_points[timing_key]:
            raise EventConflictError("D30 typed transition timing changed")
    for transition_key in sorted(
        transition_points, key=lambda item: (item[0], _c_key(item[1]))
    ):
        row = cursor.execute(
            """
            SELECT epoch_id, transition_id, payload_hash, transition_kind,
                   from_revision, to_revision, override_rows_written
            FROM groundloop_m4_evaluation_counter_transition
            WHERE epoch_id = %s AND transition_id = %s
            FOR UPDATE
            """,
            transition_key,
        ).fetchone()
        if row is None or tuple(row) != transition_points[transition_key]:
            raise EventConflictError("D30 typed M4 transition changed")

    for owner in typed:
        assert owner.d24 is not None
        work_accumulator = cursor.execute(
            f"""
            SELECT epoch_id, {_d30_work_columns("")}, work_digest,
                   updated_revision, terminalized
            FROM groundloop_m5_runtime_work_accumulator
            WHERE epoch_id = %s FOR UPDATE
            """,
            (owner.epoch_id,),
        ).fetchone()
        timing_accumulator = cursor.execute(
            """
            SELECT epoch_id,
                   coordinator_non_db_non_neural_ns, neural_wall_ns,
                   postgres_roundtrip_wall_ns, external_io_wall_ns,
                   end_to_end_wall_ns, postgres_server_execution_ns,
                   postgres_lock_wait_ns, postgres_wal_bytes,
                   postgres_shared_block_reads,
                   required_expected_count, required_observed_count,
                   required_missing_count,
                   postgres_server_execution_expected_count,
                   postgres_server_execution_observed_count,
                   postgres_server_execution_missing_count,
                   postgres_lock_wait_expected_count,
                   postgres_lock_wait_observed_count,
                   postgres_lock_wait_missing_count,
                   postgres_wal_bytes_expected_count,
                   postgres_wal_bytes_observed_count,
                   postgres_wal_bytes_missing_count,
                   postgres_shared_block_reads_expected_count,
                   postgres_shared_block_reads_observed_count,
                   postgres_shared_block_reads_missing_count,
                   pending_contribution_kind, pending_source_id,
                   pending_contribution_key_digest, pending_anchor_revision,
                   updated_revision, terminalized
            FROM groundloop_m5_runtime_timing_accumulator
            WHERE epoch_id = %s FOR UPDATE
            """,
            (owner.epoch_id,),
        ).fetchone()
        terminal_rows = _d30_terminal_rows(
            cursor,
            structural_event_id=str(owner.epoch_row[1]),
            epoch_id=owner.epoch_id,
            lock_authority=True,
        )
        rerun_result = _load_canonical_terminal_result(
            cursor,
            structural_event_id=str(owner.epoch_row[1]),
            payload_hash=_strip(owner.epoch_row[2]),
        )
        rerun_call_work = _load_terminal_work(
            cursor,
            structural_event_id=str(owner.epoch_row[1]),
            epoch_id=owner.epoch_id,
            work_kind="call",
        )
        if (
            work_accumulator is None
            or tuple(work_accumulator) != owner.d24.work_accumulator_row
            or timing_accumulator is None
            or tuple(timing_accumulator) != owner.d24.timing_accumulator_row
            or terminal_rows != owner.d24.terminal_rows
            or rerun_result != owner.d24.terminal_result
            or rerun_call_work != owner.d24.terminal_call_work
        ):
            raise EventConflictError("D30 typed terminal accounting changed")


def _lock_d29_tier_10_authority(
    cursor: Cursor[Any],
    locator: _D29LocatorAuthority,
    *,
    predecessor_epoch_id: int,
    candidate_policy_id: str,
) -> tuple[dict[str, _DirectAttemptAuthority], tuple[CandidateDependency, ...]]:
    """Acquire the gathered tier-10 closure in the frozen global order."""

    # 10.1: every attempt, then every durable attempt output/result.
    direct_attempts = _lock_d29_direct_attempts(cursor, locator)
    _lock_d29_m5_attempt_and_output_rows(cursor, locator)
    _lock_d30_typed_owner_evidence(cursor, locator)

    preliminary_projections: dict[tuple[int, str], tuple[object, ...] | None] = {}
    for owner in locator.d30_claims.owners:
        for job_id, row in owner.projections:
            projection_coordinate = (owner.epoch_id, job_id)
            if projection_coordinate in preliminary_projections:
                raise EventConflictError("D30 direct terminal projection changed")
            preliminary_projections[projection_coordinate] = row
    projection_coordinates = tuple(
        sorted(
            preliminary_projections,
            key=lambda item: (item[0], _c_key(item[1])),
        )
    )
    if projection_coordinates != locator.direct_job_coordinates:
        raise EventConflictError("D30 direct terminal projection changed")
    for projection_epoch_id, job_id in locator.direct_job_coordinates:
        projection_coordinate = (projection_epoch_id, job_id)
        if projection_coordinate not in preliminary_projections:
            raise EventConflictError("D30 direct terminal projection changed")
        row = cursor.execute(
            """
            SELECT epoch_id, job_id, terminal_state, terminal_reason,
                   m4_completion_digest, completed_revision,
                   terminal_identity_hash
            FROM groundloop_m5_direct_terminal_projection
            WHERE epoch_id = %s AND job_id = %s
            FOR UPDATE
            """,
            (projection_epoch_id, job_id),
        ).fetchone()
        if (None if row is None else tuple(row)) != preliminary_projections[
            projection_coordinate
        ]:
            raise EventConflictError("D30 direct terminal projection changed")

    # 10.2: all direct-M4 dependencies, then all M5 dependencies.
    locked_direct_dependencies: list[tuple[int, str, str]] = []
    for direct_dependency_coordinate in locator.direct_dependency_coordinates:
        row = cursor.execute(
            """
            SELECT epoch_id, parent_job_id, child_job_id
            FROM groundloop_semantic_job_dependency
            WHERE epoch_id = %s AND parent_job_id = %s AND child_job_id = %s
            FOR UPDATE
            """,
            direct_dependency_coordinate,
        ).fetchone()
        if row is None:
            raise EventConflictError("direct locator dependency disappeared")
        locked_direct_dependencies.append((int(str(row[0])), str(row[1]), str(row[2])))
    if tuple(locked_direct_dependencies) != locator.direct_dependency_coordinates:
        raise EventConflictError("direct locator dependency closure changed")
    for owner in locator.d30_claims.owners:
        rerun = tuple(
            tuple(row)
            for row in cursor.execute(
                """
                SELECT epoch_id, parent_job_id, child_job_id
                FROM groundloop_semantic_job_dependency
                WHERE epoch_id = %s
                ORDER BY parent_job_id COLLATE "C", child_job_id COLLATE "C"
                """,
                (owner.epoch_id,),
            ).fetchall()
        )
        if rerun != owner.dependencies:
            raise EventConflictError("D30 direct dependency epoch range changed")

    expected_m5_dependencies = set(locator.candidate_dependency_coordinates) | set(
        locator.verifier_dependency_coordinates
    )
    m5_dependency_roots = {
        (epoch_id, parent_id)
        for epoch_id, parent_id, _child_id in expected_m5_dependencies
    } | set(locator.candidate_root_job_coordinates)
    locked_m5_dependencies: set[tuple[int, str, str]] = set()
    for epoch_id, root_id in sorted(
        m5_dependency_roots, key=lambda item: (item[0], _c_key(item[1]))
    ):
        locked_m5_dependencies.update(
            _locate_d29_m5_dependency_edges(
                cursor, epoch_id=epoch_id, parent_job_id=root_id
            )
        )
    if locked_m5_dependencies != expected_m5_dependencies:
        raise EventConflictError("M5 locator dependency closure changed")
    for m5_dependency_coordinate in sorted(
        locked_m5_dependencies,
        key=lambda item: (item[0], _c_key(item[1]), _c_key(item[2])),
    ):
        _lock_d29_m5_dependency_edge(cursor, m5_dependency_coordinate)

    direct_roots = tuple(
        sorted(
            (
                (owner.epoch_id, str(job[0]))
                for owner in locator.d30_claims.owners
                for job in owner.jobs
                if job[2] is None
            ),
            key=lambda item: (item[0], _c_key(item[1])),
        )
    )

    # 10.3: every discovery-result header before any channel hit.
    preliminary_parent_results = {
        job_id: result
        for owner in locator.d30_claims.owners
        for job_id, result in owner.discovery_results
    }
    for _epoch_id, root_id in direct_roots:
        row = cursor.execute(
            """
            SELECT root_job_id, epoch_id, result_artifact_id,
                   result_artifact_hash, fallback_satisfied,
                   channel_hit_count, admitted_pair_count,
                   channel_set_hash, admitted_pair_set_hash
            FROM groundloop_m4_discovery_result
            WHERE root_job_id = %s
            FOR UPDATE
            """,
            (root_id,),
        ).fetchone()
        expected = preliminary_parent_results.get(root_id)
        if expected is None or row is None or tuple(row) != expected:
            raise EventConflictError("D30 parent discovery result changed")
    for _epoch_id, root_id in locator.candidate_root_job_coordinates:
        cursor.execute(
            """
            SELECT root_job_id
            FROM groundloop_m5_requirement_discovery_result
            WHERE root_job_id = %s
            FOR UPDATE
            """,
            (root_id,),
        ).fetchone()

    # 10.4: D30 deliberately performs no direct-root hit range.
    for _epoch_id, root_id in locator.candidate_root_job_coordinates:
        cursor.execute(
            """
            SELECT hit_digest
            FROM groundloop_m5_requirement_channel_hit
            WHERE root_job_id = %s
            ORDER BY channel COLLATE "C", rank,
                     semantic_pair_digest COLLATE "C"
            FOR UPDATE
            """,
            (root_id,),
        ).fetchall()

    # 10.5: all selections precede every admitted pair/source row.
    for _epoch_id, root_id in locator.candidate_root_job_coordinates:
        cursor.execute(
            """
            SELECT selection_digest
            FROM groundloop_m5_requirement_scope_selection
            WHERE root_job_id = %s
            ORDER BY fused_rank, semantic_pair_digest COLLATE "C"
            FOR UPDATE
            """,
            (root_id,),
        ).fetchall()

    # 10.6: direct admissions, M5 admissions, then M5 source rows.
    preliminary_admissions = {
        claim.admitted_pair_id: claim.admitted_pair_row
        for claim in locator.d30_claims.dynamic
    }
    for admitted_pair_id in locator.direct_admitted_pair_ids:
        row = cursor.execute(
            """
            SELECT admitted_pair_id, epoch_id, claim_id, chunk_version_id,
                   candidate_policy_id, fused_rank, reasons,
                   mandatory_lineage
            FROM groundloop_admitted_pair
            WHERE admitted_pair_id = %s
            FOR UPDATE
            """,
            (admitted_pair_id,),
        ).fetchone()
        if (
            row is None
            or admitted_pair_id not in preliminary_admissions
            or tuple(row) != preliminary_admissions[admitted_pair_id]
        ):
            raise EventConflictError("D30 cited admitted-pair point changed")
    for admitted_digest in locator.qualifying_admitted_pair_digests:
        if (
            cursor.execute(
                """
                SELECT admitted_pair_digest
                FROM groundloop_m5_requirement_admitted_pair
                WHERE admitted_pair_digest = %s
                FOR UPDATE
                """,
                (admitted_digest,),
            ).fetchone()
            is None
        ):
            raise EventConflictError("candidate admission disappeared before lock")
    for admitted_digest in locator.qualifying_admitted_pair_digests:
        cursor.execute(
            """
            SELECT admitted_pair_digest, root_job_id
            FROM groundloop_m5_requirement_admitted_pair_source
            WHERE admitted_pair_digest = %s
            ORDER BY root_job_id COLLATE "C"
            FOR UPDATE
            """,
            (admitted_digest,),
        ).fetchall()

    # 10.7: all verifier executions.
    for claim in locator.d30_claims.dynamic:
        locked_execution_rows = _d30_execution_coordinates(
            cursor,
            observation_id=claim.currency.observation_id,
            job_id=claim.child_job_id,
            admitted_pair_id=claim.admitted_pair_id,
            lock_authority=True,
        )
        _validate_d30_locked_execution_coordinates(
            preliminary_rows=claim.execution_rows,
            locked_rows=locked_execution_rows,
        )
    for observation_id in locator.verifier_observation_ids:
        if (
            cursor.execute(
                """
                SELECT observation_id
                FROM groundloop_m5_requirement_verifier_execution
                WHERE observation_id = %s
                FOR UPDATE
                """,
                (observation_id,),
            ).fetchone()
            is None
        ):
            raise EventConflictError("requirement verifier execution disappeared")

    # D30 activation-base rows are exact output/result points.  Deduplicate
    # every shared coordinate before acquisition, then lock each relation in
    # the frozen M3 relation/primary-key order.  Epoch rows are immutable
    # sealed history and are byte-reread without an earlier lock.
    m3_epoch_points: dict[int, tuple[object, ...]] = {}
    m3_execution_points: dict[str, tuple[object, ...]] = {}
    m3_run_points: dict[str, tuple[object, ...]] = {}
    m3_candidate_points: dict[str, tuple[object, ...]] = {}
    m3_embedding_points: dict[tuple[str, str], tuple[object, ...]] = {}
    m3_artifact_use_points: dict[tuple[str, str, str], tuple[object, ...]] = {}
    for bootstrap_claim in locator.d30_claims.bootstrap:
        _remember_d30_point(
            m3_epoch_points,
            int(str(bootstrap_claim.epoch_row[0])),
            bootstrap_claim.epoch_row,
            label="activation-base M3 epoch coordinate",
        )
        _remember_d30_point(
            m3_execution_points,
            bootstrap_claim.currency.observation_id,
            bootstrap_claim.execution_row,
            label="activation-base M3 execution coordinate",
        )
        _remember_d30_point(
            m3_run_points,
            str(bootstrap_claim.run_row[0]),
            bootstrap_claim.run_row,
            label="activation-base M3 run coordinate",
        )
        _remember_d30_point(
            m3_candidate_points,
            str(bootstrap_claim.candidate_row[0]),
            bootstrap_claim.candidate_row,
            label="activation-base M3 candidate coordinate",
        )
        _remember_d30_point(
            m3_embedding_points,
            (
                bootstrap_claim.currency.chunk_version_id,
                str(bootstrap_claim.embedding_model_row[0]),
            ),
            bootstrap_claim.chunk_embedding_row,
            label="activation-base M3 embedding coordinate",
        )
        for expected_use in bootstrap_claim.artifact_use_rows:
            use_coordinate = (
                str(expected_use[0]),
                str(expected_use[1]),
                str(expected_use[2]),
            )
            _remember_d30_point(
                m3_artifact_use_points,
                use_coordinate,
                expected_use,
                label="activation-base artifact use coordinate",
            )

    for epoch_id in sorted(m3_epoch_points):
        epoch = cursor.execute(
            """
            SELECT epoch_id, event_id, payload_hash, revision,
                   structural_status, semantic_status, evaluation_state,
                   publication_mode, sealed_at
            FROM groundloop_epoch WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        if epoch is None or tuple(epoch) != m3_epoch_points[epoch_id]:
            raise EventConflictError("D30 activation-base M3 epoch changed")

    for observation_id in sorted(m3_execution_points, key=_c_key):
        execution = cursor.execute(
            """
            SELECT observation_id, run_id, candidate_id, model_artifact_id,
                   prompt_artifact_id, calibration_version, temperature,
                   raw_logits, raw_output_hash, reused_from_observation_id
            FROM groundloop_verification_execution
            WHERE observation_id = %s
            FOR UPDATE
            """,
            (observation_id,),
        ).fetchone()
        if execution is None or tuple(execution) != m3_execution_points[observation_id]:
            raise EventConflictError("D30 activation-base M3 execution changed")

    for run_id in sorted(m3_run_points, key=_c_key):
        run = cursor.execute(
            """
            SELECT run_id, schema_version, status, config_hash, input_hash,
                   corpus_hash, question_id, answer_version_id,
                   semantic_epoch_id, manifest, failure_code, started_at,
                   completed_at
            FROM groundloop_pipeline_run WHERE run_id = %s
            FOR UPDATE
            """,
            (run_id,),
        ).fetchone()
        if run is None or tuple(run) != m3_run_points[run_id]:
            raise EventConflictError("D30 activation-base M3 run changed")

    for candidate_id in sorted(m3_candidate_points, key=_c_key):
        candidate = cursor.execute(
            """
            SELECT candidate_id, run_id, query_kind, query_id, claim_id,
                   chunk_version_id, embedding_model_artifact_id,
                   method_version, score, rank
            FROM groundloop_retrieval_candidate WHERE candidate_id = %s
            FOR UPDATE
            """,
            (candidate_id,),
        ).fetchone()
        if candidate is None or tuple(candidate) != m3_candidate_points[candidate_id]:
            raise EventConflictError("D30 activation-base M3 candidate changed")

    model_points: dict[str, tuple[object, ...]] = {}
    prompt_points: dict[str, tuple[object, ...]] = {}
    for dynamic_claim in locator.d30_claims.dynamic:
        if dynamic_claim.model_row is not None:
            artifact_id = str(dynamic_claim.model_row[0])
            _remember_d30_point(
                model_points,
                artifact_id,
                dynamic_claim.model_row,
                label="named model artifact",
            )
        if dynamic_claim.prompt_row is not None:
            artifact_id = str(dynamic_claim.prompt_row[0])
            _remember_d30_point(
                prompt_points,
                artifact_id,
                dynamic_claim.prompt_row,
                label="named prompt artifact",
            )
    for bootstrap_claim in locator.d30_claims.bootstrap:
        for expected_model in (
            bootstrap_claim.model_row,
            bootstrap_claim.embedding_model_row,
        ):
            artifact_id = str(expected_model[0])
            _remember_d30_point(
                model_points,
                artifact_id,
                expected_model,
                label="named model artifact",
            )
        artifact_id = str(bootstrap_claim.prompt_row[0])
        _remember_d30_point(
            prompt_points,
            artifact_id,
            bootstrap_claim.prompt_row,
            label="named prompt artifact",
        )
    for artifact_id in sorted(model_points, key=_c_key):
        row = cursor.execute(
            """
            SELECT model_artifact_id, task, provider, model_id,
                   immutable_revision, tokenizer_revision, license_id,
                   config_hash, artifact_sha256
            FROM groundloop_model_artifact WHERE model_artifact_id = %s
            FOR UPDATE
            """,
            (artifact_id,),
        ).fetchone()
        if row is None or tuple(row) != model_points[artifact_id]:
            raise EventConflictError("D30 named model artifact changed")
    for artifact_id in sorted(prompt_points, key=_c_key):
        row = cursor.execute(
            """
            SELECT prompt_artifact_id, task, version, template,
                   template_hash, decoding_config_hash
            FROM groundloop_prompt_artifact WHERE prompt_artifact_id = %s
            FOR UPDATE
            """,
            (artifact_id,),
        ).fetchone()
        if row is None or tuple(row) != prompt_points[artifact_id]:
            raise EventConflictError("D30 named prompt artifact changed")
    for chunk_id, model_id in sorted(
        m3_embedding_points,
        key=lambda item: (_c_key(item[0]), _c_key(item[1])),
    ):
        embedding = cursor.execute(
            """
            SELECT chunk_version_id, model_artifact_id, embedding::text,
                   input_hash
            FROM groundloop_chunk_embedding
            WHERE chunk_version_id = %s AND model_artifact_id = %s
            FOR UPDATE
            """,
            (chunk_id, model_id),
        ).fetchone()
        if (
            embedding is None
            or tuple(embedding) != m3_embedding_points[(chunk_id, model_id)]
        ):
            raise EventConflictError("D30 activation-base embedding changed")
    for artifact_use_coordinate in sorted(
        m3_artifact_use_points,
        key=lambda item: (_c_key(item[0]), _c_key(item[1]), _c_key(item[2])),
    ):
        use = cursor.execute(
            """
            SELECT run_id, artifact_kind, artifact_id, reused
            FROM groundloop_pipeline_artifact_use
            WHERE run_id = %s AND artifact_kind = %s AND artifact_id = %s
            FOR UPDATE
            """,
            artifact_use_coordinate,
        ).fetchone()
        if use is None or tuple(use) != m3_artifact_use_points[artifact_use_coordinate]:
            raise EventConflictError("D30 activation-base artifact use changed")

    # 10.8 and 10.9: artifacts before pair inputs.
    for artifact_id in locator.verifier_artifact_ids:
        if (
            cursor.execute(
                """
                SELECT artifact_id
                FROM groundloop_m5_requirement_verifier_artifact
                WHERE artifact_id = %s
                FOR UPDATE
                """,
                (artifact_id,),
            ).fetchone()
            is None
        ):
            raise EventConflictError("requirement verifier artifact disappeared")
    for pair_input_hash in locator.verifier_pair_input_hashes:
        if (
            cursor.execute(
                """
                SELECT pair_input_hash
                FROM groundloop_m5_requirement_pair_input
                WHERE pair_input_hash = %s
                FOR UPDATE
                """,
                (pair_input_hash,),
            ).fetchone()
            is None
        ):
            raise EventConflictError("requirement pair input disappeared")

    # 10.10: frontier heads are the final tier-10 relation.
    direct_candidates = _lock_d29_direct_frontier(
        cursor,
        locator,
        predecessor_epoch_id=predecessor_epoch_id,
        candidate_policy_id=candidate_policy_id,
    )
    return direct_attempts, direct_candidates


def _lock_d29_direct_frontier(
    cursor: Cursor[Any],
    locator: _D29LocatorAuthority,
    *,
    predecessor_epoch_id: int,
    candidate_policy_id: str,
) -> tuple[CandidateDependency, ...]:
    """Lock and validate only the direct-frontier points found by locators."""

    dependencies: list[CandidateDependency] = []
    for claim_id, chunk_id, policy_id, valid_from_epoch in locator.direct_frontier_keys:
        row = cursor.execute(
            """
            SELECT frontier.claim_id, frontier.chunk_version_id,
                   frontier.candidate_policy_id, frontier.frontier_state,
                   frontier.rank, frontier.retrieval_score,
                   frontier.candidate_artifact_hash,
                   frontier.valid_from_epoch, frontier.valid_to_epoch,
                   creator.structural_status, creator.semantic_status,
                   creator.evaluation_state, creator.publication_mode,
                   creator.sealed_at, creator_update.candidate_policy_id
            FROM groundloop_candidate_frontier AS frontier
            JOIN groundloop_epoch AS creator
              ON creator.epoch_id = frontier.valid_from_epoch
            JOIN groundloop_m4_update AS creator_update
              ON creator_update.epoch_id = creator.epoch_id
            WHERE frontier.claim_id = %s
              AND frontier.chunk_version_id = %s
              AND frontier.candidate_policy_id = %s
              AND frontier.valid_from_epoch = %s
            FOR UPDATE OF frontier
            """,
            (claim_id, chunk_id, policy_id, valid_from_epoch),
        ).fetchone()
        if row is None:
            raise EventConflictError("direct frontier locator disappeared")
        try:
            entry = M4FrontierEntry(
                claim_id=str(row[0]),
                chunk_version_id=str(row[1]),
                candidate_policy_id=str(row[2]),
                state=M4FrontierState(str(row[3])),
                rank=int(str(row[4])),
                retrieval_score=float(str(row[5])),
                candidate_artifact_hash=_strip(row[6]),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError("direct frontier DTO is malformed") from error
        if (
            entry.claim_id != claim_id
            or entry.chunk_version_id != chunk_id
            or entry.candidate_policy_id != policy_id
            or int(row[7]) != valid_from_epoch
            or valid_from_epoch > predecessor_epoch_id
            or row[8] is not None
            or tuple(row[9:13]) != ("committed", "sealed", "complete", "strict")
            or row[13] is None
            or str(row[14]) != entry.candidate_policy_id
        ):
            raise EventConflictError("direct frontier authority is malformed")
        if entry.candidate_policy_id != candidate_policy_id:
            raise EventConflictError(
                "cross-policy direct candidate withdrawal is not authorized"
            )
        dependencies.append(
            CandidateDependency(
                stable_m4_digest(
                    "m4-frontier-edge-v1",
                    entry.claim_id,
                    entry.chunk_version_id,
                    entry.candidate_policy_id,
                    entry.candidate_artifact_hash,
                ),
                PairKey(entry.claim_id, entry.chunk_version_id),
            )
        )
    return tuple(
        sorted(
            set(dependencies),
            key=lambda item: (
                _c_key(item.pair.claim_id),
                _c_key(item.pair.chunk_version_id),
                _c_key(item.candidate_edge_id),
            ),
        )
    )


def _validate_d30_activation_row(
    activation: tuple[object, ...] | None,
    *,
    predecessor_epoch_id: int,
) -> int:
    if (
        activation is None
        or not str(activation[0]).strip()
        or len(_strip(activation[1])) != 64
        or str(activation[4]) != "m5_active"
        or type(activation[5]) is not int
        or int(activation[5]) < 1
        or int(str(activation[6])) != int(str(activation[7]))
        or int(str(activation[6])) != predecessor_epoch_id
        or type(activation[8]) is not int
        or tuple(activation[9:13]) != ("committed", "sealed", "complete", "strict")
        or activation[13] is None
    ):
        raise EventConflictError("D30 activation/head authority changed")
    base_epoch_id = int(str(activation[2]))
    if base_epoch_id > predecessor_epoch_id:
        raise EventConflictError("D30 activation base is after the predecessor")
    return base_epoch_id


def _validate_d30_m3_bootstrap(
    claim: _D30M3ClaimLocator,
    *,
    activation_base_epoch_id: int,
    predecessor_epoch_id: int,
) -> None:
    """Validate one activation-base claim from positive frozen-M3 closure."""

    currency = claim.currency
    observation = claim.observation_row
    epoch = claim.epoch_row
    execution = claim.execution_row
    run = claim.run_row
    candidate = claim.candidate_row
    try:
        manifest_value = run[9]
        if type(manifest_value) is not dict:
            raise ValidationError("M3 run manifest must be an exact mapping")
        manifest = manifest_from_dict(manifest_value)
        if not _d30_exact_runtime_type(
            manifest, PipelineRunManifest
        ) or not _d30_exact_json_equal(
            manifest_value,
            _d30_json_compatible(manifest_to_dict(manifest)),
        ):
            raise ValidationError("M3 run manifest bytes are not exact")
        scores = ScoreTriple(
            float(str(observation[5])),
            float(str(observation[6])),
            float(str(observation[7])),
        )
        observation_produced_epoch = int(str(observation[12]))
        epoch_id = int(str(epoch[0]))
        run_semantic_epoch = int(str(run[8]))
        manifest_semantic_epoch = int(str(manifest.semantic_epoch_id))
        manifest_confirmed_epoch = int(str(manifest.confirmed_as_of_epoch))
        execution_temperature = float(str(execution[6]))
        candidate_score = float(str(candidate[8]))
        candidate_rank = int(str(candidate[9]))
        raw_execution_logits = execution[7]
        if not isinstance(raw_execution_logits, (list, tuple)):
            raise ValidationError("M3 execution logits must be an exact sequence")
        stored_logits = tuple(float(str(value)) for value in raw_execution_logits)
    except (IndexError, KeyError, TypeError, ValueError, ValidationError) as error:
        raise EventConflictError(
            "D30 activation-base M3 manifest is invalid"
        ) from error
    answer_version_id = manifest.answer_version_id
    if answer_version_id is None:
        raise EventConflictError("D30 activation-base M3 answer identity is absent")
    atomic_matches = tuple(
        atomic
        for atomic in manifest.claims
        if "claim-"
        + stable_ai_digest(
            "m3-claim-v1",
            answer_version_id,
            atomic.local_claim_id,
            atomic.text,
        )
        == currency.subject_id
    )
    if len(atomic_matches) != 1:
        raise EventConflictError("D30 activation-base M3 claim linkage is ambiguous")
    atomic_claim = atomic_matches[0]
    local_claim_id = atomic_claim.local_claim_id
    verification_matches = tuple(
        result
        for result in manifest.verifications
        if result.claim_id == local_claim_id
        and result.chunk_version_id == currency.chunk_version_id
        and result.candidate_id == str(candidate[0])
    )
    candidate_matches = tuple(
        item
        for item in manifest.retrieval_candidates
        if item.candidate_id == str(candidate[0])
    )
    if len(verification_matches) != 1 or len(candidate_matches) != 1:
        raise EventConflictError("D30 activation-base manifest linkage is ambiguous")
    verification = verification_matches[0]
    manifest_candidate = candidate_matches[0]
    expected_image_epochs = tuple(
        dict.fromkeys((activation_base_epoch_id, predecessor_epoch_id))
    )
    try:
        if len(claim.image_rows) != len(expected_image_epochs):
            raise ValidationError("M3 image point count changed")
        actual_image_epochs = tuple(int(str(row[0])) for row in claim.image_rows)
        if actual_image_epochs != expected_image_epochs:
            raise ValidationError("M3 image epoch order changed")
        image_chunk_values: list[tuple[str, str]] = []
        for expected_epoch, row in zip(
            expected_image_epochs, claim.image_rows, strict=True
        ):
            if len(row) != 28:
                raise ValidationError("M3 image point shape changed")
            registry_id = _exact_text("M3 registry id", row[1])
            claim_count = int(str(row[3]))
            claim_set_hash = _exact_digest("M3 claim-set hash", _strip(row[4]))
            member_ordinal = int(str(row[7]))
            chunk = ChunkVersion(
                chunk_version_id=str(row[15]),
                document_version_id=str(row[16]),
                chunk_index=int(str(row[17])),
                text=str(row[18]),
                text_hash=_strip(row[19]),
            )
            chunker_version = _exact_text("M3 chunker version", row[20])
            chunk_valid_from = int(str(row[21]))
            chunk_valid_to = None if row[22] is None else int(str(row[22]))
            document_version_id = _exact_text("M3 document version id", row[23])
            document_id = _exact_text("M3 document id", row[24])
            content_hash = _exact_digest("M3 document content hash", _strip(row[25]))
            version_valid_from = int(str(row[26]))
            version_valid_to = None if row[27] is None else int(str(row[27]))
            image_chunk_values.append((chunk.text, chunk.text_hash))
            if (
                int(str(row[0])) != expected_epoch
                or str(row[2]) != registry_id
                or str(row[5]) != registry_id
                or claim_count < 1
                or not claim_set_hash
                or member_ordinal < 0
                or member_ordinal >= claim_count
                or str(row[6]) != currency.subject_id
                or str(row[8]) != currency.subject_id
                or str(row[9]) != answer_version_id
                or str(row[10]) != atomic_claim.text
                or type(row[14]) is not bool
                or row[14] != atomic_claim.required
                or chunk.chunk_version_id != currency.chunk_version_id
                or chunk.document_version_id != document_version_id
                or not chunker_version
                or chunk_valid_from > expected_epoch
                or (chunk_valid_to is not None and expected_epoch >= chunk_valid_to)
                or not document_id
                or not content_hash
                or version_valid_from > expected_epoch
                or (version_valid_to is not None and expected_epoch >= version_valid_to)
            ):
                raise ValidationError("M3 image closure changed")
        if len(set(image_chunk_values)) != 1:
            raise ValidationError("M3 image chunk bytes changed")
        image_chunk_text, image_chunk_hash = image_chunk_values[0]
        manifest_chunk_hashes = tuple(
            text_hash
            for chunk_id, text_hash in manifest.chunk_text_hashes
            if chunk_id == currency.chunk_version_id
        )
        embedding_input_hash = _exact_digest(
            "M3 embedding input hash", _strip(claim.chunk_embedding_row[3])
        )
        if (
            manifest.chunk_version_ids.count(currency.chunk_version_id) != 1
            or manifest_chunk_hashes != (image_chunk_hash,)
            or embedding_input_hash
            != hashlib.sha256(image_chunk_text.encode("utf-8")).hexdigest()
        ):
            raise ValidationError("M3 image manifest/embedding closure changed")
    except (IndexError, TypeError, ValueError, ValidationError) as error:
        raise EventConflictError("D30 activation-base M3 image changed") from error
    expected_observation_id = "observation-" + stable_ai_digest(
        "m3-observation-v1",
        currency.subject_id,
        currency.chunk_version_id,
        "direct_verification",
        verification.model_artifact_id,
        verification.prompt_artifact_id,
        verification.calibration_version,
        verification.input_hash,
    )
    try:
        expected_logits = tuple(
            float(value) for value in (verification.raw_logits or (0.0, 0.0, 0.0))
        )
    except (TypeError, ValueError) as error:
        raise EventConflictError("D30 activation-base logits are malformed") from error
    reused_ids = set(manifest.reused_artifact_ids)
    new_ids = set(manifest.new_artifact_ids)
    expected_artifact_coordinates = {
        ("model", str(execution[3])),
        ("prompt", str(execution[4])),
        ("model", str(candidate[6])),
        (
            "embedding",
            f"embedding:{currency.chunk_version_id}:{str(candidate[6])}",
        ),
        ("retrieval", str(candidate[0])),
        ("verification", currency.observation_id),
    }
    actual_artifact_coordinates = {
        (str(row[1]), str(row[2])) for row in claim.artifact_use_rows
    }
    if (
        len(claim.artifact_use_rows) != len(expected_artifact_coordinates)
        or actual_artifact_coordinates != expected_artifact_coordinates
        or any(str(row[0]) != str(run[0]) for row in claim.artifact_use_rows)
        or any(
            str(row[2]) not in reused_ids | new_ids for row in claim.artifact_use_rows
        )
        or any(
            bool(row[3]) != (str(row[2]) in reused_ids)
            for row in claim.artifact_use_rows
        )
    ):
        raise EventConflictError("D30 activation-base artifact-use closure changed")
    if (
        currency.installed_revision != 0
        or observation[0] != currency.observation_id
        or str(observation[1]) != "claim"
        or tuple(str(value) for value in observation[2:5])
        != (currency.subject_id, currency.chunk_version_id, currency.task_type)
        or currency.task_type != "direct_verification"
        or observation[14] is not True
        or observation_produced_epoch > activation_base_epoch_id
        or str(observation[0]) != expected_observation_id
        or (scores.support, scores.refute, scores.neutral)
        != (
            verification.scores.support,
            verification.scores.refute,
            verification.scores.neutral,
        )
        or str(observation[8]) != str(claim.model_row[3])
        or str(observation[9]) != str(claim.model_row[4])
        or str(observation[10])
        != f"{str(claim.prompt_row[2])}:{_strip(claim.prompt_row[4])}"
        or _strip(observation[11]) != verification.input_hash
        or _strip(observation[13]) != verification.raw_output_hash
        or tuple(epoch[1:8])
        != (
            f"m3-run:{str(run[0])}",
            _strip(run[4]),
            0,
            "committed",
            "sealed",
            "complete",
            "provisional",
        )
        or epoch[8] is None
        or epoch_id != observation_produced_epoch
        or str(run[0]) != manifest.run_id
        or str(run[1]) != manifest.schema_version
        or str(run[2]) != PipelineRunStatus.PUBLISHED.value
        or manifest.status is not PipelineRunStatus.PUBLISHED
        or _strip(run[3]) != manifest.config_hash
        or _strip(run[4]) != manifest.input_hash
        or _strip(run[5]) != manifest.corpus_hash
        or str(run[6]) != manifest.question_id
        or str(run[7]) != str(manifest.answer_version_id)
        or run_semantic_epoch != manifest_semantic_epoch
        or manifest_confirmed_epoch != run_semantic_epoch
        or run_semantic_epoch != observation_produced_epoch
        or run[10] is not None
        or run[12] is None
        or tuple(str(value) for value in execution[:5])
        != (
            currency.observation_id,
            str(run[0]),
            str(candidate[0]),
            str(claim.model_row[0]),
            str(claim.prompt_row[0]),
        )
        or verification.model_artifact_id != str(execution[3])
        or verification.prompt_artifact_id != str(execution[4])
        or str(execution[3]) not in manifest.model_artifact_ids
        or str(candidate[6]) not in manifest.model_artifact_ids
        or str(execution[4]) not in manifest.prompt_artifact_ids
        or str(execution[5]) != verification.calibration_version
        or execution_temperature != verification.temperature
        or stored_logits != expected_logits
        or _strip(execution[8]) != verification.raw_output_hash
        or execution[9] is not None
        or str(candidate[1]) != str(run[0])
        or str(candidate[2]) != "claim"
        or str(candidate[3]) != currency.subject_id
        or str(candidate[4]) != currency.subject_id
        or str(candidate[5]) != currency.chunk_version_id
        or str(candidate[6]) != manifest_candidate.embedding_model_artifact_id
        or str(candidate[7]) != manifest_candidate.method_version
        or candidate_score != manifest_candidate.score
        or candidate_rank != manifest_candidate.rank
        or manifest_candidate.query_kind is not QueryKind.CLAIM
        or manifest_candidate.query_id != currency.subject_id
        or manifest_candidate.chunk_version_id != currency.chunk_version_id
        or str(claim.model_row[1]) != "verification"
        or str(claim.prompt_row[1]) != "verification"
        or str(claim.embedding_model_row[1]) != "embedding"
        or tuple(str(value) for value in claim.chunk_embedding_row[:2])
        != (currency.chunk_version_id, str(claim.embedding_model_row[0]))
    ):
        raise EventConflictError("D30 activation-base M3 provenance changed")
    if epoch_id > predecessor_epoch_id:
        raise EventConflictError("D30 activation-base observation is from the future")


def _validate_d30_dynamic_claim(
    claim: _D30DynamicClaimLocator,
    *,
    owner: _D30OwnerLocator,
    candidate_policy_id: str,
) -> None:
    """Validate retained dynamic bytes without reconstructing a classic artifact."""

    currency = claim.currency
    observation = claim.observation_row
    delta = claim.delta_row
    jobs = {str(row[0]): row for row in owner.jobs}
    child = jobs.get(claim.child_job_id)
    parent = jobs.get(claim.parent_job_id)
    if child is None or parent is None:
        raise EventConflictError("D30 selected child escaped its complete owner map")
    admitted = claim.admitted_pair_row
    result = claim.parent_result_row
    predecessor = claim.predecessor_candidate
    if claim.predecessor_epoch_id is None:
        if (
            predecessor is not None
            or delta[5] is not None
            or owner.update_row[3] is not None
        ):
            raise EventConflictError("D30 dynamic predecessor absence changed")
    else:
        predecessor_epoch = claim.predecessor_epoch_id
        if (
            owner.update_row[3] is None
            or int(str(owner.update_row[3])) != predecessor_epoch
        ):
            raise EventConflictError("D30 dynamic predecessor epoch changed")
        effective_base: str | None = None
        if predecessor is not None:
            try:
                valid_from = int(str(predecessor[5]))
                valid_to = None if predecessor[6] is None else int(str(predecessor[6]))
            except (TypeError, ValueError) as error:
                raise EventConflictError(
                    "D30 predecessor interval is malformed"
                ) from error
            if (
                tuple(str(value) for value in predecessor[:4]) != currency.full_key
                or valid_from > predecessor_epoch
                or (valid_to is not None and valid_to <= valid_from)
            ):
                raise EventConflictError("D30 predecessor candidate changed")
            if valid_to is None or predecessor_epoch < valid_to:
                effective_base = str(predecessor[4])
        if (None if delta[5] is None else str(delta[5])) != effective_base:
            raise EventConflictError("D30 dynamic base observation changed")
    reasons = admitted[6]
    if not isinstance(reasons, (list, tuple)):
        raise EventConflictError("D30 admitted-pair reasons are malformed")
    reason_values = tuple(str(value) for value in reasons)
    if (
        not reason_values
        or reason_values != tuple(sorted(set(reason_values), key=_c_key))
        or bool(admitted[7]) != (AdmissionChannel.LINEAGE.value in reason_values)
    ):
        raise EventConflictError("D30 admitted-pair commitments changed")
    if (
        currency.installed_revision < 1
        or (owner.runtime_row is not None and not currency.task_type.strip())
        or observation[0] != currency.observation_id
        or str(observation[1]) != "claim"
        or tuple(str(value) for value in observation[2:5])
        != (currency.subject_id, currency.chunk_version_id, currency.task_type)
        or observation[14] is not True
        or int(str(observation[12])) != owner.epoch_id
        or int(str(observation[12])) != currency.installed_revision
        or int(str(delta[0])) != owner.epoch_id
        or tuple(str(value) for value in delta[1:5]) != currency.full_key
        or str(delta[6]) != currency.observation_id
        or type(delta[7]) is not int
        or str(child[3]) != M4JobKind.VERIFY_PAIR.value
        or str(child[4]) != candidate_policy_id
        or child[2] is None
        or str(child[2]) != claim.parent_job_id
        or str(child[7]) != currency.subject_id
        or str(child[8]) != currency.chunk_version_id
        or str(child[10]) != M4JobState.COMPLETED_ACTIVE.value
        or type(child[17]) is not int
        or int(delta[7]) != int(child[17])
        or _strip(admitted[0]) != claim.admitted_pair_id
        or int(str(admitted[1])) != owner.epoch_id
        or tuple(str(value) for value in admitted[2:5])
        != (currency.subject_id, currency.chunk_version_id, candidate_policy_id)
        or _strip(result[0]) != claim.parent_job_id
        or int(str(result[1])) != owner.epoch_id
        or str(result[2]) != str(parent[14])
        or _strip(result[3]) != _strip(parent[15])
        or type(result[4]) is not bool
        or any(
            type(result[index]) is not int or int(str(result[index])) < 0
            for index in (5, 6)
        )
        or any(len(_strip(result[index])) != 64 for index in (7, 8))
    ):
        raise EventConflictError("D30 dynamic claim retained authority changed")
    present = tuple(row for row in claim.execution_rows if row is not None)
    if not present:
        if observation[13] is None or _strip(observation[13]) != _strip(child[15]):
            raise EventConflictError("D30 execution-absent result hash changed")
        return
    if len(present) != 3 or not all(row == present[0] for row in present):
        raise EventConflictError("D30 optional execution coordinates disagree")
    execution_row = present[0]
    raw_execution_logits = execution_row[10]
    if not isinstance(raw_execution_logits, (list, tuple)):
        raise EventConflictError("D30 optional execution logits are malformed")
    try:
        raw_logits = tuple(float(str(value)) for value in raw_execution_logits)
        if len(raw_logits) != 3:
            raise ValidationError("D30 direct logits changed")
        execution = M5TypedDirectVerificationExecution(
            observation_id=str(execution_row[0]),
            job_id=str(execution_row[1]),
            admitted_pair_id=_strip(execution_row[2]),
            model_artifact_id=str(execution_row[3]),
            prompt_artifact_id=str(execution_row[4]),
            execution_spec_hash=_strip(execution_row[5]),
            pair_input_hash=_strip(execution_row[6]),
            calibration_version=str(execution_row[7]),
            calibration_artifact_sha256=_strip(execution_row[8]),
            temperature=float(str(execution_row[9])),
            raw_logits=(raw_logits[0], raw_logits[1], raw_logits[2]),
            raw_output_hash=_strip(execution_row[11]),
            reused_from_observation_id=(
                None if execution_row[12] is None else str(execution_row[12])
            ),
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError("D30 optional execution is malformed") from error
    if (
        execution.observation_id != currency.observation_id
        or execution.job_id != claim.child_job_id
        or execution.admitted_pair_id != claim.admitted_pair_id
        or execution.execution_spec_hash != _strip(child[6])
        or observation[13] is None
        or execution.raw_output_hash != _strip(observation[13])
        or claim.model_row is None
        or str(claim.model_row[0]) != execution.model_artifact_id
        or str(claim.model_row[1]) != "verification"
        or claim.prompt_row is None
        or str(claim.prompt_row[0]) != execution.prompt_artifact_id
        or str(claim.prompt_row[1]) != "verification"
    ):
        raise EventConflictError("D30 optional execution binding changed")


def _d30_work_from_row(
    row: tuple[object, ...], *, counter_offset: int, digest_index: int
) -> M5RuntimeWork:
    try:
        return M5RuntimeWork(
            **{
                name: int(str(value))
                for name, value in zip(
                    _D30_WORK_COUNTERS,
                    row[counter_offset : counter_offset + len(_D30_WORK_COUNTERS)],
                    strict=True,
                )
            },
            work_digest=_strip(row[digest_index]),
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError("D30 typed work vector is malformed") from error


def _d30_timing_observation(
    row: tuple[object, ...], *, required_index: int, digest_index: int
) -> M5RuntimeTimingObservation:
    required = row[required_index]
    values = row[required_index + 1 : required_index + 10]
    if type(required) is not bool:
        raise EventConflictError("D30 typed timing flag is malformed")
    try:
        timing = (
            M5RuntimeTiming(
                coordinator_non_db_non_neural_ns=int(str(values[0])),
                neural_wall_ns=int(str(values[1])),
                postgres_roundtrip_wall_ns=int(str(values[2])),
                external_io_wall_ns=int(str(values[3])),
                end_to_end_wall_ns=int(str(values[4])),
                postgres_server_execution_ns=_optional_int(values[5]),
                postgres_lock_wait_ns=_optional_int(values[6]),
                postgres_wal_bytes=_optional_int(values[7]),
                postgres_shared_block_reads=_optional_int(values[8]),
            )
            if required
            else None
        )
        if not required and any(value is not None for value in values):
            raise ValidationError("missing timing observation retained values")
        return M5RuntimeTimingObservation(
            required_interval_observed=required,
            timing=timing,
            observation_digest=_strip(row[digest_index]),
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError("D30 typed timing observation changed") from error


def _validate_d30_contribution(
    row: tuple[object, ...],
    *,
    epoch_id: int,
    contribution_kind: M5RuntimeWorkContributionKind,
    source_id: str,
    source_identity_hash: str,
    work: M5RuntimeWork,
    applied_revision: int | None = None,
) -> int:
    offset = len(_D30_WORK_COUNTERS)
    stored_work = _d30_work_from_row(row, counter_offset=1, digest_index=offset + 1)
    expected_key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=contribution_kind,
        source_id=source_id,
    )
    try:
        stored_revision = int(str(row[offset + 6]))
    except (TypeError, ValueError) as error:
        raise EventConflictError("D30 contribution revision is malformed") from error
    if (
        int(str(row[0])) != epoch_id
        or stored_work != work
        or str(row[offset + 2]) != contribution_kind.value
        or str(row[offset + 3]) != source_id
        or _strip(row[offset + 4]) != source_identity_hash
        or _strip(row[offset + 5]) != expected_key
        or (applied_revision is not None and stored_revision != applied_revision)
    ):
        raise EventConflictError("D30 typed work contribution changed")
    return stored_revision


def _d30_json_object(value: object, keys: set[str], *, label: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise EventConflictError(f"D30 {label} JSON shape changed")
    return value


def _d30_exact_json_equal(left: object, right: object) -> bool:
    if type(left) is not type(right):
        return False
    if type(left) is dict:
        assert isinstance(left, dict) and isinstance(right, dict)
        return set(left) == set(right) and all(
            _d30_exact_json_equal(left[key], right[key]) for key in left
        )
    if type(left) is list:
        assert isinstance(left, list) and isinstance(right, list)
        return len(left) == len(right) and all(
            _d30_exact_json_equal(first, second)
            for first, second in zip(left, right, strict=True)
        )
    return left == right


def _d30_json_compatible(value: object) -> object:
    """Return the exact JSON container shape produced by PostgreSQL JSONB."""

    if type(value) is dict:
        assert isinstance(value, dict)
        return {key: _d30_json_compatible(item) for key, item in value.items()}
    if type(value) in (list, tuple):
        assert isinstance(value, (list, tuple))
        return [_d30_json_compatible(item) for item in value]
    return value


def _d30_exact_runtime_type(value: object, annotation: object) -> bool:
    """Recursively reject bool/int and other aliases in parsed held DTOs."""

    origin = get_origin(annotation)
    if origin is UnionType:
        return any(
            _d30_exact_runtime_type(value, alternative)
            for alternative in get_args(annotation)
        )
    if origin is tuple:
        if type(value) is not tuple:
            return False
        arguments = get_args(annotation)
        if len(arguments) == 2 and arguments[1] is Ellipsis:
            return all(_d30_exact_runtime_type(item, arguments[0]) for item in value)
        return len(value) == len(arguments) and all(
            _d30_exact_runtime_type(item, expected)
            for item, expected in zip(value, arguments, strict=True)
        )
    if isinstance(annotation, type) and is_dataclass(annotation):
        if type(value) is not annotation:
            return False
        hints = get_type_hints(annotation)
        return all(
            _d30_exact_runtime_type(getattr(value, field.name), hints[field.name])
            for field in fields(annotation)
        )
    return isinstance(annotation, type) and type(value) is annotation


def _d30_f64_hex(value: object, *, label: str) -> float:
    if type(value) is not str:
        raise EventConflictError(f"D30 {label} is not an exact f64 encoding")
    try:
        raw = bytes.fromhex(value)
        if len(raw) != 8:
            raise ValueError("wrong f64 width")
        return float(struct.unpack(">d", raw)[0])
    except (ValueError, struct.error) as error:
        raise EventConflictError(f"D30 {label} is not an exact f64 encoding") from error


def _validate_d30_late_envelope(
    *,
    owner: _D30OwnerLocator,
    located: _D30D24AttemptLocator,
    attempt: tuple[object, ...],
    job: tuple[object, ...],
) -> tuple[str, str]:
    """Validate one retained typed-direct late envelope without issuing SQL."""

    row = located.late_envelope_row
    if row is None:
        raise EventConflictError("D30 expired evidence lacks its late envelope")
    try:
        return_kind = M5TypedDirectReturnKind(str(row[1]))
        job_kind = M4JobKind(str(job[3]))
        pair = (
            PairKey(str(job[7]), str(job[8]))
            if job_kind is M4JobKind.VERIFY_PAIR
            else None
        )
        spec = M4LogicalJobSpec(
            job_id=str(job[0]),
            event_id=str(owner.epoch_row[1]),
            kind=job_kind,
            candidate_policy_id=str(job[4]),
            payload_hash=_strip(job[5]),
            execution_spec_hash=_strip(job[6]),
            parent_job_id=None if job[2] is None else str(job[2]),
            pair=pair,
            target_claim_id=(
                str(job[7]) if job_kind is M4JobKind.FRONTIER_RETRIEVE else None
            ),
            target_chunk_version_id=(
                str(job[8]) if job_kind is M4JobKind.IMPACT_DISCOVERY else None
            ),
            expandable=bool(job[9]),
        )
        checked_attempt = M4JobAttempt(
            attempt_id=str(attempt[0]),
            job_id=str(attempt[1]),
            execution_spec_hash=_strip(attempt[2]),
            attempt_ordinal=int(str(attempt[3])),
            lease_token_hash=_strip(attempt[4]),
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError("D30 late envelope base binding changed") from error

    job_binding = _d30_json_object(
        row[9],
        {
            "job_id",
            "event_id",
            "job_kind",
            "candidate_policy_id",
            "payload_hash",
            "execution_spec_hash",
            "parent_job_id",
            "pair_claim_id",
            "pair_chunk_version_id",
            "target_claim_id",
            "target_chunk_version_id",
            "expandable",
        },
        label="late job binding",
    )
    expected_job_binding: dict[str, object] = {
        "job_id": spec.job_id,
        "event_id": spec.event_id,
        "job_kind": spec.kind.value,
        "candidate_policy_id": spec.candidate_policy_id,
        "payload_hash": spec.payload_hash,
        "execution_spec_hash": spec.execution_spec_hash,
        "parent_job_id": spec.parent_job_id,
        "pair_claim_id": None if spec.pair is None else spec.pair.claim_id,
        "pair_chunk_version_id": (
            None if spec.pair is None else spec.pair.chunk_version_id
        ),
        "target_claim_id": spec.target_claim_id,
        "target_chunk_version_id": spec.target_chunk_version_id,
        "expandable": spec.expandable,
    }
    attempt_binding = _d30_json_object(
        row[10],
        {
            "attempt_id",
            "job_id",
            "execution_spec_hash",
            "attempt_ordinal",
            "lease_token_hash",
        },
        label="late attempt binding",
    )
    expected_attempt_binding: dict[str, object] = {
        "attempt_id": checked_attempt.attempt_id,
        "job_id": checked_attempt.job_id,
        "execution_spec_hash": checked_attempt.execution_spec_hash,
        "attempt_ordinal": checked_attempt.attempt_ordinal,
        "lease_token_hash": checked_attempt.lease_token_hash,
    }
    if (
        job_binding != expected_job_binding
        or attempt_binding != expected_attempt_binding
    ):
        raise EventConflictError("D30 late envelope base bytes changed")

    completion_binding = _d30_json_object(
        row[11],
        {
            "job_id",
            "payload_hash",
            "execution_spec_hash",
            "result_artifact_id",
            "result_artifact_hash",
            "terminal_state",
            "completion_digest",
            "child_parent_job_id",
            "child_completion_digest",
            "child_set_hash",
            "child_job_ids",
        },
        label="late completion binding",
    )
    child_ids_value = completion_binding["child_job_ids"]
    if type(child_ids_value) is not list or any(
        type(value) is not str for value in child_ids_value
    ):
        raise EventConflictError("D30 late child closure is malformed")
    child_ids = tuple(child_ids_value)
    closure_values = (
        completion_binding["child_parent_job_id"],
        completion_binding["child_completion_digest"],
        completion_binding["child_set_hash"],
    )
    try:
        child_closure = (
            None
            if all(value is None for value in closure_values) and not child_ids
            else M4ChildClosure(
                parent_job_id=str(closure_values[0]),
                completion_digest=_strip(closure_values[1]),
                child_job_ids=child_ids,
                child_set_hash=_strip(closure_values[2]),
            )
        )
        completion = M4JobCompletion(
            job_id=str(completion_binding["job_id"]),
            payload_hash=_strip(completion_binding["payload_hash"]),
            execution_spec_hash=_strip(completion_binding["execution_spec_hash"]),
            result_artifact_id=str(completion_binding["result_artifact_id"]),
            result_artifact_hash=_strip(completion_binding["result_artifact_hash"]),
            terminal_state=M4JobState(str(completion_binding["terminal_state"])),
            completion_digest=_strip(completion_binding["completion_digest"]),
            child_closure=child_closure,
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError("D30 late completion binding changed") from error
    if (
        completion.job_id != spec.job_id
        or completion.payload_hash != spec.payload_hash
        or completion.execution_spec_hash != spec.execution_spec_hash
        or completion.result_artifact_id != str(row[4])
        or completion.result_artifact_hash != _strip(row[5])
        or spec.expandable != (child_closure is not None)
    ):
        raise EventConflictError("D30 late completion escaped its job")

    job_digest = digests.typed_direct_late_job_binding_digest(
        job_id=spec.job_id,
        event_id=spec.event_id,
        job_kind=spec.kind,
        candidate_policy_id=spec.candidate_policy_id,
        payload_hash=spec.payload_hash,
        execution_spec_hash=spec.execution_spec_hash,
        parent_job_id=spec.parent_job_id,
        pair_claim_id=None if spec.pair is None else spec.pair.claim_id,
        pair_chunk_version_id=(
            None if spec.pair is None else spec.pair.chunk_version_id
        ),
        target_claim_id=spec.target_claim_id,
        target_chunk_version_id=spec.target_chunk_version_id,
        expandable=spec.expandable,
    )
    attempt_digest = digests.typed_direct_late_attempt_binding_digest(
        attempt_id=checked_attempt.attempt_id,
        job_id=checked_attempt.job_id,
        execution_spec_hash=checked_attempt.execution_spec_hash,
        attempt_ordinal=checked_attempt.attempt_ordinal,
        lease_token_hash=checked_attempt.lease_token_hash,
    )
    completion_digest = digests.typed_direct_late_completion_binding_digest(
        job_id=completion.job_id,
        payload_hash=completion.payload_hash,
        execution_spec_hash=completion.execution_spec_hash,
        result_artifact_id=completion.result_artifact_id,
        result_artifact_hash=completion.result_artifact_hash,
        terminal_state=completion.terminal_state,
        completion_digest=completion.completion_digest,
        child_parent_job_id=(
            None if child_closure is None else child_closure.parent_job_id
        ),
        child_completion_digest=(
            None if child_closure is None else child_closure.completion_digest
        ),
        child_set_hash=None if child_closure is None else child_closure.child_set_hash,
        child_job_ids=() if child_closure is None else child_closure.child_job_ids,
    )

    discovery_digest: str | None = None
    scope_digest: str | None = None
    verifier_digest: str | None = None
    discovery: M4DiscoveryResult | None = None
    scope: M4DiscoveryScope | None = None
    scope_kind: M5TypedDirectScopeKind | None = None
    explicit_ids: tuple[str, ...] | None = None
    closed_revision: int | None = None
    execution: M5TypedDirectVerificationExecution | None = None
    observation: SemanticObservation | None = None
    observation_produced_epoch: int | None = None
    observation_raw_output_hash: str | None = None
    observation_eligible: bool | None = None
    requested_make_effective: bool | None = None
    if return_kind is M5TypedDirectReturnKind.DISCOVERY:
        discovery_binding = _d30_json_object(
            row[12],
            {
                "root_job_id",
                "result_artifact_id",
                "result_artifact_hash",
                "fallback_satisfied",
                "channel_hit_count",
                "admitted_pair_count",
                "channel_set_hash",
                "admitted_pair_set_hash",
                "channel_hits",
                "admitted_pairs",
            },
            label="late discovery binding",
        )
        scope_binding = _d30_json_object(
            row[13],
            {
                "root_job_id",
                "epoch_id",
                "registry_snapshot_id",
                "registered_claim_ids",
                "closed",
                "persisted_scope_kind",
                "explicit_claim_ids",
                "closed_revision",
            },
            label="late scope binding",
        )
        if row[14] is not None or any(value is not None for value in row[6:9]):
            raise EventConflictError("D30 discovery envelope has verifier fields")
        raw_hits = discovery_binding["channel_hits"]
        raw_pairs = discovery_binding["admitted_pairs"]
        if type(raw_hits) is not list or type(raw_pairs) is not list:
            raise EventConflictError("D30 late discovery members are malformed")
        hits: list[M4ChannelHit] = []
        admitted: list[AdmittedPair] = []
        try:
            for value in raw_hits:
                item = _d30_json_object(
                    value,
                    {
                        "epoch_id",
                        "claim_id",
                        "chunk_version_id",
                        "candidate_policy_id",
                        "channel",
                        "rank",
                        "score",
                        "channel_artifact_hash",
                    },
                    label="late channel hit",
                )
                hits.append(
                    M4ChannelHit(
                        epoch_id=int(str(item["epoch_id"])),
                        pair=PairKey(
                            str(item["claim_id"]), str(item["chunk_version_id"])
                        ),
                        candidate_policy_id=str(item["candidate_policy_id"]),
                        channel=AdmissionChannel(str(item["channel"])),
                        rank=int(str(item["rank"])),
                        score=(
                            None
                            if item["score"] is None
                            else _d30_f64_hex(item["score"], label="late channel score")
                        ),
                        channel_artifact_hash=_strip(item["channel_artifact_hash"]),
                    )
                )
            for value in raw_pairs:
                item = _d30_json_object(
                    value,
                    {
                        "epoch_id",
                        "claim_id",
                        "chunk_version_id",
                        "candidate_policy_id",
                        "fused_rank",
                        "reasons",
                        "mandatory_lineage",
                    },
                    label="late admitted pair",
                )
                reasons = item["reasons"]
                if type(reasons) is not list:
                    raise ValidationError("late reasons are not a list")
                admitted.append(
                    AdmittedPair(
                        epoch_id=int(str(item["epoch_id"])),
                        pair=PairKey(
                            str(item["claim_id"]), str(item["chunk_version_id"])
                        ),
                        candidate_policy_id=str(item["candidate_policy_id"]),
                        fused_rank=int(str(item["fused_rank"])),
                        reasons=tuple(
                            AdmissionChannel(str(reason)) for reason in reasons
                        ),
                        mandatory_lineage=bool(item["mandatory_lineage"]),
                    )
                )
            discovery = M4DiscoveryResult(
                root_job_id=str(discovery_binding["root_job_id"]),
                result_artifact_id=str(discovery_binding["result_artifact_id"]),
                result_artifact_hash=_strip(discovery_binding["result_artifact_hash"]),
                admitted_pairs=tuple(admitted),
                fallback_satisfied=bool(discovery_binding["fallback_satisfied"]),
                channel_hits=tuple(hits),
            )
            registered = scope_binding["registered_claim_ids"]
            explicit = scope_binding["explicit_claim_ids"]
            if type(registered) is not list or (
                explicit is not None and type(explicit) is not list
            ):
                raise ValidationError("late scope lists are malformed")
            scope = M4DiscoveryScope(
                root_job_id=str(scope_binding["root_job_id"]),
                registry_snapshot_id=str(scope_binding["registry_snapshot_id"]),
                registered_claim_ids=tuple(str(value) for value in registered),
                closed=bool(scope_binding["closed"]),
            )
            scope_kind = M5TypedDirectScopeKind(
                str(scope_binding["persisted_scope_kind"])
            )
            explicit_ids = (
                None if explicit is None else tuple(str(value) for value in explicit)
            )
            closed_revision = (
                None
                if scope_binding["closed_revision"] is None
                else int(str(scope_binding["closed_revision"]))
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError("D30 late discovery closure changed") from error
        if (
            job_kind not in {M4JobKind.IMPACT_DISCOVERY, M4JobKind.FRONTIER_RETRIEVE}
            or discovery.root_job_id != spec.job_id
            or discovery.result_artifact_id != completion.result_artifact_id
            or discovery.result_artifact_hash != completion.result_artifact_hash
            or scope.root_job_id != spec.job_id
            or int(str(scope_binding["epoch_id"])) != owner.epoch_id
            or scope.closed != (closed_revision is not None)
            or (
                scope_kind is M5TypedDirectScopeKind.EXPLICIT_CLAIMS
                and explicit_ids != scope.registered_claim_ids
            )
            or (
                scope_kind is M5TypedDirectScopeKind.ALL_REGISTERED_CLAIMS
                and explicit_ids is not None
            )
        ):
            raise EventConflictError("D30 late discovery binding escaped its job")
        channel_identities = tuple(
            sorted(
                stable_m4_digest(
                    "m4-discovery-channel-v1",
                    str(item.epoch_id),
                    item.pair.claim_id,
                    item.pair.chunk_version_id,
                    item.candidate_policy_id,
                    item.channel.value,
                    str(item.rank),
                    "" if item.score is None else format(item.score, ".17g"),
                    item.channel_artifact_hash,
                )
                for item in hits
            )
        )
        admitted_identities = tuple(
            sorted(
                stable_m4_digest(
                    "m4-admitted-pair-v1",
                    str(item.epoch_id),
                    item.pair.claim_id,
                    item.pair.chunk_version_id,
                    item.candidate_policy_id,
                )
                for item in admitted
            )
        )
        if (
            int(str(discovery_binding["channel_hit_count"])) != len(hits)
            or int(str(discovery_binding["admitted_pair_count"])) != len(admitted)
            or _strip(discovery_binding["channel_set_hash"])
            != stable_m4_digest("m4-discovery-channel-set-v1", *channel_identities)
            or _strip(discovery_binding["admitted_pair_set_hash"])
            != stable_m4_digest("m4-discovery-admitted-set-v1", *admitted_identities)
        ):
            raise EventConflictError("D30 late discovery aggregate changed")
        discovery_digest = digests.typed_direct_late_discovery_binding_digest(
            root_job_id=discovery.root_job_id,
            result_artifact_id=discovery.result_artifact_id,
            result_artifact_hash=discovery.result_artifact_hash,
            fallback_satisfied=discovery.fallback_satisfied,
            channel_hit_count=len(hits),
            admitted_pair_count=len(admitted),
            channel_set_hash=_strip(discovery_binding["channel_set_hash"]),
            admitted_pair_set_hash=_strip(discovery_binding["admitted_pair_set_hash"]),
            channel_hits=tuple(
                (
                    item.epoch_id,
                    item.pair.claim_id,
                    item.pair.chunk_version_id,
                    item.candidate_policy_id,
                    item.channel,
                    item.rank,
                    item.score,
                    item.channel_artifact_hash,
                )
                for item in hits
            ),
            admitted_pairs=tuple(
                (
                    item.epoch_id,
                    item.pair.claim_id,
                    item.pair.chunk_version_id,
                    item.candidate_policy_id,
                    item.fused_rank,
                    item.reasons,
                    item.mandatory_lineage,
                )
                for item in admitted
            ),
        )
        scope_digest = digests.typed_direct_late_scope_binding_digest(
            root_job_id=scope.root_job_id,
            epoch_id=owner.epoch_id,
            registry_snapshot_id=scope.registry_snapshot_id,
            registered_claim_ids=scope.registered_claim_ids,
            closed=scope.closed,
            persisted_scope_kind=scope_kind,
            explicit_claim_ids=explicit_ids,
            closed_revision=closed_revision,
        )
    else:
        verifier_binding = _d30_json_object(
            row[14],
            {
                "result_artifact_id",
                "result_artifact_hash",
                "verification_execution",
                "observation",
            },
            label="late verifier binding",
        )
        if row[12] is not None or row[13] is not None:
            raise EventConflictError("D30 verifier envelope has discovery fields")
        observation_value = _d30_json_object(
            verifier_binding["observation"],
            {
                "observation_id",
                "subject_kind",
                "subject_id",
                "chunk_version_id",
                "task_type",
                "support_score",
                "refute_score",
                "neutral_score",
                "model_id",
                "model_version",
                "prompt_version",
                "input_hash",
                "produced_epoch",
                "raw_output_hash",
                "eligible_for_currency",
                "requested_make_effective",
            },
            label="late verifier observation",
        )
        if (
            type(observation_value["produced_epoch"]) is not int
            or type(observation_value["eligible_for_currency"]) is not bool
            or type(observation_value["requested_make_effective"]) is not bool
        ):
            raise EventConflictError("D30 late verifier scalar types changed")
        observation_produced_epoch = int(observation_value["produced_epoch"])
        observation_raw_output_hash = _strip(observation_value["raw_output_hash"])
        observation_eligible = observation_value["eligible_for_currency"]
        requested_make_effective = observation_value["requested_make_effective"]
        execution_value = verifier_binding["verification_execution"]
        try:
            observation = SemanticObservation(
                observation_id=str(observation_value["observation_id"]),
                subject_kind=SubjectKind(str(observation_value["subject_kind"])),
                subject_id=str(observation_value["subject_id"]),
                chunk_version_id=str(observation_value["chunk_version_id"]),
                task_type=str(observation_value["task_type"]),
                support_score=_d30_f64_hex(
                    observation_value["support_score"], label="late support score"
                ),
                refute_score=_d30_f64_hex(
                    observation_value["refute_score"], label="late refute score"
                ),
                neutral_score=_d30_f64_hex(
                    observation_value["neutral_score"], label="late neutral score"
                ),
                producer=ModelStamp(
                    str(observation_value["model_id"]),
                    str(observation_value["model_version"]),
                    str(observation_value["prompt_version"]),
                ),
                input_hash=str(observation_value["input_hash"]),
            )
            if execution_value is not None:
                execution_object = _d30_json_object(
                    execution_value,
                    {
                        "observation_id",
                        "job_id",
                        "admitted_pair_id",
                        "model_artifact_id",
                        "prompt_artifact_id",
                        "execution_spec_hash",
                        "pair_input_hash",
                        "calibration_version",
                        "calibration_artifact_sha256",
                        "temperature",
                        "raw_logits",
                        "raw_output_hash",
                        "reused_from_observation_id",
                    },
                    label="late verifier execution",
                )
                logits = execution_object["raw_logits"]
                if type(logits) is not list or len(logits) != 3:
                    raise ValidationError("late logits are malformed")
                execution = M5TypedDirectVerificationExecution(
                    observation_id=str(execution_object["observation_id"]),
                    job_id=str(execution_object["job_id"]),
                    admitted_pair_id=_strip(execution_object["admitted_pair_id"]),
                    model_artifact_id=str(execution_object["model_artifact_id"]),
                    prompt_artifact_id=str(execution_object["prompt_artifact_id"]),
                    execution_spec_hash=_strip(execution_object["execution_spec_hash"]),
                    pair_input_hash=_strip(execution_object["pair_input_hash"]),
                    calibration_version=str(execution_object["calibration_version"]),
                    calibration_artifact_sha256=_strip(
                        execution_object["calibration_artifact_sha256"]
                    ),
                    temperature=_d30_f64_hex(
                        execution_object["temperature"],
                        label="late verifier temperature",
                    ),
                    raw_logits=tuple(
                        _d30_f64_hex(value, label="late verifier logit")
                        for value in logits
                    ),  # type: ignore[arg-type]
                    raw_output_hash=_strip(execution_object["raw_output_hash"]),
                    reused_from_observation_id=(
                        None
                        if execution_object["reused_from_observation_id"] is None
                        else str(execution_object["reused_from_observation_id"])
                    ),
                )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError("D30 late verifier closure changed") from error
        if (
            job_kind is not M4JobKind.VERIFY_PAIR
            or str(verifier_binding["result_artifact_id"])
            != completion.result_artifact_id
            or _strip(verifier_binding["result_artifact_hash"])
            != completion.result_artifact_hash
            or observation.subject_kind is not SubjectKind.CLAIM
            or spec.pair is None
            or (observation.subject_id, observation.chunk_version_id)
            != (spec.pair.claim_id, spec.pair.chunk_version_id)
            or type(row[6]) is not bool
            or row[6] != (execution is not None)
            or type(row[7]) is not bool
            or row[7] != observation_value["eligible_for_currency"]
            or type(row[8]) is not bool
            or row[8] != observation_value["requested_make_effective"]
            or (
                execution is not None
                and (
                    execution.observation_id != observation.observation_id
                    or execution.job_id != spec.job_id
                    or execution.execution_spec_hash != spec.execution_spec_hash
                    or execution.raw_output_hash
                    != _strip(observation_value["raw_output_hash"])
                )
            )
        ):
            raise EventConflictError("D30 late verifier binding escaped its job")
        execution_values = (
            None
            if execution is None
            else (
                execution.observation_id,
                execution.job_id,
                execution.admitted_pair_id,
                execution.model_artifact_id,
                execution.prompt_artifact_id,
                execution.execution_spec_hash,
                execution.pair_input_hash,
                execution.calibration_version,
                execution.calibration_artifact_sha256,
                execution.temperature,
                execution.raw_logits,
                execution.raw_output_hash,
                execution.reused_from_observation_id,
            )
        )
        verifier_digest = digests.typed_direct_late_verifier_binding_digest(
            result_artifact_id=completion.result_artifact_id,
            result_artifact_hash=completion.result_artifact_hash,
            verification_execution_present=execution is not None,
            verification_execution=execution_values,
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
            observation_eligible_for_currency=observation_eligible,
            requested_make_effective=requested_make_effective,
        )

    envelope_digest = digests.typed_direct_late_return_envelope_digest(
        epoch_id=owner.epoch_id,
        return_kind=return_kind,
        job_binding_digest=job_digest,
        attempt_binding_digest=attempt_digest,
        completion_binding_digest=completion_digest,
        discovery_binding_digest=discovery_digest,
        scope_binding_digest=scope_digest,
        verifier_binding_digest=verifier_digest,
    )
    try:
        envelope = M5TypedDirectLateReturnEnvelope(
            epoch_id=owner.epoch_id,
            return_kind=return_kind,
            job_id=spec.job_id,
            attempt_id=checked_attempt.attempt_id,
            result_artifact_id=completion.result_artifact_id,
            result_artifact_hash=completion.result_artifact_hash,
            verification_execution_present=(
                None
                if return_kind is M5TypedDirectReturnKind.DISCOVERY
                else cast(bool, row[6])
            ),
            observation_eligible_for_currency=(
                None
                if return_kind is M5TypedDirectReturnKind.DISCOVERY
                else cast(bool, row[7])
            ),
            requested_make_effective=(
                None
                if return_kind is M5TypedDirectReturnKind.DISCOVERY
                else cast(bool, row[8])
            ),
            job=spec,
            attempt=checked_attempt,
            completion=completion,
            discovery=discovery,
            scope=scope,
            persisted_scope_kind=scope_kind,
            explicit_claim_ids=explicit_ids,
            closed_revision=closed_revision,
            verification_execution=execution,
            observation=observation,
            observation_produced_epoch=observation_produced_epoch,
            observation_raw_output_hash=observation_raw_output_hash,
            job_binding_digest=job_digest,
            attempt_binding_digest=attempt_digest,
            completion_binding_digest=completion_digest,
            discovery_binding_digest=discovery_digest,
            scope_binding_digest=scope_digest,
            verifier_binding_digest=verifier_digest,
            envelope_digest=envelope_digest,
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError("D30 late envelope contract changed") from error
    canonical_bindings = _envelope_json_bindings(envelope)
    expected_json = (
        (
            canonical_bindings[0],
            canonical_bindings[1],
            canonical_bindings[2],
            canonical_bindings[3],
            canonical_bindings[4],
            None,
        )
        if return_kind is M5TypedDirectReturnKind.DISCOVERY
        else (
            canonical_bindings[0],
            canonical_bindings[1],
            canonical_bindings[2],
            None,
            None,
            canonical_bindings[3],
        )
    )
    if (
        int(str(row[0])) != owner.epoch_id
        or str(row[2]) != spec.job_id
        or str(row[3]) != checked_attempt.attempt_id
        or str(row[4]) != completion.result_artifact_id
        or _strip(row[5]) != completion.result_artifact_hash
        or _strip(row[15]) != job_digest
        or _strip(row[16]) != attempt_digest
        or _strip(row[17]) != completion_digest
        or (None if row[18] is None else _strip(row[18])) != discovery_digest
        or (None if row[19] is None else _strip(row[19])) != scope_digest
        or (None if row[20] is None else _strip(row[20])) != verifier_digest
        or _strip(row[21]) != envelope_digest
        or not all(
            _d30_exact_json_equal(actual, expected)
            for actual, expected in zip(row[9:15], expected_json, strict=True)
        )
    ):
        raise EventConflictError("D30 late envelope digest closure changed")
    return envelope_digest, completion.result_artifact_hash


def _validate_d30_d24_owner_from_held_rows(owner: _D30OwnerLocator) -> None:
    """Validate the gathered/locked D24 closure without issuing SQL."""

    d24 = owner.d24
    if d24 is None:
        raise EventConflictError("D30 typed owner lacks D24 authority")
    jobs = {str(row[0]): row for row in owner.jobs}
    attempts = {str(row[0]): row for _job_id, rows in owner.attempts for row in rows}
    expected_attempt_keys = tuple(
        (job_id, str(row[0])) for job_id, rows in owner.attempts for row in rows
    )
    located_attempt_keys = tuple(
        (located.job_id, located.attempt_id) for located in d24.attempts
    )
    if (
        len(set(located_attempt_keys)) != len(located_attempt_keys)
        or located_attempt_keys != expected_attempt_keys
    ):
        raise EventConflictError("D30 D24 attempt locator set changed")
    dispatch_offset = len(_D30_WORK_COUNTERS)
    evidence_offset = len(_D30_WORK_COUNTERS)
    zero_work = M5RuntimeWork()
    result = d24.terminal_result
    if (
        result.event_id != str(owner.epoch_row[1])
        or result.payload_hash != _strip(owner.epoch_row[2])
        or result.epoch_id != owner.epoch_id
        or result.replayed_outcome is not M5ReplayedOutcome.SEALED
    ):
        raise EventConflictError("D30 typed terminal result changed")
    for located in d24.attempts:
        attempt = attempts.get(located.attempt_id)
        job = jobs.get(located.job_id)
        if attempt is None or job is None:
            raise EventConflictError("D30 D24 attempt escaped the owner map")
        if str(attempt[1]) != located.job_id or located.attempt_state != str(
            attempt[5]
        ):
            raise EventConflictError("D30 D24 attempt state changed")
        dispatch_row = located.dispatch_row
        try:
            maximum = _d30_work_from_row(
                dispatch_row,
                counter_offset=1,
                digest_index=dispatch_offset + 1,
            )
            dispatch = M5DispatchRecord(
                epoch_id=int(str(dispatch_row[0])),
                subgraph=M5RuntimeSubgraph(str(dispatch_row[dispatch_offset + 2])),
                attempt_id=str(dispatch_row[dispatch_offset + 3]),
                logical_job_id=str(dispatch_row[dispatch_offset + 4]),
                attempt_ordinal=int(str(dispatch_row[dispatch_offset + 5])),
                job_kind=str(dispatch_row[dispatch_offset + 6]),
                fallback_required=bool(dispatch_row[dispatch_offset + 7]),
                dispatched_revision=int(str(dispatch_row[dispatch_offset + 8])),
                lease_expires_at=cast(datetime, dispatch_row[dispatch_offset + 9]),
                maximum_ambiguous_call_work=maximum,
                record_digest=_strip(dispatch_row[dispatch_offset + 10]),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError("D30 typed dispatch changed") from error
        if (
            dispatch.epoch_id != owner.epoch_id
            or dispatch.subgraph is not M5RuntimeSubgraph.DIRECT
            or dispatch.attempt_id != located.attempt_id
            or dispatch.logical_job_id != located.job_id
            or dispatch.attempt_ordinal != int(str(attempt[3]))
            or dispatch.job_kind != str(job[3])
            or dispatch.fallback_required
            or dispatch.lease_expires_at != attempt[6]
            or dispatch.dispatched_revision > int(str(owner.epoch_row[3]))
        ):
            raise EventConflictError("D30 typed dispatch binding changed")
        _validate_d30_contribution(
            located.acquisition_contribution_row,
            epoch_id=owner.epoch_id,
            contribution_kind=M5RuntimeWorkContributionKind.DIRECT_ACQUISITION,
            source_id=dispatch.record_digest,
            source_identity_hash=dispatch.record_digest,
            work=zero_work,
            applied_revision=dispatch.dispatched_revision,
        )

        evidence: M5AttemptExecutionEvidence | None = None
        if located.evidence_row is not None:
            row = located.evidence_row
            attempt_work = _d30_work_from_row(
                row,
                counter_offset=1,
                digest_index=evidence_offset + 1,
            )
            try:
                evidence = M5AttemptExecutionEvidence(
                    epoch_id=int(str(row[0])),
                    subgraph=M5RuntimeSubgraph(str(row[evidence_offset + 2])),
                    attempt_id=str(row[evidence_offset + 3]),
                    disposition=M5ExecutionEvidenceDisposition(
                        str(row[evidence_offset + 4])
                    ),
                    result_or_error_hash=_strip(row[evidence_offset + 5]),
                    attempt_work=attempt_work,
                    attempt_timing_digest=_strip(row[evidence_offset + 6]),
                    evidence_digest=_strip(row[evidence_offset + 7]),
                )
                evidence.validate_dispatch(dispatch)
            except (TypeError, ValueError, ValidationError) as error:
                raise EventConflictError(
                    "D30 typed execution evidence changed"
                ) from error
        state = located.attempt_state
        sidecars = (
            located.late_envelope_row,
            located.expired_return_row,
            located.postterminal_timing_row,
            located.postterminal_audit_row,
            located.preterminal_late_contribution_row,
        )
        successful_dispositions = {
            M5ExecutionEvidenceDisposition.RETURNED,
            M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
        }
        event_accounting = False
        expected_attempt_revision: int | None = None
        expected_timing_points = {
            (
                M5RuntimeWorkContributionKind.DIRECT_ACQUISITION.value,
                dispatch.record_digest,
                dispatch.dispatched_revision,
            )
        }
        if state == "completed":
            if (
                evidence is None
                or evidence.disposition not in successful_dispositions
                or evidence.result_or_error_hash != _strip(job[15])
                or any(value is not None for value in sidecars)
            ):
                raise EventConflictError("D30 completed attempt evidence changed")
            event_accounting = True
            expected_attempt_revision = int(str(job[17]))
        elif state == "failed":
            if (
                evidence is None
                or evidence.disposition
                is not M5ExecutionEvidenceDisposition.RETRYABLE_FAILURE
                or any(value is not None for value in sidecars)
            ):
                raise EventConflictError("D30 failed attempt evidence changed")
            event_accounting = True
        elif state == "expired":
            if (
                located.transition_contribution_row is not None
                or located.m4_transition_row is not None
            ):
                raise EventConflictError("D30 expired attempt has a completion")
            if evidence is None:
                if (
                    located.timing_row is not None
                    or located.attempt_contribution_row is not None
                    or any(value is not None for value in sidecars)
                ):
                    raise EventConflictError(
                        "D30 unresolved expired attempt has dependent rows"
                    )
            else:
                if evidence.disposition not in successful_dispositions:
                    raise EventConflictError("D30 expired attempt evidence changed")
                envelope_digest, worker_artifact_hash = _validate_d30_late_envelope(
                    owner=owner,
                    located=located,
                    attempt=attempt,
                    job=job,
                )
                expired_row = located.expired_return_row
                if expired_row is None or type(expired_row[13]) is not bool:
                    raise EventConflictError(
                        "D30 expired evidence lacks its audit sidecar"
                    )
                try:
                    expired = M5ExpiredAttemptReturn.build(
                        subgraph=M5RuntimeSubgraph.DIRECT,
                        epoch_id=owner.epoch_id,
                        attempt_id=located.attempt_id,
                        logical_job_id=located.job_id,
                        worker_output_digest=envelope_digest,
                        worker_artifact_hash=worker_artifact_hash,
                        activity_snapshot_epoch_id=int(str(expired_row[8])),
                        activity_snapshot_revision=int(str(expired_row[9])),
                        received_after_terminal=expired_row[13],
                    )
                except (TypeError, ValueError, ValidationError) as error:
                    raise EventConflictError(
                        "D30 expired-return identity changed"
                    ) from error
                if (
                    evidence.result_or_error_hash != envelope_digest
                    or int(str(expired_row[0])) != owner.epoch_id
                    or str(expired_row[1]) != M5RuntimeSubgraph.DIRECT.value
                    or str(expired_row[2]) != located.attempt_id
                    or str(expired_row[3]) != located.job_id
                    or _strip(expired_row[4]) != _strip(attempt[4])
                    or expired_row[5] != attempt[6]
                    or _strip(expired_row[6]) != envelope_digest
                    or _strip(expired_row[7]) != worker_artifact_hash
                    or int(str(expired_row[8])) != owner.epoch_id
                    or expired_row[10]
                    != {
                        "cancelled_by_event_id": None,
                        "cancelled_by_epoch_id": None,
                        "cancellation_reason": None,
                    }
                    or str(expired_row[11]) != "attempt_expired"
                    or _strip(expired_row[12]) != evidence.evidence_digest
                    or _strip(expired_row[14]) != expired.expired_return_digest
                ):
                    raise EventConflictError("D30 expired-return closure changed")
                activity_revision = expired.activity_snapshot_revision
                if expired.received_after_terminal:
                    if (
                        activity_revision != int(str(owner.epoch_row[3]))
                        or located.timing_row is not None
                        or located.attempt_contribution_row is not None
                        or located.preterminal_late_contribution_row is not None
                        or located.postterminal_timing_row is None
                        or located.postterminal_audit_row is None
                    ):
                        raise EventConflictError(
                            "D30 postterminal late-return closure changed"
                        )
                    post_timing = located.postterminal_timing_row
                    observation = _d30_timing_observation(
                        post_timing, required_index=3, digest_index=13
                    )
                    if (
                        int(str(post_timing[0])) != owner.epoch_id
                        or str(post_timing[1]) != M5RuntimeSubgraph.DIRECT.value
                        or str(post_timing[2]) != located.attempt_id
                        or _strip(post_timing[14]) != evidence.attempt_timing_digest
                    ):
                        raise EventConflictError(
                            "D30 postterminal timing binding changed"
                        )
                    try:
                        evidence.validate_timing(observation)
                    except ValidationError as error:
                        raise EventConflictError(
                            "D30 postterminal timing digest changed"
                        ) from error
                    audit = located.postterminal_audit_row
                    if tuple(audit) != (
                        owner.epoch_id,
                        M5RuntimeSubgraph.DIRECT.value,
                        located.attempt_id,
                        "expired_return",
                        expired.expired_return_digest,
                        evidence.evidence_digest,
                        evidence.attempt_work.work_digest,
                        evidence.attempt_timing_digest,
                        result.logical_result_hash,
                    ):
                        raise EventConflictError(
                            "D30 postterminal audit binding changed"
                        )
                else:
                    if (
                        activity_revision >= int(str(owner.epoch_row[3]))
                        or located.postterminal_timing_row is not None
                        or located.postterminal_audit_row is not None
                        or located.timing_row is None
                        or located.attempt_contribution_row is None
                        or located.preterminal_late_contribution_row is None
                    ):
                        raise EventConflictError(
                            "D30 preterminal late-return closure changed"
                        )
                    event_accounting = True
                    expected_attempt_revision = activity_revision
                    late_work = M5RuntimeWork(requirement_late_attempt_artifact_count=1)
                    _validate_d30_contribution(
                        located.preterminal_late_contribution_row,
                        epoch_id=owner.epoch_id,
                        contribution_kind=(
                            M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN
                        ),
                        source_id=located.attempt_id,
                        source_identity_hash=expired.expired_return_digest,
                        work=late_work,
                        applied_revision=activity_revision,
                    )
                    expected_timing_points.add(
                        (
                            M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN.value,
                            located.attempt_id,
                            activity_revision,
                        )
                    )
        else:
            raise EventConflictError("D30 sealed owner retained an open attempt")

        attempt_revision: int | None = None
        if event_accounting:
            assert evidence is not None
            if located.timing_row is None or located.attempt_contribution_row is None:
                raise EventConflictError("D30 execution evidence closure is incomplete")
            timing_observation = _d30_timing_observation(
                located.timing_row, required_index=4, digest_index=14
            )
            if (
                int(str(located.timing_row[0])) != owner.epoch_id
                or str(located.timing_row[1]) != M5RuntimeSubgraph.DIRECT.value
                or str(located.timing_row[2]) != located.attempt_id
                or _strip(located.timing_row[3]) != evidence.evidence_digest
                or _strip(located.timing_row[15]) != evidence.attempt_timing_digest
            ):
                raise EventConflictError("D30 attempt timing binding changed")
            try:
                evidence.validate_timing(timing_observation)
            except ValidationError as error:
                raise EventConflictError("D30 attempt timing digest changed") from error
            attempt_revision = _validate_d30_contribution(
                located.attempt_contribution_row,
                epoch_id=owner.epoch_id,
                contribution_kind=(
                    M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION
                ),
                source_id=located.attempt_id,
                source_identity_hash=evidence.evidence_digest,
                work=evidence.attempt_work,
                applied_revision=expected_attempt_revision,
            )
            if state == "failed" and attempt_revision >= int(str(job[17])):
                raise EventConflictError("D30 failed attempt revision changed")

        if state == "completed":
            expected_timing_points.add(
                (
                    M5RuntimeWorkContributionKind.DIRECT_TRANSITION.value,
                    _strip(job[13]),
                    int(str(job[17])),
                )
            )
            if (
                located.transition_contribution_row is None
                or located.m4_transition_row is None
            ):
                raise EventConflictError("D30 completed transition closure is absent")
            transition = located.m4_transition_row
            if (
                int(str(transition[0])) != owner.epoch_id
                or str(transition[1]) != _strip(job[13])
                or str(transition[3]) != "delta"
                or int(str(transition[5])) != int(str(job[17]))
                or int(str(transition[4])) + 1 != int(str(transition[5]))
            ):
                raise EventConflictError("D30 M4 completion transition changed")
            transition_work = _d30_work_from_row(
                located.transition_contribution_row,
                counter_offset=1,
                digest_index=len(_D30_WORK_COUNTERS) + 1,
            )
            _validate_d30_work_counter_subset(
                transition_work,
                allowed=_D30_DIRECT_TRANSITION_COUNTERS,
                label="direct-transition",
            )
            _validate_d30_contribution(
                located.transition_contribution_row,
                epoch_id=owner.epoch_id,
                contribution_kind=M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
                source_id=_strip(job[13]),
                source_identity_hash=_strip(transition[2]),
                work=transition_work,
                applied_revision=int(str(job[17])),
            )
        elif state == "failed":
            assert attempt_revision is not None
            expected_timing_points.add(
                (
                    M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION.value,
                    located.attempt_id,
                    attempt_revision,
                )
            )
            if (
                located.transition_contribution_row is not None
                or located.m4_transition_row is not None
            ):
                raise EventConflictError("D30 failed attempt has a completion")

        actual_timing_points = tuple(
            (str(row[1]), str(row[2]), int(str(row[4])))
            for row in located.transition_timing_rows
        )
        if (
            len(set(actual_timing_points)) != len(actual_timing_points)
            or set(actual_timing_points) != expected_timing_points
        ):
            raise EventConflictError("D30 transition timing set changed")
        for row in located.transition_timing_rows:
            observation = _d30_timing_observation(
                row, required_index=5, digest_index=15
            )
            expected_key = digests.runtime_work_contribution_key_digest(
                epoch_id=owner.epoch_id,
                contribution_kind=str(row[1]),
                source_id=str(row[2]),
            )
            if (
                int(str(row[0])) != owner.epoch_id
                or (str(row[1]), str(row[2]), int(str(row[4])))
                not in expected_timing_points
                or _strip(row[3]) != expected_key
                or _strip(row[16])
                != digests.transition_call_timing_digest(
                    epoch_id=owner.epoch_id,
                    contribution_kind=str(row[1]),
                    source_id=str(row[2]),
                    contribution_key_digest=expected_key,
                    anchor_revision=int(str(row[4])),
                    observation_digest=observation.observation_digest,
                )
            ):
                raise EventConflictError("D30 transition timing digest changed")
    accumulator = d24.work_accumulator_row
    accumulated_work = _d30_work_from_row(
        accumulator,
        counter_offset=1,
        digest_index=len(_D30_WORK_COUNTERS) + 1,
    )
    if (
        int(str(accumulator[0])) != owner.epoch_id
        or accumulated_work != result.event_work
        or int(str(accumulator[len(_D30_WORK_COUNTERS) + 2]))
        != int(str(owner.epoch_row[3]))
        or accumulator[len(_D30_WORK_COUNTERS) + 3] is not True
    ):
        raise EventConflictError("D30 terminal work accumulator changed")
    timing_row = d24.timing_accumulator_row
    try:
        accumulated_coverage = M5RuntimeTimingCoverage(
            required_expected_count=int(str(timing_row[10])),
            required_observed_count=int(str(timing_row[11])),
            required_missing_count=int(str(timing_row[12])),
            postgres_server_execution_expected_count=int(str(timing_row[13])),
            postgres_server_execution_observed_count=int(str(timing_row[14])),
            postgres_server_execution_missing_count=int(str(timing_row[15])),
            postgres_lock_wait_expected_count=int(str(timing_row[16])),
            postgres_lock_wait_observed_count=int(str(timing_row[17])),
            postgres_lock_wait_missing_count=int(str(timing_row[18])),
            postgres_wal_bytes_expected_count=int(str(timing_row[19])),
            postgres_wal_bytes_observed_count=int(str(timing_row[20])),
            postgres_wal_bytes_missing_count=int(str(timing_row[21])),
            postgres_shared_block_reads_expected_count=int(str(timing_row[22])),
            postgres_shared_block_reads_observed_count=int(str(timing_row[23])),
            postgres_shared_block_reads_missing_count=int(str(timing_row[24])),
            terminal_client_roundtrip_included=False,
        )
        optional_coordinates = (
            (6, 14, 15),
            (7, 17, 18),
            (8, 20, 21),
            (9, 23, 24),
        )

        def projected_optional(
            value_index: int, observed_index: int, missing_index: int
        ) -> int | None:
            return (
                int(str(timing_row[value_index]))
                if int(str(timing_row[observed_index])) > 0
                and int(str(timing_row[missing_index])) == 0
                else None
            )

        optional_values = tuple(
            projected_optional(*coordinate) for coordinate in optional_coordinates
        )
        accumulated_timing = M5RuntimeTiming(
            coordinator_non_db_non_neural_ns=int(str(timing_row[1])),
            neural_wall_ns=int(str(timing_row[2])),
            postgres_roundtrip_wall_ns=int(str(timing_row[3])),
            external_io_wall_ns=int(str(timing_row[4])),
            end_to_end_wall_ns=int(str(timing_row[5])),
            postgres_server_execution_ns=optional_values[0],
            postgres_lock_wait_ns=optional_values[1],
            postgres_wal_bytes=optional_values[2],
            postgres_shared_block_reads=optional_values[3],
        )
        accumulated_coverage.validate_aggregate(accumulated_timing)
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError("D30 terminal timing accumulator changed") from error
    if (
        int(str(timing_row[0])) != owner.epoch_id
        or accumulated_timing != result.event_timing
        or accumulated_coverage != result.event_timing_coverage
        or any(value is not None for value in timing_row[25:29])
        or int(str(timing_row[29])) != int(str(owner.epoch_row[3]))
        or timing_row[30] is not True
    ):
        raise EventConflictError("D30 terminal timing cutoff changed")
    publication = result.publication_receipt
    if publication is None:
        raise EventConflictError("D30 sealed owner lost its publication receipt")
    seal_identity = digests.seal_contribution_source_digest(
        structural_event_id=str(owner.epoch_row[1]),
        combined_status_delta_set_hash=digests.combined_status_delta_set_digest(
            result.combined_deltas
        ),
        changed_state_set_hash=digests.changed_state_set_digest(
            reference.reference_digest for reference in result.changed_state_references
        ),
        publication_id=publication.publication_id,
    )
    seal_work = _d30_work_from_row(
        d24.seal_contribution_row,
        counter_offset=1,
        digest_index=len(_D30_WORK_COUNTERS) + 1,
    )
    _validate_d30_work_counter_subset(
        seal_work,
        allowed=_D30_SEAL_COUNTERS,
        label="seal",
    )
    if seal_work.public_delta_count != len(result.combined_deltas):
        raise EventConflictError("D30 seal public-delta accounting changed")
    _validate_d30_contribution(
        d24.seal_contribution_row,
        epoch_id=owner.epoch_id,
        contribution_kind=M5RuntimeWorkContributionKind.SEAL,
        source_id=str(owner.epoch_row[1]),
        source_identity_hash=seal_identity,
        work=seal_work,
        applied_revision=int(str(owner.epoch_row[3])),
    )


def _validate_d30_owner_topology_from_held_rows(
    owner: _D30OwnerLocator,
    *,
    candidate_policy_id: str,
    verifier_execution_spec_hash: str,
    activation_base_epoch_id: int,
    predecessor_epoch_id: int,
) -> dict[str, M4LogicalJobSpec]:
    """Validate one complete D30 owner using only already-held row bytes."""

    epoch = owner.epoch_row
    update = owner.update_row
    if (
        int(str(epoch[0])) != owner.epoch_id
        or not str(epoch[1]).strip()
        or len(_strip(epoch[2])) != 64
        or type(epoch[3]) is not int
        or int(epoch[3]) < 1
        or tuple(epoch[4:8]) != ("committed", "sealed", "complete", "strict")
        or epoch[8] is None
        or owner.epoch_id > predecessor_epoch_id
        or int(str(update[0])) != owner.epoch_id
        or str(update[2]) != candidate_policy_id
        or (update[3] is not None and int(str(update[3])) >= owner.epoch_id)
        or not str(update[4]).strip()
        or type(update[5]) is not dict
    ):
        raise EventConflictError("D30 direct owner publication authority changed")
    try:
        UpdateKind(str(update[1]))
    except ValueError as error:
        raise EventConflictError("D30 direct owner update kind is invalid") from error

    if owner.jobs != tuple(sorted(owner.jobs, key=lambda row: _c_key(str(row[0])))):
        raise EventConflictError("D30 direct owner jobs are not C-sorted")
    if owner.dependencies != tuple(
        sorted(
            owner.dependencies,
            key=lambda row: (
                int(str(row[0])),
                _c_key(str(row[1])),
                _c_key(str(row[2])),
            ),
        )
    ):
        raise EventConflictError("D30 direct owner dependencies are not C-sorted")

    jobs: dict[str, tuple[object, ...]] = {}
    specs: dict[str, M4LogicalJobSpec] = {}
    verifier_pairs: set[PairKey] = set()
    for row in owner.jobs:
        job_id = str(row[0])
        if job_id in jobs or int(str(row[1])) != owner.epoch_id:
            raise EventConflictError("D30 direct owner repeats or crosses a job")
        jobs[job_id] = row
        try:
            kind = M4JobKind(str(row[3]))
            state = M4JobState(str(row[10]))
            if state not in {
                M4JobState.COMPLETED_ACTIVE,
                M4JobState.COMPLETED_INACTIVE,
            }:
                raise ValidationError("sealed D30 owner has a noncompleted job")
            claim_id = None if row[7] is None else str(row[7])
            chunk_id = None if row[8] is None else str(row[8])
            parent_id = None if row[2] is None else str(row[2])
            pair = (
                PairKey(claim_id, chunk_id)
                if kind is M4JobKind.VERIFY_PAIR
                and claim_id is not None
                and chunk_id is not None
                else None
            )
            spec = M4LogicalJobSpec(
                job_id=job_id,
                event_id=str(epoch[1]),
                kind=kind,
                candidate_policy_id=str(row[4]),
                payload_hash=_strip(row[5]),
                execution_spec_hash=_strip(row[6]),
                parent_job_id=parent_id,
                pair=pair,
                target_claim_id=(
                    claim_id if kind is M4JobKind.FRONTIER_RETRIEVE else None
                ),
                target_chunk_version_id=(
                    chunk_id if kind is M4JobKind.IMPACT_DISCOVERY else None
                ),
                expandable=bool(row[9]),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError("D30 direct owner job identity changed") from error
        expected_payload = stable_m4_digest(
            "m4-application-job-payload-v1",
            _strip(epoch[2]),
            kind.value,
            parent_id or "",
            claim_id or "",
            chunk_id or "",
        )
        if (
            spec.candidate_policy_id != candidate_policy_id
            or (
                kind is M4JobKind.VERIFY_PAIR
                and spec.execution_spec_hash != verifier_execution_spec_hash
            )
            or spec.payload_hash != expected_payload
        ):
            raise EventConflictError("D30 direct owner job policy/payload changed")
        if pair is not None:
            if pair in verifier_pairs:
                raise EventConflictError("D30 direct owner repeats a verifier pair")
            verifier_pairs.add(pair)
        specs[job_id] = spec

    expected_dependencies: set[tuple[int, str, str]] = set()
    children: dict[str, list[str]] = {}
    for job_id, spec in specs.items():
        if spec.parent_job_id is None:
            if spec.kind not in {
                M4JobKind.IMPACT_DISCOVERY,
                M4JobKind.FRONTIER_RETRIEVE,
            }:
                raise EventConflictError("D30 direct owner has a parentless child")
            continue
        parent = specs.get(spec.parent_job_id)
        if (
            spec.kind is not M4JobKind.VERIFY_PAIR
            or parent is None
            or parent.parent_job_id is not None
            or parent.kind
            not in {M4JobKind.IMPACT_DISCOVERY, M4JobKind.FRONTIER_RETRIEVE}
        ):
            raise EventConflictError("D30 direct owner has an orphan or grandchild")
        pair = spec.pair
        if (
            pair is None
            or (
                parent.kind is M4JobKind.IMPACT_DISCOVERY
                and parent.target_chunk_version_id != pair.chunk_version_id
            )
            or (
                parent.kind is M4JobKind.FRONTIER_RETRIEVE
                and parent.target_claim_id != pair.claim_id
            )
        ):
            raise EventConflictError("D30 verifier child target disagrees with root")
        coordinate = (owner.epoch_id, spec.parent_job_id, job_id)
        expected_dependencies.add(coordinate)
        children.setdefault(spec.parent_job_id, []).append(job_id)
    actual_dependencies = {
        (int(str(row[0])), str(row[1]), str(row[2])) for row in owner.dependencies
    }
    if (
        len(actual_dependencies) != len(owner.dependencies)
        or actual_dependencies != expected_dependencies
    ):
        raise EventConflictError("D30 direct owner dependency topology changed")

    scope_points = dict(owner.scopes)
    attempt_points = dict(owner.attempts)
    result_points = dict(owner.discovery_results)
    projection_points = dict(owner.projections)
    if any(
        set(points) != set(jobs)
        for points in (scope_points, attempt_points, result_points, projection_points)
    ):
        raise EventConflictError("D30 direct owner point map is incomplete")

    typed_owner = owner.runtime_row is not None
    if typed_owner:
        runtime = owner.runtime_row
        assert runtime is not None
        if (
            int(str(runtime[0])) != owner.epoch_id
            or str(runtime[1]) != str(epoch[1])
            or str(runtime[2]) != candidate_policy_id
            or (runtime[6] is None) != (update[3] is None)
            or (runtime[6] is not None and int(str(runtime[6])) != int(str(update[3])))
            or str(runtime[8]) != "sealed"
            or type(runtime[9]) is not int
            or int(runtime[9]) != int(epoch[3])
            or tuple(runtime[10:13]) != (0, 0, 0)
            or runtime[13] is None
        ):
            raise EventConflictError("D30 typed owner runtime header changed")
    elif owner.epoch_id > activation_base_epoch_id:
        raise EventConflictError("D30 legacy owner is after the activation base")

    for job_id, row in jobs.items():
        spec = specs[job_id]
        child_ids = tuple(sorted(children.get(job_id, ()), key=_c_key))
        _validate_m4_job_state(spec, row, child_ids)

        scope = scope_points[job_id]
        expects_scope = spec.kind is M4JobKind.IMPACT_DISCOVERY
        if expects_scope:
            if (
                scope is None
                or str(scope[0]) != job_id
                or int(str(scope[1])) != owner.epoch_id
                or str(scope[2]) != str(update[4])
                or str(scope[3]) != "all_registered_claims"
                or scope[4] is not None
                or type(scope[5]) is not int
                or type(row[17]) is not int
                or int(scope[5]) != int(row[17])
            ):
                raise EventConflictError("D30 impact-root scope changed")
        elif scope is not None:
            raise EventConflictError("D30 frontier/child unexpectedly owns a scope")

        attempts_for_job = attempt_points[job_id]
        if not attempts_for_job or str(attempts_for_job[-1][5]) != "completed":
            raise EventConflictError("D30 completed job lacks a successful attempt")

        result = result_points[job_id]
        if spec.parent_job_id is None:
            if (
                result is None
                or str(result[0]) != job_id
                or int(str(result[1])) != owner.epoch_id
                or not str(result[2]).strip()
                or str(result[2]) != str(row[14])
                or len(_strip(result[3])) != 64
                or _strip(result[3]) != _strip(row[15])
                or type(result[4]) is not bool
                or any(
                    type(result[index]) is not int or int(str(result[index])) < 0
                    for index in (5, 6)
                )
                or any(len(_strip(result[index])) != 64 for index in (7, 8))
            ):
                raise EventConflictError("D30 parent discovery result changed")
        elif result is not None:
            raise EventConflictError("D30 verifier child owns a discovery result")

        projection = projection_points[job_id]
        if typed_owner:
            if projection is None:
                raise EventConflictError("D30 typed owner lost a terminal projection")
            try:
                checked_projection = M5TypedDirectTerminalProjection(
                    terminal_state=M4JobState(str(projection[2])),
                    terminal_reason=(
                        None if projection[3] is None else str(projection[3])
                    ),
                    m4_completion_digest=(
                        None if projection[4] is None else _strip(projection[4])
                    ),
                    completed_revision=int(str(projection[5])),
                    terminal_identity_hash=_strip(projection[6]),
                )
                checked_projection.validate_job(job_id)
            except (TypeError, ValueError, ValidationError) as error:
                raise EventConflictError(
                    "D30 typed terminal projection is malformed"
                ) from error
            if (
                int(str(projection[0])) != owner.epoch_id
                or str(projection[1]) != job_id
                or checked_projection.terminal_state.value != str(row[10])
                or checked_projection.m4_completion_digest != _strip(row[13])
                or type(row[17]) is not int
                or checked_projection.completed_revision != int(row[17])
            ):
                raise EventConflictError("D30 typed terminal projection changed")
        elif projection is not None:
            raise EventConflictError("D30 legacy owner has a typed projection")
    if typed_owner:
        _validate_d30_d24_owner_from_held_rows(owner)
    elif owner.d24 is not None:
        raise EventConflictError("D30 legacy owner has typed D24 authority")
    return specs


def _reread_d30_owner_headers(cursor: Cursor[Any], owner: _D30OwnerLocator) -> None:
    """Nonlocking PK reread of each historical owner header/classifier."""

    epoch = cursor.execute(
        """
        SELECT epoch_id, event_id, payload_hash, revision, structural_status,
               semantic_status, evaluation_state, publication_mode, sealed_at
        FROM groundloop_epoch WHERE epoch_id = %s
        """,
        (owner.epoch_id,),
    ).fetchone()
    update = cursor.execute(
        """
        SELECT epoch_id, update_kind, candidate_policy_id,
               previous_published_epoch_id, registry_snapshot_id, manifest
        FROM groundloop_m4_update WHERE epoch_id = %s
        """,
        (owner.epoch_id,),
    ).fetchone()
    runtime = cursor.execute(
        """
        SELECT epoch_id, structural_event_id, candidate_policy_id,
               candidate_policy_manifest_hash,
               requirement_registry_snapshot_digest,
               active_chunk_snapshot_digest,
               expected_previous_published_epoch_id,
               requirement_root_set_hash, runtime_state, revision,
               open_work_count, open_scope_count, blocking_failure_count,
               terminal_at
        FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
        """,
        (owner.epoch_id,),
    ).fetchone()
    if (
        epoch is None
        or tuple(epoch) != owner.epoch_row
        or update is None
        or tuple(update) != owner.update_row
        or (None if runtime is None else tuple(runtime)) != owner.runtime_row
    ):
        raise EventConflictError("D30 dynamic owner header changed")


def _lock_d30_dynamic_owner_topology(
    cursor: Cursor[Any],
    locator: _D29LocatorAuthority,
    attempts: dict[str, _DirectAttemptAuthority],
    scopes: dict[str, _DirectScopeAuthority],
    *,
    predecessor_epoch_id: int,
    candidate_policy_id: str,
    verifier_execution_spec_hash: str,
) -> dict[str, _D30DynamicClaimLocator]:
    """Finalize D30 owner branches after their complete tier-8/9/10 locks."""

    if not locator.d30_claims.currency_rows:
        return {}
    activation_base = _validate_d30_activation_row(
        locator.d30_claims.activation_row,
        predecessor_epoch_id=predecessor_epoch_id,
    )
    owner_by_epoch = {owner.epoch_id: owner for owner in locator.d30_claims.owners}
    if len(owner_by_epoch) != len(locator.d30_claims.owners):
        raise EventConflictError("D30 owner epoch map is ambiguous")
    ordered_owners = tuple(
        sorted(locator.d30_claims.owners, key=lambda owner: owner.epoch_id)
    )
    if ordered_owners != locator.d30_claims.owners:
        raise EventConflictError("D30 owner epochs are not sorted")
    all_pairs: set[PairKey] = set()
    for topology_owner in ordered_owners:
        _reread_d30_owner_headers(cursor, topology_owner)
        specs = _validate_d30_owner_topology_from_held_rows(
            topology_owner,
            candidate_policy_id=candidate_policy_id,
            verifier_execution_spec_hash=verifier_execution_spec_hash,
            activation_base_epoch_id=activation_base,
            predecessor_epoch_id=predecessor_epoch_id,
        )
        owner_job_ids = set(specs)
        if not owner_job_ids <= set(attempts):
            raise EventConflictError("D30 owner escaped held attempt authority")
        expected_scopes = {
            job_id
            for job_id, spec in specs.items()
            if spec.kind is M4JobKind.IMPACT_DISCOVERY
        }
        if not expected_scopes <= set(scopes):
            raise EventConflictError("D30 owner escaped held scope authority")
        for spec in specs.values():
            if spec.pair is not None:
                if spec.pair in all_pairs:
                    raise EventConflictError("D30 owners repeat a verifier pair")
                all_pairs.add(spec.pair)
    authority: dict[str, _D30DynamicClaimLocator] = {}
    selected_children: set[str] = set()
    for dynamic_claim in locator.d30_claims.dynamic:
        dynamic_owner = owner_by_epoch.get(dynamic_claim.owner_epoch_id)
        if dynamic_owner is None:
            raise EventConflictError("D30 dynamic claim lost its complete owner")
        if dynamic_claim.child_job_id in selected_children:
            raise EventConflictError("D30 claim holders reuse a child revision")
        selected_children.add(dynamic_claim.child_job_id)
        _validate_d30_dynamic_claim(
            dynamic_claim,
            owner=dynamic_owner,
            candidate_policy_id=candidate_policy_id,
        )
        if dynamic_claim.currency.observation_id in authority:
            raise EventConflictError("D30 observation authority is ambiguous")
        authority[dynamic_claim.currency.observation_id] = dynamic_claim

    for bootstrap_claim in locator.d30_claims.bootstrap:
        _validate_d30_m3_bootstrap(
            bootstrap_claim,
            activation_base_epoch_id=activation_base,
            predecessor_epoch_id=predecessor_epoch_id,
        )
    return authority


def _lock_d29_direct_claim_before_images(
    cursor: Cursor[Any],
    claim_images: tuple[_D29DirectClaimBeforeImage, ...],
    *,
    predecessor_epoch_id: int,
) -> None:
    """Lock and revalidate the bounded legacy/current M4 before-image closure."""

    claim_ids = tuple(image.claim_id for image in claim_images)
    if claim_ids != tuple(sorted(set(claim_ids), key=_c_key)):
        raise EventConflictError("D29 direct claim before-image keys changed")
    for image in claim_images:
        state_rows = cursor.execute(
            """
            SELECT support_count, refute_count, best_support_score,
                   best_refute_score, supporting_observation_ids,
                   refuting_observation_ids, status, certificate_digest,
                   valid_from_epoch
            FROM groundloop_published_claim_state
            WHERE claim_id = %s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            FOR UPDATE
            """,
            (image.claim_id, predecessor_epoch_id, predecessor_epoch_id),
        ).fetchall()
        materialized_rows = cursor.execute(
            """
            SELECT support_count, refute_count, best_support_score,
                   best_refute_score, supporting_observation_ids,
                   refuting_observation_ids, status, updated_epoch,
                   updated_revision
            FROM groundloop_claim_state_materialized
            WHERE claim_id = %s
            FOR UPDATE
            """,
            (image.claim_id,),
        ).fetchall()
        certificate_rows = cursor.execute(
            """
            SELECT support_observation_id, refute_observation_id,
                   repaired_epoch, repaired_revision
            FROM groundloop_claim_certificate
            WHERE claim_id = %s
            FOR UPDATE
            """,
            (image.claim_id,),
        ).fetchall()
        if (
            len(state_rows) != 1
            or len(materialized_rows) != 1
            or len(certificate_rows) != 1
        ):
            raise EventConflictError(
                "D29 direct claim materialized certificate point changed"
            )
        state = state_rows[0]
        materialized = materialized_rows[0]
        certificate = certificate_rows[0]
        try:
            locked_state = (
                int(state[0]),
                int(state[1]),
                None if state[2] is None else float(str(state[2])),
                None if state[3] is None else float(str(state[3])),
                tuple(str(value) for value in state[4]),
                tuple(str(value) for value in state[5]),
                str(state[6]),
                _strip(state[7]),
                int(state[8]),
            )
            locked_materialized = (
                int(materialized[0]),
                int(materialized[1]),
                None if materialized[2] is None else float(str(materialized[2])),
                None if materialized[3] is None else float(str(materialized[3])),
                tuple(str(value) for value in materialized[4]),
                tuple(str(value) for value in materialized[5]),
                str(materialized[6]),
                int(materialized[7]),
                int(materialized[8]),
            )
            locked_certificate = (
                None if certificate[0] is None else str(certificate[0]),
                None if certificate[1] is None else str(certificate[1]),
                int(certificate[2]),
                int(certificate[3]),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError(
                "D29 direct claim materialized certificate point is malformed"
            ) from error
        expected_state = (
            image.support_count,
            image.refute_count,
            image.best_support_score,
            image.best_refute_score,
            image.supporting_observation_ids,
            image.refuting_observation_ids,
            image.status,
            image.certificate_digest,
            image.state_valid_from_epoch,
        )
        expected_materialized = (
            image.support_count,
            image.refute_count,
            image.best_support_score,
            image.best_refute_score,
            image.supporting_observation_ids,
            image.refuting_observation_ids,
            image.status,
            image.materialized_updated_epoch,
            image.materialized_updated_revision,
        )
        expected_certificate = (
            image.certificate_support_observation_id,
            image.certificate_refute_observation_id,
            image.certificate_repaired_epoch,
            image.certificate_repaired_revision,
        )
        if (
            locked_state != expected_state
            or locked_materialized != expected_materialized
            or locked_certificate != expected_certificate
            or image.materialized_updated_epoch != image.state_valid_from_epoch
            or image.certificate_repaired_epoch != image.materialized_updated_epoch
            or image.certificate_repaired_revision
            != image.materialized_updated_revision
        ):
            raise EventConflictError(
                "D29 direct claim materialized certificate closure changed"
            )


def _lock_d29_direct_answer_before_images(
    cursor: Cursor[Any],
    answer_images: tuple[_D29DirectAnswerBeforeImage, ...],
    *,
    predecessor_epoch_id: int,
) -> None:
    """Lock and revalidate the complete bounded direct-answer before images."""

    answer_ids = tuple(image.answer_version_id for image in answer_images)
    if answer_ids != tuple(sorted(set(answer_ids), key=_c_key)):
        raise EventConflictError("D29 direct answer before-image keys changed")
    for image in answer_images:
        rows = cursor.execute(
            """
            SELECT required_claim_count, supported_count, unsupported_count,
                   refuted_count, conflicted_count, status
            FROM groundloop_published_answer_state
            WHERE answer_version_id = %s AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            FOR UPDATE
            """,
            (
                image.answer_version_id,
                predecessor_epoch_id,
                predecessor_epoch_id,
            ),
        ).fetchall()
        if len(rows) != 1:
            raise EventConflictError("D29 direct answer point changed")
        row = rows[0]
        try:
            locked = (
                int(row[0]),
                int(row[1]),
                int(row[2]),
                int(row[3]),
                int(row[4]),
                str(row[5]),
            )
        except (TypeError, ValueError) as error:
            raise EventConflictError("D29 direct answer point is malformed") from error
        expected = (
            image.required_claim_count,
            image.supported_count,
            image.unsupported_count,
            image.refuted_count,
            image.conflicted_count,
            image.status,
        )
        if locked != expected:
            raise EventConflictError("D29 direct answer point changed")


def _lock_d29_observation_authority(
    cursor: Cursor[Any],
    locator: _D29LocatorAuthority,
    *,
    predecessor_epoch_id: int,
    candidate_policy_id: str,
    verifier_authority: dict[str, _RequirementVerifierAuthority],
    bootstrap_authority: dict[str, _BootstrapObservationAuthority],
    d30_dynamic_authority: dict[str, _D30DynamicClaimLocator],
) -> tuple[
    tuple[M5WithdrawnObservationEdge, ...],
    tuple[ObservationDependency, ...],
]:
    """Acquire tier 11a in exact semantic-observation then currency order."""

    currency_typed_keys = tuple(
        sorted(
            {
                ("requirement", requirement_id, chunk_id, task_type, observation_id)
                for requirement_id, chunk_id, task_type, observation_id in (
                    locator.requirement_currency_keys
                )
            }
            | {
                (
                    currency.subject_kind,
                    currency.subject_id,
                    currency.chunk_version_id,
                    currency.task_type,
                    currency.observation_id,
                )
                for currency in locator.d30_claims.currency_rows
            },
            key=lambda item: tuple(_c_key(value) for value in item),
        )
    )
    if len(currency_typed_keys) != (
        len(locator.requirement_currency_keys) + len(locator.d30_claims.currency_rows)
    ):
        raise EventConflictError("D30 typed observation locator is ambiguous")
    explicit_withdrawn_typed_keys = tuple(
        ("claim", claim_id, chunk_id, task_type, observation_id)
        for claim_id, chunk_id, task_type, observation_id in (
            locator.direct_currency_keys
        )
    )
    remaining_typed_keys = tuple(
        (
            str(row[1]),
            str(row[2]),
            str(row[3]),
            str(row[4]),
            str(row[0]),
        )
        for row in locator.direct_remaining_observation_rows
    )
    direct_currency_typed_keys = tuple(
        (
            row.subject_kind,
            row.subject_id,
            row.chunk_version_id,
            row.task_type,
            row.observation_id,
        )
        for row in locator.d30_claims.currency_rows
    )
    retained_unpartitioned_direct_state = not any(
        (
            locator.direct_currency_keys,
            locator.direct_claim_before_images,
            locator.direct_answer_before_images,
            locator.direct_remaining_observation_rows,
            locator.direct_observation_source_rows,
        )
    )
    withdrawn_typed_keys = (
        direct_currency_typed_keys
        if retained_unpartitioned_direct_state
        else explicit_withdrawn_typed_keys
    )
    withdrawn_direct_observation_ids = {item[4] for item in withdrawn_typed_keys}
    remaining_direct_observation_ids = {item[4] for item in remaining_typed_keys}
    direct_currency_observation_ids = {item[4] for item in direct_currency_typed_keys}
    if (
        len(set(withdrawn_typed_keys)) != len(withdrawn_typed_keys)
        or len(withdrawn_direct_observation_ids) != len(withdrawn_typed_keys)
        or len(set(remaining_typed_keys)) != len(remaining_typed_keys)
        or len(remaining_direct_observation_ids) != len(remaining_typed_keys)
        or len(set(direct_currency_typed_keys)) != len(direct_currency_typed_keys)
        or len(direct_currency_observation_ids) != len(direct_currency_typed_keys)
        or not set(remaining_typed_keys) <= set(currency_typed_keys)
        or set(withdrawn_typed_keys) & set(remaining_typed_keys)
        or withdrawn_direct_observation_ids & remaining_direct_observation_ids
        or set(direct_currency_typed_keys)
        != set(withdrawn_typed_keys) | set(remaining_typed_keys)
    ):
        raise EventConflictError("D29 direct observation currency partition changed")
    semantic_typed_keys = currency_typed_keys
    locked_observations: dict[str, tuple[object, ...]] = {}
    preliminary_claim_observations = {
        claim.currency.observation_id: claim.observation_row
        for claim in locator.d30_claims.dynamic
    }
    preliminary_claim_observations.update(
        {
            claim.currency.observation_id: claim.observation_row
            for claim in locator.d30_claims.bootstrap
        }
    )
    remaining_preliminary_observations = {
        str(row[0]): row for row in locator.direct_remaining_observation_rows
    }
    if len(preliminary_claim_observations) != len(
        locator.d30_claims.currency_rows
    ) or any(
        preliminary_claim_observations.get(observation_id) != row
        for observation_id, row in remaining_preliminary_observations.items()
    ):
        raise EventConflictError("D29 preliminary direct observations are ambiguous")
    for (
        subject_kind,
        subject_id,
        chunk_id,
        task_type,
        observation_id,
    ) in semantic_typed_keys:
        row = cursor.execute(
            """
            SELECT observation_id, subject_kind::text, subject_id,
                   chunk_version_id, task_type, support_score, refute_score,
                   neutral_score, model_id, model_version, prompt_version,
                   input_hash, produced_epoch, raw_output_hash,
                   eligible_for_currency
            FROM groundloop_semantic_observation
            WHERE observation_id = %s
            FOR UPDATE
            """,
            (observation_id,),
        ).fetchone()
        if (
            row is None
            or str(row[0]) != observation_id
            or tuple(str(value) for value in row[1:5])
            != (subject_kind, subject_id, chunk_id, task_type)
            or observation_id in locked_observations
            or (
                subject_kind == "claim"
                and tuple(row) != preliminary_claim_observations.get(observation_id)
            )
        ):
            raise EventConflictError("observation locator disappeared")
        locked_observations[observation_id] = tuple(row)

    direct_sources_by_chunk: dict[str, tuple[str, str, str]] = {}
    for (
        observation_id,
        chunk_id,
        text,
        text_hash,
    ) in locator.direct_observation_source_rows:
        if (
            observation_id not in locked_observations
            or str(locked_observations[observation_id][3]) != chunk_id
        ):
            raise EventConflictError("D29 direct observation source locator changed")
        source_row = (chunk_id, text, text_hash)
        existing = direct_sources_by_chunk.get(chunk_id)
        if existing is not None and existing != source_row:
            raise EventConflictError("D29 direct observation source is ambiguous")
        direct_sources_by_chunk[chunk_id] = source_row
    for chunk_id in sorted(direct_sources_by_chunk, key=_c_key):
        row = cursor.execute(
            """
            SELECT chunk_version_id, text, text_hash
            FROM groundloop_chunk_version
            WHERE chunk_version_id = %s
            """,
            (chunk_id,),
        ).fetchone()
        if (
            row is None
            or (
                str(row[0]),
                str(row[1]),
                _strip(row[2]),
            )
            != direct_sources_by_chunk[chunk_id]
        ):
            raise EventConflictError(
                "D29 direct observation source changed before lock"
            )

    locked_deltas: dict[str, tuple[object, ...]] = {}
    for claim in sorted(
        locator.d30_claims.dynamic,
        key=lambda item: (
            item.owner_epoch_id,
            *(_c_key(value) for value in item.currency.full_key),
        ),
    ):
        row = cursor.execute(
            """
            SELECT epoch_id, subject_kind::text, subject_id,
                   chunk_version_id, task_type, base_observation_id,
                   working_observation_id, installed_revision
            FROM groundloop_working_observation_delta
            WHERE epoch_id = %s AND subject_kind = %s
              AND subject_id = %s AND chunk_version_id = %s
              AND task_type = %s
            FOR UPDATE
            """,
            (claim.owner_epoch_id, *claim.currency.full_key),
        ).fetchone()
        if (
            row is None
            or tuple(row) != claim.delta_row
            or claim.currency.observation_id in locked_deltas
        ):
            raise EventConflictError("D30 working-delta authority changed")
        locked_deltas[claim.currency.observation_id] = tuple(row)

    locked_current: dict[tuple[str, str, str, str], tuple[object, ...]] = {}
    for (
        subject_kind,
        subject_id,
        chunk_id,
        task_type,
        _observation_id,
    ) in currency_typed_keys:
        key = (subject_kind, subject_id, chunk_id, task_type)
        row = cursor.execute(
            """
            SELECT subject_kind::text, subject_id, chunk_version_id,
                   task_type, observation_id, installed_revision
            FROM groundloop_observation_currency
            WHERE subject_kind = %s AND subject_id = %s
              AND chunk_version_id = %s AND task_type = %s
            FOR UPDATE
            """,
            key,
        ).fetchone()
        if row is None or tuple(str(value) for value in row[:4]) != key:
            raise EventConflictError("current observation currency changed")
        locked_current[key] = tuple(row)

    dynamic_by_key = {
        claim.currency.full_key: claim for claim in locator.d30_claims.dynamic
    }
    locked_published: dict[tuple[str, str, str, str], tuple[object, ...]] = {}
    locked_predecessors: dict[str, tuple[object, ...] | None] = {}
    for (
        subject_kind,
        subject_id,
        chunk_id,
        task_type,
        _observation_id,
    ) in currency_typed_keys:
        key = (subject_kind, subject_id, chunk_id, task_type)
        row = cursor.execute(
            """
            SELECT subject_kind::text, subject_id, chunk_version_id, task_type,
                   observation_id, valid_from_epoch, valid_to_epoch
            FROM groundloop_published_observation_currency
            WHERE subject_kind = %s AND subject_id = %s
              AND chunk_version_id = %s AND task_type = %s
              AND valid_to_epoch IS NULL
            FOR UPDATE
            """,
            key,
        ).fetchone()
        if row is None or tuple(str(value) for value in row[:4]) != key:
            raise EventConflictError("open published observation currency changed")
        locked_published[key] = tuple(row)
        dynamic_claim = dynamic_by_key.get(key)
        if dynamic_claim is None:
            continue
        preliminary = dynamic_claim.predecessor_candidate
        if preliminary is not None:
            point = cursor.execute(
                """
                SELECT subject_kind::text, subject_id, chunk_version_id,
                       task_type, observation_id, valid_from_epoch,
                       valid_to_epoch
                FROM groundloop_published_observation_currency
                WHERE subject_kind = %s AND subject_id = %s
                  AND chunk_version_id = %s AND task_type = %s
                  AND valid_from_epoch = %s
                FOR UPDATE
                """,
                (*key, int(str(preliminary[5]))),
            ).fetchone()
            if point is None or tuple(point) != preliminary:
                raise EventConflictError("D30 predecessor currency point changed")
        rerun = _probe_d30_predecessor_currency(
            cursor,
            dynamic_claim.currency,
            dynamic_claim.predecessor_epoch_id,
        )
        if rerun != preliminary:
            raise EventConflictError("D30 predecessor currency probe changed")
        locked_predecessors[dynamic_claim.currency.observation_id] = rerun

    # Reread each already locked working-delta point only after every owner,
    # head, current-currency, and predecessor guard is held.  No new key is
    # discovered here; the exact preliminary/locked/rerun bytes must agree.
    for claim in sorted(
        locator.d30_claims.dynamic,
        key=lambda item: (
            item.owner_epoch_id,
            *(_c_key(value) for value in item.currency.full_key),
        ),
    ):
        rerun_delta = cursor.execute(
            """
            SELECT epoch_id, subject_kind::text, subject_id,
                   chunk_version_id, task_type, base_observation_id,
                   working_observation_id, installed_revision
            FROM groundloop_working_observation_delta
            WHERE epoch_id = %s AND subject_kind = %s
              AND subject_id = %s AND chunk_version_id = %s
              AND task_type = %s
            """,
            (claim.owner_epoch_id, *claim.currency.full_key),
        ).fetchone()
        if rerun_delta is None or tuple(rerun_delta) != locked_deltas.get(
            claim.currency.observation_id
        ):
            raise EventConflictError("D30 working-delta guarded rerun changed")

    requirement_edges: list[M5WithdrawnObservationEdge] = []
    for (
        requirement_id,
        chunk_id,
        task_type,
        observation_id,
    ) in locator.requirement_currency_keys:
        current = locked_current[("requirement", requirement_id, chunk_id, task_type)]
        published = locked_published[
            ("requirement", requirement_id, chunk_id, task_type)
        ]
        observation = locked_observations[observation_id]
        verifier = verifier_authority.get(observation_id)
        bootstrap = bootstrap_authority.get(observation_id)
        if (
            current is None
            or published is None
            or str(current[4]) != observation_id
            or type(current[5]) is not int
            or int(current[5]) < 0
            or str(published[4]) != observation_id
            or int(str(published[5])) > predecessor_epoch_id
            or published[6] is not None
            or observation[1] != "requirement"
            or str(observation[2]) != requirement_id
            or str(observation[3]) != chunk_id
            or str(observation[4]) != task_type
            or observation[14] is not True
            or (verifier is None) == (bootstrap is None)
        ):
            raise EventConflictError("requirement currency authority changed")
        try:
            checked_observation = SemanticObservation(
                observation_id=str(observation[0]),
                subject_kind=SubjectKind.REQUIREMENT,
                subject_id=str(observation[2]),
                chunk_version_id=str(observation[3]),
                task_type=str(observation[4]),
                support_score=float(str(observation[5])),
                refute_score=float(str(observation[6])),
                neutral_score=float(str(observation[7])),
                producer=ModelStamp(
                    str(observation[8]),
                    str(observation[9]),
                    str(observation[10]),
                ),
                input_hash=_exact_digest(
                    "requirement observation input", observation[11]
                ),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError(
                "requirement observation bytes are malformed"
            ) from error
        if verifier is not None:
            if (
                checked_observation != verifier.observation
                or verifier.candidate_policy_id != candidate_policy_id
                or int(str(observation[12])) != verifier.produced_epoch_id
                or observation[13] != verifier.raw_output_hash
                or bool(observation[14]) != verifier.eligible_for_currency
                or not _is_sealed_lineage_owner(
                    cursor, verifier.produced_epoch_id, predecessor_epoch_id
                )
            ):
                raise EventConflictError(
                    "requirement verifier currency provenance changed"
                )
        elif bootstrap is not None and (
            int(str(observation[12])) > bootstrap.base_epoch_id
            or int(str(published[5])) > bootstrap.base_epoch_id
        ):
            raise EventConflictError(
                "requirement bootstrap currency provenance changed"
            )
        requirement_edges.append(
            M5WithdrawnObservationEdge(
                observation_id,
                requirement_id,
                chunk_id,
                candidate_policy_id,
            )
        )

    direct_dependencies: list[ObservationDependency] = []
    bootstrap_claims = {
        claim.currency.observation_id: claim for claim in locator.d30_claims.bootstrap
    }
    owner_by_epoch = {owner.epoch_id: owner for owner in locator.d30_claims.owners}
    activation_base = (
        _validate_d30_activation_row(
            locator.d30_claims.activation_row,
            predecessor_epoch_id=predecessor_epoch_id,
        )
        if locator.d30_claims.currency_rows
        else 0
    )
    for currency in locator.d30_claims.currency_rows:
        claim_id = currency.subject_id
        chunk_id = currency.chunk_version_id
        task_type = currency.task_type
        observation_id = currency.observation_id
        current = locked_current[currency.full_key]
        published = locked_published[currency.full_key]
        observation = locked_observations[observation_id]
        dynamic_claim = d30_dynamic_authority.get(observation_id)
        bootstrap_claim = bootstrap_claims.get(observation_id)
        if (
            tuple(current)
            != (
                *currency.full_key,
                currency.observation_id,
                currency.installed_revision,
            )
            or str(published[4]) != observation_id
            or int(str(published[5])) > predecessor_epoch_id
            or published[6] is not None
            or observation[1] != "claim"
            or str(observation[2]) != claim_id
            or str(observation[3]) != chunk_id
            or str(observation[4]) != task_type
            or observation[14] is not True
            or (dynamic_claim is None) == (bootstrap_claim is None)
        ):
            raise EventConflictError("direct currency authority changed")
        if dynamic_claim is not None:
            owner = owner_by_epoch.get(dynamic_claim.owner_epoch_id)
            if (
                owner is None
                or tuple(observation) != dynamic_claim.observation_row
                or locked_deltas.get(observation_id) != dynamic_claim.delta_row
                or locked_predecessors.get(observation_id)
                != dynamic_claim.predecessor_candidate
                or int(str(published[5])) != dynamic_claim.owner_epoch_id
                or currency.installed_revision != dynamic_claim.owner_epoch_id
            ):
                raise EventConflictError("D30 dynamic currency provenance changed")
            _validate_d30_dynamic_claim(
                dynamic_claim,
                owner=owner,
                candidate_policy_id=candidate_policy_id,
            )
        elif bootstrap_claim is not None:
            if (
                currency.installed_revision != 0
                or tuple(observation) != bootstrap_claim.observation_row
                or int(str(published[5])) > activation_base
                or int(str(published[5])) > predecessor_epoch_id
            ):
                raise EventConflictError("D30 activation-base currency changed")
            _validate_d30_m3_bootstrap(
                bootstrap_claim,
                activation_base_epoch_id=activation_base,
                predecessor_epoch_id=predecessor_epoch_id,
            )
        if observation_id in withdrawn_direct_observation_ids:
            direct_dependencies.append(
                ObservationDependency(observation_id, PairKey(claim_id, chunk_id))
            )
    canonical_requirement_edges = tuple(
        sorted(
            set(requirement_edges),
            key=lambda edge: (
                _c_key(edge.observation_id),
                _c_key(edge.requirement_version_id),
                _c_key(edge.chunk_version_id),
                _c_key(edge.candidate_policy_id),
            ),
        )
    )
    canonical_direct_dependencies = tuple(
        sorted(
            set(direct_dependencies),
            key=lambda item: (
                _c_key(item.pair.claim_id),
                _c_key(item.pair.chunk_version_id),
                _c_key(item.observation_id),
            ),
        )
    )
    locked_withdrawn_ids = {
        item.observation_id for item in canonical_direct_dependencies
    }
    locked_remaining_ids = {
        str(row[0]) for row in locator.direct_remaining_observation_rows
    }
    if (
        locked_withdrawn_ids != withdrawn_direct_observation_ids
        or set(locked_observations) != {item[4] for item in currency_typed_keys}
        or (
            not retained_unpartitioned_direct_state
            and {
                observation_id
                for observation_id, _chunk_id, _text, _text_hash in (
                    locator.direct_observation_source_rows
                )
            }
            != locked_withdrawn_ids | locked_remaining_ids
        )
    ):
        raise EventConflictError("D29 locked direct observation partition changed")
    for direct_claim_image in locator.direct_claim_before_images:
        expected_remaining = (
            set(direct_claim_image.supporting_observation_ids)
            | set(direct_claim_image.refuting_observation_ids)
        ) - set(direct_claim_image.withdrawn_observation_ids)
        if (
            set(direct_claim_image.remaining_support_observation_ids)
            | set(direct_claim_image.remaining_refute_observation_ids)
            != expected_remaining
            or not set(direct_claim_image.withdrawn_observation_ids)
            <= locked_withdrawn_ids
            or not expected_remaining <= locked_remaining_ids
        ):
            raise EventConflictError("D29 direct claim partition changed before lock")
    return canonical_requirement_edges, canonical_direct_dependencies


def _locked_direct_withdrawal(
    *,
    source_chunks: tuple[str, ...],
    observations: tuple[ObservationDependency, ...],
    candidates: tuple[CandidateDependency, ...],
) -> StructuralWithdrawal:
    plan = plan_withdrawal(
        ReverseDependencyIndex.build(observations, candidates), source_chunks
    )
    fallback_claim_ids = tuple(sorted(set(plan.affected_claim_ids)))
    return StructuralWithdrawal(plan, fallback_claim_ids)


def _require_coordinate_subset(
    exact: _DocumentDeclarationCoordinates,
    prospective: _DocumentDeclarationCoordinates,
) -> None:
    fields = (
        "direct_job_ids",
        "direct_scope_root_job_ids",
        "requirement_job_ids",
        "requirement_scope_root_job_ids",
    )
    for field in fields:
        exact_values = set(getattr(exact, field))
        prospective_values = set(getattr(prospective, field))
        if not exact_values <= prospective_values:
            raise EventConflictError(
                "exact D29 declaration escaped prospective reservations"
            )


def _bounded_requirement_plan(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    *,
    candidate_policy_id: str,
    predecessor_epoch_id: int,
) -> M5RequirementWithdrawalPlan:
    """Build the nonauthoritative, compare-only absent-event preview."""

    checked = _document_event(event)
    direct = checked.direct_plan
    assert direct is not None
    chunks = _canonical_texts(
        "deactivated chunk IDs", direct.deactivated_chunk_version_ids
    )
    snapshots = _predecessor_snapshot_ids(cursor, predecessor_epoch_id)
    for chunk_id in chunks:
        if not _require_active_membership(
            cursor,
            predecessor_epoch_id=predecessor_epoch_id,
            snapshots=snapshots,
            chunk_version_id=chunk_id,
        ):
            raise EventConflictError("deactivated chunk was not active at predecessor")
    candidates = _candidate_edges(
        cursor,
        chunks=chunks,
        predecessor_epoch_id=predecessor_epoch_id,
        snapshots=snapshots,
        candidate_policy_id=candidate_policy_id,
        lock_authority=False,
    )
    observations = _observation_edges(
        cursor,
        chunks=chunks,
        predecessor_epoch_id=predecessor_epoch_id,
        snapshots=snapshots,
        candidate_policy_id=candidate_policy_id,
        lock_authority=False,
    )
    _assert_zero_cancellation_authority(cursor, predecessor_epoch_id)
    active_requirement_ids = {edge.requirement_version_id for edge in candidates} | {
        edge.requirement_version_id for edge in observations
    }
    active_requirements = tuple(
        sorted(
            active_requirement_ids,
            key=_c_key,
        )
    )
    return plan_requirement_withdrawal(
        event_id=checked.structural_event_id,
        deactivated_chunk_version_ids=chunks,
        candidate_edges=candidates,
        observation_edges=observations,
        cancelled_job_ids=(),
        active_requirement_version_ids=active_requirements,
    )


def _validate_m5_job_state(
    spec: M5LogicalJobSpec,
    row: tuple[object, ...],
    child_job_ids: tuple[str, ...],
) -> None:
    """Validate all mutable closure columns for one retained M5 declaration."""

    try:
        state = M5JobState(str(row[19]))
    except ValueError as error:
        raise EventConflictError("retained M5 job state is invalid") from error
    if row[31] is None:
        raise EventConflictError("retained M5 job creation time is absent")
    result_id = None if row[20] is None else _strip(row[20])
    result_hash = None if row[21] is None else _strip(row[21])
    scope_closure = None if row[22] is None else _strip(row[22])
    child_set = None if row[23] is None else _strip(row[23])
    try:
        reason = None if row[24] is None else M5TerminalReason(str(row[24]))
    except ValueError as error:
        raise EventConflictError("retained M5 terminal reason is invalid") from error
    completion_digest = None if row[25] is None else _strip(row[25])
    cancelled_event = None if row[26] is None else str(row[26])
    cancelled_epoch = None if row[27] is None else int(str(row[27]))
    cancellation_reason = None if row[28] is None else str(row[28])
    completed_revision = None if row[30] is None else int(str(row[30]))
    completed_at = row[32]
    if not state.terminal:
        if any(
            value is not None
            for value in (
                result_id,
                result_hash,
                scope_closure,
                child_set,
                reason,
                completion_digest,
                cancelled_event,
                cancelled_epoch,
                cancellation_reason,
                completed_revision,
                completed_at,
            )
        ):
            raise EventConflictError("retained nonterminal M5 job has terminal closure")
        return
    if (
        completed_revision is None
        or completed_revision < int(str(row[29]))
        or completed_at is None
        or completion_digest is None
    ):
        raise EventConflictError("retained terminal M5 job lacks its cutoff")
    if state is M5JobState.CANCELLED:
        if (
            cancelled_event is None
            or not cancelled_event.strip()
            or cancelled_epoch is None
            or reason is None
            or cancellation_reason != reason.value
        ):
            raise EventConflictError("retained M5 cancellation binding is invalid")
    elif any(
        value is not None
        for value in (cancelled_event, cancelled_epoch, cancellation_reason)
    ):
        raise EventConflictError(
            "retained non-cancelled M5 job has cancellation authority"
        )
    if state in {M5JobState.COMPLETED_ACTIVE, M5JobState.COMPLETED_INACTIVE}:
        expected_child_set = (
            digests.child_set_digest(child_job_ids) if spec.expandable else None
        )
        if child_set != expected_child_set:
            raise EventConflictError("retained M5 child-set closure is invalid")
        if state is M5JobState.COMPLETED_INACTIVE and child_job_ids:
            raise EventConflictError(
                "retained inactive M5 root unexpectedly has children"
            )
    try:
        completion = M5JobCompletion(
            logical_job_id=spec.logical_job_id,
            payload_hash=spec.payload_hash,
            execution_spec_hash=spec.execution_spec_hash,
            terminal_state=state,
            result_artifact_id=result_id,
            result_artifact_hash=result_hash,
            scope_closure_digest=scope_closure,
            child_set_hash=child_set,
            archive_reason=reason,
            completion_digest=completion_digest,
        )
        completion.validate_job(spec)
    except ValidationError as error:
        raise EventConflictError("retained M5 completion is invalid") from error


def _validate_m5_scope_state(
    root_job: M5LogicalJobSpec,
    job_row: tuple[object, ...],
    scope_row: tuple[object, ...],
) -> None:
    try:
        job_state = M5JobState(str(job_row[19]))
        scope_state = M5ScopeState(str(scope_row[9]))
    except ValueError as error:
        raise EventConflictError("retained M5 root/scope state is invalid") from error
    if scope_row[17] is None:
        raise EventConflictError("retained M5 scope creation time is absent")
    staged_hash = None if scope_row[11] is None else _strip(scope_row[11])
    scope_closure = None if scope_row[12] is None else _strip(scope_row[12])
    child_set = None if scope_row[13] is None else _strip(scope_row[13])
    completion = None if scope_row[14] is None else _strip(scope_row[14])
    staged_revision = None if scope_row[15] is None else int(str(scope_row[15]))
    closed_revision = None if scope_row[16] is None else int(str(scope_row[16]))
    closed_at = scope_row[18]
    created_revision = int(str(scope_row[10]))
    if (staged_hash is None) != (staged_revision is None):
        raise EventConflictError("retained M5 staged scope closure is partial")
    if staged_revision is not None and staged_revision < created_revision:
        raise EventConflictError("retained M5 staged scope revision is invalid")
    if scope_state is M5ScopeState.OPEN:
        if (
            any(
                value is not None
                for value in (
                    staged_hash,
                    scope_closure,
                    child_set,
                    completion,
                    staged_revision,
                    closed_revision,
                    closed_at,
                )
            )
            or job_state.terminal
        ):
            raise EventConflictError("retained open M5 scope closure is invalid")
        return
    if scope_state is M5ScopeState.RESULT_STAGED:
        if (
            staged_hash is None
            or staged_revision is None
            or any(
                value is not None
                for value in (
                    scope_closure,
                    child_set,
                    completion,
                    closed_revision,
                    closed_at,
                )
            )
            or job_state.terminal
        ):
            raise EventConflictError("retained staged M5 scope closure is invalid")
        return
    expected_scope_state = {
        M5JobState.COMPLETED_ACTIVE: M5ScopeState.CLOSED_ACTIVE,
        M5JobState.COMPLETED_INACTIVE: M5ScopeState.CLOSED_INACTIVE,
        M5JobState.TERMINAL_FAILED: M5ScopeState.TERMINAL_FAILED,
        M5JobState.CANCELLED: M5ScopeState.CANCELLED,
    }.get(job_state)
    if (
        expected_scope_state is not scope_state
        or completion is None
        or completion != (None if job_row[25] is None else _strip(job_row[25]))
        or closed_revision is None
        or closed_revision != int(str(job_row[30]))
        or closed_at is None
    ):
        raise EventConflictError("retained M5 scope terminal binding is invalid")
    if scope_state in {M5ScopeState.CLOSED_ACTIVE, M5ScopeState.CLOSED_INACTIVE}:
        if (
            staged_hash is None
            or staged_hash != _strip(job_row[21])
            or scope_closure != _strip(job_row[22])
            or child_set != _strip(job_row[23])
        ):
            raise EventConflictError(
                "retained M5 scope closure differs from root completion"
            )
    elif scope_closure is not None or child_set is not None:
        raise EventConflictError("retained failed M5 scope has child closure")
    if root_job.parent_job_id is not None:
        raise EventConflictError("retained child cannot own an M5 scope")


def _stored_requirement_declarations(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    closure: _DocumentClosure,
) -> tuple[M5RequirementWithdrawalPlan, tuple[M5RequirementRootDeclaration, ...]]:
    rows = cursor.execute(
        """
        WITH hydrated AS MATERIALIZED (
            SELECT logical_job_id, epoch_id, structural_event_id, job_kind,
                   candidate_policy_id, candidate_policy_manifest_hash,
                   parent_job_id, subject_kind::text, subject_id,
                   chunk_version_id, semantic_pair_digest,
                   admitted_pair_digest, scope_contract_digest,
                   requirement_registry_snapshot_digest,
                   active_chunk_snapshot_digest, role_template_hash,
                   execution_spec_hash, expandable, payload_hash, job_state,
                   result_artifact_id, result_artifact_hash,
                   scope_closure_digest, child_set_hash, archive_reason,
                   completion_digest, cancelled_by_event_id,
                   cancelled_by_epoch_id, cancellation_reason,
                   created_revision, completed_revision, created_at, completed_at
            FROM groundloop_m5_semantic_job
            WHERE epoch_id = %s
            ORDER BY job_state, logical_job_id
        )
        SELECT *
        FROM hydrated
        ORDER BY logical_job_id COLLATE "C"
        """,
        (closure.epoch_id,),
    ).fetchall()
    ordered = sorted(rows, key=lambda row: _c_key(_strip(row[0])))
    ids = tuple(_strip(row[0]) for row in ordered)
    if len(ids) != len(set(ids)):
        raise EventConflictError("retained M5 declaration repeats a job")
    specs: dict[str, M5LogicalJobSpec] = {}
    state_rows: dict[str, tuple[object, ...]] = {}
    for raw in ordered:
        row = tuple(raw)
        job_id = _strip(row[0])
        try:
            kind = M5JobKind(str(row[3]))
        except ValueError as error:
            raise EventConflictError("retained M5 job kind is invalid") from error
        pair = None
        if kind is M5JobKind.VERIFY_REQUIREMENT_PAIR:
            if (
                row[7] != "requirement"
                or row[8] is None
                or row[9] is None
                or row[11] is None
            ):
                raise EventConflictError("retained M5 child lacks its pair")
            try:
                _exact_digest("retained admitted-pair digest", _strip(row[11]))
            except ValidationError as error:
                raise EventConflictError(
                    "retained M5 child admitted-pair identity is invalid"
                ) from error
            pair = SemanticPairKey(SubjectKind.REQUIREMENT, str(row[8]), str(row[9]))
        elif any(row[index] is not None for index in (6, 7, 8, 9, 10, 11)):
            raise EventConflictError("retained M5 root has child-only fields")
        try:
            spec = M5LogicalJobSpec(
                logical_job_id=job_id,
                structural_event_id=str(row[2]),
                job_kind=kind,
                candidate_policy_id=_strip(row[4]),
                candidate_policy_manifest_hash=_strip(row[5]),
                parent_job_id=None if row[6] is None else _strip(row[6]),
                pair=pair,
                semantic_pair_digest=(None if row[10] is None else _strip(row[10])),
                scope_contract_digest=_strip(row[12]),
                requirement_registry_snapshot_digest=_strip(row[13]),
                active_chunk_snapshot_digest=_strip(row[14]),
                role_template_hash=_strip(row[15]),
                execution_spec_hash=_strip(row[16]),
                expandable=bool(row[17]),
                payload_hash=_strip(row[18]),
            )
        except ValidationError as error:
            raise EventConflictError("retained M5 job identity is invalid") from error
        if (
            int(row[1]) != closure.epoch_id
            or spec.structural_event_id != event.structural_event_id
            or spec.candidate_policy_id != closure.candidate_policy.candidate_policy_id
            or spec.candidate_policy_manifest_hash
            != closure.candidate_policy.manifest_hash
            or spec.requirement_registry_snapshot_digest
            != closure.requirement_snapshot_digest
            or spec.active_chunk_snapshot_digest != closure.chunk_snapshot_digest
            or str(row[19]) not in _ALL_JOB_STATES
            or type(row[29]) is not int
            or int(row[29]) < 1
        ):
            raise EventConflictError("retained M5 job declaration changed")
        specs[job_id] = spec
        state_rows[job_id] = row
    children_by_parent: dict[str, list[str]] = {}
    for spec in specs.values():
        if spec.parent_job_id is not None:
            children_by_parent.setdefault(spec.parent_job_id, []).append(
                spec.logical_job_id
            )
    for job_id in ids:
        children = tuple(sorted(children_by_parent.get(job_id, ()), key=_c_key))
        _validate_m5_job_state(specs[job_id], state_rows[job_id], children)
    actual_open_work_count = sum(
        str(row[19]) in _NONTERMINAL_JOB_STATES for row in state_rows.values()
    )
    actual_blocking_failure_count = sum(
        str(row[19]) == M5JobState.TERMINAL_FAILED.value for row in state_rows.values()
    )
    roots: list[M5RequirementRootDeclaration] = []
    forward_keys: list[M5RequirementFallbackKey] = []
    reverse_chunks: list[str] = []
    actual_open_scope_count = 0
    for job_id in ids:
        spec = specs[job_id]
        scope_rows = cursor.execute(
            """
            SELECT root_job_id, epoch_id, direction,
                   requirement_version_id, inserted_chunk_version_id,
                   candidate_policy_id,
                   requirement_registry_snapshot_digest,
                   active_chunk_snapshot_digest, scope_contract_digest,
                   scope_state, created_revision,
                   staged_result_artifact_hash, scope_closure_digest,
                   child_set_hash, completion_digest, staged_revision,
                   closed_revision, created_at, closed_at
            FROM groundloop_m5_discovery_scope
            WHERE root_job_id = %s
            """,
            (job_id,),
        ).fetchall()
        provenance_rows = cursor.execute(
            """
            SELECT fallback_required, provenance_digest
            FROM groundloop_m5_requirement_root_provenance
            WHERE epoch_id = %s AND root_job_id = %s
            """,
            (closure.epoch_id, job_id),
        ).fetchall()
        if spec.parent_job_id is not None:
            if scope_rows or provenance_rows or spec.parent_job_id not in specs:
                raise EventConflictError("retained M5 child closure is inconsistent")
            parent = specs[spec.parent_job_id]
            if (
                parent.parent_job_id is not None
                or parent.scope_contract_digest != spec.scope_contract_digest
            ):
                raise EventConflictError("retained M5 child has another root scope")
            continue
        if len(scope_rows) != 1:
            raise EventConflictError("retained M5 root lacks exactly one scope")
        scope_row = scope_rows[0]
        try:
            scope = M5DiscoveryScopeContract(
                direction=M5DiscoveryDirection(str(scope_row[2])),
                requirement_version_id=(
                    None if scope_row[3] is None else str(scope_row[3])
                ),
                inserted_chunk_version_id=(
                    None if scope_row[4] is None else str(scope_row[4])
                ),
                candidate_policy_id=_strip(scope_row[5]),
                requirement_registry_snapshot_digest=_strip(scope_row[6]),
                active_chunk_snapshot_digest=_strip(scope_row[7]),
                scope_contract_digest=_strip(scope_row[8]),
            )
        except (ValueError, ValidationError) as error:
            raise EventConflictError("retained M5 scope identity is invalid") from error
        try:
            spec.validate_manifest_and_scope(closure.candidate_policy, scope)
            for child_id in children_by_parent.get(job_id, ()):
                specs[child_id].validate_manifest_and_scope(
                    closure.candidate_policy, scope
                )
        except ValidationError as error:
            raise EventConflictError(
                "retained M5 job manifest/scope binding changed"
            ) from error
        if (
            _strip(scope_row[0]) != job_id
            or int(scope_row[1]) != closure.epoch_id
            or scope.scope_contract_digest != spec.scope_contract_digest
            or str(scope_row[9])
            not in {
                "open",
                "result_staged",
                "closed_active",
                "closed_inactive",
                "terminal_failed",
                "cancelled",
            }
            or type(scope_row[10]) is not int
            or int(scope_row[10]) < 1
        ):
            raise EventConflictError("retained M5 scope declaration changed")
        _validate_m5_scope_state(spec, state_rows[job_id], tuple(scope_row))
        if str(scope_row[9]) in {"open", "result_staged"}:
            actual_open_scope_count += 1
        if spec.job_kind is M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL:
            if len(provenance_rows) != 1 or provenance_rows[0][0] is not True:
                raise EventConflictError("document forward root lacks provenance")
            expected_provenance = digests.requirement_root_provenance_digest(
                epoch_id=closure.epoch_id,
                root_job_id=job_id,
                fallback_required=True,
            )
            if _strip(provenance_rows[0][1]) != expected_provenance:
                raise EventConflictError("document forward provenance is invalid")
            assert scope.requirement_version_id is not None
            forward_keys.append(
                M5RequirementFallbackKey(
                    scope.requirement_version_id, spec.candidate_policy_id
                )
            )
        else:
            if provenance_rows:
                raise EventConflictError("reverse root has fallback provenance")
            assert scope.inserted_chunk_version_id is not None
            reverse_chunks.append(scope.inserted_chunk_version_id)
        roots.append(M5RequirementRootDeclaration(scope=scope, job=spec))
    if (
        actual_open_work_count != closure.open_work_count
        or actual_open_scope_count != closure.open_scope_count
        or actual_blocking_failure_count != closure.blocking_failure_count
    ):
        raise EventConflictError(
            "retained M5 runtime counters differ from hydrated jobs/scopes"
        )
    root_tuple = tuple(sorted(roots, key=lambda item: _c_key(item.job.logical_job_id)))
    if (
        digests.requirement_root_set_digest(
            item.job.logical_job_id for item in root_tuple
        )
        != closure.requirement_root_set_hash
    ):
        raise EventConflictError("retained M5 root-set hash is invalid")
    direct = event.direct_plan
    assert direct is not None
    expected_reverse = tuple(sorted(direct.inserted_chunk_version_ids, key=_c_key))
    if tuple(sorted(reverse_chunks, key=_c_key)) != expected_reverse:
        raise EventConflictError("retained reverse roots differ from inserted chunks")
    fallback = tuple(sorted(set(forward_keys)))
    deactivated = tuple(sorted(direct.deactivated_chunk_version_ids, key=_c_key))
    plan = M5RequirementWithdrawalPlan(
        event_id=event.structural_event_id,
        deactivated_chunk_version_ids=deactivated,
        withdrawn_candidate_pair_digests=(),
        withdrawn_observation_ids=(),
        cancelled_job_ids=(),
        fallback_keys=fallback,
        plan_digest=digests.requirement_withdrawal_plan_digest(
            event_id=event.structural_event_id,
            deactivated_chunk_version_ids=deactivated,
            withdrawn_candidate_pair_digests=(),
            withdrawn_observation_ids=(),
            cancelled_job_ids=(),
            fallback_keys=(
                (key.requirement_version_id, key.candidate_policy_id)
                for key in fallback
            ),
        ),
    )
    return plan, root_tuple


def _hydrate_retained_requirement_open(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    *,
    expected_epoch_id: int | None = None,
) -> tuple[M5RequirementWithdrawalPlan, tuple[M5RequirementRootDeclaration, ...]]:
    _require_d29_document_route_authority(cursor)
    closure = _read_existing_document_closure(cursor, event)
    if closure is None:
        raise EventConflictError("retained D29 document declaration is absent")
    if expected_epoch_id is not None and closure.epoch_id != expected_epoch_id:
        raise EventConflictError("retained D29 document epoch changed")
    _validated_structural_source_chunks(
        cursor, event, closure, lock_authority=False, event_local_only=True
    )
    plan, roots = _stored_requirement_declarations(cursor, event, closure)
    _validate_composed_requirement_roots(
        event,
        closure.candidate_policy,
        plan,
        roots,
        closure.requirement_root_set_hash,
    )
    _raise_if_terminal(cursor, event, closure)
    return plan, roots


def _plan_document_requirement_withdrawal(
    cursor: Cursor[Any], event: M5TypedEventPlan
) -> M5RequirementWithdrawalPlan:
    """Public-call implementation: absent preview or retained hydration."""

    _require_d29_document_route_authority(cursor)
    checked = _document_event(event)
    closure = _read_existing_document_closure(cursor, checked)
    if closure is not None:
        _validated_structural_source_chunks(
            cursor,
            checked,
            closure,
            lock_authority=False,
            event_local_only=True,
        )
        plan, roots = _stored_requirement_declarations(cursor, checked, closure)
        _validate_composed_requirement_roots(
            checked,
            closure.candidate_policy,
            plan,
            roots,
            closure.requirement_root_set_hash,
        )
        _raise_if_terminal(cursor, checked, closure)
        return plan
    policy = _policy_by_id(cursor, checked.candidate_policy_id)
    if policy.manifest_hash != checked.candidate_policy_manifest_hash:
        raise EventConflictError("document preview policy binding changed")
    return _bounded_requirement_plan(
        cursor,
        checked,
        candidate_policy_id=policy.candidate_policy_id,
        predecessor_epoch_id=checked.expected_previous_published_epoch_id,
    )


def _validate_m4_job_state(
    spec: M4LogicalJobSpec,
    row: tuple[object, ...],
    child_job_ids: tuple[str, ...],
) -> None:
    """Validate the durable M4 completion shape without importing a store facade."""

    try:
        state = M4JobState(str(row[10]))
    except ValueError as error:
        raise EventConflictError("retained direct job state is invalid") from error
    if row[16] is None or type(row[16]) is not int or int(row[16]) < 0:
        raise EventConflictError("retained direct job creation revision is invalid")
    if row[18] is None:
        raise EventConflictError("retained direct job creation time is absent")
    child_closed = row[11] is True
    child_set_hash = None if row[12] is None else _strip(row[12])
    completion_digest = None if row[13] is None else _strip(row[13])
    result_artifact_id = None if row[14] is None else str(row[14])
    result_artifact_hash = None if row[15] is None else _strip(row[15])
    completed_revision = None if row[17] is None else int(str(row[17]))
    completed_at = row[19]
    if not state.terminal:
        if (
            child_closed
            or child_set_hash is not None
            or completion_digest is not None
            or result_artifact_id is not None
            or result_artifact_hash is not None
            or completed_revision is not None
            or completed_at is not None
            or child_job_ids
        ):
            raise EventConflictError(
                "retained nonterminal direct job has terminal closure"
            )
        return
    if (
        completed_revision is None
        or completed_revision < int(row[16])
        or completed_at is None
    ):
        raise EventConflictError("retained terminal direct job lacks its cutoff")
    if state in {M4JobState.TERMINAL_FAILED, M4JobState.CANCELLED}:
        if (
            child_closed
            or child_set_hash is not None
            or completion_digest is not None
            or result_artifact_id is not None
            or result_artifact_hash is not None
            or child_job_ids
        ):
            raise EventConflictError(
                "retained failed direct job has successful completion fields"
            )
        return
    if (
        completion_digest is None
        or result_artifact_id is None
        or result_artifact_hash is None
    ):
        raise EventConflictError("retained completed direct job lacks its result")
    closure: M4ChildClosure | None = None
    if spec.expandable:
        if not child_closed or child_set_hash is None:
            raise EventConflictError(
                "retained expandable direct completion lacks child closure"
            )
        try:
            closure = M4ChildClosure(
                parent_job_id=spec.job_id,
                completion_digest=stable_m4_digest(
                    "m4-expandable-completion-v1",
                    spec.job_id,
                    result_artifact_hash,
                    child_set_hash,
                ),
                child_job_ids=child_job_ids,
                child_set_hash=child_set_hash,
            )
        except ValidationError as error:
            raise EventConflictError(
                "retained direct child-set closure is invalid"
            ) from error
        if state is M4JobState.COMPLETED_INACTIVE and child_job_ids:
            raise EventConflictError(
                "retained inactive direct root unexpectedly has children"
            )
    elif child_closed or child_set_hash is not None or child_job_ids:
        raise EventConflictError("retained direct child has a child closure")
    try:
        M4JobCompletion(
            job_id=spec.job_id,
            payload_hash=spec.payload_hash,
            execution_spec_hash=spec.execution_spec_hash,
            result_artifact_id=result_artifact_id,
            result_artifact_hash=result_artifact_hash,
            terminal_state=state,
            completion_digest=completion_digest,
            child_closure=closure,
        )
    except ValidationError as error:
        raise EventConflictError(
            "retained direct completion digest is invalid"
        ) from error


def _validate_retained_direct_verifier_closure(
    cursor: Cursor[Any],
    spec: M4LogicalJobSpec,
    row: tuple[object, ...],
) -> None:
    """Validate one completed verifier's immutable result/provenance closure."""

    state = M4JobState(str(row[10]))
    completed = state in {
        M4JobState.COMPLETED_ACTIVE,
        M4JobState.COMPLETED_INACTIVE,
    }
    if not completed:
        return
    execution_rows = cursor.execute(
        """
        SELECT observation_id, job_id, btrim(admitted_pair_id),
               model_artifact_id, prompt_artifact_id,
               btrim(execution_spec_hash), btrim(pair_input_hash),
               calibration_version, btrim(calibration_artifact_sha256),
               temperature, raw_logits, btrim(raw_output_hash),
               reused_from_observation_id
        FROM groundloop_m4_verification_execution
        WHERE job_id = %s
        ORDER BY observation_id COLLATE "C"
        """,
        (spec.job_id,),
    ).fetchall()
    if len(execution_rows) != 1 or spec.pair is None:
        raise EventConflictError(
            "completed retained verifier lacks one execution provenance row"
        )
    execution_row = execution_rows[0]
    admitted_row = cursor.execute(
        """
        SELECT admitted_pair_id, epoch_id, claim_id, chunk_version_id,
               candidate_policy_id, fused_rank, reasons, mandatory_lineage
        FROM groundloop_admitted_pair
        WHERE admitted_pair_id = %s
        """,
        (_strip(execution_row[2]),),
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
        (spec.pair.claim_id, spec.pair.chunk_version_id),
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
        (str(execution_row[0]),),
    ).fetchone()
    if admitted_row is None or input_row is None or observation_row is None:
        raise EventConflictError(
            "completed retained verifier result closure is incomplete"
        )
    citations = tuple(
        str(item[0])
        for item in cursor.execute(
            """
            SELECT chunk_version_id
            FROM groundloop_answer_citation
            WHERE answer_version_id = %s
            ORDER BY citation_ordinal
            """,
            (str(input_row[2]),),
        ).fetchall()
    )
    model = cursor.execute(
        """
        SELECT model_artifact_id, task, model_id, immutable_revision
        FROM groundloop_model_artifact WHERE model_artifact_id = %s
        """,
        (str(execution_row[3]),),
    ).fetchone()
    prompt = cursor.execute(
        """
        SELECT prompt_artifact_id, task, version
        FROM groundloop_prompt_artifact WHERE prompt_artifact_id = %s
        """,
        (str(execution_row[4]),),
    ).fetchone()
    direct_policy = _load_direct_candidate_policy(cursor, spec.candidate_policy_id)
    decision_row = cursor.execute(
        """
        SELECT policy_version, support_threshold, refute_threshold,
               tie_rule_version
        FROM groundloop_decision_policy
        WHERE policy_version = %s
        """,
        (direct_policy.decision_policy_version,),
    ).fetchone()
    try:
        pair_input = PairVerificationInput(
            pair=spec.pair,
            claim_text=str(input_row[0]),
            claim_required=bool(input_row[1]),
            claim_cited_chunk_version_ids=citations,
            document_version_id=str(input_row[3]),
            chunk_index=int(input_row[4]),
            chunk_text=str(input_row[5]),
            chunk_text_hash=_strip(input_row[6]),
            chunker_artifact_id=str(input_row[7]),
        )
        raw_logits = tuple(float(value) for value in execution_row[10])
        if len(raw_logits) != 3:
            raise ValidationError("retained direct verifier logits changed")
        execution = M5TypedDirectVerificationExecution(
            observation_id=str(execution_row[0]),
            job_id=str(execution_row[1]),
            admitted_pair_id=_strip(execution_row[2]),
            model_artifact_id=str(execution_row[3]),
            prompt_artifact_id=str(execution_row[4]),
            execution_spec_hash=_strip(execution_row[5]),
            pair_input_hash=_strip(execution_row[6]),
            calibration_version=str(execution_row[7]),
            calibration_artifact_sha256=_strip(execution_row[8]),
            temperature=float(execution_row[9]),
            raw_logits=(raw_logits[0], raw_logits[1], raw_logits[2]),
            raw_output_hash=_strip(execution_row[11]),
            reused_from_observation_id=(
                None if execution_row[12] is None else str(execution_row[12])
            ),
        )
        admitted = AdmittedPair(
            epoch_id=int(admitted_row[1]),
            pair=PairKey(str(admitted_row[2]), str(admitted_row[3])),
            candidate_policy_id=str(admitted_row[4]),
            fused_rank=int(admitted_row[5]),
            reasons=tuple(AdmissionChannel(str(reason)) for reason in admitted_row[6]),
            mandatory_lineage=bool(admitted_row[7]),
        )
        if decision_row is None:
            raise ValidationError("retained direct decision policy disappeared")
        decision_policy = DecisionPolicy(
            str(decision_row[0]),
            float(decision_row[1]),
            float(decision_row[2]),
            str(decision_row[3]),
        )
        observation = SemanticObservation(
            observation_id=str(observation_row[0]),
            subject_kind=SubjectKind(str(observation_row[1])),
            subject_id=str(observation_row[2]),
            chunk_version_id=str(observation_row[3]),
            task_type=str(observation_row[4]),
            support_score=float(observation_row[5]),
            refute_score=float(observation_row[6]),
            neutral_score=float(observation_row[7]),
            producer=ModelStamp(
                str(observation_row[8]),
                str(observation_row[9]),
                str(observation_row[10]),
            ),
            input_hash=_strip(observation_row[11]),
        )
        result = AIVerificationResult(
            claim_id=spec.pair.claim_id,
            chunk_version_id=spec.pair.chunk_version_id,
            candidate_id=stable_ai_digest(
                "verification-candidate-v1",
                spec.pair.claim_id,
                spec.pair.chunk_version_id,
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
            artifact_id=str(row[14]),
            pair=spec.pair,
            pair_input_hash=pair_input.input_hash,
            execution_spec_hash=execution.execution_spec_hash,
            decision_policy_version=decision_policy.policy_version,
            decision_policy_hash=decision_policy_hash(decision_policy),
            operational_label=derive_operational_label(result, decision_policy),
            result=result,
        )
        expected_observation = artifact.to_semantic_observation(
            model_id=str(model[2]) if model is not None else "",
            model_revision=str(model[3]) if model is not None else "",
            prompt_version=str(prompt[2]) if prompt is not None else "",
        )
    except (TypeError, ValueError, ValidationError) as error:
        raise EventConflictError(
            "completed retained verifier result closure is malformed"
        ) from error
    expected_admitted_id = _direct_admitted_pair_id(admitted)
    if (
        _strip(admitted_row[0]) != expected_admitted_id
        or execution.admitted_pair_id != expected_admitted_id
        or execution.job_id != spec.job_id
        or execution.execution_spec_hash != spec.execution_spec_hash
        or execution.execution_spec_hash != direct_policy.verifier_execution_spec_hash
        or execution.pair_input_hash != pair_input.input_hash
        or admitted.epoch_id != int(str(row[1]))
        or admitted.pair != spec.pair
        or admitted.candidate_policy_id != spec.candidate_policy_id
        or decision_policy.policy_version != direct_policy.decision_policy_version
        or model is None
        or tuple(model[:2]) != (execution.model_artifact_id, "verification")
        or prompt is None
        or tuple(prompt[:2]) != (execution.prompt_artifact_id, "verification")
        or observation.task_type != "verify"
        or observation != expected_observation
        or int(str(observation_row[12])) != int(str(row[1]))
        or _strip(observation_row[13]) != execution.raw_output_hash
        or type(observation_row[14]) is not bool
        or artifact.artifact_id != str(row[14])
        or verification_artifact_payload_hash(artifact) != _strip(row[15])
    ):
        raise EventConflictError(
            "completed retained verifier result/provenance identity changed"
        )


def _stored_direct_open(
    cursor: Cursor[Any], event: M5TypedEventPlan, closure: _DocumentClosure
) -> M5DirectOpenPlan:
    direct = event.direct_plan
    assert direct is not None
    rows = cursor.execute(
        """
        SELECT job_id, epoch_id, parent_job_id, job_kind,
               candidate_policy_id, payload_hash, execution_spec_hash,
               claim_id, chunk_version_id, expandable, job_state,
               child_closed, child_set_hash, completion_digest,
               result_artifact_id, result_artifact_hash,
               created_revision, completed_revision, created_at, completed_at
        FROM groundloop_semantic_job
        WHERE epoch_id = %s AND job_state = ANY(%s)
        ORDER BY job_id COLLATE "C"
        """,
        (closure.epoch_id, list(_ALL_JOB_STATES)),
    ).fetchall()
    ordered = sorted(rows, key=lambda row: _c_key(str(row[0])))
    job_ids = tuple(str(row[0]) for row in ordered)
    if len(job_ids) != len(set(job_ids)):
        raise EventConflictError("retained direct declaration repeats a job")
    specs: dict[str, M4LogicalJobSpec] = {}
    state_rows: dict[str, tuple[object, ...]] = {}
    for raw in ordered:
        row = tuple(raw)
        job_id = str(row[0])
        try:
            kind = M4JobKind(str(row[3]))
        except ValueError as error:
            raise EventConflictError("retained direct job kind is invalid") from error
        parent_id = None if row[2] is None else str(row[2])
        claim_id = None if row[7] is None else str(row[7])
        chunk_id = None if row[8] is None else str(row[8])
        pair = (
            PairKey(claim_id, chunk_id)
            if kind is M4JobKind.VERIFY_PAIR
            and claim_id is not None
            and chunk_id is not None
            else None
        )
        try:
            spec = M4LogicalJobSpec(
                job_id=job_id,
                event_id=event.structural_event_id,
                kind=kind,
                candidate_policy_id=str(row[4]),
                payload_hash=_strip(row[5]),
                execution_spec_hash=_strip(row[6]),
                parent_job_id=parent_id,
                pair=pair,
                target_claim_id=(
                    claim_id if kind is M4JobKind.FRONTIER_RETRIEVE else None
                ),
                target_chunk_version_id=(
                    chunk_id if kind is M4JobKind.IMPACT_DISCOVERY else None
                ),
                expandable=bool(row[9]),
            )
        except ValidationError as error:
            raise EventConflictError(
                "retained direct job identity is invalid"
            ) from error
        expected_payload = stable_m4_digest(
            "m4-application-job-payload-v1",
            event.payload_hash,
            kind.value,
            parent_id or "",
            claim_id or "",
            chunk_id or "",
        )
        if (
            int(row[1]) != closure.epoch_id
            or spec.candidate_policy_id != closure.candidate_policy.candidate_policy_id
            or spec.payload_hash != expected_payload
            or str(row[10]) not in _ALL_JOB_STATES
            or type(row[16]) is not int
            or int(row[16]) < 0
        ):
            raise EventConflictError("retained direct job declaration changed")
        specs[job_id] = spec
        state_rows[job_id] = row
    dependency_rows = cursor.execute(
        """
        SELECT epoch_id, parent_job_id, child_job_id
        FROM groundloop_semantic_job_dependency
        WHERE epoch_id = %s
        ORDER BY parent_job_id COLLATE "C", child_job_id COLLATE "C"
        """,
        (closure.epoch_id,),
    ).fetchall()
    dependencies = tuple(
        sorted(
            ((int(row[0]), str(row[1]), str(row[2])) for row in dependency_rows),
            key=lambda edge: (_c_key(edge[1]), _c_key(edge[2])),
        )
    )
    expected_dependencies = tuple(
        sorted(
            (
                (closure.epoch_id, spec.parent_job_id, spec.job_id)
                for spec in specs.values()
                if spec.parent_job_id is not None
            ),
            key=lambda edge: (_c_key(str(edge[1])), _c_key(edge[2])),
        )
    )
    if dependencies != expected_dependencies:
        raise EventConflictError("retained direct dependency bijection changed")
    children_by_parent: dict[str, list[str]] = {}
    for _epoch_id, parent_job_id, child_job_id in dependencies:
        children_by_parent.setdefault(parent_job_id, []).append(child_job_id)
    for job_id in job_ids:
        _validate_m4_job_state(
            specs[job_id],
            state_rows[job_id],
            tuple(children_by_parent.get(job_id, ())),
        )
        if specs[job_id].kind is M4JobKind.VERIFY_PAIR:
            _validate_retained_direct_verifier_closure(
                cursor,
                specs[job_id],
                state_rows[job_id],
            )
        elif specs[job_id].kind in {
            M4JobKind.IMPACT_DISCOVERY,
            M4JobKind.FRONTIER_RETRIEVE,
        }:
            _validate_direct_discovery_closure(
                cursor,
                specs[job_id],
                state_rows[job_id],
                tuple(
                    specs[child_id] for child_id in children_by_parent.get(job_id, ())
                ),
                lock_authority=False,
            )
    roots: list[M4LogicalJobSpec] = []
    scopes: list[M4DiscoveryScope] = []
    for job_id in job_ids:
        spec = specs[job_id]
        scope_rows = cursor.execute(
            """
            SELECT root_job_id, epoch_id, registry_snapshot_id,
                   scope_kind, explicit_claim_ids, closed_revision
            FROM groundloop_discovery_scope
            WHERE root_job_id = %s
            """,
            (job_id,),
        ).fetchall()
        if spec.parent_job_id is not None:
            if (
                spec.kind is not M4JobKind.VERIFY_PAIR
                or spec.parent_job_id not in specs
                or specs[spec.parent_job_id].parent_job_id is not None
                or scope_rows
            ):
                raise EventConflictError("retained direct child closure changed")
            continue
        if spec.kind not in {
            M4JobKind.IMPACT_DISCOVERY,
            M4JobKind.FRONTIER_RETRIEVE,
        }:
            raise EventConflictError("retained direct root has child kind")
        roots.append(spec)
        if spec.kind is M4JobKind.IMPACT_DISCOVERY:
            if len(scope_rows) != 1:
                raise EventConflictError("direct impact root lacks one scope")
            scope_row = scope_rows[0]
            explicit = scope_row[4]
            if scope_row[3] != "all_registered_claims" or explicit is not None:
                raise EventConflictError("retained direct scope kind is invalid")
            members = direct.registered_claim_ids
            if (
                str(scope_row[0]) != job_id
                or int(scope_row[1]) != closure.epoch_id
                or str(scope_row[2]) != direct.claim_registry_snapshot_id
            ):
                raise EventConflictError("retained direct scope binding changed")
            root_state = M4JobState(str(state_rows[job_id][10]))
            expected_closed_revision = (
                int(str(state_rows[job_id][17]))
                if root_state
                in {M4JobState.COMPLETED_ACTIVE, M4JobState.COMPLETED_INACTIVE}
                else None
            )
            actual_closed_revision = None if scope_row[5] is None else int(scope_row[5])
            if actual_closed_revision != expected_closed_revision:
                raise EventConflictError(
                    "retained direct scope closure differs from its root"
                )
            scopes.append(
                M4DiscoveryScope(
                    root_job_id=job_id,
                    registry_snapshot_id=str(scope_row[2]),
                    registered_claim_ids=members,
                    closed=scope_row[5] is not None,
                )
            )
        elif scope_rows:
            raise EventConflictError("direct frontier root unexpectedly has a scope")
    root_tuple = tuple(sorted(roots, key=lambda item: _c_key(item.job_id)))
    scope_tuple = tuple(sorted(scopes, key=lambda item: _c_key(item.root_job_id)))
    root_ids = tuple(item.job_id for item in root_tuple)
    scope_ids = tuple(item.root_job_id for item in scope_tuple)
    fallback_claims = tuple(
        sorted(
            (
                item.target_claim_id
                for item in root_tuple
                if item.kind is M4JobKind.FRONTIER_RETRIEVE
                and item.target_claim_id is not None
            ),
            key=_c_key,
        )
    )
    if (
        root_ids != closure.direct_root_job_ids
        or scope_ids != closure.direct_scope_root_job_ids
        or fallback_claims != closure.direct_fallback_claim_ids
    ):
        raise EventConflictError("retained direct declaration differs from manifest")
    inserted_targets = tuple(
        sorted(
            (
                item.target_chunk_version_id
                for item in root_tuple
                if item.kind is M4JobKind.IMPACT_DISCOVERY
                and item.target_chunk_version_id is not None
            ),
            key=_c_key,
        )
    )
    if inserted_targets != tuple(sorted(direct.inserted_chunk_version_ids, key=_c_key)):
        raise EventConflictError("retained impact roots differ from inserted chunks")
    chunks = tuple(sorted(direct.deactivated_chunk_version_ids, key=_c_key))
    withdrawal = StructuralWithdrawal(
        WithdrawalPlan(
            deactivated_chunk_ids=chunks,
            observation_ids=(),
            candidate_edge_ids=(),
            affected_pairs=(),
            affected_claim_ids=(),
            chunk_lookups=len(chunks),
            observation_edge_visits=0,
            candidate_edge_visits=0,
        ),
        fallback_claims,
    )
    return M5DirectOpenPlan(
        _structural_payload_from_event(event),
        withdrawal,
        root_tuple,
        scope_tuple,
    )


def _hydrate_retained_direct_open(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    *,
    expected_epoch_id: int,
) -> M5DirectOpenPlan:
    _require_d29_document_route_authority(cursor)
    closure = _read_existing_document_closure(cursor, event)
    if closure is None or closure.epoch_id != expected_epoch_id:
        raise EventConflictError("retained direct document epoch changed")
    _validated_structural_source_chunks(
        cursor, event, closure, lock_authority=False, event_local_only=True
    )
    direct_open = _stored_direct_open(cursor, event, closure)
    _raise_if_terminal(cursor, event, closure)
    return direct_open


def _direct_withdrawal_preview(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    *,
    source_chunks: tuple[str, ...] | None = None,
) -> StructuralWithdrawal:
    checked = _document_event(event)
    direct = checked.direct_plan
    assert direct is not None
    chunks = (
        tuple(sorted(direct.deactivated_chunk_version_ids, key=_c_key))
        if source_chunks is None
        else _canonical_texts("locked direct deactivation chunks", source_chunks)
    )
    observation_values: list[tuple[object, ...]] = []
    candidate_values_raw: list[tuple[object, ...]] = []
    for chunk_id in chunks:
        observation_values.extend(
            tuple(row)
            for row in cursor.execute(
                """
                SELECT observation_id, subject_id, chunk_version_id
                FROM groundloop_observation_currency
                WHERE subject_kind = 'claim' AND chunk_version_id = %s
                ORDER BY observation_id COLLATE "C"
                """,
                (chunk_id,),
            ).fetchall()
        )
        candidate_values_raw.extend(
            tuple(row)
            for row in cursor.execute(
                """
                SELECT frontier.claim_id, frontier.chunk_version_id,
                       frontier.candidate_policy_id, frontier.frontier_state,
                       frontier.rank, frontier.retrieval_score,
                       frontier.candidate_artifact_hash,
                       frontier.valid_from_epoch, frontier.valid_to_epoch,
                       creator.structural_status, creator.semantic_status,
                       creator.evaluation_state, creator.publication_mode,
                       creator.sealed_at, creator_update.candidate_policy_id
                FROM groundloop_candidate_frontier AS frontier
                JOIN groundloop_epoch AS creator
                  ON creator.epoch_id = frontier.valid_from_epoch
                JOIN groundloop_m4_update AS creator_update
                  ON creator_update.epoch_id = creator.epoch_id
                WHERE frontier.chunk_version_id = %s
                  AND frontier.valid_to_epoch IS NULL
                ORDER BY frontier.claim_id COLLATE "C",
                         frontier.candidate_policy_id COLLATE "C",
                         frontier.valid_from_epoch
                """,
                (chunk_id,),
            ).fetchall()
        )
    observation_rows = tuple(
        sorted(observation_values, key=lambda row: _c_key(str(row[0])))
    )
    candidate_rows = tuple(
        sorted(
            candidate_values_raw,
            key=lambda row: (
                _c_key(str(row[1])),
                _c_key(str(row[0])),
                _c_key(str(row[2])),
                int(str(row[7])),
            ),
        )
    )
    observations = tuple(
        ObservationDependency(str(row[0]), PairKey(str(row[1]), str(row[2])))
        for row in observation_rows
    )
    candidate_values: list[CandidateDependency] = []
    for row in candidate_rows:
        try:
            entry = M4FrontierEntry(
                claim_id=str(row[0]),
                chunk_version_id=str(row[1]),
                candidate_policy_id=str(row[2]),
                state=M4FrontierState(str(row[3])),
                rank=int(str(row[4])),
                retrieval_score=float(str(row[5])),
                candidate_artifact_hash=_strip(row[6]),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise EventConflictError("direct frontier DTO is malformed") from error
        if (
            row[8] is not None
            or int(str(row[7])) > checked.expected_previous_published_epoch_id
            or tuple(row[9:13]) != ("committed", "sealed", "complete", "strict")
            or row[13] is None
            or str(row[14]) != entry.candidate_policy_id
        ):
            raise EventConflictError("direct frontier authority is malformed")
        if entry.candidate_policy_id != checked.candidate_policy_id:
            raise EventConflictError(
                "cross-policy direct candidate withdrawal is not authorized"
            )
        candidate_values.append(
            CandidateDependency(
                stable_m4_digest(
                    "m4-frontier-edge-v1",
                    entry.claim_id,
                    entry.chunk_version_id,
                    entry.candidate_policy_id,
                    entry.candidate_artifact_hash,
                ),
                PairKey(entry.claim_id, entry.chunk_version_id),
            )
        )
    candidates = tuple(candidate_values)
    plan = plan_withdrawal(
        ReverseDependencyIndex.build(observations, candidates), chunks
    )
    fallback_claim_ids = tuple(sorted(set(plan.affected_claim_ids)))
    return StructuralWithdrawal(plan, fallback_claim_ids)


def _direct_root_job(
    event: M5TypedEventPlan,
    execution_policy: ApplicationExecutionPolicy,
    *,
    kind: M4JobKind,
    claim_id: str = "",
    chunk_version_id: str = "",
) -> M4LogicalJobSpec:
    if kind is M4JobKind.IMPACT_DISCOVERY:
        execution_hash = execution_policy.impact_discovery_execution_spec_hash
    elif kind is M4JobKind.FRONTIER_RETRIEVE:
        execution_hash = execution_policy.frontier_retrieval_execution_spec_hash
    else:
        raise ValidationError("prospective direct root has verifier kind")
    job_id = M4LogicalJobSpec.derive_job_id(
        event_id=event.structural_event_id,
        kind=kind,
        candidate_policy_id=event.candidate_policy_id,
        execution_spec_hash=execution_hash,
        claim_id=claim_id,
        chunk_version_id=chunk_version_id,
    )
    return M4LogicalJobSpec(
        job_id=job_id,
        event_id=event.structural_event_id,
        kind=kind,
        candidate_policy_id=event.candidate_policy_id,
        payload_hash=stable_m4_digest(
            "m4-application-job-payload-v1",
            event.payload_hash,
            kind.value,
            "",
            claim_id,
            chunk_version_id,
        ),
        execution_spec_hash=execution_hash,
        target_claim_id=(claim_id if kind is M4JobKind.FRONTIER_RETRIEVE else None),
        target_chunk_version_id=(
            chunk_version_id if kind is M4JobKind.IMPACT_DISCOVERY else None
        ),
        expandable=True,
    )


def _document_requirement_roots(
    event: M5TypedEventPlan,
    manifest: M5CandidatePolicyManifest,
    withdrawal: M5RequirementWithdrawalPlan,
) -> tuple[M5RequirementRootDeclaration, ...]:
    checked = _document_event(event)
    if (
        type(manifest) is not M5CandidatePolicyManifest
        or manifest.candidate_policy_id != checked.candidate_policy_id
        or manifest.manifest_hash != checked.candidate_policy_manifest_hash
        or type(withdrawal) is not M5RequirementWithdrawalPlan
        or withdrawal.event_id != checked.structural_event_id
    ):
        raise ValidationError("D29 requirement declaration inputs are inconsistent")
    if any(
        key.candidate_policy_id != manifest.candidate_policy_id
        for key in withdrawal.fallback_keys
    ):
        raise ValidationError("D29 fallback declarations mix candidate policies")
    roots: list[M5RequirementRootDeclaration] = []
    for key in withdrawal.fallback_keys:
        scope = M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            requirement_version_id=key.requirement_version_id,
            inserted_chunk_version_id=None,
            candidate_policy_id=manifest.candidate_policy_id,
            requirement_registry_snapshot_digest=(
                checked.requirement_registry_snapshot.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=(
                checked.active_chunk_snapshot.active_chunk_snapshot_digest
            ),
        )
        scope.validate_snapshots(
            checked.requirement_registry_snapshot, checked.active_chunk_snapshot
        )
        roots.append(
            M5RequirementRootDeclaration(
                scope=scope,
                job=M5LogicalJobSpec.build(
                    structural_event_id=checked.structural_event_id,
                    job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
                    manifest=manifest,
                    scope=scope,
                ),
            )
        )
    direct = checked.direct_plan
    assert direct is not None
    for chunk_id in direct.inserted_chunk_version_ids:
        scope = M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.REVERSE_CHUNK,
            requirement_version_id=None,
            inserted_chunk_version_id=chunk_id,
            candidate_policy_id=manifest.candidate_policy_id,
            requirement_registry_snapshot_digest=(
                checked.requirement_registry_snapshot.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=(
                checked.active_chunk_snapshot.active_chunk_snapshot_digest
            ),
        )
        scope.validate_snapshots(
            checked.requirement_registry_snapshot, checked.active_chunk_snapshot
        )
        roots.append(
            M5RequirementRootDeclaration(
                scope=scope,
                job=M5LogicalJobSpec.build(
                    structural_event_id=checked.structural_event_id,
                    job_kind=M5JobKind.REVERSE_REQUIREMENT_DISCOVERY,
                    manifest=manifest,
                    scope=scope,
                ),
            )
        )
    ordered = tuple(sorted(roots, key=lambda item: _c_key(item.job.logical_job_id)))
    if len({item.job.logical_job_id for item in ordered}) != len(ordered):
        raise EventConflictError("D29 requirement declarations collide")
    return ordered


def _validate_composed_requirement_roots(
    event: M5TypedEventPlan,
    manifest: M5CandidatePolicyManifest,
    withdrawal: M5RequirementWithdrawalPlan,
    retained: tuple[M5RequirementRootDeclaration, ...],
    retained_root_set_hash: str,
) -> None:
    composed = _document_requirement_roots(event, manifest, withdrawal)
    composed_hash = digests.requirement_root_set_digest(
        item.job.logical_job_id for item in composed
    )
    if composed != retained or composed_hash != retained_root_set_hash:
        raise EventConflictError(
            "application-composed requirement declarations changed"
        )


@dataclass(frozen=True, slots=True)
class _DocumentDeclarationCoordinates:
    """Transaction-local tier-8/tier-9 declaration reservation coordinates."""

    direct_scope_root_job_ids: tuple[str, ...]
    requirement_scope_root_job_ids: tuple[str, ...]
    direct_job_ids: tuple[str, ...]
    requirement_job_ids: tuple[str, ...]


def _document_declaration_coordinates(
    direct_open: M5DirectOpenPlan,
    requirement_roots: tuple[M5RequirementRootDeclaration, ...],
) -> _DocumentDeclarationCoordinates:
    if (
        type(direct_open) is not M5DirectOpenPlan
        or type(requirement_roots) is not tuple
    ):
        raise ValidationError("D29 declaration coordinates require exact plans")
    if any(
        type(root) is not M5RequirementRootDeclaration for root in requirement_roots
    ):
        raise ValidationError("D29 requirement coordinate has another type")
    return _DocumentDeclarationCoordinates(
        direct_scope_root_job_ids=_canonical_texts(
            "prospective direct scope IDs",
            (scope.root_job_id for scope in direct_open.discovery_scopes),
        ),
        requirement_scope_root_job_ids=_canonical_texts(
            "prospective requirement scope IDs",
            (root.job.logical_job_id for root in requirement_roots),
        ),
        direct_job_ids=_canonical_texts(
            "prospective direct job IDs",
            (root.job_id for root in direct_open.root_jobs),
        ),
        requirement_job_ids=_canonical_texts(
            "prospective requirement job IDs",
            (root.job.logical_job_id for root in requirement_roots),
        ),
    )


def _direct_open_from_withdrawal(
    event: M5TypedEventPlan,
    execution_policy: ApplicationExecutionPolicy,
    withdrawal: StructuralWithdrawal,
) -> M5DirectOpenPlan:
    checked = _document_event(event)
    if type(execution_policy) is not ApplicationExecutionPolicy:
        raise ValidationError("D29 direct declaration requires exact execution policy")
    direct = checked.direct_plan
    assert direct is not None
    roots = [
        _direct_root_job(
            checked,
            execution_policy,
            kind=M4JobKind.IMPACT_DISCOVERY,
            chunk_version_id=chunk_id,
        )
        for chunk_id in direct.inserted_chunk_version_ids
    ]
    roots.extend(
        _direct_root_job(
            checked,
            execution_policy,
            kind=M4JobKind.FRONTIER_RETRIEVE,
            claim_id=claim_id,
        )
        for claim_id in withdrawal.fallback_claim_ids
    )
    root_tuple = tuple(sorted(roots, key=lambda item: _c_key(item.job_id)))
    scopes = tuple(
        M4DiscoveryScope(
            root_job_id=root.job_id,
            registry_snapshot_id=direct.claim_registry_snapshot_id,
            registered_claim_ids=direct.registered_claim_ids,
        )
        for root in root_tuple
        if root.kind is M4JobKind.IMPACT_DISCOVERY
    )
    return M5DirectOpenPlan(
        _structural_payload_from_event(checked),
        withdrawal,
        root_tuple,
        scopes,
    )


def _capture_d29_document_open_binding(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    closure: _DocumentClosure,
    source_chunks: tuple[str, ...],
) -> _D29DocumentOpenBinding:
    row = cursor.execute(
        "SELECT pg_backend_pid(), pg_current_xact_id()::text, session_user"
    ).fetchone()
    if row is None or type(row[0]) is not int or not str(row[2]).strip():
        raise EventConflictError("D29 document transaction identity is unavailable")
    try:
        transaction_identity = int(str(row[1]))
    except (TypeError, ValueError) as error:
        raise EventConflictError(
            "D29 document transaction identity is malformed"
        ) from error
    return _D29DocumentOpenBinding(
        cursor_object_identity=id(cursor),
        backend_identity=row[0],
        transaction_identity=transaction_identity,
        session_role=str(row[2]),
        epoch_id=closure.epoch_id,
        structural_event_id=event.structural_event_id,
        source_identity_hash=event.payload_hash,
        predecessor_epoch_id=closure.previous_epoch_id,
        source_chunks=source_chunks,
    )


def _seal_prepared_document_open(prepared: _PreparedDocumentOpen) -> None:
    if prepared.authority_snapshot is not None:
        raise EventConflictError("D29 document locator authority was already sealed")
    snapshot = _PreparedDocumentOpenSnapshot(
        snapshot_identity=0,
        binding=deepcopy(prepared.binding),
        prepared_identity=prepared.prepared_identity,
        event=deepcopy(prepared.event),
        execution_policy=deepcopy(prepared.execution_policy),
        closure=deepcopy(prepared.closure),
        source_chunks=deepcopy(prepared.source_chunks),
        locator=deepcopy(prepared.locator),
        located_observation_edges=deepcopy(prepared.located_observation_edges),
        bootstrap_authority=deepcopy(prepared.bootstrap_authority),
    )
    object.__setattr__(snapshot, "snapshot_identity", id(snapshot))
    prepared.authority_snapshot = snapshot


def _validate_prepared_document_open(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    prepared: _PreparedDocumentOpen,
    *,
    execution_policy: ApplicationExecutionPolicy,
    phase: _DocumentOpenPhase,
) -> None:
    checked = _document_event(event)
    if type(prepared) is not _PreparedDocumentOpen:
        raise ValidationError("D29 prepared authority has another concrete type")
    if prepared.prepared_identity != id(prepared):
        raise ValidationError("D29 prepared authority was copied or replaced")
    snapshot = prepared.authority_snapshot
    if type(snapshot) is not _PreparedDocumentOpenSnapshot or (
        snapshot.snapshot_identity != id(snapshot)
    ):
        raise ValidationError("D29 prepared authority snapshot was replaced")
    if (
        prepared.binding != snapshot.binding
        or prepared.prepared_identity != snapshot.prepared_identity
        or prepared.event != snapshot.event
        or prepared.execution_policy != snapshot.execution_policy
        or prepared.closure != snapshot.closure
        or prepared.source_chunks != snapshot.source_chunks
        or prepared.locator != snapshot.locator
        or prepared.located_observation_edges != snapshot.located_observation_edges
        or prepared.bootstrap_authority != snapshot.bootstrap_authority
    ):
        raise EventConflictError("D29 prepared locator authority changed")
    if (
        prepared.phase is not phase
        or checked != prepared.event
        or type(execution_policy) is not ApplicationExecutionPolicy
        or execution_policy != prepared.execution_policy
    ):
        raise ValidationError("D29 prepared locator phase or input changed")
    current_binding = _capture_d29_document_open_binding(
        cursor,
        checked,
        prepared.closure,
        prepared.source_chunks,
    )
    if current_binding != prepared.binding:
        raise ValidationError("D29 prepared authority changed transaction context")


def _build_d29_direct_matching_authority(
    binding: _D29DocumentOpenBinding,
    locator: _D29LocatorAuthority,
) -> _D29DirectMatchingAuthority:
    withdrawn_ids_from_currency = {
        observation_id
        for _claim_id, _chunk_id, _task_type, observation_id in (
            locator.direct_currency_keys
        )
    }
    withdrawn_rows = {
        item.currency.observation_id: item.observation_row
        for item in locator.d30_claims.dynamic
        if item.currency.observation_id in withdrawn_ids_from_currency
    }
    withdrawn_rows.update(
        {
            item.currency.observation_id: item.observation_row
            for item in locator.d30_claims.bootstrap
            if item.currency.observation_id in withdrawn_ids_from_currency
        }
    )
    withdrawn = tuple(
        _semantic_observation_from_row(
            withdrawn_rows[key], label="D29 withdrawn direct observation"
        )
        for key in sorted(withdrawn_rows, key=_c_key)
    )
    remaining = tuple(
        _semantic_observation_from_row(row, label="D29 remaining direct observation")
        for row in locator.direct_remaining_observation_rows
    )
    withdrawn_ids = {item.observation_id for item in withdrawn}
    remaining_ids = {item.observation_id for item in remaining}
    observation_text_hashes = tuple(
        (observation_id, text_hash)
        for observation_id, _chunk_id, _text, text_hash in (
            locator.direct_observation_source_rows
        )
    )
    if (
        len(withdrawn_ids) != len(withdrawn)
        or len(remaining_ids) != len(remaining)
        or withdrawn_ids & remaining_ids
        or withdrawn_ids != withdrawn_ids_from_currency
        or len({item[0] for item in observation_text_hashes})
        != len(observation_text_hashes)
        or {item[0] for item in observation_text_hashes}
        != withdrawn_ids | remaining_ids
    ):
        raise EventConflictError("D29 direct matching observation partition changed")
    for claim in locator.direct_claim_before_images:
        state_ids = set(claim.supporting_observation_ids) | set(
            claim.refuting_observation_ids
        )
        claim_withdrawn = set(claim.withdrawn_observation_ids)
        claim_remaining = set(claim.remaining_support_observation_ids) | set(
            claim.remaining_refute_observation_ids
        )
        if (
            claim_remaining != state_ids - claim_withdrawn
            or not claim_withdrawn <= withdrawn_ids
            or not claim_remaining <= remaining_ids
        ):
            raise EventConflictError("D29 direct matching claim partition changed")
    authority = _D29DirectMatchingAuthority(
        binding=deepcopy(binding),
        authority_identity=0,
        claim_before_images=deepcopy(locator.direct_claim_before_images),
        answer_before_images=deepcopy(locator.direct_answer_before_images),
        withdrawn_observations=deepcopy(withdrawn),
        remaining_observations=deepcopy(remaining),
        observation_text_hashes=deepcopy(observation_text_hashes),
    )
    object.__setattr__(authority, "authority_identity", id(authority))
    snapshot = _D29DirectMatchingAuthoritySnapshot(
        snapshot_identity=0,
        binding=deepcopy(authority.binding),
        authority_identity=authority.authority_identity,
        claim_before_images=deepcopy(authority.claim_before_images),
        answer_before_images=deepcopy(authority.answer_before_images),
        withdrawn_observations=deepcopy(authority.withdrawn_observations),
        remaining_observations=deepcopy(authority.remaining_observations),
        observation_text_hashes=deepcopy(authority.observation_text_hashes),
    )
    object.__setattr__(snapshot, "snapshot_identity", id(snapshot))
    object.__setattr__(authority, "authority_snapshot", snapshot)
    return authority


def _validate_d29_direct_matching_authority(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    source_id: str,
    source_identity_hash: str,
    authority: _D29DirectMatchingAuthority,
) -> None:
    """Validate private D29 direct evidence without rediscovering any key."""

    if type(authority) is not _D29DirectMatchingAuthority or (
        authority.authority_identity != id(authority)
    ):
        raise ValidationError("D29 direct matching authority was copied or replaced")
    snapshot = authority.authority_snapshot
    if type(snapshot) is not _D29DirectMatchingAuthoritySnapshot or (
        snapshot.snapshot_identity != id(snapshot)
    ):
        raise ValidationError("D29 direct matching authority snapshot was replaced")
    if (
        authority.binding != snapshot.binding
        or authority.authority_identity != snapshot.authority_identity
        or authority.claim_before_images != snapshot.claim_before_images
        or authority.answer_before_images != snapshot.answer_before_images
        or authority.withdrawn_observations != snapshot.withdrawn_observations
        or authority.remaining_observations != snapshot.remaining_observations
        or authority.observation_text_hashes != snapshot.observation_text_hashes
    ):
        raise EventConflictError("D29 direct matching authority changed")
    binding = authority.binding
    if (
        type(epoch_id) is not int
        or type(source_id) is not str
        or type(source_identity_hash) is not str
        or binding.cursor_object_identity != id(cursor)
        or binding.epoch_id != epoch_id
        or binding.structural_event_id != source_id
        or binding.source_identity_hash != source_identity_hash
    ):
        raise ValidationError("D29 direct matching source binding changed")
    row = cursor.execute(
        "SELECT pg_backend_pid(), pg_current_xact_id()::text, session_user"
    ).fetchone()
    if row is None:
        raise ValidationError("D29 direct matching transaction is unavailable")
    try:
        current = (int(row[0]), int(str(row[1])), str(row[2]))
    except (TypeError, ValueError) as error:
        raise ValidationError(
            "D29 direct matching transaction identity is malformed"
        ) from error
    if current != (
        binding.backend_identity,
        binding.transaction_identity,
        binding.session_role,
    ):
        raise ValidationError("D29 direct matching transaction context changed")


def _preview_document_direct_open(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    *,
    execution_policy: ApplicationExecutionPolicy,
) -> M5DirectOpenPlan:
    _require_d29_document_route_authority(cursor)
    checked = _document_event(event)
    if type(execution_policy) is not ApplicationExecutionPolicy:
        raise ValidationError("D29 direct preview requires exact execution policy")
    closure = _read_existing_document_closure(cursor, checked)
    if closure is not None:
        _validated_structural_source_chunks(
            cursor,
            checked,
            closure,
            lock_authority=False,
            event_local_only=True,
        )
        direct_open = _stored_direct_open(cursor, checked, closure)
        _raise_if_terminal(cursor, checked, closure)
        return direct_open
    _require_d30_read_committed(cursor)
    withdrawal = _direct_withdrawal_preview(cursor, checked)
    return _direct_open_from_withdrawal(checked, execution_policy, withdrawal)


def _prepare_locked_document_open(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    *,
    execution_policy: ApplicationExecutionPolicy,
) -> _PreparedDocumentOpen:
    """Gather and bind every D29 locator after tier 7 and before tier 8."""

    _require_d29_document_route_authority(cursor)
    checked = _document_event(event)
    if type(execution_policy) is not ApplicationExecutionPolicy:
        raise ValidationError("D29 prepare requires exact execution policy")
    closure = _read_existing_document_closure(
        cursor,
        checked,
        allow_missing_event_snapshots=True,
    )
    if closure is None:
        raise EventConflictError("locked D29 document source epoch is absent")
    source_chunks = _validated_structural_source_chunks(
        cursor,
        checked,
        lock_authority=True,
        closure=closure,
    )
    _require_d30_read_committed(cursor)
    locator = _gather_d29_locator_authority(
        cursor,
        checked,
        closure,
        execution_policy=execution_policy,
        source_chunks=source_chunks,
    )
    located_observation_edges = tuple(
        sorted(
            {
                M5WithdrawnObservationEdge(
                    observation_id,
                    requirement_id,
                    chunk_id,
                    closure.candidate_policy.candidate_policy_id,
                )
                for (
                    requirement_id,
                    chunk_id,
                    _task_type,
                    observation_id,
                ) in locator.requirement_currency_keys
            },
            key=lambda edge: (
                _c_key(edge.observation_id),
                _c_key(edge.requirement_version_id),
                _c_key(edge.chunk_version_id),
                _c_key(edge.candidate_policy_id),
            ),
        )
    )
    _assert_zero_cancellation_authority(cursor, closure.previous_epoch_id)
    bootstrap_authority = _lock_d29_bootstrap_provenance(
        cursor,
        locator,
        predecessor_epoch_id=closure.previous_epoch_id,
    )
    binding = _capture_d29_document_open_binding(
        cursor, checked, closure, source_chunks
    )
    prepared = _PreparedDocumentOpen(
        binding=binding,
        prepared_identity=0,
        event=checked,
        execution_policy=execution_policy,
        closure=closure,
        source_chunks=source_chunks,
        locator=locator,
        located_observation_edges=located_observation_edges,
        bootstrap_authority=tuple(
            sorted(bootstrap_authority.items(), key=lambda item: _c_key(item[0]))
        ),
    )
    prepared.prepared_identity = id(prepared)
    _seal_prepared_document_open(prepared)
    return prepared


def _continue_locked_document_open(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    prepared: _PreparedDocumentOpen,
    *,
    execution_policy: ApplicationExecutionPolicy,
    supplied_direct_open: M5DirectOpenPlan,
    supplied_requirement_withdrawal: M5RequirementWithdrawalPlan,
    supplied_requirement_roots: tuple[M5RequirementRootDeclaration, ...],
    supplied_requirement_root_set_hash: str,
) -> _LockedDocumentOpenResult:
    """Consume one D29 locator image and finish the held tier-8--11a path."""

    checked = _document_event(event)
    _validate_prepared_document_open(
        cursor,
        checked,
        prepared,
        execution_policy=execution_policy,
        phase=_DocumentOpenPhase.LOCATORS_GATHERED,
    )
    closure = _read_existing_document_closure(cursor, checked)
    if closure is None or closure != prepared.closure:
        raise EventConflictError("D29 prepared document closure changed")
    source_chunks = _validated_structural_source_chunks(
        cursor,
        checked,
        lock_authority=False,
        closure=closure,
    )
    if source_chunks != prepared.source_chunks:
        raise EventConflictError("D29 prepared structural source changed")
    _validate_d29_event_snapshot_image(cursor, checked, closure)
    prepared.phase = _DocumentOpenPhase.CONSUMED
    locator = prepared.locator
    snapshots = locator.predecessor_snapshots
    bootstrap_authority = dict(prepared.bootstrap_authority)
    touched_direct_chunk_ids = tuple(
        sorted(
            set(source_chunks)
            | {
                chunk_id
                for _observation_id, chunk_id, _text, _text_hash in (
                    locator.direct_observation_source_rows
                )
            },
            key=_c_key,
        )
    )
    _lock_d29_touched_membership(
        cursor,
        locator,
        predecessor_epoch_id=closure.previous_epoch_id,
        snapshots=snapshots,
        source_chunks=touched_direct_chunk_ids,
    )
    direct_scopes = _lock_d29_scopes_and_reserve(cursor, locator)
    _lock_d29_jobs_and_reserve(cursor, locator)
    direct_attempts, direct_candidates = _lock_d29_tier_10_authority(
        cursor,
        locator,
        predecessor_epoch_id=closure.previous_epoch_id,
        candidate_policy_id=closure.candidate_policy.candidate_policy_id,
    )
    _lock_d29_attempt_outputs(cursor, locator)
    d30_dynamic_authority = _lock_d30_dynamic_owner_topology(
        cursor,
        locator,
        direct_attempts,
        direct_scopes,
        predecessor_epoch_id=closure.previous_epoch_id,
        candidate_policy_id=closure.candidate_policy.candidate_policy_id,
        verifier_execution_spec_hash=(
            closure.candidate_policy.verifier_execution_spec_hash
        ),
    )
    _lock_d29_candidate_source_closure(cursor, locator)
    candidate_edges = _candidate_edges(
        cursor,
        chunks=source_chunks,
        predecessor_epoch_id=closure.previous_epoch_id,
        snapshots=snapshots,
        candidate_policy_id=closure.candidate_policy.candidate_policy_id,
        lock_authority=False,
        located_rows=locator.admitted_locator_keys,
        qualifying_digests=frozenset(locator.qualifying_admitted_pair_digests),
    )
    verifier_authority = _lock_d29_verifier_provenance(cursor, locator)
    observation_edges, direct_observations = _lock_d29_observation_authority(
        cursor,
        locator,
        predecessor_epoch_id=closure.previous_epoch_id,
        candidate_policy_id=closure.candidate_policy.candidate_policy_id,
        verifier_authority=verifier_authority,
        bootstrap_authority=bootstrap_authority,
        d30_dynamic_authority=d30_dynamic_authority,
    )
    if observation_edges != prepared.located_observation_edges:
        raise EventConflictError("requirement observation locators changed before lock")
    direct_matching_authority = _build_d29_direct_matching_authority(
        prepared.binding,
        locator,
    )
    active_requirement_ids = {
        edge.requirement_version_id for edge in candidate_edges
    } | {edge.requirement_version_id for edge in observation_edges}
    requirement_withdrawal = plan_requirement_withdrawal(
        event_id=checked.structural_event_id,
        deactivated_chunk_version_ids=source_chunks,
        candidate_edges=candidate_edges,
        observation_edges=observation_edges,
        cancelled_job_ids=(),
        active_requirement_version_ids=tuple(
            sorted(active_requirement_ids, key=_c_key)
        ),
    )
    direct_withdrawal = _locked_direct_withdrawal(
        source_chunks=source_chunks,
        observations=direct_observations,
        candidates=direct_candidates,
    )
    direct_open = _direct_open_from_withdrawal(
        checked, execution_policy, direct_withdrawal
    )
    requirement_roots = _document_requirement_roots(
        checked, closure.candidate_policy, requirement_withdrawal
    )
    root_set_hash = digests.requirement_root_set_digest(
        item.job.logical_job_id for item in requirement_roots
    )
    exact_coordinates = _document_declaration_coordinates(
        direct_open, requirement_roots
    )
    _require_coordinate_subset(exact_coordinates, locator.prospective_coordinates)
    manifest_arrays = _validate_document_manifest(
        _document_declaration_manifest(direct_open)
    )
    if manifest_arrays != (
        closure.direct_root_job_ids,
        closure.direct_scope_root_job_ids,
        closure.direct_fallback_claim_ids,
    ):
        raise EventConflictError("locked direct declaration differs from manifest")
    if (
        type(supplied_direct_open) is not M5DirectOpenPlan
        or supplied_direct_open != direct_open
        or type(supplied_requirement_withdrawal) is not M5RequirementWithdrawalPlan
        or supplied_requirement_withdrawal != requirement_withdrawal
        or type(supplied_requirement_roots) is not tuple
        or supplied_requirement_roots != requirement_roots
        or type(supplied_requirement_root_set_hash) is not str
        or supplied_requirement_root_set_hash != root_set_hash
        or closure.requirement_root_set_hash != root_set_hash
    ):
        raise EventConflictError("D29 compare-only open proposal changed")
    return _LockedDocumentOpenResult(
        direct_open=direct_open,
        requirement_withdrawal=requirement_withdrawal,
        requirement_roots=requirement_roots,
        requirement_root_set_hash=root_set_hash,
        direct_matching_authority=direct_matching_authority,
    )


def _derive_locked_document_open(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    *,
    execution_policy: ApplicationExecutionPolicy,
    supplied_direct_open: M5DirectOpenPlan,
    supplied_requirement_withdrawal: M5RequirementWithdrawalPlan,
    supplied_requirement_roots: tuple[M5RequirementRootDeclaration, ...],
    supplied_requirement_root_set_hash: str,
) -> tuple[
    M5DirectOpenPlan,
    M5RequirementWithdrawalPlan,
    tuple[M5RequirementRootDeclaration, ...],
    str,
]:
    """Retained monolithic projection over the two private D29 phases."""

    _require_d29_document_route_authority(cursor)
    checked = _document_event(event)
    prepared = _prepare_locked_document_open(
        cursor,
        checked,
        execution_policy=execution_policy,
    )
    result = _continue_locked_document_open(
        cursor,
        checked,
        prepared,
        execution_policy=execution_policy,
        supplied_direct_open=supplied_direct_open,
        supplied_requirement_withdrawal=supplied_requirement_withdrawal,
        supplied_requirement_roots=supplied_requirement_roots,
        supplied_requirement_root_set_hash=supplied_requirement_root_set_hash,
    )
    return (
        result.direct_open,
        result.requirement_withdrawal,
        result.requirement_roots,
        result.requirement_root_set_hash,
    )


def _load_retained_document_open(
    cursor: Cursor[Any],
    event: M5TypedEventPlan,
    *,
    expected_epoch_id: int,
) -> tuple[
    M5DirectOpenPlan,
    M5RequirementWithdrawalPlan,
    tuple[M5RequirementRootDeclaration, ...],
    str,
]:
    """Hydrate one complete immutable existing-event declaration projection."""

    _require_d29_document_route_authority(cursor)
    checked = _document_event(event)
    closure = _read_existing_document_closure(cursor, checked)
    if closure is None or closure.epoch_id != expected_epoch_id:
        raise EventConflictError("retained D29 document epoch changed")
    _validated_structural_source_chunks(
        cursor,
        checked,
        closure,
        lock_authority=False,
        event_local_only=True,
    )
    direct_open = _stored_direct_open(cursor, checked, closure)
    requirement_withdrawal, requirement_roots = _stored_requirement_declarations(
        cursor, checked, closure
    )
    _validate_composed_requirement_roots(
        checked,
        closure.candidate_policy,
        requirement_withdrawal,
        requirement_roots,
        closure.requirement_root_set_hash,
    )
    _document_declaration_coordinates(direct_open, requirement_roots)
    _raise_if_terminal(cursor, checked, closure)
    return (
        direct_open,
        requirement_withdrawal,
        requirement_roots,
        closure.requirement_root_set_hash,
    )
