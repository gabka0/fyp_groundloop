from __future__ import annotations

from dataclasses import fields, replace
from datetime import UTC, datetime, timedelta
from inspect import signature

import pytest

from groundloop.domain import (
    DecisionPolicy,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
    VerificationLabel,
)
from groundloop.errors import ValidationError
from groundloop.events import ChunkInput, InsertDocumentEvent
from groundloop.m4.application import (
    DiscoveryResult as M4DiscoveryResult,
)
from groundloop.m4.application import (
    DynamicEventPlan,
    OpenEventReceipt,
    PublicationReceipt,
)
from groundloop.m4.application import (
    JobLease as M4PublicJobLease,
)
from groundloop.m4.application import (
    ObservationApplicationPort as M4ObservationApplicationPort,
)
from groundloop.m4.application import (
    RuntimeTransitionPort as M4RuntimeTransitionPort,
)
from groundloop.m4.contracts import (
    AdmissionChannel as M4AdmissionChannel,
)
from groundloop.m4.contracts import (
    AdmittedPair as M4AdmittedPair,
)
from groundloop.m4.contracts import (
    ChannelHit as M4ChannelHit,
)
from groundloop.m4.contracts import (
    ChildClosure as M4ChildClosure,
)
from groundloop.m4.contracts import (
    CorpusUpdateIdentity,
    UpdateKind,
    VectorIndexKind,
    stable_m4_digest,
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
    PairKey as M4PairKey,
)
from groundloop.m5.digests import normalize_text_v1, normalized_text_hash_v1
from groundloop.m5.events import legacy_event_payload_digest
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5AcquisitionDisposition,
    M5ActivationReceipt,
    M5ActivationRequest,
    M5AttemptArchiveReason,
    M5AttemptCompletionReceipt,
    M5AttemptDisposition,
    M5AttemptExecutionEvidence,
    M5AttemptOutput,
    M5AttemptResultArtifact,
    M5CallAmbiguityReport,
    M5CancellationPlan,
    M5CancellationReceipt,
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
    M5JobLease,
    M5JobState,
    M5LeaseTerminalProjection,
    M5LogicalJobSpec,
    M5OwnerPendingCounter,
    M5ReplayedOutcome,
    M5RequirementAdmissionChannel,
    M5RequirementChannelHit,
    M5RequirementDiscoveryResult,
    M5RequirementPairInput,
    M5RequirementRootProvenance,
    M5RequirementScopeSelection,
    M5RequirementVerifierArtifact,
    M5RetrievalTermination,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeOperationalConfig,
    M5RuntimeSubgraph,
    M5RuntimeTiming,
    M5RuntimeTimingCoverage,
    M5RuntimeTimingObservation,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5StateReferenceKind,
    M5TerminalReason,
    M5TextNormalizerProvenance,
    M5TransitionTimingAnchor,
    M5TransitionTimingReceipt,
    M5TypedDirectJobLease,
    M5TypedDirectLateReturnEnvelope,
    M5TypedDirectReturnKind,
    M5TypedDirectScopeKind,
    M5TypedDirectTerminalProjection,
    M5TypedDirectVerificationExecution,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
    SemanticPairKey,
    validate_changed_state_references,
    validate_requirement_channel_hits,
)

H1 = "1" * 64
H2 = "2" * 64
H3 = "3" * 64
H4 = "4" * 64
D24_DEADLINE = datetime(2026, 8, 6, 12, 0, tzinfo=UTC)


def make_manifest(
    *,
    forward_budget: int = 2,
    reverse_budget: int = 2,
    lineage: bool = True,
) -> M5CandidatePolicyManifest:
    return M5CandidatePolicyManifest.build(
        embedding_model_artifact_id="embedding-artifact",
        requirement_role_template_hash=H1,
        chunk_role_template_hash=H2,
        vector_method_version="vector-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=H3,
        vector_search_config_hash=H4,
        lexical_method_version="lexical-v1",
        lexical_config_hash=H1,
        lexical_postgres_version="postgres-17.5",
        lexical_regconfig_identity="simple",
        fusion_version="rank-interleave-v1",
        reverse_budget_per_inserted_chunk=reverse_budget,
        forward_budget_per_requirement=forward_budget,
        verifier_execution_spec_hash=H3,
        decision_policy_version="decision-v1",
        lineage_safety_override=lineage,
    )


def make_snapshots() -> tuple[RequirementRegistrySnapshot, ActiveChunkSnapshot]:
    requirements = RequirementRegistrySnapshot.build(
        (
            RequirementRegistrySnapshotEntry.build(
                requirement_version_id="req-2",
                group_version_id="group-2",
                group_family_id="family-2",
                owner_claim_id="claim-2",
                requirement_text=" second\t requirement ",
            ),
            RequirementRegistrySnapshotEntry.build(
                requirement_version_id="req-1",
                group_version_id="group-1",
                group_family_id="family-1",
                owner_claim_id="claim-1",
                requirement_text=" first\nrequirement ",
            ),
        )
    )
    chunks = ActiveChunkSnapshot.build(
        (
            ActiveChunkSnapshotEntry.build(
                chunk_version_id="chunk-2", chunk_text="Chunk two"
            ),
            ActiveChunkSnapshotEntry.build(
                chunk_version_id="chunk-1", chunk_text=" Chunk\tone "
            ),
        )
    )
    return requirements, chunks


def make_scope(
    direction: M5DiscoveryDirection,
    manifest: M5CandidatePolicyManifest | None = None,
) -> M5DiscoveryScopeContract:
    manifest = manifest or make_manifest()
    requirements, chunks = make_snapshots()
    return M5DiscoveryScopeContract.build(
        direction=direction,
        requirement_version_id=(
            "req-1" if direction is M5DiscoveryDirection.FORWARD_REQUIREMENT else None
        ),
        inserted_chunk_version_id=(
            "chunk-1" if direction is M5DiscoveryDirection.REVERSE_CHUNK else None
        ),
        candidate_policy_id=manifest.candidate_policy_id,
        requirement_registry_snapshot_digest=(
            requirements.requirement_registry_snapshot_digest
        ),
        active_chunk_snapshot_digest=chunks.active_chunk_snapshot_digest,
    )


def make_hits(
    *,
    root_job_id: str = "root-1",
    scope_contract_digest: str = H1,
    policy_id: str = "policy",
) -> tuple[M5RequirementChannelHit, ...]:
    pair_1 = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-1")
    pair_2 = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-2")
    hits = (
        M5RequirementChannelHit.build(
            epoch_id=3,
            root_job_id=root_job_id,
            scope_contract_digest=scope_contract_digest,
            pair=pair_2,
            candidate_policy_id=policy_id,
            channel=M5RequirementAdmissionChannel.VECTOR,
            rank=2,
            score=0.8,
            channel_artifact_hash=H1,
        ),
        M5RequirementChannelHit.build(
            epoch_id=3,
            root_job_id=root_job_id,
            scope_contract_digest=scope_contract_digest,
            pair=pair_1,
            candidate_policy_id=policy_id,
            channel=M5RequirementAdmissionChannel.VECTOR,
            rank=1,
            score=0.9,
            channel_artifact_hash=H2,
        ),
        M5RequirementChannelHit.build(
            epoch_id=3,
            root_job_id=root_job_id,
            scope_contract_digest=scope_contract_digest,
            pair=pair_1,
            candidate_policy_id=policy_id,
            channel=M5RequirementAdmissionChannel.LEXICAL,
            rank=1,
            score=0.7,
            channel_artifact_hash=H3,
        ),
        M5RequirementChannelHit.build(
            epoch_id=3,
            root_job_id=root_job_id,
            scope_contract_digest=scope_contract_digest,
            pair=pair_2,
            candidate_policy_id=policy_id,
            channel=M5RequirementAdmissionChannel.LINEAGE,
            rank=1,
            score=None,
            channel_artifact_hash=H4,
        ),
    )
    return tuple(
        sorted(
            hits,
            key=lambda hit: (
                hit.channel.value,
                hit.rank,
                hit.semantic_pair_digest,
            ),
        )
    )


def test_normalizer_all_whitespace_members_nonmember_and_no_unicode_folding() -> None:
    provenance = M5TextNormalizerProvenance()
    for codepoint in provenance.whitespace_codepoints:
        assert normalize_text_v1(f" a{chr(codepoint)}{chr(codepoint)}b ") == "a b"
    for nonmember in (0, 8, 14, 27, 134, 161, 8203, 12287):
        assert chr(nonmember) in normalize_text_v1(f"a{chr(nonmember)}b")
    assert normalize_text_v1("é") != normalize_text_v1("é")


def test_normative_dto_field_topology_is_exact() -> None:
    assert tuple(field.name for field in fields(M5TextNormalizerProvenance)) == (
        "normalizer_id",
        "whitespace_codepoints",
        "boundary_rule",
        "internal_rule",
        "other_codepoint_rule",
        "unicode_normalization_rule",
        "encoding",
        "hash_algorithm",
    )
    assert tuple(field.name for field in fields(M5CandidatePolicyManifest)) == (
        "candidate_policy_id",
        "embedding_model_artifact_id",
        "requirement_role_template_hash",
        "chunk_role_template_hash",
        "vector_method_version",
        "vector_index_kind",
        "vector_index_build_config_hash",
        "vector_search_config_hash",
        "lexical_method_version",
        "lexical_config_hash",
        "lexical_postgres_version",
        "lexical_regconfig_identity",
        "fusion_version",
        "reverse_budget_per_inserted_chunk",
        "forward_budget_per_requirement",
        "verifier_execution_spec_hash",
        "decision_policy_version",
        "lineage_safety_override",
    )
    assert M5RuntimeWork.counter_names() == (
        "deactivated_chunk_count",
        "withdrawn_candidate_edge_count",
        "withdrawn_current_observation_count",
        "direct_discovery_call_count",
        "direct_verifier_call_count",
        "direct_observation_artifact_count",
        "direct_effective_observation_count",
        "direct_inactive_completion_count",
        "requirement_forward_retrieval_call_count",
        "requirement_reverse_retrieval_call_count",
        "requirement_fallback_forward_call_count",
        "requirement_verifier_call_count",
        "requirement_observation_artifact_count",
        "requirement_effective_observation_count",
        "requirement_inactive_completion_count",
        "requirement_cancelled_job_count",
        "requirement_late_attempt_artifact_count",
        "requirement_channel_hit_count",
        "requirement_pre_dedup_selection_count",
        "requirement_admitted_pair_count",
        "group_state_write_count",
        "claim_state_write_count",
        "answer_state_write_count",
        "certificate_binding_write_count",
        "public_delta_count",
        "bytes_hashed",
        "bytes_serialized",
        "embedding_model_call_count",
        "verifier_model_call_count",
        "embedding_input_token_count",
        "verifier_input_token_count",
        "verifier_output_token_count",
    )
    assert tuple(field.name for field in fields(M5CancellationPlan)) == (
        "structural_event_id",
        "epoch_id",
        "cancelled_job_ids",
        "reason",
        "plan_digest",
    )
    assert tuple(field.name for field in fields(M5AttemptCompletionReceipt)) == (
        "logical_job_id",
        "attempt_id",
        "resulting_revision",
        "exact_replay",
    )
    assert tuple(field.name for field in fields(M5CancellationReceipt)) == (
        "cancelled_job_ids",
        "resulting_revision",
        "exact_replay",
    )


def test_candidate_manifest_builds_exact_id_and_rejects_policy_drift() -> None:
    manifest = make_manifest()

    assert manifest.candidate_policy_id == manifest.manifest_hash
    assert manifest.forward_retrieval_execution_spec_hash != (
        manifest.reverse_retrieval_execution_spec_hash
    )
    assert len(manifest.requirement_verifier_role_binding_hash) == 64
    with pytest.raises(ValidationError):
        replace(manifest, candidate_policy_id=H4)
    with pytest.raises(ValidationError):
        replace(manifest, fusion_version="union-v1", candidate_policy_id=H4)
    with pytest.raises(ValidationError):
        make_manifest(forward_budget=0)


def test_snapshots_sort_build_and_reject_count_order_hash_drift() -> None:
    requirements, chunks = make_snapshots()

    assert tuple(entry.requirement_version_id for entry in requirements.entries) == (
        "req-1",
        "req-2",
    )
    assert tuple(entry.chunk_version_id for entry in chunks.entries) == (
        "chunk-1",
        "chunk-2",
    )
    assert requirements.requirement_count == 2
    assert chunks.chunk_count == 2
    with pytest.raises(ValidationError):
        replace(requirements, requirement_count=1)
    with pytest.raises(ValidationError):
        replace(requirements, entries=tuple(reversed(requirements.entries)))
    with pytest.raises(ValidationError):
        changed_entry = replace(chunks.entries[0], text_hash=H4)
        replace(chunks, entries=(changed_entry, chunks.entries[1]))


def test_pair_and_scope_enforce_requirement_kind_membership_and_direction() -> None:
    requirements, chunks = make_snapshots()
    pair = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-1")
    forward = make_scope(M5DiscoveryDirection.FORWARD_REQUIREMENT)
    reverse = make_scope(M5DiscoveryDirection.REVERSE_CHUNK)

    pair.validate_snapshots(requirements, chunks)
    forward.validate_snapshots(requirements, chunks)
    forward.validate_pair(pair)
    reverse.validate_pair(pair)
    with pytest.raises(ValidationError):
        SemanticPairKey(SubjectKind.CLAIM, "claim-1", "chunk-1")
    with pytest.raises(ValidationError):
        M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            requirement_version_id="req-1",
            inserted_chunk_version_id="chunk-1",
            candidate_policy_id=make_manifest().candidate_policy_id,
            requirement_registry_snapshot_digest=(
                requirements.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=chunks.active_chunk_snapshot_digest,
        )
    with pytest.raises(ValidationError):
        forward.validate_pair(
            SemanticPairKey(SubjectKind.REQUIREMENT, "req-2", "chunk-1")
        )


def test_channel_hits_enforce_score_null_dense_rank_and_canonical_order() -> None:
    hits = make_hits()
    validate_requirement_channel_hits(hits)

    with pytest.raises(ValidationError):
        validate_requirement_channel_hits(tuple(reversed(hits)))
    with pytest.raises(ValidationError):
        replace(
            next(
                hit
                for hit in hits
                if hit.channel is M5RequirementAdmissionChannel.LINEAGE
            ),
            score=0.0,
        )
    with pytest.raises(ValidationError):
        replace(
            next(
                hit
                for hit in hits
                if hit.channel is M5RequirementAdmissionChannel.VECTOR and hit.rank == 2
            ),
            rank=3,
        )


def test_selection_and_discovery_identity_reject_nested_or_count_drift() -> None:
    hits = make_hits()
    pair = hits[0].pair
    selection = M5RequirementScopeSelection.build(
        root_job_id="root-1",
        scope_contract_digest=H1,
        pair=pair,
        fused_rank=1,
        reasons=(hits[0].channel,),
    )
    result = M5RequirementDiscoveryResult.build(
        root_job_id="root-1",
        scope_contract_digest=H1,
        termination=M5RetrievalTermination.SNAPSHOT_EXHAUSTED,
        channel_hits=hits,
        selections=(selection,),
    )

    assert result.result_artifact_id == digests.requirement_discovery_artifact_id(
        result.root_job_id, result.result_artifact_hash
    )
    with pytest.raises(ValidationError):
        replace(selection, mandatory_lineage=not selection.mandatory_lineage)
    with pytest.raises(ValidationError):
        replace(result, approximate_selection_count=0)
    with pytest.raises(ValidationError):
        replace(result, root_job_id="another-root")


def test_pair_input_binds_both_hashes_and_normalizer() -> None:
    requirements, chunks = make_snapshots()
    pair = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-1")
    pair_input = M5RequirementPairInput.build(
        pair=pair,
        scope_contract_digest=H1,
        candidate_policy_id="policy",
        owner_claim_id="claim-1",
        group_version_id="group-1",
        group_family_id="family-1",
        requirement_ordinal=0,
        requirement_text=" first\nrequirement ",
        document_version_id="document-version-1",
        chunk_index=0,
        chunk_text=" Chunk\tone ",
        stored_chunk_text_hash=H2,
        chunker_artifact_id="chunker-v1",
    )

    pair_input.validate_bound_rows(
        requirement_entry=requirements.member("req-1"),
        chunk_entry=chunks.member("chunk-1"),
    )
    assert pair_input.requirement_text_hash == normalized_text_hash_v1(
        "first requirement"
    )
    with pytest.raises(ValidationError):
        replace(pair_input, m5_chunk_text_hash=H3)
    with pytest.raises(ValidationError):
        replace(pair_input, normalizer_id="another-normalizer")


def test_verifier_checked_builder_scores_logits_policy_and_observation() -> None:
    pair = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-1")
    policy = DecisionPolicy("decision-v1", 0.6, 0.6)
    artifact = M5RequirementVerifierArtifact.build_checked(
        pair=pair,
        pair_input_hash=H1,
        execution_spec_hash=H2,
        model_artifact_id="model-artifact",
        model_id="model",
        model_revision="revision",
        prompt_artifact_id="prompt-artifact",
        prompt_version="prompt-v1",
        calibration_version="cal-v1",
        calibration_artifact_hash=None,
        temperature=1.0,
        decision_policy=policy,
        support_score=0.7,
        refute_score=0.1,
        neutral_score=0.2,
        raw_logits=(-1.0, 2.0, 0.0),
        raw_output_hash=H3,
    )
    observation = artifact.to_semantic_observation()

    assert artifact.operational_label is VerificationLabel.SUPPORT
    artifact.validate_decision_policy(policy)
    assert observation.subject_kind is SubjectKind.REQUIREMENT
    assert observation.task_type == "verify_requirement_v1"
    with pytest.raises(ValidationError):
        replace(artifact, temperature=0.0)
    with pytest.raises(ValidationError):
        replace(artifact, support_score=0.8)
    with pytest.raises(ValidationError):
        replace(artifact, raw_logits=(0.0, 1.0))  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        artifact.validate_decision_policy(DecisionPolicy("decision-v2", 0.6, 0.6))


def test_all_legal_job_shapes_and_wrong_direction_or_null_shapes() -> None:
    manifest = make_manifest()
    forward_scope = make_scope(M5DiscoveryDirection.FORWARD_REQUIREMENT, manifest)
    reverse_scope = make_scope(M5DiscoveryDirection.REVERSE_CHUNK, manifest)
    forward = M5LogicalJobSpec.build(
        structural_event_id="event",
        job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
        manifest=manifest,
        scope=forward_scope,
    )
    reverse = M5LogicalJobSpec.build(
        structural_event_id="event",
        job_kind=M5JobKind.REVERSE_REQUIREMENT_DISCOVERY,
        manifest=manifest,
        scope=reverse_scope,
    )
    pair = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-1")
    verifier = M5LogicalJobSpec.build(
        structural_event_id="event",
        job_kind=M5JobKind.VERIFY_REQUIREMENT_PAIR,
        manifest=manifest,
        scope=forward_scope,
        parent_job_id=forward.logical_job_id,
        pair=pair,
    )

    assert forward.expandable and reverse.expandable and not verifier.expandable
    with pytest.raises(ValidationError):
        M5LogicalJobSpec.build(
            structural_event_id="event",
            job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
            manifest=manifest,
            scope=reverse_scope,
        )
    with pytest.raises(ValidationError):
        replace(forward, pair=pair, semantic_pair_digest=pair.semantic_pair_digest)
    with pytest.raises(ValidationError):
        replace(verifier, parent_job_id=None)


def test_all_completion_shapes_and_canonical_inactive_root_closure() -> None:
    manifest = make_manifest()
    scope = make_scope(M5DiscoveryDirection.FORWARD_REQUIREMENT, manifest)
    root = M5LogicalJobSpec.build(
        structural_event_id="event",
        job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
        manifest=manifest,
        scope=scope,
    )
    pair = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-1")
    child = M5LogicalJobSpec.build(
        structural_event_id="event",
        job_kind=M5JobKind.VERIFY_REQUIREMENT_PAIR,
        manifest=manifest,
        scope=scope,
        parent_job_id=root.logical_job_id,
        pair=pair,
    )
    root_active = M5JobCompletion.build(
        job=root,
        terminal_state=M5JobState.COMPLETED_ACTIVE,
        result_artifact_id=H1,
        result_artifact_hash=H2,
        scope_closure_digest=digests.discovery_scope_closure_digest(
            scope.scope_contract_digest, (pair.semantic_pair_digest,)
        ),
        child_set_hash=digests.child_set_digest((child.logical_job_id,)),
    )
    verifier_active = M5JobCompletion.build(
        job=child,
        terminal_state=M5JobState.COMPLETED_ACTIVE,
        result_artifact_id=H3,
        result_artifact_hash=H4,
    )
    root_inactive = M5JobCompletion.build(
        job=root,
        terminal_state=M5JobState.COMPLETED_INACTIVE,
        result_artifact_id=H1,
        result_artifact_hash=H2,
        scope_closure_digest=digests.discovery_scope_closure_digest(
            scope.scope_contract_digest, ()
        ),
        child_set_hash=digests.child_set_digest(()),
        archive_reason=M5TerminalReason.SUBJECT_INACTIVE,
    )
    cancelled = M5JobCompletion.build(
        job=root,
        terminal_state=M5JobState.CANCELLED,
        archive_reason=M5TerminalReason.SCOPE_RETIRED,
    )
    failed = M5JobCompletion.build(
        job=root,
        terminal_state=M5JobState.TERMINAL_FAILED,
        archive_reason=M5TerminalReason.RETRY_EXHAUSTED,
    )
    verifier_inactive = M5JobCompletion.build(
        job=child,
        terminal_state=M5JobState.COMPLETED_INACTIVE,
        result_artifact_id=H1,
        result_artifact_hash=H2,
        archive_reason=M5TerminalReason.CHUNK_INACTIVE,
    )

    assert (
        len(
            {
                root_active.completion_digest,
                verifier_active.completion_digest,
                root_inactive.completion_digest,
                cancelled.completion_digest,
                failed.completion_digest,
                verifier_inactive.completion_digest,
            }
        )
        == 6
    )
    with pytest.raises(ValidationError):
        replace(cancelled, result_artifact_id=H1, result_artifact_hash=H2)
    with pytest.raises(ValidationError):
        M5JobCompletion.build(
            job=root,
            terminal_state=M5JobState.COMPLETED_INACTIVE,
            result_artifact_id=H1,
            result_artifact_hash=H2,
            scope_closure_digest=H3,
            child_set_hash=digests.child_set_digest(()),
            archive_reason=M5TerminalReason.SUBJECT_INACTIVE,
        )
    with pytest.raises(ValidationError):
        M5JobCompletion.build(
            job=root,
            terminal_state=M5JobState.COMPLETED_INACTIVE,
            result_artifact_id=H1,
            result_artifact_hash=H2,
            scope_closure_digest=digests.discovery_scope_closure_digest(
                scope.scope_contract_digest, ()
            ),
            child_set_hash=digests.child_set_digest(()),
            archive_reason=M5TerminalReason.CHUNK_INACTIVE,
        )


def test_attempt_output_and_activity_artifact_enforce_identity_and_shape() -> None:
    attempt = M5JobAttempt.build(
        logical_job_id=H1,
        attempt_ordinal=1,
        execution_spec_hash=H2,
        lease_token_hash=H3,
    )
    output = M5AttemptOutput.build(
        attempt=attempt,
        job_epoch_id=4,
        payload_hash=H4,
        result_artifact_id=H2,
        result_artifact_hash=H3,
    )
    artifact = M5AttemptResultArtifact.build(
        attempt_output=output,
        job_state_at_receipt=M5JobState.RUNNING,
        job_state_after=M5JobState.COMPLETED_ACTIVE,
        disposition=M5AttemptDisposition.VERIFIER_COMPLETED_ACTIVE,
        activity_snapshot_epoch_id=5,
        activity_snapshot_revision=2,
        epoch_active=True,
        chunk_active=True,
        requirement_active=True,
        group_active=True,
        archive_reason=None,
    )

    artifact.validate_job_shape(M5JobKind.VERIFY_REQUIREMENT_PAIR)
    with pytest.raises(ValidationError):
        artifact.validate_job_shape(M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL)
    with pytest.raises(ValidationError):
        M5JobAttempt.build(
            logical_job_id=H1,
            attempt_ordinal=0,
            execution_spec_hash=H2,
            lease_token_hash=H3,
        )
    with pytest.raises(ValidationError):
        replace(output, result_artifact_hash=H1)


def test_cancelled_late_attempt_requires_original_cancellation_attribution() -> None:
    output = M5AttemptOutput.build(
        attempt=M5JobAttempt.build(
            logical_job_id=H1,
            attempt_ordinal=2,
            execution_spec_hash=H2,
            lease_token_hash=H3,
        ),
        job_epoch_id=4,
        payload_hash=H4,
        result_artifact_id=H2,
        result_artifact_hash=H3,
    )
    late = M5AttemptResultArtifact.build(
        attempt_output=output,
        job_state_at_receipt=M5JobState.CANCELLED,
        job_state_after=M5JobState.CANCELLED,
        disposition=M5AttemptDisposition.TERMINAL_AUDIT_ONLY,
        activity_snapshot_epoch_id=9,
        activity_snapshot_revision=0,
        epoch_active=True,
        chunk_active=True,
        requirement_active=True,
        group_active=True,
        archive_reason=M5AttemptArchiveReason.JOB_ALREADY_TERMINAL,
        cancelled_by_event_id="replacement-event",
        cancelled_by_epoch_id=7,
        cancellation_reason=M5TerminalReason.SUBJECT_INACTIVE,
    )

    assert late.cancelled_by_epoch_id == 7
    with pytest.raises(ValidationError):
        replace(late, cancelled_by_event_id=None)


def test_activation_request_receipt_and_replay_identity() -> None:
    request = M5ActivationRequest.build(
        activation_id="activation-1",
        expected_mode_revision=4,
        expected_base_m4_epoch_id=7,
        core_schema_bundle_sha256=H1,
        bootstrap_state_hash=H2,
    )
    first = M5ActivationReceipt.build(request)
    replay = M5ActivationReceipt.build(request, replayed=True)

    first.validate_request(request)
    assert first.receipt_hash == replay.receipt_hash
    assert not first.replayed and replay.replayed
    with pytest.raises(ValidationError):
        replace(request, expected_m4_publication_id="wrong")
    with pytest.raises(ValidationError):
        replace(first, m5_publication_epoch_id=8)


def test_state_references_work_and_timing_are_byte_total() -> None:
    reference = M5ChangedStateReference.build(
        kind=M5StateReferenceKind.CLAIM_CERTIFICATE,
        object_id="claim-1",
        epoch_id=3,
        revision=1,
        state_artifact_hash=H1,
    )
    validate_changed_state_references((reference,))
    work = M5RuntimeWork(bytes_hashed=10, verifier_model_call_count=1)

    assert not work.is_zero
    assert M5RuntimeWork().is_zero
    assert work.work_digest != M5RuntimeWork().work_digest
    M5RuntimeTiming(postgres_server_execution_ns=None, end_to_end_wall_ns=2)
    with pytest.raises(ValidationError):
        M5RuntimeWork(bytes_hashed=True)
    with pytest.raises(ValidationError):
        replace(work, bytes_hashed=11)
    with pytest.raises(ValidationError):
        M5RuntimeTiming(neural_wall_ns=-1)


def test_owner_pending_counter_preserves_exact_multiplicity() -> None:
    counter = M5OwnerPendingCounter(
        epoch_id=3,
        owner_claim_id="claim-1",
        broad_reverse_scope_count=1,
        forward_scope_count=2,
        verifier_job_count=3,
        blocking_failure_count=4,
    )

    assert counter.pending_multiplicity == 10
    with pytest.raises(ValidationError):
        replace(counter, verifier_job_count=-1)
    with pytest.raises(ValidationError):
        replace(counter, verifier_job_count=True)


def _logical_hash(
    *,
    event_id: str,
    payload_hash: str,
    epoch_id: int,
    outcome: M5ReplayedOutcome,
    publication: PublicationReceipt | None,
    work: M5RuntimeWork,
    failure: M5RunFailureReason | None,
) -> str:
    return digests.event_run_logical_result_digest(
        event_id=event_id,
        payload_hash=payload_hash,
        epoch_id=epoch_id,
        sealed_or_failed_outcome=outcome,
        original_open_receipt_binding_hash=digests.open_event_receipt_binding_digest(
            epoch_id=epoch_id,
            replayed=False,
            already_sealed=False,
            publication_id=None,
            already_failed=False,
            failure_reason=None,
        ),
        original_publication_receipt_binding_hash=(
            digests.publication_receipt_binding_digest(
                epoch_id=publication.epoch_id,
                publication_id=publication.publication_id,
                replayed=False,
            )
            if publication is not None
            else None
        ),
        event_work_digest=work.work_digest,
        combined_status_delta_set_hash=digests.combined_status_delta_set_digest(()),
        changed_state_set_hash=digests.changed_state_set_digest(()),
        failure_reason=failure,
    )


def _c5_replayed_result(
    *,
    outcome: M5ReplayedOutcome,
    active_cutoff: bool,
    receipt_replayed: bool,
    call_work: M5RuntimeWork,
    with_coverage: bool = False,
) -> M5EventRunResult:
    epoch_id = 5
    publication_id = stable_m4_digest("m4-publication-v1", str(epoch_id))
    sealed = outcome is M5ReplayedOutcome.SEALED
    failure_reason = None if sealed else M5RunFailureReason.INVALID_ARTIFACT
    if active_cutoff:
        open_receipt = OpenEventReceipt(
            epoch_id=epoch_id,
            replayed=receipt_replayed,
            already_sealed=False,
        )
    elif sealed:
        open_receipt = OpenEventReceipt(
            epoch_id=epoch_id,
            replayed=True,
            already_sealed=True,
            publication_id=publication_id,
        )
    else:
        assert failure_reason is not None
        open_receipt = OpenEventReceipt(
            epoch_id=epoch_id,
            replayed=True,
            already_sealed=False,
            already_failed=True,
            failure_reason=failure_reason.value,
        )
    publication_receipt = (
        PublicationReceipt(epoch_id, publication_id, True) if sealed else None
    )
    event_timing = M5RuntimeTiming()
    call_timing = (
        M5RuntimeTiming(postgres_roundtrip_wall_ns=3, end_to_end_wall_ns=5)
        if with_coverage
        else M5RuntimeTiming()
    )
    event_coverage = (
        M5RuntimeTimingCoverage.single_point(
            None,
            terminal_client_roundtrip_included=False,
        )
        if with_coverage
        else None
    )
    call_coverage = (
        M5RuntimeTimingCoverage.single_point(
            call_timing,
            terminal_client_roundtrip_included=True,
        )
        if with_coverage
        else None
    )
    return M5EventRunResult.build(
        event_id="event",
        payload_hash=H1,
        epoch_id=epoch_id,
        state=M5RunState.REPLAYED,
        replayed_outcome=outcome,
        open_receipt=open_receipt,
        publication_receipt=publication_receipt,
        event_work=M5RuntimeWork(requirement_verifier_call_count=1),
        call_work=call_work,
        event_timing=event_timing,
        call_timing=call_timing,
        combined_deltas=(),
        changed_state_references=(),
        failure_reason=failure_reason,
        event_timing_coverage=event_coverage,
        call_timing_coverage=call_coverage,
    )


@pytest.mark.parametrize(
    "outcome",
    (M5ReplayedOutcome.SEALED, M5ReplayedOutcome.FAILED),
    ids=("sealed", "failed"),
)
def test_c5_ordinary_terminal_known_replay_requires_zero_call_work(
    outcome: M5ReplayedOutcome,
) -> None:
    replay = _c5_replayed_result(
        outcome=outcome,
        active_cutoff=False,
        receipt_replayed=True,
        call_work=M5RuntimeWork(),
    )

    assert replay.call_work.is_zero
    with pytest.raises(ValidationError):
        replace(replay, call_work=M5RuntimeWork(bytes_hashed=1))


@pytest.mark.parametrize(
    "outcome",
    (M5ReplayedOutcome.SEALED, M5ReplayedOutcome.FAILED),
    ids=("sealed", "failed"),
)
@pytest.mark.parametrize(
    "receipt_replayed",
    (False, True),
    ids=("fresh-nonterminal-open", "resumed-nonterminal-open"),
)
@pytest.mark.parametrize(
    "call_work",
    (
        M5RuntimeWork(),
        M5RuntimeWork(
            requirement_verifier_call_count=1,
            verifier_model_call_count=1,
            verifier_input_token_count=7,
            verifier_output_token_count=2,
            bytes_hashed=11,
            bytes_serialized=13,
        ),
    ),
    ids=("zero-call-work", "nonzero-call-work"),
)
@pytest.mark.parametrize(
    "with_coverage",
    (False, True),
    ids=("coverage-jointly-absent", "coverage-jointly-present"),
)
def test_c5_active_cutoff_replay_matrix_preserves_logical_identity(
    outcome: M5ReplayedOutcome,
    receipt_replayed: bool,
    call_work: M5RuntimeWork,
    with_coverage: bool,
) -> None:
    canonical = _c5_replayed_result(
        outcome=outcome,
        active_cutoff=False,
        receipt_replayed=True,
        call_work=M5RuntimeWork(),
        with_coverage=with_coverage,
    )
    projection = _c5_replayed_result(
        outcome=outcome,
        active_cutoff=True,
        receipt_replayed=receipt_replayed,
        call_work=call_work,
        with_coverage=with_coverage,
    )

    assert projection.state is M5RunState.REPLAYED
    assert projection.open_receipt.replayed is receipt_replayed
    assert not projection.open_receipt.already_sealed
    assert not projection.open_receipt.already_failed
    assert projection.call_work == call_work
    assert projection.logical_result_hash == canonical.logical_result_hash
    assert projection.event_work == canonical.event_work
    assert projection.publication_receipt == canonical.publication_receipt
    assert projection.failure_reason is canonical.failure_reason


def test_c5_replay_rejects_mixed_publication_failure_and_receipt_shapes() -> None:
    sealed = _c5_replayed_result(
        outcome=M5ReplayedOutcome.SEALED,
        active_cutoff=True,
        receipt_replayed=False,
        call_work=M5RuntimeWork(bytes_hashed=1),
    )
    failed = _c5_replayed_result(
        outcome=M5ReplayedOutcome.FAILED,
        active_cutoff=True,
        receipt_replayed=True,
        call_work=M5RuntimeWork(),
    )
    publication_id = stable_m4_digest("m4-publication-v1", "5")

    invalid_sealed_changes = (
        {
            "publication_receipt": None,
        },
        {
            "publication_receipt": PublicationReceipt(5, publication_id, False),
        },
        {
            "failure_reason": M5RunFailureReason.INVALID_ARTIFACT,
        },
        {
            "open_receipt": OpenEventReceipt(
                5,
                True,
                False,
                None,
                True,
                M5RunFailureReason.INVALID_ARTIFACT.value,
            ),
        },
    )
    for changes in invalid_sealed_changes:
        with pytest.raises(ValidationError):
            replace(sealed, **changes)

    invalid_failed_changes = (
        {
            "publication_receipt": PublicationReceipt(5, publication_id, True),
        },
        {
            "failure_reason": None,
        },
        {
            "open_receipt": OpenEventReceipt(
                5,
                True,
                True,
                publication_id,
            ),
        },
        {
            "open_receipt": OpenEventReceipt(
                5,
                True,
                False,
                None,
                True,
                M5RunFailureReason.RETRIEVAL_ERROR.value,
            ),
        },
    )
    for changes in invalid_failed_changes:
        with pytest.raises(ValidationError):
            replace(failed, **changes)

    with pytest.raises(ValidationError):
        replace(sealed, open_receipt=OpenEventReceipt(6, False, False))
    with pytest.raises(ValidationError):
        replace(
            sealed,
            open_receipt=OpenEventReceipt(
                5,
                True,
                True,
                stable_m4_digest("m4-publication-v1", "6"),
            ),
        )
    with pytest.raises(ValidationError):
        OpenEventReceipt(5, False, False, publication_id)
    with pytest.raises(ValidationError):
        OpenEventReceipt(
            5,
            False,
            False,
            None,
            False,
            M5RunFailureReason.INVALID_ARTIFACT.value,
        )


@pytest.mark.parametrize(
    ("replayed", "already_sealed", "already_failed"),
    (
        pytest.param(0, False, False, id="active-replayed-int-zero"),
        pytest.param(1, False, False, id="active-replayed-int-one"),
        pytest.param(False, 0, False, id="active-sealed-flag-int-zero"),
        pytest.param(False, False, 0, id="active-failed-flag-int-zero"),
        pytest.param(True, 1, False, id="ordinary-sealed-flag-int-one"),
    ),
)
def test_c5_replay_rejects_boolean_impostor_receipt_flags(
    replayed: object,
    already_sealed: object,
    already_failed: object,
) -> None:
    sealed = _c5_replayed_result(
        outcome=M5ReplayedOutcome.SEALED,
        active_cutoff=True,
        receipt_replayed=False,
        call_work=M5RuntimeWork(),
    )
    publication_id = stable_m4_digest("m4-publication-v1", "5")
    receipt_publication_id = publication_id if already_sealed == 1 else None
    malformed = OpenEventReceipt(
        epoch_id=5,
        replayed=replayed,  # type: ignore[arg-type]
        already_sealed=already_sealed,  # type: ignore[arg-type]
        publication_id=receipt_publication_id,
        already_failed=already_failed,  # type: ignore[arg-type]
    )

    with pytest.raises(ValidationError):
        replace(sealed, open_receipt=malformed)

    with pytest.raises(ValidationError):
        replace(
            sealed,
            publication_receipt=PublicationReceipt(
                5,
                publication_id,
                1,  # type: ignore[arg-type]
            ),
        )


def test_c5_active_cutoff_replay_keeps_exact_terminal_timing_coverage_rules() -> None:
    replay = _c5_replayed_result(
        outcome=M5ReplayedOutcome.SEALED,
        active_cutoff=True,
        receipt_replayed=True,
        call_work=M5RuntimeWork(bytes_hashed=1),
        with_coverage=True,
    )
    assert replay.event_timing_coverage is not None
    assert replay.call_timing_coverage is not None

    with pytest.raises(ValidationError):
        replace(replay, event_timing_coverage=None)
    with pytest.raises(ValidationError):
        replace(replay, call_timing_coverage=None)
    with pytest.raises(ValidationError):
        replace(
            replay,
            event_timing=M5RuntimeTiming(end_to_end_wall_ns=1),
        )
    with pytest.raises(ValidationError):
        replace(
            replay,
            call_timing_coverage=M5RuntimeTimingCoverage.single_point(
                replay.call_timing,
                terminal_client_roundtrip_included=False,
            ),
        )
    with pytest.raises(ValidationError):
        replace(
            replay,
            event_timing=M5RuntimeTiming(),
            event_timing_coverage=M5RuntimeTimingCoverage.single_point(
                M5RuntimeTiming(),
                terminal_client_roundtrip_included=True,
            ),
        )


@pytest.mark.parametrize(
    "outcome",
    (M5ReplayedOutcome.SEALED, M5ReplayedOutcome.FAILED),
    ids=("sealed", "failed"),
)
@pytest.mark.parametrize(
    "receipt_replayed",
    (False, True),
    ids=("fresh-nonterminal-open", "resumed-nonterminal-open"),
)
def test_c5_active_cutoff_replay_accepts_exact_missing_call_coverage(
    outcome: M5ReplayedOutcome,
    receipt_replayed: bool,
) -> None:
    replay = _c5_replayed_result(
        outcome=outcome,
        active_cutoff=True,
        receipt_replayed=receipt_replayed,
        call_work=M5RuntimeWork(bytes_hashed=1),
    )
    missing = M5RuntimeTimingCoverage.single_point(
        None,
        terminal_client_roundtrip_included=False,
    )
    covered = replace(
        replay,
        event_timing_coverage=missing,
        call_timing_coverage=missing,
    )

    assert covered.call_timing_coverage is not None
    assert covered.call_timing_coverage.required_expected_count == 1
    assert covered.call_timing_coverage.required_missing_count == 1
    assert not covered.call_timing_coverage.terminal_client_roundtrip_included
    with pytest.raises(ValidationError):
        replace(
            covered,
            call_timing_coverage=M5RuntimeTimingCoverage.single_point(
                M5RuntimeTiming(),
                terminal_client_roundtrip_included=False,
            ),
        )


def test_run_result_sealed_replay_has_stable_logical_hash_and_zero_call_work() -> None:
    work = M5RuntimeWork(requirement_verifier_call_count=1)
    publication_id = stable_publication_id = stable_m4_digest("m4-publication-v1", "5")
    publication = PublicationReceipt(5, publication_id, False)
    logical_hash = _logical_hash(
        event_id="event",
        payload_hash=H1,
        epoch_id=5,
        outcome=M5ReplayedOutcome.SEALED,
        publication=publication,
        work=work,
        failure=None,
    )
    sealed = M5EventRunResult.build(
        event_id="event",
        payload_hash=H1,
        epoch_id=5,
        state=M5RunState.SEALED,
        replayed_outcome=None,
        open_receipt=OpenEventReceipt(5, False, False),
        publication_receipt=publication,
        event_work=work,
        call_work=work,
        event_timing=M5RuntimeTiming(),
        call_timing=M5RuntimeTiming(),
        combined_deltas=(),
        changed_state_references=(),
        failure_reason=None,
    )
    replay = M5EventRunResult(
        "event",
        H1,
        5,
        M5RunState.REPLAYED,
        M5ReplayedOutcome.SEALED,
        OpenEventReceipt(5, True, True, stable_publication_id),
        PublicationReceipt(5, stable_publication_id, True),
        work,
        M5RuntimeWork(),
        M5RuntimeTiming(),
        M5RuntimeTiming(postgres_roundtrip_wall_ns=1),
        (),
        (),
        None,
        logical_hash,
    )

    assert sealed.logical_result_hash == replay.logical_result_hash
    with pytest.raises(ValidationError):
        replace(replay, call_work=M5RuntimeWork(bytes_hashed=1))


def test_run_result_failed_blocked_and_invalid_shapes() -> None:
    work = M5RuntimeWork(requirement_verifier_call_count=1)
    failure = M5RunFailureReason.INVALID_ARTIFACT
    logical_hash = _logical_hash(
        event_id="event",
        payload_hash=H1,
        epoch_id=5,
        outcome=M5ReplayedOutcome.FAILED,
        publication=None,
        work=work,
        failure=failure,
    )
    failed = M5EventRunResult(
        "event",
        H1,
        5,
        M5RunState.FAILED,
        None,
        OpenEventReceipt(5, False, False),
        None,
        work,
        work,
        M5RuntimeTiming(),
        M5RuntimeTiming(),
        (),
        (),
        failure,
        logical_hash,
    )
    blocked = M5EventRunResult(
        "event",
        H1,
        5,
        M5RunState.BLOCKED,
        None,
        OpenEventReceipt(5, False, False),
        None,
        work,
        work,
        M5RuntimeTiming(),
        M5RuntimeTiming(),
        (),
        (),
        M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
        None,
    )

    assert failed.logical_result_hash == logical_hash
    assert blocked.logical_result_hash is None
    replay = M5EventRunResult(
        "event",
        H1,
        5,
        M5RunState.REPLAYED,
        M5ReplayedOutcome.FAILED,
        OpenEventReceipt(
            5,
            True,
            False,
            None,
            True,
            M5RunFailureReason.INVALID_ARTIFACT.value,
        ),
        None,
        work,
        M5RuntimeWork(),
        M5RuntimeTiming(),
        M5RuntimeTiming(postgres_roundtrip_wall_ns=1),
        (),
        (),
        failure,
        logical_hash,
    )
    assert replay.logical_result_hash == failed.logical_result_hash
    with pytest.raises(ValidationError):
        replace(blocked, failure_reason=M5RunFailureReason.INVARIANT_FAILURE)
    with pytest.raises(ValidationError):
        replace(
            replay,
            open_receipt=replace(
                replay.open_receipt,
                failure_reason=M5RunFailureReason.RETRIEVAL_ERROR.value,
            ),
        )


def test_typed_event_plan_binds_legacy_payload_direct_plan_and_predecessor() -> None:
    manifest = make_manifest()
    requirements, chunks = make_snapshots()
    event = InsertDocumentEvent(
        event_id="event",
        document_id="document",
        document_version_id="document-version",
        content_hash=H1,
        chunks=(ChunkInput("new-chunk", 0, "text"),),
    )
    payload = legacy_event_payload_digest(event)
    update = CorpusUpdateIdentity(
        event_id="event",
        payload_hash=payload,
        update_kind=UpdateKind.INSERT,
        previous_published_epoch_id=5,
        candidate_policy_id=manifest.candidate_policy_id,
    )
    direct = DynamicEventPlan(
        update=update,
        inserted_chunk_version_ids=("new-chunk",),
        deactivated_chunk_version_ids=(),
        registered_claim_ids=(),
        claim_registry_snapshot_id="claims",
    )
    plan = M5TypedEventPlan(
        "event",
        event,
        payload,
        direct,
        manifest.candidate_policy_id,
        manifest.manifest_hash,
        requirements,
        chunks,
        5,
    )

    assert plan.direct_plan is direct
    with pytest.raises(ValidationError):
        replace(plan, direct_plan=None)
    with pytest.raises(ValidationError):
        replace(plan, expected_previous_published_epoch_id=6)


def test_job_lease_replay_cannot_redispatch() -> None:
    attempt = M5JobAttempt.build(
        logical_job_id=H1,
        attempt_ordinal=1,
        execution_spec_hash=H2,
        lease_token_hash=H3,
    )

    M5JobLease(H1, attempt, 2, True, False)
    M5JobLease(H1, attempt, 2, False, True)
    with pytest.raises(ValidationError):
        M5JobLease(H1, attempt, 2, True, True)


@pytest.mark.parametrize(
    "reason",
    (
        M5TerminalReason.SUBJECT_INACTIVE,
        M5TerminalReason.SCOPE_RETIRED,
        M5TerminalReason.EPOCH_FAILED,
    ),
)
def test_cancellation_plan_accepts_exact_frozen_reasons(
    reason: M5TerminalReason,
) -> None:
    plan = M5CancellationPlan.build(
        structural_event_id="event-λ",
        epoch_id=7,
        cancelled_job_ids=(H1, H2),
        reason=reason,
    )

    assert plan == M5CancellationPlan(
        structural_event_id="event-λ",
        epoch_id=7,
        cancelled_job_ids=(H1, H2),
        reason=reason,
        plan_digest=digests.cancellation_plan_digest(
            structural_event_id="event-λ",
            epoch_id=7,
            cancelled_job_ids=(H1, H2),
            reason=reason,
        ),
    )


@pytest.mark.parametrize(
    "reason",
    tuple(
        reason
        for reason in M5TerminalReason
        if reason
        not in {
            M5TerminalReason.SUBJECT_INACTIVE,
            M5TerminalReason.SCOPE_RETIRED,
            M5TerminalReason.EPOCH_FAILED,
        }
    ),
)
def test_cancellation_plan_rejects_every_other_terminal_reason(
    reason: M5TerminalReason,
) -> None:
    with pytest.raises(ValidationError):
        M5CancellationPlan.build(
            structural_event_id="event",
            epoch_id=7,
            cancelled_job_ids=(H1,),
            reason=reason,
        )


def test_cancellation_plan_rejects_empty_noncanonical_or_invalid_shape() -> None:
    with pytest.raises(ValidationError):
        M5CancellationPlan.build(
            structural_event_id="event",
            epoch_id=7,
            cancelled_job_ids=(),
            reason=M5TerminalReason.SUBJECT_INACTIVE,
        )
    with pytest.raises(ValidationError):
        M5CancellationPlan.build(
            structural_event_id="event",
            epoch_id=7,
            cancelled_job_ids=(H2, H1),
            reason=M5TerminalReason.SUBJECT_INACTIVE,
        )
    with pytest.raises(ValidationError):
        M5CancellationPlan.build(
            structural_event_id="event",
            epoch_id=7,
            cancelled_job_ids=(H1, H1),
            reason=M5TerminalReason.SUBJECT_INACTIVE,
        )
    with pytest.raises(ValidationError):
        M5CancellationPlan.build(
            structural_event_id="event",
            epoch_id=7,
            cancelled_job_ids=[H1],  # type: ignore[arg-type]
            reason=M5TerminalReason.SUBJECT_INACTIVE,
        )
    with pytest.raises(ValidationError):
        M5CancellationPlan.build(
            structural_event_id="event",
            epoch_id=7,
            cancelled_job_ids=("A" * 64,),
            reason=M5TerminalReason.SUBJECT_INACTIVE,
        )
    with pytest.raises(ValidationError):
        M5CancellationPlan.build(
            structural_event_id="",
            epoch_id=7,
            cancelled_job_ids=(H1,),
            reason=M5TerminalReason.SUBJECT_INACTIVE,
        )
    with pytest.raises(ValidationError):
        M5CancellationPlan.build(
            structural_event_id="event",
            epoch_id=0,
            cancelled_job_ids=(H1,),
            reason=M5TerminalReason.SUBJECT_INACTIVE,
        )
    with pytest.raises(ValidationError):
        M5CancellationPlan.build(
            structural_event_id="event",
            epoch_id=True,
            cancelled_job_ids=(H1,),
            reason=M5TerminalReason.SUBJECT_INACTIVE,
        )
    with pytest.raises(ValidationError):
        M5CancellationPlan.build(
            structural_event_id="event",
            epoch_id=7,
            cancelled_job_ids=(H1,),
            reason="subject_inactive",  # type: ignore[arg-type]
        )


def test_cancellation_plan_rejects_every_stale_one_field_identity() -> None:
    plan = M5CancellationPlan.build(
        structural_event_id="event",
        epoch_id=7,
        cancelled_job_ids=(H1, H2),
        reason=M5TerminalReason.SUBJECT_INACTIVE,
    )

    with pytest.raises(ValidationError):
        replace(plan, structural_event_id="another-event")
    with pytest.raises(ValidationError):
        replace(plan, epoch_id=8)
    with pytest.raises(ValidationError):
        replace(plan, cancelled_job_ids=(H1, H3))
    with pytest.raises(ValidationError):
        replace(plan, reason=M5TerminalReason.SCOPE_RETIRED)
    with pytest.raises(ValidationError):
        replace(plan, plan_digest=H4)


def test_retry_failure_and_cancellation_use_existing_receipt_shapes() -> None:
    fresh_retry = M5AttemptCompletionReceipt(H1, H2, 8, False)
    replayed_retry = M5AttemptCompletionReceipt(H1, H2, 8, True)
    fresh_cancellation = M5CancellationReceipt((H1, H2), 9, False)
    replayed_cancellation = M5CancellationReceipt((H1, H2), 9, True)

    assert fresh_retry.exact_replay is False
    assert replayed_retry.exact_replay is True
    assert fresh_cancellation.cancelled_job_ids == (H1, H2)
    assert replayed_cancellation.exact_replay is True

    with pytest.raises(ValidationError):
        M5AttemptCompletionReceipt("not-a-hash", H2, 8, False)
    with pytest.raises(ValidationError):
        M5AttemptCompletionReceipt(H1, "not-a-hash", 8, False)
    with pytest.raises(ValidationError):
        M5AttemptCompletionReceipt(H1, H2, 0, False)
    with pytest.raises(ValidationError):
        M5AttemptCompletionReceipt(H1, H2, 8, 1)  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        M5CancellationReceipt((H2, H1), 9, False)
    with pytest.raises(ValidationError):
        M5CancellationReceipt((H1, H1), 9, False)


def test_d24_wire_enums_are_total_and_do_not_relabel_prior_values() -> None:
    assert tuple(item.value for item in M5AcquisitionDisposition) == (
        "dispatch_new",
        "dispatch_takeover",
        "live_lease",
        "result_reserved",
        "terminal",
    )
    assert tuple(item.value for item in M5RuntimeSubgraph) == (
        "direct",
        "requirement",
    )
    assert tuple(item.value for item in M5ExecutionEvidenceDisposition) == (
        "returned",
        "reused_artifact",
        "retryable_failure",
        "terminal_failure",
    )
    assert tuple(item.value for item in M5TypedDirectReturnKind) == (
        "discovery",
        "verifier",
    )
    assert tuple(item.value for item in M5TypedDirectScopeKind) == (
        "all_registered_claims",
        "explicit_claims",
    )
    assert tuple(item.value for item in M5RuntimeWorkContributionKind) == (
        "structural_open",
        "m5_acquisition",
        "direct_acquisition",
        "m5_attempt_execution",
        "direct_attempt_execution",
        "root_result_stage",
        "root_barrier",
        "verifier_completion",
        "cancellation",
        "terminal_job_failure",
        "direct_transition",
        "preterminal_late_return",
        "epoch_failure",
        "seal",
    )
    assert M5AttemptArchiveReason.ATTEMPT_EXPIRED.value == "attempt_expired"
    assert M5RunFailureReason.WORK_IN_PROGRESS.value == "work_in_progress"


def test_d24_public_dto_field_topology_is_exact() -> None:
    expected_topology = {
        M5RuntimeOperationalConfig: ("lease_duration_ms", "config_digest"),
        M5LeaseTerminalProjection: (
            "terminal_state",
            "terminal_reason",
            "completion_digest",
            "terminal_identity_hash",
        ),
        M5JobAttempt: (
            "attempt_id",
            "logical_job_id",
            "attempt_ordinal",
            "execution_spec_hash",
            "lease_token_hash",
            "lease_expires_at",
            "attempt_work_digest",
        ),
        M5JobLease: (
            "logical_job_id",
            "attempt",
            "resulting_revision",
            "should_execute",
            "exact_replay",
            "lease_expires_at",
            "dispatch_record_digest",
            "disposition",
            "terminal_projection",
        ),
        M5TypedDirectTerminalProjection: (
            "terminal_state",
            "terminal_reason",
            "m4_completion_digest",
            "completed_revision",
            "terminal_identity_hash",
        ),
        M5TypedDirectJobLease: (
            "job_id",
            "attempt_id",
            "lease_token_hash",
            "lease_expires_at",
            "dispatch_record_digest",
            "resulting_revision",
            "disposition",
            "should_execute",
            "exact_replay",
            "already_completed",
            "terminal_projection",
        ),
        M5DispatchRecord: (
            "epoch_id",
            "subgraph",
            "attempt_id",
            "logical_job_id",
            "attempt_ordinal",
            "job_kind",
            "fallback_required",
            "dispatched_revision",
            "lease_expires_at",
            "maximum_ambiguous_call_work",
            "record_digest",
        ),
        M5RequirementRootProvenance: (
            "epoch_id",
            "root_job_id",
            "fallback_required",
            "provenance_digest",
        ),
        M5AttemptExecutionEvidence: (
            "epoch_id",
            "subgraph",
            "attempt_id",
            "disposition",
            "result_or_error_hash",
            "attempt_work",
            "attempt_timing_digest",
            "evidence_digest",
        ),
        M5CallAmbiguityReport: (
            "durable_dispatch_count",
            "confirmed_execution_count",
            "unresolved_dispatch_count",
            "confirmed_call_lower",
            "possible_call_upper",
        ),
        M5ExpiredAttemptReturn: (
            "subgraph",
            "epoch_id",
            "attempt_id",
            "logical_job_id",
            "worker_output_digest",
            "worker_artifact_hash",
            "activity_snapshot_epoch_id",
            "activity_snapshot_revision",
            "received_after_terminal",
            "expired_return_digest",
        ),
        M5RuntimeTimingObservation: (
            "required_interval_observed",
            "timing",
            "observation_digest",
        ),
        M5RuntimeTiming: (
            "coordinator_non_db_non_neural_ns",
            "neural_wall_ns",
            "postgres_roundtrip_wall_ns",
            "external_io_wall_ns",
            "end_to_end_wall_ns",
            "postgres_server_execution_ns",
            "postgres_lock_wait_ns",
            "postgres_wal_bytes",
            "postgres_shared_block_reads",
        ),
        M5RuntimeTimingCoverage: (
            "required_expected_count",
            "required_observed_count",
            "required_missing_count",
            "postgres_server_execution_expected_count",
            "postgres_server_execution_observed_count",
            "postgres_server_execution_missing_count",
            "postgres_lock_wait_expected_count",
            "postgres_lock_wait_observed_count",
            "postgres_lock_wait_missing_count",
            "postgres_wal_bytes_expected_count",
            "postgres_wal_bytes_observed_count",
            "postgres_wal_bytes_missing_count",
            "postgres_shared_block_reads_expected_count",
            "postgres_shared_block_reads_observed_count",
            "postgres_shared_block_reads_missing_count",
            "terminal_client_roundtrip_included",
        ),
        M5TransitionTimingAnchor: (
            "epoch_id",
            "contribution_kind",
            "source_id",
            "contribution_key_digest",
            "anchor_revision",
            "terminal_transition",
        ),
        M5TransitionTimingReceipt: (
            "anchor",
            "transition_timing_digest",
            "event_timing",
            "event_timing_coverage",
            "resulting_revision",
            "exact_replay",
        ),
        M5TypedDirectVerificationExecution: (
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
        ),
        M5TypedDirectLateReturnEnvelope: (
            "epoch_id",
            "return_kind",
            "job_id",
            "attempt_id",
            "result_artifact_id",
            "result_artifact_hash",
            "verification_execution_present",
            "observation_eligible_for_currency",
            "requested_make_effective",
            "job",
            "attempt",
            "completion",
            "discovery",
            "scope",
            "persisted_scope_kind",
            "explicit_claim_ids",
            "closed_revision",
            "verification_execution",
            "observation",
            "observation_produced_epoch",
            "observation_raw_output_hash",
            "job_binding_digest",
            "attempt_binding_digest",
            "completion_binding_digest",
            "discovery_binding_digest",
            "scope_binding_digest",
            "verifier_binding_digest",
            "envelope_digest",
        ),
        M5EventRunResult: (
            "event_id",
            "payload_hash",
            "epoch_id",
            "state",
            "replayed_outcome",
            "open_receipt",
            "publication_receipt",
            "event_work",
            "call_work",
            "event_timing",
            "call_timing",
            "combined_deltas",
            "changed_state_references",
            "failure_reason",
            "logical_result_hash",
            "event_timing_coverage",
            "call_timing_coverage",
        ),
    }
    for dto, expected_fields in expected_topology.items():
        assert tuple(field.name for field in fields(dto)) == expected_fields


def test_d24_public_m4_dto_and_api_signature_snapshot_is_unchanged() -> None:
    expected_topology = {
        M4PublicJobLease: (
            "job_id",
            "should_execute",
            "already_completed",
            "attempt_id",
            "lease_token_hash",
            "expected_revision",
        ),
        M4LogicalJobSpec: (
            "job_id",
            "event_id",
            "kind",
            "candidate_policy_id",
            "payload_hash",
            "execution_spec_hash",
            "parent_job_id",
            "pair",
            "target_claim_id",
            "target_chunk_version_id",
            "expandable",
        ),
        M4JobAttempt: (
            "attempt_id",
            "job_id",
            "execution_spec_hash",
            "attempt_ordinal",
            "lease_token_hash",
        ),
        M4JobCompletion: (
            "job_id",
            "payload_hash",
            "execution_spec_hash",
            "result_artifact_id",
            "result_artifact_hash",
            "terminal_state",
            "completion_digest",
            "child_closure",
        ),
        M4ChildClosure: (
            "parent_job_id",
            "completion_digest",
            "child_job_ids",
            "child_set_hash",
        ),
        M4DiscoveryResult: (
            "root_job_id",
            "result_artifact_id",
            "result_artifact_hash",
            "admitted_pairs",
            "fallback_satisfied",
            "channel_hits",
        ),
        M4DiscoveryScope: (
            "root_job_id",
            "registry_snapshot_id",
            "registered_claim_ids",
            "closed",
        ),
        M4ChannelHit: (
            "epoch_id",
            "pair",
            "candidate_policy_id",
            "channel",
            "rank",
            "score",
            "channel_artifact_hash",
        ),
        M4AdmittedPair: (
            "epoch_id",
            "pair",
            "candidate_policy_id",
            "fused_rank",
            "reasons",
            "mandatory_lineage",
        ),
        SemanticObservation: (
            "observation_id",
            "subject_kind",
            "subject_id",
            "chunk_version_id",
            "task_type",
            "support_score",
            "refute_score",
            "neutral_score",
            "producer",
            "input_hash",
        ),
    }
    for dto, expected_fields in expected_topology.items():
        assert tuple(field.name for field in fields(dto)) == expected_fields

    assert str(signature(M4RuntimeTransitionPort.acquire_job)) == (
        "(self, epoch_id: 'int', spec: 'LogicalJobSpec') -> 'JobLease'"
    )
    assert str(signature(M4RuntimeTransitionPort.complete_expansion)) == (
        "(self, epoch_id: 'int', lease: 'JobLease', discovery: 'DiscoveryResult', "
        "completion: 'JobCompletion', child_jobs: 'tuple[LogicalJobSpec, ...]') "
        "-> 'None'"
    )
    assert str(
        signature(M4ObservationApplicationPort.complete_verifier_atomically)
    ) == (
        "(self, epoch_id: 'int', lease: 'JobLease', verifier_job: "
        "'LogicalJobSpec', completion: 'JobCompletion', observation: "
        "'SemanticObservation', *, make_effective: 'bool') -> "
        "'ObservationCompletionReceipt'"
    )


def test_d24_operational_config_and_legacy_attempt_bridge_are_explicit() -> None:
    config = M5RuntimeOperationalConfig.build(30_000)
    assert config.config_digest == digests.runtime_operational_config_digest(30_000)
    with pytest.raises(ValidationError):
        M5RuntimeOperationalConfig.build(0)
    with pytest.raises(ValidationError):
        M5RuntimeOperationalConfig.build(86_400_001)
    with pytest.raises(ValidationError):
        replace(config, lease_duration_ms=30_001)
    provenance = M5RequirementRootProvenance.build(
        epoch_id=7, root_job_id=H1, fallback_required=True
    )
    assert provenance.provenance_digest == digests.requirement_root_provenance_digest(
        epoch_id=7, root_job_id=H1, fallback_required=True
    )
    with pytest.raises(ValidationError):
        replace(provenance, fallback_required=False)

    legacy = M5JobAttempt.build(
        logical_job_id=H1,
        attempt_ordinal=1,
        execution_spec_hash=H2,
        lease_token_hash=H3,
    )
    operational = M5JobAttempt.build(
        logical_job_id=H1,
        attempt_ordinal=1,
        execution_spec_hash=H2,
        lease_token_hash=H3,
        lease_expires_at=D24_DEADLINE,
        attempt_work_digest=M5RuntimeWork().work_digest,
    )
    assert not legacy.has_operational_lease
    assert operational.has_operational_lease
    assert operational.attempt_id == legacy.attempt_id
    with pytest.raises(ValidationError):
        replace(legacy, lease_expires_at=D24_DEADLINE)
    with pytest.raises(ValidationError):
        replace(legacy, attempt_work_digest=M5RuntimeWork().work_digest)
    with pytest.raises(ValidationError):
        replace(operational, lease_expires_at=D24_DEADLINE.replace(tzinfo=None))


def test_d24_requirement_lease_dispositions_enforce_the_total_shape() -> None:
    attempt = M5JobAttempt.build(
        logical_job_id=H1,
        attempt_ordinal=1,
        execution_spec_hash=H2,
        lease_token_hash=H3,
        lease_expires_at=D24_DEADLINE,
        attempt_work_digest=M5RuntimeWork().work_digest,
    )
    dispatch = M5DispatchRecord.build(
        epoch_id=7,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=attempt.attempt_id,
        logical_job_id=attempt.logical_job_id,
        attempt_ordinal=attempt.attempt_ordinal,
        job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL.value,
        fallback_required=True,
        dispatched_revision=2,
        lease_expires_at=D24_DEADLINE,
    )
    for disposition in (
        M5AcquisitionDisposition.DISPATCH_NEW,
        M5AcquisitionDisposition.DISPATCH_TAKEOVER,
    ):
        M5JobLease(
            H1,
            attempt,
            2,
            True,
            False,
            D24_DEADLINE,
            dispatch.record_digest,
            disposition,
        )
    for disposition in (
        M5AcquisitionDisposition.LIVE_LEASE,
        M5AcquisitionDisposition.RESULT_RESERVED,
    ):
        M5JobLease(
            H1,
            attempt,
            2,
            False,
            True,
            D24_DEADLINE,
            dispatch.record_digest,
            disposition,
        )
    settled_attempt = replace(attempt, attempt_work_digest=H4)
    M5JobLease(
        H1,
        settled_attempt,
        2,
        False,
        True,
        D24_DEADLINE,
        dispatch.record_digest,
        M5AcquisitionDisposition.RESULT_RESERVED,
    )
    for disposition, should_execute, exact_replay in (
        (M5AcquisitionDisposition.DISPATCH_NEW, True, False),
        (M5AcquisitionDisposition.DISPATCH_TAKEOVER, True, False),
        (M5AcquisitionDisposition.LIVE_LEASE, False, True),
    ):
        with pytest.raises(ValidationError):
            M5JobLease(
                H1,
                settled_attempt,
                2,
                should_execute,
                exact_replay,
                D24_DEADLINE,
                dispatch.record_digest,
                disposition,
            )
    with pytest.raises(ValidationError):
        M5JobLease(
            H1,
            settled_attempt,
            2,
            True,
            False,
            D24_DEADLINE,
            dispatch.record_digest,
            M5AcquisitionDisposition.RESULT_RESERVED,
        )
    projection = M5LeaseTerminalProjection.build(
        logical_job_id=H1,
        terminal_state=M5JobState.CANCELLED,
        terminal_reason=M5TerminalReason.EPOCH_FAILED,
        completion_digest=H4,
    )
    M5JobLease(
        H1,
        attempt,
        3,
        False,
        True,
        D24_DEADLINE,
        dispatch.record_digest,
        M5AcquisitionDisposition.TERMINAL,
        projection,
    )
    M5JobLease(
        H1,
        None,
        3,
        False,
        True,
        None,
        None,
        M5AcquisitionDisposition.TERMINAL,
        projection,
    )

    legacy = M5JobAttempt.build(
        logical_job_id=H1,
        attempt_ordinal=1,
        execution_spec_hash=H2,
        lease_token_hash=H3,
    )
    M5JobLease(H1, legacy, 2, True, False)
    with pytest.raises(ValidationError):
        M5JobLease(
            H1,
            legacy,
            2,
            True,
            False,
            None,
            dispatch.record_digest,
            M5AcquisitionDisposition.DISPATCH_NEW,
        )
    with pytest.raises(ValidationError):
        M5JobLease(
            H1,
            attempt,
            2,
            False,
            True,
            D24_DEADLINE,
            None,
            M5AcquisitionDisposition.TERMINAL,
            projection,
        )
    with pytest.raises(ValidationError):
        M5JobLease(
            H1,
            attempt,
            2,
            False,
            True,
            D24_DEADLINE + timedelta(seconds=1),
            dispatch.record_digest,
            M5AcquisitionDisposition.LIVE_LEASE,
        )
    with pytest.raises(ValidationError):
        replace(projection, terminal_identity_hash=H1).validate_job(H1)
    for terminal_state, terminal_reason in (
        (M5JobState.COMPLETED_INACTIVE, M5TerminalReason.RETRY_EXHAUSTED),
        (M5JobState.TERMINAL_FAILED, M5TerminalReason.SUBJECT_INACTIVE),
        (M5JobState.CANCELLED, M5TerminalReason.VERIFIER_ERROR),
    ):
        with pytest.raises(ValidationError):
            M5LeaseTerminalProjection.build(
                logical_job_id=H1,
                terminal_state=terminal_state,
                terminal_reason=terminal_reason,
                completion_digest=H4,
            )


def test_d24_typed_direct_lease_projection_is_total() -> None:
    active_projection = M5TypedDirectTerminalProjection.build(
        job_id="direct-job",
        terminal_state=M4JobState.COMPLETED_ACTIVE,
        terminal_reason=None,
        m4_completion_digest=H1,
        completed_revision=5,
    )
    M5TypedDirectJobLease(
        job_id="direct-job",
        attempt_id=None,
        lease_token_hash=None,
        lease_expires_at=None,
        dispatch_record_digest=None,
        resulting_revision=5,
        disposition=M5AcquisitionDisposition.TERMINAL,
        should_execute=False,
        exact_replay=True,
        already_completed=True,
        terminal_projection=active_projection,
    )
    with pytest.raises(ValidationError):
        M5TypedDirectJobLease(
            job_id="direct-job",
            attempt_id=None,
            lease_token_hash=None,
            lease_expires_at=None,
            dispatch_record_digest=None,
            resulting_revision=4,
            disposition=M5AcquisitionDisposition.TERMINAL,
            should_execute=False,
            exact_replay=True,
            already_completed=True,
            terminal_projection=active_projection,
        )
    for disposition, should_execute, exact_replay in (
        (M5AcquisitionDisposition.DISPATCH_NEW, True, False),
        (M5AcquisitionDisposition.DISPATCH_TAKEOVER, True, False),
        (M5AcquisitionDisposition.LIVE_LEASE, False, True),
    ):
        M5TypedDirectJobLease(
            job_id="direct-job",
            attempt_id="direct-attempt",
            lease_token_hash=H2,
            lease_expires_at=D24_DEADLINE,
            dispatch_record_digest=H3,
            resulting_revision=5,
            disposition=disposition,
            should_execute=should_execute,
            exact_replay=exact_replay,
            already_completed=False,
            terminal_projection=None,
        )
    with pytest.raises(ValidationError):
        M5TypedDirectJobLease(
            "direct-job",
            "direct-attempt",
            H2,
            D24_DEADLINE,
            H3,
            5,
            M5AcquisitionDisposition.RESULT_RESERVED,
            False,
            True,
            False,
            None,
        )
    with pytest.raises(ValidationError):
        M5TypedDirectJobLease(
            "direct-job",
            "direct-attempt",
            H2,
            D24_DEADLINE,
            None,
            5,
            M5AcquisitionDisposition.TERMINAL,
            False,
            True,
            True,
            active_projection,
        )
    with pytest.raises(ValidationError):
        M5TypedDirectTerminalProjection.build(
            job_id="direct-job",
            terminal_state=M4JobState.COMPLETED_INACTIVE,
            terminal_reason="wrong",
            m4_completion_digest=H1,
            completed_revision=5,
        )


def test_d24_dispatch_evidence_and_ambiguity_are_separate_exact_records() -> None:
    requirement_dispatch = M5DispatchRecord.build(
        epoch_id=7,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=H1,
        logical_job_id=H2,
        attempt_ordinal=1,
        job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL.value,
        fallback_required=True,
        dispatched_revision=2,
        lease_expires_at=D24_DEADLINE,
    )
    direct_dispatch = M5DispatchRecord.build(
        epoch_id=7,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id="direct-attempt",
        logical_job_id="direct-job",
        attempt_ordinal=1,
        job_kind=M4JobKind.VERIFY_PAIR.value,
        fallback_required=False,
        dispatched_revision=3,
        lease_expires_at=D24_DEADLINE,
    )
    assert (
        replace(
            requirement_dispatch,
            lease_expires_at=D24_DEADLINE + timedelta(seconds=5),
        ).record_digest
        == requirement_dispatch.record_digest
    )
    with pytest.raises(ValidationError):
        replace(requirement_dispatch, fallback_required=False)

    timing = M5RuntimeTimingObservation.build(M5RuntimeTiming(neural_wall_ns=9))
    timing_digest = digests.attempt_runtime_timing_digest(
        epoch_id=7,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=H1,
        observation_digest=timing.observation_digest,
    )
    evidence = M5AttemptExecutionEvidence.build(
        epoch_id=7,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=H1,
        disposition=M5ExecutionEvidenceDisposition.RETURNED,
        result_or_error_hash=H3,
        attempt_work=M5RuntimeWork(
            requirement_forward_retrieval_call_count=1,
            requirement_fallback_forward_call_count=1,
            embedding_model_call_count=1,
            embedding_input_token_count=12,
        ),
        attempt_timing_digest=timing_digest,
    )
    evidence.validate_dispatch(requirement_dispatch)
    evidence.validate_timing(timing)
    report = M5CallAmbiguityReport.build(
        dispatches=(direct_dispatch, requirement_dispatch),
        execution_evidence=(evidence,),
    )
    replay = M5CallAmbiguityReport.build(
        dispatches=(requirement_dispatch, direct_dispatch),
        execution_evidence=(evidence,),
    )
    assert report == replay
    assert report.durable_dispatch_count == 2
    assert report.confirmed_execution_count == 1
    assert report.unresolved_dispatch_count == 1
    assert report.confirmed_call_lower.requirement_forward_retrieval_call_count == 1
    assert report.possible_call_upper.direct_verifier_call_count == 1
    assert report.possible_call_upper.verifier_model_call_count == 1
    assert report.confirmed_call_lower.embedding_input_token_count == 0

    with pytest.raises(ValidationError):
        M5AttemptExecutionEvidence.build(
            epoch_id=7,
            subgraph=M5RuntimeSubgraph.REQUIREMENT,
            attempt_id=H1,
            disposition=M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
            result_or_error_hash=H3,
            attempt_work=M5RuntimeWork(embedding_model_call_count=1),
            attempt_timing_digest=timing_digest,
        )
    persistence_work = M5AttemptExecutionEvidence.build(
        epoch_id=7,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=H1,
        disposition=M5ExecutionEvidenceDisposition.RETURNED,
        result_or_error_hash=H3,
        attempt_work=M5RuntimeWork(group_state_write_count=1),
        attempt_timing_digest=timing_digest,
    )
    with pytest.raises(ValidationError):
        persistence_work.validate_dispatch(requirement_dispatch)
    excess_work = M5AttemptExecutionEvidence.build(
        epoch_id=7,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=H1,
        disposition=M5ExecutionEvidenceDisposition.RETURNED,
        result_or_error_hash=H3,
        attempt_work=M5RuntimeWork(
            requirement_forward_retrieval_call_count=2,
            requirement_fallback_forward_call_count=2,
            embedding_model_call_count=2,
        ),
        attempt_timing_digest=timing_digest,
    )
    with pytest.raises(ValidationError):
        excess_work.validate_dispatch(requirement_dispatch)


def test_d24_timing_observation_coverage_and_anchor_do_not_confuse_zero_missing() -> (
    None
):
    missing = M5RuntimeTimingObservation.build(None)
    measured_zero = M5RuntimeTimingObservation.build(M5RuntimeTiming())
    assert not missing.required_interval_observed
    assert measured_zero.required_interval_observed
    assert missing.observation_digest != measured_zero.observation_digest
    with pytest.raises(ValidationError):
        replace(missing, required_interval_observed=True)
    with pytest.raises(ValidationError):
        replace(measured_zero, timing=None)
    for required_field in (
        "coordinator_non_db_non_neural_ns",
        "neural_wall_ns",
        "postgres_roundtrip_wall_ns",
        "external_io_wall_ns",
        "end_to_end_wall_ns",
    ):
        with pytest.raises(ValidationError):
            replace(M5RuntimeTiming(), **{required_field: None})

    missing_coverage = M5RuntimeTimingCoverage.single_point(
        None, terminal_client_roundtrip_included=False
    )
    zero_coverage = M5RuntimeTimingCoverage.single_point(
        M5RuntimeTiming(), terminal_client_roundtrip_included=False
    )
    with pytest.raises(ValidationError):
        M5RuntimeTimingCoverage.single_point(
            None, terminal_client_roundtrip_included=True
        )
    missing_coverage.validate_aggregate(M5RuntimeTiming())
    zero_coverage.validate_aggregate(M5RuntimeTiming())
    assert missing_coverage.required_missing_count == 1
    assert zero_coverage.required_observed_count == 1
    assert missing_coverage != zero_coverage
    for family in (
        "required",
        "postgres_server_execution",
        "postgres_lock_wait",
        "postgres_wal_bytes",
        "postgres_shared_block_reads",
    ):
        with pytest.raises(ValidationError):
            replace(
                missing_coverage,
                **{
                    f"{family}_expected_count": 2,
                    f"{family}_missing_count": 2,
                },
            )
    with pytest.raises(ValidationError):
        replace(
            zero_coverage,
            postgres_server_execution_observed_count=1,
            postgres_server_execution_missing_count=0,
        ).validate_aggregate(M5RuntimeTiming(postgres_server_execution_ns=None))

    anchor = M5TransitionTimingAnchor.build(
        epoch_id=7,
        contribution_kind=M5RuntimeWorkContributionKind.M5_ACQUISITION,
        source_id=H1,
        anchor_revision=2,
        terminal_transition=False,
    )
    with pytest.raises(ValidationError):
        M5TransitionTimingAnchor.build(
            epoch_id=7,
            contribution_kind=M5RuntimeWorkContributionKind.M5_ACQUISITION,
            source_id=H1,
            anchor_revision=2,
            terminal_transition=True,
        )
    with pytest.raises(ValidationError):
        M5TransitionTimingAnchor.build(
            epoch_id=7,
            contribution_kind=(M5RuntimeWorkContributionKind.TERMINAL_JOB_FAILURE),
            source_id=H1,
            anchor_revision=2,
            terminal_transition=False,
        )
    with pytest.raises(ValidationError):
        M5TransitionTimingAnchor.build(
            epoch_id=7,
            contribution_kind=M5RuntimeWorkContributionKind.EPOCH_FAILURE,
            source_id=H1,
            anchor_revision=2,
            terminal_transition=False,
        )
    terminal_anchor = M5TransitionTimingAnchor.build(
        epoch_id=7,
        contribution_kind=M5RuntimeWorkContributionKind.EPOCH_FAILURE,
        source_id=H1,
        anchor_revision=2,
        terminal_transition=True,
    )
    with pytest.raises(ValidationError):
        M5TransitionTimingReceipt(
            terminal_anchor,
            H2,
            M5RuntimeTiming(),
            missing_coverage,
            2,
            False,
        )
    transition_digest = digests.transition_call_timing_digest(
        epoch_id=anchor.epoch_id,
        contribution_kind=anchor.contribution_kind,
        source_id=anchor.source_id,
        contribution_key_digest=anchor.contribution_key_digest,
        anchor_revision=anchor.anchor_revision,
        observation_digest=measured_zero.observation_digest,
    )
    receipt = M5TransitionTimingReceipt(
        anchor,
        transition_digest,
        M5RuntimeTiming(),
        zero_coverage,
        2,
        False,
    )
    receipt.validate_observation(measured_zero)
    replace(receipt, exact_replay=True).validate_observation(measured_zero)
    with pytest.raises(ValidationError):
        receipt.validate_observation(missing)


def test_d24_expired_return_and_running_audit_shape_have_exact_identity() -> None:
    expired = M5ExpiredAttemptReturn.build(
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        epoch_id=7,
        attempt_id=H1,
        logical_job_id=H2,
        worker_output_digest=H3,
        worker_artifact_hash=H4,
        activity_snapshot_epoch_id=7,
        activity_snapshot_revision=3,
        received_after_terminal=False,
    )
    assert expired.expired_return_digest == digests.expired_attempt_return_digest(
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        epoch_id=7,
        attempt_id=H1,
        logical_job_id=H2,
        worker_output_digest=H3,
        worker_artifact_hash=H4,
        activity_snapshot_epoch_id=7,
        activity_snapshot_revision=3,
        received_after_terminal=False,
    )
    with pytest.raises(ValidationError):
        replace(expired, received_after_terminal=True)

    output = M5AttemptOutput.build(
        attempt=M5JobAttempt.build(
            logical_job_id=H2,
            attempt_ordinal=1,
            execution_spec_hash=H3,
            lease_token_hash=H4,
        ),
        job_epoch_id=7,
        payload_hash=H1,
        result_artifact_id=H3,
        result_artifact_hash=H4,
    )
    artifact = M5AttemptResultArtifact.build(
        attempt_output=output,
        job_state_at_receipt=M5JobState.RUNNING,
        job_state_after=M5JobState.RUNNING,
        disposition=M5AttemptDisposition.TERMINAL_AUDIT_ONLY,
        activity_snapshot_epoch_id=7,
        activity_snapshot_revision=3,
        epoch_active=True,
        chunk_active=True,
        requirement_active=True,
        group_active=True,
        archive_reason=M5AttemptArchiveReason.ATTEMPT_EXPIRED,
    )
    assert artifact.archive_reason is M5AttemptArchiveReason.ATTEMPT_EXPIRED
    with pytest.raises(ValidationError):
        replace(artifact, job_state_after=M5JobState.COMPLETED_ACTIVE)

    for terminal_state in (
        M5JobState.COMPLETED_ACTIVE,
        M5JobState.COMPLETED_INACTIVE,
        M5JobState.TERMINAL_FAILED,
        M5JobState.CANCELLED,
    ):
        cancelled = terminal_state is M5JobState.CANCELLED
        terminal_artifact = M5AttemptResultArtifact.build(
            attempt_output=output,
            job_state_at_receipt=terminal_state,
            job_state_after=terminal_state,
            disposition=M5AttemptDisposition.TERMINAL_AUDIT_ONLY,
            activity_snapshot_epoch_id=7,
            activity_snapshot_revision=4,
            epoch_active=False,
            chunk_active=True,
            requirement_active=True,
            group_active=True,
            archive_reason=M5AttemptArchiveReason.ATTEMPT_EXPIRED,
            cancelled_by_event_id="cancel-event" if cancelled else None,
            cancelled_by_epoch_id=7 if cancelled else None,
            cancellation_reason=(M5TerminalReason.EPOCH_FAILED if cancelled else None),
        )
        assert terminal_artifact.job_state_after is terminal_state
        with pytest.raises(ValidationError):
            replace(terminal_artifact, job_state_after=M5JobState.RUNNING)


def _m4_job(
    *,
    kind: M4JobKind,
    pair: M4PairKey | None = None,
    target_claim_id: str | None = None,
    target_chunk_version_id: str | None = None,
    parent_job_id: str | None = None,
) -> M4LogicalJobSpec:
    job_id = M4LogicalJobSpec.derive_job_id(
        event_id="event",
        kind=kind,
        candidate_policy_id="policy",
        execution_spec_hash=H1,
        parent_job_id=parent_job_id or "",
        claim_id=(pair.claim_id if pair is not None else target_claim_id or ""),
        chunk_version_id=(
            pair.chunk_version_id if pair is not None else target_chunk_version_id or ""
        ),
    )
    return M4LogicalJobSpec(
        job_id=job_id,
        event_id="event",
        kind=kind,
        candidate_policy_id="policy",
        payload_hash=H2,
        execution_spec_hash=H1,
        parent_job_id=parent_job_id,
        pair=pair,
        target_claim_id=target_claim_id,
        target_chunk_version_id=target_chunk_version_id,
        expandable=kind is not M4JobKind.VERIFY_PAIR,
    )


def _m4_attempt(job: M4LogicalJobSpec) -> M4JobAttempt:
    return M4JobAttempt(
        stable_m4_digest("m4-job-attempt-v1", job.job_id, "1"),
        job.job_id,
        job.execution_spec_hash,
        1,
        stable_m4_digest("m4-lease-token-v1", job.job_id, "1"),
    )


def test_d24_typed_direct_discovery_envelope_binds_exact_epoch_policy_and_scope() -> (
    None
):
    root = _m4_job(
        kind=M4JobKind.IMPACT_DISCOVERY,
        target_chunk_version_id="chunk-1",
    )
    pair = M4PairKey("claim-1", "chunk-1")
    hit = M4ChannelHit(
        7,
        pair,
        "policy",
        M4AdmissionChannel.VECTOR,
        1,
        -0.0,
        H1,
    )
    admitted = M4AdmittedPair(
        7,
        pair,
        "policy",
        1,
        (M4AdmissionChannel.VECTOR,),
        False,
    )
    discovery = M4DiscoveryResult(root.job_id, "result", H4, (admitted,), True, (hit,))
    closure = M4ChildClosure.build(
        parent_job_id=root.job_id,
        result_artifact_hash=H4,
        child_job_ids=("child-job",),
    )
    completion = M4JobCompletion.build(
        job_id=root.job_id,
        payload_hash=root.payload_hash,
        execution_spec_hash=root.execution_spec_hash,
        result_artifact_id="result",
        result_artifact_hash=H4,
        terminal_state=M4JobState.COMPLETED_ACTIVE,
        child_closure=closure,
    )
    scope = M4DiscoveryScope(root.job_id, "registry", ("claim-1",), True)
    envelope = M5TypedDirectLateReturnEnvelope.build_discovery(
        epoch_id=7,
        job=root,
        attempt=_m4_attempt(root),
        completion=completion,
        discovery=discovery,
        scope=scope,
        persisted_scope_kind=M5TypedDirectScopeKind.ALL_REGISTERED_CLAIMS,
        explicit_claim_ids=None,
        closed_revision=4,
    )
    assert envelope.return_kind is M5TypedDirectReturnKind.DISCOVERY
    assert envelope.verifier_binding_digest is None
    assert envelope.discovery_binding_digest is not None
    assert envelope.scope_binding_digest is not None
    with pytest.raises(ValidationError):
        replace(envelope, discovery_binding_digest=H1)
    with pytest.raises(ValidationError):
        M5TypedDirectLateReturnEnvelope.build_discovery(
            epoch_id=7,
            job=root,
            attempt=replace(_m4_attempt(root), attempt_id="forged-attempt"),
            completion=completion,
            discovery=discovery,
            scope=scope,
            persisted_scope_kind=M5TypedDirectScopeKind.ALL_REGISTERED_CLAIMS,
            explicit_claim_ids=None,
            closed_revision=4,
        )
    with pytest.raises(ValidationError):
        M5TypedDirectLateReturnEnvelope.build_discovery(
            epoch_id=7,
            job=root,
            attempt=_m4_attempt(root),
            completion=replace(
                completion,
                child_closure=replace(closure, completion_digest=H3),
            ),
            discovery=discovery,
            scope=scope,
            persisted_scope_kind=M5TypedDirectScopeKind.ALL_REGISTERED_CLAIMS,
            explicit_claim_ids=None,
            closed_revision=4,
        )

    with pytest.raises(ValidationError):
        M5TypedDirectLateReturnEnvelope.build_discovery(
            epoch_id=7,
            job=root,
            attempt=_m4_attempt(root),
            completion=completion,
            discovery=replace(discovery, channel_hits=(replace(hit, epoch_id=8),)),
            scope=scope,
            persisted_scope_kind=M5TypedDirectScopeKind.ALL_REGISTERED_CLAIMS,
            explicit_claim_ids=None,
            closed_revision=4,
        )
    with pytest.raises(ValidationError):
        M5TypedDirectLateReturnEnvelope.build_discovery(
            epoch_id=7,
            job=root,
            attempt=_m4_attempt(root),
            completion=completion,
            discovery=replace(
                discovery,
                admitted_pairs=(replace(admitted, candidate_policy_id="other"),),
            ),
            scope=scope,
            persisted_scope_kind=M5TypedDirectScopeKind.ALL_REGISTERED_CLAIMS,
            explicit_claim_ids=None,
            closed_revision=4,
        )
    with pytest.raises(ValidationError):
        M5TypedDirectLateReturnEnvelope.build_discovery(
            epoch_id=7,
            job=root,
            attempt=_m4_attempt(root),
            completion=completion,
            discovery=discovery,
            scope=scope,
            persisted_scope_kind=M5TypedDirectScopeKind.EXPLICIT_CLAIMS,
            explicit_claim_ids=("claim-2",),
            closed_revision=4,
        )


def test_d24_typed_direct_verifier_envelope_enforces_whole_optional_tuple() -> None:
    pair = M4PairKey("claim-1", "chunk-1")
    job = _m4_job(
        kind=M4JobKind.VERIFY_PAIR,
        pair=pair,
        parent_job_id="root-job",
    )
    completion = M4JobCompletion.build(
        job_id=job.job_id,
        payload_hash=job.payload_hash,
        execution_spec_hash=job.execution_spec_hash,
        result_artifact_id="verifier-result",
        result_artifact_hash=H4,
        terminal_state=M4JobState.COMPLETED_ACTIVE,
    )
    observation = SemanticObservation(
        observation_id="observation",
        subject_kind=SubjectKind.CLAIM,
        subject_id=pair.claim_id,
        chunk_version_id=pair.chunk_version_id,
        task_type="verify_support_v1",
        support_score=0.5,
        refute_score=0.25,
        neutral_score=0.25,
        producer=ModelStamp("model", "revision", "prompt"),
        input_hash=H2,
    )
    execution = M5TypedDirectVerificationExecution(
        observation_id=observation.observation_id,
        job_id=job.job_id,
        admitted_pair_id=stable_m4_digest(
            "m4-admitted-pair-v1",
            "7",
            pair.claim_id,
            pair.chunk_version_id,
            job.candidate_policy_id,
        ),
        model_artifact_id="model-artifact",
        prompt_artifact_id="prompt-artifact",
        execution_spec_hash=job.execution_spec_hash,
        pair_input_hash=H3,
        calibration_version="calibration",
        calibration_artifact_sha256=H1,
        temperature=1.0,
        raw_logits=(-0.0, 1.0, 2.0),
        raw_output_hash=H3,
        reused_from_observation_id=None,
    )
    present = M5TypedDirectLateReturnEnvelope.build_verifier(
        epoch_id=7,
        job=job,
        attempt=_m4_attempt(job),
        completion=completion,
        verification_execution=execution,
        observation=observation,
        observation_produced_epoch=7,
        observation_raw_output_hash=H3,
        observation_eligible_for_currency=True,
        requested_make_effective=True,
    )
    reused = M5TypedDirectLateReturnEnvelope.build_verifier(
        epoch_id=7,
        job=job,
        attempt=_m4_attempt(job),
        completion=completion,
        verification_execution=replace(
            execution, reused_from_observation_id="prior-observation"
        ),
        observation=observation,
        observation_produced_epoch=7,
        observation_raw_output_hash=H3,
        observation_eligible_for_currency=True,
        requested_make_effective=True,
    )
    absent = M5TypedDirectLateReturnEnvelope.build_verifier(
        epoch_id=7,
        job=job,
        attempt=_m4_attempt(job),
        completion=completion,
        verification_execution=None,
        observation=observation,
        observation_produced_epoch=7,
        observation_raw_output_hash=completion.result_artifact_hash,
        observation_eligible_for_currency=True,
        requested_make_effective=False,
    )
    make_inactive = M5TypedDirectLateReturnEnvelope.build_verifier(
        epoch_id=7,
        job=job,
        attempt=_m4_attempt(job),
        completion=completion,
        verification_execution=execution,
        observation=observation,
        observation_produced_epoch=7,
        observation_raw_output_hash=H3,
        observation_eligible_for_currency=True,
        requested_make_effective=False,
    )
    assert present.verifier_binding_digest != reused.verifier_binding_digest
    assert present.envelope_digest != absent.envelope_digest
    assert present.verifier_binding_digest != make_inactive.verifier_binding_digest
    assert present.job is job and present.completion is completion
    with pytest.raises(ValidationError):
        replace(present, verification_execution_present=False)
    with pytest.raises(ValidationError):
        M5TypedDirectLateReturnEnvelope.build_verifier(
            epoch_id=7,
            job=job,
            attempt=_m4_attempt(job),
            completion=completion,
            verification_execution=None,
            observation=observation,
            observation_produced_epoch=7,
            observation_raw_output_hash=H3,
            observation_eligible_for_currency=True,
            requested_make_effective=False,
        )
    with pytest.raises(ValidationError):
        M5TypedDirectLateReturnEnvelope.build_verifier(
            epoch_id=7,
            job=job,
            attempt=_m4_attempt(job),
            completion=completion,
            verification_execution=execution,
            observation=observation,
            observation_produced_epoch=7,
            observation_raw_output_hash=H3,
            observation_eligible_for_currency=False,
            requested_make_effective=True,
        )
    with pytest.raises(ValidationError):
        M5TypedDirectLateReturnEnvelope.build_verifier(
            epoch_id=7,
            job=job,
            attempt=_m4_attempt(job),
            completion=completion,
            verification_execution=execution,
            observation=observation,
            observation_produced_epoch=8,
            observation_raw_output_hash=H3,
            observation_eligible_for_currency=True,
            requested_make_effective=True,
        )
    for required_field in (
        field.name
        for field in fields(M5TypedDirectVerificationExecution)
        if field.name != "reused_from_observation_id"
    ):
        with pytest.raises(ValidationError):
            replace(execution, **{required_field: None})
    with pytest.raises(ValidationError):
        replace(execution, raw_logits=(1.0, 2.0))  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        replace(execution, raw_logits=(1.0, 2.0, float("nan")))
    with pytest.raises(ValidationError):
        replace(execution, temperature=0.0)
    with pytest.raises(ValidationError):
        replace(execution, temperature=-1.0)
    with pytest.raises(ValidationError):
        replace(
            execution,
            reused_from_observation_id=execution.observation_id,
        )


def test_d24_event_result_coverage_bridge_never_fabricates_observed_zero() -> None:
    blocked = M5EventRunResult.build(
        event_id="event",
        payload_hash=H1,
        epoch_id=5,
        state=M5RunState.BLOCKED,
        replayed_outcome=None,
        open_receipt=OpenEventReceipt(5, False, False),
        publication_receipt=None,
        event_work=M5RuntimeWork(),
        call_work=M5RuntimeWork(),
        event_timing=M5RuntimeTiming(),
        call_timing=M5RuntimeTiming(),
        combined_deltas=(),
        changed_state_references=(),
        failure_reason=M5RunFailureReason.WORK_IN_PROGRESS,
    )
    assert blocked.event_timing_coverage is None
    assert blocked.call_timing_coverage is None
    measured_zero = M5RuntimeTimingCoverage.single_point(
        M5RuntimeTiming(), terminal_client_roundtrip_included=False
    )
    missing = M5RuntimeTimingCoverage.single_point(
        None, terminal_client_roundtrip_included=False
    )
    covered = replace(
        blocked,
        event_timing_coverage=missing,
        call_timing_coverage=measured_zero,
    )
    assert covered.event_timing_coverage != covered.call_timing_coverage
    zero_point = M5RuntimeTimingCoverage(
        required_expected_count=0,
        required_observed_count=0,
        required_missing_count=0,
        postgres_server_execution_expected_count=0,
        postgres_server_execution_observed_count=0,
        postgres_server_execution_missing_count=0,
        postgres_lock_wait_expected_count=0,
        postgres_lock_wait_observed_count=0,
        postgres_lock_wait_missing_count=0,
        postgres_wal_bytes_expected_count=0,
        postgres_wal_bytes_observed_count=0,
        postgres_wal_bytes_missing_count=0,
        postgres_shared_block_reads_expected_count=0,
        postgres_shared_block_reads_observed_count=0,
        postgres_shared_block_reads_missing_count=0,
        terminal_client_roundtrip_included=False,
    )
    with pytest.raises(ValidationError):
        replace(covered, call_timing_coverage=zero_point)
    terminal_call = M5RuntimeTimingCoverage.single_point(
        M5RuntimeTiming(), terminal_client_roundtrip_included=True
    )
    with pytest.raises(ValidationError):
        replace(covered, call_timing_coverage=terminal_call)
    with pytest.raises(ValidationError):
        replace(blocked, event_timing_coverage=missing)
    with pytest.raises(ValidationError):
        M5EventRunResult.build(
            event_id="event",
            payload_hash=H1,
            epoch_id=5,
            state=M5RunState.FAILED,
            replayed_outcome=None,
            open_receipt=OpenEventReceipt(5, False, False),
            publication_receipt=None,
            event_work=M5RuntimeWork(),
            call_work=M5RuntimeWork(),
            event_timing=M5RuntimeTiming(),
            call_timing=M5RuntimeTiming(),
            combined_deltas=(),
            changed_state_references=(),
            failure_reason=M5RunFailureReason.WORK_IN_PROGRESS,
        )

    def failed_with_coverage(
        event_coverage: M5RuntimeTimingCoverage,
        call_coverage: M5RuntimeTimingCoverage,
    ) -> M5EventRunResult:
        return M5EventRunResult.build(
            event_id="event",
            payload_hash=H1,
            epoch_id=5,
            state=M5RunState.FAILED,
            replayed_outcome=None,
            open_receipt=OpenEventReceipt(5, False, False),
            publication_receipt=None,
            event_work=M5RuntimeWork(),
            call_work=M5RuntimeWork(),
            event_timing=M5RuntimeTiming(),
            call_timing=M5RuntimeTiming(),
            combined_deltas=(),
            changed_state_references=(),
            failure_reason=M5RunFailureReason.RETRIEVAL_ERROR,
            event_timing_coverage=event_coverage,
            call_timing_coverage=call_coverage,
        )

    baseline = failed_with_coverage(missing, missing)
    event_coverage_changed = failed_with_coverage(measured_zero, missing)
    call_coverage_changed = failed_with_coverage(missing, terminal_call)
    assert baseline.logical_result_hash is not None
    assert (
        baseline.logical_result_hash
        == event_coverage_changed.logical_result_hash
        == call_coverage_changed.logical_result_hash
    )
    with pytest.raises(ValidationError):
        failed_with_coverage(missing, measured_zero)

    def replayed_with_coverage(
        outcome: M5ReplayedOutcome,
        call_coverage: M5RuntimeTimingCoverage,
    ) -> M5EventRunResult:
        publication_id = stable_m4_digest("m4-publication-v1", "5")
        sealed = outcome is M5ReplayedOutcome.SEALED
        return M5EventRunResult.build(
            event_id="event",
            payload_hash=H1,
            epoch_id=5,
            state=M5RunState.REPLAYED,
            replayed_outcome=outcome,
            open_receipt=OpenEventReceipt(
                epoch_id=5,
                replayed=True,
                already_sealed=sealed,
                publication_id=publication_id if sealed else None,
                already_failed=not sealed,
                failure_reason=(
                    None if sealed else M5RunFailureReason.RETRIEVAL_ERROR.value
                ),
            ),
            publication_receipt=(
                PublicationReceipt(5, publication_id, True) if sealed else None
            ),
            event_work=M5RuntimeWork(),
            call_work=M5RuntimeWork(),
            event_timing=M5RuntimeTiming(),
            call_timing=M5RuntimeTiming(),
            combined_deltas=(),
            changed_state_references=(),
            failure_reason=(None if sealed else M5RunFailureReason.RETRIEVAL_ERROR),
            event_timing_coverage=missing,
            call_timing_coverage=call_coverage,
        )

    for outcome in (M5ReplayedOutcome.SEALED, M5ReplayedOutcome.FAILED):
        observed_replay = replayed_with_coverage(outcome, terminal_call)
        missing_replay = replayed_with_coverage(outcome, missing)
        assert observed_replay.state is M5RunState.REPLAYED
        assert missing_replay.state is M5RunState.REPLAYED
        with pytest.raises(ValidationError):
            replayed_with_coverage(outcome, measured_zero)
