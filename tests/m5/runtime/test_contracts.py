from __future__ import annotations

from dataclasses import fields, replace

import pytest

from groundloop.domain import DecisionPolicy, SubjectKind, VerificationLabel
from groundloop.errors import ValidationError
from groundloop.events import ChunkInput, InsertDocumentEvent
from groundloop.m4.application import (
    DynamicEventPlan,
    OpenEventReceipt,
    PublicationReceipt,
)
from groundloop.m4.contracts import (
    CorpusUpdateIdentity,
    UpdateKind,
    VectorIndexKind,
    stable_m4_digest,
)
from groundloop.m5.digests import normalize_text_v1, normalized_text_hash_v1
from groundloop.m5.events import legacy_event_payload_digest
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5ActivationReceipt,
    M5ActivationRequest,
    M5AttemptArchiveReason,
    M5AttemptDisposition,
    M5AttemptOutput,
    M5AttemptResultArtifact,
    M5CandidatePolicyManifest,
    M5ChangedStateReference,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5EventRunResult,
    M5JobAttempt,
    M5JobCompletion,
    M5JobKind,
    M5JobLease,
    M5JobState,
    M5LogicalJobSpec,
    M5OwnerPendingCounter,
    M5ReplayedOutcome,
    M5RequirementAdmissionChannel,
    M5RequirementChannelHit,
    M5RequirementDiscoveryResult,
    M5RequirementPairInput,
    M5RequirementScopeSelection,
    M5RequirementVerifierArtifact,
    M5RetrievalTermination,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5StateReferenceKind,
    M5TerminalReason,
    M5TextNormalizerProvenance,
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
