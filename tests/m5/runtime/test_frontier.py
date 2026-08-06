from __future__ import annotations

from dataclasses import replace
from itertools import permutations, product

import pytest

from groundloop.domain import SubjectKind
from groundloop.errors import ValidationError
from groundloop.m4.contracts import VectorIndexKind
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5AttemptArchiveReason,
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5JobCompletion,
    M5JobKind,
    M5JobState,
    M5LogicalJobSpec,
    M5RequirementAdmissionChannel,
    M5RequirementChannelHit,
    M5RequirementDiscoveryResult,
    M5RequirementFallbackKey,
    M5RequirementScopeSelection,
    M5RetrievalTermination,
    M5TerminalReason,
    SemanticPairKey,
)
from groundloop.m5.runtime.frontier import (
    M5WithdrawnCandidateEdge,
    M5WithdrawnObservationEdge,
    advance_requirement_frontier,
    build_forward_frontier_head,
    build_rank_interleaved_discovery_result,
    build_root_barrier_plan,
    classify_attempt_activity,
    coalesce_forward_root_keys,
    deduplicate_discovery_results,
    plan_requirement_withdrawal,
    rank_interleave_scope_selections,
    validate_directional_hit_order,
)

H1 = "1" * 64
H2 = "2" * 64
H3 = "3" * 64
H4 = "4" * 64


def manifest(*, budget: int = 2, lineage: bool = True) -> M5CandidatePolicyManifest:
    return M5CandidatePolicyManifest.build(
        embedding_model_artifact_id="embedding",
        requirement_role_template_hash=H1,
        chunk_role_template_hash=H2,
        vector_method_version="vector-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=H3,
        vector_search_config_hash=H4,
        lexical_method_version="lexical-v1",
        lexical_config_hash=H1,
        lexical_postgres_version="postgres-17",
        lexical_regconfig_identity="simple",
        fusion_version="rank-interleave-v1",
        reverse_budget_per_inserted_chunk=budget,
        forward_budget_per_requirement=budget,
        verifier_execution_spec_hash=H3,
        decision_policy_version="decision-v1",
        lineage_safety_override=lineage,
    )


def scope(policy: M5CandidatePolicyManifest) -> M5DiscoveryScopeContract:
    return M5DiscoveryScopeContract.build(
        direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
        requirement_version_id="req-1",
        inserted_chunk_version_id=None,
        candidate_policy_id=policy.candidate_policy_id,
        requirement_registry_snapshot_digest=H3,
        active_chunk_snapshot_digest=H4,
    )


def hit(
    *,
    root: str,
    scope_digest: str,
    policy_id: str,
    pair: SemanticPairKey,
    channel: M5RequirementAdmissionChannel,
    rank: int,
    score: float | None,
    artifact_hash: str,
) -> M5RequirementChannelHit:
    return M5RequirementChannelHit.build(
        epoch_id=9,
        root_job_id=root,
        scope_contract_digest=scope_digest,
        pair=pair,
        candidate_policy_id=policy_id,
        channel=channel,
        rank=rank,
        score=score,
        channel_artifact_hash=artifact_hash,
    )


def canonical_hits(
    hits: tuple[M5RequirementChannelHit, ...],
) -> tuple[M5RequirementChannelHit, ...]:
    return tuple(
        sorted(
            hits,
            key=lambda item: (
                item.channel.value,
                item.rank,
                item.semantic_pair_digest,
            ),
        )
    )


def test_rank_interleave_is_vector_first_deduplicated_and_lineage_appended() -> None:
    policy = manifest(budget=2)
    pair_a = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-a")
    pair_b = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-b")
    pair_c = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-c")
    pair_d = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-d")
    hits = canonical_hits(
        (
            hit(
                root="root",
                scope_digest=H1,
                policy_id=policy.candidate_policy_id,
                pair=pair_a,
                channel=M5RequirementAdmissionChannel.VECTOR,
                rank=1,
                score=0.9,
                artifact_hash=H1,
            ),
            hit(
                root="root",
                scope_digest=H1,
                policy_id=policy.candidate_policy_id,
                pair=pair_b,
                channel=M5RequirementAdmissionChannel.VECTOR,
                rank=2,
                score=0.8,
                artifact_hash=H2,
            ),
            hit(
                root="root",
                scope_digest=H1,
                policy_id=policy.candidate_policy_id,
                pair=pair_a,
                channel=M5RequirementAdmissionChannel.LEXICAL,
                rank=1,
                score=0.7,
                artifact_hash=H3,
            ),
            hit(
                root="root",
                scope_digest=H1,
                policy_id=policy.candidate_policy_id,
                pair=pair_c,
                channel=M5RequirementAdmissionChannel.LEXICAL,
                rank=2,
                score=0.6,
                artifact_hash=H4,
            ),
            hit(
                root="root",
                scope_digest=H1,
                policy_id=policy.candidate_policy_id,
                pair=pair_d,
                channel=M5RequirementAdmissionChannel.LINEAGE,
                rank=1,
                score=None,
                artifact_hash=H1,
            ),
            hit(
                root="root",
                scope_digest=H1,
                policy_id=policy.candidate_policy_id,
                pair=pair_b,
                channel=M5RequirementAdmissionChannel.LINEAGE,
                rank=2,
                score=None,
                artifact_hash=H2,
            ),
        )
    )

    selections = rank_interleave_scope_selections(
        hits=hits,
        direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
        approximate_budget=2,
        lineage_safety_override=True,
    )

    assert tuple(selection.pair for selection in selections[:2]) == (pair_a, pair_b)
    assert selections[0].reasons == (
        M5RequirementAdmissionChannel.LEXICAL,
        M5RequirementAdmissionChannel.VECTOR,
    )
    assert selections[1].mandatory_lineage
    assert tuple(
        selection.semantic_pair_digest for selection in selections[2:]
    ) == tuple(sorted((pair_d.semantic_pair_digest,)))
    assert pair_c not in {selection.pair for selection in selections}


def test_nonselected_hit_never_promotes_and_short_empty_results_close_normally() -> (
    None
):
    policy = manifest(budget=1, lineage=False)
    pair_a = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-a")
    pair_b = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-b")
    hits = canonical_hits(
        (
            hit(
                root="root",
                scope_digest=H1,
                policy_id=policy.candidate_policy_id,
                pair=pair_a,
                channel=M5RequirementAdmissionChannel.VECTOR,
                rank=1,
                score=0.9,
                artifact_hash=H1,
            ),
            hit(
                root="root",
                scope_digest=H1,
                policy_id=policy.candidate_policy_id,
                pair=pair_b,
                channel=M5RequirementAdmissionChannel.LEXICAL,
                rank=1,
                score=0.8,
                artifact_hash=H2,
            ),
        )
    )
    full = build_rank_interleaved_discovery_result(
        hits=hits,
        direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
        manifest=policy,
        root_job_id="root",
        scope_contract_digest=H1,
        eligible_snapshot_exhausted=False,
    )
    empty = build_rank_interleaved_discovery_result(
        hits=(),
        direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
        manifest=policy,
        root_job_id="empty-root",
        scope_contract_digest=H1,
        eligible_snapshot_exhausted=True,
    )

    assert full.termination is M5RetrievalTermination.BUDGET_FILLED
    assert tuple(selection.pair for selection in full.selections) == (pair_a,)
    assert pair_b in {item.pair for item in full.channel_hits}
    assert empty.termination is M5RetrievalTermination.SNAPSHOT_EXHAUSTED
    assert not empty.selections
    with pytest.raises(ValidationError):
        build_rank_interleaved_discovery_result(
            hits=(),
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            manifest=policy,
            root_job_id="unavailable-is-not-empty-success",
            scope_contract_digest=H1,
            eligible_snapshot_exhausted=False,
        )


def test_lineage_disabled_rejects_lineage_hits_instead_of_hiding_them() -> None:
    policy = manifest(lineage=False)
    pair = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-a")
    hits = canonical_hits(
        (
            hit(
                root="root",
                scope_digest=H1,
                policy_id=policy.candidate_policy_id,
                pair=pair,
                channel=M5RequirementAdmissionChannel.LINEAGE,
                rank=1,
                score=None,
                artifact_hash=H1,
            ),
        )
    )

    with pytest.raises(ValidationError):
        rank_interleave_scope_selections(
            hits=hits,
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            approximate_budget=2,
            lineage_safety_override=False,
        )


def test_directional_score_rank_and_utf8_tie_breaker_are_exact() -> None:
    pair_a = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-a")
    pair_b = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-b")
    valid = canonical_hits(
        (
            hit(
                root="root",
                scope_digest=H1,
                policy_id="policy",
                pair=pair_a,
                channel=M5RequirementAdmissionChannel.VECTOR,
                rank=1,
                score=0.5,
                artifact_hash=H1,
            ),
            hit(
                root="root",
                scope_digest=H1,
                policy_id="policy",
                pair=pair_b,
                channel=M5RequirementAdmissionChannel.VECTOR,
                rank=2,
                score=0.5,
                artifact_hash=H2,
            ),
        )
    )
    validate_directional_hit_order(valid, M5DiscoveryDirection.FORWARD_REQUIREMENT)

    invalid = canonical_hits(
        (
            replace(
                valid[0],
                rank=2,
                hit_digest=digests.requirement_channel_hit_digest(
                    epoch_id=valid[0].epoch_id,
                    root_job_id=valid[0].root_job_id,
                    scope_contract_digest=valid[0].scope_contract_digest,
                    semantic_pair_digest_value=valid[0].semantic_pair_digest,
                    candidate_policy_id=valid[0].candidate_policy_id,
                    channel=valid[0].channel,
                    rank=2,
                    score=valid[0].score,
                    channel_artifact_hash=valid[0].channel_artifact_hash,
                ),
            ),
            replace(
                valid[1],
                rank=1,
                hit_digest=digests.requirement_channel_hit_digest(
                    epoch_id=valid[1].epoch_id,
                    root_job_id=valid[1].root_job_id,
                    scope_contract_digest=valid[1].scope_contract_digest,
                    semantic_pair_digest_value=valid[1].semantic_pair_digest,
                    candidate_policy_id=valid[1].candidate_policy_id,
                    channel=valid[1].channel,
                    rank=1,
                    score=valid[1].score,
                    channel_artifact_hash=valid[1].channel_artifact_hash,
                ),
            ),
        )
    )
    with pytest.raises(ValidationError):
        validate_directional_hit_order(
            invalid, M5DiscoveryDirection.FORWARD_REQUIREMENT
        )


def _result(
    *,
    root: str,
    policy_id: str,
    pairs: tuple[SemanticPairKey, ...],
    scope_digest: str = H4,
) -> M5RequirementDiscoveryResult:
    local_hits = canonical_hits(
        tuple(
            hit(
                root=root,
                scope_digest=scope_digest,
                policy_id=policy_id,
                pair=pair,
                channel=M5RequirementAdmissionChannel.VECTOR,
                rank=rank,
                score=1.0 - rank / 10,
                artifact_hash=(H1, H2, H3, H4)[(rank - 1) % 4],
            )
            for rank, pair in enumerate(pairs, start=1)
        )
    )
    selections = tuple(
        M5RequirementScopeSelection.build(
            root_job_id=root,
            scope_contract_digest=scope_digest,
            pair=pair,
            fused_rank=rank,
            reasons=(M5RequirementAdmissionChannel.VECTOR,),
        )
        for rank, pair in enumerate(pairs, start=1)
    )
    return M5RequirementDiscoveryResult.build(
        root_job_id=root,
        scope_contract_digest=scope_digest,
        termination=M5RetrievalTermination.SNAPSHOT_EXHAUSTED,
        channel_hits=local_hits,
        selections=selections,
    )


def test_event_wide_dedup_is_permutation_stable_with_least_root_owner() -> None:
    policy = manifest()
    pair_a = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-a")
    pair_b = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-b")
    pair_c = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-c")
    results = (
        _result(root=H3, policy_id=policy.candidate_policy_id, pairs=(pair_a,)),
        _result(
            root=H1,
            policy_id=policy.candidate_policy_id,
            pairs=(pair_a, pair_b),
        ),
        _result(
            root=H2,
            policy_id=policy.candidate_policy_id,
            pairs=(pair_b, pair_c),
        ),
    )
    plans = tuple(
        deduplicate_discovery_results(
            epoch_id=9,
            candidate_policy_id=policy.candidate_policy_id,
            results=ordering,
        )
        for ordering in permutations(results)
    )

    assert all(plan == plans[0] for plan in plans)
    owner_by_pair = {
        pair.semantic_pair_digest: pair.owner_root_job_id
        for pair in plans[0].admitted_pairs
    }
    assert owner_by_pair[pair_a.semantic_pair_digest] == H1
    assert owner_by_pair[pair_b.semantic_pair_digest] == H1
    assert owner_by_pair[pair_c.semantic_pair_digest] == H2
    assert plans[0].root_ownership[-1].semantic_pair_digests == ()
    assert plans[0].root_ownership[-1].scope_closure_digest == (
        digests.discovery_scope_closure_digest(H4, ())
    )
    inactive = deduplicate_discovery_results(
        epoch_id=9,
        candidate_policy_id=policy.candidate_policy_id,
        results=results,
        inactive_root_job_ids=(H1,),
    )
    assert all(
        source.root_job_id != H1
        for pair in inactive.admitted_pairs
        for source in pair.sources
    )
    assert (
        next(
            ownership
            for ownership in inactive.root_ownership
            if ownership.root_job_id == H1
        ).semantic_pair_digests
        == ()
    )


def test_child_sets_closures_and_barrier_hash_are_permutation_stable() -> None:
    policy = manifest()
    scope_contract = scope(policy)
    pair_a = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-a")
    pair_b = SemanticPairKey(SubjectKind.REQUIREMENT, "req-1", "chunk-b")
    results = (
        _result(
            root=H2,
            policy_id=policy.candidate_policy_id,
            pairs=(pair_a,),
            scope_digest=scope_contract.scope_contract_digest,
        ),
        _result(
            root=H1,
            policy_id=policy.candidate_policy_id,
            pairs=(pair_a, pair_b),
            scope_digest=scope_contract.scope_contract_digest,
        ),
    )
    dedup = deduplicate_discovery_results(
        epoch_id=9,
        candidate_policy_id=policy.candidate_policy_id,
        results=results,
    )
    children = tuple(
        M5LogicalJobSpec.build(
            structural_event_id="event",
            job_kind=M5JobKind.VERIFY_REQUIREMENT_PAIR,
            manifest=policy,
            scope=scope_contract,
            parent_job_id=pair.owner_root_job_id,
            pair=pair.pair,
        )
        for pair in dedup.admitted_pairs
    )
    plans = tuple(
        build_root_barrier_plan(
            structural_event_id="event",
            results=result_order,
            deduplication=dedup,
            child_jobs=child_order,
        )
        for result_order in permutations(results)
        for child_order in permutations(children)
    )

    assert all(plan == plans[0] for plan in plans)
    assert plans[0].requirement_root_set_hash == digests.requirement_root_set_digest(
        (H1, H2)
    )


def test_multi_chunk_withdrawal_coalesces_fallback_and_excludes_retired() -> None:
    candidate_edges = (
        M5WithdrawnCandidateEdge(H1, "req-active", "chunk-a", "policy"),
        M5WithdrawnCandidateEdge(H2, "req-active", "chunk-b", "policy"),
        M5WithdrawnCandidateEdge(H3, "req-retired", "chunk-a", "policy"),
    )
    observation_edges = (
        M5WithdrawnObservationEdge("observation-1", "req-active", "chunk-b", "policy"),
        M5WithdrawnObservationEdge("observation-2", "req-active", "chunk-c", "policy"),
    )
    plan = plan_requirement_withdrawal(
        event_id="delete-event",
        deactivated_chunk_version_ids=("chunk-b", "chunk-a"),
        candidate_edges=candidate_edges,
        observation_edges=observation_edges,
        cancelled_job_ids=(H2, H1, H1),
        active_requirement_version_ids=("req-active",),
    )

    assert plan.deactivated_chunk_version_ids == ("chunk-a", "chunk-b")
    assert plan.withdrawn_candidate_pair_digests == (H1, H2, H3)
    assert plan.withdrawn_observation_ids == ("observation-1",)
    assert plan.cancelled_job_ids == (H1, H2)
    assert plan.fallback_keys == (M5RequirementFallbackKey("req-active", "policy"),)
    assert (
        coalesce_forward_root_keys(
            new_requirement_keys=plan.fallback_keys,
            fallback_keys=plan.fallback_keys,
        )
        == plan.fallback_keys
    )


@pytest.mark.parametrize(
    ("epoch_active", "requirement_active", "group_active", "chunk_active"),
    tuple(product((False, True), repeat=4)),
)
def test_activity_classifier_exhaustive_precedence(
    epoch_active: bool,
    requirement_active: bool,
    group_active: bool,
    chunk_active: bool,
) -> None:
    actual = classify_attempt_activity(
        epoch_active=epoch_active,
        requirement_active=requirement_active,
        group_active=group_active,
        chunk_active=chunk_active,
        job_already_terminal=False,
    )
    if not epoch_active:
        expected = M5AttemptArchiveReason.EPOCH_FAILED
    elif not requirement_active or not group_active:
        expected = M5AttemptArchiveReason.SUBJECT_INACTIVE
    elif not chunk_active:
        expected = M5AttemptArchiveReason.CHUNK_INACTIVE
    else:
        expected = None
    assert actual is expected


def test_activity_classifier_terminal_reason_has_lowest_precedence() -> None:
    assert (
        classify_attempt_activity(
            epoch_active=True,
            requirement_active=True,
            group_active=True,
            chunk_active=True,
            job_already_terminal=True,
        )
        is M5AttemptArchiveReason.JOB_ALREADY_TERMINAL
    )


def test_empty_and_short_forward_success_update_head_but_failure_does_not() -> None:
    policy = manifest(budget=2)
    scope_contract = scope(policy)
    root = M5LogicalJobSpec.build(
        structural_event_id="event",
        job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
        manifest=policy,
        scope=scope_contract,
    )
    result = M5RequirementDiscoveryResult.build(
        root_job_id=root.logical_job_id,
        scope_contract_digest=scope_contract.scope_contract_digest,
        termination=M5RetrievalTermination.SNAPSHOT_EXHAUSTED,
        channel_hits=(),
        selections=(),
    )
    completion = M5JobCompletion.build(
        job=root,
        terminal_state=M5JobState.COMPLETED_ACTIVE,
        result_artifact_id=result.result_artifact_id,
        result_artifact_hash=result.result_artifact_hash,
        scope_closure_digest=digests.discovery_scope_closure_digest(
            scope_contract.scope_contract_digest, ()
        ),
        child_set_hash=digests.child_set_digest(()),
    )
    first = build_forward_frontier_head(
        job=root,
        scope=scope_contract,
        discovery_result=result,
        completion=completion,
        completed_epoch_id=10,
        completed_revision=3,
    )
    later = replace(first, completed_epoch_id=11, completed_revision=1)

    assert advance_requirement_frontier(None, first) is first
    assert advance_requirement_frontier(first, later) is later
    assert advance_requirement_frontier(later, first) is later
    with pytest.raises(ValidationError):
        advance_requirement_frontier(first, replace(first, latest_completion_digest=H1))
    failed_completion = M5JobCompletion.build(
        job=root,
        terminal_state=M5JobState.TERMINAL_FAILED,
        archive_reason=M5TerminalReason.RETRIEVAL_ERROR,
    )
    with pytest.raises(ValidationError):
        build_forward_frontier_head(
            job=root,
            scope=scope_contract,
            discovery_result=result,
            completion=failed_completion,
            completed_epoch_id=11,
            completed_revision=1,
        )
