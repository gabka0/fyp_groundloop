"""Contract tests for the coordinator-owned M4 boundary."""

import pytest

from groundloop.domain import VerificationLabel
from groundloop.errors import ValidationError
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    AffectedSets,
    CandidatePolicyManifest,
    ChildClosure,
    FullPairAuditResult,
    JobCompletion,
    JobKind,
    JobState,
    JudgmentSourceKind,
    LogicalJobSpec,
    PairJudgment,
    PairKey,
    VectorIndexKind,
    stable_m4_digest,
)

HASH = "a" * 64


def _judgment(claim_id: str, chunk_id: str) -> PairJudgment:
    return PairJudgment(
        pair=PairKey(claim_id, chunk_id),
        source_kind=JudgmentSourceKind.MODEL,
        source_artifact_id="verifier-v1",
        decision_policy_or_guideline_id="policy-v1",
        derived_label=VerificationLabel.NEUTRAL,
        input_hash=HASH,
        split_id="development",
        support_score=0.1,
        refute_score=0.2,
        neutral_score=0.7,
    )


def test_child_closure_is_canonical_and_empty_closure_is_explicit() -> None:
    closure = ChildClosure.build(
        parent_job_id="parent",
        result_artifact_hash=HASH,
        child_job_ids=("b", "a", "a"),
    )
    assert closure.child_job_ids == ("a", "b")
    assert closure.child_set_hash == stable_m4_digest("m4-child-set-v1", "a", "b")

    empty = ChildClosure.build(
        parent_job_id="parent-empty",
        result_artifact_hash=HASH,
        child_job_ids=(),
    )
    assert empty.child_job_ids == ()
    assert empty.child_set_hash == stable_m4_digest("m4-child-set-v1")


def test_completion_digest_binds_result_state_and_child_set() -> None:
    closure = ChildClosure.build(
        parent_job_id="parent",
        result_artifact_hash=HASH,
        child_job_ids=("child",),
    )
    completion = JobCompletion.build(
        job_id="parent",
        payload_hash=HASH,
        execution_spec_hash=HASH,
        result_artifact_id="discovery-result",
        result_artifact_hash=HASH,
        terminal_state=JobState.COMPLETED_ACTIVE,
        child_closure=closure,
    )
    assert completion.child_closure == closure

    with pytest.raises(ValidationError, match="completed state"):
        JobCompletion.build(
            job_id="parent",
            payload_hash=HASH,
            execution_spec_hash=HASH,
            result_artifact_id="discovery-result",
            result_artifact_hash=HASH,
            terminal_state=JobState.RUNNING,
            child_closure=closure,
        )


def test_verify_job_requires_pair_and_expandable_kind_is_frozen() -> None:
    job_id = LogicalJobSpec.derive_job_id(
        event_id="event",
        kind=JobKind.VERIFY_PAIR,
        candidate_policy_id="candidate-policy",
        execution_spec_hash=HASH,
        claim_id="claim",
        chunk_version_id="chunk",
    )
    job = LogicalJobSpec(
        job_id=job_id,
        event_id="event",
        kind=JobKind.VERIFY_PAIR,
        candidate_policy_id="candidate-policy",
        payload_hash=HASH,
        execution_spec_hash=HASH,
        pair=PairKey("claim", "chunk"),
    )
    assert not job.expandable

    with pytest.raises(ValidationError, match="requires a pair"):
        LogicalJobSpec(
            job_id=LogicalJobSpec.derive_job_id(
                event_id="event",
                kind=JobKind.VERIFY_PAIR,
                candidate_policy_id="candidate-policy",
                execution_spec_hash=HASH,
            ),
            event_id="event",
            kind=JobKind.VERIFY_PAIR,
            candidate_policy_id="candidate-policy",
            payload_hash=HASH,
            execution_spec_hash=HASH,
        )


def test_expandable_job_targets_are_persisted_and_identity_checked() -> None:
    discovery_id = LogicalJobSpec.derive_job_id(
        event_id="event",
        kind=JobKind.IMPACT_DISCOVERY,
        candidate_policy_id="policy",
        execution_spec_hash=HASH,
        chunk_version_id="chunk",
    )
    discovery = LogicalJobSpec(
        job_id=discovery_id,
        event_id="event",
        kind=JobKind.IMPACT_DISCOVERY,
        candidate_policy_id="policy",
        payload_hash=HASH,
        execution_spec_hash=HASH,
        target_chunk_version_id="chunk",
        expandable=True,
    )
    assert discovery.target_chunk_version_id == "chunk"

    frontier_id = LogicalJobSpec.derive_job_id(
        event_id="event",
        kind=JobKind.FRONTIER_RETRIEVE,
        candidate_policy_id="policy",
        execution_spec_hash=HASH,
        claim_id="claim",
    )
    frontier = LogicalJobSpec(
        job_id=frontier_id,
        event_id="event",
        kind=JobKind.FRONTIER_RETRIEVE,
        candidate_policy_id="policy",
        payload_hash=HASH,
        execution_spec_hash=HASH,
        target_claim_id="claim",
        expandable=True,
    )
    assert frontier.target_claim_id == "claim"

    with pytest.raises(ValidationError, match="job_id"):
        LogicalJobSpec(
            job_id=frontier_id,
            event_id="event",
            kind=JobKind.FRONTIER_RETRIEVE,
            candidate_policy_id="policy",
            payload_hash=HASH,
            execution_spec_hash=HASH,
            target_claim_id="different-claim",
            expandable=True,
        )


def test_candidate_policy_hash_binds_index_and_registry_provenance() -> None:
    policy = CandidatePolicyManifest.build(
        policy_id="policy",
        embedding_model_artifact_id="embedding",
        claim_role_template_hash=HASH,
        chunk_role_template_hash=HASH,
        vector_method_version="reverse-bge-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=HASH,
        vector_search_config_hash=HASH,
        lexical_method_version="lexical-v1",
        lexical_config_hash=HASH,
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        claim_registry_snapshot_id="registry-1",
        claim_count=2,
        fusion_version="rank-interleave-v1",
        approximate_cap_per_inserted_chunk=10,
        frontier_depth=4,
        verifier_execution_spec_hash=HASH,
        decision_policy_version="m3-policy-v1",
    )
    assert policy.policy_hash == policy.expected_policy_hash()

    with pytest.raises(ValidationError, match="policy_hash"):
        CandidatePolicyManifest(
            policy_id=policy.policy_id,
            policy_hash="b" * 64,
            embedding_model_artifact_id=policy.embedding_model_artifact_id,
            claim_role_template_hash=policy.claim_role_template_hash,
            chunk_role_template_hash=policy.chunk_role_template_hash,
            vector_method_version=policy.vector_method_version,
            vector_index_kind=VectorIndexKind.HNSW,
            vector_index_build_config_hash=policy.vector_index_build_config_hash,
            vector_search_config_hash=policy.vector_search_config_hash,
            lexical_method_version=policy.lexical_method_version,
            lexical_config_hash=policy.lexical_config_hash,
            lexical_postgres_version=policy.lexical_postgres_version,
            lexical_regconfig_identity=policy.lexical_regconfig_identity,
            claim_registry_snapshot_id=policy.claim_registry_snapshot_id,
            claim_count=policy.claim_count,
            fusion_version=policy.fusion_version,
            approximate_cap_per_inserted_chunk=(
                policy.approximate_cap_per_inserted_chunk
            ),
            frontier_depth=policy.frontier_depth,
            verifier_execution_spec_hash=policy.verifier_execution_spec_hash,
            decision_policy_version=policy.decision_policy_version,
        )


def test_admitted_pair_deduplicates_execution_but_retains_reasons() -> None:
    pair = AdmittedPair(
        epoch_id=1,
        pair=PairKey("claim", "chunk"),
        candidate_policy_id="policy",
        fused_rank=1,
        reasons=(
            AdmissionChannel.LEXICAL,
            AdmissionChannel.LINEAGE,
            AdmissionChannel.VECTOR,
        ),
        mandatory_lineage=True,
    )
    assert len(pair.reasons) == 3

    with pytest.raises(ValidationError, match="sorted and unique"):
        AdmittedPair(
            epoch_id=1,
            pair=pair.pair,
            candidate_policy_id="policy",
            fused_rank=1,
            reasons=(AdmissionChannel.VECTOR, AdmissionChannel.LEXICAL),
            mandatory_lineage=False,
        )


def test_teacher_and_human_judgments_have_distinct_payload_rules() -> None:
    teacher = _judgment("claim", "chunk")
    human = PairJudgment(
        pair=teacher.pair,
        source_kind=JudgmentSourceKind.HUMAN,
        source_artifact_id="annotator-round-1",
        decision_policy_or_guideline_id="guideline-v1",
        derived_label=VerificationLabel.SUPPORT,
        input_hash=HASH,
        split_id="human-test",
    )
    assert teacher.source_kind is not human.source_kind

    with pytest.raises(ValidationError, match="must not masquerade"):
        PairJudgment(
            pair=teacher.pair,
            source_kind=JudgmentSourceKind.HUMAN,
            source_artifact_id="human",
            decision_policy_or_guideline_id="guide",
            derived_label=VerificationLabel.SUPPORT,
            input_hash=HASH,
            split_id="test",
            support_score=1.0,
        )


def test_full_pair_audit_rejects_missing_or_selective_pair_sets() -> None:
    judgments = tuple(
        _judgment(claim_id, chunk_id)
        for claim_id in ("c1", "c2")
        for chunk_id in ("p1", "p2")
    )
    result = FullPairAuditResult(
        event_id="event",
        inserted_active_chunk_ids=("p1", "p2"),
        registered_claim_ids=("c1", "c2"),
        judgments=judgments,
        manifest_id="full-pair-v1",
    )
    assert result.expected_pair_count == 4

    with pytest.raises(ValidationError, match="exact Cartesian"):
        FullPairAuditResult(
            event_id="event",
            inserted_active_chunk_ids=("p1", "p2"),
            registered_claim_ids=("c1", "c2"),
            judgments=judgments[:-1],
            manifest_id="selective-is-not-an-oracle",
        )


def test_affected_sets_enforce_only_same_baseline_containment() -> None:
    sets = AffectedSets(
        baseline_id="exhaustive-total",
        materialized_state_claim_ids=("c1", "c2", "c3"),
        decision_summary_claim_ids=("c1", "c2"),
        status_claim_ids=("c1",),
        answer_status_ids=("a1",),
    )
    assert sets.answer_status_ids == ("a1",)

    with pytest.raises(ValidationError, match="containment"):
        AffectedSets(
            baseline_id="broken",
            materialized_state_claim_ids=("c1",),
            decision_summary_claim_ids=("c1",),
            status_claim_ids=("c2",),
            answer_status_ids=(),
        )
