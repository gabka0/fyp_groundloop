"""Contract tests for the coordinator-owned M4 boundary."""

import pytest

from groundloop.domain import VerificationLabel
from groundloop.errors import ValidationError
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    AffectedSets,
    ChildClosure,
    FullPairAuditResult,
    JobCompletion,
    JobKind,
    JobState,
    JudgmentSourceKind,
    LogicalJobSpec,
    PairJudgment,
    PairKey,
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
            job_id="job",
            event_id="event",
            kind=JobKind.VERIFY_PAIR,
            candidate_policy_id="candidate-policy",
            payload_hash=HASH,
            execution_spec_hash=HASH,
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
