"""Same-baseline affected-set computation for exhaustive M4 comparisons."""

from __future__ import annotations

from collections.abc import Iterable

from groundloop.domain import AnswerState, ClaimState
from groundloop.errors import ValidationError
from groundloop.m4.contracts import AffectedSets, FullPairAuditResult


def _claim_map(states: Iterable[ClaimState]) -> dict[str, ClaimState]:
    result: dict[str, ClaimState] = {}
    for state in states:
        if state.claim_id in result:
            raise ValidationError(f"duplicate claim state {state.claim_id}")
        result[state.claim_id] = state
    return result


def _answer_map(states: Iterable[AnswerState]) -> dict[str, AnswerState]:
    result: dict[str, AnswerState] = {}
    for state in states:
        if state.answer_version_id in result:
            raise ValidationError(
                f"duplicate answer state {state.answer_version_id}"
            )
        result[state.answer_version_id] = state
    return result


def _decision_summary(state: ClaimState) -> tuple[object, ...]:
    return (
        state.support_count,
        state.refute_count,
        state.best_support_score,
        state.best_refute_score,
        state.status,
    )


def compute_affected_sets(
    *,
    baseline_id: str,
    before_claim_states: Iterable[ClaimState],
    after_claim_states: Iterable[ClaimState],
    before_answer_states: Iterable[AnswerState],
    after_answer_states: Iterable[AnswerState],
) -> AffectedSets:
    """Compare complete rows and their frozen diagnostic projections."""
    before_claims = _claim_map(before_claim_states)
    after_claims = _claim_map(after_claim_states)
    if before_claims.keys() != after_claims.keys():
        raise ValidationError("affected-set claim domains must be identical")
    before_answers = _answer_map(before_answer_states)
    after_answers = _answer_map(after_answer_states)
    if before_answers.keys() != after_answers.keys():
        raise ValidationError("affected-set answer domains must be identical")

    materialized = tuple(
        sorted(
            claim_id
            for claim_id in before_claims
            if before_claims[claim_id] != after_claims[claim_id]
        )
    )
    summary = tuple(
        sorted(
            claim_id
            for claim_id in before_claims
            if _decision_summary(before_claims[claim_id])
            != _decision_summary(after_claims[claim_id])
        )
    )
    status = tuple(
        sorted(
            claim_id
            for claim_id in before_claims
            if before_claims[claim_id].status is not after_claims[claim_id].status
        )
    )
    answers = tuple(
        sorted(
            answer_id
            for answer_id in before_answers
            if before_answers[answer_id].status
            is not after_answers[answer_id].status
        )
    )
    return AffectedSets(
        baseline_id=baseline_id,
        materialized_state_claim_ids=materialized,
        decision_summary_claim_ids=summary,
        status_claim_ids=status,
        answer_status_ids=answers,
    )


def pair_positive_claim_ids(result: FullPairAuditResult) -> tuple[str, ...]:
    """Return the claim projection of verifier-relative positive pairs."""
    return tuple(sorted({pair.claim_id for pair in result.positive_pairs}))
