"""Independent direct-witness recomputation from immutable pair judgments."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from groundloop.domain import (
    AnswerState,
    AnswerStatus,
    ClaimState,
    ClaimStatus,
    VerificationLabel,
)
from groundloop.errors import ValidationError
from groundloop.m4.contracts import (
    JudgmentSourceKind,
    PairJudgment,
    PairKey,
    stable_m4_digest,
)


def _observation_id(judgment: PairJudgment) -> str:
    return "refresh-observation-" + stable_m4_digest(
        "m4-refresh-observation-v1",
        judgment.pair.claim_id,
        judgment.pair.chunk_version_id,
        judgment.source_kind.value,
        judgment.source_artifact_id,
        judgment.decision_policy_or_guideline_id,
        judgment.input_hash,
    )


def _claim_status(supported: bool, refuted: bool) -> ClaimStatus:
    if supported and refuted:
        return ClaimStatus.CONFLICTED
    if supported:
        return ClaimStatus.SUPPORTED
    if refuted:
        return ClaimStatus.REFUTED
    return ClaimStatus.UNSUPPORTED


def _answer_status(required: tuple[ClaimState, ...]) -> AnswerStatus:
    refuted = sum(state.status is ClaimStatus.REFUTED for state in required)
    conflicted = sum(state.status is ClaimStatus.CONFLICTED for state in required)
    supported = sum(state.status is ClaimStatus.SUPPORTED for state in required)
    if refuted:
        return AnswerStatus.CONTRADICTED
    if conflicted:
        return AnswerStatus.CONFLICTED
    if required and supported == len(required):
        return AnswerStatus.VALID
    if supported:
        return AnswerStatus.PARTIALLY_SUPPORTED
    return AnswerStatus.UNSUPPORTED


def recompute_grounding_states(
    *,
    claim_to_answer: Mapping[str, str],
    required_claim_ids: frozenset[str],
    chunk_text_hashes: Mapping[str, str],
    judgments: Iterable[PairJudgment],
) -> tuple[tuple[ClaimState, ...], tuple[AnswerState, ...]]:
    """Recompute direct-witness states without M1/M2 semantic code sharing."""
    claim_ids = tuple(sorted(claim_to_answer))
    if not claim_ids:
        raise ValidationError("grounding recomputation requires registered claims")
    if any(not answer_id.strip() for answer_id in claim_to_answer.values()):
        raise ValidationError("claim-to-answer mapping contains an empty answer ID")
    unknown_required = required_claim_ids - set(claim_ids)
    if unknown_required:
        raise ValidationError("required claims must belong to the claim registry")
    answer_ids = tuple(sorted(set(claim_to_answer.values())))
    for answer_id in answer_ids:
        if not any(
            claim_id in required_claim_ids
            and claim_to_answer[claim_id] == answer_id
            for claim_id in claim_ids
        ):
            raise ValidationError(f"answer {answer_id} has no required claim")

    by_claim: dict[str, list[PairJudgment]] = {claim_id: [] for claim_id in claim_ids}
    seen_pairs: set[PairKey] = set()
    for judgment in judgments:
        if judgment.source_kind is not JudgmentSourceKind.MODEL:
            raise ValidationError("grounding states require model score judgments")
        if judgment.pair in seen_pairs:
            raise ValidationError("grounding recomputation received duplicate pairs")
        seen_pairs.add(judgment.pair)
        if judgment.pair.claim_id not in by_claim:
            raise ValidationError("judgment references an unregistered claim")
        if judgment.pair.chunk_version_id not in chunk_text_hashes:
            raise ValidationError("judgment references an unknown active chunk")
        by_claim[judgment.pair.claim_id].append(judgment)

    claim_states: list[ClaimState] = []
    for claim_id in claim_ids:
        supporting = tuple(
            sorted(
                (
                    _observation_id(judgment),
                    judgment,
                )
                for judgment in by_claim[claim_id]
                if judgment.derived_label is VerificationLabel.SUPPORT
            )
        )
        refuting = tuple(
            sorted(
                (
                    _observation_id(judgment),
                    judgment,
                )
                for judgment in by_claim[claim_id]
                if judgment.derived_label is VerificationLabel.REFUTE
            )
        )
        support_hashes = {
            chunk_text_hashes[judgment.pair.chunk_version_id]
            for _, judgment in supporting
        }
        refute_hashes = {
            chunk_text_hashes[judgment.pair.chunk_version_id]
            for _, judgment in refuting
        }
        support_scores = tuple(
            float(judgment.support_score)
            for _, judgment in supporting
            if judgment.support_score is not None
        )
        refute_scores = tuple(
            float(judgment.refute_score)
            for _, judgment in refuting
            if judgment.refute_score is not None
        )
        claim_states.append(
            ClaimState(
                claim_id=claim_id,
                support_count=len(support_hashes),
                refute_count=len(refute_hashes),
                best_support_score=max(support_scores, default=None),
                best_refute_score=max(refute_scores, default=None),
                supporting_observation_ids=tuple(item[0] for item in supporting),
                refuting_observation_ids=tuple(item[0] for item in refuting),
                status=_claim_status(bool(support_hashes), bool(refute_hashes)),
            )
        )

    by_claim_id = {state.claim_id: state for state in claim_states}
    answer_states: list[AnswerState] = []
    for answer_id in answer_ids:
        required = tuple(
            by_claim_id[claim_id]
            for claim_id in claim_ids
            if claim_id in required_claim_ids
            and claim_to_answer[claim_id] == answer_id
        )
        supported = sum(
            state.status is ClaimStatus.SUPPORTED for state in required
        )
        unsupported = sum(
            state.status is ClaimStatus.UNSUPPORTED for state in required
        )
        refuted = sum(state.status is ClaimStatus.REFUTED for state in required)
        conflicted = sum(
            state.status is ClaimStatus.CONFLICTED for state in required
        )
        answer_states.append(
            AnswerState(
                answer_version_id=answer_id,
                required_claim_count=len(required),
                supported_count=supported,
                unsupported_count=unsupported,
                refuted_count=refuted,
                conflicted_count=conflicted,
                status=_answer_status(required),
            )
        )
    return tuple(claim_states), tuple(answer_states)
