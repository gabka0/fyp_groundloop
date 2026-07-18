"""Pure full-recomputation reference semantics (the M1 oracle).

Every function recomputes from scratch over the repository snapshot. Nothing
here mutates state, and nothing here is incremental: this module defines the
semantics that every later incremental engine must match exactly.

Counts follow D-11: distinct normalized text hashes of contributing active
chunks, so textual duplicates never inflate witness counts. Contributing
observation identifiers are reported in full for provenance and full-state
test comparisons.
"""

from groundloop.domain import (
    AnswerState,
    AnswerStatus,
    ClaimState,
    ClaimStatus,
    SubjectKind,
    VerificationLabel,
)
from groundloop.policy import decide
from groundloop.repository import InMemoryRepository


def compute_claim_state(repo: InMemoryRepository, claim_id: str) -> ClaimState:
    """Recompute one claim's state from active, current observations."""
    repo.claim(claim_id)  # raises on dangling reference
    policy = repo.current_policy()
    support_hashes: set[str] = set()
    refute_hashes: set[str] = set()
    supporting_ids: list[str] = []
    refuting_ids: list[str] = []

    for observation in repo.current_observations():
        if observation.subject_kind is not SubjectKind.CLAIM:
            continue
        if observation.subject_id != claim_id:
            continue
        if not repo.is_chunk_active(observation.chunk_version_id):
            continue
        label = decide(observation, policy)
        if label is VerificationLabel.NEUTRAL:
            continue
        text_hash = repo.chunk_version(observation.chunk_version_id).text_hash
        if label is VerificationLabel.SUPPORT:
            support_hashes.add(text_hash)
            supporting_ids.append(observation.observation_id)
        else:
            refute_hashes.add(text_hash)
            refuting_ids.append(observation.observation_id)

    supported = len(support_hashes) > 0
    refuted = len(refute_hashes) > 0
    if supported and refuted:
        status = ClaimStatus.CONFLICTED
    elif supported:
        status = ClaimStatus.SUPPORTED
    elif refuted:
        status = ClaimStatus.REFUTED
    else:
        status = ClaimStatus.UNSUPPORTED

    return ClaimState(
        claim_id=claim_id,
        support_count=len(support_hashes),
        refute_count=len(refute_hashes),
        best_support_score=max(
            (
                repo.observation(observation_id).support_score
                for observation_id in supporting_ids
            ),
            default=None,
        ),
        best_refute_score=max(
            (
                repo.observation(observation_id).refute_score
                for observation_id in refuting_ids
            ),
            default=None,
        ),
        supporting_observation_ids=tuple(sorted(supporting_ids)),
        refuting_observation_ids=tuple(sorted(refuting_ids)),
        status=status,
    )


def compute_answer_state(
    repo: InMemoryRepository,
    answer_version_id: str,
    claim_states: dict[str, ClaimState],
) -> AnswerState:
    """Aggregate an answer's state over its required claims only."""
    repo.answer(answer_version_id)  # raises on dangling reference
    required_states = [
        claim_states[claim_id]
        for claim_id in repo.claim_ids_of_answer(answer_version_id)
        if repo.claim(claim_id).required
    ]
    supported = sum(1 for s in required_states if s.status is ClaimStatus.SUPPORTED)
    unsupported = sum(1 for s in required_states if s.status is ClaimStatus.UNSUPPORTED)
    refuted = sum(1 for s in required_states if s.status is ClaimStatus.REFUTED)
    conflicted = sum(1 for s in required_states if s.status is ClaimStatus.CONFLICTED)

    if refuted > 0:
        status = AnswerStatus.CONTRADICTED
    elif conflicted > 0:
        status = AnswerStatus.CONFLICTED
    elif required_states and supported == len(required_states):
        status = AnswerStatus.VALID
    elif supported > 0:
        status = AnswerStatus.PARTIALLY_SUPPORTED
    else:
        status = AnswerStatus.UNSUPPORTED

    return AnswerState(
        answer_version_id=answer_version_id,
        required_claim_count=len(required_states),
        supported_count=supported,
        unsupported_count=unsupported,
        refuted_count=refuted,
        conflicted_count=conflicted,
        status=status,
    )


def compute_all_states(
    repo: InMemoryRepository,
) -> tuple[dict[str, ClaimState], dict[str, AnswerState]]:
    """Recompute every registered claim and answer state from scratch."""
    claim_states = {
        claim_id: compute_claim_state(repo, claim_id)
        for claim_id in repo.all_claim_ids()
    }
    answer_states = {
        answer_id: compute_answer_state(repo, answer_id, claim_states)
        for answer_id in repo.all_answer_ids()
    }
    return claim_states, answer_states
