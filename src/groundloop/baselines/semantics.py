"""Independent structured reference functions used only by baseline engines.

This module deliberately does not import ``groundloop.reference`` or the
incremental engine.  It restates the frozen direct-witness semantics so the
global and keyed recomputation baselines are independent implementations.
"""

from __future__ import annotations

from dataclasses import dataclass

from groundloop.domain import (
    AnswerState,
    AnswerStatus,
    ClaimState,
    ClaimStatus,
    SubjectKind,
    VerificationLabel,
)
from groundloop.events import (
    DeleteDocumentVersionEvent,
    Event,
    ObserveEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
)
from groundloop.policy import decide
from groundloop.repository import InMemoryRepository


@dataclass(frozen=True, slots=True)
class SemanticWork:
    claim_keys: int = 0
    answer_keys: int = 0
    observations_scanned: int = 0
    candidates: int = 0


def recompute_claim(
    repository: InMemoryRepository, claim_id: str
) -> tuple[ClaimState, int]:
    """Evaluate one direct-witness claim from base records."""
    repository.claim(claim_id)
    policy = repository.current_policy()
    support_hashes: set[str] = set()
    refute_hashes: set[str] = set()
    supporting_ids: list[str] = []
    refuting_ids: list[str] = []
    scanned = 0
    for observation in repository.current_observations():
        scanned += 1
        if observation.subject_kind is not SubjectKind.CLAIM:
            continue
        if observation.subject_id != claim_id:
            continue
        if not repository.is_chunk_active(observation.chunk_version_id):
            continue
        label = decide(observation, policy)
        text_hash = repository.chunk_version(observation.chunk_version_id).text_hash
        if label is VerificationLabel.SUPPORT:
            support_hashes.add(text_hash)
            supporting_ids.append(observation.observation_id)
        elif label is VerificationLabel.REFUTE:
            refute_hashes.add(text_hash)
            refuting_ids.append(observation.observation_id)

    if support_hashes and refute_hashes:
        status = ClaimStatus.CONFLICTED
    elif support_hashes:
        status = ClaimStatus.SUPPORTED
    elif refute_hashes:
        status = ClaimStatus.REFUTED
    else:
        status = ClaimStatus.UNSUPPORTED
    state = ClaimState(
        claim_id=claim_id,
        support_count=len(support_hashes),
        refute_count=len(refute_hashes),
        best_support_score=max(
            (
                repository.observation(observation_id).support_score
                for observation_id in supporting_ids
            ),
            default=None,
        ),
        best_refute_score=max(
            (
                repository.observation(observation_id).refute_score
                for observation_id in refuting_ids
            ),
            default=None,
        ),
        supporting_observation_ids=tuple(sorted(supporting_ids)),
        refuting_observation_ids=tuple(sorted(refuting_ids)),
        status=status,
    )
    return state, scanned


def recompute_answer(
    repository: InMemoryRepository,
    answer_id: str,
    claim_states: dict[str, ClaimState],
) -> AnswerState:
    required = [
        claim_states[claim_id]
        for claim_id in repository.claim_ids_of_answer(answer_id)
        if repository.claim(claim_id).required
    ]
    supported = sum(state.status is ClaimStatus.SUPPORTED for state in required)
    unsupported = sum(state.status is ClaimStatus.UNSUPPORTED for state in required)
    refuted = sum(state.status is ClaimStatus.REFUTED for state in required)
    conflicted = sum(state.status is ClaimStatus.CONFLICTED for state in required)
    if refuted:
        status = AnswerStatus.CONTRADICTED
    elif conflicted:
        status = AnswerStatus.CONFLICTED
    elif supported == len(required):
        status = AnswerStatus.VALID
    elif supported:
        status = AnswerStatus.PARTIALLY_SUPPORTED
    else:
        status = AnswerStatus.UNSUPPORTED
    return AnswerState(
        answer_version_id=answer_id,
        required_claim_count=len(required),
        supported_count=supported,
        unsupported_count=unsupported,
        refuted_count=refuted,
        conflicted_count=conflicted,
        status=status,
    )


def recompute_all(
    repository: InMemoryRepository,
) -> tuple[dict[str, ClaimState], dict[str, AnswerState], SemanticWork]:
    claims: dict[str, ClaimState] = {}
    scanned = 0
    for claim_id in repository.all_claim_ids():
        state, claim_scanned = recompute_claim(repository, claim_id)
        claims[claim_id] = state
        scanned += claim_scanned
    answers = {
        answer_id: recompute_answer(repository, answer_id, claims)
        for answer_id in repository.all_answer_ids()
    }
    return (
        claims,
        answers,
        SemanticWork(
            claim_keys=len(claims),
            answer_keys=len(answers),
            observations_scanned=scanned,
            candidates=scanned,
        ),
    )


def affected_claims(
    event: Event,
    before: InMemoryRepository,
    after: InMemoryRepository,
) -> tuple[set[str], int]:
    """Return an exact affected-claim superset without using derived state."""
    affected: set[str] = set()
    candidates = 0
    if isinstance(event, ObserveEvent):
        if event.observation.subject_kind is SubjectKind.CLAIM:
            affected.add(event.observation.subject_id)
        return affected, 1
    if isinstance(event, PolicyChangeEvent):
        for observation in before.current_observations():
            if not before.is_chunk_active(observation.chunk_version_id):
                continue
            candidates += 1
            if observation.subject_kind is SubjectKind.CLAIM and decide(
                observation, before.current_policy()
            ) is not decide(observation, after.current_policy()):
                affected.add(observation.subject_id)
        return affected, candidates
    document_version_id: str | None = None
    if isinstance(event, DeleteDocumentVersionEvent):
        document_version_id = event.document_version_id
    elif isinstance(event, ReplaceDocumentVersionEvent):
        document_version_id = event.old_document_version_id
    if document_version_id is None:
        return affected, candidates
    for chunk_id in before.chunk_ids_of_document_version(document_version_id):
        for observation_id in before.current_observation_ids_for_chunk(chunk_id):
            candidates += 1
            observation = before.observation(observation_id)
            if observation.subject_kind is SubjectKind.CLAIM:
                affected.add(observation.subject_id)
    return affected, candidates
