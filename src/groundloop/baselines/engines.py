"""Exact structured baselines, signed-delta treatment, and heuristic policies."""

from __future__ import annotations

from dataclasses import dataclass

from groundloop.baselines.models import EngineClass
from groundloop.baselines.semantics import (
    affected_claims,
    recompute_all,
    recompute_answer,
    recompute_claim,
)
from groundloop.domain import AnswerState, ClaimState, SubjectKind
from groundloop.events import (
    DeleteDocumentVersionEvent,
    Event,
    ReplaceDocumentVersionEvent,
)
from groundloop.incremental import IncrementalMaintenanceEngine
from groundloop.repository import InMemoryRepository


@dataclass(frozen=True, slots=True)
class EngineEvaluation:
    claim_states: dict[str, ClaimState] | None
    answer_states: dict[str, AnswerState] | None
    claims_touched: int
    answers_touched: int
    observations_touched: int
    candidate_count: int
    status_changes: int
    predicted_invalid_claim_ids: frozenset[str] = frozenset()
    predicted_invalid_answer_ids: frozenset[str] = frozenset()


class FullRecomputationBaseline:
    """Semantics-equivalent global structured recomputation from base records."""

    name = "global_full_recompute"
    engine_class = EngineClass.EXACT_BASELINE
    semantic_equivalent = True

    def __init__(self, repository: InMemoryRepository) -> None:
        claims, answers, _ = recompute_all(repository)
        self.claim_states = claims
        self.answer_states = answers

    def process(
        self,
        event: Event,
        before: InMemoryRepository,
        after: InMemoryRepository,
    ) -> EngineEvaluation:
        del event, before
        old_claims = self.claim_states
        old_answers = self.answer_states
        claims, answers, work = recompute_all(after)
        changes = sum(
            old_claims[claim_id].status is not state.status
            for claim_id, state in claims.items()
        ) + sum(
            old_answers[answer_id].status is not state.status
            for answer_id, state in answers.items()
        )
        self.claim_states = claims
        self.answer_states = answers
        return EngineEvaluation(
            claim_states=claims,
            answer_states=answers,
            claims_touched=work.claim_keys,
            answers_touched=work.answer_keys,
            observations_touched=work.observations_scanned,
            candidate_count=work.candidates,
            status_changes=changes,
        )


class KeyedRecomputationBaseline:
    """Semantics-equivalent recomputation restricted to exactly affected keys.

    Candidate discovery and per-key recomputation are independent of both the
    global baseline and the signed-delta engine.  This is not a heuristic: the
    affected-key rules form an exact superset for every currently supported
    structured event type.
    """

    name = "keyed_affected_recompute"
    engine_class = EngineClass.EXACT_BASELINE
    semantic_equivalent = True

    def __init__(self, repository: InMemoryRepository) -> None:
        claims, answers, _ = recompute_all(repository)
        self.claim_states = claims
        self.answer_states = answers

    def process(
        self,
        event: Event,
        before: InMemoryRepository,
        after: InMemoryRepository,
    ) -> EngineEvaluation:
        affected, candidate_count = affected_claims(event, before, after)
        observations_scanned = 0
        old_claim_statuses = {
            claim_id: self.claim_states[claim_id].status for claim_id in affected
        }
        affected_answers: set[str] = set()
        for claim_id in sorted(affected):
            state, scanned = recompute_claim(after, claim_id)
            self.claim_states[claim_id] = state
            observations_scanned += scanned
            affected_answers.add(after.claim(claim_id).answer_version_id)
        old_answer_statuses = {
            answer_id: self.answer_states[answer_id].status
            for answer_id in affected_answers
        }
        for answer_id in sorted(affected_answers):
            self.answer_states[answer_id] = recompute_answer(
                after, answer_id, self.claim_states
            )
        changes = sum(
            old_claim_statuses[claim_id] is not self.claim_states[claim_id].status
            for claim_id in affected
        ) + sum(
            old_answer_statuses[answer_id] is not self.answer_states[answer_id].status
            for answer_id in affected_answers
        )
        return EngineEvaluation(
            claim_states=dict(self.claim_states),
            answer_states=dict(self.answer_states),
            claims_touched=len(affected),
            answers_touched=len(affected_answers),
            observations_touched=observations_scanned,
            candidate_count=candidate_count,
            status_changes=changes,
        )


class SignedDeltaTreatment:
    """Read-only wrapper around the existing M2 signed-delta engine."""

    name = "signed_delta_treatment"
    engine_class = EngineClass.TREATMENT
    semantic_equivalent = True

    def __init__(self, repository: InMemoryRepository) -> None:
        self.engine = IncrementalMaintenanceEngine.from_repository(repository)

    @property
    def claim_states(self) -> dict[str, ClaimState]:
        return self.engine.claim_states

    @property
    def answer_states(self) -> dict[str, AnswerState]:
        return self.engine.answer_states

    def process(
        self,
        event: Event,
        before: InMemoryRepository,
        after: InMemoryRepository,
    ) -> EngineEvaluation:
        self.engine.apply_committed_event(event, before, after)
        stats = self.engine.last_stats
        return EngineEvaluation(
            claim_states=self.engine.claim_states,
            answer_states=self.engine.answer_states,
            claims_touched=stats.claim_keys_touched,
            answers_touched=stats.answer_keys_touched,
            observations_touched=(
                stats.active_observations_added + stats.active_observations_removed
            ),
            candidate_count=stats.policy_index_candidates,
            status_changes=stats.claim_status_changes + stats.answer_status_changes,
        )


def _direct_dependencies(
    event: Event, before: InMemoryRepository
) -> tuple[set[str], set[str], int]:
    document_version_id: str | None = None
    if isinstance(event, DeleteDocumentVersionEvent):
        document_version_id = event.document_version_id
    elif isinstance(event, ReplaceDocumentVersionEvent):
        document_version_id = event.old_document_version_id
    if document_version_id is None:
        return set(), set(), 0
    claims: set[str] = set()
    candidates = 0
    for chunk_id in before.chunk_ids_of_document_version(document_version_id):
        for observation_id in before.current_observation_ids_for_chunk(chunk_id):
            candidates += 1
            observation = before.observation(observation_id)
            if observation.subject_kind is SubjectKind.CLAIM:
                claims.add(observation.subject_id)
    answers = {before.claim(claim_id).answer_version_id for claim_id in claims}
    return claims, answers, candidates


class DirectCitationInvalidationBaseline:
    """Heuristic policy: invalidate every directly dependent claim.

    Its output is an action set, not a GroundLoop state.  Alternative witnesses
    can make the action a false invalidation, so this baseline is never admitted
    to exact-state speedup tables.
    """

    name = "direct_citation_invalidation"
    engine_class = EngineClass.HEURISTIC_POLICY
    semantic_equivalent = False

    def __init__(self, repository: InMemoryRepository) -> None:
        del repository
        self.last_invalid_claims: frozenset[str] = frozenset()
        self.last_invalid_answers: frozenset[str] = frozenset()

    def process(
        self,
        event: Event,
        before: InMemoryRepository,
        after: InMemoryRepository,
    ) -> EngineEvaluation:
        del after
        claims, answers, candidates = _direct_dependencies(event, before)
        self.last_invalid_claims = frozenset(claims)
        self.last_invalid_answers = frozenset(answers)
        return EngineEvaluation(
            claim_states=None,
            answer_states=None,
            claims_touched=len(claims),
            answers_touched=len(answers),
            observations_touched=candidates,
            candidate_count=candidates,
            status_changes=0,
            predicted_invalid_claim_ids=self.last_invalid_claims,
            predicted_invalid_answer_ids=self.last_invalid_answers,
        )


class SourceInvalidationBaseline:
    """Heuristic policy: invalidate every claim in a source-associated answer."""

    name = "source_level_invalidation"
    engine_class = EngineClass.HEURISTIC_POLICY
    semantic_equivalent = False

    def __init__(self, repository: InMemoryRepository) -> None:
        del repository
        self.last_invalid_claims: frozenset[str] = frozenset()
        self.last_invalid_answers: frozenset[str] = frozenset()

    def process(
        self,
        event: Event,
        before: InMemoryRepository,
        after: InMemoryRepository,
    ) -> EngineEvaluation:
        del after
        _, associated_answers, candidates = _direct_dependencies(event, before)
        claims = {
            claim_id
            for answer_id in associated_answers
            for claim_id in before.claim_ids_of_answer(answer_id)
        }
        self.last_invalid_claims = frozenset(claims)
        self.last_invalid_answers = frozenset(associated_answers)
        return EngineEvaluation(
            claim_states=None,
            answer_states=None,
            claims_touched=len(claims),
            answers_touched=len(associated_answers),
            observations_touched=candidates,
            candidate_count=candidates,
            status_changes=0,
            predicted_invalid_claim_ids=self.last_invalid_claims,
            predicted_invalid_answer_ids=self.last_invalid_answers,
        )
