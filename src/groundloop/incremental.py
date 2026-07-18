"""Independent signed-delta maintenance for GroundLoop M2.

The engine never calls the reference recomputation functions to update its
state. It consumes a committed event plus the before/after base repositories,
maintains distinct-content hash refcounts, score multisets, decisions,
certificates, claim states, and answer aggregates, and exposes complete states
for differential comparison with the M1 oracle.

M2 currently covers the direct-witness CORE. Evidence groups arrive in M5.
"""

from __future__ import annotations

import heapq
from bisect import bisect_left, insort
from collections import Counter
from dataclasses import dataclass, field

from groundloop.domain import (
    AnswerState,
    AnswerStatus,
    ClaimState,
    ClaimStatus,
    DecisionPolicy,
    SemanticObservation,
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
class ClaimCertificate:
    """Compact direct-witness certificate for one claim."""

    claim_id: str
    support_observation_id: str | None
    refute_observation_id: str | None


@dataclass(frozen=True, slots=True)
class MaintenanceStats:
    """Inspectable work performed for the most recent committed event."""

    active_observations_added: int = 0
    active_observations_removed: int = 0
    decision_flips: int = 0
    policy_index_candidates: int = 0
    distinct_hash_crossings: int = 0
    claim_keys_touched: int = 0
    claim_status_changes: int = 0
    answer_keys_touched: int = 0
    answer_status_changes: int = 0


@dataclass(slots=True)
class _MutableStats:
    active_observations_added: int = 0
    active_observations_removed: int = 0
    decision_flips: int = 0
    policy_index_candidates: int = 0
    distinct_hash_crossings: int = 0
    claim_status_changes: int = 0
    answer_status_changes: int = 0

    def freeze(
        self, touched_claims: set[str], touched_answers: set[str]
    ) -> MaintenanceStats:
        return MaintenanceStats(
            active_observations_added=self.active_observations_added,
            active_observations_removed=self.active_observations_removed,
            decision_flips=self.decision_flips,
            policy_index_candidates=self.policy_index_candidates,
            distinct_hash_crossings=self.distinct_hash_crossings,
            claim_keys_touched=len(touched_claims),
            claim_status_changes=self.claim_status_changes,
            answer_keys_touched=len(touched_answers),
            answer_status_changes=self.answer_status_changes,
        )


@dataclass(slots=True)
class _ScoreMultiset:
    """Counted maximum with lazy heap deletion."""

    counts: dict[float, int] = field(default_factory=dict)
    max_heap: list[float] = field(default_factory=list)

    def add(self, value: float) -> None:
        self.counts[value] = self.counts.get(value, 0) + 1
        heapq.heappush(self.max_heap, -value)

    def remove(self, value: float) -> None:
        count = self.counts.get(value, 0)
        if count <= 0:
            raise AssertionError(f"cannot remove absent score {value}")
        if count == 1:
            del self.counts[value]
        else:
            self.counts[value] = count - 1

    def maximum(self) -> float | None:
        while self.max_heap and -self.max_heap[0] not in self.counts:
            heapq.heappop(self.max_heap)
        return -self.max_heap[0] if self.max_heap else None


@dataclass(slots=True)
class _ClaimAccumulator:
    support_hash_counts: dict[str, int] = field(default_factory=dict)
    refute_hash_counts: dict[str, int] = field(default_factory=dict)
    support_observation_ids: set[str] = field(default_factory=set)
    refute_observation_ids: set[str] = field(default_factory=set)
    support_scores: _ScoreMultiset = field(default_factory=_ScoreMultiset)
    refute_scores: _ScoreMultiset = field(default_factory=_ScoreMultiset)


@dataclass(frozen=True, slots=True)
class _Contribution:
    observation_id: str
    claim_id: str
    text_hash: str
    label: VerificationLabel
    score: float


@dataclass(slots=True)
class _ScoreRangeIndex:
    """Active-observation threshold index.

    Python's sorted lists make point insertion/removal O(E); threshold range
    discovery is O(log E + m). PostgreSQL M2 uses B-tree indexes for both
    updates and range scans. This transparent prototype is sufficient to prove
    candidate correctness and measure m without pretending to be the final
    physical structure.
    """

    support_entries: list[tuple[float, str]] = field(default_factory=list)
    refute_entries: list[tuple[float, str]] = field(default_factory=list)

    def add(self, observation: SemanticObservation) -> None:
        insort(
            self.support_entries,
            (observation.support_score, observation.observation_id),
        )
        insort(
            self.refute_entries,
            (observation.refute_score, observation.observation_id),
        )

    def remove(self, observation: SemanticObservation) -> None:
        self._remove_entry(
            self.support_entries,
            (observation.support_score, observation.observation_id),
        )
        self._remove_entry(
            self.refute_entries,
            (observation.refute_score, observation.observation_id),
        )

    @staticmethod
    def _remove_entry(
        entries: list[tuple[float, str]], target: tuple[float, str]
    ) -> None:
        position = bisect_left(entries, target)
        if position >= len(entries) or entries[position] != target:
            raise AssertionError(f"score-index entry missing: {target}")
        entries.pop(position)

    @staticmethod
    def _range_ids(
        entries: list[tuple[float, str]], old: float, new: float
    ) -> set[str]:
        if old == new:
            return set()
        lower, upper = sorted((old, new))
        start = bisect_left(entries, (lower, ""))
        stop = bisect_left(entries, (upper, ""))
        return {observation_id for _, observation_id in entries[start:stop]}

    def policy_candidates(self, old: DecisionPolicy, new: DecisionPolicy) -> set[str]:
        if old.tie_rule_version != new.tie_rule_version:
            raise AssertionError("M2 range path requires the same frozen tie rule")
        return self._range_ids(
            self.support_entries,
            old.support_threshold,
            new.support_threshold,
        ) | self._range_ids(
            self.refute_entries,
            old.refute_threshold,
            new.refute_threshold,
        )


def _claim_status(support_count: int, refute_count: int) -> ClaimStatus:
    if support_count > 0 and refute_count > 0:
        return ClaimStatus.CONFLICTED
    if support_count > 0:
        return ClaimStatus.SUPPORTED
    if refute_count > 0:
        return ClaimStatus.REFUTED
    return ClaimStatus.UNSUPPORTED


def _answer_status(counts: Counter[ClaimStatus], required: int) -> AnswerStatus:
    if counts[ClaimStatus.REFUTED] > 0:
        return AnswerStatus.CONTRADICTED
    if counts[ClaimStatus.CONFLICTED] > 0:
        return AnswerStatus.CONFLICTED
    if required > 0 and counts[ClaimStatus.SUPPORTED] == required:
        return AnswerStatus.VALID
    if counts[ClaimStatus.SUPPORTED] > 0:
        return AnswerStatus.PARTIALLY_SUPPORTED
    return AnswerStatus.UNSUPPORTED


@dataclass(slots=True)
class IncrementalMaintenanceEngine:
    """Independent direct-witness M2 maintenance engine."""

    _claim_accumulators: dict[str, _ClaimAccumulator] = field(default_factory=dict)
    _claim_states: dict[str, ClaimState] = field(default_factory=dict)
    _answer_status_counts: dict[str, Counter[ClaimStatus]] = field(default_factory=dict)
    _answer_states: dict[str, AnswerState] = field(default_factory=dict)
    _claim_to_answer: dict[str, str] = field(default_factory=dict)
    _claim_required: dict[str, bool] = field(default_factory=dict)
    _answer_to_claims: dict[str, tuple[str, ...]] = field(default_factory=dict)
    _active_labels: dict[str, VerificationLabel] = field(default_factory=dict)
    _contributions: dict[str, _Contribution] = field(default_factory=dict)
    _score_index: _ScoreRangeIndex = field(default_factory=_ScoreRangeIndex)
    _certificates: dict[str, ClaimCertificate] = field(default_factory=dict)
    last_stats: MaintenanceStats = field(default_factory=MaintenanceStats)

    @classmethod
    def from_repository(
        cls, repository: InMemoryRepository
    ) -> IncrementalMaintenanceEngine:
        engine = cls()
        engine.sync_registry(repository)
        for observation in repository.current_observations():
            if repository.is_chunk_active(observation.chunk_version_id):
                engine._activate_observation(
                    observation,
                    repository,
                    repository.current_policy(),
                    set(),
                    _MutableStats(),
                )
        for claim_id in engine._claim_accumulators:
            engine._claim_states[claim_id] = engine._state_from_accumulator(claim_id)
            engine._repair_certificate(claim_id)
        engine._rebuild_answer_aggregates(repository)
        engine.last_stats = MaintenanceStats()
        return engine

    @property
    def claim_states(self) -> dict[str, ClaimState]:
        return dict(self._claim_states)

    @property
    def answer_states(self) -> dict[str, AnswerState]:
        return dict(self._answer_states)

    @property
    def certificates(self) -> dict[str, ClaimCertificate]:
        return dict(self._certificates)

    def sync_registry(self, repository: InMemoryRepository) -> bool:
        """Import newly registered static claims/answers, never observations."""
        registry_changed = False
        for claim_id in repository.all_claim_ids():
            if claim_id in self._claim_accumulators:
                continue
            registry_changed = True
            claim = repository.claim(claim_id)
            self._claim_accumulators[claim_id] = _ClaimAccumulator()
            self._claim_to_answer[claim_id] = claim.answer_version_id
            self._claim_required[claim_id] = claim.required
            self._claim_states[claim_id] = self._state_from_accumulator(claim_id)
            self._repair_certificate(claim_id)
        for answer_id in repository.all_answer_ids():
            claim_ids = repository.claim_ids_of_answer(answer_id)
            known = self._answer_to_claims.get(answer_id)
            if known is not None and known != claim_ids:
                raise AssertionError("claims of an immutable answer changed")
            if known is None:
                registry_changed = True
            self._answer_to_claims[answer_id] = claim_ids
        if registry_changed:
            self._rebuild_answer_aggregates(repository)
        return registry_changed

    def _rebuild_answer_aggregates(self, repository: InMemoryRepository) -> None:
        self._answer_status_counts.clear()
        self._answer_states.clear()
        for answer_id in repository.all_answer_ids():
            counts: Counter[ClaimStatus] = Counter()
            for claim_id in self._answer_to_claims[answer_id]:
                if self._claim_required[claim_id]:
                    counts[self._claim_states[claim_id].status] += 1
            self._answer_status_counts[answer_id] = counts
            self._answer_states[answer_id] = self._state_from_answer_counts(answer_id)

    def apply_committed_event(
        self,
        event: Event,
        before: InMemoryRepository,
        after: InMemoryRepository,
    ) -> None:
        """Maintain state for an event already committed to ``after``.

        ``before`` and ``after`` are used only as base-relation/index access;
        no reference state function is called.
        """
        self.sync_registry(before)
        dirty_claims: set[str] = set()
        dirty_answers: set[str] = set()
        stats = _MutableStats()

        if isinstance(event, DeleteDocumentVersionEvent):
            self._withdraw_document_version(
                event.document_version_id, before, dirty_claims, stats
            )
        elif isinstance(event, ReplaceDocumentVersionEvent):
            self._withdraw_document_version(
                event.old_document_version_id, before, dirty_claims, stats
            )
        elif isinstance(event, ObserveEvent):
            previous_id = before.current_observation_id(event.observation.key)
            if previous_id is not None:
                self._deactivate_observation(
                    before.observation(previous_id), dirty_claims, stats
                )
            if after.is_chunk_active(event.observation.chunk_version_id):
                self._activate_observation(
                    event.observation,
                    after,
                    after.current_policy(),
                    dirty_claims,
                    stats,
                )
        elif isinstance(event, PolicyChangeEvent):
            self._apply_policy_change(before, after, dirty_claims, stats)

        old_claim_statuses = {
            claim_id: self._claim_states[claim_id].status for claim_id in dirty_claims
        }
        for claim_id in sorted(dirty_claims):
            old_state = self._claim_states[claim_id]
            new_state = self._state_from_accumulator(claim_id)
            self._claim_states[claim_id] = new_state
            self._repair_certificate(claim_id)
            if old_state.status is not new_state.status:
                stats.claim_status_changes += 1
                if self._claim_required[claim_id]:
                    answer_id = self._claim_to_answer[claim_id]
                    counts = self._answer_status_counts[answer_id]
                    counts[old_claim_statuses[claim_id]] -= 1
                    counts[new_state.status] += 1
                    dirty_answers.add(answer_id)

        for answer_id in sorted(dirty_answers):
            old = self._answer_states[answer_id]
            new = self._state_from_answer_counts(answer_id)
            self._answer_states[answer_id] = new
            if old.status is not new.status:
                stats.answer_status_changes += 1

        self.last_stats = stats.freeze(dirty_claims, dirty_answers)

    def _withdraw_document_version(
        self,
        document_version_id: str,
        before: InMemoryRepository,
        dirty_claims: set[str],
        stats: _MutableStats,
    ) -> None:
        for chunk_id in before.chunk_ids_of_document_version(document_version_id):
            for observation_id in before.current_observation_ids_for_chunk(chunk_id):
                self._deactivate_observation(
                    before.observation(observation_id), dirty_claims, stats
                )

    def _activate_observation(
        self,
        observation: SemanticObservation,
        repository: InMemoryRepository,
        policy: DecisionPolicy,
        dirty_claims: set[str],
        stats: _MutableStats,
    ) -> None:
        if observation.observation_id in self._active_labels:
            raise AssertionError("observation already active in incremental engine")
        label = decide(observation, policy)
        self._active_labels[observation.observation_id] = label
        self._score_index.add(observation)
        stats.active_observations_added += 1
        if observation.subject_kind is not SubjectKind.CLAIM:
            return
        if label is VerificationLabel.NEUTRAL:
            return
        text_hash = repository.chunk_version(observation.chunk_version_id).text_hash
        score = (
            observation.support_score
            if label is VerificationLabel.SUPPORT
            else observation.refute_score
        )
        contribution = _Contribution(
            observation_id=observation.observation_id,
            claim_id=observation.subject_id,
            text_hash=text_hash,
            label=label,
            score=score,
        )
        self._contributions[observation.observation_id] = contribution
        self._add_contribution(contribution, dirty_claims, stats)

    def _deactivate_observation(
        self,
        observation: SemanticObservation,
        dirty_claims: set[str],
        stats: _MutableStats,
    ) -> None:
        if observation.observation_id not in self._active_labels:
            return
        self._score_index.remove(observation)
        del self._active_labels[observation.observation_id]
        stats.active_observations_removed += 1
        contribution = self._contributions.pop(observation.observation_id, None)
        if contribution is not None:
            self._remove_contribution(contribution, dirty_claims, stats)

    def _apply_policy_change(
        self,
        before: InMemoryRepository,
        after: InMemoryRepository,
        dirty_claims: set[str],
        stats: _MutableStats,
    ) -> None:
        candidates = self._score_index.policy_candidates(
            before.current_policy(), after.current_policy()
        )
        stats.policy_index_candidates = len(candidates)
        for observation_id in sorted(candidates):
            observation = before.observation(observation_id)
            old_label = self._active_labels[observation_id]
            new_label = decide(observation, after.current_policy())
            if old_label is new_label:
                continue
            stats.decision_flips += 1
            old_contribution = self._contributions.pop(observation_id, None)
            if old_contribution is not None:
                self._remove_contribution(old_contribution, dirty_claims, stats)
            self._active_labels[observation_id] = new_label
            if (
                observation.subject_kind is SubjectKind.CLAIM
                and new_label is not VerificationLabel.NEUTRAL
            ):
                text_hash = before.chunk_version(observation.chunk_version_id).text_hash
                score = (
                    observation.support_score
                    if new_label is VerificationLabel.SUPPORT
                    else observation.refute_score
                )
                new_contribution = _Contribution(
                    observation_id=observation_id,
                    claim_id=observation.subject_id,
                    text_hash=text_hash,
                    label=new_label,
                    score=score,
                )
                self._contributions[observation_id] = new_contribution
                self._add_contribution(new_contribution, dirty_claims, stats)

    def _add_contribution(
        self,
        contribution: _Contribution,
        dirty_claims: set[str],
        stats: _MutableStats,
    ) -> None:
        accumulator = self._claim_accumulators[contribution.claim_id]
        if contribution.label is VerificationLabel.SUPPORT:
            counts = accumulator.support_hash_counts
            ids = accumulator.support_observation_ids
            scores = accumulator.support_scores
        else:
            counts = accumulator.refute_hash_counts
            ids = accumulator.refute_observation_ids
            scores = accumulator.refute_scores
        old_count = counts.get(contribution.text_hash, 0)
        counts[contribution.text_hash] = old_count + 1
        if old_count == 0:
            stats.distinct_hash_crossings += 1
        ids.add(contribution.observation_id)
        scores.add(contribution.score)
        dirty_claims.add(contribution.claim_id)

    def _remove_contribution(
        self,
        contribution: _Contribution,
        dirty_claims: set[str],
        stats: _MutableStats,
    ) -> None:
        accumulator = self._claim_accumulators[contribution.claim_id]
        if contribution.label is VerificationLabel.SUPPORT:
            counts = accumulator.support_hash_counts
            ids = accumulator.support_observation_ids
            scores = accumulator.support_scores
        else:
            counts = accumulator.refute_hash_counts
            ids = accumulator.refute_observation_ids
            scores = accumulator.refute_scores
        old_count = counts.get(contribution.text_hash, 0)
        if old_count <= 0:
            raise AssertionError("cannot remove absent distinct-content contribution")
        if old_count == 1:
            del counts[contribution.text_hash]
            stats.distinct_hash_crossings += 1
        else:
            counts[contribution.text_hash] = old_count - 1
        ids.remove(contribution.observation_id)
        scores.remove(contribution.score)
        dirty_claims.add(contribution.claim_id)

    def _state_from_accumulator(self, claim_id: str) -> ClaimState:
        accumulator = self._claim_accumulators[claim_id]
        support_count = len(accumulator.support_hash_counts)
        refute_count = len(accumulator.refute_hash_counts)
        return ClaimState(
            claim_id=claim_id,
            support_count=support_count,
            refute_count=refute_count,
            best_support_score=accumulator.support_scores.maximum(),
            best_refute_score=accumulator.refute_scores.maximum(),
            supporting_observation_ids=tuple(
                sorted(accumulator.support_observation_ids)
            ),
            refuting_observation_ids=tuple(sorted(accumulator.refute_observation_ids)),
            status=_claim_status(support_count, refute_count),
        )

    def _state_from_answer_counts(self, answer_id: str) -> AnswerState:
        counts = self._answer_status_counts[answer_id]
        required = sum(
            1
            for claim_id in self._answer_to_claims[answer_id]
            if self._claim_required[claim_id]
        )
        return AnswerState(
            answer_version_id=answer_id,
            required_claim_count=required,
            supported_count=counts[ClaimStatus.SUPPORTED],
            unsupported_count=counts[ClaimStatus.UNSUPPORTED],
            refuted_count=counts[ClaimStatus.REFUTED],
            conflicted_count=counts[ClaimStatus.CONFLICTED],
            status=_answer_status(counts, required),
        )

    def _repair_certificate(self, claim_id: str) -> None:
        state = self._claim_states[claim_id]
        self._certificates[claim_id] = ClaimCertificate(
            claim_id=claim_id,
            support_observation_id=(
                state.supporting_observation_ids[0]
                if state.supporting_observation_ids
                else None
            ),
            refute_observation_id=(
                state.refuting_observation_ids[0]
                if state.refuting_observation_ids
                else None
            ),
        )

    def validate_certificates(self) -> None:
        for claim_id, state in self._claim_states.items():
            certificate = self._certificates[claim_id]
            if state.support_count > 0:
                assert certificate.support_observation_id in (
                    state.supporting_observation_ids
                )
            else:
                assert certificate.support_observation_id is None
            if state.refute_count > 0:
                assert certificate.refute_observation_id in (
                    state.refuting_observation_ids
                )
            else:
                assert certificate.refute_observation_id is None
