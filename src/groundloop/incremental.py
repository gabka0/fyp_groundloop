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
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from typing import Generic, TypeVar

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
from groundloop.errors import ValidationError
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
    """Inspectable work performed for the most recent committed event.

    ``score_index_shift_*`` retains its original public name for compatibility,
    but after M4.7 it counts AVL node/rotation work and its conservative
    logarithmic upper bound; no Python-list shifting remains.
    """

    active_observations_added: int = 0
    active_observations_removed: int = 0
    decision_flips: int = 0
    policy_index_candidates: int = 0
    distinct_hash_crossings: int = 0
    claim_keys_touched: int = 0
    claim_status_changes: int = 0
    answer_keys_touched: int = 0
    answer_status_changes: int = 0
    score_index_point_updates: int = 0
    score_index_entries_before: int = 0
    score_index_shift_work: int = 0
    score_index_shift_upper_bound: int = 0


@dataclass(slots=True)
class _MutableStats:
    active_observations_added: int = 0
    active_observations_removed: int = 0
    decision_flips: int = 0
    policy_index_candidates: int = 0
    distinct_hash_crossings: int = 0
    claim_status_changes: int = 0
    answer_status_changes: int = 0
    score_index_point_updates: int = 0
    score_index_entries_before: int = 0
    score_index_shift_work: int = 0

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
            score_index_point_updates=self.score_index_point_updates,
            score_index_entries_before=self.score_index_entries_before,
            score_index_shift_work=self.score_index_shift_work,
            score_index_shift_upper_bound=(
                self.score_index_point_updates
                * (
                    4
                    * (
                        self.score_index_entries_before
                        + self.active_observations_added
                        + 1
                    ).bit_length()
                    + 4
                )
            ),
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


@dataclass(frozen=True, slots=True)
class _ScoreMultisetSnapshot:
    """Immutable copy-on-write image of one affected score multiset."""

    counts: tuple[tuple[float, int], ...]
    max_heap: tuple[float, ...]

    @classmethod
    def capture(cls, value: _ScoreMultiset) -> _ScoreMultisetSnapshot:
        return cls(tuple(sorted(value.counts.items())), tuple(value.max_heap))

    def materialize(self) -> _ScoreMultiset:
        return _ScoreMultiset(counts=dict(self.counts), max_heap=list(self.max_heap))


@dataclass(frozen=True, slots=True)
class _ClaimAccumulatorSnapshot:
    """Immutable image of one claim accumulator, never of the whole engine."""

    support_hash_counts: tuple[tuple[str, int], ...]
    refute_hash_counts: tuple[tuple[str, int], ...]
    support_observation_ids: frozenset[str]
    refute_observation_ids: frozenset[str]
    support_scores: _ScoreMultisetSnapshot
    refute_scores: _ScoreMultisetSnapshot

    @classmethod
    def capture(cls, value: _ClaimAccumulator) -> _ClaimAccumulatorSnapshot:
        return cls(
            support_hash_counts=tuple(sorted(value.support_hash_counts.items())),
            refute_hash_counts=tuple(sorted(value.refute_hash_counts.items())),
            support_observation_ids=frozenset(value.support_observation_ids),
            refute_observation_ids=frozenset(value.refute_observation_ids),
            support_scores=_ScoreMultisetSnapshot.capture(value.support_scores),
            refute_scores=_ScoreMultisetSnapshot.capture(value.refute_scores),
        )

    def materialize(self) -> _ClaimAccumulator:
        return _ClaimAccumulator(
            support_hash_counts=dict(self.support_hash_counts),
            refute_hash_counts=dict(self.refute_hash_counts),
            support_observation_ids=set(self.support_observation_ids),
            refute_observation_ids=set(self.refute_observation_ids),
            support_scores=self.support_scores.materialize(),
            refute_scores=self.refute_scores.materialize(),
        )


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class _PointChange(Generic[T]):
    """Preconditioned replacement of one dictionary key."""

    key: str
    before: T | None
    after: T | None


@dataclass(frozen=True, slots=True)
class _ScoreIndexChange:
    observation: SemanticObservation
    add: bool


@dataclass(frozen=True, slots=True)
class IncrementalStatePatch:
    """Immutable, affected-key transaction prepared from one committed event.

    The private tuples are deliberately point-granular. Applying this object
    never copies an engine-wide dictionary. A patch is revision-bound and can
    therefore be applied exactly once and only to the state from which it was
    prepared.
    """

    event_id: str
    expected_state_revision: int
    active_label_changes: tuple[_PointChange[VerificationLabel], ...]
    contribution_changes: tuple[_PointChange[_Contribution], ...]
    accumulator_changes: tuple[_PointChange[_ClaimAccumulatorSnapshot], ...]
    claim_state_changes: tuple[_PointChange[ClaimState], ...]
    certificate_changes: tuple[_PointChange[ClaimCertificate], ...]
    answer_count_changes: tuple[
        _PointChange[tuple[tuple[ClaimStatus, int], ...]], ...
    ]
    answer_state_changes: tuple[_PointChange[AnswerState], ...]
    score_index_changes: tuple[_ScoreIndexChange, ...]
    stats: MaintenanceStats

    @property
    def touched_claim_ids(self) -> tuple[str, ...]:
        return tuple(change.key for change in self.claim_state_changes)

    @property
    def touched_answer_ids(self) -> tuple[str, ...]:
        return tuple(change.key for change in self.answer_state_changes)


_ScoreKey = tuple[float, str]


@dataclass(slots=True)
class _AVLNode:
    key: _ScoreKey
    left: _AVLNode | None = None
    right: _AVLNode | None = None
    height: int = 1


@dataclass(slots=True)
class _AVLSet:
    """Deterministic AVL set with worst-case logarithmic point updates."""

    root: _AVLNode | None = None
    size: int = 0

    def __len__(self) -> int:
        return self.size

    @staticmethod
    def _height(node: _AVLNode | None) -> int:
        return 0 if node is None else node.height

    @classmethod
    def _refresh(cls, node: _AVLNode) -> None:
        node.height = 1 + max(cls._height(node.left), cls._height(node.right))

    @classmethod
    def _rotate_left(cls, node: _AVLNode) -> _AVLNode:
        pivot = node.right
        assert pivot is not None
        node.right = pivot.left
        pivot.left = node
        cls._refresh(node)
        cls._refresh(pivot)
        return pivot

    @classmethod
    def _rotate_right(cls, node: _AVLNode) -> _AVLNode:
        pivot = node.left
        assert pivot is not None
        node.left = pivot.right
        pivot.right = node
        cls._refresh(node)
        cls._refresh(pivot)
        return pivot

    @classmethod
    def _rebalance(cls, node: _AVLNode) -> tuple[_AVLNode, int]:
        cls._refresh(node)
        balance = cls._height(node.left) - cls._height(node.right)
        rotations = 0
        if balance > 1:
            assert node.left is not None
            if cls._height(node.left.left) < cls._height(node.left.right):
                node.left = cls._rotate_left(node.left)
                rotations += 1
            return cls._rotate_right(node), rotations + 1
        if balance < -1:
            assert node.right is not None
            if cls._height(node.right.right) < cls._height(node.right.left):
                node.right = cls._rotate_right(node.right)
                rotations += 1
            return cls._rotate_left(node), rotations + 1
        return node, rotations

    @classmethod
    def _insert(
        cls, node: _AVLNode | None, key: _ScoreKey
    ) -> tuple[_AVLNode, int]:
        if node is None:
            return _AVLNode(key), 1
        if key == node.key:
            raise AssertionError(f"duplicate score-index entry: {key}")
        if key < node.key:
            node.left, work = cls._insert(node.left, key)
        else:
            node.right, work = cls._insert(node.right, key)
        node, rotations = cls._rebalance(node)
        return node, work + rotations + 1

    def add(self, key: _ScoreKey) -> int:
        self.root, work = self._insert(self.root, key)
        self.size += 1
        return work

    @classmethod
    def _delete(
        cls, node: _AVLNode | None, key: _ScoreKey
    ) -> tuple[_AVLNode | None, int]:
        if node is None:
            raise AssertionError(f"score-index entry missing: {key}")
        work = 1
        if key < node.key:
            node.left, child_work = cls._delete(node.left, key)
            work += child_work
        elif key > node.key:
            node.right, child_work = cls._delete(node.right, key)
            work += child_work
        elif node.left is None:
            return node.right, work
        elif node.right is None:
            return node.left, work
        else:
            successor = node.right
            while successor.left is not None:
                successor = successor.left
                work += 1
            node.key = successor.key
            node.right, child_work = cls._delete(node.right, successor.key)
            work += child_work
        node, rotations = cls._rebalance(node)
        return node, work + rotations

    def remove(self, key: _ScoreKey) -> int:
        self.root, work = self._delete(self.root, key)
        self.size -= 1
        return work

    def contains(self, key: _ScoreKey) -> bool:
        node = self.root
        while node is not None:
            if key == node.key:
                return True
            node = node.left if key < node.key else node.right
        return False

    def ids_in_score_range(self, lower: float, upper: float) -> set[str]:
        result: set[str] = set()
        lower_key = (lower, "")
        upper_key = (upper, "")

        def visit(node: _AVLNode | None) -> None:
            if node is None:
                return
            if node.key >= lower_key:
                visit(node.left)
            if lower_key <= node.key < upper_key:
                _, observation_id = node.key
                result.add(observation_id)
            if node.key < upper_key:
                visit(node.right)

        visit(self.root)
        return result


@dataclass(slots=True)
class _ScoreRangeIndex:
    """AVL-backed active-observation threshold index.

    Point insertion, deletion, and membership are worst-case ``O(log E)``;
    threshold discovery is ``O(log E + m)`` for ``m`` returned candidates.
    """

    support_entries: _AVLSet = field(default_factory=_AVLSet)
    refute_entries: _AVLSet = field(default_factory=_AVLSet)

    def add(self, observation: SemanticObservation) -> int:
        return self.support_entries.add(
            (observation.support_score, observation.observation_id)
        ) + self.refute_entries.add(
            (observation.refute_score, observation.observation_id)
        )

    def remove(self, observation: SemanticObservation) -> int:
        return self.support_entries.remove(
            (observation.support_score, observation.observation_id)
        ) + self.refute_entries.remove(
            (observation.refute_score, observation.observation_id)
        )

    def contains(self, observation: SemanticObservation) -> bool:
        """Return whether both score entries for ``observation`` are present."""
        return self.support_entries.contains(
            (observation.support_score, observation.observation_id)
        ) and self.refute_entries.contains(
            (observation.refute_score, observation.observation_id)
        )

    @staticmethod
    def _range_ids(
        entries: _AVLSet, old: float, new: float
    ) -> set[str]:
        if old == new:
            return set()
        lower, upper = sorted((old, new))
        return entries.ids_in_score_range(lower, upper)

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
    _answer_required_counts: dict[str, int] = field(default_factory=dict)
    _active_labels: dict[str, VerificationLabel] = field(default_factory=dict)
    _contributions: dict[str, _Contribution] = field(default_factory=dict)
    _score_index: _ScoreRangeIndex = field(default_factory=_ScoreRangeIndex)
    _certificates: dict[str, ClaimCertificate] = field(default_factory=dict)
    _state_revision: int = 0
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

    def claim_state(self, claim_id: str) -> ClaimState:
        """Return one maintained claim state without copying the state map."""
        return self._claim_states[claim_id]

    def answer_state(self, answer_id: str) -> AnswerState:
        """Return one maintained answer state without copying the state map."""
        return self._answer_states[answer_id]

    def certificate(self, claim_id: str) -> ClaimCertificate:
        """Return one maintained direct-witness certificate."""
        return self._certificates[claim_id]

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
            self._answer_required_counts[answer_id] = sum(
                1 for claim_id in claim_ids if self._claim_required[claim_id]
            )
        if registry_changed:
            self._rebuild_answer_aggregates(repository)
            self._state_revision += 1
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
        stats.score_index_entries_before = len(self._score_index.support_entries)

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
        self._state_revision += 1

    def prepare_committed_event_patch(
        self,
        event: Event,
        before: InMemoryRepository,
        after: InMemoryRepository,
    ) -> IncrementalStatePatch:
        """Prepare an immutable affected-key transaction without mutation.

        The claim/answer registry must already be synchronized. This explicit
        precondition prevents a nominal point-update path from hiding the
        engine-wide answer rebuild performed by :meth:`sync_registry`. The
        method deliberately does not scan the registry to re-check that
        caller-owned precondition; an affected unknown key fails at point
        lookup.
        """
        label_values: dict[str, VerificationLabel | None] = {}
        contribution_values: dict[str, _Contribution | None] = {}
        accumulator_before: dict[str, _ClaimAccumulatorSnapshot] = {}
        accumulators: dict[str, _ClaimAccumulator] = {}
        score_changes: list[_ScoreIndexChange] = []
        dirty_claims: set[str] = set()
        dirty_answers: set[str] = set()
        stats = _MutableStats(
            score_index_entries_before=len(self._score_index.support_entries)
        )

        def active_label(observation_id: str) -> VerificationLabel | None:
            if observation_id in label_values:
                return label_values[observation_id]
            return self._active_labels.get(observation_id)

        def contribution(observation_id: str) -> _Contribution | None:
            if observation_id in contribution_values:
                return contribution_values[observation_id]
            return self._contributions.get(observation_id)

        def accumulator(claim_id: str) -> _ClaimAccumulator:
            current = accumulators.get(claim_id)
            if current is not None:
                return current
            original = self._claim_accumulators[claim_id]
            snapshot = _ClaimAccumulatorSnapshot.capture(original)
            accumulator_before[claim_id] = snapshot
            current = snapshot.materialize()
            accumulators[claim_id] = current
            return current

        def add_contribution(value: _Contribution) -> None:
            current = accumulator(value.claim_id)
            if value.label is VerificationLabel.SUPPORT:
                counts = current.support_hash_counts
                ids = current.support_observation_ids
                scores = current.support_scores
            else:
                counts = current.refute_hash_counts
                ids = current.refute_observation_ids
                scores = current.refute_scores
            old_count = counts.get(value.text_hash, 0)
            counts[value.text_hash] = old_count + 1
            if old_count == 0:
                stats.distinct_hash_crossings += 1
            ids.add(value.observation_id)
            scores.add(value.score)
            dirty_claims.add(value.claim_id)

        def remove_contribution(value: _Contribution) -> None:
            current = accumulator(value.claim_id)
            if value.label is VerificationLabel.SUPPORT:
                counts = current.support_hash_counts
                ids = current.support_observation_ids
                scores = current.support_scores
            else:
                counts = current.refute_hash_counts
                ids = current.refute_observation_ids
                scores = current.refute_scores
            old_count = counts.get(value.text_hash, 0)
            if old_count <= 0:
                raise AssertionError(
                    "cannot remove absent distinct-content contribution"
                )
            if old_count == 1:
                del counts[value.text_hash]
                stats.distinct_hash_crossings += 1
            else:
                counts[value.text_hash] = old_count - 1
            ids.remove(value.observation_id)
            scores.remove(value.score)
            dirty_claims.add(value.claim_id)

        def deactivate(observation: SemanticObservation) -> None:
            if active_label(observation.observation_id) is None:
                return
            label_values[observation.observation_id] = None
            score_changes.append(_ScoreIndexChange(observation, add=False))
            stats.active_observations_removed += 1
            old_contribution = contribution(observation.observation_id)
            if old_contribution is not None:
                contribution_values[observation.observation_id] = None
                remove_contribution(old_contribution)

        def activate(
            observation: SemanticObservation,
            repository: InMemoryRepository,
            policy: DecisionPolicy,
        ) -> None:
            if active_label(observation.observation_id) is not None:
                raise AssertionError("observation already active in incremental engine")
            label = decide(observation, policy)
            label_values[observation.observation_id] = label
            score_changes.append(_ScoreIndexChange(observation, add=True))
            stats.active_observations_added += 1
            if (
                observation.subject_kind is not SubjectKind.CLAIM
                or label is VerificationLabel.NEUTRAL
            ):
                return
            text_hash = repository.chunk_version(
                observation.chunk_version_id
            ).text_hash
            score = (
                observation.support_score
                if label is VerificationLabel.SUPPORT
                else observation.refute_score
            )
            value = _Contribution(
                observation_id=observation.observation_id,
                claim_id=observation.subject_id,
                text_hash=text_hash,
                label=label,
                score=score,
            )
            contribution_values[observation.observation_id] = value
            add_contribution(value)

        def withdraw_document(document_version_id: str) -> None:
            for chunk_id in before.chunk_ids_of_document_version(document_version_id):
                for observation_id in before.current_observation_ids_for_chunk(
                    chunk_id
                ):
                    deactivate(before.observation(observation_id))

        if isinstance(event, DeleteDocumentVersionEvent):
            withdraw_document(event.document_version_id)
        elif isinstance(event, ReplaceDocumentVersionEvent):
            withdraw_document(event.old_document_version_id)
        elif isinstance(event, ObserveEvent):
            previous_id = before.current_observation_id(event.observation.key)
            if previous_id is not None:
                deactivate(before.observation(previous_id))
            if after.is_chunk_active(event.observation.chunk_version_id):
                activate(event.observation, after, after.current_policy())
        elif isinstance(event, PolicyChangeEvent):
            candidates = self._score_index.policy_candidates(
                before.current_policy(), after.current_policy()
            )
            stats.policy_index_candidates = len(candidates)
            for observation_id in sorted(candidates):
                observation = before.observation(observation_id)
                old_label = active_label(observation_id)
                if old_label is None:
                    raise AssertionError("policy candidate is not active")
                new_label = decide(observation, after.current_policy())
                if old_label is new_label:
                    continue
                stats.decision_flips += 1
                old_contribution = contribution(observation_id)
                if old_contribution is not None:
                    contribution_values[observation_id] = None
                    remove_contribution(old_contribution)
                label_values[observation_id] = new_label
                if (
                    observation.subject_kind is SubjectKind.CLAIM
                    and new_label is not VerificationLabel.NEUTRAL
                ):
                    text_hash = before.chunk_version(
                        observation.chunk_version_id
                    ).text_hash
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
                    contribution_values[observation_id] = new_contribution
                    add_contribution(new_contribution)

        claim_state_changes: list[_PointChange[ClaimState]] = []
        certificate_changes: list[_PointChange[ClaimCertificate]] = []
        answer_counts: dict[str, Counter[ClaimStatus]] = {}
        for claim_id in sorted(dirty_claims):
            old_claim_state = self._claim_states[claim_id]
            new_claim_state = self._state_from_accumulator_value(
                claim_id, accumulators[claim_id]
            )
            claim_state_changes.append(
                _PointChange(claim_id, old_claim_state, new_claim_state)
            )
            old_certificate = self._certificates[claim_id]
            new_certificate = self._certificate_from_state(new_claim_state)
            certificate_changes.append(
                _PointChange(claim_id, old_certificate, new_certificate)
            )
            if old_claim_state.status is not new_claim_state.status:
                stats.claim_status_changes += 1
                if self._claim_required[claim_id]:
                    answer_id = self._claim_to_answer[claim_id]
                    counts = answer_counts.setdefault(
                        answer_id, Counter(self._answer_status_counts[answer_id])
                    )
                    counts[old_claim_state.status] -= 1
                    counts[new_claim_state.status] += 1
                    dirty_answers.add(answer_id)

        answer_count_changes: list[
            _PointChange[tuple[tuple[ClaimStatus, int], ...]]
        ] = []
        answer_state_changes: list[_PointChange[AnswerState]] = []
        for answer_id in sorted(dirty_answers):
            old_counts = self._counter_snapshot(
                self._answer_status_counts[answer_id]
            )
            new_counts = self._counter_snapshot(answer_counts[answer_id])
            answer_count_changes.append(
                _PointChange(answer_id, old_counts, new_counts)
            )
            old_answer_state = self._answer_states[answer_id]
            new_answer_state = self._state_from_answer_count_value(
                answer_id, answer_counts[answer_id]
            )
            answer_state_changes.append(
                _PointChange(answer_id, old_answer_state, new_answer_state)
            )
            if old_answer_state.status is not new_answer_state.status:
                stats.answer_status_changes += 1

        stats.score_index_point_updates = 2 * len(score_changes)
        frozen_stats = stats.freeze(dirty_claims, dirty_answers)
        return IncrementalStatePatch(
            event_id=event.event_id,
            expected_state_revision=self._state_revision,
            active_label_changes=self._point_changes(
                self._active_labels, label_values
            ),
            contribution_changes=self._point_changes(
                self._contributions, contribution_values
            ),
            accumulator_changes=tuple(
                _PointChange(
                    claim_id,
                    accumulator_before[claim_id],
                    _ClaimAccumulatorSnapshot.capture(accumulators[claim_id]),
                )
                for claim_id in sorted(accumulators)
            ),
            claim_state_changes=tuple(claim_state_changes),
            certificate_changes=tuple(certificate_changes),
            answer_count_changes=tuple(answer_count_changes),
            answer_state_changes=tuple(answer_state_changes),
            score_index_changes=tuple(score_changes),
            stats=frozen_stats,
        )

    def prepare_noop_event_patch(self, event_id: str) -> IncrementalStatePatch:
        """Prepare an empty direct-state patch for one M5-only event.

        Applying the patch advances the direct engine revision exactly once,
        so a later patch prepared at the old revision is stale.  It changes no
        v1 claim, answer, certificate, observation, contribution, or score
        index entry.
        """

        if not isinstance(event_id, str) or not event_id.strip():
            raise ValidationError("event_id must be a nonempty identifier")
        return IncrementalStatePatch(
            event_id=event_id,
            expected_state_revision=self._state_revision,
            active_label_changes=(),
            contribution_changes=(),
            accumulator_changes=(),
            claim_state_changes=(),
            certificate_changes=(),
            answer_count_changes=(),
            answer_state_changes=(),
            score_index_changes=(),
            stats=MaintenanceStats(),
        )

    def preview_claim_states_after_patch(
        self,
        patch: IncrementalStatePatch,
        claim_ids: Iterable[str],
    ) -> dict[str, ClaimState]:
        """Read requested post-patch claims without mutating the engine.

        Preconditions are checked once with their existing ordered-index
        costs.  The remaining dispatch builds one affected lookup and performs
        one point lookup per requested ID: O(claim changes + requested IDs)
        expected work and no registry scan.
        """

        self._validate_patch_preconditions(patch)
        changed = {change.key: change.after for change in patch.claim_state_changes}
        result: dict[str, ClaimState] = {}
        for claim_id in dict.fromkeys(claim_ids):
            value = changed.get(claim_id)
            if value is None:
                value = self._claim_states[claim_id]
            result[claim_id] = value
        return result

    def apply_state_patch(
        self,
        patch: IncrementalStatePatch,
        *,
        failure_injector: Callable[[str], None] | None = None,
    ) -> None:
        """Atomically apply one prepared patch, rolling back injected failure."""
        self._validate_patch_preconditions(patch)
        undo: list[Callable[[], None]] = []
        shift_work = 0

        def checkpoint(name: str) -> None:
            if failure_injector is not None:
                failure_injector(name)

        try:
            for label_change in patch.active_label_changes:
                undo.append(
                    self._apply_point_change(self._active_labels, label_change)
                )
                checkpoint(f"active_label:{label_change.key}")
            for contribution_change in patch.contribution_changes:
                undo.append(
                    self._apply_point_change(
                        self._contributions, contribution_change
                    )
                )
                checkpoint(f"contribution:{contribution_change.key}")
            for accumulator_change in patch.accumulator_changes:
                before = accumulator_change.before
                after = accumulator_change.after
                assert before is not None and after is not None
                self._claim_accumulators[accumulator_change.key] = after.materialize()

                def restore_accumulator(
                    key: str = accumulator_change.key,
                    value: _ClaimAccumulatorSnapshot = before,
                ) -> None:
                    self._claim_accumulators[key] = value.materialize()

                undo.append(
                    restore_accumulator
                )
                checkpoint(f"accumulator:{accumulator_change.key}")
            for claim_state_change in patch.claim_state_changes:
                undo.append(
                    self._apply_point_change(self._claim_states, claim_state_change)
                )
                checkpoint(f"claim_state:{claim_state_change.key}")
            for certificate_change in patch.certificate_changes:
                undo.append(
                    self._apply_point_change(
                        self._certificates, certificate_change
                    )
                )
                checkpoint(f"certificate:{certificate_change.key}")
            for count_change in patch.answer_count_changes:
                before_counts = count_change.before
                after_counts = count_change.after
                assert before_counts is not None and after_counts is not None
                self._answer_status_counts[count_change.key] = Counter(
                    dict(after_counts)
                )

                def restore_answer_counts(
                    key: str = count_change.key,
                    value: tuple[tuple[ClaimStatus, int], ...] = before_counts,
                ) -> None:
                    self._answer_status_counts[key] = Counter(dict(value))

                undo.append(restore_answer_counts)
                checkpoint(f"answer_counts:{count_change.key}")
            for answer_state_change in patch.answer_state_changes:
                undo.append(
                    self._apply_point_change(
                        self._answer_states, answer_state_change
                    )
                )
                checkpoint(f"answer_state:{answer_state_change.key}")
            for score_change in patch.score_index_changes:
                if score_change.add:
                    shift_work += self._score_index.add(score_change.observation)

                    def undo_score_add(
                        observation: SemanticObservation = score_change.observation,
                    ) -> None:
                        self._score_index.remove(observation)

                    undo.append(undo_score_add)
                else:
                    shift_work += self._score_index.remove(score_change.observation)

                    def undo_score_remove(
                        observation: SemanticObservation = score_change.observation,
                    ) -> None:
                        self._score_index.add(observation)

                    undo.append(undo_score_remove)
                checkpoint(
                    f"score_index:{score_change.observation.observation_id}"
                )
            previous_stats = self.last_stats
            self.last_stats = replace(patch.stats, score_index_shift_work=shift_work)

            def restore_stats(value: MaintenanceStats = previous_stats) -> None:
                self.last_stats = value

            undo.append(restore_stats)
            self._state_revision += 1

            def restore_revision() -> None:
                self._state_revision -= 1

            undo.append(restore_revision)
            checkpoint("published")
        except Exception:
            for rollback in reversed(undo):
                rollback()
            raise

    @staticmethod
    def _point_changes(
        current: Mapping[str, T], values: Mapping[str, T | None]
    ) -> tuple[_PointChange[T], ...]:
        return tuple(
            _PointChange(key, current.get(key), values[key]) for key in sorted(values)
        )

    @staticmethod
    def _apply_point_change(
        target: dict[str, T], change: _PointChange[T]
    ) -> Callable[[], None]:
        if change.after is None:
            del target[change.key]
        else:
            target[change.key] = change.after

        if change.before is None:
            def remove_new_key(key: str = change.key) -> None:
                target.pop(key, None)

            return remove_new_key

        def restore_old_value(
            key: str = change.key, value: T = change.before
        ) -> None:
            target[key] = value

        return restore_old_value

    @staticmethod
    def _counter_snapshot(
        counts: Counter[ClaimStatus],
    ) -> tuple[tuple[ClaimStatus, int], ...]:
        return tuple(sorted(counts.items(), key=lambda item: item[0].value))

    def _validate_patch_preconditions(self, patch: IncrementalStatePatch) -> None:
        if patch.expected_state_revision != self._state_revision:
            raise AssertionError("state patch is stale or was already applied")

        def validate_map(
            current: Mapping[str, T], changes: tuple[_PointChange[T], ...]
        ) -> None:
            for change in changes:
                if current.get(change.key) != change.before:
                    raise AssertionError(
                        f"state patch precondition failed for key {change.key}"
                    )

        validate_map(self._active_labels, patch.active_label_changes)
        validate_map(self._contributions, patch.contribution_changes)
        validate_map(self._claim_states, patch.claim_state_changes)
        validate_map(self._certificates, patch.certificate_changes)
        validate_map(self._answer_states, patch.answer_state_changes)
        for accumulator_change in patch.accumulator_changes:
            current = _ClaimAccumulatorSnapshot.capture(
                self._claim_accumulators[accumulator_change.key]
            )
            if current != accumulator_change.before:
                raise AssertionError(
                    "state patch accumulator precondition failed for "
                    f"{accumulator_change.key}"
                )
        for count_change in patch.answer_count_changes:
            current_counts = self._counter_snapshot(
                self._answer_status_counts[count_change.key]
            )
            if current_counts != count_change.before:
                raise AssertionError(
                    "state patch answer-count precondition failed for "
                    f"{count_change.key}"
                )
        for score_change in patch.score_index_changes:
            if self._score_index.contains(score_change.observation) == score_change.add:
                action = "add" if score_change.add else "remove"
                raise AssertionError(
                    f"state patch cannot {action} score entries for "
                    f"{score_change.observation.observation_id}"
                )

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
        stats.score_index_shift_work += self._score_index.add(observation)
        stats.score_index_point_updates += 2
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
        stats.score_index_shift_work += self._score_index.remove(observation)
        stats.score_index_point_updates += 2
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
        return self._state_from_accumulator_value(
            claim_id, self._claim_accumulators[claim_id]
        )

    @staticmethod
    def _state_from_accumulator_value(
        claim_id: str, accumulator: _ClaimAccumulator
    ) -> ClaimState:
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
        return self._state_from_answer_count_value(
            answer_id, self._answer_status_counts[answer_id]
        )

    def _state_from_answer_count_value(
        self, answer_id: str, counts: Counter[ClaimStatus]
    ) -> AnswerState:
        required = self._answer_required_counts[answer_id]
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
        self._certificates[claim_id] = self._certificate_from_state(state)

    @staticmethod
    def _certificate_from_state(state: ClaimState) -> ClaimCertificate:
        return ClaimCertificate(
            claim_id=state.claim_id,
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
