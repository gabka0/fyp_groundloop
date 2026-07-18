"""Independent direct-witness engine using the exact-flip policy index.

The maintenance path does not import or call the current incremental engine or
the full-recomputation oracle.  Repository snapshots are used only as immutable
base/index access around an already committed event.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

from groundloop.domain import (
    AnswerState,
    AnswerStatus,
    ClaimState,
    ClaimStatus,
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
from groundloop.optimized.avl import ScoreOrderedSet
from groundloop.optimized.exact_flip import (
    ExactFlipIndex,
    PotentialPartition,
    potential_partition,
)
from groundloop.policy import decide
from groundloop.repository import InMemoryRepository


@dataclass(frozen=True, slots=True)
class OptimizedCertificate:
    claim_id: str
    support_observation_id: str | None
    refute_observation_id: str | None


@dataclass(frozen=True, slots=True)
class OptimizedStats:
    """Kernel work for the most recent committed event.

    ``policy_index_candidates`` counts rows emitted by the partitioned index;
    for a threshold-only change it equals the actual label-flip count.
    """

    active_observations_added: int = 0
    active_observations_removed: int = 0
    point_index_updates: int = 0
    policy_index_searches: int = 0
    policy_index_candidates: int = 0
    decision_flips: int = 0
    distinct_hash_crossings: int = 0
    claim_keys_touched: int = 0
    claim_status_changes: int = 0
    answer_keys_touched: int = 0
    answer_status_changes: int = 0


@dataclass(slots=True)
class _MutableStats:
    active_observations_added: int = 0
    active_observations_removed: int = 0
    point_index_updates: int = 0
    policy_index_searches: int = 0
    policy_index_candidates: int = 0
    decision_flips: int = 0
    distinct_hash_crossings: int = 0
    claim_status_changes: int = 0
    answer_status_changes: int = 0

    def freeze(
        self, touched_claims: set[str], touched_answers: set[str]
    ) -> OptimizedStats:
        return OptimizedStats(
            active_observations_added=self.active_observations_added,
            active_observations_removed=self.active_observations_removed,
            point_index_updates=self.point_index_updates,
            policy_index_searches=self.policy_index_searches,
            policy_index_candidates=self.policy_index_candidates,
            decision_flips=self.decision_flips,
            distinct_hash_crossings=self.distinct_hash_crossings,
            claim_keys_touched=len(touched_claims),
            claim_status_changes=self.claim_status_changes,
            answer_keys_touched=len(touched_answers),
            answer_status_changes=self.answer_status_changes,
        )


@dataclass(slots=True)
class _ClaimAggregate:
    support_hash_refcounts: dict[str, int] = field(default_factory=dict)
    refute_hash_refcounts: dict[str, int] = field(default_factory=dict)
    support_ids: set[str] = field(default_factory=set)
    refute_ids: set[str] = field(default_factory=set)
    support_potential_scores: ScoreOrderedSet = field(default_factory=ScoreOrderedSet)
    refute_potential_scores: ScoreOrderedSet = field(default_factory=ScoreOrderedSet)


@dataclass(frozen=True, slots=True)
class _ActiveObservation:
    observation: SemanticObservation
    text_hash: str


def _claim_status(aggregate: _ClaimAggregate) -> ClaimStatus:
    supported = bool(aggregate.support_hash_refcounts)
    refuted = bool(aggregate.refute_hash_refcounts)
    if supported and refuted:
        return ClaimStatus.CONFLICTED
    if supported:
        return ClaimStatus.SUPPORTED
    if refuted:
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
class ExactFlipMaintenanceEngine:
    """Direct-witness maintenance with worst-case ordered-index bounds."""

    _claims: dict[str, _ClaimAggregate] = field(default_factory=dict)
    _claim_statuses: dict[str, ClaimStatus] = field(default_factory=dict)
    _claim_to_answer: dict[str, str] = field(default_factory=dict)
    _claim_required: dict[str, bool] = field(default_factory=dict)
    _answer_to_claims: dict[str, tuple[str, ...]] = field(default_factory=dict)
    _answer_counts: dict[str, Counter[ClaimStatus]] = field(default_factory=dict)
    _answer_statuses: dict[str, AnswerStatus] = field(default_factory=dict)
    _active: dict[str, _ActiveObservation] = field(default_factory=dict)
    _labels: dict[str, VerificationLabel] = field(default_factory=dict)
    _policy_index: ExactFlipIndex = field(default_factory=ExactFlipIndex)
    _certificates: dict[str, OptimizedCertificate] = field(default_factory=dict)
    last_stats: OptimizedStats = field(default_factory=OptimizedStats)

    @classmethod
    def from_repository(
        cls, repository: InMemoryRepository
    ) -> ExactFlipMaintenanceEngine:
        engine = cls()
        engine._sync_registry(repository)
        stats = _MutableStats()
        touched: set[str] = set()
        for observation in repository.current_observations():
            if repository.is_chunk_active(observation.chunk_version_id):
                engine._activate(
                    observation,
                    repository,
                    touched,
                    stats,
                )
        for claim_id, aggregate in engine._claims.items():
            engine._claim_statuses[claim_id] = _claim_status(aggregate)
            engine._repair_certificate(claim_id)
        engine._rebuild_answer_counts()
        engine.last_stats = OptimizedStats()
        return engine

    @property
    def claim_states(self) -> dict[str, ClaimState]:
        """Materialize full provenance state; cost includes output size."""
        return {
            claim_id: self._materialize_claim_state(claim_id)
            for claim_id in self._claims
        }

    @property
    def answer_states(self) -> dict[str, AnswerState]:
        return {
            answer_id: self._materialize_answer_state(answer_id)
            for answer_id in self._answer_to_claims
        }

    @property
    def certificates(self) -> dict[str, OptimizedCertificate]:
        return dict(self._certificates)

    @property
    def indexed_potential_observations(self) -> int:
        return len(self._policy_index)

    def apply_committed_event(
        self,
        event: Event,
        before: InMemoryRepository,
        after: InMemoryRepository,
    ) -> None:
        """Apply one event whose base mutation is visible in ``after``."""
        if before.recorded_event(event.event_id) is not None:
            self.last_stats = OptimizedStats()
            return

        self._sync_registry(after)
        touched_claims: set[str] = set()
        touched_answers: set[str] = set()
        stats = _MutableStats()

        if isinstance(event, DeleteDocumentVersionEvent):
            self._withdraw_document(
                event.document_version_id, before, touched_claims, stats
            )
        elif isinstance(event, ReplaceDocumentVersionEvent):
            self._withdraw_document(
                event.old_document_version_id, before, touched_claims, stats
            )
        elif isinstance(event, ObserveEvent):
            previous = before.current_observation_id(event.observation.key)
            if previous is not None:
                self._deactivate(previous, touched_claims, stats)
            if after.is_chunk_active(event.observation.chunk_version_id):
                self._activate(event.observation, after, touched_claims, stats)
        elif isinstance(event, PolicyChangeEvent):
            self._change_policy(before, after, touched_claims, stats)

        for claim_id in touched_claims:
            old_status = self._claim_statuses[claim_id]
            new_status = _claim_status(self._claims[claim_id])
            self._claim_statuses[claim_id] = new_status
            self._repair_certificate(claim_id)
            if old_status is not new_status:
                stats.claim_status_changes += 1
                if self._claim_required[claim_id]:
                    answer_id = self._claim_to_answer[claim_id]
                    counts = self._answer_counts[answer_id]
                    counts[old_status] -= 1
                    counts[new_status] += 1
                    touched_answers.add(answer_id)

        for answer_id in touched_answers:
            old = self._answer_statuses[answer_id]
            required = self._required_count(answer_id)
            new = _answer_status(self._answer_counts[answer_id], required)
            self._answer_statuses[answer_id] = new
            if old is not new:
                stats.answer_status_changes += 1

        self.last_stats = stats.freeze(touched_claims, touched_answers)

    def _sync_registry(self, repository: InMemoryRepository) -> None:
        changed = False
        for claim_id in repository.all_claim_ids():
            if claim_id in self._claims:
                continue
            claim = repository.claim(claim_id)
            self._claims[claim_id] = _ClaimAggregate()
            self._claim_statuses[claim_id] = ClaimStatus.UNSUPPORTED
            self._claim_to_answer[claim_id] = claim.answer_version_id
            self._claim_required[claim_id] = claim.required
            self._repair_certificate(claim_id)
            changed = True
        for answer_id in repository.all_answer_ids():
            claim_ids = repository.claim_ids_of_answer(answer_id)
            known = self._answer_to_claims.get(answer_id)
            if known is not None and known != claim_ids:
                raise AssertionError("immutable answer claim membership changed")
            if known is None:
                self._answer_to_claims[answer_id] = claim_ids
                changed = True
        if changed:
            self._rebuild_answer_counts()

    def _rebuild_answer_counts(self) -> None:
        self._answer_counts.clear()
        self._answer_statuses.clear()
        for answer_id, claim_ids in self._answer_to_claims.items():
            counts: Counter[ClaimStatus] = Counter()
            for claim_id in claim_ids:
                if self._claim_required[claim_id]:
                    counts[self._claim_statuses[claim_id]] += 1
            self._answer_counts[answer_id] = counts
            self._answer_statuses[answer_id] = _answer_status(
                counts, self._required_count(answer_id)
            )

    def _required_count(self, answer_id: str) -> int:
        return sum(
            1
            for claim_id in self._answer_to_claims[answer_id]
            if self._claim_required[claim_id]
        )

    def _withdraw_document(
        self,
        document_version_id: str,
        before: InMemoryRepository,
        touched_claims: set[str],
        stats: _MutableStats,
    ) -> None:
        for chunk_id in before.chunk_ids_of_document_version(document_version_id):
            for observation_id in before.current_observation_ids_for_chunk(chunk_id):
                self._deactivate(observation_id, touched_claims, stats)

    def _activate(
        self,
        observation: SemanticObservation,
        repository: InMemoryRepository,
        touched_claims: set[str],
        stats: _MutableStats,
    ) -> None:
        if observation.observation_id in self._active:
            raise AssertionError("optimized engine activated an observation twice")
        text_hash = repository.chunk_version(observation.chunk_version_id).text_hash
        self._active[observation.observation_id] = _ActiveObservation(
            observation, text_hash
        )
        label = decide(observation, repository.current_policy())
        self._labels[observation.observation_id] = label
        partition = potential_partition(observation)
        self._policy_index.add(observation)
        stats.active_observations_added += 1
        if partition is not PotentialPartition.ALWAYS_NEUTRAL:
            stats.point_index_updates += 1
        if observation.subject_kind is not SubjectKind.CLAIM:
            return
        aggregate = self._claims[observation.subject_id]
        if partition is PotentialPartition.SUPPORT:
            aggregate.support_potential_scores.add(
                (observation.support_score, observation.observation_id)
            )
        elif partition is PotentialPartition.REFUTE:
            aggregate.refute_potential_scores.add(
                (observation.refute_score, observation.observation_id)
            )
        self._add_label(observation.observation_id, label, touched_claims, stats)

    def _deactivate(
        self,
        observation_id: str,
        touched_claims: set[str],
        stats: _MutableStats,
    ) -> None:
        active = self._active.get(observation_id)
        if active is None:
            return
        observation = active.observation
        label = self._labels.pop(observation_id)
        self._remove_label(observation_id, label, touched_claims, stats)
        del self._active[observation_id]
        partition = potential_partition(observation)
        self._policy_index.remove(observation)
        stats.active_observations_removed += 1
        if partition is not PotentialPartition.ALWAYS_NEUTRAL:
            stats.point_index_updates += 1
        if observation.subject_kind is not SubjectKind.CLAIM:
            return
        aggregate = self._claims[observation.subject_id]
        if partition is PotentialPartition.SUPPORT:
            aggregate.support_potential_scores.remove(
                (observation.support_score, observation_id)
            )
        elif partition is PotentialPartition.REFUTE:
            aggregate.refute_potential_scores.remove(
                (observation.refute_score, observation_id)
            )

    def _change_policy(
        self,
        before: InMemoryRepository,
        after: InMemoryRepository,
        touched_claims: set[str],
        stats: _MutableStats,
    ) -> None:
        query = self._policy_index.exact_flips(
            before.current_policy(), after.current_policy()
        )
        stats.policy_index_searches = query.searches
        stats.policy_index_candidates = len(query.observation_ids)
        for observation_id in query.observation_ids:
            active = self._active[observation_id]
            old_label = self._labels[observation_id]
            new_label = decide(active.observation, after.current_policy())
            if old_label is new_label:
                raise AssertionError("exact-flip index emitted a non-flipping row")
            self._remove_label(observation_id, old_label, touched_claims, stats)
            self._labels[observation_id] = new_label
            self._add_label(observation_id, new_label, touched_claims, stats)
            stats.decision_flips += 1

    def _add_label(
        self,
        observation_id: str,
        label: VerificationLabel,
        touched_claims: set[str],
        stats: _MutableStats,
    ) -> None:
        if label is VerificationLabel.NEUTRAL:
            return
        active = self._active[observation_id]
        observation = active.observation
        if observation.subject_kind is not SubjectKind.CLAIM:
            return
        aggregate = self._claims[observation.subject_id]
        if label is VerificationLabel.SUPPORT:
            refcounts = aggregate.support_hash_refcounts
            ids = aggregate.support_ids
        else:
            refcounts = aggregate.refute_hash_refcounts
            ids = aggregate.refute_ids
        old = refcounts.get(active.text_hash, 0)
        refcounts[active.text_hash] = old + 1
        if old == 0:
            stats.distinct_hash_crossings += 1
        ids.add(observation_id)
        touched_claims.add(observation.subject_id)

    def _remove_label(
        self,
        observation_id: str,
        label: VerificationLabel,
        touched_claims: set[str],
        stats: _MutableStats,
    ) -> None:
        if label is VerificationLabel.NEUTRAL:
            return
        active = self._active[observation_id]
        observation = active.observation
        if observation.subject_kind is not SubjectKind.CLAIM:
            return
        aggregate = self._claims[observation.subject_id]
        if label is VerificationLabel.SUPPORT:
            refcounts = aggregate.support_hash_refcounts
            ids = aggregate.support_ids
        else:
            refcounts = aggregate.refute_hash_refcounts
            ids = aggregate.refute_ids
        old = refcounts.get(active.text_hash, 0)
        if old <= 0:
            raise AssertionError("removing absent optimized content contribution")
        if old == 1:
            del refcounts[active.text_hash]
            stats.distinct_hash_crossings += 1
        else:
            refcounts[active.text_hash] = old - 1
        ids.remove(observation_id)
        touched_claims.add(observation.subject_id)

    def _repair_certificate(self, claim_id: str) -> None:
        aggregate = self._claims[claim_id]
        old = self._certificates.get(claim_id)
        support_id = old.support_observation_id if old is not None else None
        refute_id = old.refute_observation_id if old is not None else None
        if support_id not in aggregate.support_ids:
            support_id = next(iter(aggregate.support_ids), None)
        if refute_id not in aggregate.refute_ids:
            refute_id = next(iter(aggregate.refute_ids), None)
        self._certificates[claim_id] = OptimizedCertificate(
            claim_id=claim_id,
            support_observation_id=support_id,
            refute_observation_id=refute_id,
        )

    def _materialize_claim_state(self, claim_id: str) -> ClaimState:
        aggregate = self._claims[claim_id]
        support_count = len(aggregate.support_hash_refcounts)
        refute_count = len(aggregate.refute_hash_refcounts)
        return ClaimState(
            claim_id=claim_id,
            support_count=support_count,
            refute_count=refute_count,
            best_support_score=(
                aggregate.support_potential_scores.maximum_score()
                if aggregate.support_ids
                else None
            ),
            best_refute_score=(
                aggregate.refute_potential_scores.maximum_score()
                if aggregate.refute_ids
                else None
            ),
            supporting_observation_ids=tuple(sorted(aggregate.support_ids)),
            refuting_observation_ids=tuple(sorted(aggregate.refute_ids)),
            status=self._claim_statuses[claim_id],
        )

    def _materialize_answer_state(self, answer_id: str) -> AnswerState:
        counts = self._answer_counts[answer_id]
        return AnswerState(
            answer_version_id=answer_id,
            required_claim_count=self._required_count(answer_id),
            supported_count=counts[ClaimStatus.SUPPORTED],
            unsupported_count=counts[ClaimStatus.UNSUPPORTED],
            refuted_count=counts[ClaimStatus.REFUTED],
            conflicted_count=counts[ClaimStatus.CONFLICTED],
            status=self._answer_statuses[answer_id],
        )

    def validate(self) -> None:
        self._policy_index.validate()
        for claim_id, aggregate in self._claims.items():
            aggregate.support_potential_scores.validate()
            aggregate.refute_potential_scores.validate()
            if self._claim_statuses[claim_id] is not _claim_status(aggregate):
                raise AssertionError("optimized claim status mismatch")
            certificate = self._certificates[claim_id]
            if aggregate.support_ids:
                if certificate.support_observation_id not in aggregate.support_ids:
                    raise AssertionError("invalid optimized support certificate")
            elif certificate.support_observation_id is not None:
                raise AssertionError("spurious optimized support certificate")
            if aggregate.refute_ids:
                if certificate.refute_observation_id not in aggregate.refute_ids:
                    raise AssertionError("invalid optimized refute certificate")
            elif certificate.refute_observation_id is not None:
                raise AssertionError("spurious optimized refute certificate")
