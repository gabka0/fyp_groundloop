"""In-memory historical repository for the M1 reference semantics.

Immutable records are stored append-only. Activity is temporal: half-open
epoch validity intervals live here, never on the historical objects.
Observation currency (D-8) is enforced here: at most one current observation
per ObservationKey; a new registration for an occupied key supersedes the old
observation, which remains stored and queryable.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field, fields

from groundloop.domain import (
    AnswerVersion,
    ChunkVersion,
    Claim,
    DecisionPolicy,
    DocumentVersion,
    ObservationKey,
    Question,
    SemanticObservation,
    StatusDelta,
    SubjectKind,
)
from groundloop.errors import (
    DanglingReferenceError,
    DuplicateIdentifierError,
    ValidationError,
)

Interval = tuple[int, int | None]


def _interval_active(interval: Interval) -> bool:
    return interval[1] is None


@dataclass(slots=True)
class InMemoryRepository:
    """Historical store plus activation indexes and the event registry."""

    current_epoch: int = 0

    _document_versions: dict[str, DocumentVersion] = field(default_factory=dict)
    _document_version_validity: dict[str, Interval] = field(default_factory=dict)
    _versions_by_document: dict[str, list[str]] = field(default_factory=dict)

    _chunk_versions: dict[str, ChunkVersion] = field(default_factory=dict)
    _chunk_validity: dict[str, Interval] = field(default_factory=dict)
    _chunks_by_document_version: dict[str, list[str]] = field(default_factory=dict)

    _questions: dict[str, Question] = field(default_factory=dict)
    _answers: dict[str, AnswerVersion] = field(default_factory=dict)
    _claims: dict[str, Claim] = field(default_factory=dict)
    _claims_by_answer: dict[str, list[str]] = field(default_factory=dict)

    _observations: dict[str, SemanticObservation] = field(default_factory=dict)
    _current_by_key: dict[ObservationKey, str] = field(default_factory=dict)
    _superseded: set[str] = field(default_factory=set)
    _observations_by_chunk: dict[str, list[str]] = field(default_factory=dict)

    _policies: dict[str, DecisionPolicy] = field(default_factory=dict)
    _policy_validity: dict[str, Interval] = field(default_factory=dict)
    _current_policy_version: str | None = None

    _processed_events: dict[str, tuple[str, tuple[StatusDelta, ...]]] = field(
        default_factory=dict
    )
    status_deltas: list[StatusDelta] = field(default_factory=list)

    # ---------------------------------------------------------------- epochs

    def advance_epoch(self) -> int:
        self.current_epoch += 1
        return self.current_epoch

    def replace_with(self, staged: InMemoryRepository) -> None:
        """Atomically adopt a fully validated staged repository snapshot."""
        for repository_field in fields(self):
            setattr(
                self,
                repository_field.name,
                getattr(staged, repository_field.name),
            )

    # ---------------------------------------------------------- registration

    def register_question(self, question: Question) -> None:
        if question.question_id in self._questions:
            raise DuplicateIdentifierError(
                f"question {question.question_id} already registered"
            )
        self._questions[question.question_id] = question

    def register_answer(self, answer: AnswerVersion, claims: tuple[Claim, ...]) -> None:
        if answer.answer_version_id in self._answers:
            raise DuplicateIdentifierError(
                f"answer {answer.answer_version_id} already registered"
            )
        if answer.question_id not in self._questions:
            raise DanglingReferenceError(
                f"answer {answer.answer_version_id} references missing "
                f"question {answer.question_id}"
            )
        if not any(claim.required for claim in claims):
            raise ValidationError(
                f"answer {answer.answer_version_id} has no required claim; "
                "rejected (v0.2 invariant 9)"
            )
        incoming_claim_ids: set[str] = set()
        for claim in claims:
            if claim.claim_id in self._claims or claim.claim_id in incoming_claim_ids:
                raise DuplicateIdentifierError(
                    f"claim {claim.claim_id} already registered"
                )
            incoming_claim_ids.add(claim.claim_id)
            if claim.answer_version_id != answer.answer_version_id:
                raise DanglingReferenceError(
                    f"claim {claim.claim_id} references answer "
                    f"{claim.answer_version_id}, expected "
                    f"{answer.answer_version_id}"
                )
        self._answers[answer.answer_version_id] = answer
        self._claims_by_answer[answer.answer_version_id] = []
        for claim in claims:
            self._claims[claim.claim_id] = claim
            self._claims_by_answer[answer.answer_version_id].append(claim.claim_id)

    def register_document_version(
        self,
        version: DocumentVersion,
        chunks: tuple[ChunkVersion, ...],
        epoch: int,
    ) -> None:
        """Append and activate a document version and its chunks."""
        if version.document_version_id in self._document_versions:
            raise DuplicateIdentifierError(
                f"document version {version.document_version_id} already exists"
            )
        active = self.active_document_version(version.document_id)
        if active is not None:
            raise ValidationError(
                f"document {version.document_id} already has active version "
                f"{active}; use REPLACE (v0.2 invariant 2)"
            )
        incoming_chunk_ids: set[str] = set()
        for chunk in chunks:
            if (
                chunk.chunk_version_id in self._chunk_versions
                or chunk.chunk_version_id in incoming_chunk_ids
            ):
                raise DuplicateIdentifierError(
                    f"chunk version {chunk.chunk_version_id} already exists"
                )
            incoming_chunk_ids.add(chunk.chunk_version_id)
            if chunk.document_version_id != version.document_version_id:
                raise DanglingReferenceError(
                    f"chunk {chunk.chunk_version_id} references document "
                    f"version {chunk.document_version_id}, expected "
                    f"{version.document_version_id}"
                )
        self._document_versions[version.document_version_id] = version
        self._document_version_validity[version.document_version_id] = (epoch, None)
        self._versions_by_document.setdefault(version.document_id, []).append(
            version.document_version_id
        )
        self._chunks_by_document_version[version.document_version_id] = []
        for chunk in chunks:
            self._chunk_versions[chunk.chunk_version_id] = chunk
            self._chunk_validity[chunk.chunk_version_id] = (epoch, None)
            self._chunks_by_document_version[version.document_version_id].append(
                chunk.chunk_version_id
            )

    def deactivate_document_version(self, document_version_id: str, epoch: int) -> None:
        """Close the validity interval of a version and all its chunks."""
        interval = self._document_version_validity.get(document_version_id)
        if interval is None:
            raise DanglingReferenceError(
                f"document version {document_version_id} does not exist"
            )
        if not _interval_active(interval):
            raise ValidationError(
                f"document version {document_version_id} is already inactive"
            )
        self._document_version_validity[document_version_id] = (interval[0], epoch)
        for chunk_id in self._chunks_by_document_version[document_version_id]:
            chunk_interval = self._chunk_validity[chunk_id]
            if _interval_active(chunk_interval):
                self._chunk_validity[chunk_id] = (chunk_interval[0], epoch)

    def register_observation(self, observation: SemanticObservation) -> None:
        """Append an observation, superseding the current key holder (D-8).

        The chunk version must exist but need not be active (D-18): a late
        observation for an inactive chunk is stored, auditable, and inert.
        """
        if observation.observation_id in self._observations:
            raise DuplicateIdentifierError(
                f"observation {observation.observation_id} already registered"
            )
        if observation.chunk_version_id not in self._chunk_versions:
            raise DanglingReferenceError(
                f"observation {observation.observation_id} references missing "
                f"chunk version {observation.chunk_version_id}"
            )
        if observation.subject_kind is SubjectKind.CLAIM:
            if observation.subject_id not in self._claims:
                raise DanglingReferenceError(
                    f"observation {observation.observation_id} references "
                    f"missing claim {observation.subject_id}"
                )
        else:
            raise DanglingReferenceError(
                f"observation {observation.observation_id} references "
                f"requirement {observation.subject_id}; requirement subjects "
                "arrive in M5"
            )
        previous = self._current_by_key.get(observation.key)
        if previous is not None:
            self._superseded.add(previous)
        self._observations[observation.observation_id] = observation
        self._current_by_key[observation.key] = observation.observation_id
        self._observations_by_chunk.setdefault(observation.chunk_version_id, []).append(
            observation.observation_id
        )

    def activate_policy(self, policy: DecisionPolicy, epoch: int) -> None:
        if policy.policy_version in self._policies:
            raise DuplicateIdentifierError(
                f"policy {policy.policy_version} already registered"
            )
        if self._current_policy_version is not None:
            old = self._policy_validity[self._current_policy_version]
            self._policy_validity[self._current_policy_version] = (old[0], epoch)
        self._policies[policy.policy_version] = policy
        self._policy_validity[policy.policy_version] = (epoch, None)
        self._current_policy_version = policy.policy_version

    # ---------------------------------------------------------------- events

    def recorded_event(
        self, event_id: str
    ) -> tuple[str, tuple[StatusDelta, ...]] | None:
        return self._processed_events.get(event_id)

    def record_event(
        self, event_id: str, digest: str, deltas: tuple[StatusDelta, ...]
    ) -> None:
        self._processed_events[event_id] = (digest, deltas)
        self.status_deltas.extend(deltas)

    # --------------------------------------------------------------- queries

    def current_policy(self) -> DecisionPolicy:
        if self._current_policy_version is None:
            raise ValidationError("no decision policy has been activated")
        return self._policies[self._current_policy_version]

    def has_document_version(self, document_version_id: str) -> bool:
        return document_version_id in self._document_versions

    def has_chunk_version(self, chunk_version_id: str) -> bool:
        return chunk_version_id in self._chunk_versions

    def active_document_version(self, document_id: str) -> str | None:
        for version_id in self._versions_by_document.get(document_id, []):
            if _interval_active(self._document_version_validity[version_id]):
                return version_id
        return None

    def document_version(self, document_version_id: str) -> DocumentVersion:
        try:
            return self._document_versions[document_version_id]
        except KeyError as exc:
            raise DanglingReferenceError(
                f"document version {document_version_id} does not exist"
            ) from exc

    def document_versions_of(self, document_id: str) -> tuple[str, ...]:
        return tuple(self._versions_by_document.get(document_id, []))

    def chunk_version(self, chunk_version_id: str) -> ChunkVersion:
        try:
            return self._chunk_versions[chunk_version_id]
        except KeyError as exc:
            raise DanglingReferenceError(
                f"chunk version {chunk_version_id} does not exist"
            ) from exc

    def is_chunk_active(self, chunk_version_id: str) -> bool:
        interval = self._chunk_validity.get(chunk_version_id)
        return interval is not None and _interval_active(interval)

    def chunk_ids_of_document_version(
        self, document_version_id: str
    ) -> tuple[str, ...]:
        return tuple(self._chunks_by_document_version.get(document_version_id, []))

    def claim(self, claim_id: str) -> Claim:
        try:
            return self._claims[claim_id]
        except KeyError as exc:
            raise DanglingReferenceError(f"claim {claim_id} does not exist") from exc

    def all_claim_ids(self) -> tuple[str, ...]:
        return tuple(self._claims)

    def answer(self, answer_version_id: str) -> AnswerVersion:
        try:
            return self._answers[answer_version_id]
        except KeyError as exc:
            raise DanglingReferenceError(
                f"answer {answer_version_id} does not exist"
            ) from exc

    def all_answer_ids(self) -> tuple[str, ...]:
        return tuple(self._answers)

    def claim_ids_of_answer(self, answer_version_id: str) -> tuple[str, ...]:
        return tuple(self._claims_by_answer.get(answer_version_id, []))

    def observation(self, observation_id: str) -> SemanticObservation:
        try:
            return self._observations[observation_id]
        except KeyError as exc:
            raise DanglingReferenceError(
                f"observation {observation_id} does not exist"
            ) from exc

    def is_observation_superseded(self, observation_id: str) -> bool:
        if observation_id not in self._observations:
            raise DanglingReferenceError(f"observation {observation_id} does not exist")
        return observation_id in self._superseded

    def observations_for_chunk(self, chunk_version_id: str) -> tuple[str, ...]:
        """All observations ever recorded against a chunk, including
        superseded and inactive ones (audit history)."""
        return tuple(self._observations_by_chunk.get(chunk_version_id, []))

    def current_observations(self) -> Iterator[SemanticObservation]:
        """Current (non-superseded) observations, regardless of chunk
        activity. Activity filtering is the reference layer's join."""
        for observation_id in self._current_by_key.values():
            yield self._observations[observation_id]

    def current_observation_id(self, key: ObservationKey) -> str | None:
        """Return the current holder of one observation currency key."""
        return self._current_by_key.get(key)

    def current_observation_ids_for_chunk(
        self, chunk_version_id: str
    ) -> tuple[str, ...]:
        """Current observation IDs for a chunk, including inactive chunks."""
        current = self._current_by_key
        return tuple(
            observation_id
            for observation_id in self._observations_by_chunk.get(chunk_version_id, [])
            if current.get(self._observations[observation_id].key) == observation_id
        )
