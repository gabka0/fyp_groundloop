"""Event payloads and application for the M1 reference semantics.

Events are the only way corpus activity, observations, and policy change.
Application is all-or-nothing from the caller's perspective: mutation and
reference recomputation occur on a deep staged copy. The live repository is
replaced only after the event, derived states, and event record all succeed.
The event layer emits a StatusDelta only for externally visible changes.

Idempotence (v0.2 Section 12): an event identifier is processed at most
once. Exact replay returns the recorded deltas without mutation; the same
identifier with a different payload digest raises EventConflictError.
"""

import hashlib
from copy import deepcopy
from dataclasses import dataclass

from groundloop.domain import (
    ChunkVersion,
    DecisionPolicy,
    DocumentVersion,
    SemanticObservation,
    StatusDelta,
    UpdateOperation,
)
from groundloop.errors import (
    DuplicateIdentifierError,
    EventConflictError,
    InvalidEventError,
)
from groundloop.reference import compute_all_states
from groundloop.repository import InMemoryRepository


@dataclass(frozen=True, slots=True)
class ChunkInput:
    """Chunk content supplied by an insert or replace event."""

    chunk_version_id: str
    chunk_index: int
    text: str


@dataclass(frozen=True, slots=True)
class InsertDocumentEvent:
    event_id: str
    document_id: str
    document_version_id: str
    content_hash: str
    chunks: tuple[ChunkInput, ...]


@dataclass(frozen=True, slots=True)
class DeleteDocumentVersionEvent:
    event_id: str
    document_version_id: str


@dataclass(frozen=True, slots=True)
class ReplaceDocumentVersionEvent:
    event_id: str
    document_id: str
    old_document_version_id: str
    new_document_version_id: str
    content_hash: str
    chunks: tuple[ChunkInput, ...]


@dataclass(frozen=True, slots=True)
class PolicyChangeEvent:
    event_id: str
    policy: DecisionPolicy


@dataclass(frozen=True, slots=True)
class ObserveEvent:
    event_id: str
    observation: SemanticObservation


Event = (
    InsertDocumentEvent
    | DeleteDocumentVersionEvent
    | ReplaceDocumentVersionEvent
    | PolicyChangeEvent
    | ObserveEvent
)


def _digest(event: Event) -> str:
    """Canonical payload digest. Frozen dataclass repr is deterministic."""
    return hashlib.sha256(repr(event).encode("utf-8")).hexdigest()


def _operation(event: Event) -> UpdateOperation:
    if isinstance(event, InsertDocumentEvent):
        return UpdateOperation.INSERT
    if isinstance(event, DeleteDocumentVersionEvent):
        return UpdateOperation.DELETE
    if isinstance(event, ReplaceDocumentVersionEvent):
        return UpdateOperation.REPLACE
    if isinstance(event, PolicyChangeEvent):
        return UpdateOperation.POLICY_CHANGE
    return UpdateOperation.OBSERVE


def _target(event: Event) -> str:
    if isinstance(event, InsertDocumentEvent):
        return f"document_version={event.document_version_id}"
    if isinstance(event, DeleteDocumentVersionEvent):
        return f"document_version={event.document_version_id}"
    if isinstance(event, ReplaceDocumentVersionEvent):
        return (
            f"old_document_version={event.old_document_version_id} "
            f"new_document_version={event.new_document_version_id}"
        )
    if isinstance(event, PolicyChangeEvent):
        return f"policy={event.policy.policy_version}"
    return f"observation={event.observation.observation_id}"


def _build_chunks(
    document_version_id: str, chunks: tuple[ChunkInput, ...]
) -> tuple[ChunkVersion, ...]:
    return tuple(
        ChunkVersion(
            chunk_version_id=chunk.chunk_version_id,
            document_version_id=document_version_id,
            chunk_index=chunk.chunk_index,
            text=chunk.text,
        )
        for chunk in chunks
    )


def _validate_replace(
    repo: InMemoryRepository, event: ReplaceDocumentVersionEvent
) -> None:
    old = repo.document_version(event.old_document_version_id)
    if old.document_id != event.document_id:
        raise InvalidEventError(
            f"event {event.event_id}: version {event.old_document_version_id} "
            f"belongs to document {old.document_id}, not {event.document_id}"
        )
    if repo.active_document_version(event.document_id) != event.old_document_version_id:
        raise InvalidEventError(
            f"event {event.event_id}: version {event.old_document_version_id} "
            f"is not the active version of document {event.document_id}"
        )
    if repo.has_document_version(event.new_document_version_id):
        raise DuplicateIdentifierError(
            f"event {event.event_id}: document version "
            f"{event.new_document_version_id} already exists"
        )
    for chunk in event.chunks:
        if repo.has_chunk_version(chunk.chunk_version_id):
            raise DuplicateIdentifierError(
                f"event {event.event_id}: chunk version "
                f"{chunk.chunk_version_id} already exists"
            )


def _mutate(repo: InMemoryRepository, event: Event, epoch: int) -> None:
    if isinstance(event, InsertDocumentEvent):
        version = DocumentVersion(
            document_version_id=event.document_version_id,
            document_id=event.document_id,
            content_hash=event.content_hash,
        )
        repo.register_document_version(
            version, _build_chunks(event.document_version_id, event.chunks), epoch
        )
    elif isinstance(event, DeleteDocumentVersionEvent):
        repo.deactivate_document_version(event.document_version_id, epoch)
    elif isinstance(event, ReplaceDocumentVersionEvent):
        _validate_replace(repo, event)
        repo.deactivate_document_version(event.old_document_version_id, epoch)
        version = DocumentVersion(
            document_version_id=event.new_document_version_id,
            document_id=event.document_id,
            content_hash=event.content_hash,
        )
        repo.register_document_version(
            version, _build_chunks(event.new_document_version_id, event.chunks), epoch
        )
    elif isinstance(event, PolicyChangeEvent):
        repo.activate_policy(event.policy, epoch)
    else:
        repo.register_observation(event.observation)


def apply_event(repo: InMemoryRepository, event: Event) -> tuple[StatusDelta, ...]:
    """Apply one event atomically with idempotence, emitting status deltas.

    A rejected event leaves the live repository exactly unchanged, including
    its current epoch, indexes, history, event registry, and status-delta log.
    The copy-and-commit strategy is deliberately simple for the M1 oracle; M2
    persistence will use database transactions.
    """
    digest = _digest(event)
    recorded = repo.recorded_event(event.event_id)
    if recorded is not None:
        recorded_digest, recorded_deltas = recorded
        if recorded_digest == digest:
            return recorded_deltas
        raise EventConflictError(
            f"event {event.event_id} was already processed with a different payload"
        )

    before_claims, before_answers = compute_all_states(repo)
    staged = deepcopy(repo)
    epoch = staged.advance_epoch()
    _mutate(staged, event, epoch)
    after_claims, after_answers = compute_all_states(staged)

    reason = f"event={event.event_id} op={_operation(event).value} {_target(event)}"
    deltas: list[StatusDelta] = []
    for claim_id in sorted(after_claims):
        old = before_claims.get(claim_id)
        new = after_claims[claim_id]
        if old is not None and old.status is not new.status:
            deltas.append(
                StatusDelta(
                    event_id=event.event_id,
                    object_type="claim",
                    object_id=claim_id,
                    old_status=old.status.value,
                    new_status=new.status.value,
                    reason=reason,
                )
            )
    for answer_id in sorted(after_answers):
        old_answer = before_answers.get(answer_id)
        new_answer = after_answers[answer_id]
        if old_answer is not None and old_answer.status is not new_answer.status:
            deltas.append(
                StatusDelta(
                    event_id=event.event_id,
                    object_type="answer",
                    object_id=answer_id,
                    old_status=old_answer.status.value,
                    new_status=new_answer.status.value,
                    reason=reason,
                )
            )

    result = tuple(deltas)
    staged.record_event(event.event_id, digest, result)
    repo.replace_with(staged)
    return result
