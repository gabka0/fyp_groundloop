"""Failure-atomic M5 structural and requirement-observation events."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass

from groundloop.domain import SemanticObservation, StatusDelta
from groundloop.errors import EventConflictError, ValidationError
from groundloop.events import Event as LegacyEvent
from groundloop.m5.digests import (
    enum_field,
    f64_field,
    hash_field,
    stable_m5_digest,
    text_field,
)
from groundloop.m5.domain import EvidenceGroupVersion
from groundloop.m5.reference import compute_reference_states
from groundloop.m5.repository import M5Repository


@dataclass(frozen=True, slots=True)
class RegisterGroupEvent:
    event_id: str
    group: EvidenceGroupVersion


@dataclass(frozen=True, slots=True)
class ReplaceGroupEvent:
    event_id: str
    old_group_version_id: str
    successor: EvidenceGroupVersion


@dataclass(frozen=True, slots=True)
class RetireGroupEvent:
    event_id: str
    group_version_id: str


@dataclass(frozen=True, slots=True)
class ObserveRequirementEvent:
    event_id: str
    observation: SemanticObservation


M5Event = (
    RegisterGroupEvent | ReplaceGroupEvent | RetireGroupEvent | ObserveRequirementEvent
)
FailureInjector = Callable[[str, M5Repository], None]


@dataclass(frozen=True, slots=True)
class M5ApplyResult:
    deltas: tuple[StatusDelta, ...]
    replayed: bool


def legacy_event_payload_digest(event: LegacyEvent) -> str:
    """Reproduce the frozen M1 event payload digest without reinterpretation."""

    return hashlib.sha256(repr(event).encode("utf-8")).hexdigest()


def m5_event_payload_digest(event: M5Event) -> str:
    if isinstance(event, RegisterGroupEvent):
        return stable_m5_digest(
            "m5-register-group-event-v1",
            text_field(event.group.group_family_id),
            text_field(event.group.owner_claim_id),
            hash_field(event.group.record_payload_hash),
        )
    if isinstance(event, ReplaceGroupEvent):
        return stable_m5_digest(
            "m5-replace-group-event-v1",
            text_field(event.old_group_version_id),
            hash_field(event.successor.record_payload_hash),
        )
    if isinstance(event, RetireGroupEvent):
        return stable_m5_digest(
            "m5-retire-group-event-v1", text_field(event.group_version_id)
        )
    observation = event.observation
    return stable_m5_digest(
        "m5-observe-requirement-event-v1",
        text_field(observation.observation_id),
        enum_field(observation.subject_kind),
        text_field(observation.subject_id),
        text_field(observation.chunk_version_id),
        text_field(observation.task_type),
        f64_field(observation.support_score),
        f64_field(observation.refute_score),
        f64_field(observation.neutral_score),
        text_field(observation.producer.model_id),
        text_field(observation.producer.model_version),
        text_field(observation.producer.prompt_version),
        hash_field(observation.input_hash),
    )


def apply_m5_event(
    repo: M5Repository,
    event: M5Event,
    failure_injector: FailureInjector | None = None,
) -> M5ApplyResult:
    if not event.event_id.strip():
        raise ValidationError("event_id must be a nonempty identifier")
    payload_hash = m5_event_payload_digest(event)
    recorded = repo.recorded_event(event.event_id)
    if recorded is not None:
        recorded_hash, recorded_deltas = recorded
        if recorded_hash != payload_hash:
            raise EventConflictError(
                f"event {event.event_id} was already processed with another payload"
            )
        return M5ApplyResult(deltas=recorded_deltas, replayed=True)

    before = compute_reference_states(repo)
    staged = deepcopy(repo)
    point = staged.begin_event_epoch()
    if failure_injector is not None:
        failure_injector("after_epoch_open", staged)
    if isinstance(event, RegisterGroupEvent):
        staged.register_group(event.group, point.epoch_id)
        target = f"group_version={event.group.group_version_id}"
        operation = "register_group"
    elif isinstance(event, ReplaceGroupEvent):
        staged.replace_group(
            event.old_group_version_id, event.successor, point.epoch_id
        )
        target = (
            f"old_group_version={event.old_group_version_id} "
            f"new_group_version={event.successor.group_version_id}"
        )
        operation = "replace_group"
    elif isinstance(event, RetireGroupEvent):
        staged.retire_group(event.group_version_id, point.epoch_id, event.event_id)
        target = f"group_version={event.group_version_id}"
        operation = "retire_group"
    else:
        staged.register_requirement_observation(event.observation, point)
        target = f"observation={event.observation.observation_id}"
        operation = "observe_requirement"

    if failure_injector is not None:
        failure_injector("after_mutation", staged)

    after = compute_reference_states(staged)
    if failure_injector is not None:
        failure_injector("after_recompute", staged)
    reason = f"event={event.event_id} op={operation} {target}"
    deltas: list[StatusDelta] = []
    for claim_id in sorted(after.claims):
        old_state = before.claims[claim_id]
        new_state = after.claims[claim_id]
        if old_state.status is not new_state.status:
            deltas.append(
                StatusDelta(
                    event_id=event.event_id,
                    object_type="claim",
                    object_id=claim_id,
                    old_status=old_state.status.value,
                    new_status=new_state.status.value,
                    reason=reason,
                )
            )
    for answer_id in sorted(after.answers):
        old_answer_state = before.answers[answer_id]
        new_answer_state = after.answers[answer_id]
        if old_answer_state.status is not new_answer_state.status:
            deltas.append(
                StatusDelta(
                    event_id=event.event_id,
                    object_type="answer",
                    object_id=answer_id,
                    old_status=old_answer_state.status.value,
                    new_status=new_answer_state.status.value,
                    reason=reason,
                )
            )

    logical_deltas = tuple(deltas)
    staged.record_event(event.event_id, payload_hash, logical_deltas)
    if failure_injector is not None:
        failure_injector("after_event_record", staged)
        failure_injector("before_commit", staged)
    repo.replace_with(staged)
    return M5ApplyResult(deltas=logical_deltas, replayed=False)


__all__ = [
    "M5ApplyResult",
    "M5Event",
    "FailureInjector",
    "ObserveRequirementEvent",
    "RegisterGroupEvent",
    "ReplaceGroupEvent",
    "RetireGroupEvent",
    "apply_m5_event",
    "legacy_event_payload_digest",
    "m5_event_payload_digest",
]
