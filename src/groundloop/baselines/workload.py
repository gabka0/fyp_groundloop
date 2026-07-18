"""Deterministic structured workload generation over E, C, A, k, f, and skew."""

from __future__ import annotations

import random
from dataclasses import dataclass

from groundloop.baselines.models import Locality, WorkloadParameters
from groundloop.domain import (
    AnswerVersion,
    Claim,
    DecisionPolicy,
    ModelStamp,
    Question,
    SemanticObservation,
    SubjectKind,
)
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    Event,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    apply_event,
)
from groundloop.repository import InMemoryRepository

_STAMP = ModelStamp("baseline-generator", "v1", "p1")


@dataclass(slots=True)
class GeneratedWorkload:
    name: str
    seed: int
    parameters: WorkloadParameters
    initial_repository: InMemoryRepository
    events: tuple[Event, ...]


def _weighted_without_replacement(
    rng: random.Random, population_size: int, count: int, skew: float
) -> list[int]:
    remaining = list(range(population_size))
    selected: list[int] = []
    for _ in range(count):
        weights = [1.0 / ((index + 1) ** skew) for index in remaining]
        choice = rng.choices(remaining, weights=weights, k=1)[0]
        selected.append(choice)
        remaining.remove(choice)
    return selected


def generate_workload(
    name: str, parameters: WorkloadParameters, seed: int
) -> GeneratedWorkload:
    """Build a legal initial snapshot and a policy/delete update pair.

    The policy event flips exactly ``f`` active observation decisions before
    the deletion.  The deletion targets a minimum-fanout chunk for ``local``,
    a maximum-fanout chunk for ``high_fanout``, and a seeded random chunk for
    ``mixed``.  No model calls or random semantic labels occur at run time.
    """
    rng = random.Random(seed)
    repository = InMemoryRepository()
    apply_event(
        repository,
        PolicyChangeEvent(
            event_id=f"{name}-setup-policy",
            policy=DecisionPolicy(f"{name}-k0", 0.8, 0.8),
        ),
    )

    claim_ids = [f"{name}-claim-{index}" for index in range(parameters.C)]
    answer_claims: list[list[str]] = [[] for _ in range(parameters.A)]
    for index, claim_id in enumerate(claim_ids):
        answer_claims[index % parameters.A].append(claim_id)
    for answer_index, claims in enumerate(answer_claims):
        answer_id = f"{name}-answer-{answer_index}"
        question_id = f"{name}-question-{answer_index}"
        repository.register_question(Question(question_id, f"question {answer_index}"))
        repository.register_answer(
            AnswerVersion(answer_id, question_id, f"answer {answer_index}", _STAMP),
            tuple(
                Claim(claim_id, answer_id, f"claim {claim_id}", _STAMP, True)
                for claim_id in claims
            ),
        )

    chunk_count = (parameters.E + parameters.k - 1) // parameters.k
    duplicate_chunks = int(chunk_count * parameters.duplicate_content_ratio)
    chunk_ids: list[str] = []
    version_ids: list[str] = []
    for chunk_index in range(chunk_count):
        document_id = f"{name}-document-{chunk_index}"
        version_id = f"{name}-version-{chunk_index}"
        chunk_id = f"{name}-chunk-{chunk_index}"
        if duplicate_chunks and chunk_index >= chunk_count - duplicate_chunks:
            text = f"{name} duplicate evidence"
        else:
            text = f"{name} distinct evidence {chunk_index}"
        apply_event(
            repository,
            InsertDocumentEvent(
                event_id=f"{name}-setup-insert-{chunk_index}",
                document_id=document_id,
                document_version_id=version_id,
                content_hash=f"content-{version_id}",
                chunks=(ChunkInput(chunk_id, 0, text),),
            ),
        )
        chunk_ids.append(chunk_id)
        version_ids.append(version_id)

    remaining = parameters.E
    observation_index = 0
    fanouts: list[int] = []
    for chunk_id in chunk_ids:
        fanout = min(parameters.k, remaining)
        fanouts.append(fanout)
        selected_claims = _weighted_without_replacement(
            rng, parameters.C, fanout, parameters.skew
        )
        for claim_index in selected_claims:
            is_flip = observation_index < parameters.f
            support_score = 0.7 if is_flip else 0.9
            observation = SemanticObservation(
                observation_id=f"{name}-observation-{observation_index}",
                subject_kind=SubjectKind.CLAIM,
                subject_id=claim_ids[claim_index],
                chunk_version_id=chunk_id,
                task_type="verify",
                support_score=support_score,
                refute_score=0.05,
                neutral_score=0.05,
                producer=_STAMP,
                input_hash=f"input-{name}-{observation_index}",
            )
            apply_event(
                repository,
                ObserveEvent(
                    event_id=f"{name}-setup-observe-{observation_index}",
                    observation=observation,
                ),
            )
            observation_index += 1
        remaining -= fanout

    if parameters.locality is Locality.LOCAL:
        target_index = min(range(chunk_count), key=fanouts.__getitem__)
    elif parameters.locality is Locality.HIGH_FANOUT:
        target_index = max(range(chunk_count), key=fanouts.__getitem__)
    else:
        target_index = rng.randrange(chunk_count)
    events: tuple[Event, ...] = (
        PolicyChangeEvent(
            event_id=f"{name}-policy-flip",
            policy=DecisionPolicy(f"{name}-k1", 0.6, 0.8),
        ),
        DeleteDocumentVersionEvent(
            event_id=f"{name}-delete",
            document_version_id=version_ids[target_index],
        ),
    )
    return GeneratedWorkload(name, seed, parameters, repository, events)
