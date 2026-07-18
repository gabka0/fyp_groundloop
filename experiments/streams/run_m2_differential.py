#!/usr/bin/env python3
"""Generate bounded randomized streams and check M2 after every event.

The streams are deliberately sharded. Event history is immutable, so keeping
each stream bounded prevents the M1 copy-and-commit oracle from turning a
100,000-event correctness run into a quadratic memory benchmark. The reported
event count is aggregate across independent, reproducibly seeded streams.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import dataclass

from groundloop.differential import DifferentialRunner
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
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
    apply_event,
)
from groundloop.repository import InMemoryRepository

STAMP = ModelStamp("stream-generator", "v1", "p1")
TEXT_POOL = (
    "shared alpha evidence",
    "shared beta evidence",
    "independent gamma evidence",
    "independent delta evidence",
)


@dataclass(slots=True)
class _DocumentCursor:
    document_id: str
    generation: int = 0
    version_id: str | None = None
    chunk_id: str | None = None


def _base_repository(stream_id: int) -> InMemoryRepository:
    repository = InMemoryRepository()
    apply_event(
        repository,
        PolicyChangeEvent(
            event_id=f"s{stream_id}-policy-0",
            policy=DecisionPolicy(f"s{stream_id}-k0", 0.8, 0.8),
        ),
    )
    answer_id = f"s{stream_id}-answer"
    question_id = f"s{stream_id}-question"
    repository.register_question(Question(question_id, "generated question"))
    repository.register_answer(
        AnswerVersion(answer_id, question_id, "generated answer", STAMP),
        tuple(
            Claim(
                claim_id=f"s{stream_id}-claim-{index}",
                answer_version_id=answer_id,
                text=f"claim {index}",
                extractor=STAMP,
                required=True,
            )
            for index in range(3)
        ),
    )
    return repository


def _insert_event(
    stream_id: int,
    event_index: int,
    cursor: _DocumentCursor,
    rng: random.Random,
) -> InsertDocumentEvent:
    cursor.generation += 1
    cursor.version_id = f"s{stream_id}-{cursor.document_id}-v{cursor.generation}"
    cursor.chunk_id = f"s{stream_id}-{cursor.document_id}-p{cursor.generation}"
    return InsertDocumentEvent(
        event_id=f"s{stream_id}-event-{event_index}",
        document_id=cursor.document_id,
        document_version_id=cursor.version_id,
        content_hash=f"content-{cursor.version_id}",
        chunks=(ChunkInput(cursor.chunk_id, 0, rng.choice(TEXT_POOL)),),
    )


def run_randomized_differential(
    *, total_events: int, events_per_stream: int, seed: int
) -> dict[str, int | float]:
    """Run exactly ``total_events`` successful events and return metrics."""
    if total_events <= 0 or events_per_stream <= 0:
        raise ValueError("event counts must be positive")
    rng = random.Random(seed)
    completed = 0
    stream_id = 0
    started = time.perf_counter()

    while completed < total_events:
        repository = _base_repository(stream_id)
        runner = DifferentialRunner.from_repository(repository)
        cursors = [_DocumentCursor(f"doc-{index}") for index in range(3)]
        stream_budget = min(events_per_stream, total_events - completed)

        for local_index in range(stream_budget):
            event_index = completed + local_index + 1
            active = [cursor for cursor in cursors if cursor.version_id is not None]
            inactive = [cursor for cursor in cursors if cursor.version_id is None]
            choice = rng.random()

            if not active or (inactive and choice < 0.12):
                cursor = rng.choice(inactive)
                event = _insert_event(stream_id, event_index, cursor, rng)
            elif choice < 0.20:
                cursor = rng.choice(active)
                assert cursor.version_id is not None
                event = DeleteDocumentVersionEvent(
                    event_id=f"s{stream_id}-event-{event_index}",
                    document_version_id=cursor.version_id,
                )
                cursor.version_id = None
                cursor.chunk_id = None
            elif choice < 0.30:
                cursor = rng.choice(active)
                assert cursor.version_id is not None
                old_version_id = cursor.version_id
                cursor.generation += 1
                cursor.version_id = (
                    f"s{stream_id}-{cursor.document_id}-v{cursor.generation}"
                )
                cursor.chunk_id = (
                    f"s{stream_id}-{cursor.document_id}-p{cursor.generation}"
                )
                event = ReplaceDocumentVersionEvent(
                    event_id=f"s{stream_id}-event-{event_index}",
                    document_id=cursor.document_id,
                    old_document_version_id=old_version_id,
                    new_document_version_id=cursor.version_id,
                    content_hash=f"content-{cursor.version_id}",
                    chunks=(ChunkInput(cursor.chunk_id, 0, rng.choice(TEXT_POOL)),),
                )
            elif choice < 0.42:
                threshold_values = (0.25, 0.5, 0.7, 0.8, 0.9)
                event = PolicyChangeEvent(
                    event_id=f"s{stream_id}-event-{event_index}",
                    policy=DecisionPolicy(
                        policy_version=f"s{stream_id}-k{event_index}",
                        support_threshold=rng.choice(threshold_values),
                        refute_threshold=rng.choice(threshold_values),
                    ),
                )
            else:
                cursor = rng.choice(active)
                assert cursor.chunk_id is not None
                claim_index = rng.randrange(3)
                observation_id = f"s{stream_id}-observation-{event_index}"
                event = ObserveEvent(
                    event_id=f"s{stream_id}-event-{event_index}",
                    observation=SemanticObservation(
                        observation_id=observation_id,
                        subject_kind=SubjectKind.CLAIM,
                        subject_id=f"s{stream_id}-claim-{claim_index}",
                        chunk_version_id=cursor.chunk_id,
                        task_type="verify",
                        support_score=rng.random(),
                        refute_score=rng.random(),
                        neutral_score=rng.random(),
                        producer=STAMP,
                        input_hash=f"input-{observation_id}",
                    ),
                )

            runner.apply(event)

        completed += stream_budget
        stream_id += 1

    elapsed = time.perf_counter() - started
    return {
        "events": completed,
        "streams": stream_id,
        "seed": seed,
        "events_per_stream": events_per_stream,
        "elapsed_seconds": round(elapsed, 6),
        "events_per_second": round(completed / elapsed, 3),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--events-per-stream", type=int, default=100)
    parser.add_argument("--seed", type=int, default=20260718)
    args = parser.parse_args()
    print(
        json.dumps(
            run_randomized_differential(
                total_events=args.events,
                events_per_stream=args.events_per_stream,
                seed=args.seed,
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
