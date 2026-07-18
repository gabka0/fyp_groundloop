"""Paired benchmark runner with explicit kernel and staged timing scopes."""

from __future__ import annotations

import sys
import time
from copy import deepcopy
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, Protocol

from groundloop.baselines.engines import (
    DirectCitationInvalidationBaseline,
    EngineEvaluation,
    FullRecomputationBaseline,
    KeyedRecomputationBaseline,
    SignedDeltaTreatment,
    SourceInvalidationBaseline,
)
from groundloop.baselines.models import MetricsRecord, TimingScope, TouchedObjects
from groundloop.baselines.workload import GeneratedWorkload
from groundloop.domain import AnswerState, ClaimState
from groundloop.events import Event, apply_event
from groundloop.repository import InMemoryRepository


class BaselineEngine(Protocol):
    name: str
    engine_class: Any
    semantic_equivalent: bool

    def process(
        self,
        event: Event,
        before: InMemoryRepository,
        after: InMemoryRepository,
    ) -> EngineEvaluation: ...


def _deep_size(value: object) -> int:
    """Approximate retained Python bytes, deduplicating shared references."""
    seen: set[int] = set()

    def visit(item: object) -> int:
        identifier = id(item)
        if identifier in seen:
            return 0
        seen.add(identifier)
        size = sys.getsizeof(item)
        if isinstance(item, dict):
            return size + sum(visit(key) + visit(child) for key, child in item.items())
        if isinstance(item, (tuple, list, set, frozenset)):
            return size + sum(visit(child) for child in item)
        if is_dataclass(item) and not isinstance(item, type):
            return size + sum(
                visit(getattr(item, field.name)) for field in fields(item)
            )
        return size

    return visit(value)


def _engine_state(engine: BaselineEngine) -> object:
    if isinstance(engine, SignedDeltaTreatment):
        return engine.engine
    if isinstance(engine, (FullRecomputationBaseline, KeyedRecomputationBaseline)):
        return (engine.claim_states, engine.answer_states)
    if isinstance(
        engine, (SourceInvalidationBaseline, DirectCitationInvalidationBaseline)
    ):
        return (engine.last_invalid_claims, engine.last_invalid_answers)
    raise TypeError(f"unknown baseline engine {type(engine)!r}")


def _new_engines(repository: InMemoryRepository) -> list[BaselineEngine]:
    return [
        FullRecomputationBaseline(repository),
        KeyedRecomputationBaseline(repository),
        SignedDeltaTreatment(repository),
        SourceInvalidationBaseline(repository),
        DirectCitationInvalidationBaseline(repository),
    ]


def _changed_keys(
    before_claims: dict[str, ClaimState],
    before_answers: dict[str, AnswerState],
    after_claims: dict[str, ClaimState],
    after_answers: dict[str, AnswerState],
) -> tuple[set[str], set[str]]:
    claims = {
        claim_id
        for claim_id, state in after_claims.items()
        if before_claims[claim_id].status is not state.status
    }
    answers = {
        answer_id
        for answer_id, state in after_answers.items()
        if before_answers[answer_id].status is not state.status
    }
    return claims, answers


def _disagreement(
    evaluation: EngineEvaluation,
    true_claims: set[str],
    true_answers: set[str],
) -> tuple[int, int, int]:
    predicted_claims = set(evaluation.predicted_invalid_claim_ids)
    predicted_answers = set(evaluation.predicted_invalid_answer_ids)
    false_invalidations = len(predicted_claims - true_claims) + len(
        predicted_answers - true_answers
    )
    stale_exposure = len(true_claims - predicted_claims) + len(
        true_answers - predicted_answers
    )
    return false_invalidations + stale_exposure, false_invalidations, stale_exposure


def _event_type(event: Event) -> str:
    name = type(event).__name__
    return name.removesuffix("Event")


def _record(
    *,
    workload: GeneratedWorkload,
    run_id: str,
    trial: int,
    event_index: int,
    event: Event,
    engine: BaselineEngine,
    evaluation: EngineEvaluation,
    elapsed_ns: int,
    timing_scope: TimingScope,
    maintained_bytes: int,
    true_claims: set[str],
    true_answers: set[str],
) -> MetricsRecord:
    if engine.semantic_equivalent:
        disagreement = false_invalidations = stale_exposure = 0
    else:
        disagreement, false_invalidations, stale_exposure = _disagreement(
            evaluation, true_claims, true_answers
        )
    staged = timing_scope is TimingScope.WITH_ORACLE_STAGING
    return MetricsRecord(
        run_id=run_id,
        scenario=workload.name,
        seed=workload.seed,
        trial=trial,
        event_index=event_index,
        workload=workload.parameters,
        engine=engine.name,
        engine_class=engine.engine_class,
        semantic_equivalent=engine.semantic_equivalent,
        event_type=_event_type(event),
        timing_scope=timing_scope,
        wall_time_ns=elapsed_ns,
        touched_objects=TouchedObjects(
            claims=evaluation.claims_touched,
            answers=evaluation.answers_touched,
            observations=evaluation.observations_touched,
        ),
        candidate_count=evaluation.candidate_count,
        status_changes=evaluation.status_changes,
        maintained_bytes=maintained_bytes,
        oracle_included=staged,
        copy_staging_included=staged,
        semantic_disagreement_count=disagreement,
        false_invalidation_count=false_invalidations,
        stale_state_exposure_count=stale_exposure,
    )


def _run_trial(
    workload: GeneratedWorkload, run_id: str, trial: int, record: bool
) -> list[MetricsRecord]:
    repository = deepcopy(workload.initial_repository)
    engines = _new_engines(repository)
    records: list[MetricsRecord] = []
    for event_index, event in enumerate(workload.events):
        before = repository
        before_full = engines[0]
        assert isinstance(before_full, FullRecomputationBaseline)
        before_claims = dict(before_full.claim_states)
        before_answers = dict(before_full.answer_states)

        after = deepcopy(before)
        apply_event(after, event)

        staged_runs: list[tuple[BaselineEngine, EngineEvaluation, int, int]] = []
        for engine in engines:
            staged_engine = deepcopy(engine)
            started = time.perf_counter_ns()
            staged_before = deepcopy(before)
            staged_after = deepcopy(staged_before)
            apply_event(staged_after, event)
            staged_evaluation = staged_engine.process(
                event, staged_before, staged_after
            )
            elapsed = time.perf_counter_ns() - started
            staged_runs.append(
                (
                    staged_engine,
                    staged_evaluation,
                    elapsed,
                    _deep_size(_engine_state(staged_engine)),
                )
            )

        kernel_runs: list[tuple[BaselineEngine, EngineEvaluation, int, int]] = []
        for engine in engines:
            started = time.perf_counter_ns()
            evaluation = engine.process(event, before, after)
            elapsed = time.perf_counter_ns() - started
            kernel_runs.append(
                (engine, evaluation, elapsed, _deep_size(_engine_state(engine)))
            )

        global_evaluation = kernel_runs[0][1]
        assert global_evaluation.claim_states is not None
        assert global_evaluation.answer_states is not None
        for exact_index in (1, 2):
            exact_evaluation = kernel_runs[exact_index][1]
            if exact_evaluation.claim_states != global_evaluation.claim_states:
                raise AssertionError(
                    f"{engines[exact_index].name} claim state differs from global "
                    f"recomputation after {event.event_id}"
                )
            if exact_evaluation.answer_states != global_evaluation.answer_states:
                raise AssertionError(
                    f"{engines[exact_index].name} answer state differs from global "
                    f"recomputation after {event.event_id}"
                )
        true_claims, true_answers = _changed_keys(
            before_claims,
            before_answers,
            global_evaluation.claim_states,
            global_evaluation.answer_states,
        )
        if record:
            for engine, evaluation, elapsed, maintained in staged_runs:
                records.append(
                    _record(
                        workload=workload,
                        run_id=run_id,
                        trial=trial,
                        event_index=event_index,
                        event=event,
                        engine=engine,
                        evaluation=evaluation,
                        elapsed_ns=elapsed,
                        timing_scope=TimingScope.WITH_ORACLE_STAGING,
                        maintained_bytes=maintained,
                        true_claims=true_claims,
                        true_answers=true_answers,
                    )
                )
            for engine, evaluation, elapsed, maintained in kernel_runs:
                records.append(
                    _record(
                        workload=workload,
                        run_id=run_id,
                        trial=trial,
                        event_index=event_index,
                        event=event,
                        engine=engine,
                        evaluation=evaluation,
                        elapsed_ns=elapsed,
                        timing_scope=TimingScope.KERNEL_ONLY,
                        maintained_bytes=maintained,
                        true_claims=true_claims,
                        true_answers=true_answers,
                    )
                )
        repository = after
    return records


def run_paired_benchmark(
    workload: GeneratedWorkload,
    *,
    run_id: str,
    warmup: int,
    repetitions: int,
) -> list[MetricsRecord]:
    if warmup < 0 or repetitions <= 0:
        raise ValueError("warmup must be nonnegative and repetitions positive")
    for warmup_index in range(warmup):
        _run_trial(workload, run_id, -(warmup_index + 1), record=False)
    records: list[MetricsRecord] = []
    for trial in range(repetitions):
        records.extend(_run_trial(workload, run_id, trial, record=True))
    return records


def write_jsonl(path: Path, records: list[MetricsRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{record.to_json()}\n" for record in records))
