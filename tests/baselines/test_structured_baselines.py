"""Correctness and semantic-boundary tests for structured baselines."""

from __future__ import annotations

import json
from copy import deepcopy

from groundloop.baselines.analysis import summarize
from groundloop.baselines.engines import (
    DirectCitationInvalidationBaseline,
    FullRecomputationBaseline,
    KeyedRecomputationBaseline,
    SignedDeltaTreatment,
    SourceInvalidationBaseline,
)
from groundloop.baselines.models import (
    METRICS_SCHEMA_VERSION,
    EngineClass,
    Locality,
    TimingScope,
    WorkloadParameters,
)
from groundloop.baselines.runner import run_paired_benchmark
from groundloop.baselines.workload import generate_workload
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
    apply_event,
)
from groundloop.repository import InMemoryRepository

STAMP = ModelStamp("test", "v1", "p1")


def _parameters(locality: Locality = Locality.LOCAL) -> WorkloadParameters:
    return WorkloadParameters(
        E=12,
        C=6,
        A=2,
        k=3,
        f=2,
        duplicate_content_ratio=0.25,
        skew=0.5,
        locality=locality,
    )


def test_generated_workload_is_deterministic_and_has_requested_dimensions() -> None:
    first = generate_workload("deterministic", _parameters(), 41)
    second = generate_workload("deterministic", _parameters(), 41)

    assert first.events == second.events
    assert tuple(first.initial_repository.current_observations()) == tuple(
        second.initial_repository.current_observations()
    )
    assert len(tuple(first.initial_repository.current_observations())) == 12
    assert len(first.initial_repository.all_claim_ids()) == 6
    assert len(first.initial_repository.all_answer_ids()) == 2


def test_three_semantics_equivalent_engines_agree_after_every_event() -> None:
    workload = generate_workload("exact", _parameters(), 73)
    repository = deepcopy(workload.initial_repository)
    full = FullRecomputationBaseline(repository)
    keyed = KeyedRecomputationBaseline(repository)
    treatment = SignedDeltaTreatment(repository)

    for event in workload.events:
        before = repository
        after = deepcopy(before)
        apply_event(after, event)
        full_result = full.process(event, before, after)
        keyed_result = keyed.process(event, before, after)
        treatment_result = treatment.process(event, before, after)
        assert full_result.claim_states == keyed_result.claim_states
        assert full_result.answer_states == keyed_result.answer_states
        assert full_result.claim_states == treatment_result.claim_states
        assert full_result.answer_states == treatment_result.answer_states
        repository = after


def _alternative_witness_repository() -> InMemoryRepository:
    repository = InMemoryRepository()
    apply_event(
        repository,
        PolicyChangeEvent("setup-policy", DecisionPolicy("k0", 0.8, 0.8)),
    )
    repository.register_question(Question("q", "question"))
    repository.register_answer(
        AnswerVersion("a", "q", "answer", STAMP),
        (Claim("c", "a", "claim", STAMP, True),),
    )
    for suffix in ("one", "two"):
        apply_event(
            repository,
            InsertDocumentEvent(
                event_id=f"insert-{suffix}",
                document_id=f"doc-{suffix}",
                document_version_id=f"version-{suffix}",
                content_hash=f"content-{suffix}",
                chunks=(ChunkInput(f"chunk-{suffix}", 0, f"evidence {suffix}"),),
            ),
        )
        observation = SemanticObservation(
            observation_id=f"observation-{suffix}",
            subject_kind=SubjectKind.CLAIM,
            subject_id="c",
            chunk_version_id=f"chunk-{suffix}",
            task_type="verify",
            support_score=0.9,
            refute_score=0.05,
            neutral_score=0.05,
            producer=STAMP,
            input_hash=f"input-{suffix}",
        )
        apply_event(repository, ObserveEvent(f"observe-{suffix}", observation))
    return repository


def test_heuristics_false_invalidate_alternative_support() -> None:
    before = _alternative_witness_repository()
    event = DeleteDocumentVersionEvent("delete-one", "version-one")
    after = deepcopy(before)
    apply_event(after, event)
    full = FullRecomputationBaseline(before)
    exact = full.process(event, before, after)
    assert exact.status_changes == 0

    for heuristic in (
        DirectCitationInvalidationBaseline(before),
        SourceInvalidationBaseline(before),
    ):
        result = heuristic.process(event, before, after)
        assert heuristic.semantic_equivalent is False
        assert result.claim_states is None
        assert result.answer_states is None
        assert result.predicted_invalid_claim_ids == frozenset({"c"})
        assert result.predicted_invalid_answer_ids == frozenset({"a"})


def test_raw_schema_and_summary_exclude_heuristics_from_exact_speedups() -> None:
    workload = generate_workload("schema", _parameters(), 99)
    records = run_paired_benchmark(workload, run_id="test-run", warmup=0, repetitions=1)
    assert len(records) == 20
    payload = json.loads(records[0].to_json())
    assert payload["schema_version"] == METRICS_SCHEMA_VERSION
    assert set(payload["workload"]) == {
        "E",
        "C",
        "A",
        "k",
        "f",
        "duplicate_content_ratio",
        "skew",
        "locality",
    }
    assert {record.timing_scope for record in records} == {
        TimingScope.KERNEL_ONLY,
        TimingScope.WITH_ORACLE_STAGING,
    }
    assert all(
        record.semantic_disagreement_count == 0
        for record in records
        if record.semantic_equivalent
    )

    rows = summarize(records)
    assert all(
        row.speedup_vs_global_full is None
        for row in rows
        if not row.semantic_equivalent
    )
    assert all(
        row.speedup_vs_global_full is not None
        for row in rows
        if row.semantic_equivalent
    )
    assert {
        record.engine_class for record in records if not record.semantic_equivalent
    } == {EngineClass.HEURISTIC_POLICY}


def test_high_fanout_workload_exposes_honest_degradation() -> None:
    local = generate_workload("local", _parameters(Locality.LOCAL), 101)
    high_parameters = WorkloadParameters(
        E=12,
        C=6,
        A=2,
        k=6,
        f=6,
        duplicate_content_ratio=0.0,
        skew=0.0,
        locality=Locality.HIGH_FANOUT,
    )
    high = generate_workload("high", high_parameters, 101)
    local_records = run_paired_benchmark(local, run_id="local", warmup=0, repetitions=1)
    high_records = run_paired_benchmark(high, run_id="high", warmup=0, repetitions=1)
    local_delete = next(
        record
        for record in local_records
        if record.engine == "signed_delta_treatment"
        and record.event_type == "DeleteDocumentVersion"
        and record.timing_scope is TimingScope.KERNEL_ONLY
    )
    high_delete = next(
        record
        for record in high_records
        if record.engine == "signed_delta_treatment"
        and record.event_type == "DeleteDocumentVersion"
        and record.timing_scope is TimingScope.KERNEL_ONLY
    )
    assert (
        high_delete.touched_objects.observations
        > local_delete.touched_objects.observations
    )
    assert high_delete.touched_objects.claims >= local_delete.touched_objects.claims
