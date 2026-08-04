from __future__ import annotations

import hashlib

from groundloop.m5.digests import stable_m5_digest, text_field
from groundloop.m5.evaluation.baselines import (
    Availability,
    BaselineId,
    EvaluationClaimStatus,
    MeasurementValidity,
    run_baselines,
)
from groundloop.m5.evaluation.histories import (
    CohortKind,
    ControlledHistory,
    EvaluationGroup,
    EvaluationRequirement,
    EvaluationUnit,
    HistoryInitialState,
    HistoryOperation,
    HistoryOperationKind,
    HistoryStratum,
    build_authored_controlled_histories,
    build_evaluation_event,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _direct_only_history() -> ControlledHistory:
    history_id = stable_m5_digest(
        "m5-test-direct-only-history-v1", text_field("direct-only")
    )
    units = (
        EvaluationUnit(
            "inactive-requirement-unit",
            _sha("requirement"),
            "source-requirement",
        ),
        EvaluationUnit(
            "direct-unit",
            _sha("direct"),
            "source-direct",
            independently_annotated_direct_support=True,
        ),
    )
    group = EvaluationGroup(
        "direct-only-group",
        (
            EvaluationRequirement(
                "direct-only-requirement", ("inactive-requirement-unit",)
            ),
        ),
    )
    operations = (
        HistoryOperation(HistoryOperationKind.INITIALIZE),
        HistoryOperation(
            HistoryOperationKind.SOURCE_DELETE,
            source_version_id="source-direct",
        ),
    )
    return ControlledHistory(
        history_id=history_id,
        cluster_id="direct-only-source-cluster",
        cohort_kind=CohortKind.CONTROLLED,
        tags=("direct_only",),
        initial=HistoryInitialState(
            units=units,
            groups=(group,),
            active_unit_ids=("direct-unit",),
            active_source_version_ids=("source-direct", "source-requirement"),
            active_group_ids=(group.group_id,),
        ),
        events=tuple(
            build_evaluation_event(history_id, ordinal, operation)
            for ordinal, operation in enumerate(operations)
        ),
        stratum=HistoryStratum(
            requirement_count=1,
            maximum_requirement_degree=1,
            has_content_overlap=False,
            initial_sdr_complete=False,
            has_alternative_assignment=False,
            provenance="explicit-test-fixture",
        ),
    )


def test_authored_histories_cover_the_frozen_dynamic_cases() -> None:
    histories = build_authored_controlled_histories()
    tags = {tag for history in histories for tag in history.tags}
    operations = {
        event.operation.kind for history in histories for event in history.events
    }

    assert {
        "duplicate_content",
        "alternative_witness",
        "alternative_groups",
        "final_witness_loss",
        "matching_only_loss",
        "direct_support",
        "refutation_conflict",
        "supersession",
        "policy_change",
    } <= tags
    assert {
        HistoryOperationKind.UNIT_INSERT,
        HistoryOperationKind.UNIT_DELETE,
        HistoryOperationKind.UNIT_REPLACE,
        HistoryOperationKind.SOURCE_INSERT,
        HistoryOperationKind.SOURCE_DELETE,
        HistoryOperationKind.GROUP_SUPERSEDE,
        HistoryOperationKind.POLICY_CHANGE,
    } <= operations


def test_every_baseline_consumes_identical_event_ids_and_hashes() -> None:
    histories = build_authored_controlled_histories()
    run = run_baselines(histories)

    assert len(run.baseline_specs) == 7
    for history in histories:
        for event in history.events:
            points = tuple(
                point
                for point in run.points
                if point.history_id == history.history_id
                and point.event_ordinal == event.ordinal
            )
            assert len(points) == 7
            assert {point.event_id for point in points} == {event.event_id}
            assert {point.event_hash for point in points} == {event.event_hash}
    assert all(point.work.model_calls == 0 for point in run.points)
    assert all(point.work.model_tokens == 0 for point in run.points)


def test_direct_witness_unavailability_is_explicit_per_cohort() -> None:
    run = run_baselines(build_authored_controlled_histories())
    direct = tuple(
        point for point in run.points if point.baseline_id is BaselineId.DIRECT_WITNESS
    )

    assert any(point.availability is Availability.AVAILABLE for point in direct)
    assert any(point.availability is Availability.UNAVAILABLE for point in direct)
    assert all(
        point.unavailable_reason
        == "no independently annotated whole-claim evidence unit"
        for point in direct
        if point.availability is Availability.UNAVAILABLE
    )


def test_non_distinct_ablation_shares_the_direct_support_disjunct() -> None:
    history = next(
        item
        for item in build_authored_controlled_histories()
        if "direct_support" in item.tags
    )
    run = run_baselines((history,))
    after_group_loss = {
        point.baseline_id: point for point in run.points if point.event_ordinal == 1
    }

    assert after_group_loss[BaselineId.NON_DISTINCT_CONJUNCTION].y_hat is True
    assert after_group_loss[BaselineId.GROUNDLOOP_HALL_SDR].y_hat is True
    assert after_group_loss[BaselineId.DIRECT_WITNESS].y_hat is True
    assert after_group_loss[BaselineId.NON_DISTINCT_CONJUNCTION].direct_source_support


def test_conflict_and_refute_only_states_use_the_full_status_truth_table() -> None:
    history = next(
        item
        for item in build_authored_controlled_histories()
        if "refutation_conflict" in item.tags
    )
    run = run_baselines((history,))
    target = {
        point.event_ordinal: point
        for point in run.points
        if point.baseline_id is BaselineId.GROUNDLOOP_HALL_SDR
    }

    assert target[1].predicted_status is EvaluationClaimStatus.CONFLICTED
    assert target[1].exact_status is EvaluationClaimStatus.CONFLICTED
    assert target[2].predicted_status is EvaluationClaimStatus.REFUTED
    assert target[2].exact_status is EvaluationClaimStatus.REFUTED


def test_pure_hall_scaffold_cannot_emit_target_runtime_latency() -> None:
    run = run_baselines(build_authored_controlled_histories(), measure_latency=True)
    target = tuple(
        point
        for point in run.points
        if point.baseline_id is BaselineId.GROUNDLOOP_HALL_SDR
    )
    affected = tuple(
        point
        for point in run.points
        if point.baseline_id is BaselineId.AFFECTED_GROUP_FULL_MATCHING
    )
    all_group = tuple(
        point
        for point in run.points
        if point.baseline_id is BaselineId.ALL_GROUP_FULL_RECOMPUTATION
    )

    assert all(point.latency_ns is None for point in target)
    assert all(point.timing_mode == "disabled_functional_only" for point in target)
    assert all(
        point.execution_backend == "pure-affected-group-hall-recompute-scaffold-v1"
        for point in target
    )
    assert all(
        point.measurement_validity is MeasurementValidity.FUNCTIONAL_PROTOCOL_ONLY
        for point in target
    )
    assert all(point.latency_ns is not None for point in (*affected, *all_group))


def test_exact_matching_comparators_match_target_labels_and_have_exact_counters() -> (
    None
):
    run = run_baselines(build_authored_controlled_histories())
    by_key = {
        (point.history_id, point.event_ordinal, point.baseline_id): point
        for point in run.points
    }
    for history in build_authored_controlled_histories():
        for event in history.events:
            target = by_key[
                (history.history_id, event.ordinal, BaselineId.GROUNDLOOP_HALL_SDR)
            ]
            for comparator in (
                BaselineId.AFFECTED_GROUP_FULL_MATCHING,
                BaselineId.ALL_GROUP_FULL_RECOMPUTATION,
            ):
                point = by_key[(history.history_id, event.ordinal, comparator)]
                assert point.y_hat == target.y_hat
                assert point.predicted_status is target.predicted_status
                assert point.groundloop_sdr_complete == target.groundloop_sdr_complete
                assert point.work.groups_rematched <= point.work.group_keys_touched
                assert point.work.edge_checks >= 0


def test_source_invalidation_freezes_independent_direct_citation_source() -> None:
    run = run_baselines((_direct_only_history(),))
    source = tuple(
        point
        for point in run.points
        if point.baseline_id is BaselineId.SOURCE_INVALIDATION
    )
    frozen_requirement = tuple(
        point
        for point in run.points
        if point.baseline_id is BaselineId.FROZEN_DIRECT_CITATION
    )

    assert tuple(point.y_hat for point in source) == (True, False)
    assert tuple(point.y_hat for point in frozen_requirement) == (False, False)
