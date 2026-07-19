"""Controlled dynamic workload and machine-report acceptance tests."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from groundloop.domain import AnswerStatus, ClaimStatus
from groundloop.errors import ValidationError
from groundloop.m4.contracts import PairKey, UpdateKind, stable_m4_digest
from groundloop.m4.oracles import (
    AnswerStatusResult,
    BootstrapConfig,
    ClaimStatusResult,
    ControlledWorkload,
    EvaluationProvenance,
    MetricName,
    OracleEventResult,
    OracleKind,
    TreatmentEventResult,
    WorkloadSplit,
    build_controlled_dynamic_workload_v1,
    build_paired_evaluation_report,
    derive_event_metric_record,
)


def _hash(label: str) -> str:
    return stable_m4_digest("wave3-fixture", label)


def _provenance(
    workload: ControlledWorkload,
    split: WorkloadSplit,
    policy_id: str,
) -> EvaluationProvenance:
    return EvaluationProvenance(
        schema_version="m4-evaluation-v1",
        dataset_version=workload.dataset_version,
        split_id=split.value,
        split_manifest_hash=workload.split_manifest_hash,
        seed_manifest_hash=workload.seed_manifest_hash,
        treatment_policy_id=policy_id,
        treatment_policy_hash=_hash(f"policy-{policy_id}"),
        verifier_model_artifact_id="controlled-verifier-v1",
        verifier_execution_spec_hash=_hash("verifier-execution"),
        decision_policy_id="controlled-decision-v1",
        oracle_kind=OracleKind.EXHAUSTIVE_DELTA,
        baseline_id="Bw-to-Bx",
        oracle_policy_id="controlled-full-pair-v1",
        oracle_policy_hash=_hash("oracle-policy"),
    )


def _controlled_results(
    workload: ControlledWorkload,
    split: WorkloadSplit,
) -> tuple[
    tuple[OracleEventResult, ...],
    tuple[TreatmentEventResult, ...],
    tuple[TreatmentEventResult, ...],
]:
    oracle_results: list[OracleEventResult] = []
    first_results: list[TreatmentEventResult] = []
    second_results: list[TreatmentEventResult] = []
    for history in workload.histories_for_split(split):
        for event in history.events:
            decoy_claim, missed_claim = event.registered_claim_ids
            answer_id = event.registered_answer_ids[0]
            if event.update_kind is UpdateKind.INSERT:
                inserted = event.inserted_chunk_version_ids[0]
                positive = (PairKey(missed_claim, inserted),)
                oracle_claim = ClaimStatusResult(
                    missed_claim, ClaimStatus.SUPPORTED
                )
                oracle_answer = AnswerStatusResult(answer_id, AnswerStatus.VALID)
                first_admitted = (PairKey(decoy_claim, inserted),)
                first_claim = ClaimStatusResult(
                    missed_claim, ClaimStatus.UNSUPPORTED
                )
                first_answer = AnswerStatusResult(
                    answer_id, AnswerStatus.UNSUPPORTED
                )
                second_admitted = positive
                second_claim = oracle_claim
                second_answer = oracle_answer
            elif event.update_kind is UpdateKind.REPLACE:
                inserted = event.inserted_chunk_version_ids[0]
                positive = (PairKey(missed_claim, inserted),)
                oracle_claim = ClaimStatusResult(missed_claim, ClaimStatus.REFUTED)
                oracle_answer = AnswerStatusResult(
                    answer_id, AnswerStatus.CONTRADICTED
                )
                first_admitted = positive
                first_claim = oracle_claim
                first_answer = oracle_answer
                second_admitted = positive
                second_claim = oracle_claim
                second_answer = oracle_answer
            else:
                positive = ()
                oracle_claim = ClaimStatusResult(
                    missed_claim, ClaimStatus.UNSUPPORTED
                )
                oracle_answer = AnswerStatusResult(
                    answer_id, AnswerStatus.UNSUPPORTED
                )
                first_admitted = ()
                first_claim = oracle_claim
                first_answer = oracle_answer
                second_admitted = ()
                second_claim = oracle_claim
                second_answer = oracle_answer

            oracle_results.append(
                OracleEventResult(
                    event_id=event.event_id,
                    manifest_id=f"oracle-{event.event_id}",
                    positive_pairs=positive,
                    affected_claim_statuses=(oracle_claim,),
                    affected_answer_statuses=(oracle_answer,),
                )
            )
            first_results.append(
                TreatmentEventResult(
                    event_id=event.event_id,
                    manifest_id=f"first-{event.event_id}",
                    admitted_pairs=first_admitted,
                    claim_post_statuses=(first_claim,),
                    answer_post_statuses=(first_answer,),
                )
            )
            second_results.append(
                TreatmentEventResult(
                    event_id=event.event_id,
                    manifest_id=f"second-{event.event_id}",
                    admitted_pairs=second_admitted,
                    claim_post_statuses=(second_claim,),
                    answer_post_statuses=(second_answer,),
                )
            )
    return tuple(oracle_results), tuple(first_results), tuple(second_results)


def test_builtin_workload_is_deterministic_dynamic_and_canonical() -> None:
    first = build_controlled_dynamic_workload_v1()
    second = build_controlled_dynamic_workload_v1()

    assert first == second
    assert first.manifest_hash == second.manifest_hash
    assert first.to_canonical_json() == second.to_canonical_json()
    assert len(first.histories) == 4
    assert {
        history.split for history in first.histories
    } == {WorkloadSplit.DEVELOPMENT, WorkloadSplit.TEST}
    assert all(
        tuple(event.update_kind for event in history.events)
        == (UpdateKind.INSERT, UpdateKind.REPLACE, UpdateKind.DELETE)
        for history in first.histories
    )
    assert all(history.events[0].deliberate_miss_pairs for history in first.histories)
    assert all(
        history.events[2].inserted_chunk_version_ids == ()
        for history in first.histories
    )
    payload = json.loads(first.to_canonical_json())
    assert payload["manifest_hash"] == first.manifest_hash
    assert payload["seed_manifest_hash"] == first.seed_manifest_hash
    assert payload["split_manifest_hash"] == first.split_manifest_hash

    changed_seed = replace(first, generator_seed=first.generator_seed + 1)
    assert changed_seed.seed_manifest_hash != first.seed_manifest_hash
    assert changed_seed.split_manifest_hash == first.split_manifest_hash
    changed_histories = list(first.histories)
    changed_histories[0] = replace(
        changed_histories[0], split=WorkloadSplit.TEST
    )
    changed_split = replace(first, histories=tuple(changed_histories))
    assert changed_split.seed_manifest_hash == first.seed_manifest_hash
    assert changed_split.split_manifest_hash != first.split_manifest_hash


def test_event_shape_and_history_snapshot_chain_fail_closed() -> None:
    workload = build_controlled_dynamic_workload_v1()
    history = workload.histories[0]
    insert = history.events[0]

    with pytest.raises(ValidationError, match="update kind"):
        replace(insert, deactivated_chunk_version_ids=("old-chunk",))
    with pytest.raises(ValidationError, match="outside inserted domain"):
        replace(
            insert,
            deliberate_miss_pairs=(
                PairKey(insert.registered_claim_ids[0], "not-inserted"),
            ),
        )
    with pytest.raises(ValidationError, match="snapshot chain"):
        replace(
            history,
            events=(
                insert,
                replace(
                    history.events[1],
                    corpus_snapshot_before_hash=_hash("wrong-before"),
                ),
                history.events[2],
            ),
        )


def test_workload_rejects_content_and_claim_family_leakage_across_splits() -> None:
    workload = build_controlled_dynamic_workload_v1()
    histories = list(workload.histories)
    development = next(
        history
        for history in histories
        if history.split is WorkloadSplit.DEVELOPMENT
    )
    test_index = next(
        index
        for index, history in enumerate(histories)
        if history.split is WorkloadSplit.TEST
    )
    test_history = histories[test_index]
    leaked_content = tuple(
        sorted(
            (
                development.normalized_content_hashes[0],
                test_history.normalized_content_hashes[1],
            )
        )
    )
    histories[test_index] = replace(
        test_history, normalized_content_hashes=leaked_content
    )
    with pytest.raises(ValidationError, match="content identity leaks"):
        replace(workload, histories=tuple(histories))

    histories[test_index] = replace(
        test_history, claim_family_ids=development.claim_family_ids
    )
    with pytest.raises(ValidationError, match="claim-family identity leaks"):
        replace(workload, histories=tuple(histories))


def test_metric_derivation_separates_admission_from_status_detection() -> None:
    workload = build_controlled_dynamic_workload_v1()
    history = workload.histories_for_split(WorkloadSplit.TEST)[0]
    event = history.events[0]
    missed_pair = event.deliberate_miss_pairs[0]
    claim_id = missed_pair.claim_id
    answer_id = event.registered_answer_ids[0]
    oracle = OracleEventResult(
        event_id=event.event_id,
        manifest_id="oracle",
        positive_pairs=(missed_pair,),
        affected_claim_statuses=(
            ClaimStatusResult(claim_id, ClaimStatus.SUPPORTED),
        ),
        affected_answer_statuses=(
            AnswerStatusResult(answer_id, AnswerStatus.VALID),
        ),
    )
    treatment = TreatmentEventResult(
        event_id=event.event_id,
        manifest_id="treatment",
        admitted_pairs=(missed_pair,),
        claim_post_statuses=(
            ClaimStatusResult(claim_id, ClaimStatus.UNSUPPORTED),
        ),
        answer_post_statuses=(
            AnswerStatusResult(answer_id, AnswerStatus.UNSUPPORTED),
        ),
    )
    record = derive_event_metric_record(
        run_id="run",
        provenance=_provenance(workload, WorkloadSplit.TEST, "policy"),
        history=history,
        event=event,
        oracle=oracle,
        treatment=treatment,
    )

    assert record.metric(MetricName.POSITIVE_PAIR_RECALL).value == 1.0
    assert record.metric(MetricName.POSITIVE_CLAIM_RECALL).value == 1.0
    assert record.metric(MetricName.STATUS_EFFECT_RECALL).value == 0.0
    assert record.metric(MetricName.ANSWER_EFFECT_RECALL).value == 0.0

    with pytest.raises(ValidationError, match="not oracle-positive"):
        derive_event_metric_record(
            run_id="run",
            provenance=_provenance(workload, WorkloadSplit.TEST, "policy"),
            history=history,
            event=event,
            oracle=replace(oracle, positive_pairs=()),
            treatment=treatment,
        )


def test_paired_report_is_canonical_complete_and_preserves_na_and_misses() -> None:
    workload = build_controlled_dynamic_workload_v1()
    split = WorkloadSplit.TEST
    oracle, first, second = _controlled_results(workload, split)
    config = BootstrapConfig(
        config_id="wave3-controlled-bootstrap",
        seed=314159,
        replicate_count=50,
        confidence_level=0.90,
    )
    kwargs = {
        "workload": workload,
        "split": split,
        "first_run_id": "run-first",
        "second_run_id": "run-second",
        "first_provenance": _provenance(workload, split, "policy-first"),
        "second_provenance": _provenance(workload, split, "policy-second"),
        "bootstrap_config": config,
    }
    report = build_paired_evaluation_report(
        **kwargs,
        oracle_results=oracle,
        first_results=first,
        second_results=second,
    )
    reordered = build_paired_evaluation_report(
        **kwargs,
        oracle_results=tuple(reversed(oracle)),
        first_results=tuple(reversed(first)),
        second_results=tuple(reversed(second)),
    )

    assert report == reordered
    assert report.to_canonical_json() == reordered.to_canonical_json()
    payload = json.loads(report.to_canonical_json())
    assert payload["report_manifest_hash"] == report.manifest_hash
    assert payload["workload"]["manifest_hash"] == workload.manifest_hash
    assert payload["first_run"]["provenance"]["manifest_hash"] == (
        report.first_run.provenance.manifest_hash
    )
    assert payload["second_run"]["provenance"]["manifest_hash"] == (
        report.second_run.provenance.manifest_hash
    )
    assert len(payload["first_run"]["events"]) == 6
    delete_events = [
        event
        for event in payload["first_run"]["events"]
        if event["event_type"] == "delete"
    ]
    assert len(delete_events) == 2
    for event in delete_events:
        pair_metric = next(
            metric
            for metric in event["metrics"]
            if metric["metric_name"] == "positive_pair_recall"
        )
        assert pair_metric["numerator"] == 0
        assert pair_metric["denominator"] == 0
        assert pair_metric["value"] is None

    deliberate = [
        diagnostic
        for diagnostic in payload["event_diagnostics"]
        if diagnostic["deliberate_miss_pairs"]
    ]
    assert len(deliberate) == 2
    assert all(item["first_missed_positive_pairs"] for item in deliberate)
    assert all(item["second_missed_positive_pairs"] == [] for item in deliberate)
    assert all(
        result["first_provenance_manifest_hash"]
        == report.first_run.provenance.manifest_hash
        for result in payload["bootstrap_results"]
    )


def test_report_rejects_missing_results_and_wrong_workload_provenance() -> None:
    workload = build_controlled_dynamic_workload_v1()
    split = WorkloadSplit.TEST
    oracle, first, second = _controlled_results(workload, split)
    config = BootstrapConfig("small", seed=1, replicate_count=5)
    base = {
        "workload": workload,
        "split": split,
        "first_run_id": "run-first",
        "second_run_id": "run-second",
        "first_provenance": _provenance(workload, split, "policy-first"),
        "second_provenance": _provenance(workload, split, "policy-second"),
        "oracle_results": oracle,
        "first_results": first,
        "second_results": second,
        "bootstrap_config": config,
    }
    with pytest.raises(ValidationError, match="selected workload events"):
        build_paired_evaluation_report(**(base | {"oracle_results": oracle[:-1]}))

    wrong = replace(
        base["first_provenance"],
        split_manifest_hash=_hash("wrong-split-manifest"),
    )
    with pytest.raises(ValidationError, match="split manifest"):
        build_paired_evaluation_report(**(base | {"first_provenance": wrong}))
