"""Acceptance tests for the frozen-history M4.9 evaluation harness."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest

from experiments.m4_selective_study.run_controlled_study import (
    _histories,
    _oracle_material,
    build_controlled_study,
)
from groundloop.errors import ValidationError
from groundloop.m4.empirical_eval import (
    AblationKind,
    EmpiricalMetric,
    FrozenOracleEvent,
    HistoryAssignment,
    evaluate_frozen_history,
    write_empirical_bundle,
)
from groundloop.m4.event_audit import (
    EventAuditDisposition,
    PersistedEventAudit,
)


@pytest.fixture(scope="module")
def controlled_spec():
    return build_controlled_study()


@pytest.fixture(scope="module")
def controlled_report(controlled_spec):
    return evaluate_frozen_history(controlled_spec)


def _policy_id(controlled_spec, kind: AblationKind) -> str:
    return next(
        item.policy_id
        for item in controlled_spec.policies
        if item.kind is kind
    )


def _canonical_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _persisted_fixture():
    history = next(
        item for item in _histories() if item.assignment.history_id == "test-h1"
    )
    material = _oracle_material(history)[0]
    oracle = material.event
    refresh = material.refresh
    result = {
        "full_pair_audit": {
            "manifest_id": oracle.event_audit_manifest_id,
            "positive_pairs": [
                {
                    "claim_id": pair.claim_id,
                    "chunk_version_id": pair.chunk_version_id,
                }
                for pair in oracle.positive_pairs
            ],
        },
        "exhaustive_additive": {
            "claim_states": [
                {"claim_id": item.claim_id, "status": item.status.value}
                for item in refresh.claim_states
            ],
            "answer_states": [
                {
                    "answer_version_id": item.answer_version_id,
                    "status": item.status.value,
                }
                for item in refresh.answer_states
            ],
            "affected_from_working": {
                "status_claim_ids": [
                    item.object_id for item in oracle.claim_status_effects
                ],
                "answer_status_ids": [
                    item.object_id for item in oracle.answer_status_effects
                ],
            },
        },
        "snapshot_refresh": {
            "manifest_id": refresh.manifest_id,
            "retrieved_pairs": [
                {
                    "claim_id": pair.claim_id,
                    "chunk_version_id": pair.chunk_version_id,
                }
                for pair in refresh.retrieved_pairs
            ],
        },
    }
    result_hash = _canonical_hash(result)
    manifest = {
        "schema_version": "m4-event-audit-v1",
        "event": {"event_id": oracle.event_id, "epoch_id": 17},
        "result": result,
        "result_hash": result_hash,
    }
    manifest["manifest_hash"] = _canonical_hash(manifest)
    audit = PersistedEventAudit(
        evaluation_run_id="evaluation-run",
        event_id=oracle.event_id,
        epoch_id=17,
        disposition=EventAuditDisposition.CREATED,
        treatment_manifest_id="treatment",
        baseline_manifest_id="baseline",
        audit_manifest_id=oracle.event_audit_manifest_id,
        refresh_manifest_id=refresh.manifest_id,
        input_hash="a" * 64,
        result_hash=result_hash,
        expected_pair_count=3,
        positive_pair_count=len(oracle.positive_pairs),
        missed_positive_pairs=(),
        detected_deliberate_miss_pairs=(),
        selective_exhaustive_status_claim_ids=(),
        selective_exhaustive_answer_status_ids=(),
        selective_refresh_status_claim_ids=(),
        selective_refresh_answer_status_ids=(),
    )
    return oracle, refresh, audit, manifest


def test_all_ablations_share_identical_events_and_exhaustive_is_exact(
    controlled_spec,
    controlled_report,
) -> None:
    expected_event_ids = {item.event_id for item in controlled_spec.oracle_events}
    assert len(expected_event_ids) == 12
    for policy in controlled_spec.policies:
        actual = {
            item.event_id
            for item in controlled_report.events
            if item.policy_id == policy.policy_id
        }
        assert actual == expected_event_ids

    exhaustive_id = _policy_id(controlled_spec, AblationKind.EXHAUSTIVE_REFRESH)
    exhaustive = [
        item
        for item in controlled_report.metric_summaries
        if item.policy_id == exhaustive_id
    ]
    assert {item.metric for item in exhaustive} == set(EmpiricalMetric)
    assert all(item.micro_value == 1.0 for item in exhaustive)
    assert all(item.eligible_history_cluster_count == 3 for item in exhaustive)
    assert all(item.confidence_lower == 1.0 for item in exhaustive)
    assert all(item.confidence_upper == 1.0 for item in exhaustive)


def test_misses_answer_effects_timeouts_and_missing_measurements_are_explicit(
    controlled_spec,
    controlled_report,
) -> None:
    vector_id = _policy_id(controlled_spec, AblationKind.VECTOR_ONLY)
    vector = [item for item in controlled_report.events if item.policy_id == vector_id]
    assert any(item.missed_positive_pairs for item in vector)
    assert any(item.missed_answer_effect_ids for item in vector)
    timed_out = [item for item in vector if item.work.timeout_pair_count]
    assert len(timed_out) == 1
    assert timed_out[0].failure_code == "controlled_timeout_injection"

    work = next(
        item for item in controlled_report.work_summaries if item.policy_id == vector_id
    )
    assert work.timeout_pairs == 1
    assert work.failed_event_count == 1
    assert work.input_tokens is None
    assert work.input_token_observation_count == 0
    assert work.end_to_end_latency_ms is None
    assert work.end_to_end_latency_observation_count == 0

    delete_events = [item for item in vector if item.event_type == "delete"]
    assert delete_events
    assert all(
        next(
            metric
            for metric in item.metrics
            if metric.metric is EmpiricalMetric.POSITIVE_PAIR_IMPACT_RECALL
        ).value
        is None
        for item in delete_events
    )


def test_split_leakage_and_missing_paired_event_fail_closed(controlled_spec) -> None:
    histories = list(controlled_spec.histories)
    development = next(item for item in histories if item.split_id == "development")
    test_index = next(
        index for index, item in enumerate(histories) if item.split_id == "test"
    )
    test_history = histories[test_index]
    histories[test_index] = replace(
        test_history,
        component_ids=tuple(
            sorted((*test_history.component_ids, development.component_ids[0]))
        ),
    )
    with pytest.raises(ValidationError, match="leaks across splits"):
        replace(controlled_spec, histories=tuple(histories))

    one_policy = controlled_spec.policies[0].policy_id
    removed = False
    treatments = []
    for row in controlled_spec.treatments:
        if row.policy_id == one_policy and not removed:
            removed = True
            continue
        treatments.append(row)
    with pytest.raises(ValidationError, match="identical event IDs"):
        replace(controlled_spec, treatments=tuple(treatments))


def test_connected_same_split_histories_form_one_bootstrap_cluster(
    controlled_spec,
) -> None:
    histories = list(controlled_spec.histories)
    test_indexes = [
        index for index, item in enumerate(histories) if item.split_id == "test"
    ]
    first = histories[test_indexes[0]]
    second = histories[test_indexes[1]]
    shared = "same-split-connected-component"
    histories[test_indexes[0]] = HistoryAssignment(
        first.history_id,
        first.split_id,
        tuple(sorted((*first.component_ids, shared))),
    )
    histories[test_indexes[1]] = HistoryAssignment(
        second.history_id,
        second.split_id,
        tuple(sorted((*second.component_ids, shared))),
    )
    report = evaluate_frozen_history(
        replace(
            controlled_spec,
            histories=tuple(histories),
            bootstrap_replicates=50,
        )
    )
    assert all(
        item.eligible_history_cluster_count == 2
        for item in report.metric_summaries
    )


def test_bundle_is_byte_deterministic_and_hash_complete(
    controlled_report,
    tmp_path,
) -> None:
    first = write_empirical_bundle(controlled_report, tmp_path / "first")
    second = write_empirical_bundle(controlled_report, tmp_path / "second")
    assert first.study_manifest_hash == second.study_manifest_hash
    assert first.report_hash == second.report_hash
    assert first.bundle_manifest_hash == second.bundle_manifest_hash

    manifest = json.loads(first.bundle_manifest_path.read_text(encoding="utf-8"))
    assert manifest["study_manifest_hash"] == first.study_manifest_hash
    assert manifest["report_hash"] == first.report_hash
    for item in manifest["files"]:
        content = (first.output_directory / item["path"]).read_bytes()
        assert hashlib.sha256(content).hexdigest() == item["sha256"]
        assert content == (second.output_directory / item["path"]).read_bytes()
    study = json.loads(first.study_manifest_path.read_text(encoding="utf-8"))
    boundaries = study["scientific_boundaries"]
    assert boundaries["structured_oracle_correctness"].startswith("precondition")
    assert boundaries["reported_recall_scope"].startswith("empirical_ai_quality")
    assert boundaries["objective_truth_claim"] is False


def test_exhaustive_treatment_must_equal_snapshot_refresh(controlled_spec) -> None:
    exhaustive_id = _policy_id(controlled_spec, AblationKind.EXHAUSTIVE_REFRESH)
    treatments = list(controlled_spec.treatments)
    index = next(
        index
        for index, item in enumerate(treatments)
        if item.policy_id == exhaustive_id and item.admitted_pairs
    )
    row = treatments[index]
    shortened = row.admitted_pairs[:-1]
    treatments[index] = replace(
        row,
        admitted_pairs=shortened,
        work=replace(
            row.work,
            attempted_pair_count=len(shortened),
            completed_pair_count=len(shortened),
        ),
    )
    with pytest.raises(ValidationError, match="SnapshotRefresh_k"):
        replace(controlled_spec, treatments=tuple(treatments))


def test_persisted_factory_rejects_same_count_positive_pair_substitution() -> None:
    expected, refresh, audit, manifest = _persisted_fixture()
    frozen = FrozenOracleEvent.from_persisted_manifest(
        audit=audit,
        refresh=refresh,
        persisted_manifest=manifest,
        history_id=expected.history_id,
        event_index=expected.event_index,
        event_type=expected.event_type,
        event_manifest_hash=expected.event_manifest_hash,
    )
    assert frozen.positive_pairs == expected.positive_pairs
    assert frozen.claim_status_effects == expected.claim_status_effects
    assert frozen.answer_status_effects == expected.answer_status_effects

    tampered = json.loads(json.dumps(manifest))
    positive = tampered["result"]["full_pair_audit"]["positive_pairs"]
    positive[0]["claim_id"] = "same-count-wrong-claim"
    tampered["result_hash"] = _canonical_hash(tampered["result"])
    del tampered["manifest_hash"]
    tampered["manifest_hash"] = _canonical_hash(tampered)
    with pytest.raises(ValidationError, match="result hash mismatch"):
        FrozenOracleEvent.from_persisted_manifest(
            audit=audit,
            refresh=refresh,
            persisted_manifest=tampered,
            history_id=expected.history_id,
            event_index=expected.event_index,
            event_type=expected.event_type,
            event_manifest_hash=expected.event_manifest_hash,
        )

    with pytest.raises(ValidationError, match="projection hash"):
        replace(
            expected,
            positive_pairs=(
                type(expected.positive_pairs[0])(
                    "same-count-wrong-claim",
                    expected.positive_pairs[0].chunk_version_id,
                ),
            ),
        )
