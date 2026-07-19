"""Controlled tests for provenance-safe M4 evaluation mechanics."""

from __future__ import annotations

from dataclasses import replace

import pytest

from groundloop.errors import ValidationError
from groundloop.m4.contracts import stable_m4_digest
from groundloop.m4.oracles import (
    FROZEN_HISTORY_BOOTSTRAP_V1,
    BootstrapConfig,
    EvaluationProvenance,
    EventMetricRecord,
    MetricCount,
    MetricName,
    MetricUnit,
    OracleKind,
    align_paired_policy_records,
    paired_history_cluster_bootstrap,
    summarize_policy_metric,
    validate_policy_run,
)


def _hash(label: str) -> str:
    return stable_m4_digest("evaluation-fixture", label)


def _provenance(
    policy_id: str,
    *,
    split_id: str = "test",
    verifier_id: str = "verifier-v1",
) -> EvaluationProvenance:
    return EvaluationProvenance(
        schema_version="m4-evaluation-v1",
        dataset_version="controlled-v1",
        split_id=split_id,
        split_manifest_hash=_hash(f"split-{split_id}"),
        seed_manifest_hash=_hash("seeds"),
        treatment_policy_id=policy_id,
        treatment_policy_hash=_hash(f"policy-{policy_id}"),
        verifier_model_artifact_id=verifier_id,
        verifier_execution_spec_hash=_hash(f"execution-{verifier_id}"),
        decision_policy_id="decision-v1",
        oracle_kind=OracleKind.EXHAUSTIVE_DELTA,
        baseline_id="Bw-to-Bx",
        oracle_policy_id="full-pair-v1",
        oracle_policy_hash=_hash("full-pair-v1"),
    )


def _metrics(
    *,
    positive_pair: tuple[int, int] = (0, 0),
    positive_claim: tuple[int, int] = (0, 0),
    status_effect: tuple[int, int] = (0, 0),
    answer_effect: tuple[int, int] = (0, 0),
) -> tuple[MetricCount, ...]:
    values = {
        MetricName.POSITIVE_PAIR_RECALL: positive_pair,
        MetricName.POSITIVE_CLAIM_RECALL: positive_claim,
        MetricName.STATUS_EFFECT_RECALL: status_effect,
        MetricName.ANSWER_EFFECT_RECALL: answer_effect,
    }
    return tuple(
        MetricCount(name, *values[name]) for name in sorted(MetricName)
    )


def _record(
    policy_id: str,
    *,
    history_id: str,
    event_id: str,
    event_index: int,
    positive_pair: tuple[int, int] = (0, 0),
    split_id: str = "test",
    verifier_id: str = "verifier-v1",
) -> EventMetricRecord:
    return EventMetricRecord(
        run_id=f"run-{policy_id}",
        provenance=_provenance(
            policy_id, split_id=split_id, verifier_id=verifier_id
        ),
        history_id=history_id,
        event_id=event_id,
        event_index=event_index,
        event_type="insert",
        corpus_snapshot_before_hash=_hash(f"before-{history_id}-{event_index}"),
        corpus_snapshot_after_hash=_hash(f"after-{history_id}-{event_index}"),
        treatment_manifest_id=f"treatment-{policy_id}-{event_id}",
        baseline_manifest_id=f"baseline-{event_id}",
        metrics=_metrics(positive_pair=positive_pair),
    )


def test_zero_denominator_is_na_and_remains_in_raw_summary() -> None:
    not_applicable = MetricCount(MetricName.POSITIVE_PAIR_RECALL, 0, 0)
    assert not_applicable.value is None
    assert not_applicable.unit is MetricUnit.PAIR

    records = (
        replace(
            _record(
                "policy-a",
                history_id="history",
                event_id="delete-only",
                event_index=0,
            ),
            failure_code="timeout-retained",
        ),
        _record(
            "policy-a",
            history_id="history",
            event_id="insert",
            event_index=1,
            positive_pair=(1, 2),
        ),
    )
    summary = summarize_policy_metric(
        records, MetricName.POSITIVE_PAIR_RECALL
    )

    assert summary.total_event_count == 2
    assert summary.eligible_event_count == 1
    assert summary.pooled_numerator == 1
    assert summary.pooled_denominator == 2
    assert summary.micro_value == 0.5
    assert summary.macro_event_value == 0.5


def test_metric_counts_and_complete_event_schema_fail_closed() -> None:
    with pytest.raises(ValidationError, match="nonnegative"):
        MetricCount(MetricName.POSITIVE_PAIR_RECALL, -1, 1)
    with pytest.raises(ValidationError, match="exceeds"):
        MetricCount(MetricName.POSITIVE_PAIR_RECALL, 2, 1)
    with pytest.raises(ValidationError, match="must be integers"):
        MetricCount(MetricName.POSITIVE_PAIR_RECALL, True, 1)

    record = _record(
        "policy-a", history_id="history", event_id="event", event_index=0
    )
    with pytest.raises(ValidationError, match="every frozen metric"):
        replace(record, metrics=(record.metrics[0],))


def test_homogeneous_run_rejects_mixed_policy_split_and_duplicate_events() -> None:
    first = _record(
        "policy-a", history_id="history", event_id="event", event_index=0
    )
    mixed_policy = replace(
        _record(
            "policy-b", history_id="history", event_id="other", event_index=1
        ),
        run_id=first.run_id,
    )
    with pytest.raises(ValidationError, match="evaluation provenance"):
        validate_policy_run((first, mixed_policy))

    mixed_split = replace(
        _record(
            "policy-a",
            history_id="history",
            event_id="other",
            event_index=1,
            split_id="development",
        ),
        run_id=first.run_id,
    )
    with pytest.raises(ValidationError, match="evaluation provenance"):
        validate_policy_run((first, mixed_split))

    mixed_verifier = replace(
        _record(
            "policy-a",
            history_id="history",
            event_id="other",
            event_index=1,
            verifier_id="verifier-v2",
        ),
        run_id=first.run_id,
    )
    with pytest.raises(ValidationError, match="evaluation provenance"):
        validate_policy_run((first, mixed_verifier))

    with pytest.raises(ValidationError, match="duplicate event ID"):
        validate_policy_run((first, first))

    assert first.provenance.manifest_hash != replace(
        first.provenance,
        split_id="development",
    ).manifest_hash


def test_paired_alignment_requires_same_events_and_experimental_identity() -> None:
    first = (
        _record(
            "policy-a", history_id="h", event_id="e1", event_index=0,
            positive_pair=(1, 1)
        ),
        _record(
            "policy-a", history_id="h", event_id="e2", event_index=1,
            positive_pair=(0, 1)
        ),
    )
    second = (
        _record(
            "policy-b", history_id="h", event_id="e1", event_index=0,
            positive_pair=(0, 1)
        ),
        _record(
            "policy-b", history_id="h", event_id="e2", event_index=1,
            positive_pair=(0, 1)
        ),
    )
    paired = align_paired_policy_records(tuple(reversed(first)), second)
    assert tuple(pair.first.event_id for pair in paired.event_pairs) == ("e1", "e2")

    with pytest.raises(ValidationError, match="missing event IDs"):
        align_paired_policy_records(first, second[:1])

    mismatched_event = (replace(second[0], event_type="replacement"), second[1])
    with pytest.raises(ValidationError, match="event identity mismatch"):
        align_paired_policy_records(first, mismatched_event)

    mismatched_split = tuple(
        _record(
            "policy-b",
            history_id="h",
            event_id=record.event_id,
            event_index=record.event_index,
            positive_pair=(0, 1),
            split_id="development",
        )
        for record in first
    )
    with pytest.raises(ValidationError, match="experimental identities"):
        align_paired_policy_records(first, mismatched_split)


def test_paired_alignment_rejects_denominator_and_identity_collisions() -> None:
    first = _record(
        "policy-a",
        history_id="history",
        event_id="event",
        event_index=0,
        positive_pair=(1, 1),
    )
    second = _record(
        "policy-b",
        history_id="history",
        event_id="event",
        event_index=0,
        positive_pair=(1, 2),
    )
    with pytest.raises(ValidationError, match="denominator mismatch"):
        align_paired_policy_records((first,), (second,))

    collision_provenance = replace(
        second.provenance,
        treatment_policy_id=first.provenance.treatment_policy_id,
    )
    collision = replace(second, provenance=collision_provenance)
    with pytest.raises(ValidationError, match="conflicting hashes"):
        align_paired_policy_records((first,), (collision,))

    alias_provenance = replace(
        second.provenance,
        treatment_policy_hash=first.provenance.treatment_policy_hash,
    )
    alias = replace(second, provenance=alias_provenance)
    with pytest.raises(ValidationError, match="distinct policy hashes"):
        align_paired_policy_records((first,), (alias,))


def test_history_cluster_bootstrap_is_deterministic_and_cluster_safe() -> None:
    first = (
        _record(
            "policy-a", history_id="h1", event_id="h1-e0", event_index=0,
            positive_pair=(1, 1)
        ),
        _record(
            "policy-a", history_id="h2", event_id="h2-e0", event_index=0,
            positive_pair=(0, 1)
        ),
    )
    second = (
        _record(
            "policy-b", history_id="h1", event_id="h1-e0", event_index=0,
            positive_pair=(0, 1)
        ),
        _record(
            "policy-b", history_id="h2", event_id="h2-e0", event_index=0,
            positive_pair=(0, 1)
        ),
    )
    config = BootstrapConfig(
        config_id="controlled-bootstrap-v1",
        seed=20260719,
        replicate_count=200,
        confidence_level=0.90,
    )
    paired = align_paired_policy_records(first, second)
    original = paired_history_cluster_bootstrap(
        paired, metric_name=MetricName.POSITIVE_PAIR_RECALL, config=config
    )
    repeated = paired_history_cluster_bootstrap(
        paired, metric_name=MetricName.POSITIVE_PAIR_RECALL, config=config
    )

    assert original == repeated
    assert original.total_cluster_ids == ("h1", "h2")
    assert original.eligible_cluster_count == 2
    assert original.observed_difference == 0.5

    first_duplicate = _record(
        "policy-a", history_id="h1", event_id="h1-e1", event_index=1,
        positive_pair=(1, 1)
    )
    second_duplicate = _record(
        "policy-b", history_id="h1", event_id="h1-e1", event_index=1,
        positive_pair=(0, 1)
    )
    duplicated = paired_history_cluster_bootstrap(
        align_paired_policy_records(
            (*first, first_duplicate), (*second, second_duplicate)
        ),
        metric_name=MetricName.POSITIVE_PAIR_RECALL,
        config=config,
    )

    assert duplicated.total_cluster_count == 2
    assert duplicated.eligible_cluster_count == 2
    assert duplicated.observed_difference == original.observed_difference
    assert duplicated.replicate_differences == original.replicate_differences
    assert duplicated.confidence_lower == original.confidence_lower
    assert duplicated.confidence_upper == original.confidence_upper


def test_all_na_histories_produce_descriptive_na_not_fake_interval() -> None:
    first = tuple(
        _record("policy-a", history_id=history, event_id=f"{history}-e", event_index=0)
        for history in ("h1", "h2")
    )
    second = tuple(
        _record("policy-b", history_id=history, event_id=f"{history}-e", event_index=0)
        for history in ("h1", "h2")
    )
    result = paired_history_cluster_bootstrap(
        align_paired_policy_records(first, second),
        metric_name=MetricName.POSITIVE_PAIR_RECALL,
        config=BootstrapConfig("na-bootstrap", seed=7, replicate_count=20),
    )

    assert result.total_cluster_count == 2
    assert result.eligible_cluster_count == 0
    assert result.observed_difference is None
    assert result.confidence_lower is None
    assert result.confidence_upper is None
    assert result.replicate_differences == ()


def test_single_history_is_descriptive_and_frozen_config_is_manifested() -> None:
    first = _record(
        "policy-a", history_id="h1", event_id="event", event_index=0,
        positive_pair=(1, 1)
    )
    second = _record(
        "policy-b", history_id="h1", event_id="event", event_index=0,
        positive_pair=(0, 1)
    )
    result = paired_history_cluster_bootstrap(
        align_paired_policy_records((first,), (second,)),
        metric_name=MetricName.POSITIVE_PAIR_RECALL,
        config=FROZEN_HISTORY_BOOTSTRAP_V1,
    )

    assert result.total_cluster_count == 1
    assert result.eligible_cluster_count == 1
    assert result.observed_difference == 1.0
    assert result.confidence_lower is None
    assert result.confidence_upper is None
    assert result.replicate_differences == ()
    assert result.config_manifest_hash == FROZEN_HISTORY_BOOTSTRAP_V1.manifest_hash
