from __future__ import annotations

from groundloop.m5.evaluation.baselines import BaselineId, run_baselines
from groundloop.m5.evaluation.histories import build_authored_controlled_histories
from groundloop.m5.evaluation.metrics import (
    BOOTSTRAP_RESAMPLES,
    BOOTSTRAP_SEED,
    build_metric_report,
)


def test_raw_numerators_denominators_and_undefined_rates_are_preserved() -> None:
    run = run_baselines(build_authored_controlled_histories())
    report = build_metric_report(run, resamples=100)
    summaries = {item.baseline_id: item for item in report.summaries}
    target = summaries[BaselineId.GROUNDLOOP_HALL_SDR]

    assert target.false_invalidation.numerator == 1
    assert target.false_invalidation.denominator > 0
    assert target.false_retention.denominator > 0
    assert target.exact_structured_agreement.numerator == (
        target.exact_structured_agreement.denominator
    )
    assert target.work.model_calls == 0
    assert target.work.model_tokens == 0


def test_clustered_bootstrap_is_deterministic_and_uses_frozen_defaults() -> None:
    run = run_baselines(build_authored_controlled_histories())
    first = build_metric_report(run, resamples=250)
    second = build_metric_report(run, resamples=250)

    assert first == second
    assert first.bootstrap_seed == BOOTSTRAP_SEED == 20260802
    assert BOOTSTRAP_RESAMPLES == 10_000


def test_paired_subset_draws_have_fixed_eligible_cluster_size() -> None:
    run = run_baselines(build_authored_controlled_histories())
    report = build_metric_report(run, resamples=400)
    direct_pairs = tuple(
        item
        for item in report.paired_differences
        if item.comparator is BaselineId.DIRECT_WITNESS
    )

    assert direct_pairs
    assert all(item.interval.cluster_count == 1 for item in direct_pairs)
    assert all(item.interval.requested_resamples == 400 for item in direct_pairs)
    assert all(item.interval.defined_resamples == 400 for item in direct_pairs)


def test_full_status_agreement_reports_conflict_and_refute_states_as_exact() -> None:
    run = run_baselines(build_authored_controlled_histories())
    report = build_metric_report(run, resamples=100)
    summaries = {item.baseline_id: item for item in report.summaries}

    for baseline in (
        BaselineId.GROUNDLOOP_HALL_SDR,
        BaselineId.AFFECTED_GROUP_FULL_MATCHING,
        BaselineId.ALL_GROUP_FULL_RECOMPUTATION,
    ):
        agreement = summaries[baseline].exact_structured_agreement
        assert agreement.numerator == agreement.denominator
