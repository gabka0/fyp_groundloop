"""Deterministic controlled M4.6 evaluation runner."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from groundloop.domain import AnswerState, ClaimState
from groundloop.errors import ValidationError
from groundloop.m4.contracts import (
    AdmissionChannel,
    PairJudgment,
    PairKey,
    stable_m4_digest,
)
from groundloop.m4.experiments.config import ControlledEvaluationConfig
from groundloop.m4.experiments.contracts import (
    ChannelCount,
    DeliberateMissRecord,
    EvaluationPolicy,
    ExhaustiveEventMeasurement,
    PolicyEvaluation,
    PolicyEventMeasurement,
    PolicyKind,
    RankedChannelCandidate,
    VerifierWorkRecord,
)
from groundloop.m4.experiments.fixture import (
    FixtureEvent,
    FixtureHistory,
    FrozenControlledFixture,
)
from groundloop.m4.oracles import (
    AnswerStatusResult,
    ClaimStatusResult,
    ControlledEventSpec,
    EvaluationProvenance,
    EventMetricRecord,
    MetricName,
    OracleEventResult,
    OracleKind,
    PairJudge,
    PolicyMetricSummary,
    TreatmentEventResult,
    align_paired_policy_records,
    derive_event_metric_record,
    paired_history_cluster_bootstrap,
    recompute_grounding_states,
    run_full_pair_audit,
    summarize_policy_metric,
)
from groundloop.m4.oracles.testing import DeterministicJudgmentTable


class _MeasuredJudge(PairJudge):
    """Count actual nonempty adapter invocations and attempted pair units."""

    def __init__(self, judgments: tuple[PairJudgment, ...]) -> None:
        self._delegate = DeterministicJudgmentTable(judgments)
        self._batch_calls = 0
        self._attempted = 0
        self._completed = 0

    def judge_pairs(self, pairs: tuple[PairKey, ...]) -> tuple[PairJudgment, ...]:
        if not pairs:
            return ()
        self._batch_calls += 1
        self._attempted += len(pairs)
        results = self._delegate.judge_pairs(pairs)
        self._completed += len(results)
        return results

    @property
    def work(self) -> VerifierWorkRecord:
        return VerifierWorkRecord(
            batch_call_count=self._batch_calls,
            attempted_pair_count=self._attempted,
            completed_pair_count=self._completed,
            failed_pair_count=self._attempted - self._completed,
        )


@dataclass(frozen=True, slots=True)
class _Selection:
    pairs: tuple[PairKey, ...]
    channel_counts: tuple[ChannelCount, ...]
    approximate_pair_count: int
    lineage_extra_count: int
    frontier_extra_count: int


@dataclass(frozen=True, slots=True)
class ControlledEvaluationResult:
    config: ControlledEvaluationConfig
    fixture: FrozenControlledFixture
    exhaustive_measurements: tuple[ExhaustiveEventMeasurement, ...]
    exhaustive_metric_records: tuple[EventMetricRecord, ...]
    policy_evaluations: tuple[PolicyEvaluation, ...]
    deliberate_misses: tuple[DeliberateMissRecord, ...]

    def __post_init__(self) -> None:
        expected_event_ids = {
            event.event_id
            for history in self.fixture.workload.histories_for_split(
                self.config.split
            )
            for event in history.events
        }
        exhaustive_work_ids = {
            item.event_id for item in self.exhaustive_measurements
        }
        exhaustive_metric_ids = {
            item.event_id for item in self.exhaustive_metric_records
        }
        if exhaustive_work_ids != expected_event_ids or (
            exhaustive_metric_ids != expected_event_ids
        ):
            raise ValidationError("exhaustive records do not cover the selected split")
        if len(self.exhaustive_measurements) != len(expected_event_ids) or len(
            self.exhaustive_metric_records
        ) != len(expected_event_ids):
            raise ValidationError("exhaustive records contain duplicate events")
        if self.policy_evaluations != tuple(
            sorted(self.policy_evaluations, key=lambda item: item.policy.policy_id)
        ):
            raise ValidationError("policy evaluations must use canonical order")
        if tuple(item.policy for item in self.policy_evaluations) != (
            self.config.policies
        ):
            raise ValidationError(
                "evaluation result does not cover configured policies"
            )
        if any(
            {item.metric_record.event_id for item in evaluation.event_measurements}
            != expected_event_ids
            for evaluation in self.policy_evaluations
        ):
            raise ValidationError("a policy run does not cover the selected split")
        if self.deliberate_misses != tuple(
            sorted(
                self.deliberate_misses,
                key=lambda item: (item.policy_id, item.history_id, item.event_id),
            )
        ):
            raise ValidationError("deliberate misses must use canonical order")
        expected_miss_keys = {
            (policy.policy_id, history.history_id, event.event_id)
            for policy in self.config.policies
            for history in self.fixture.workload.histories_for_split(
                self.config.split
            )
            for event in history.events
        }
        actual_miss_keys = {
            (item.policy_id, item.history_id, item.event_id)
            for item in self.deliberate_misses
        }
        if actual_miss_keys != expected_miss_keys or len(
            self.deliberate_misses
        ) != len(expected_miss_keys):
            raise ValidationError("deliberate miss records are incomplete")


def _provenance(
    *,
    config: ControlledEvaluationConfig,
    fixture: FrozenControlledFixture,
    treatment_policy_id: str,
    treatment_policy_hash: str,
) -> EvaluationProvenance:
    return EvaluationProvenance(
        schema_version="m4-evaluation-v1",
        dataset_version=fixture.workload.dataset_version,
        split_id=config.split.value,
        split_manifest_hash=fixture.workload.split_manifest_hash,
        seed_manifest_hash=fixture.workload.seed_manifest_hash,
        treatment_policy_id=treatment_policy_id,
        treatment_policy_hash=treatment_policy_hash,
        verifier_model_artifact_id=config.verifier_model_artifact_id,
        verifier_execution_spec_hash=config.verifier_execution_spec_hash,
        decision_policy_id=config.decision_policy_id,
        oracle_kind=OracleKind.EXHAUSTIVE_DELTA,
        baseline_id="Bw-to-Bx",
        oracle_policy_id=config.oracle_policy_id,
        oracle_policy_hash=config.oracle_policy_hash,
    )


def _fixture_maps(
    fixture: FrozenControlledFixture,
) -> tuple[dict[str, FixtureHistory], dict[str, FixtureEvent]]:
    return (
        {history.history_id: history for history in fixture.histories},
        {event.event_id: event for event in fixture.events},
    )


def _registry(
    history: FixtureHistory,
) -> tuple[dict[str, str], frozenset[str], dict[str, str]]:
    return (
        {claim.claim_id: claim.answer_version_id for claim in history.claims},
        frozenset(claim.claim_id for claim in history.claims if claim.required),
        dict(history.chunk_text_hashes),
    )


def _recompute(
    *,
    history: FixtureHistory,
    active_chunk_ids: set[str],
    judgments: dict[PairKey, PairJudgment],
) -> tuple[tuple[ClaimState, ...], tuple[AnswerState, ...]]:
    claim_to_answer, required, all_hashes = _registry(history)
    return recompute_grounding_states(
        claim_to_answer=claim_to_answer,
        required_claim_ids=required,
        chunk_text_hashes={
            chunk_id: all_hashes[chunk_id] for chunk_id in sorted(active_chunk_ids)
        },
        judgments=tuple(judgments[pair] for pair in sorted(judgments)),
    )


def _affected_results(
    *,
    before_claims: tuple[ClaimState, ...],
    after_claims: tuple[ClaimState, ...],
    before_answers: tuple[AnswerState, ...],
    after_answers: tuple[AnswerState, ...],
) -> tuple[tuple[ClaimStatusResult, ...], tuple[AnswerStatusResult, ...]]:
    before_claim_map = {state.claim_id: state for state in before_claims}
    before_answer_map = {state.answer_version_id: state for state in before_answers}
    claims = tuple(
        ClaimStatusResult(state.claim_id, state.status)
        for state in after_claims
        if before_claim_map[state.claim_id].status is not state.status
    )
    answers = tuple(
        AnswerStatusResult(state.answer_version_id, state.status)
        for state in after_answers
        if before_answer_map[state.answer_version_id].status is not state.status
    )
    return claims, answers


def _status_projection(
    *,
    actual_claims: tuple[ClaimState, ...],
    actual_answers: tuple[AnswerState, ...],
    oracle: OracleEventResult,
) -> tuple[tuple[ClaimStatusResult, ...], tuple[AnswerStatusResult, ...]]:
    claim_map = {state.claim_id: state.status for state in actual_claims}
    answer_map = {state.answer_version_id: state.status for state in actual_answers}
    return (
        tuple(
            ClaimStatusResult(item.claim_id, claim_map[item.claim_id])
            for item in oracle.affected_claim_statuses
        ),
        tuple(
            AnswerStatusResult(
                item.answer_version_id, answer_map[item.answer_version_id]
            )
            for item in oracle.affected_answer_statuses
        ),
    )


def _ranked_by_channel(
    candidates: tuple[RankedChannelCandidate, ...],
) -> dict[tuple[str, AdmissionChannel], tuple[RankedChannelCandidate, ...]]:
    groups: defaultdict[
        tuple[str, AdmissionChannel], list[RankedChannelCandidate]
    ] = defaultdict(list)
    for candidate in candidates:
        groups[(candidate.pair.chunk_version_id, candidate.channel)].append(candidate)
    return {
        key: tuple(sorted(values, key=lambda item: (item.rank, item.pair.claim_id)))
        for key, values in groups.items()
    }


def _approximate_channels(kind: PolicyKind) -> tuple[AdmissionChannel, ...]:
    if kind is PolicyKind.VECTOR_ONLY:
        return (AdmissionChannel.VECTOR,)
    if kind is PolicyKind.LEXICAL_ONLY:
        return (AdmissionChannel.LEXICAL,)
    return (AdmissionChannel.VECTOR, AdmissionChannel.LEXICAL)


def _select_pairs(
    *,
    policy: EvaluationPolicy,
    inserted_chunk_ids: tuple[str, ...],
    candidates: tuple[RankedChannelCandidate, ...],
) -> _Selection:
    inserted = set(inserted_chunk_ids)
    if any(candidate.pair.chunk_version_id not in inserted for candidate in candidates):
        raise ValidationError("candidate lies outside the inserted event domain")
    grouped = _ranked_by_channel(candidates)
    approximate: set[PairKey] = set()
    channels = _approximate_channels(policy.kind)
    for chunk_id in inserted_chunk_ids:
        positions = {channel: 0 for channel in channels}
        while len(
            {pair for pair in approximate if pair.chunk_version_id == chunk_id}
        ) < policy.approximate_budget_per_inserted_chunk:
            advanced = False
            for channel in channels:
                group = grouped.get((chunk_id, channel), ())
                position = positions[channel]
                if position >= len(group):
                    continue
                advanced = True
                positions[channel] += 1
                approximate.add(group[position].pair)
                if len(
                    {
                        pair
                        for pair in approximate
                        if pair.chunk_version_id == chunk_id
                    }
                ) == policy.approximate_budget_per_inserted_chunk:
                    break
            if not advanced:
                break

    selected = set(approximate)
    lineage_extras: set[PairKey] = set()
    if policy.kind in {
        PolicyKind.UNION_LINEAGE,
        PolicyKind.UNION_LINEAGE_FRONTIER,
    }:
        lineage = {
            candidate.pair
            for candidate in candidates
            if candidate.channel is AdmissionChannel.LINEAGE
        }
        lineage_extras = lineage - selected
        selected.update(lineage_extras)

    frontier_extras: set[PairKey] = set()
    if policy.kind is PolicyKind.UNION_LINEAGE_FRONTIER:
        for chunk_id in inserted_chunk_ids:
            frontier = grouped.get((chunk_id, AdmissionChannel.FRONTIER), ())
            additions = 0
            for candidate in frontier:
                if candidate.pair in selected:
                    continue
                frontier_extras.add(candidate.pair)
                selected.add(candidate.pair)
                additions += 1
                if additions == policy.frontier_budget_per_inserted_chunk:
                    break

    counts: defaultdict[AdmissionChannel, int] = defaultdict(int)
    for candidate in candidates:
        counts[candidate.channel] += 1
    return _Selection(
        pairs=tuple(sorted(selected)),
        channel_counts=tuple(
            ChannelCount(channel, count)
            for channel, count in sorted(counts.items(), key=lambda item: item[0])
        ),
        approximate_pair_count=len(approximate),
        lineage_extra_count=len(lineage_extras),
        frontier_extra_count=len(frontier_extras),
    )


def _apply_structure(
    *,
    event: ControlledEventSpec,
    active_chunks: set[str],
    current_judgments: dict[PairKey, PairJudgment],
) -> None:
    deactivated = set(event.deactivated_chunk_version_ids)
    active_chunks.difference_update(deactivated)
    active_chunks.update(event.inserted_chunk_version_ids)
    for pair in tuple(current_judgments):
        if pair.chunk_version_id in deactivated:
            del current_judgments[pair]


def _treatment_manifest(
    *,
    policy: EvaluationPolicy,
    event: ControlledEventSpec,
    judgments: tuple[PairJudgment, ...],
) -> str:
    return stable_m4_digest(
        "m4-controlled-treatment-event-v1",
        policy.manifest_hash,
        event.manifest_hash,
        *(
            part
            for judgment in judgments
            for part in (
                judgment.pair.claim_id,
                judgment.pair.chunk_version_id,
                judgment.input_hash,
                judgment.derived_label.value,
            )
        ),
    )


def _build_exhaustive(
    *,
    config: ControlledEvaluationConfig,
    fixture: FrozenControlledFixture,
) -> tuple[
    tuple[OracleEventResult, ...],
    tuple[EventMetricRecord, ...],
    tuple[ExhaustiveEventMeasurement, ...],
]:
    history_fixtures, event_fixtures = _fixture_maps(fixture)
    exhaustive_policy_id = "exhaustive-cartesian"
    exhaustive_policy_hash = stable_m4_digest(
        "m4-controlled-exhaustive-treatment-v1", config.oracle_policy_hash
    )
    provenance = _provenance(
        config=config,
        fixture=fixture,
        treatment_policy_id=exhaustive_policy_id,
        treatment_policy_hash=exhaustive_policy_hash,
    )
    oracle_results: list[OracleEventResult] = []
    records: list[EventMetricRecord] = []
    work_records: list[ExhaustiveEventMeasurement] = []
    for history in fixture.workload.histories_for_split(config.split):
        fixture_history = history_fixtures[history.history_id]
        active_chunks: set[str] = set()
        current: dict[PairKey, PairJudgment] = {}
        before_claims, before_answers = _recompute(
            history=fixture_history,
            active_chunk_ids=active_chunks,
            judgments=current,
        )
        for event in history.events:
            event_fixture = event_fixtures[event.event_id]
            _apply_structure(
                event=event,
                active_chunks=active_chunks,
                current_judgments=current,
            )
            judge = _MeasuredJudge(event_fixture.judgments)
            audit = run_full_pair_audit(
                event_id=event.event_id,
                registered_claim_ids=event.registered_claim_ids,
                inserted_active_chunk_ids=event.inserted_chunk_version_ids,
                judge=judge,
            )
            for judgment in audit.judgments:
                current[judgment.pair] = judgment
            after_claims, after_answers = _recompute(
                history=fixture_history,
                active_chunk_ids=active_chunks,
                judgments=current,
            )
            affected_claims, affected_answers = _affected_results(
                before_claims=before_claims,
                after_claims=after_claims,
                before_answers=before_answers,
                after_answers=after_answers,
            )
            oracle = OracleEventResult(
                event_id=event.event_id,
                manifest_id=audit.manifest_id,
                positive_pairs=audit.positive_pairs,
                affected_claim_statuses=affected_claims,
                affected_answer_statuses=affected_answers,
            )
            treatment = TreatmentEventResult(
                event_id=event.event_id,
                manifest_id=stable_m4_digest(
                    "m4-controlled-exhaustive-event-v1",
                    audit.manifest_id,
                    exhaustive_policy_hash,
                ),
                admitted_pairs=tuple(judgment.pair for judgment in audit.judgments),
                claim_post_statuses=affected_claims,
                answer_post_statuses=affected_answers,
            )
            records.append(
                derive_event_metric_record(
                    run_id="controlled-exhaustive-run-v1",
                    provenance=provenance,
                    history=history,
                    event=event,
                    oracle=oracle,
                    treatment=treatment,
                )
            )
            work_records.append(
                ExhaustiveEventMeasurement(
                    history_id=history.history_id,
                    event_id=event.event_id,
                    expected_cartesian_pair_count=audit.expected_pair_count,
                    work=judge.work,
                    audit_manifest_id=audit.manifest_id,
                )
            )
            oracle_results.append(oracle)
            before_claims, before_answers = after_claims, after_answers
    return tuple(oracle_results), tuple(records), tuple(work_records)


def _run_policy(
    *,
    config: ControlledEvaluationConfig,
    fixture: FrozenControlledFixture,
    policy: EvaluationPolicy,
    oracles: dict[str, OracleEventResult],
    exhaustive_records: tuple[EventMetricRecord, ...],
) -> tuple[PolicyEvaluation, tuple[DeliberateMissRecord, ...]]:
    history_fixtures, event_fixtures = _fixture_maps(fixture)
    provenance = _provenance(
        config=config,
        fixture=fixture,
        treatment_policy_id=policy.policy_id,
        treatment_policy_hash=policy.manifest_hash,
    )
    measurements: list[PolicyEventMeasurement] = []
    misses: list[DeliberateMissRecord] = []
    for history in fixture.workload.histories_for_split(config.split):
        fixture_history = history_fixtures[history.history_id]
        active_chunks: set[str] = set()
        current: dict[PairKey, PairJudgment] = {}
        for event in history.events:
            event_fixture = event_fixtures[event.event_id]
            _apply_structure(
                event=event,
                active_chunks=active_chunks,
                current_judgments=current,
            )
            selection = _select_pairs(
                policy=policy,
                inserted_chunk_ids=event.inserted_chunk_version_ids,
                candidates=event_fixture.candidates,
            )
            judge = _MeasuredJudge(event_fixture.judgments)
            selected_judgments = judge.judge_pairs(selection.pairs)
            for judgment in selected_judgments:
                current[judgment.pair] = judgment
            claim_states, answer_states = _recompute(
                history=fixture_history,
                active_chunk_ids=active_chunks,
                judgments=current,
            )
            oracle = oracles[event.event_id]
            claim_results, answer_results = _status_projection(
                actual_claims=claim_states,
                actual_answers=answer_states,
                oracle=oracle,
            )
            treatment_manifest = _treatment_manifest(
                policy=policy,
                event=event,
                judgments=selected_judgments,
            )
            treatment = TreatmentEventResult(
                event_id=event.event_id,
                manifest_id=treatment_manifest,
                admitted_pairs=selection.pairs,
                claim_post_statuses=claim_results,
                answer_post_statuses=answer_results,
            )
            metric_record = derive_event_metric_record(
                run_id=f"controlled-{policy.policy_id}-run-v1",
                provenance=provenance,
                history=history,
                event=event,
                oracle=oracle,
                treatment=treatment,
            )
            measurements.append(
                PolicyEventMeasurement(
                    policy=policy,
                    metric_record=metric_record,
                    admitted_pairs=selection.pairs,
                    channel_counts=selection.channel_counts,
                    inserted_chunk_count=len(event.inserted_chunk_version_ids),
                    approximate_selected_pair_count=(
                        selection.approximate_pair_count
                    ),
                    mandatory_lineage_extra_pair_count=(
                        selection.lineage_extra_count
                    ),
                    frontier_extra_pair_count=selection.frontier_extra_count,
                    verifier_work=judge.work,
                    treatment_manifest_hash=treatment_manifest,
                )
            )
            positives = set(oracle.positive_pairs)
            admitted = set(selection.pairs)
            designated = set(event.deliberate_miss_pairs)
            positive_designated = designated & positives
            misses.append(
                DeliberateMissRecord(
                    policy_id=policy.policy_id,
                    history_id=history.history_id,
                    event_id=event.event_id,
                    designated_pairs=tuple(sorted(designated)),
                    oracle_positive_designated_pairs=tuple(
                        sorted(positive_designated)
                    ),
                    missed_designated_pairs=tuple(
                        sorted(positive_designated - admitted)
                    ),
                    all_missed_positive_pairs=tuple(sorted(positives - admitted)),
                )
            )

    metric_records = tuple(item.metric_record for item in measurements)
    paired = align_paired_policy_records(metric_records, exhaustive_records)
    summaries: tuple[PolicyMetricSummary, ...] = tuple(
        summarize_policy_metric(metric_records, name) for name in sorted(MetricName)
    )
    bootstraps = tuple(
        paired_history_cluster_bootstrap(
            paired,
            metric_name=name,
            config=config.bootstrap,
        )
        for name in sorted(MetricName)
    )
    return (
        PolicyEvaluation(
            policy=policy,
            event_measurements=tuple(measurements),
            metric_summaries=summaries,
            versus_exhaustive_bootstrap=bootstraps,
        ),
        tuple(misses),
    )


def run_controlled_evaluation(
    *,
    config: ControlledEvaluationConfig,
    fixture: FrozenControlledFixture,
) -> ControlledEvaluationResult:
    """Run all frozen policies over the same exact table-driven history."""
    if config.workload_builder_id != "build_controlled_dynamic_workload_v1":
        raise ValidationError("configuration and fixture builder differ")
    oracles, exhaustive_records, exhaustive_work = _build_exhaustive(
        config=config,
        fixture=fixture,
    )
    oracle_map = {result.event_id: result for result in oracles}
    evaluations: list[PolicyEvaluation] = []
    misses: list[DeliberateMissRecord] = []
    for policy in config.policies:
        evaluation, policy_misses = _run_policy(
            config=config,
            fixture=fixture,
            policy=policy,
            oracles=oracle_map,
            exhaustive_records=exhaustive_records,
        )
        evaluations.append(evaluation)
        misses.extend(policy_misses)
    return ControlledEvaluationResult(
        config=config,
        fixture=fixture,
        exhaustive_measurements=tuple(
            sorted(exhaustive_work, key=lambda item: (item.history_id, item.event_id))
        ),
        exhaustive_metric_records=tuple(exhaustive_records),
        policy_evaluations=tuple(
            sorted(evaluations, key=lambda item: item.policy.policy_id)
        ),
        deliberate_misses=tuple(
            sorted(
                misses,
                key=lambda item: (item.policy_id, item.history_id, item.event_id),
            )
        ),
    )
