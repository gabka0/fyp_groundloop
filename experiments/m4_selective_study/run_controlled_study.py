#!/usr/bin/env python3
"""Run the deterministic M4.9 controlled selective-maintenance study.

This is a synthetic table-judgment experiment.  It executes the real
full-pair and SnapshotRefresh_k oracle code, but makes no real-model latency,
token, or quality claim.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from groundloop.domain import AnswerState, ClaimState, VerificationLabel
from groundloop.m4.contracts import (
    JudgmentSourceKind,
    PairJudgment,
    PairKey,
    SnapshotRefreshResult,
    stable_m4_digest,
)
from groundloop.m4.empirical_eval import (
    AblationKind,
    EmpiricalStudySpec,
    FrozenOracleEvent,
    HistoryAssignment,
    ObjectStatus,
    PolicySpec,
    TreatmentEvent,
    VerifierMeasurement,
    evaluate_frozen_history,
    write_empirical_bundle,
)
from groundloop.m4.oracles.full_pair import run_full_pair_audit
from groundloop.m4.oracles.grounding import recompute_grounding_states
from groundloop.m4.oracles.refresh import (
    RefreshChunk,
    RefreshClaim,
    run_snapshot_refresh,
)
from groundloop.m4.oracles.testing import DeterministicJudgmentTable


@dataclass(frozen=True, slots=True)
class _Event:
    event_id: str
    event_index: int
    event_type: str
    inserted: tuple[str, ...]
    deactivated: tuple[str, ...]
    channels: tuple[tuple[str, tuple[PairKey, ...]], ...]

    def channel(self, name: str) -> tuple[PairKey, ...]:
        return dict(self.channels).get(name, ())


@dataclass(frozen=True, slots=True)
class _History:
    assignment: HistoryAssignment
    claims: tuple[RefreshClaim, ...]
    chunks: tuple[RefreshChunk, ...]
    judgments: tuple[PairJudgment, ...]
    events: tuple[_Event, ...]


@dataclass(frozen=True, slots=True)
class _OracleMaterial:
    event: FrozenOracleEvent
    refresh: SnapshotRefreshResult


def _scores(label: VerificationLabel) -> tuple[float, float, float]:
    return {
        VerificationLabel.SUPPORT: (0.92, 0.03, 0.05),
        VerificationLabel.REFUTE: (0.03, 0.92, 0.05),
        VerificationLabel.NEUTRAL: (0.04, 0.04, 0.92),
    }[label]


def _judgment(pair: PairKey, label: VerificationLabel, split_id: str) -> PairJudgment:
    support, refute, neutral = _scores(label)
    return PairJudgment(
        pair=pair,
        source_kind=JudgmentSourceKind.MODEL,
        source_artifact_id="controlled-table-verifier-no-model-v1",
        decision_policy_or_guideline_id="controlled-decision-v1",
        derived_label=label,
        input_hash=stable_m4_digest(
            "m4-9-controlled-input-v1", pair.claim_id, pair.chunk_version_id
        ),
        split_id=split_id,
        support_score=support,
        refute_score=refute,
        neutral_score=neutral,
    )


def _chunks(prefix: str) -> tuple[RefreshChunk, ...]:
    vectors = ((1.0, 0.0), (0.0, 1.0), (-1.0, 0.0))
    return tuple(
        RefreshChunk(
            chunk_version_id=f"{prefix}-chunk-v{index}",
            text_hash=stable_m4_digest("m4-9-controlled-text", prefix, str(index)),
            vector=vector,
        )
        for index, vector in enumerate(vectors, start=1)
    )


def _history(split_id: str, ordinal: int) -> _History:
    prefix = f"{split_id}-h{ordinal}"
    answer = f"{prefix}-answer"
    core = f"{prefix}-claim-core"
    auxiliary = f"{prefix}-claim-auxiliary"
    decoy = f"{prefix}-claim-decoy"
    claims = tuple(
        sorted(
            (
                RefreshClaim(auxiliary, answer, False, (0.0, 1.0)),
                RefreshClaim(core, answer, True, (1.0, 0.0)),
                RefreshClaim(decoy, answer, False, (-1.0, 0.0)),
            ),
            key=lambda item: item.claim_id,
        )
    )
    chunks = _chunks(prefix)
    judgments: list[PairJudgment] = []
    for chunk_index, chunk in enumerate(chunks, start=1):
        for claim in claims:
            label = VerificationLabel.NEUTRAL
            if claim.claim_id == core:
                label = {
                    1: VerificationLabel.SUPPORT,
                    2: VerificationLabel.REFUTE,
                    3: VerificationLabel.SUPPORT,
                }[chunk_index]
            elif claim.claim_id == auxiliary and chunk_index == 2:
                label = VerificationLabel.SUPPORT
            judgments.append(
                _judgment(
                    PairKey(claim.claim_id, chunk.chunk_version_id),
                    label,
                    split_id,
                )
            )

    chunk1, chunk2, chunk3 = (item.chunk_version_id for item in chunks)
    c1 = {
        "vector": (PairKey(decoy, chunk1),),
        "lexical": (PairKey(core, chunk1),),
        "lineage": (),
        "frontier": (PairKey(auxiliary, chunk1),),
        "fresh": (PairKey(core, chunk1),),
    }
    c2 = {
        "vector": (PairKey(core if ordinal % 3 == 0 else decoy, chunk2),),
        "lexical": (PairKey(core if ordinal % 3 == 1 else decoy, chunk2),),
        "lineage": (PairKey(core, chunk2),) if ordinal % 3 == 2 else (),
        "frontier": (PairKey(auxiliary, chunk2),),
        "fresh": (PairKey(core, chunk2),),
    }
    c3 = {
        "vector": (PairKey(decoy, chunk3),),
        "lexical": (PairKey(auxiliary, chunk3),),
        "lineage": (PairKey(core, chunk3),) if ordinal % 3 == 1 else (),
        "frontier": (PairKey(core, chunk3),) if ordinal % 3 == 0 else (),
        "fresh": (PairKey(core, chunk3),),
    }
    events = (
        _Event(
            f"{prefix}-insert-support",
            0,
            "insert",
            (chunk1,),
            (),
            tuple(sorted(c1.items())),
        ),
        _Event(
            f"{prefix}-replace-refute",
            1,
            "replace",
            (chunk2,),
            (chunk1,),
            tuple(sorted(c2.items())),
        ),
        _Event(
            f"{prefix}-insert-alternative-support",
            2,
            "insert",
            (chunk3,),
            (),
            tuple(sorted(c3.items())),
        ),
        _Event(
            f"{prefix}-delete-refutation",
            3,
            "delete",
            (),
            (chunk2,),
            (),
        ),
    )
    return _History(
        assignment=HistoryAssignment(
            history_id=prefix,
            split_id=split_id,
            component_ids=tuple(
                sorted(
                    (
                        f"{prefix}-document-lineage",
                        f"{prefix}-claim-family",
                        stable_m4_digest("m4-9-content-component", prefix),
                    )
                )
            ),
        ),
        claims=claims,
        chunks=chunks,
        judgments=tuple(sorted(judgments, key=lambda item: item.pair)),
        events=events,
    )


def _histories() -> tuple[_History, ...]:
    return tuple(
        sorted(
            (
                *(_history("development", ordinal) for ordinal in range(1, 4)),
                *(_history("test", ordinal) for ordinal in range(1, 4)),
            ),
            key=lambda item: item.assignment.history_id,
        )
    )


def _recompute(
    history: _History,
    active: set[str],
    current: dict[PairKey, PairJudgment],
) -> tuple[tuple[ClaimState, ...], tuple[AnswerState, ...]]:
    return recompute_grounding_states(
        claim_to_answer={
            claim.claim_id: claim.answer_version_id for claim in history.claims
        },
        required_claim_ids=frozenset(
            claim.claim_id for claim in history.claims if claim.required
        ),
        chunk_text_hashes={
            chunk.chunk_version_id: chunk.text_hash
            for chunk in history.chunks
            if chunk.chunk_version_id in active
        },
        judgments=tuple(current[pair] for pair in sorted(current)),
    )


def _apply_structure(
    event: _Event,
    active: set[str],
    current: dict[PairKey, PairJudgment],
) -> None:
    removed = set(event.deactivated)
    active.difference_update(removed)
    active.update(event.inserted)
    for pair in tuple(current):
        if pair.chunk_version_id in removed:
            del current[pair]


def _status_effects(
    before_claims: tuple[ClaimState, ...],
    after_claims: tuple[ClaimState, ...],
    before_answers: tuple[AnswerState, ...],
    after_answers: tuple[AnswerState, ...],
) -> tuple[tuple[ObjectStatus, ...], tuple[ObjectStatus, ...]]:
    old_claims = {item.claim_id: item.status.value for item in before_claims}
    old_answers = {
        item.answer_version_id: item.status.value for item in before_answers
    }
    return (
        tuple(
            ObjectStatus(item.claim_id, item.status.value)
            for item in after_claims
            if old_claims[item.claim_id] != item.status.value
        ),
        tuple(
            ObjectStatus(item.answer_version_id, item.status.value)
            for item in after_answers
            if old_answers[item.answer_version_id] != item.status.value
        ),
    )


def _oracle_material(history: _History) -> tuple[_OracleMaterial, ...]:
    judge = DeterministicJudgmentTable(history.judgments)
    active: set[str] = set()
    current: dict[PairKey, PairJudgment] = {}
    before_claims, before_answers = _recompute(history, active, current)
    chunks = {item.chunk_version_id: item for item in history.chunks}
    result: list[_OracleMaterial] = []
    for event in history.events:
        _apply_structure(event, active, current)
        audit = run_full_pair_audit(
            event_id=event.event_id,
            registered_claim_ids=tuple(item.claim_id for item in history.claims),
            inserted_active_chunk_ids=event.inserted,
            judge=judge,
        )
        for judgment in audit.judgments:
            current[judgment.pair] = judgment
        after_claims, after_answers = _recompute(history, active, current)
        claim_effects, answer_effects = _status_effects(
            before_claims, after_claims, before_answers, after_answers
        )
        snapshot_hash = stable_m4_digest(
            "m4-9-controlled-active-snapshot",
            history.assignment.history_id,
            *sorted(active),
        )
        refresh = run_snapshot_refresh(
            corpus_snapshot_hash=snapshot_hash,
            refresh_policy_id="controlled-exhaustive-refresh-all-active-v1",
            depth_k=max(1, len(active)),
            claims=history.claims,
            active_chunks=tuple(chunks[item] for item in sorted(active)),
            judge=judge,
        )
        if (refresh.claim_states, refresh.answer_states) != (
            after_claims,
            after_answers,
        ):
            raise RuntimeError("controlled exhaustive delta and refresh differ")
        frozen = FrozenOracleEvent(
            history_id=history.assignment.history_id,
            event_id=event.event_id,
            event_index=event.event_index,
            event_type=event.event_type,
            event_manifest_hash=stable_m4_digest(
                "m4-9-controlled-event",
                event.event_id,
                *event.inserted,
                "removed",
                *event.deactivated,
            ),
            event_audit_manifest_id=audit.manifest_id,
            event_audit_result_hash=stable_m4_digest(
                "m4-9-controlled-event-audit-result-v1",
                audit.manifest_id,
                refresh.manifest_id,
                *(
                    f"{pair.claim_id}:{pair.chunk_version_id}"
                    for pair in audit.positive_pairs
                ),
            ),
            snapshot_refresh_manifest_id=refresh.manifest_id,
            positive_pairs=audit.positive_pairs,
            claim_status_effects=claim_effects,
            answer_status_effects=answer_effects,
            snapshot_refresh_pairs=refresh.retrieved_pairs,
        )
        result.append(_OracleMaterial(frozen, refresh))
        before_claims, before_answers = after_claims, after_answers
    return tuple(result)


def _policies() -> tuple[PolicySpec, ...]:
    return tuple(
        sorted(
            (
                PolicySpec(
                    policy_id=f"m4-9-{kind.value}-v1",
                    kind=kind,
                    policy_hash=stable_m4_digest(
                        "m4-9-controlled-ablation-v1", kind.value
                    ),
                )
                for kind in AblationKind
            ),
            key=lambda item: item.policy_id,
        )
    )


def _selected(event: _Event, kind: AblationKind) -> tuple[PairKey, ...]:
    names: tuple[str, ...]
    if kind is AblationKind.VECTOR_ONLY:
        names = ("vector",)
    elif kind is AblationKind.LEXICAL_ONLY:
        names = ("lexical",)
    elif kind is AblationKind.UNION:
        names = ("vector", "lexical")
    elif kind is AblationKind.LINEAGE:
        names = ("vector", "lexical", "lineage")
    elif kind is AblationKind.FRONTIER:
        names = ("vector", "lexical", "lineage", "frontier")
    elif kind is AblationKind.FRESH_FALLBACK:
        names = ("vector", "lexical", "lineage", "frontier", "fresh")
    else:
        raise ValueError("exhaustive refresh selection is supplied by the oracle")
    return tuple(sorted({pair for name in names for pair in event.channel(name)}))


def _project_effect_statuses(
    oracle: FrozenOracleEvent,
    claims: tuple[ClaimState, ...],
    answers: tuple[AnswerState, ...],
) -> tuple[tuple[ObjectStatus, ...], tuple[ObjectStatus, ...]]:
    claim_status = {item.claim_id: item.status.value for item in claims}
    answer_status = {
        item.answer_version_id: item.status.value for item in answers
    }
    return (
        tuple(
            ObjectStatus(item.object_id, claim_status[item.object_id])
            for item in oracle.claim_status_effects
        ),
        tuple(
            ObjectStatus(item.object_id, answer_status[item.object_id])
            for item in oracle.answer_status_effects
        ),
    )


def _treatments(
    histories: tuple[_History, ...],
    policies: tuple[PolicySpec, ...],
    oracle_material: dict[str, _OracleMaterial],
) -> tuple[TreatmentEvent, ...]:
    rows: list[TreatmentEvent] = []
    policy_by_kind = {item.kind: item for item in policies}
    for history in histories:
        if history.assignment.split_id != "test":
            continue
        judge_by_pair = {item.pair: item for item in history.judgments}
        for kind in AblationKind:
            policy = policy_by_kind[kind]
            if kind is AblationKind.EXHAUSTIVE_REFRESH:
                for event in history.events:
                    material = oracle_material[event.event_id]
                    refresh = material.refresh
                    claims, answers = _project_effect_statuses(
                        material.event, refresh.claim_states, refresh.answer_states
                    )
                    pair_count = len(refresh.retrieved_pairs)
                    rows.append(
                        TreatmentEvent(
                            policy_id=policy.policy_id,
                            event_id=event.event_id,
                            treatment_manifest_hash=stable_m4_digest(
                                "m4-9-exhaustive-treatment-v1",
                                policy.policy_hash,
                                refresh.manifest_id,
                            ),
                            admitted_pairs=refresh.retrieved_pairs,
                            claim_post_statuses=claims,
                            answer_post_statuses=answers,
                            work=VerifierMeasurement(
                                attempted_pair_count=pair_count,
                                completed_pair_count=pair_count,
                                failed_pair_count=0,
                                timeout_pair_count=0,
                                call_count=1 if pair_count else 0,
                            ),
                        )
                    )
                continue

            active: set[str] = set()
            current: dict[PairKey, PairJudgment] = {}
            for event in history.events:
                _apply_structure(event, active, current)
                selected = _selected(event, kind)
                timed_out: tuple[PairKey, ...] = ()
                if (
                    kind is AblationKind.VECTOR_ONLY
                    and history.assignment.history_id == "test-h2"
                    and event.event_index == 1
                    and selected
                ):
                    timed_out = (selected[0],)
                for pair in selected:
                    if pair not in timed_out:
                        current[pair] = judge_by_pair[pair]
                claim_states, answer_states = _recompute(history, active, current)
                material = oracle_material[event.event_id]
                claim_projection, answer_projection = _project_effect_statuses(
                    material.event, claim_states, answer_states
                )
                completed = len(selected) - len(timed_out)
                rows.append(
                    TreatmentEvent(
                        policy_id=policy.policy_id,
                        event_id=event.event_id,
                        treatment_manifest_hash=stable_m4_digest(
                            "m4-9-controlled-treatment-v1",
                            policy.policy_hash,
                            material.event.event_manifest_hash,
                            *(
                                f"{pair.claim_id}:{pair.chunk_version_id}"
                                for pair in selected
                            ),
                            "timeouts",
                            *(
                                f"{pair.claim_id}:{pair.chunk_version_id}"
                                for pair in timed_out
                            ),
                        ),
                        admitted_pairs=selected,
                        claim_post_statuses=claim_projection,
                        answer_post_statuses=answer_projection,
                        work=VerifierMeasurement(
                            attempted_pair_count=len(selected),
                            completed_pair_count=completed,
                            failed_pair_count=0,
                            timeout_pair_count=len(timed_out),
                            call_count=1 if selected else 0,
                        ),
                        failure_code=(
                            "controlled_timeout_injection" if timed_out else None
                        ),
                    )
                )
    return tuple(rows)


def build_controlled_study() -> EmpiricalStudySpec:
    histories = _histories()
    selected = tuple(
        history
        for history in histories
        if history.assignment.split_id == "test"
    )
    oracle_by_event = {
        material.event.event_id: material
        for history in selected
        for material in _oracle_material(history)
    }
    policies = _policies()
    return EmpiricalStudySpec(
        schema_version="m4-empirical-study-v1",
        study_id="m4-9-controlled-selective-maintenance-v1",
        dataset_version="m4-9-controlled-dynamic-history-v1",
        selected_split_id="test",
        measurement_source=(
            "controlled_table_no_real_models_with_one_timeout_fault_injection"
        ),
        verifier_identity="controlled-table-verifier-no-model-v1",
        decision_policy_id="controlled-decision-v1",
        histories=tuple(history.assignment for history in histories),
        policies=policies,
        oracle_events=tuple(
            sorted(
                (material.event for material in oracle_by_event.values()),
                key=lambda item: (item.history_id, item.event_index, item.event_id),
            )
        ),
        treatments=_treatments(selected, policies, oracle_by_event),
        bootstrap_seed=20260720,
        bootstrap_replicates=10_000,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=Path("/tmp/groundloop-m4-9-controlled"),
    )
    args = parser.parse_args()
    report = evaluate_frozen_history(build_controlled_study())
    bundle = write_empirical_bundle(report, args.output_directory)
    print(
        f"study_manifest_hash={bundle.study_manifest_hash}\n"
        f"report_hash={bundle.report_hash}\n"
        f"bundle_manifest_hash={bundle.bundle_manifest_hash}\n"
        f"output_directory={bundle.output_directory}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
