"""Frozen seven-baseline protocol for deterministic M5 histories."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from enum import StrEnum

from groundloop.errors import ValidationError
from groundloop.m5.evaluation.histories import (
    ControlledHistory,
    EvaluationEvent,
    EvaluationGroup,
    EvaluationUnit,
    HistoryOperationKind,
    history_event_identity_digest,
)


class BaselineId(StrEnum):
    SOURCE_INVALIDATION = "source_invalidation"
    FROZEN_DIRECT_CITATION = "frozen_direct_citation"
    DIRECT_WITNESS = "direct_witness"
    NON_DISTINCT_CONJUNCTION = "non_distinct_conjunction"
    GROUNDLOOP_HALL_SDR = "groundloop_hall_sdr"
    AFFECTED_GROUP_FULL_MATCHING = "affected_group_full_matching"
    ALL_GROUP_FULL_RECOMPUTATION = "all_group_full_recomputation"


class BaselineRole(StrEnum):
    DIFFERENT_SEMANTIC_POLICY = "different_semantic_policy"
    SEMANTIC_ABLATION = "semantic_ablation"
    TARGET_ALGORITHM = "target_algorithm"
    SAME_SEMANTICS_SYSTEMS_COMPARATOR = "same_semantics_systems_comparator"


class Availability(StrEnum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


class MeasurementValidity(StrEnum):
    FUNCTIONAL_PROTOCOL_ONLY = "functional_protocol_only"
    CONTROLLED_PYTHON_SYSTEMS_COMPARATOR = "controlled_python_systems_comparator"


class EvaluationClaimStatus(StrEnum):
    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"
    REFUTED = "refuted"
    CONFLICTED = "conflicted"


@dataclass(frozen=True, slots=True)
class BaselineSpec:
    baseline_id: BaselineId
    ordinal: int
    display_name: str
    role: BaselineRole
    execution_backend: str
    measurement_validity: MeasurementValidity


FROZEN_BASELINES = (
    BaselineSpec(
        BaselineId.SOURCE_INVALIDATION,
        1,
        "Source-level invalidation",
        BaselineRole.DIFFERENT_SEMANTIC_POLICY,
        "pure-policy-simulator-v1",
        MeasurementValidity.FUNCTIONAL_PROTOCOL_ONLY,
    ),
    BaselineSpec(
        BaselineId.FROZEN_DIRECT_CITATION,
        2,
        "Frozen direct-citation invalidation",
        BaselineRole.DIFFERENT_SEMANTIC_POLICY,
        "pure-policy-simulator-v1",
        MeasurementValidity.FUNCTIONAL_PROTOCOL_ONLY,
    ),
    BaselineSpec(
        BaselineId.DIRECT_WITNESS,
        3,
        "Direct-witness-only GroundLoop",
        BaselineRole.DIFFERENT_SEMANTIC_POLICY,
        "pure-policy-simulator-v1",
        MeasurementValidity.FUNCTIONAL_PROTOCOL_ONLY,
    ),
    BaselineSpec(
        BaselineId.NON_DISTINCT_CONJUNCTION,
        4,
        "Non-distinct requirement conjunction",
        BaselineRole.SEMANTIC_ABLATION,
        "pure-policy-simulator-v1",
        MeasurementValidity.FUNCTIONAL_PROTOCOL_ONLY,
    ),
    BaselineSpec(
        BaselineId.GROUNDLOOP_HALL_SDR,
        5,
        "GroundLoop Hall/SDR maintenance",
        BaselineRole.TARGET_ALGORITHM,
        "pure-affected-group-hall-recompute-scaffold-v1",
        MeasurementValidity.FUNCTIONAL_PROTOCOL_ONLY,
    ),
    BaselineSpec(
        BaselineId.AFFECTED_GROUP_FULL_MATCHING,
        6,
        "Affected-group full matching",
        BaselineRole.SAME_SEMANTICS_SYSTEMS_COMPARATOR,
        "pure-affected-group-full-matching-v1",
        MeasurementValidity.CONTROLLED_PYTHON_SYSTEMS_COMPARATOR,
    ),
    BaselineSpec(
        BaselineId.ALL_GROUP_FULL_RECOMPUTATION,
        7,
        "All-group full recomputation",
        BaselineRole.SAME_SEMANTICS_SYSTEMS_COMPARATOR,
        "pure-all-group-full-matching-v1",
        MeasurementValidity.CONTROLLED_PYTHON_SYSTEMS_COMPARATOR,
    ),
)


@dataclass(frozen=True, slots=True)
class WorkCounters:
    requirement_keys_touched: int = 0
    group_keys_touched: int = 0
    claim_keys_touched: int = 1
    answer_keys_touched: int = 0
    edge_checks: int = 0
    hall_subset_masks: int = 0
    groups_rematched: int = 0
    source_checks: int = 0
    citation_checks: int = 0
    verifier_pairs: int = 0
    model_calls: int = 0
    model_tokens: int = 0


@dataclass(frozen=True, slots=True)
class BaselinePoint:
    history_id: str
    cluster_id: str
    baseline_id: BaselineId
    baseline_role: BaselineRole
    execution_backend: str
    measurement_validity: MeasurementValidity
    event_ordinal: int
    event_id: str
    event_hash: str
    availability: Availability
    unavailable_reason: str | None
    source_semantic_label: bool
    groundloop_sdr_complete: bool
    direct_source_support: bool
    refute_active: bool
    y_hat: bool | None
    predicted_status: EvaluationClaimStatus | None
    exact_status: EvaluationClaimStatus
    work: WorkCounters
    latency_ns: int | None
    timing_mode: str
    state_bytes: int | None
    certificate_bytes: int | None

    def __post_init__(self) -> None:
        if self.availability is Availability.UNAVAILABLE:
            if (
                self.y_hat is not None
                or self.predicted_status is not None
                or self.unavailable_reason is None
            ):
                raise ValidationError(
                    "UNAVAILABLE results require a reason and no score"
                )
        elif (
            self.y_hat is None
            or self.predicted_status is None
            or self.unavailable_reason is not None
        ):
            raise ValidationError("available results require a score and no reason")
        if self.y_hat is not None and self.predicted_status is not _claim_status(
            self.y_hat, self.refute_active
        ):
            raise ValidationError("predicted status does not match support/refute bits")
        exact_support = self.direct_source_support or self.groundloop_sdr_complete
        if self.exact_status is not _claim_status(exact_support, self.refute_active):
            raise ValidationError("exact status does not match support/refute bits")
        if self.work.model_calls != 0 or self.work.model_tokens != 0:
            raise ValidationError("controlled baselines must report zero model work")
        if (
            self.measurement_validity is MeasurementValidity.FUNCTIONAL_PROTOCOL_ONLY
            and self.latency_ns is not None
        ):
            raise ValidationError("functional-only simulators cannot report latency")


@dataclass(frozen=True, slots=True)
class EvaluationRun:
    schema: str
    event_identity_digest: str
    baseline_specs: tuple[BaselineSpec, ...]
    points: tuple[BaselinePoint, ...]
    timing_mode: str
    evaluator: str


@dataclass(slots=True)
class _State:
    units_by_id: dict[str, EvaluationUnit]
    groups_by_id: dict[str, EvaluationGroup]
    active_unit_ids: set[str]
    active_source_ids: set[str]
    active_group_ids: set[str]
    policy_version: str

    def effective_unit_ids(self) -> set[str]:
        return {
            unit_id
            for unit_id in self.active_unit_ids
            if self.units_by_id[unit_id].source_version_id in self.active_source_ids
        }


@dataclass(slots=True)
class _StrategyCache:
    group_complete: dict[str, bool] = field(default_factory=dict)
    group_assignment: dict[str, tuple[str | None, ...]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _Prediction:
    y_hat: bool
    work: WorkCounters
    state_bytes: int
    certificate_bytes: int


def _initial_state(history: ControlledHistory) -> _State:
    return _State(
        units_by_id={item.unit_id: item for item in history.initial.units},
        groups_by_id={item.group_id: item for item in history.initial.groups},
        active_unit_ids=set(history.initial.active_unit_ids),
        active_source_ids=set(history.initial.active_source_version_ids),
        active_group_ids=set(history.initial.active_group_ids),
        policy_version=history.initial.policy_version,
    )


def _apply_event(state: _State, event: EvaluationEvent) -> None:
    operation = event.operation
    if operation.kind is HistoryOperationKind.INITIALIZE:
        return
    if operation.kind in (
        HistoryOperationKind.UNIT_INSERT,
        HistoryOperationKind.UNIT_DELETE,
    ):
        assert operation.unit_id is not None
        if operation.unit_id not in state.units_by_id:
            raise ValidationError(f"event references unknown unit {operation.unit_id}")
        if operation.kind is HistoryOperationKind.UNIT_INSERT:
            state.active_unit_ids.add(operation.unit_id)
        else:
            state.active_unit_ids.discard(operation.unit_id)
        return
    if operation.kind is HistoryOperationKind.UNIT_REPLACE:
        assert operation.unit_id is not None
        assert operation.replacement_unit_id is not None
        if not {operation.unit_id, operation.replacement_unit_id} <= set(
            state.units_by_id
        ):
            raise ValidationError("replacement references an unknown unit")
        state.active_unit_ids.discard(operation.unit_id)
        state.active_unit_ids.add(operation.replacement_unit_id)
        return
    if operation.kind in (
        HistoryOperationKind.SOURCE_INSERT,
        HistoryOperationKind.SOURCE_DELETE,
    ):
        assert operation.source_version_id is not None
        known_sources = {item.source_version_id for item in state.units_by_id.values()}
        if operation.source_version_id not in known_sources:
            raise ValidationError("event references an unknown source version")
        if operation.kind is HistoryOperationKind.SOURCE_INSERT:
            state.active_source_ids.add(operation.source_version_id)
        else:
            state.active_source_ids.discard(operation.source_version_id)
        return
    if operation.kind is HistoryOperationKind.GROUP_SUPERSEDE:
        assert operation.old_group_id is not None
        assert operation.new_group_id is not None
        if not {operation.old_group_id, operation.new_group_id} <= set(
            state.groups_by_id
        ):
            raise ValidationError("supersession references an unknown group")
        state.active_group_ids.discard(operation.old_group_id)
        state.active_group_ids.add(operation.new_group_id)
        return
    assert operation.policy_version is not None
    state.policy_version = operation.policy_version


def _active_edges(state: _State, group: EvaluationGroup) -> tuple[tuple[str, ...], ...]:
    effective = state.effective_unit_ids()
    return tuple(
        tuple(
            sorted(
                {
                    state.units_by_id[unit_id].text_hash
                    for unit_id in requirement.witness_unit_ids
                    if unit_id in effective
                }
            )
        )
        for requirement in group.requirements
    )


def _hall_with_work(edges: tuple[tuple[str, ...], ...]) -> tuple[bool, int, int]:
    requirement_count = len(edges)
    edge_checks = 0
    masks = 0
    for mask in range(1, 1 << requirement_count):
        masks += 1
        union: set[str] = set()
        left_count = 0
        for ordinal, requirement_edges in enumerate(edges):
            if mask & (1 << ordinal):
                left_count += 1
                edge_checks += len(requirement_edges)
                union.update(requirement_edges)
        if len(union) < left_count:
            return False, edge_checks, masks
    return True, edge_checks, masks


def _matching_with_work(
    edges: tuple[tuple[str, ...], ...],
) -> tuple[int, tuple[str | None, ...], int]:
    owner_by_hash: dict[str, int] = {}
    assignment: list[str | None] = [None] * len(edges)
    edge_checks = 0

    def augment(ordinal: int, seen: set[str]) -> bool:
        nonlocal edge_checks
        for text_hash in edges[ordinal]:
            edge_checks += 1
            if text_hash in seen:
                continue
            seen.add(text_hash)
            previous = owner_by_hash.get(text_hash)
            if previous is None or augment(previous, seen):
                owner_by_hash[text_hash] = ordinal
                assignment[ordinal] = text_hash
                return True
        return False

    size = 0
    for ordinal in range(len(edges)):
        if augment(ordinal, set()):
            size += 1
    return size, tuple(assignment), edge_checks


def _direct_support(state: _State) -> bool:
    effective = state.effective_unit_ids()
    return any(
        state.units_by_id[unit_id].independently_annotated_direct_support
        for unit_id in effective
    )


def _direct_refute(state: _State) -> bool:
    effective = state.effective_unit_ids()
    return any(
        state.units_by_id[unit_id].independently_annotated_direct_refute
        for unit_id in effective
    )


def _claim_status(support: bool, refute: bool) -> EvaluationClaimStatus:
    if support and refute:
        return EvaluationClaimStatus.CONFLICTED
    if support:
        return EvaluationClaimStatus.SUPPORTED
    if refute:
        return EvaluationClaimStatus.REFUTED
    return EvaluationClaimStatus.UNSUPPORTED


def _source_conjunction(state: _State) -> bool:
    return any(
        all(_active_edges(state, state.groups_by_id[group_id]))
        for group_id in state.active_group_ids
    )


def _exact_sdr(state: _State) -> bool:
    for group_id in sorted(state.active_group_ids):
        group = state.groups_by_id[group_id]
        edges = _active_edges(state, group)
        size, _, _ = _matching_with_work(edges)
        if size == len(group.requirements):
            return True
    return False


def _affected_group_ids(state: _State, event: EvaluationEvent) -> tuple[str, ...]:
    operation = event.operation
    if operation.kind in (
        HistoryOperationKind.INITIALIZE,
        HistoryOperationKind.POLICY_CHANGE,
    ):
        return tuple(sorted(state.active_group_ids))
    if operation.kind is HistoryOperationKind.GROUP_SUPERSEDE:
        assert operation.old_group_id is not None
        assert operation.new_group_id is not None
        return tuple(sorted({operation.old_group_id, operation.new_group_id}))
    affected_units: set[str] = set()
    if operation.unit_id is not None:
        affected_units.add(operation.unit_id)
    if operation.replacement_unit_id is not None:
        affected_units.add(operation.replacement_unit_id)
    if operation.source_version_id is not None:
        affected_units.update(
            unit.unit_id
            for unit in state.units_by_id.values()
            if unit.source_version_id == operation.source_version_id
        )
    result = {
        group.group_id
        for group in state.groups_by_id.values()
        if any(
            unit_id in affected_units
            for requirement in group.requirements
            for unit_id in requirement.witness_unit_ids
        )
    }
    return tuple(sorted(result))


def _certificate_bytes(
    state: _State,
    assignments: dict[str, tuple[str | None, ...]],
) -> int:
    complete = [
        (group_id, assignments[group_id])
        for group_id in sorted(state.active_group_ids)
        if group_id in assignments
        and all(value is not None for value in assignments[group_id])
    ]
    if not complete:
        return 0
    group_id, assignment = complete[0]
    payload = {"group_id": group_id, "assignment_by_ordinal": assignment}
    return len(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )


def _state_bytes(baseline_id: BaselineId, payload: object) -> int:
    return len(
        json.dumps(
            {"baseline": baseline_id.value, "state": payload},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )


def _predict_group_strategy(
    *,
    baseline_id: BaselineId,
    state: _State,
    event: EvaluationEvent,
    cache: _StrategyCache,
    hall: bool,
    all_groups: bool,
) -> _Prediction:
    affected = (
        tuple(sorted(state.active_group_ids))
        if all_groups
        else _affected_group_ids(state, event)
    )
    edge_checks = 0
    masks = 0
    groups_rematched = 0
    requirement_keys: set[str] = set()
    for group_id in affected:
        if group_id not in state.active_group_ids:
            cache.group_complete.pop(group_id, None)
            cache.group_assignment.pop(group_id, None)
            continue
        group = state.groups_by_id[group_id]
        requirement_keys.update(item.requirement_id for item in group.requirements)
        edges = _active_edges(state, group)
        groups_rematched += 1
        if hall:
            complete, local_edges, local_masks = _hall_with_work(edges)
            edge_checks += local_edges
            masks += local_masks
            if complete:
                size, assignment, certificate_edges = _matching_with_work(edges)
                edge_checks += certificate_edges
                assert size == len(group.requirements)
                cache.group_assignment[group_id] = assignment
            else:
                cache.group_assignment.pop(group_id, None)
        else:
            size, assignment, local_edges = _matching_with_work(edges)
            edge_checks += local_edges
            complete = size == len(group.requirements)
            if complete:
                cache.group_assignment[group_id] = assignment
            else:
                cache.group_assignment.pop(group_id, None)
        cache.group_complete[group_id] = complete
    direct = _direct_support(state)
    y_hat = direct or any(
        cache.group_complete.get(group_id, False) for group_id in state.active_group_ids
    )
    encoded_cache = {
        "policy_version": state.policy_version,
        "groups": [
            [group_id, cache.group_complete.get(group_id, False)]
            for group_id in sorted(state.active_group_ids)
        ],
        "direct": direct,
    }
    return _Prediction(
        y_hat=y_hat,
        work=WorkCounters(
            requirement_keys_touched=len(requirement_keys),
            group_keys_touched=len(set(affected)),
            edge_checks=edge_checks,
            hall_subset_masks=masks,
            groups_rematched=groups_rematched,
        ),
        state_bytes=_state_bytes(baseline_id, encoded_cache),
        certificate_bytes=_certificate_bytes(state, cache.group_assignment),
    )


def _predict_simple(
    *,
    baseline_id: BaselineId,
    state: _State,
    frozen_source_ids: tuple[str, ...],
    frozen_citation_ids: tuple[str, ...],
) -> _Prediction:
    effective = state.effective_unit_ids()
    if baseline_id is BaselineId.SOURCE_INVALIDATION:
        y_hat = bool(frozen_source_ids) and all(
            source_id in state.active_source_ids for source_id in frozen_source_ids
        )
        work = WorkCounters(source_checks=len(frozen_source_ids))
        payload: object = {"frozen_source_ids": frozen_source_ids}
    elif baseline_id is BaselineId.FROZEN_DIRECT_CITATION:
        y_hat = bool(frozen_citation_ids) and all(
            unit_id in effective for unit_id in frozen_citation_ids
        )
        work = WorkCounters(citation_checks=len(frozen_citation_ids))
        payload = {"frozen_citation_ids": frozen_citation_ids}
    elif baseline_id is BaselineId.DIRECT_WITNESS:
        y_hat = _direct_support(state)
        work = WorkCounters(
            citation_checks=sum(
                unit.independently_annotated_direct_support
                for unit in state.units_by_id.values()
            )
        )
        payload = {"direct_support": y_hat}
    else:
        group_ids = tuple(sorted(state.active_group_ids))
        requirement_count = sum(
            len(state.groups_by_id[group_id].requirements) for group_id in group_ids
        )
        edge_checks = sum(
            len(edges)
            for group_id in group_ids
            for edges in _active_edges(state, state.groups_by_id[group_id])
        )
        y_hat = _direct_support(state) or _source_conjunction(state)
        work = WorkCounters(
            requirement_keys_touched=requirement_count,
            group_keys_touched=len(group_ids),
            edge_checks=edge_checks,
        )
        payload = {"active_group_ids": group_ids, "direct": _direct_support(state)}
    return _Prediction(
        y_hat=y_hat,
        work=work,
        state_bytes=_state_bytes(baseline_id, payload),
        certificate_bytes=0,
    )


def _frozen_inputs(
    history: ControlledHistory,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    state = _initial_state(history)
    effective = state.effective_unit_ids()
    citation_ids: set[str] = set()
    for group_id in sorted(state.active_group_ids):
        group = state.groups_by_id[group_id]
        for requirement in group.requirements:
            active = sorted(set(requirement.witness_unit_ids) & effective)
            if active:
                citation_ids.add(active[0])
    direct_citation_ids = {
        unit_id
        for unit_id in effective
        if state.units_by_id[unit_id].independently_annotated_direct_support
    }
    source_ids = tuple(
        sorted(
            {
                state.units_by_id[unit_id].source_version_id
                for unit_id in citation_ids | direct_citation_ids
            }
        )
    )
    return source_ids, tuple(sorted(citation_ids))


def run_baselines(
    histories: tuple[ControlledHistory, ...], *, measure_latency: bool = False
) -> EvaluationRun:
    """Run every available baseline over identical event identities."""

    points: list[BaselinePoint] = []
    for history in histories:
        state = _initial_state(history)
        frozen_sources, frozen_citations = _frozen_inputs(history)
        has_direct_fixture = any(
            unit.independently_annotated_direct_support
            for unit in history.initial.units
        )
        caches = {spec.baseline_id: _StrategyCache() for spec in FROZEN_BASELINES}
        for event in history.events:
            _apply_event(state, event)
            direct = _direct_support(state)
            source_label = direct or _source_conjunction(state)
            exact_sdr = _exact_sdr(state)
            refute = _direct_refute(state)
            exact_status = _claim_status(direct or exact_sdr, refute)
            for spec in FROZEN_BASELINES:
                unavailable = (
                    spec.baseline_id is BaselineId.DIRECT_WITNESS
                    and not has_direct_fixture
                )
                if unavailable:
                    points.append(
                        BaselinePoint(
                            history_id=history.history_id,
                            cluster_id=history.cluster_id,
                            baseline_id=spec.baseline_id,
                            baseline_role=spec.role,
                            execution_backend=spec.execution_backend,
                            measurement_validity=spec.measurement_validity,
                            event_ordinal=event.ordinal,
                            event_id=event.event_id,
                            event_hash=event.event_hash,
                            availability=Availability.UNAVAILABLE,
                            unavailable_reason=(
                                "no independently annotated whole-claim evidence unit"
                            ),
                            source_semantic_label=source_label,
                            groundloop_sdr_complete=exact_sdr,
                            direct_source_support=direct,
                            refute_active=refute,
                            y_hat=None,
                            predicted_status=None,
                            exact_status=exact_status,
                            work=WorkCounters(),
                            latency_ns=None,
                            timing_mode="not_measured",
                            state_bytes=None,
                            certificate_bytes=None,
                        )
                    )
                    continue
                latency_permitted = (
                    spec.measurement_validity
                    is MeasurementValidity.CONTROLLED_PYTHON_SYSTEMS_COMPARATOR
                )
                start = (
                    time.perf_counter_ns()
                    if measure_latency and latency_permitted
                    else 0
                )
                if spec.baseline_id in (
                    BaselineId.SOURCE_INVALIDATION,
                    BaselineId.FROZEN_DIRECT_CITATION,
                    BaselineId.DIRECT_WITNESS,
                    BaselineId.NON_DISTINCT_CONJUNCTION,
                ):
                    prediction = _predict_simple(
                        baseline_id=spec.baseline_id,
                        state=state,
                        frozen_source_ids=frozen_sources,
                        frozen_citation_ids=frozen_citations,
                    )
                else:
                    prediction = _predict_group_strategy(
                        baseline_id=spec.baseline_id,
                        state=state,
                        event=event,
                        cache=caches[spec.baseline_id],
                        hall=spec.baseline_id is BaselineId.GROUNDLOOP_HALL_SDR,
                        all_groups=(
                            spec.baseline_id is BaselineId.ALL_GROUP_FULL_RECOMPUTATION
                        ),
                    )
                latency = (
                    time.perf_counter_ns() - start
                    if measure_latency and latency_permitted
                    else None
                )
                points.append(
                    BaselinePoint(
                        history_id=history.history_id,
                        cluster_id=history.cluster_id,
                        baseline_id=spec.baseline_id,
                        baseline_role=spec.role,
                        execution_backend=spec.execution_backend,
                        measurement_validity=spec.measurement_validity,
                        event_ordinal=event.ordinal,
                        event_id=event.event_id,
                        event_hash=event.event_hash,
                        availability=Availability.AVAILABLE,
                        unavailable_reason=None,
                        source_semantic_label=source_label,
                        groundloop_sdr_complete=exact_sdr,
                        direct_source_support=direct,
                        refute_active=refute,
                        y_hat=prediction.y_hat,
                        predicted_status=_claim_status(prediction.y_hat, refute),
                        exact_status=exact_status,
                        work=prediction.work,
                        latency_ns=latency,
                        timing_mode=(
                            "perf_counter_ns"
                            if measure_latency and latency_permitted
                            else (
                                "disabled_functional_only"
                                if measure_latency
                                else "not_measured"
                            )
                        ),
                        state_bytes=prediction.state_bytes,
                        certificate_bytes=prediction.certificate_bytes,
                    )
                )
    return EvaluationRun(
        schema="groundloop-m5-baseline-run-v1",
        event_identity_digest=history_event_identity_digest(histories),
        baseline_specs=FROZEN_BASELINES,
        points=tuple(points),
        timing_mode="mixed_validity" if measure_latency else "not_measured",
        evaluator="pure-evaluation-protocol-v1",
    )


__all__ = [
    "Availability",
    "BaselineId",
    "BaselinePoint",
    "BaselineRole",
    "BaselineSpec",
    "EvaluationRun",
    "EvaluationClaimStatus",
    "FROZEN_BASELINES",
    "MeasurementValidity",
    "WorkCounters",
    "run_baselines",
]
