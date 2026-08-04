"""Deterministic dynamic histories for the M5 controlled evaluation."""

from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from dataclasses import dataclass
from enum import StrEnum

from groundloop.errors import ValidationError
from groundloop.m5.digests import (
    enum_field,
    hash_field,
    int_field,
    option_field,
    sequence_field,
    stable_m5_digest,
    text_field,
)
from groundloop.m5.evaluation.records import (
    SourceAnnotation,
    SourceRequirement,
    WiceAdapterResult,
    WiceSplit,
)


class CohortKind(StrEnum):
    WICE_PRIMARY = "wice_primary_retrospective"
    CONTROLLED = "controlled_authored"


class HistoryOperationKind(StrEnum):
    INITIALIZE = "initialize"
    UNIT_INSERT = "unit_insert"
    UNIT_DELETE = "unit_delete"
    UNIT_REPLACE = "unit_replace"
    SOURCE_INSERT = "source_insert"
    SOURCE_DELETE = "source_delete"
    GROUP_SUPERSEDE = "group_supersede"
    POLICY_CHANGE = "policy_change"


def _require_identifier(name: str, value: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{name} must be a nonempty identifier")


def _require_sha(name: str, value: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


@dataclass(frozen=True, slots=True)
class EvaluationUnit:
    unit_id: str
    text_hash: str
    source_version_id: str
    independently_annotated_direct_support: bool = False
    independently_annotated_direct_refute: bool = False

    def __post_init__(self) -> None:
        _require_identifier("unit_id", self.unit_id)
        _require_sha("unit text_hash", self.text_hash)
        _require_identifier("source_version_id", self.source_version_id)


@dataclass(frozen=True, slots=True)
class EvaluationRequirement:
    requirement_id: str
    witness_unit_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_identifier("requirement_id", self.requirement_id)
        if tuple(sorted(set(self.witness_unit_ids))) != self.witness_unit_ids:
            raise ValidationError("witness unit IDs must be unique and sorted")
        if not self.witness_unit_ids:
            raise ValidationError("a controlled requirement needs source witnesses")


@dataclass(frozen=True, slots=True)
class EvaluationGroup:
    group_id: str
    requirements: tuple[EvaluationRequirement, ...]

    def __post_init__(self) -> None:
        _require_identifier("group_id", self.group_id)
        if not 1 <= len(self.requirements) <= 8:
            raise ValidationError("an evaluation group needs one to eight requirements")
        ids = tuple(item.requirement_id for item in self.requirements)
        if len(ids) != len(set(ids)):
            raise ValidationError("evaluation requirement IDs must be unique")


@dataclass(frozen=True, slots=True)
class HistoryInitialState:
    units: tuple[EvaluationUnit, ...]
    groups: tuple[EvaluationGroup, ...]
    active_unit_ids: tuple[str, ...]
    active_source_version_ids: tuple[str, ...]
    active_group_ids: tuple[str, ...]
    policy_version: str = "controlled-projection-policy-v1"

    def __post_init__(self) -> None:
        _require_identifier("policy_version", self.policy_version)
        unit_ids = tuple(item.unit_id for item in self.units)
        if len(unit_ids) != len(set(unit_ids)):
            raise ValidationError("evaluation unit IDs must be unique")
        group_ids = tuple(item.group_id for item in self.groups)
        if len(group_ids) != len(set(group_ids)):
            raise ValidationError("evaluation group IDs must be unique")
        if self.active_unit_ids != tuple(sorted(set(self.active_unit_ids))):
            raise ValidationError("active unit IDs must be unique and sorted")
        if self.active_group_ids != tuple(sorted(set(self.active_group_ids))):
            raise ValidationError("active group IDs must be unique and sorted")
        if self.active_source_version_ids != tuple(
            sorted(set(self.active_source_version_ids))
        ):
            raise ValidationError("active source IDs must be unique and sorted")
        if not set(self.active_unit_ids) <= set(unit_ids):
            raise ValidationError("active unit IDs must reference defined units")
        if not set(self.active_group_ids) <= set(group_ids):
            raise ValidationError("active group IDs must reference defined groups")
        source_ids = {item.source_version_id for item in self.units}
        if not set(self.active_source_version_ids) <= source_ids:
            raise ValidationError("active source IDs must reference defined units")
        for group in self.groups:
            for requirement in group.requirements:
                if not set(requirement.witness_unit_ids) <= set(unit_ids):
                    raise ValidationError("requirement references an unknown unit")


@dataclass(frozen=True, slots=True)
class HistoryOperation:
    kind: HistoryOperationKind
    unit_id: str | None = None
    replacement_unit_id: str | None = None
    source_version_id: str | None = None
    old_group_id: str | None = None
    new_group_id: str | None = None
    policy_version: str | None = None

    def __post_init__(self) -> None:
        expected: dict[HistoryOperationKind, tuple[str, ...]] = {
            HistoryOperationKind.INITIALIZE: (),
            HistoryOperationKind.UNIT_INSERT: ("unit_id",),
            HistoryOperationKind.UNIT_DELETE: ("unit_id",),
            HistoryOperationKind.UNIT_REPLACE: ("unit_id", "replacement_unit_id"),
            HistoryOperationKind.SOURCE_INSERT: ("source_version_id",),
            HistoryOperationKind.SOURCE_DELETE: ("source_version_id",),
            HistoryOperationKind.GROUP_SUPERSEDE: ("old_group_id", "new_group_id"),
            HistoryOperationKind.POLICY_CHANGE: ("policy_version",),
        }
        values = {
            "unit_id": self.unit_id,
            "replacement_unit_id": self.replacement_unit_id,
            "source_version_id": self.source_version_id,
            "old_group_id": self.old_group_id,
            "new_group_id": self.new_group_id,
            "policy_version": self.policy_version,
        }
        required = set(expected[self.kind])
        for name, value in values.items():
            if name in required:
                if value is None:
                    raise ValidationError(f"{self.kind.value} requires {name}")
                _require_identifier(name, value)
            elif value is not None:
                raise ValidationError(f"{self.kind.value} does not accept {name}")


@dataclass(frozen=True, slots=True)
class EvaluationEvent:
    ordinal: int
    event_id: str
    event_hash: str
    operation: HistoryOperation

    def __post_init__(self) -> None:
        if isinstance(self.ordinal, bool) or self.ordinal < 0:
            raise ValidationError("event ordinal must be nonnegative")
        _require_sha("event_id", self.event_id)
        _require_sha("event_hash", self.event_hash)


@dataclass(frozen=True, slots=True)
class HistoryStratum:
    requirement_count: int
    maximum_requirement_degree: int
    has_content_overlap: bool
    initial_sdr_complete: bool
    has_alternative_assignment: bool
    provenance: str


@dataclass(frozen=True, slots=True)
class ControlledHistory:
    history_id: str
    cluster_id: str
    cohort_kind: CohortKind
    tags: tuple[str, ...]
    initial: HistoryInitialState
    events: tuple[EvaluationEvent, ...]
    stratum: HistoryStratum

    def __post_init__(self) -> None:
        _require_identifier("history_id", self.history_id)
        _require_identifier("cluster_id", self.cluster_id)
        if not self.events:
            raise ValidationError("a history must contain events")
        if tuple(event.ordinal for event in self.events) != tuple(
            range(len(self.events))
        ):
            raise ValidationError("event ordinals must be dense from zero")
        if self.events[0].operation.kind is not HistoryOperationKind.INITIALIZE:
            raise ValidationError("the first history event must initialize")
        if len({event.event_id for event in self.events}) != len(self.events):
            raise ValidationError("history event IDs must be unique")


def _optional_text(value: str | None) -> tuple[str, ...]:
    return option_field(text_field(value) if value is not None else None)


def build_evaluation_event(
    history_id: str, ordinal: int, operation: HistoryOperation
) -> EvaluationEvent:
    """Build a stable payload hash and ID shared by every baseline."""

    event_hash = stable_m5_digest(
        "m5-controlled-evaluation-event-v1",
        text_field(history_id),
        int_field(ordinal),
        enum_field(operation.kind),
        _optional_text(operation.unit_id),
        _optional_text(operation.replacement_unit_id),
        _optional_text(operation.source_version_id),
        _optional_text(operation.old_group_id),
        _optional_text(operation.new_group_id),
        _optional_text(operation.policy_version),
    )
    event_id = stable_m5_digest(
        "m5-controlled-evaluation-event-id-v1", hash_field(event_hash)
    )
    return EvaluationEvent(
        ordinal=ordinal,
        event_id=event_id,
        event_hash=event_hash,
        operation=operation,
    )


def _history(
    *,
    name: str,
    units: tuple[EvaluationUnit, ...],
    groups: tuple[EvaluationGroup, ...],
    active_units: tuple[str, ...],
    active_groups: tuple[str, ...],
    operations: tuple[HistoryOperation, ...],
    tags: tuple[str, ...],
) -> ControlledHistory:
    history_id = stable_m5_digest("m5-authored-history-v1", text_field(name))
    initial = HistoryInitialState(
        units=units,
        groups=groups,
        active_unit_ids=tuple(sorted(active_units)),
        active_source_version_ids=tuple(
            sorted({unit.source_version_id for unit in units})
        ),
        active_group_ids=tuple(sorted(active_groups)),
    )
    events = tuple(
        build_evaluation_event(history_id, ordinal, operation)
        for ordinal, operation in enumerate(
            (HistoryOperation(HistoryOperationKind.INITIALIZE), *operations)
        )
    )
    unit_by_id = {unit.unit_id: unit for unit in units}
    degrees = [
        len({unit_by_id[item].text_hash for item in requirement.witness_unit_ids})
        for group in groups
        for requirement in group.requirements
    ]
    text_degree = Counter(
        unit_by_id[item].text_hash
        for group in groups
        for requirement in group.requirements
        for item in requirement.witness_unit_ids
    )
    from groundloop.m5.evaluation.matching import (  # local to avoid a cycle
        maximum_matching,
        perfect_matching_count,
    )

    initial_group = groups[0]
    initial_edges = tuple(
        tuple(
            sorted(
                {
                    unit_by_id[item].text_hash
                    for item in requirement.witness_unit_ids
                    if item in initial.active_unit_ids
                }
            )
        )
        for requirement in initial_group.requirements
    )
    size, _ = maximum_matching(initial_edges)
    alternatives = perfect_matching_count(initial_edges)
    return ControlledHistory(
        history_id=history_id,
        cluster_id=f"controlled:{name}",
        cohort_kind=CohortKind.CONTROLLED,
        tags=tags,
        initial=initial,
        events=events,
        stratum=HistoryStratum(
            requirement_count=max(len(group.requirements) for group in groups),
            maximum_requirement_degree=max(degrees),
            has_content_overlap=any(value > 1 for value in text_degree.values()),
            initial_sdr_complete=size == len(initial_group.requirements),
            has_alternative_assignment=alternatives > 1,
            provenance="explicitly-authored-controlled-v1",
        ),
    )


def _hash(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def build_authored_controlled_histories() -> tuple[ControlledHistory, ...]:
    """Return small deterministic histories covering every frozen adversary."""

    duplicate_units = (
        EvaluationUnit("dup-a", _hash("duplicate"), "source-dup"),
        EvaluationUnit("dup-b", _hash("duplicate"), "source-dup"),
    )
    duplicate_group = EvaluationGroup(
        "group-duplicate",
        (EvaluationRequirement("req-duplicate", ("dup-a", "dup-b")),),
    )
    duplicate = _history(
        name="duplicate-content",
        units=duplicate_units,
        groups=(duplicate_group,),
        active_units=("dup-a", "dup-b"),
        active_groups=(duplicate_group.group_id,),
        operations=(
            HistoryOperation(HistoryOperationKind.UNIT_DELETE, unit_id="dup-a"),
            HistoryOperation(HistoryOperationKind.UNIT_DELETE, unit_id="dup-b"),
            HistoryOperation(HistoryOperationKind.UNIT_INSERT, unit_id="dup-a"),
        ),
        tags=("duplicate_content", "final_witness_loss"),
    )

    alternatives_units = (
        EvaluationUnit("alt-a", _hash("alt-a"), "source-alt"),
        EvaluationUnit("alt-b", _hash("alt-b"), "source-alt"),
        EvaluationUnit("alt-c", _hash("alt-c"), "source-alt"),
    )
    alternatives_group = EvaluationGroup(
        "group-alternative-witness",
        (
            EvaluationRequirement("req-alt-0", ("alt-a", "alt-b")),
            EvaluationRequirement("req-alt-1", ("alt-c",)),
        ),
    )
    alternatives = _history(
        name="alternative-witness-replace",
        units=alternatives_units,
        groups=(alternatives_group,),
        active_units=("alt-a", "alt-c"),
        active_groups=(alternatives_group.group_id,),
        operations=(
            HistoryOperation(
                HistoryOperationKind.UNIT_REPLACE,
                unit_id="alt-a",
                replacement_unit_id="alt-b",
            ),
        ),
        tags=("alternative_witness", "replace"),
    )

    matching_units = (
        EvaluationUnit("shared", _hash("shared"), "source-match"),
        EvaluationUnit("exclusive", _hash("exclusive"), "source-match"),
    )
    matching_group = EvaluationGroup(
        "group-matching-only",
        (
            EvaluationRequirement("req-match-0", ("shared",)),
            EvaluationRequirement("req-match-1", ("exclusive", "shared")),
        ),
    )
    matching_only = _history(
        name="matching-only-loss",
        units=matching_units,
        groups=(matching_group,),
        active_units=("exclusive", "shared"),
        active_groups=(matching_group.group_id,),
        operations=(
            HistoryOperation(HistoryOperationKind.UNIT_DELETE, unit_id="exclusive"),
        ),
        tags=("matching_only_loss", "overlap"),
    )

    group_units = (
        EvaluationUnit("group-a-unit", _hash("group-a"), "source-groups"),
        EvaluationUnit("group-b-unit", _hash("group-b"), "source-groups"),
        EvaluationUnit("group-c-unit", _hash("group-c"), "source-groups"),
    )
    group_a = EvaluationGroup(
        "group-a", (EvaluationRequirement("req-group-a", ("group-a-unit",)),)
    )
    group_b = EvaluationGroup(
        "group-b", (EvaluationRequirement("req-group-b", ("group-b-unit",)),)
    )
    group_c = EvaluationGroup(
        "group-c", (EvaluationRequirement("req-group-c", ("group-c-unit",)),)
    )
    alternative_groups = _history(
        name="alternative-group-supersession",
        units=group_units,
        groups=(group_a, group_b, group_c),
        active_units=("group-a-unit", "group-b-unit", "group-c-unit"),
        active_groups=("group-a", "group-b"),
        operations=(
            HistoryOperation(
                HistoryOperationKind.GROUP_SUPERSEDE,
                old_group_id="group-a",
                new_group_id="group-c",
            ),
            HistoryOperation(HistoryOperationKind.UNIT_DELETE, unit_id="group-b-unit"),
        ),
        tags=("alternative_groups", "supersession"),
    )

    direct_units = (
        EvaluationUnit("group-support", _hash("group-support"), "source-direct"),
        EvaluationUnit(
            "direct-support",
            _hash("direct-support"),
            "source-direct",
            independently_annotated_direct_support=True,
        ),
        EvaluationUnit(
            "direct-refute",
            _hash("direct-refute"),
            "source-direct",
            independently_annotated_direct_refute=True,
        ),
    )
    direct_group = EvaluationGroup(
        "group-direct",
        (EvaluationRequirement("req-direct", ("group-support",)),),
    )
    direct_conflict = _history(
        name="direct-support-conflict-policy",
        units=direct_units,
        groups=(direct_group,),
        active_units=("direct-refute", "direct-support", "group-support"),
        active_groups=(direct_group.group_id,),
        operations=(
            HistoryOperation(HistoryOperationKind.UNIT_DELETE, unit_id="group-support"),
            HistoryOperation(
                HistoryOperationKind.UNIT_DELETE, unit_id="direct-support"
            ),
            HistoryOperation(
                HistoryOperationKind.POLICY_CHANGE,
                policy_version="controlled-projection-policy-v1-replayed",
            ),
            HistoryOperation(
                HistoryOperationKind.SOURCE_DELETE,
                source_version_id="source-direct",
            ),
            HistoryOperation(
                HistoryOperationKind.SOURCE_INSERT,
                source_version_id="source-direct",
            ),
        ),
        tags=("direct_support", "refutation_conflict", "policy_change"),
    )
    return (
        duplicate,
        alternatives,
        matching_only,
        alternative_groups,
        direct_conflict,
    )


def build_wice_primary_histories(
    adapter: WiceAdapterResult,
) -> tuple[ControlledHistory, ...]:
    """Build deterministic deletion/source-loss histories without model calls."""

    unit_by_id = {item.evidence_unit_id: item for item in adapter.evidence_units}
    source_ids_by_unit: dict[str, set[str]] = defaultdict(set)
    for member in adapter.evidence_unit_members:
        source_ids_by_unit[member.evidence_unit_id].add(member.source_document_id)
    requirements_by_group: dict[str, list[SourceRequirement]] = defaultdict(list)
    for requirement in adapter.source_requirements:
        requirements_by_group[requirement.group_version_id].append(requirement)
    annotation_by_subclaim: dict[tuple[WiceSplit, str], list[SourceAnnotation]] = (
        defaultdict(list)
    )
    primary_subclaim_ids = {
        (item.split, item.subclaim_meta_id) for item in adapter.source_requirements
    }
    for annotation in adapter.source_annotations:
        if (
            annotation.split,
            annotation.subclaim_meta_id,
        ) in primary_subclaim_ids and annotation.source_label.value == "supported":
            annotation_by_subclaim[
                (annotation.split, annotation.subclaim_meta_id)
            ].append(annotation)
    exact_by_group = {
        item.group_version_id: item for item in adapter.exact_system_states
    }
    histories: list[ControlledHistory] = []
    for claim in adapter.source_claims:
        requirement_rows = sorted(
            requirements_by_group[claim.group_version_id],
            key=lambda item: item.ordinal,
        )
        requirements: list[EvaluationRequirement] = []
        unit_ids: set[str] = set()
        for row in requirement_rows:
            subclaim_key = (row.split, row.subclaim_meta_id)
            witness_ids = tuple(
                sorted(
                    {
                        annotation.evidence_unit_id
                        for annotation in annotation_by_subclaim[subclaim_key]
                    }
                )
            )
            unit_ids.update(witness_ids)
            requirements.append(
                EvaluationRequirement(
                    requirement_id=row.requirement_version_id,
                    witness_unit_ids=witness_ids,
                )
            )
        units: list[EvaluationUnit] = []
        for unit_id in sorted(unit_ids):
            unit_source_ids = source_ids_by_unit[unit_id]
            if len(unit_source_ids) != 1:
                raise ValidationError(
                    "a WiCE evidence unit must have one derived source identity"
                )
            units.append(
                EvaluationUnit(
                    unit_id=unit_id,
                    text_hash=unit_by_id[unit_id].text_hash,
                    source_version_id=next(iter(unit_source_ids)),
                )
            )
        group = EvaluationGroup(claim.group_version_id, tuple(requirements))
        history_id = stable_m5_digest(
            "m5-wice-primary-history-v1",
            hash_field(adapter.manifest.manifest_hash),
            text_field(claim.claim_id),
        )
        history_source_ids = tuple(sorted({item.source_version_id for item in units}))
        initial = HistoryInitialState(
            units=tuple(units),
            groups=(group,),
            active_unit_ids=tuple(sorted(unit_ids)),
            active_source_version_ids=history_source_ids,
            active_group_ids=(group.group_id,),
        )
        operations = [HistoryOperation(HistoryOperationKind.INITIALIZE)]
        operations.extend(
            HistoryOperation(HistoryOperationKind.UNIT_DELETE, unit_id=unit_id)
            for unit_id in sorted(unit_ids)
        )
        operations.extend(
            HistoryOperation(
                HistoryOperationKind.SOURCE_DELETE, source_version_id=source_id
            )
            for source_id in history_source_ids
        )
        events = tuple(
            build_evaluation_event(history_id, ordinal, operation)
            for ordinal, operation in enumerate(operations)
        )
        exact = exact_by_group[group.group_id]
        degree_by_hash = Counter(
            unit_by_id[unit_id].text_hash
            for requirement in requirements
            for unit_id in requirement.witness_unit_ids
        )
        histories.append(
            ControlledHistory(
                history_id=history_id,
                cluster_id=claim.source_document_id,
                cohort_kind=CohortKind.WICE_PRIMARY,
                tags=("retrospective", "deletion", "source_loss"),
                initial=initial,
                events=events,
                stratum=HistoryStratum(
                    requirement_count=len(requirements),
                    maximum_requirement_degree=max(
                        len(requirement.witness_unit_ids)
                        for requirement in requirements
                    ),
                    has_content_overlap=any(
                        value > 1 for value in degree_by_hash.values()
                    ),
                    initial_sdr_complete=exact.groundloop_sdr_complete,
                    has_alternative_assignment=exact.perfect_matching_count > 1,
                    provenance=(
                        f"wice:{adapter.manifest.official_commit}:{claim.split.value}"
                    ),
                ),
            )
        )
    return tuple(histories)


def history_event_identity_digest(histories: tuple[ControlledHistory, ...]) -> str:
    """Bind the ordered IDs/hashes used by all applicable strategies."""

    return stable_m5_digest(
        "m5-evaluation-history-set-v1",
        sequence_field(
            sequence_field(
                (
                    text_field(history.history_id),
                    sequence_field(
                        sequence_field(
                            (text_field(event.event_id), hash_field(event.event_hash))
                        )
                        for event in history.events
                    ),
                )
            )
            for history in histories
        ),
    )


__all__ = [
    "CohortKind",
    "ControlledHistory",
    "EvaluationEvent",
    "EvaluationGroup",
    "EvaluationRequirement",
    "EvaluationUnit",
    "HistoryInitialState",
    "HistoryOperation",
    "HistoryOperationKind",
    "HistoryStratum",
    "build_authored_controlled_histories",
    "build_evaluation_event",
    "build_wice_primary_histories",
    "history_event_identity_digest",
]
