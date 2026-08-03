"""Historical in-memory sidecar repository for the frozen M5 semantics."""

from __future__ import annotations

import math
from dataclasses import dataclass, field, fields, replace

from groundloop.domain import (
    ModelStamp,
    ObservationKey,
    SemanticObservation,
    StatusDelta,
    SubjectKind,
)
from groundloop.errors import (
    DanglingReferenceError,
    DuplicateIdentifierError,
    ValidationError,
)
from groundloop.m5.domain import (
    EvidenceGroupFamily,
    EvidenceGroupFamilyRetirement,
    EvidenceGroupValidity,
    EvidenceGroupVersion,
    EvidenceRequirementVersion,
    ObservationCurrencyInterval,
    RequirementObservationRecord,
    SnapshotPoint,
)
from groundloop.repository import InMemoryRepository, RepositorySnapshot

_HEX = frozenset("0123456789abcdef")


def _validate_typed_requirement_observation(observation: SemanticObservation) -> None:
    if not isinstance(observation, SemanticObservation):
        raise ValidationError("requirement observation has the wrong record type")
    for name, value in (
        ("observation_id", observation.observation_id),
        ("subject_id", observation.subject_id),
        ("chunk_version_id", observation.chunk_version_id),
        ("task_type", observation.task_type),
    ):
        if not isinstance(value, str) or not value.strip():
            raise ValidationError(f"{name} must be a nonempty identifier")
    if observation.subject_kind is not SubjectKind.REQUIREMENT:
        raise ValidationError("ObserveRequirement requires a REQUIREMENT subject")
    if not isinstance(observation.producer, ModelStamp):
        raise ValidationError("observation producer must be a ModelStamp")
    for name, value in (
        ("producer.model_id", observation.producer.model_id),
        ("producer.model_version", observation.producer.model_version),
        ("producer.prompt_version", observation.producer.prompt_version),
    ):
        if not isinstance(value, str) or not value.strip():
            raise ValidationError(f"{name} must be a nonempty identifier")
    for name, score in (
        ("support_score", observation.support_score),
        ("refute_score", observation.refute_score),
        ("neutral_score", observation.neutral_score),
    ):
        if (
            isinstance(score, bool)
            or not isinstance(score, (int, float))
            or not math.isfinite(score)
            or not 0.0 <= score <= 1.0
        ):
            raise ValidationError(f"{name} must be a finite F64 in [0, 1]")
    if (
        not isinstance(observation.input_hash, str)
        or len(observation.input_hash) != 64
        or any(character not in _HEX for character in observation.input_hash)
    ):
        raise ValidationError("input_hash must be a lowercase SHA-256 digest")


@dataclass(frozen=True, slots=True)
class M5RepositorySnapshot:
    base: RepositorySnapshot
    semantic_revision: int
    max_revision_by_epoch: tuple[tuple[int, int], ...]
    families: tuple[EvidenceGroupFamily, ...]
    groups: tuple[EvidenceGroupVersion, ...]
    group_validity: tuple[EvidenceGroupValidity, ...]
    retirements: tuple[EvidenceGroupFamilyRetirement, ...]
    observations: tuple[RequirementObservationRecord, ...]
    currency_history: tuple[ObservationCurrencyInterval, ...]
    processed_events: tuple[tuple[str, str, tuple[StatusDelta, ...]], ...]
    status_deltas: tuple[StatusDelta, ...]


@dataclass(slots=True)
class M5Repository:
    """M5-only state composed with an unchanged M1--M4 base repository.

    M5 structural events advance the base repository's synchronous event epoch
    so chunk, policy and group intervals share one ordering.  Requirement
    observations and currency live only in this sidecar; legacy claim
    observation semantics remain owned by ``InMemoryRepository``.
    """

    base: InMemoryRepository
    semantic_revision: int = 0
    _max_revision_by_epoch: dict[int, int] = field(default_factory=dict)

    _families: dict[str, EvidenceGroupFamily] = field(default_factory=dict)
    _groups: dict[str, EvidenceGroupVersion] = field(default_factory=dict)
    _group_validity: dict[str, EvidenceGroupValidity] = field(default_factory=dict)
    _group_versions_by_family: dict[str, list[str]] = field(default_factory=dict)
    _groups_by_claim: dict[str, list[str]] = field(default_factory=dict)
    _requirements: dict[str, EvidenceRequirementVersion] = field(default_factory=dict)
    _requirements_by_group: dict[str, tuple[str, ...]] = field(default_factory=dict)
    _retirements: dict[str, EvidenceGroupFamilyRetirement] = field(default_factory=dict)

    _requirement_observations: dict[str, RequirementObservationRecord] = field(
        default_factory=dict
    )
    _currency_history: dict[ObservationKey, list[ObservationCurrencyInterval]] = field(
        default_factory=dict
    )
    _observations_by_chunk: dict[str, list[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for epoch_id in range(self.base.current_epoch + 1):
            self._max_revision_by_epoch.setdefault(epoch_id, 0)
        self._max_revision_by_epoch[self.base.current_epoch] = max(
            self._max_revision_by_epoch[self.base.current_epoch],
            self.semantic_revision,
        )

    @property
    def current_epoch(self) -> int:
        return self.base.current_epoch

    @property
    def current_point(self) -> SnapshotPoint:
        self._synchronize_base_head()
        return SnapshotPoint(self.current_epoch, self.semantic_revision)

    def _synchronize_base_head(self) -> None:
        # Legacy M1--M4 events may still be applied through the preserved base
        # alias during M5.1.  Observe a newly committed base epoch as revision
        # zero so reference reads remain coherent.  M5.4 replaces this bridge
        # with the typed activation/runtime coordinator.
        if self.current_epoch not in self._max_revision_by_epoch:
            last_known_epoch = max(self._max_revision_by_epoch, default=-1)
            for epoch_id in range(last_known_epoch + 1, self.current_epoch + 1):
                self._max_revision_by_epoch[epoch_id] = 0
            self.semantic_revision = 0

    def begin_event_epoch(self) -> SnapshotPoint:
        self._synchronize_base_head()
        epoch_id = self.base.advance_epoch()
        self.semantic_revision = 0
        self._max_revision_by_epoch[epoch_id] = 0
        return SnapshotPoint(epoch_id, 0)

    def advance_semantic_revision(self) -> SnapshotPoint:
        _ = self.current_point
        self.semantic_revision += 1
        self._max_revision_by_epoch[self.current_epoch] = self.semantic_revision
        return self.current_point

    def is_known_snapshot_point(self, point: SnapshotPoint) -> bool:
        self._synchronize_base_head()
        maximum = self._max_revision_by_epoch.get(point.epoch_id)
        return maximum is not None and point.revision <= maximum

    def require_snapshot_point(self, point: SnapshotPoint) -> None:
        if not self.is_known_snapshot_point(point):
            raise ValidationError(
                f"snapshot ({point.epoch_id}, {point.revision}) does not exist"
            )

    def replace_with(self, staged: M5Repository) -> None:
        # Preserve the identity of the caller-owned base repository.  Code that
        # constructed ``M5Repository(base)`` may still hold ``base``; silently
        # replacing that object would make subsequent legacy events diverge.
        self.base.replace_with(staged.base)
        for repository_field in fields(self):
            if repository_field.name == "base":
                continue
            setattr(self, repository_field.name, getattr(staged, repository_field.name))

    def export_snapshot(self) -> M5RepositorySnapshot:
        _ = self.current_point
        base_snapshot = self.base.export_snapshot()
        return M5RepositorySnapshot(
            base=base_snapshot,
            semantic_revision=self.semantic_revision,
            max_revision_by_epoch=tuple(sorted(self._max_revision_by_epoch.items())),
            families=tuple(self._families[key] for key in sorted(self._families)),
            groups=tuple(self._groups[key] for key in sorted(self._groups)),
            group_validity=tuple(
                self._group_validity[key] for key in sorted(self._group_validity)
            ),
            retirements=tuple(
                self._retirements[key] for key in sorted(self._retirements)
            ),
            observations=tuple(
                self._requirement_observations[key]
                for key in sorted(self._requirement_observations)
            ),
            currency_history=tuple(
                interval
                for key in sorted(self._currency_history, key=_observation_key_sort)
                for interval in self._currency_history[key]
            ),
            processed_events=base_snapshot.processed_events,
            status_deltas=base_snapshot.status_deltas,
        )

    # ---------------------------------------------------------- group writes

    def register_group(self, group: EvidenceGroupVersion, epoch_id: int) -> None:
        if group.supersedes_group_version_id is not None:
            raise ValidationError("an initial group version cannot name a predecessor")
        if group.group_family_id in self._families:
            raise DuplicateIdentifierError(
                f"group family {group.group_family_id} already exists"
            )
        self.base.claim(group.owner_claim_id)
        self._validate_new_version_identifiers(group)
        self._validate_no_active_semantic_duplicate(group, epoch_id)
        family = EvidenceGroupFamily(
            group_family_id=group.group_family_id,
            claim_id=group.owner_claim_id,
            created_epoch=epoch_id,
        )
        self._families[family.group_family_id] = family
        self._group_versions_by_family[family.group_family_id] = []
        self._groups_by_claim.setdefault(group.owner_claim_id, [])
        self._append_group_version(group, epoch_id)

    def replace_group(
        self,
        old_group_version_id: str,
        successor: EvidenceGroupVersion,
        epoch_id: int,
    ) -> None:
        old = self.group(old_group_version_id)
        if not self.is_group_active(old_group_version_id, epoch_id):
            raise ValidationError(f"group {old_group_version_id} is not active")
        family = self.family(old.group_family_id)
        if family.group_family_id in self._retirements:
            raise ValidationError(f"group family {family.group_family_id} is retired")
        if successor.group_family_id != family.group_family_id:
            raise ValidationError("a successor must remain in the same group family")
        if successor.owner_claim_id != family.claim_id:
            raise ValidationError("a successor cannot change its owner claim")
        if successor.supersedes_group_version_id != old_group_version_id:
            raise ValidationError("a successor must name the active predecessor")
        if (
            self._group_versions_by_family[family.group_family_id][-1]
            != old_group_version_id
        ):
            raise ValidationError("a successor must follow the latest family version")
        self._validate_new_version_identifiers(successor)

        old_requirement_ids = set(self._requirements_by_group[old_group_version_id])
        used_predecessors: set[str] = set()
        for requirement in successor.requirements:
            predecessor = requirement.supersedes_requirement_version_id
            if predecessor is None:
                continue
            if predecessor not in old_requirement_ids:
                raise ValidationError(
                    "a requirement predecessor must belong to the prior group version"
                )
            if predecessor in used_predecessors:
                raise ValidationError("a requirement predecessor cannot be reused")
            used_predecessors.add(predecessor)

        self._validate_no_active_semantic_duplicate(
            successor,
            epoch_id,
            excluding_group_id=old_group_version_id,
        )
        prior_validity = self._group_validity[old_group_version_id]
        self._group_validity[old_group_version_id] = replace(
            prior_validity, valid_to_epoch=epoch_id
        )
        self._append_group_version(successor, epoch_id)

    def retire_group(self, group_version_id: str, epoch_id: int, event_id: str) -> None:
        group = self.group(group_version_id)
        if not self.is_group_active(group_version_id, epoch_id):
            raise ValidationError(f"group {group_version_id} is not active")
        if group.group_family_id in self._retirements:
            raise ValidationError(f"group family {group.group_family_id} is retired")
        prior_validity = self._group_validity[group_version_id]
        self._group_validity[group_version_id] = replace(
            prior_validity, valid_to_epoch=epoch_id
        )
        self._retirements[group.group_family_id] = EvidenceGroupFamilyRetirement(
            group_family_id=group.group_family_id,
            retired_epoch_id=epoch_id,
            event_id=event_id,
        )

    def _append_group_version(self, group: EvidenceGroupVersion, epoch_id: int) -> None:
        self._groups[group.group_version_id] = group
        self._group_validity[group.group_version_id] = EvidenceGroupValidity(
            group_version_id=group.group_version_id,
            valid_from_epoch=epoch_id,
        )
        self._group_versions_by_family[group.group_family_id].append(
            group.group_version_id
        )
        self._groups_by_claim[group.owner_claim_id].append(group.group_version_id)
        requirement_ids = tuple(
            requirement.requirement_version_id for requirement in group.requirements
        )
        self._requirements_by_group[group.group_version_id] = requirement_ids
        for requirement in group.requirements:
            self._requirements[requirement.requirement_version_id] = requirement

    def _validate_new_version_identifiers(self, group: EvidenceGroupVersion) -> None:
        if group.group_version_id in self._groups:
            raise DuplicateIdentifierError(
                f"group version {group.group_version_id} already exists"
            )
        for requirement in group.requirements:
            if requirement.requirement_version_id in self._requirements:
                raise DuplicateIdentifierError(
                    f"requirement {requirement.requirement_version_id} already exists"
                )

    def _validate_no_active_semantic_duplicate(
        self,
        group: EvidenceGroupVersion,
        epoch_id: int,
        excluding_group_id: str | None = None,
    ) -> None:
        for existing_id in self._groups_by_claim.get(group.owner_claim_id, []):
            if existing_id == excluding_group_id:
                continue
            existing = self._groups[existing_id]
            if (
                existing.semantic_structure_hash == group.semantic_structure_hash
                and self.is_group_active(existing_id, epoch_id)
            ):
                raise ValidationError(
                    "an equivalent evidence-group structure is already active"
                )

    # ---------------------------------------------------- observation writes

    def register_requirement_observation(
        self,
        observation: SemanticObservation,
        point: SnapshotPoint,
    ) -> RequirementObservationRecord:
        _validate_typed_requirement_observation(observation)
        if point != self.current_point or not self.is_known_snapshot_point(point):
            raise ValidationError(
                "requirement observations must be written at the current snapshot"
            )
        if observation.observation_id in self._requirement_observations:
            raise DuplicateIdentifierError(
                f"observation {observation.observation_id} already registered"
            )
        if self.base.has_observation_id(observation.observation_id):
            raise DuplicateIdentifierError(
                f"observation {observation.observation_id} already registered"
            )
        self.requirement(observation.subject_id)
        if not self.base.has_chunk_version(observation.chunk_version_id):
            raise DanglingReferenceError(
                f"observation references missing chunk {observation.chunk_version_id}"
            )
        eligible = self.is_requirement_active(
            observation.subject_id, point.epoch_id
        ) and self.is_chunk_active_at(observation.chunk_version_id, point.epoch_id)
        history = self._currency_history.get(observation.key)
        if (
            eligible
            and history
            and history[-1].valid_to is None
            and point <= history[-1].valid_from
        ):
            raise ValidationError("currency revisions must advance monotonically")
        record = RequirementObservationRecord(
            observation=observation,
            eligible_for_currency=eligible,
            produced_at=point,
        )
        # Reserve the global ID before mutating either sidecar index. All
        # validation that can reject the write has completed above.
        self.base.reserve_external_observation_id(observation.observation_id)
        self._requirement_observations[observation.observation_id] = record
        self._observations_by_chunk.setdefault(observation.chunk_version_id, []).append(
            observation.observation_id
        )
        if eligible:
            history = self._currency_history.setdefault(observation.key, [])
            if history and history[-1].valid_to is None:
                history[-1] = replace(history[-1], valid_to=point)
            history.append(
                ObservationCurrencyInterval(
                    key=observation.key,
                    observation_id=observation.observation_id,
                    valid_from=point,
                )
            )
        return record

    # --------------------------------------------------------------- queries

    def family(self, group_family_id: str) -> EvidenceGroupFamily:
        try:
            return self._families[group_family_id]
        except KeyError as exc:
            raise DanglingReferenceError(
                f"group family {group_family_id} does not exist"
            ) from exc

    def group(self, group_version_id: str) -> EvidenceGroupVersion:
        try:
            return self._groups[group_version_id]
        except KeyError as exc:
            raise DanglingReferenceError(
                f"group version {group_version_id} does not exist"
            ) from exc

    def requirement(self, requirement_version_id: str) -> EvidenceRequirementVersion:
        try:
            return self._requirements[requirement_version_id]
        except KeyError as exc:
            raise DanglingReferenceError(
                f"requirement {requirement_version_id} does not exist"
            ) from exc

    def all_group_ids(self) -> tuple[str, ...]:
        return tuple(self._groups)

    def all_requirement_ids(self) -> tuple[str, ...]:
        return tuple(self._requirements)

    def group_ids_for_claim(self, claim_id: str) -> tuple[str, ...]:
        return tuple(self._groups_by_claim.get(claim_id, ()))

    def requirement_ids_for_group(self, group_version_id: str) -> tuple[str, ...]:
        self.group(group_version_id)
        return self._requirements_by_group[group_version_id]

    def owner_claim_id(self, requirement_version_id: str) -> str:
        requirement = self.requirement(requirement_version_id)
        return self.group(requirement.group_version_id).owner_claim_id

    def is_group_active(
        self, group_version_id: str, epoch_id: int | None = None
    ) -> bool:
        validity = self._group_validity.get(group_version_id)
        point_epoch = self.current_epoch if epoch_id is None else epoch_id
        return validity is not None and validity.contains(point_epoch)

    def is_requirement_active(
        self, requirement_version_id: str, epoch_id: int | None = None
    ) -> bool:
        requirement = self.requirement(requirement_version_id)
        return self.is_group_active(requirement.group_version_id, epoch_id)

    def active_group_ids(self, epoch_id: int | None = None) -> tuple[str, ...]:
        return tuple(
            group_id
            for group_id in self._groups
            if self.is_group_active(group_id, epoch_id)
        )

    def is_chunk_active_at(self, chunk_version_id: str, epoch_id: int) -> bool:
        for chunk, validity in self.base.export_snapshot().chunks:
            if chunk.chunk_version_id == chunk_version_id:
                return validity[0] <= epoch_id and (
                    validity[1] is None or epoch_id < validity[1]
                )
        return False

    def policy_at(self, epoch_id: int):  # type: ignore[no-untyped-def]
        for policy, validity in self.base.export_snapshot().policies:
            if validity[0] <= epoch_id and (
                validity[1] is None or epoch_id < validity[1]
            ):
                return policy
        raise DanglingReferenceError(
            f"no decision policy is active at epoch {epoch_id}"
        )

    def requirement_observation(
        self, observation_id: str
    ) -> RequirementObservationRecord:
        try:
            return self._requirement_observations[observation_id]
        except KeyError as exc:
            raise DanglingReferenceError(
                f"requirement observation {observation_id} does not exist"
            ) from exc

    def currency_observation_id_at(
        self, key: ObservationKey, point: SnapshotPoint
    ) -> str | None:
        self.require_snapshot_point(point)
        for interval in reversed(self._currency_history.get(key, ())):
            if interval.contains(point):
                return interval.observation_id
        return None

    def current_requirement_observation_ids(
        self, point: SnapshotPoint | None = None
    ) -> tuple[str, ...]:
        snapshot = self.current_point if point is None else point
        holders = (
            self.currency_observation_id_at(key, snapshot)
            for key in self._currency_history
        )
        return tuple(sorted(holder for holder in holders if holder is not None))

    def current_requirement_observations(
        self, point: SnapshotPoint | None = None
    ) -> tuple[RequirementObservationRecord, ...]:
        return tuple(
            self._requirement_observations[observation_id]
            for observation_id in self.current_requirement_observation_ids(point)
        )

    def currency_history(
        self, key: ObservationKey
    ) -> tuple[ObservationCurrencyInterval, ...]:
        return tuple(self._currency_history.get(key, ()))

    def recorded_event(
        self, event_id: str
    ) -> tuple[str, tuple[StatusDelta, ...]] | None:
        # M5 and legacy mutations share one global idempotency namespace.
        return self.base.recorded_event(event_id)

    def record_event(
        self, event_id: str, payload_hash: str, deltas: tuple[StatusDelta, ...]
    ) -> None:
        self.base.record_event(event_id, payload_hash, deltas)


def _observation_key_sort(key: ObservationKey) -> tuple[str, str, str, str]:
    return (key[0].value, key[1], key[2], key[3])


__all__ = ["M5Repository", "M5RepositorySnapshot"]
