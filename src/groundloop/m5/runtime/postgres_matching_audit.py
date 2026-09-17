"""Read-only D25 actual-image and provenance audit helpers.

The caller owns the exported ``REPEATABLE READ`` snapshot and the final head
recheck.  This module is intentionally an actual-image adapter and integrity
replayer: it never supplies rows to either independent semantic oracle and it
never opens, commits, or rolls back a transaction.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, TypeAlias, cast

from psycopg import Cursor

from groundloop.errors import ValidationError
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    MATCHING_WORK_COUNTER_NAMES,
    M5MatchingAuditError,
    M5MatchingAuditFamily,
    M5MatchingEdgeCurrent,
    M5MatchingEdgeWorking,
    M5MatchingEpochStatus,
    M5MatchingHallCurrent,
    M5MatchingHallWorking,
    M5MatchingLayer,
    M5MatchingMaskCurrent,
    M5MatchingMaskWorking,
    M5MatchingObservationCurrent,
    M5MatchingObservationWorking,
    M5MatchingProvenanceKind,
    M5PersistedMatchingPhysicalAudit,
    M5PersistedMatchingPhysicalMismatch,
    M5PersistedMatchingProvenanceMismatch,
)

ObservationRow: TypeAlias = tuple[str, str, str, int, str]
EdgeRow: TypeAlias = tuple[str, str, str, int, int]
MaskRow: TypeAlias = tuple[str, str, int]
HallRow: TypeAlias = tuple[
    str,
    int,
    tuple[int, ...],
    tuple[int, ...],
    tuple[int, ...],
    int,
    int,
    int,
]
AuditRow: TypeAlias = ObservationRow | EdgeRow | MaskRow | HallRow
AuditKey: TypeAlias = tuple[str, ...]
CurrentPoint: TypeAlias = (
    M5MatchingObservationCurrent
    | M5MatchingEdgeCurrent
    | M5MatchingMaskCurrent
    | M5MatchingHallCurrent
)
WorkingPoint: TypeAlias = (
    M5MatchingObservationWorking
    | M5MatchingEdgeWorking
    | M5MatchingMaskWorking
    | M5MatchingHallWorking
)

_FAMILIES = (
    M5MatchingAuditFamily.OBSERVATION,
    M5MatchingAuditFamily.EDGE,
    M5MatchingAuditFamily.MASK,
    M5MatchingAuditFamily.HALL,
)
_FAMILY_RANK = {family: rank for rank, family in enumerate(_FAMILIES)}
_HEX = frozenset("0123456789abcdef")


class M5MatchingAuditInvalidError(ValidationError):
    """The snapshot cannot produce a typed D25 audit artifact."""


def _text(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise M5MatchingAuditInvalidError(f"{name} must be nonempty text")
    return value


def _hash(value: object, *, name: str) -> str:
    result = _text(str(value).strip(), name=name)
    if len(result) != 64 or any(character not in _HEX for character in result):
        raise M5MatchingAuditInvalidError(f"{name} is not canonical SHA-256")
    return result


def _integer(value: object, *, name: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise M5MatchingAuditInvalidError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise M5MatchingAuditInvalidError(f"{name} is outside its accepted range")
    return value


def _integer_tuple(value: object, *, name: str) -> tuple[int, ...]:
    if not isinstance(value, (list, tuple)):
        raise M5MatchingAuditInvalidError(f"{name} must be an integer sequence")
    return tuple(_integer(item, name=f"{name} entry") for item in value)


def _boolean(value: object, *, name: str) -> bool:
    if type(value) is not bool:
        raise M5MatchingAuditInvalidError(f"{name} must be a boolean")
    return value


def _audit_fields(family: M5MatchingAuditFamily, row: AuditRow) -> tuple[str, ...]:
    if family is M5MatchingAuditFamily.OBSERVATION:
        observation = cast(ObservationRow, row)
        return digests.audit_observation_fields(*observation)
    if family is M5MatchingAuditFamily.EDGE:
        edge = cast(EdgeRow, row)
        return digests.audit_edge_fields(*edge)
    if family is M5MatchingAuditFamily.MASK:
        mask = cast(MaskRow, row)
        return digests.audit_mask_fields(*mask)
    hall = cast(HallRow, row)
    return digests.audit_hall_fields(*hall)


def _audit_key(family: M5MatchingAuditFamily, row: AuditRow) -> AuditKey:
    if family in {M5MatchingAuditFamily.EDGE, M5MatchingAuditFamily.MASK}:
        return (str(row[0]), str(row[1]))
    return (str(row[0]),)


@dataclass(frozen=True, slots=True)
class M5MatchingAuditProjection:
    """One complete canonical semantic projection from one audit reader."""

    head_epoch_id: int
    head_revision: int
    decision_policy_version: str
    observations: tuple[ObservationRow, ...]
    edges: tuple[EdgeRow, ...]
    masks: tuple[MaskRow, ...]
    halls: tuple[HallRow, ...]

    def __post_init__(self) -> None:
        _integer(self.head_epoch_id, name="head_epoch_id", minimum=1)
        _integer(self.head_revision, name="head_revision", minimum=0)
        _text(self.decision_policy_version, name="decision_policy_version")
        for family, rows in self.family_rows:
            keys: list[AuditKey] = []
            for row in rows:
                try:
                    _current_point(
                        family,
                        (*row, self.head_epoch_id, self.head_revision),
                    )
                    fields = _audit_fields(family, row)
                    digests.physical_audit_row_digest(family, fields)
                except (TypeError, ValueError, ValidationError) as exc:
                    raise M5MatchingAuditInvalidError(
                        f"expected {family.value} row is malformed"
                    ) from exc
                keys.append(_audit_key(family, row))
            if keys != sorted(keys) or len(keys) != len(set(keys)):
                raise M5MatchingAuditInvalidError(
                    f"expected {family.value} rows are not key-sorted unique"
                )

    @property
    def family_rows(
        self,
    ) -> tuple[tuple[M5MatchingAuditFamily, tuple[AuditRow, ...]], ...]:
        return (
            (M5MatchingAuditFamily.OBSERVATION, self.observations),
            (M5MatchingAuditFamily.EDGE, self.edges),
            (M5MatchingAuditFamily.MASK, self.masks),
            (M5MatchingAuditFamily.HALL, self.halls),
        )

    @property
    def row_maps(
        self,
    ) -> dict[M5MatchingAuditFamily, dict[AuditKey, tuple[str, ...]]]:
        return {
            family: {
                _audit_key(family, row): _audit_fields(family, row) for row in rows
            }
            for family, rows in self.family_rows
        }

    @property
    def projection_digest(self) -> str:
        family_digests = tuple(
            digests.physical_audit_family_digest(
                family, tuple(_audit_fields(family, row) for row in rows)
            )
            for family, rows in self.family_rows
        )
        return digests.physical_audit_projection_digest(
            self.head_epoch_id,
            self.head_revision,
            self.decision_policy_version,
            *family_digests,
        )


@dataclass(frozen=True, slots=True)
class _ActualImage:
    projection: M5MatchingAuditProjection | None
    points: dict[M5MatchingAuditFamily, dict[AuditKey, CurrentPoint]]
    malformed: tuple[M5PersistedMatchingPhysicalMismatch, ...]


def _current_audit_row(family: M5MatchingAuditFamily, row: tuple[Any, ...]) -> AuditRow:
    if family is M5MatchingAuditFamily.OBSERVATION:
        return (
            _text(row[0], name="observation_id"),
            _text(row[1], name="requirement_version_id"),
            _text(row[2], name="group_version_id"),
            _integer(row[3], name="requirement_ordinal", minimum=0),
            _hash(row[4], name="text_hash"),
        )
    if family is M5MatchingAuditFamily.EDGE:
        return (
            _text(row[0], name="requirement_version_id"),
            _hash(row[1], name="text_hash"),
            _text(row[2], name="group_version_id"),
            _integer(row[3], name="requirement_ordinal", minimum=0),
            _integer(row[4], name="refcount", minimum=1),
        )
    if family is M5MatchingAuditFamily.MASK:
        return (
            _text(row[0], name="group_version_id"),
            _hash(row[1], name="text_hash"),
            _integer(row[2], name="mask", minimum=1),
        )
    return (
        _text(row[0], name="group_version_id"),
        _integer(row[1], name="requirement_count", minimum=1),
        _integer_tuple(row[2], name="mask_histogram"),
        _integer_tuple(row[3], name="neighbor_counts"),
        _integer_tuple(row[4], name="deficiencies"),
        _integer(row[5], name="maximum_deficiency", minimum=0),
        _integer(row[6], name="matching_size", minimum=0),
        _integer(row[7], name="distinct_hash_count", minimum=0),
    )


def _current_point(
    family: M5MatchingAuditFamily, row: tuple[Any, ...]
) -> tuple[AuditRow, CurrentPoint]:
    audit_row = _current_audit_row(family, row)
    if family is M5MatchingAuditFamily.OBSERVATION:
        point: CurrentPoint = M5MatchingObservationCurrent(
            M5MatchingLayer.CURRENT,
            *cast(ObservationRow, audit_row),
            _integer(row[5], name="installed_epoch_id", minimum=1),
            _integer(row[6], name="installed_revision", minimum=0),
        )
    elif family is M5MatchingAuditFamily.EDGE:
        point = M5MatchingEdgeCurrent(
            M5MatchingLayer.CURRENT,
            *cast(EdgeRow, audit_row),
            _integer(row[5], name="installed_epoch_id", minimum=1),
            _integer(row[6], name="installed_revision", minimum=0),
        )
    elif family is M5MatchingAuditFamily.MASK:
        point = M5MatchingMaskCurrent(
            M5MatchingLayer.CURRENT,
            *cast(MaskRow, audit_row),
            _integer(row[3], name="installed_epoch_id", minimum=1),
            _integer(row[4], name="installed_revision", minimum=0),
        )
    else:
        point = M5MatchingHallCurrent(
            M5MatchingLayer.CURRENT,
            *cast(HallRow, audit_row),
            _integer(row[8], name="installed_epoch_id", minimum=1),
            _integer(row[9], name="installed_revision", minimum=0),
        )
    return audit_row, point


def _current_queries() -> tuple[tuple[M5MatchingAuditFamily, str], ...]:
    return (
        (
            M5MatchingAuditFamily.OBSERVATION,
            """SELECT observation_id, requirement_version_id, group_version_id,
                      requirement_ordinal, text_hash::text,
                      installed_epoch_id, installed_revision
                 FROM groundloop_m5_matching_observation_current
                ORDER BY observation_id COLLATE \"C\"""",
        ),
        (
            M5MatchingAuditFamily.EDGE,
            """SELECT requirement_version_id, text_hash::text,
                      group_version_id, requirement_ordinal, refcount,
                      installed_epoch_id, installed_revision
                 FROM groundloop_m5_matching_edge_current
                ORDER BY requirement_version_id COLLATE \"C\", text_hash""",
        ),
        (
            M5MatchingAuditFamily.MASK,
            """SELECT group_version_id, text_hash::text, mask,
                      installed_epoch_id, installed_revision
                 FROM groundloop_m5_matching_hash_mask_current
                ORDER BY group_version_id COLLATE \"C\", text_hash""",
        ),
        (
            M5MatchingAuditFamily.HALL,
            """SELECT group_version_id, requirement_count, mask_histogram,
                      neighbor_counts, deficiencies, maximum_deficiency,
                      matching_size, distinct_hash_count,
                      installed_epoch_id, installed_revision
                 FROM groundloop_m5_matching_hall_current
                ORDER BY group_version_id COLLATE \"C\"""",
        ),
    )


def _read_actual_image(
    cursor: Cursor[Any],
    *,
    expected: M5MatchingAuditProjection,
) -> _ActualImage:
    headers = cursor.execute(
        """
        SELECT m4.epoch_id, m5.epoch_id, m5.sealed_revision,
               epoch.revision, epoch.structural_status, epoch.semantic_status,
               epoch.evaluation_state, epoch.publication_mode,
               epoch.sealed_at IS NOT NULL,
               image.decision_policy_version, image.installed_epoch_id,
               image.installed_revision,
               (SELECT count(*) FROM groundloop_decision_policy AS active_policy
                 WHERE active_policy.valid_from_epoch <= m5.epoch_id
                   AND (active_policy.valid_to_epoch IS NULL
                        OR m5.epoch_id < active_policy.valid_to_epoch))
        FROM groundloop_m4_publication_head AS m4
        JOIN groundloop_m5_publication_head AS m5 ON m5.singleton
        JOIN groundloop_epoch AS epoch ON epoch.epoch_id = m5.epoch_id
        JOIN groundloop_m5_matching_image_current AS image ON image.singleton
        JOIN groundloop_decision_policy AS policy
          ON policy.policy_version = image.decision_policy_version
         AND policy.valid_from_epoch <= m5.epoch_id
         AND (policy.valid_to_epoch IS NULL
              OR m5.epoch_id < policy.valid_to_epoch)
        WHERE m4.singleton
        """
    ).fetchall()
    active = [
        row
        for row in headers
        if int(row[12]) == 1
        and int(row[0]) == int(row[1]) == int(row[10]) == expected.head_epoch_id
        and int(row[2]) == int(row[3]) == int(row[11]) == expected.head_revision
        and str(row[9]) == expected.decision_policy_version
    ]
    if len(active) != 1 or tuple(active[0][4:9]) != (
        "committed",
        "sealed",
        "complete",
        "strict",
        True,
    ):
        raise M5MatchingAuditInvalidError(
            "actual-image header does not match the sealed target head"
        )

    points: dict[M5MatchingAuditFamily, dict[AuditKey, CurrentPoint]] = {
        family: {} for family in _FAMILIES
    }
    decoded: dict[M5MatchingAuditFamily, list[AuditRow]] = {
        family: [] for family in _FAMILIES
    }
    malformed: list[M5PersistedMatchingPhysicalMismatch] = []
    expected_maps = expected.row_maps
    for family, query in _current_queries():
        seen: set[AuditKey] = set()
        for raw in cursor.execute(query).fetchall():
            try:
                outer = (
                    (_text(raw[0], name=f"{family.value} outer key"),)
                    if family
                    in {
                        M5MatchingAuditFamily.OBSERVATION,
                        M5MatchingAuditFamily.HALL,
                    }
                    else (
                        _text(raw[0], name=f"{family.value} outer key"),
                        _hash(raw[1], name=f"{family.value} outer hash"),
                    )
                )
            except M5MatchingAuditInvalidError:
                raise
            if outer in seen:
                raise M5MatchingAuditInvalidError(
                    f"actual {family.value} outer keys are duplicated"
                )
            seen.add(outer)
            try:
                audit_row = _current_audit_row(family, tuple(raw))
                _current_point(family, (*audit_row, 1, 0))
                fields = _audit_fields(family, audit_row)
                digests.physical_audit_row_digest(family, fields)
            except (TypeError, ValueError, ValidationError):
                expected_fields = expected_maps[family].get(outer)
                malformed.append(
                    M5PersistedMatchingPhysicalMismatch(
                        family=family,
                        key=outer,
                        expected_row_digest=(
                            None
                            if expected_fields is None
                            else digests.physical_audit_row_digest(
                                family, expected_fields
                            )
                        ),
                        actual_row_digest=None,
                        actual_error=M5MatchingAuditError.MALFORMED_PAYLOAD,
                    )
                )
                continue
            try:
                _, point = _current_point(family, tuple(raw))
            except (TypeError, ValueError, ValidationError) as exc:
                raise M5MatchingAuditInvalidError(
                    "actual installed coordinates are malformed"
                ) from exc
            decoded[family].append(audit_row)
            points[family][outer] = point
    malformed_tuple = tuple(
        sorted(malformed, key=lambda item: (_FAMILY_RANK[item.family], item.key))
    )
    if malformed_tuple:
        return _ActualImage(None, points, malformed_tuple)
    projection = M5MatchingAuditProjection(
        head_epoch_id=expected.head_epoch_id,
        head_revision=expected.head_revision,
        decision_policy_version=expected.decision_policy_version,
        observations=tuple(cast(list[ObservationRow], decoded[_FAMILIES[0]])),
        edges=tuple(cast(list[EdgeRow], decoded[_FAMILIES[1]])),
        masks=tuple(cast(list[MaskRow], decoded[_FAMILIES[2]])),
        halls=tuple(cast(list[HallRow], decoded[_FAMILIES[3]])),
    )
    return _ActualImage(projection, points, ())


def _physical_mismatches(
    expected: M5MatchingAuditProjection,
    actual: M5MatchingAuditProjection,
) -> tuple[M5PersistedMatchingPhysicalMismatch, ...]:
    result: list[M5PersistedMatchingPhysicalMismatch] = []
    expected_maps = expected.row_maps
    actual_maps = actual.row_maps
    for family in _FAMILIES:
        for key in sorted(set(expected_maps[family]) | set(actual_maps[family])):
            expected_fields = expected_maps[family].get(key)
            actual_fields = actual_maps[family].get(key)
            expected_digest = (
                None
                if expected_fields is None
                else digests.physical_audit_row_digest(family, expected_fields)
            )
            actual_digest = (
                None
                if actual_fields is None
                else digests.physical_audit_row_digest(family, actual_fields)
            )
            if expected_digest != actual_digest:
                result.append(
                    M5PersistedMatchingPhysicalMismatch(
                        family=family,
                        key=key,
                        expected_row_digest=expected_digest,
                        actual_row_digest=actual_digest,
                        actual_error=None,
                    )
                )
    return tuple(result)


def _audit_digest(values: dict[str, Any]) -> str:
    return digests.physical_audit_digest(
        head_epoch_id=values["head_epoch_id"],
        head_revision=values["head_revision"],
        decision_policy_version=values["decision_policy_version"],
        python_expected_projection_digest=values["python_expected_projection_digest"],
        sql_expected_projection_digest=values["sql_expected_projection_digest"],
        actual_projection_digest=values["actual_projection_digest"],
        actual_error=values["actual_error"],
        provenance_ok=values["provenance_ok"],
        provenance_replay_digest=values["provenance_replay_digest"],
        working_image_expected_provenance_digest=values[
            "working_image_expected_provenance_digest"
        ],
        working_image_actual_provenance_digest=values[
            "working_image_actual_provenance_digest"
        ],
        accumulator_expected_provenance_digest=values[
            "accumulator_expected_provenance_digest"
        ],
        accumulator_actual_provenance_digest=values[
            "accumulator_actual_provenance_digest"
        ],
        mismatches=tuple(
            (
                mismatch.family,
                mismatch.key,
                mismatch.expected_row_digest,
                mismatch.actual_row_digest,
                mismatch.actual_error,
            )
            for mismatch in values["mismatches"]
        ),
        provenance_mismatches=tuple(
            (
                mismatch.kind,
                mismatch.epoch_id,
                mismatch.expected_row_digest,
                mismatch.actual_row_digest,
            )
            for mismatch in values["provenance_mismatches"]
        ),
    )


def _artifact(values: dict[str, Any]) -> M5PersistedMatchingPhysicalAudit:
    return M5PersistedMatchingPhysicalAudit(
        **values,
        audit_digest=_audit_digest(values),
    )


@dataclass(frozen=True, slots=True)
class _PatchRecord:
    values: dict[str, Any]
    group_shape_preimage: bytes
    observation_preimages: tuple[bytes, ...]
    edge_preimages: tuple[bytes, ...]
    mask_preimages: tuple[bytes, ...]
    hall_preimages: tuple[bytes, ...]
    logical_patch_preimage: bytes
    logical_output_preimage: bytes
    canonical_preimage: bytes


@dataclass(frozen=True, slots=True)
class _DecodedChange:
    family: M5MatchingAuditFamily
    key: AuditKey
    before: CurrentPoint | WorkingPoint | None
    after: WorkingPoint
    patch_digest: str


@dataclass(frozen=True, slots=True)
class _ProvenanceResult:
    ok: bool
    replay_digest: str
    working_expected_digest: str
    working_actual_digest: str
    accumulator_expected_digest: str
    accumulator_actual_digest: str
    mismatches: tuple[M5PersistedMatchingProvenanceMismatch, ...]


@dataclass(frozen=True, slots=True)
class _RuntimeAuthority:
    status: M5MatchingEpochStatus
    revision: int
    base_epoch_id: int
    base_revision: int
    decision_policy_version: str


def _typed_value(node: object) -> object:
    if not isinstance(node, dict):
        raise M5MatchingAuditInvalidError("typed point node is not an object")
    tag = node.get("tag")
    if tag == "null":
        return None
    if tag in {"text", "enum", "sha256"}:
        return _text(node.get("value"), name=f"typed {tag}")
    if tag == "int":
        value = node.get("value")
        if not isinstance(value, str):
            raise M5MatchingAuditInvalidError("typed integer lacks text")
        try:
            parsed = int(value)
        except ValueError as exc:
            raise M5MatchingAuditInvalidError("typed integer is invalid") from exc
        if str(parsed) != value:
            raise M5MatchingAuditInvalidError("typed integer is noncanonical")
        return parsed
    if tag == "bool":
        value = node.get("value")
        if value not in {"0", "1"}:
            raise M5MatchingAuditInvalidError("typed boolean is invalid")
        return value == "1"
    if tag == "sequence":
        children = node.get("children")
        if not isinstance(children, list):
            raise M5MatchingAuditInvalidError("typed sequence lacks children")
        return tuple(_typed_value(child) for child in children)
    raise M5MatchingAuditInvalidError("typed point uses an unsupported tag")


def _decode_point(
    family: M5MatchingAuditFamily, node: object
) -> CurrentPoint | WorkingPoint:
    values = _typed_value(node)
    if not isinstance(values, tuple) or not values:
        raise M5MatchingAuditInvalidError("matching point is not a sequence")
    layer = values[0]
    try:
        if family is M5MatchingAuditFamily.OBSERVATION:
            if layer == "current" and len(values) == 8:
                return M5MatchingObservationCurrent(
                    M5MatchingLayer.CURRENT,
                    cast(str, values[1]),
                    cast(str, values[2]),
                    cast(str, values[3]),
                    cast(int, values[4]),
                    cast(str, values[5]),
                    cast(int, values[6]),
                    cast(int, values[7]),
                )
            if layer == "working" and len(values) == 9:
                return M5MatchingObservationWorking(
                    M5MatchingLayer.WORKING,
                    cast(int, values[1]),
                    cast(str, values[2]),
                    cast(str, values[3]),
                    cast(str, values[4]),
                    cast(int, values[5]),
                    cast(str, values[6]),
                    cast(bool, values[7]),
                    cast(int, values[8]),
                )
        elif family is M5MatchingAuditFamily.EDGE:
            if layer == "current" and len(values) == 8:
                return M5MatchingEdgeCurrent(
                    M5MatchingLayer.CURRENT,
                    cast(str, values[1]),
                    cast(str, values[2]),
                    cast(str, values[3]),
                    cast(int, values[4]),
                    cast(int, values[5]),
                    cast(int, values[6]),
                    cast(int, values[7]),
                )
            if layer == "working" and len(values) == 8:
                return M5MatchingEdgeWorking(
                    M5MatchingLayer.WORKING,
                    cast(int, values[1]),
                    cast(str, values[2]),
                    cast(str, values[3]),
                    cast(str, values[4]),
                    cast(int, values[5]),
                    cast(int, values[6]),
                    cast(int, values[7]),
                )
        elif family is M5MatchingAuditFamily.MASK:
            if layer == "current" and len(values) == 6:
                return M5MatchingMaskCurrent(
                    M5MatchingLayer.CURRENT,
                    cast(str, values[1]),
                    cast(str, values[2]),
                    cast(int, values[3]),
                    cast(int, values[4]),
                    cast(int, values[5]),
                )
            if layer == "working" and len(values) == 6:
                return M5MatchingMaskWorking(
                    M5MatchingLayer.WORKING,
                    cast(int, values[1]),
                    cast(str, values[2]),
                    cast(str, values[3]),
                    cast(int, values[4]),
                    cast(int, values[5]),
                )
        elif layer == "current" and len(values) == 11:
            return M5MatchingHallCurrent(
                M5MatchingLayer.CURRENT,
                cast(str, values[1]),
                cast(int, values[2]),
                cast(tuple[int, ...], values[3]),
                cast(tuple[int, ...], values[4]),
                cast(tuple[int, ...], values[5]),
                cast(int, values[6]),
                cast(int, values[7]),
                cast(int, values[8]),
                cast(int, values[9]),
                cast(int, values[10]),
            )
        elif layer == "working" and len(values) == 12:
            return M5MatchingHallWorking(
                M5MatchingLayer.WORKING,
                cast(int, values[1]),
                cast(str, values[2]),
                cast(bool, values[3]),
                cast(int | None, values[4]),
                cast(tuple[int, ...] | None, values[5]),
                cast(tuple[int, ...] | None, values[6]),
                cast(tuple[int, ...] | None, values[7]),
                cast(int | None, values[8]),
                cast(int | None, values[9]),
                cast(int | None, values[10]),
                cast(int, values[11]),
            )
    except (TypeError, ValueError, ValidationError) as exc:
        raise M5MatchingAuditInvalidError("matching point is malformed") from exc
    raise M5MatchingAuditInvalidError("matching point shape is invalid")


def _point_key(family: M5MatchingAuditFamily, point: object) -> AuditKey:
    if family is M5MatchingAuditFamily.OBSERVATION:
        return (cast(Any, point).observation_id,)
    if family is M5MatchingAuditFamily.EDGE:
        return (cast(Any, point).requirement_version_id, cast(Any, point).text_hash)
    if family is M5MatchingAuditFamily.MASK:
        return (cast(Any, point).group_version_id, cast(Any, point).text_hash)
    return (cast(Any, point).group_version_id,)


def _load_patch_records(cursor: Cursor[Any]) -> tuple[_PatchRecord, ...]:
    result: list[_PatchRecord] = []
    rows = cursor.execute(
        """
        SELECT to_jsonb(patch), patch.group_shape_set_preimage,
               patch.observation_change_preimages,
               patch.edge_change_preimages, patch.mask_change_preimages,
               patch.hall_change_preimages,
               patch.logical_overlay_patch_preimage,
               patch.logical_output_preimage, patch.canonical_patch_preimage
        FROM groundloop_m5_matching_patch_artifact AS patch
        ORDER BY patch.resulting_epoch_id, patch.resulting_revision,
                 patch.source_kind COLLATE "C", patch.source_id COLLATE "C"
        """
    ).fetchall()
    for row in rows:
        if not isinstance(row[0], dict):
            raise M5MatchingAuditInvalidError("patch JSON projection is malformed")
        result.append(
            _PatchRecord(
                values=dict(row[0]),
                group_shape_preimage=bytes(row[1]),
                observation_preimages=tuple(bytes(value) for value in row[2]),
                edge_preimages=tuple(bytes(value) for value in row[3]),
                mask_preimages=tuple(bytes(value) for value in row[4]),
                hall_preimages=tuple(bytes(value) for value in row[5]),
                logical_patch_preimage=bytes(row[6]),
                logical_output_preimage=bytes(row[7]),
                canonical_preimage=bytes(row[8]),
            )
        )
    return tuple(result)


def _status(runtime_state: object) -> M5MatchingEpochStatus:
    if runtime_state == "sealed":
        return M5MatchingEpochStatus.SEALED
    if runtime_state == "failed":
        return M5MatchingEpochStatus.FAILED
    return M5MatchingEpochStatus.NONTERMINAL


def _decode_changes(
    cursor: Cursor[Any], patch: _PatchRecord
) -> tuple[_DecodedChange, ...]:
    values = patch.values
    families = (
        (
            M5MatchingAuditFamily.OBSERVATION,
            "observation_change_digests",
            patch.observation_preimages,
        ),
        (M5MatchingAuditFamily.EDGE, "edge_change_digests", patch.edge_preimages),
        (M5MatchingAuditFamily.MASK, "mask_change_digests", patch.mask_preimages),
        (M5MatchingAuditFamily.HALL, "hall_change_digests", patch.hall_preimages),
    )
    result: list[_DecodedChange] = []
    for family, digest_name, preimages in families:
        declared_raw = values.get(digest_name)
        if not isinstance(declared_raw, list) or len(declared_raw) != len(preimages):
            raise M5MatchingAuditInvalidError("patch child cardinality is invalid")
        prior_sort: tuple[object, ...] | None = None
        seen: set[AuditKey] = set()
        for declared_value, preimage in zip(declared_raw, preimages, strict=True):
            declared = _hash(declared_value, name="patch child digest")
            if hashlib.sha256(preimage).hexdigest() != declared:
                raise M5MatchingAuditInvalidError("patch child digest is invalid")
            decoded_row = cursor.execute(
                """
                WITH decoded AS (
                  SELECT groundloop_m5_matching_validate_change(%s,%s) AS value
                ), shapes AS (
                  SELECT groundloop_m5_matching_validate_group_shapes(%s) AS value
                )
                SELECT decoded.value
                FROM decoded, shapes
                WHERE groundloop_m5_matching_validate_patch_change_point(
                        decoded.value, %s, %s, %s, %s, %s)
                  AND groundloop_m5_matching_validate_change_shape(
                        decoded.value, shapes.value, %s)
                """,
                (
                    preimage,
                    family.value,
                    patch.group_shape_preimage,
                    values["source_kind"],
                    values["before_epoch_id"],
                    values["before_revision"],
                    values["resulting_epoch_id"],
                    values["resulting_revision"],
                    family.value,
                ),
            ).fetchone()
            if decoded_row is None or not isinstance(decoded_row[0], dict):
                raise M5MatchingAuditInvalidError("patch child did not decode")
            decoded = decoded_row[0]
            before_node = decoded.get("before")
            after_node = decoded.get("after")
            before = (
                None
                if before_node is None
                else _decode_point(family, cast(dict[str, object], before_node)["node"])
            )
            if after_node is None:
                raise M5MatchingAuditInvalidError("patch child has no after point")
            after = _decode_point(family, cast(dict[str, object], after_node)["node"])
            if not isinstance(
                after,
                (
                    M5MatchingObservationWorking,
                    M5MatchingEdgeWorking,
                    M5MatchingMaskWorking,
                    M5MatchingHallWorking,
                ),
            ):
                raise M5MatchingAuditInvalidError("patch after point is not working")
            key = _point_key(family, after)
            if key in seen:
                raise M5MatchingAuditInvalidError("patch child key is duplicated")
            seen.add(key)
            if family is M5MatchingAuditFamily.EDGE:
                sort_key: tuple[object, ...] = (
                    cast(M5MatchingEdgeWorking, after).group_version_id,
                    cast(M5MatchingEdgeWorking, after).requirement_ordinal,
                    key[1],
                    key[0],
                )
            else:
                sort_key = key
            if prior_sort is not None and sort_key <= prior_sort:
                raise M5MatchingAuditInvalidError("patch children are not sorted")
            prior_sort = sort_key
            result.append(
                _DecodedChange(
                    family=family,
                    key=key,
                    before=before,
                    after=after,
                    patch_digest=_hash(values["patch_digest"], name="patch digest"),
                )
            )
    return tuple(result)


def _validate_outer_patch(cursor: Cursor[Any], patch: _PatchRecord) -> bool:
    values = patch.values
    expected = digests.persisted_matching_patch_digest(
        source_kind=_text(values["source_kind"], name="source_kind"),
        source_id=_text(values["source_id"], name="source_id"),
        source_identity_hash=_hash(
            values["source_identity_hash"], name="source identity"
        ),
        before_epoch_id=_integer(
            values["before_epoch_id"], name="before_epoch_id", minimum=1
        ),
        before_revision=_integer(
            values["before_revision"], name="before_revision", minimum=0
        ),
        resulting_epoch_id=_integer(
            values["resulting_epoch_id"], name="resulting_epoch_id", minimum=1
        ),
        resulting_revision=_integer(
            values["resulting_revision"], name="resulting_revision", minimum=1
        ),
        decision_policy_version=_text(
            values["decision_policy_version"], name="decision policy"
        ),
        group_shape_set_digest=_hash(
            values["group_shape_set_digest"], name="group-shape digest"
        ),
        observation_change_digests=tuple(values["observation_change_digests"]),
        edge_change_digests=tuple(values["edge_change_digests"]),
        mask_change_digests=tuple(values["mask_change_digests"]),
        hall_change_digests=tuple(values["hall_change_digests"]),
        logical_overlay_patch_digest_value=_hash(
            values["logical_overlay_patch_digest"], name="logical patch digest"
        ),
        matching_work_digest_value=_hash(
            values["matching_work_digest"], name="matching work digest"
        ),
    )
    declared = _hash(values["patch_digest"], name="patch digest")
    shape_digest = _hash(values["group_shape_set_digest"], name="shape digest")
    logical_digest = _hash(
        values["logical_overlay_patch_digest"], name="logical patch digest"
    )
    cursor.execute(
        """
        SELECT groundloop_m5_matching_validate_logical_patch(
                 %s, %s, %s, %s, %s,
                 groundloop_m5_matching_validate_group_shapes(%s))
        """,
        (
            patch.logical_patch_preimage,
            patch.logical_output_preimage,
            values["resulting_epoch_id"],
            values["resulting_revision"],
            values["decision_policy_version"],
            patch.group_shape_preimage,
        ),
    ).fetchone()
    return (
        expected == declared
        and hashlib.sha256(patch.canonical_preimage).hexdigest() == declared
        and hashlib.sha256(patch.group_shape_preimage).hexdigest() == shape_digest
        and hashlib.sha256(patch.logical_patch_preimage).hexdigest() == logical_digest
    )


def _working_queries() -> tuple[tuple[M5MatchingAuditFamily, str], ...]:
    return (
        (
            M5MatchingAuditFamily.OBSERVATION,
            """SELECT epoch_id, observation_id, requirement_version_id,
                      group_version_id, requirement_ordinal, text_hash::text,
                      present, updated_revision
                 FROM groundloop_m5_matching_observation_working
                ORDER BY epoch_id, observation_id COLLATE "C"
                """,
        ),
        (
            M5MatchingAuditFamily.EDGE,
            """SELECT epoch_id, requirement_version_id, text_hash::text,
                      group_version_id, requirement_ordinal, refcount,
                      updated_revision
                 FROM groundloop_m5_matching_edge_working
                ORDER BY epoch_id, requirement_version_id COLLATE "C", text_hash
                """,
        ),
        (
            M5MatchingAuditFamily.MASK,
            """SELECT epoch_id, group_version_id, text_hash::text, mask,
                      updated_revision
                 FROM groundloop_m5_matching_hash_mask_working
                ORDER BY epoch_id, group_version_id COLLATE "C", text_hash
                """,
        ),
        (
            M5MatchingAuditFamily.HALL,
            """SELECT epoch_id, group_version_id, present, requirement_count,
                      mask_histogram, neighbor_counts, deficiencies,
                      maximum_deficiency, matching_size, distinct_hash_count,
                      updated_revision
                 FROM groundloop_m5_matching_hall_working
                ORDER BY epoch_id, group_version_id COLLATE "C"
                """,
        ),
    )


def _working_point(family: M5MatchingAuditFamily, row: tuple[Any, ...]) -> WorkingPoint:
    try:
        if family is M5MatchingAuditFamily.OBSERVATION:
            return M5MatchingObservationWorking(
                M5MatchingLayer.WORKING,
                _integer(row[0], name="epoch_id", minimum=1),
                _text(row[1], name="observation_id"),
                _text(row[2], name="requirement_version_id"),
                _text(row[3], name="group_version_id"),
                _integer(row[4], name="requirement_ordinal", minimum=0),
                _hash(row[5], name="text_hash"),
                _boolean(row[6], name="present"),
                _integer(row[7], name="updated_revision", minimum=1),
            )
        if family is M5MatchingAuditFamily.EDGE:
            return M5MatchingEdgeWorking(
                M5MatchingLayer.WORKING,
                _integer(row[0], name="epoch_id", minimum=1),
                _text(row[1], name="requirement_version_id"),
                _hash(row[2], name="text_hash"),
                _text(row[3], name="group_version_id"),
                _integer(row[4], name="requirement_ordinal", minimum=0),
                _integer(row[5], name="refcount", minimum=0),
                _integer(row[6], name="updated_revision", minimum=1),
            )
        if family is M5MatchingAuditFamily.MASK:
            return M5MatchingMaskWorking(
                M5MatchingLayer.WORKING,
                _integer(row[0], name="epoch_id", minimum=1),
                _text(row[1], name="group_version_id"),
                _hash(row[2], name="text_hash"),
                _integer(row[3], name="mask", minimum=0),
                _integer(row[4], name="updated_revision", minimum=1),
            )
        present = _boolean(row[2], name="present")
        return M5MatchingHallWorking(
            M5MatchingLayer.WORKING,
            _integer(row[0], name="epoch_id", minimum=1),
            _text(row[1], name="group_version_id"),
            present,
            None
            if row[3] is None
            else _integer(row[3], name="requirement_count", minimum=1),
            None if row[4] is None else _integer_tuple(row[4], name="mask_histogram"),
            None if row[5] is None else _integer_tuple(row[5], name="neighbor_counts"),
            None if row[6] is None else _integer_tuple(row[6], name="deficiencies"),
            None
            if row[7] is None
            else _integer(row[7], name="maximum_deficiency", minimum=0),
            None
            if row[8] is None
            else _integer(row[8], name="matching_size", minimum=0),
            None
            if row[9] is None
            else _integer(row[9], name="distinct_hash_count", minimum=0),
            _integer(row[10], name="updated_revision", minimum=1),
        )
    except (TypeError, ValueError, ValidationError) as exc:
        raise M5MatchingAuditInvalidError("retained working row is malformed") from exc


def _read_working_points(
    cursor: Cursor[Any],
) -> dict[int, dict[M5MatchingAuditFamily, dict[AuditKey, WorkingPoint]]]:
    result: dict[int, dict[M5MatchingAuditFamily, dict[AuditKey, WorkingPoint]]] = {}
    for family, query in _working_queries():
        for raw in cursor.execute(query).fetchall():
            point = _working_point(family, tuple(raw))
            epoch_id = cast(Any, point).epoch_id
            key = _point_key(family, point)
            family_map = result.setdefault(
                epoch_id, {value: {} for value in _FAMILIES}
            )[family]
            if key in family_map:
                raise M5MatchingAuditInvalidError(
                    "retained working outer key is duplicated"
                )
            family_map[key] = point
    return result


def _is_tombstone(point: WorkingPoint) -> bool:
    if isinstance(point, M5MatchingObservationWorking):
        return not point.present
    if isinstance(point, M5MatchingEdgeWorking):
        return point.refcount == 0
    if isinstance(point, M5MatchingMaskWorking):
        return point.mask == 0
    return not point.present


def _promoted(point: WorkingPoint, *, epoch_id: int, revision: int) -> CurrentPoint:
    if isinstance(point, M5MatchingObservationWorking):
        return M5MatchingObservationCurrent(
            M5MatchingLayer.CURRENT,
            point.observation_id,
            point.requirement_version_id,
            point.group_version_id,
            point.requirement_ordinal,
            point.text_hash,
            epoch_id,
            revision,
        )
    if isinstance(point, M5MatchingEdgeWorking):
        return M5MatchingEdgeCurrent(
            M5MatchingLayer.CURRENT,
            point.requirement_version_id,
            point.text_hash,
            point.group_version_id,
            point.requirement_ordinal,
            point.refcount,
            epoch_id,
            revision,
        )
    if isinstance(point, M5MatchingMaskWorking):
        return M5MatchingMaskCurrent(
            M5MatchingLayer.CURRENT,
            point.group_version_id,
            point.text_hash,
            point.mask,
            epoch_id,
            revision,
        )
    assert point.requirement_count is not None
    assert point.mask_histogram is not None
    assert point.neighbor_counts is not None
    assert point.deficiencies is not None
    assert point.maximum_deficiency is not None
    assert point.matching_size is not None
    assert point.distinct_hash_count is not None
    return M5MatchingHallCurrent(
        M5MatchingLayer.CURRENT,
        point.group_version_id,
        point.requirement_count,
        point.mask_histogram,
        point.neighbor_counts,
        point.deficiencies,
        point.maximum_deficiency,
        point.matching_size,
        point.distinct_hash_count,
        epoch_id,
        revision,
    )


def _empty_family_maps() -> dict[M5MatchingAuditFamily, dict[AuditKey, CurrentPoint]]:
    return {family: {} for family in _FAMILIES}


def _empty_working_maps() -> dict[M5MatchingAuditFamily, dict[AuditKey, WorkingPoint]]:
    return {family: {} for family in _FAMILIES}


def _runtime_rows(cursor: Cursor[Any]) -> dict[int, _RuntimeAuthority]:
    rows = cursor.execute(
        """
        SELECT runtime.epoch_id, runtime.runtime_state, runtime.revision,
               runtime.terminal_at,
               runtime.expected_previous_published_epoch_id,
               epoch.revision, epoch.structural_status,
               epoch.semantic_status, epoch.evaluation_state,
               epoch.publication_mode, epoch.sealed_at,
               update_row.previous_published_epoch_id,
               update_row.decision_policy_version,
               predecessor.revision, predecessor.structural_status,
               predecessor.semantic_status, predecessor.evaluation_state,
               predecessor.publication_mode, predecessor.sealed_at
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_epoch AS epoch USING (epoch_id)
        JOIN groundloop_m5_update AS update_row USING (epoch_id)
        JOIN groundloop_epoch AS predecessor
          ON predecessor.epoch_id = update_row.previous_published_epoch_id
        ORDER BY runtime.epoch_id
        """
    ).fetchall()
    result: dict[int, _RuntimeAuthority] = {}
    for row in rows:
        epoch_id = _integer(row[0], name="runtime epoch", minimum=1)
        if epoch_id in result:
            raise M5MatchingAuditInvalidError("runtime epoch key is duplicated")
        runtime_state = _text(row[1], name="runtime state")
        runtime_revision = _integer(row[2], name="runtime revision", minimum=1)
        base_epoch_id = _integer(row[4], name="runtime base epoch", minimum=1)
        epoch_revision = _integer(row[5], name="epoch revision", minimum=1)
        update_base_epoch_id = _integer(row[11], name="update base epoch", minimum=1)
        base_revision = _integer(row[13], name="base revision", minimum=0)
        if runtime_revision != epoch_revision:
            raise M5MatchingAuditInvalidError("runtime and epoch revisions disagree")
        if base_epoch_id != update_base_epoch_id:
            raise M5MatchingAuditInvalidError("runtime and update bases disagree")
        if (
            tuple(row[14:18]) != ("committed", "sealed", "complete", "strict")
            or row[18] is None
        ):
            raise M5MatchingAuditInvalidError(
                "runtime predecessor is not an exact sealed base"
            )
        terminal = row[3] is not None
        epoch_state = tuple(row[6:10])
        sealed_at = row[10] is not None
        if runtime_state in {"structural_committed", "semantic_pending"}:
            valid_state = (
                epoch_state
                == (
                    "committed",
                    "pending",
                    "pending",
                    "provisional",
                )
                and not terminal
                and not sealed_at
            )
        elif runtime_state == "semantic_complete":
            valid_state = (
                epoch_state
                == (
                    "committed",
                    "complete",
                    "complete",
                    "provisional",
                )
                and not terminal
                and not sealed_at
            )
        elif runtime_state == "sealed":
            valid_state = (
                epoch_state
                == (
                    "committed",
                    "sealed",
                    "complete",
                    "strict",
                )
                and terminal
                and sealed_at
            )
        elif runtime_state == "failed":
            valid_state = (
                epoch_state
                == (
                    "failed",
                    "failed",
                    "failed",
                    "provisional",
                )
                and terminal
                and not sealed_at
            )
        else:
            valid_state = False
        if not valid_state:
            raise M5MatchingAuditInvalidError(
                "runtime and epoch terminal/status authority disagree"
            )
        result[epoch_id] = _RuntimeAuthority(
            status=_status(runtime_state),
            revision=runtime_revision,
            base_epoch_id=base_epoch_id,
            base_revision=base_revision,
            decision_policy_version=_text(row[12], name="update policy"),
        )
    return result


def _json_rows(cursor: Cursor[Any], query: str) -> dict[int, dict[str, Any]]:
    result: dict[int, dict[str, Any]] = {}
    for row in cursor.execute(query).fetchall():
        if not isinstance(row[0], dict):
            raise M5MatchingAuditInvalidError("provenance row is malformed")
        value = dict(row[0])
        epoch_id = _integer(value.get("epoch_id"), name="epoch_id", minimum=1)
        if epoch_id in result:
            raise M5MatchingAuditInvalidError("provenance epoch key is duplicated")
        result[epoch_id] = value
    return result


def _work_values(value: dict[str, Any]) -> tuple[int, ...]:
    return tuple(
        _integer(value.get(name), name=name, minimum=0)
        for name in MATCHING_WORK_COUNTER_NAMES
    )


def _work_digest_binding_matches(
    contribution: dict[str, Any], patch: dict[str, Any], expected_digest: str
) -> bool:
    return (
        _hash(
            contribution.get("matching_work_digest"),
            name="contribution work digest",
        )
        == expected_digest
        and _hash(patch.get("matching_work_digest"), name="patch work digest")
        == expected_digest
    )


def _revision_sequence_matches_authority(
    authority: _RuntimeAuthority, revisions: tuple[int, ...]
) -> bool:
    return bool(
        revisions
        and revisions[0] == 1
        and revisions == tuple(sorted(set(revisions)))
        and authority.revision >= revisions[-1]
    )


def _structural_patch_matches_authority(
    patch: dict[str, Any],
    authority: _RuntimeAuthority,
    current_header: tuple[int, int, str],
) -> bool:
    return (
        patch.get("source_kind") == "structural_open"
        and (
            _integer(patch.get("before_epoch_id"), name="patch base epoch", minimum=1),
            _integer(
                patch.get("before_revision"),
                name="patch base revision",
                minimum=0,
            ),
        )
        == (authority.base_epoch_id, authority.base_revision)
        == current_header[:2]
        and _text(patch.get("decision_policy_version"), name="patch policy")
        == authority.decision_policy_version
    )


def _audit_provenance(
    cursor: Cursor[Any],
    *,
    expected: M5MatchingAuditProjection,
    actual_points: dict[M5MatchingAuditFamily, dict[AuditKey, CurrentPoint]],
) -> _ProvenanceResult:
    runtime = _runtime_rows(cursor)
    patches = _load_patch_records(cursor)
    patch_by_digest: dict[str, _PatchRecord] = {}
    patches_by_epoch: dict[int, list[_PatchRecord]] = {}
    decoded_by_digest: dict[str, tuple[_DecodedChange, ...]] = {}
    integrity_ok = True
    for patch in patches:
        digest = _hash(patch.values.get("patch_digest"), name="patch digest")
        if digest in patch_by_digest:
            raise M5MatchingAuditInvalidError("patch digest is duplicated")
        patch_by_digest[digest] = patch
        epoch_id = _integer(
            patch.values.get("resulting_epoch_id"),
            name="patch epoch",
            minimum=1,
        )
        patches_by_epoch.setdefault(epoch_id, []).append(patch)
        integrity_ok = _validate_outer_patch(cursor, patch) and integrity_ok
        decoded_by_digest[digest] = _decode_changes(cursor, patch)

    contributions: dict[str, dict[str, Any]] = {}
    contribution_rows = cursor.execute(
        """
        SELECT to_jsonb(contribution)
        FROM groundloop_m5_matching_work_contribution AS contribution
        ORDER BY contribution.epoch_id, contribution.resulting_revision,
                 contribution.source_kind COLLATE "C",
                 contribution.source_id COLLATE "C"
        """
    ).fetchall()
    for row in contribution_rows:
        if not isinstance(row[0], dict):
            raise M5MatchingAuditInvalidError("contribution row is malformed")
        contribution = dict(row[0])
        patch_digest = _hash(
            contribution.get("patch_digest"), name="contribution patch digest"
        )
        if patch_digest in contributions:
            raise M5MatchingAuditInvalidError(
                "more than one contribution names one patch"
            )
        contributions[patch_digest] = contribution
    if set(contributions) != set(patch_by_digest):
        raise M5MatchingAuditInvalidError("patch/contribution membership is incomplete")

    counters_by_epoch: dict[int, list[tuple[int, ...]]] = {}
    contribution_digest_by_patch: dict[str, str] = {}
    for patch_digest, contribution in contributions.items():
        patch = patch_by_digest[patch_digest]
        patch_values = patch.values
        for name in (
            "source_kind",
            "source_id",
            "source_identity_hash",
            "before_epoch_id",
            "before_revision",
            "resulting_revision",
        ):
            if contribution.get(name) != patch_values.get(name):
                integrity_ok = False
        epoch_id = _integer(
            contribution.get("epoch_id"), name="contribution epoch", minimum=1
        )
        if epoch_id != _integer(
            patch_values.get("resulting_epoch_id"),
            name="patch epoch",
            minimum=1,
        ):
            integrity_ok = False
        counters = _work_values(contribution)
        work_digest = digests.matching_work_digest(counters)
        if not _work_digest_binding_matches(contribution, patch_values, work_digest):
            integrity_ok = False
        declared_contribution = _hash(
            contribution.get("contribution_digest"),
            name="contribution digest",
        )
        expected_contribution = digests.matching_work_contribution_digest(
            epoch_id=epoch_id,
            source_kind=_text(
                contribution.get("source_kind"), name="contribution source kind"
            ),
            source_id=_text(
                contribution.get("source_id"), name="contribution source id"
            ),
            source_identity_hash=_hash(
                contribution.get("source_identity_hash"),
                name="contribution source identity",
            ),
            before_epoch_id=_integer(
                contribution.get("before_epoch_id"),
                name="contribution before epoch",
                minimum=1,
            ),
            before_revision=_integer(
                contribution.get("before_revision"),
                name="contribution before revision",
                minimum=0,
            ),
            resulting_revision=_integer(
                contribution.get("resulting_revision"),
                name="contribution revision",
                minimum=1,
            ),
            patch_digest=patch_digest,
            matching_work_digest_value=work_digest,
        )
        if declared_contribution != expected_contribution:
            integrity_ok = False
        contribution_digest_by_patch[patch_digest] = declared_contribution
        counters_by_epoch.setdefault(epoch_id, []).append(counters)

    activation = cursor.execute(
        """
        SELECT activation.base_m4_epoch_id, epoch.revision,
               policy.policy_version,
               count(policy.policy_version) OVER ()
        FROM groundloop_m5_activation AS activation
        JOIN groundloop_epoch AS epoch
          ON epoch.epoch_id = activation.base_m4_epoch_id
        JOIN groundloop_decision_policy AS policy
          ON policy.valid_from_epoch <= activation.base_m4_epoch_id
         AND (policy.valid_to_epoch IS NULL
              OR activation.base_m4_epoch_id < policy.valid_to_epoch)
        WHERE activation.singleton
        """
    ).fetchall()
    if len(activation) != 1 or int(activation[0][3]) != 1:
        raise M5MatchingAuditInvalidError("activation bootstrap is ambiguous")
    base_epoch_id = _integer(activation[0][0], name="activation epoch", minimum=1)
    base_revision = _integer(activation[0][1], name="activation revision", minimum=0)
    base_policy = _text(activation[0][2], name="activation policy")

    current: dict[M5MatchingAuditFamily, dict[AuditKey, CurrentPoint]] = (
        _empty_family_maps()
    )
    current_last_touch: dict[M5MatchingAuditFamily, dict[AuditKey, str | None]] = {
        family: {} for family in _FAMILIES
    }
    all_changes = tuple(
        change
        for patch in patches
        for change in decoded_by_digest[
            _hash(patch.values.get("patch_digest"), name="patch digest")
        ]
    )
    for change in all_changes:
        before = change.before
        if isinstance(
            before,
            (
                M5MatchingObservationCurrent,
                M5MatchingEdgeCurrent,
                M5MatchingMaskCurrent,
                M5MatchingHallCurrent,
            ),
        ) and (
            before.installed_epoch_id,
            before.installed_revision,
        ) == (base_epoch_id, base_revision):
            existing = current[change.family].setdefault(change.key, before)
            if existing != before:
                raise M5MatchingAuditInvalidError(
                    "bootstrap current point has conflicting retained bytes"
                )
            current_last_touch[change.family][change.key] = None
    for family, family_points in actual_points.items():
        for key, point in family_points.items():
            if (point.installed_epoch_id, point.installed_revision) == (
                base_epoch_id,
                base_revision,
            ):
                existing = current[family].setdefault(key, point)
                if existing != point:
                    raise M5MatchingAuditInvalidError(
                        "bootstrap actual point conflicts with patch evidence"
                    )
                current_last_touch[family][key] = None

    expected_working: dict[
        int, dict[M5MatchingAuditFamily, dict[AuditKey, WorkingPoint]]
    ] = {}
    expected_working_touch: dict[
        int, dict[M5MatchingAuditFamily, dict[AuditKey, str]]
    ] = {}
    expected_headers: dict[int, tuple[str, ...]] = {}
    patch_provenance: list[tuple[str, ...]] = []
    current_header = (base_epoch_id, base_revision, base_policy)

    for epoch_id in sorted(set(runtime) | set(patches_by_epoch)):
        status_row = runtime.get(epoch_id)
        epoch_patches = sorted(
            patches_by_epoch.get(epoch_id, []),
            key=lambda value: int(value.values["resulting_revision"]),
        )
        if status_row is None or not epoch_patches:
            integrity_ok = False
            continue
        status = status_row.status
        terminal_runtime_revision = status_row.revision
        revisions = tuple(
            int(patch.values["resulting_revision"]) for patch in epoch_patches
        )
        if not _revision_sequence_matches_authority(status_row, revisions):
            integrity_ok = False
        first = epoch_patches[0].values
        before_coordinates = (status_row.base_epoch_id, status_row.base_revision)
        policy = status_row.decision_policy_version
        if not _structural_patch_matches_authority(first, status_row, current_header):
            integrity_ok = False
        working = _empty_working_maps()
        working_touch: dict[M5MatchingAuditFamily, dict[AuditKey, str]] = {
            family: {} for family in _FAMILIES
        }
        for patch in epoch_patches:
            values = patch.values
            patch_digest = _hash(values.get("patch_digest"), name="patch digest")
            if (
                _text(values.get("decision_policy_version"), name="patch policy")
                != policy
            ):
                integrity_ok = False
            for change in decoded_by_digest[patch_digest]:
                effective: CurrentPoint | WorkingPoint | None = working[
                    change.family
                ].get(change.key)
                if effective is None:
                    effective = current[change.family].get(change.key)
                if effective != change.before:
                    integrity_ok = False
                working[change.family][change.key] = change.after
                working_touch[change.family][change.key] = patch_digest
            contribution = contributions[patch_digest]
            patch_provenance.append(
                digests.patch_provenance_fields(
                    epoch_id,
                    _integer(
                        values.get("resulting_revision"),
                        name="patch revision",
                        minimum=1,
                    ),
                    _text(values.get("source_kind"), name="patch source kind"),
                    _text(values.get("source_id"), name="patch source id"),
                    status,
                    patch_digest,
                    contribution_digest_by_patch[patch_digest],
                )
            )
            if _work_values(contribution) not in counters_by_epoch[epoch_id]:
                integrity_ok = False
        last_patch_digest = _hash(
            epoch_patches[-1].values.get("patch_digest"), name="last patch digest"
        )
        updated_revision = revisions[-1]
        terminal_revision = (
            None
            if status is M5MatchingEpochStatus.NONTERMINAL
            else terminal_runtime_revision
        )
        expected_headers[epoch_id] = digests.working_image_provenance_fields(
            epoch_id,
            status,
            terminal_revision,
            before_coordinates[0],
            before_coordinates[1],
            policy,
            updated_revision,
            last_patch_digest,
        )
        expected_working[epoch_id] = working
        expected_working_touch[epoch_id] = working_touch
        if status is M5MatchingEpochStatus.SEALED:
            if terminal_runtime_revision <= updated_revision:
                integrity_ok = False
            for family in _FAMILIES:
                for key, working_point_value in working[family].items():
                    if _is_tombstone(working_point_value):
                        current[family].pop(key, None)
                        current_last_touch[family].pop(key, None)
                    else:
                        current[family][key] = _promoted(
                            working_point_value,
                            epoch_id=epoch_id,
                            revision=terminal_runtime_revision,
                        )
                        current_last_touch[family][key] = working_touch[family][key]
            current_header = (epoch_id, terminal_runtime_revision, policy)

    if current_header != (
        expected.head_epoch_id,
        expected.head_revision,
        expected.decision_policy_version,
    ):
        integrity_ok = False

    actual_working = _read_working_points(cursor)
    working_equal = True
    for epoch_id in set(expected_working) | set(actual_working):
        expected_epoch = expected_working.get(epoch_id, _empty_working_maps())
        actual_epoch = actual_working.get(epoch_id, _empty_working_maps())
        if expected_epoch != actual_epoch:
            working_equal = False

    current_equal = current == actual_points
    integrity_ok = integrity_ok and working_equal and current_equal

    actual_header_values = _json_rows(
        cursor,
        """SELECT to_jsonb(image)
             FROM groundloop_m5_matching_image_working AS image
            ORDER BY image.epoch_id""",
    )
    actual_headers: dict[int, tuple[str, ...]] = {}
    for epoch_id, value in actual_header_values.items():
        status_row = runtime.get(epoch_id)
        header_patches = patches_by_epoch.get(epoch_id)
        if status_row is None or not header_patches:
            raise M5MatchingAuditInvalidError(
                "working-image row lacks replay authority"
            )
        status = status_row.status
        runtime_revision = status_row.revision
        last_patch = max(
            header_patches, key=lambda patch: int(patch.values["resulting_revision"])
        )
        actual_headers[epoch_id] = digests.working_image_provenance_fields(
            epoch_id,
            status,
            None if status is M5MatchingEpochStatus.NONTERMINAL else runtime_revision,
            _integer(value.get("base_epoch_id"), name="image base epoch", minimum=1),
            _integer(value.get("base_revision"), name="image base revision", minimum=0),
            _text(value.get("decision_policy_version"), name="image policy"),
            _integer(value.get("updated_revision"), name="image revision", minimum=1),
            _hash(last_patch.values.get("patch_digest"), name="last patch digest"),
        )

    expected_accumulators: dict[int, tuple[str, ...]] = {}
    for epoch_id, vectors in counters_by_epoch.items():
        total = tuple(sum(values) for values in zip(*vectors, strict=True))
        status = runtime[epoch_id].status
        updated_revision = max(
            int(patch.values["resulting_revision"])
            for patch in patches_by_epoch[epoch_id]
        )
        expected_accumulators[epoch_id] = digests.accumulator_provenance_fields(
            epoch_id,
            status,
            total,
            digests.matching_work_digest(total),
            updated_revision,
        )
    actual_accumulator_values = _json_rows(
        cursor,
        """SELECT to_jsonb(accumulator)
             FROM groundloop_m5_matching_work_accumulator AS accumulator
            ORDER BY accumulator.epoch_id""",
    )
    actual_accumulators: dict[int, tuple[str, ...]] = {}
    for epoch_id, value in actual_accumulator_values.items():
        if epoch_id not in runtime:
            raise M5MatchingAuditInvalidError(
                "matching-work accumulator lacks runtime authority"
            )
        actual_accumulators[epoch_id] = digests.accumulator_provenance_fields(
            epoch_id,
            runtime[epoch_id].status,
            _work_values(value),
            _hash(value.get("matching_work_digest"), name="accumulator digest"),
            _integer(
                value.get("updated_revision"),
                name="accumulator revision",
                minimum=1,
            ),
        )

    working_expected_rows = tuple(
        expected_headers[epoch_id] for epoch_id in sorted(expected_headers)
    )
    working_actual_rows = tuple(
        actual_headers[epoch_id] for epoch_id in sorted(actual_headers)
    )
    accumulator_expected_rows = tuple(
        expected_accumulators[epoch_id] for epoch_id in sorted(expected_accumulators)
    )
    accumulator_actual_rows = tuple(
        actual_accumulators[epoch_id] for epoch_id in sorted(actual_accumulators)
    )
    provenance_mismatches: list[M5PersistedMatchingProvenanceMismatch] = []
    for kind, expected_rows, actual_rows in (
        (M5MatchingProvenanceKind.WORKING_IMAGE, expected_headers, actual_headers),
        (
            M5MatchingProvenanceKind.ACCUMULATOR,
            expected_accumulators,
            actual_accumulators,
        ),
    ):
        for epoch_id in sorted(set(expected_rows) | set(actual_rows)):
            expected_row = expected_rows.get(epoch_id)
            actual_row = actual_rows.get(epoch_id)
            expected_digest = (
                None
                if expected_row is None
                else (
                    digests.working_image_provenance_row_digest(expected_row)
                    if kind is M5MatchingProvenanceKind.WORKING_IMAGE
                    else digests.accumulator_provenance_row_digest(expected_row)
                )
            )
            actual_digest = (
                None
                if actual_row is None
                else (
                    digests.working_image_provenance_row_digest(actual_row)
                    if kind is M5MatchingProvenanceKind.WORKING_IMAGE
                    else digests.accumulator_provenance_row_digest(actual_row)
                )
            )
            if expected_digest != actual_digest:
                provenance_mismatches.append(
                    M5PersistedMatchingProvenanceMismatch(
                        kind=kind,
                        epoch_id=epoch_id,
                        expected_row_digest=expected_digest,
                        actual_row_digest=actual_digest,
                    )
                )

    current_provenance = tuple(
        digests.current_provenance_fields(
            family,
            key,
            current[family][key],
            current_last_touch[family][key],
        )
        for family in _FAMILIES
        for key in sorted(current[family])
    )
    working_provenance = tuple(
        digests.working_provenance_fields(
            family,
            key,
            expected_working[epoch_id][family][key],
            expected_working_touch[epoch_id][family][key],
        )
        for epoch_id in sorted(expected_working)
        for family in _FAMILIES
        for key in sorted(expected_working[epoch_id][family])
    )
    working_expected_digest = digests.working_image_provenance_digest(
        working_expected_rows
    )
    working_actual_digest = digests.working_image_provenance_digest(working_actual_rows)
    accumulator_expected_digest = digests.accumulator_provenance_digest(
        accumulator_expected_rows
    )
    accumulator_actual_digest = digests.accumulator_provenance_digest(
        accumulator_actual_rows
    )
    mismatch_tuple = tuple(provenance_mismatches)
    ok = (
        integrity_ok
        and working_expected_digest == working_actual_digest
        and accumulator_expected_digest == accumulator_actual_digest
        and not mismatch_tuple
    )
    return _ProvenanceResult(
        ok=ok,
        replay_digest=digests.provenance_replay_digest(
            expected.head_epoch_id,
            expected.head_revision,
            tuple(patch_provenance),
            working_expected_rows,
            accumulator_expected_rows,
            current_provenance,
            working_provenance,
        ),
        working_expected_digest=working_expected_digest,
        working_actual_digest=working_actual_digest,
        accumulator_expected_digest=accumulator_expected_digest,
        accumulator_actual_digest=accumulator_actual_digest,
        mismatches=mismatch_tuple,
    )


def _audit_fields_from_current(
    family: M5MatchingAuditFamily, point: CurrentPoint
) -> tuple[str, ...]:
    if isinstance(point, M5MatchingObservationCurrent):
        row: AuditRow = (
            point.observation_id,
            point.requirement_version_id,
            point.group_version_id,
            point.requirement_ordinal,
            point.text_hash,
        )
    elif isinstance(point, M5MatchingEdgeCurrent):
        row = (
            point.requirement_version_id,
            point.text_hash,
            point.group_version_id,
            point.requirement_ordinal,
            point.refcount,
        )
    elif isinstance(point, M5MatchingMaskCurrent):
        row = (point.group_version_id, point.text_hash, point.mask)
    else:
        row = (
            point.group_version_id,
            point.requirement_count,
            point.mask_histogram,
            point.neighbor_counts,
            point.deficiencies,
            point.maximum_deficiency,
            point.matching_size,
            point.distinct_hash_count,
        )
    return _audit_fields(family, row)


def _malformed_branch_mismatches(
    expected: M5MatchingAuditProjection,
    actual: _ActualImage,
) -> tuple[M5PersistedMatchingPhysicalMismatch, ...]:
    expected_maps = expected.row_maps
    malformed = {(row.family, row.key): row for row in actual.malformed}
    result: list[M5PersistedMatchingPhysicalMismatch] = list(actual.malformed)
    for family in _FAMILIES:
        actual_fields = {
            key: _audit_fields_from_current(family, point)
            for key, point in actual.points[family].items()
        }
        for key in sorted(set(expected_maps[family]) | set(actual_fields)):
            if (family, key) in malformed:
                continue
            expected_row = expected_maps[family].get(key)
            actual_row = actual_fields.get(key)
            expected_digest = (
                None
                if expected_row is None
                else digests.physical_audit_row_digest(family, expected_row)
            )
            actual_digest = (
                None
                if actual_row is None
                else digests.physical_audit_row_digest(family, actual_row)
            )
            if expected_digest != actual_digest:
                result.append(
                    M5PersistedMatchingPhysicalMismatch(
                        family=family,
                        key=key,
                        expected_row_digest=expected_digest,
                        actual_row_digest=actual_digest,
                        actual_error=None,
                    )
                )
    return tuple(sorted(result, key=lambda item: (_FAMILY_RANK[item.family], item.key)))


def audit_matching_actual_image(
    cursor: Cursor[Any],
    *,
    python_expected: M5MatchingAuditProjection,
    sql_expected: M5MatchingAuditProjection,
) -> M5PersistedMatchingPhysicalAudit:
    """Compare two independent expected projections with D25 physical state.

    The supplied cursor must be bound to the same imported snapshot used by
    both expected readers.  Snapshot export/import and the post-collection
    publication-head recheck remain coordinator responsibilities.
    """

    if (
        type(python_expected) is not M5MatchingAuditProjection
        or type(sql_expected) is not M5MatchingAuditProjection
    ):
        raise M5MatchingAuditInvalidError(
            "both expected readers must return exact projection DTOs"
        )
    if python_expected != sql_expected:
        raise M5MatchingAuditInvalidError(
            "independent expected matching projections disagree"
        )
    python_digest = python_expected.projection_digest
    sql_digest = sql_expected.projection_digest
    if python_digest != sql_digest:
        raise M5MatchingAuditInvalidError(
            "independent expected projection bytes disagree"
        )
    actual = _read_actual_image(cursor, expected=python_expected)
    common: dict[str, Any] = {
        "head_epoch_id": python_expected.head_epoch_id,
        "head_revision": python_expected.head_revision,
        "decision_policy_version": python_expected.decision_policy_version,
        "python_expected_projection_digest": python_digest,
        "sql_expected_projection_digest": sql_digest,
    }
    if actual.projection is None:
        return _artifact(
            {
                **common,
                "actual_projection_digest": None,
                "actual_error": M5MatchingAuditError.MALFORMED_PAYLOAD,
                "provenance_ok": False,
                "provenance_replay_digest": None,
                "working_image_expected_provenance_digest": None,
                "working_image_actual_provenance_digest": None,
                "accumulator_expected_provenance_digest": None,
                "accumulator_actual_provenance_digest": None,
                "mismatches": _malformed_branch_mismatches(python_expected, actual),
                "provenance_mismatches": (),
            }
        )

    physical_mismatches = _physical_mismatches(python_expected, actual.projection)
    provenance = _audit_provenance(
        cursor,
        expected=python_expected,
        actual_points=actual.points,
    )
    return _artifact(
        {
            **common,
            "actual_projection_digest": actual.projection.projection_digest,
            "actual_error": None,
            "provenance_ok": provenance.ok,
            "provenance_replay_digest": provenance.replay_digest,
            "working_image_expected_provenance_digest": (
                provenance.working_expected_digest
            ),
            "working_image_actual_provenance_digest": (
                provenance.working_actual_digest
            ),
            "accumulator_expected_provenance_digest": (
                provenance.accumulator_expected_digest
            ),
            "accumulator_actual_provenance_digest": (
                provenance.accumulator_actual_digest
            ),
            "mismatches": physical_mismatches,
            "provenance_mismatches": provenance.mismatches,
        }
    )


__all__ = [
    "M5MatchingAuditInvalidError",
    "M5MatchingAuditProjection",
    "audit_matching_actual_image",
]
