"""Byte-total digest recipes for the GroundLoop M5 typed runtime.

This module deliberately contains no runtime state or persistence logic.  Each
function is a direct spelling of one frozen recipe in M5-D14 or the M5.4
runtime addendum and delegates framing to :mod:`groundloop.m5.digests`.
"""

from __future__ import annotations

import hashlib
import math
import struct
from collections.abc import Iterable, Sequence
from enum import Enum
from typing import Any

from groundloop.domain import StatusDelta, SubjectKind
from groundloop.errors import ValidationError
from groundloop.m5.digests import (
    bool_field,
    enum_field,
    f64_field,
    hash_field,
    int_field,
    option_field,
    sequence_field,
    stable_m5_digest,
    text_field,
)

RequirementRegistryRow = tuple[str, str, str, str, str, str]
ActiveChunkRow = tuple[str, str]
AdmittedPairSourceRow = tuple[str, str, str]
AttemptResultValues = tuple[
    str,
    str,
    int,
    str | Enum,
    str | Enum,
    str | Enum,
    int,
    int,
    bool,
    bool | None,
    bool | None,
    bool | None,
    str | Enum | None,
    str | None,
    int | None,
    str | Enum | None,
]
ChangedStateReferenceValues = tuple[str | Enum, str, int, int, str]
TypedDirectChannelHitValues = tuple[
    int,
    str,
    str,
    str,
    str | Enum,
    int,
    float | None,
    str,
]
TypedDirectAdmittedPairValues = tuple[
    int,
    str,
    str,
    str,
    int,
    Sequence[str | Enum],
    bool,
]
TypedDirectVerificationExecutionValues = tuple[
    str,
    str,
    str,
    str,
    str,
    str,
    str,
    str,
    str,
    float,
    Sequence[float],
    str,
    str | None,
]

MatchingWorkValues = tuple[int, ...]
AuditKeyValues = tuple[str, ...]


def stable_m5_preimage(domain_tag: str, *values: tuple[str, ...]) -> bytes:
    """Return the exact framed bytes consumed by ``stable_m5_digest``."""
    if type(domain_tag) is not str or not domain_tag:
        raise ValidationError("digest domain tags must be nonempty strings")
    fields = [domain_tag]
    for value in values:
        if type(value) is not tuple or not all(type(field) is str for field in value):
            raise ValidationError("digest values must be exact typed field tuples")
        fields.extend(value)
    return b"".join(len(v.encode()).to_bytes(8, "big") + v.encode() for v in fields)


def verify_digest_preimage(digest: str, preimage: bytes) -> None:
    if type(preimage) is not bytes or hashlib.sha256(preimage).hexdigest() != digest:
        raise ValidationError("digest/preimage identity mismatch")


def _seq(values: Iterable[tuple[str, ...]]) -> tuple[str, ...]:
    return sequence_field(values)


def matching_work_digest(counters: Sequence[int]) -> str:
    """Hash the exact 37-value D25 matching/overlay work vector."""

    if len(counters) != 37:
        raise ValidationError("matching work requires exactly 37 counters")
    return stable_m5_digest("m5-matching-work-v1", *(int_field(v) for v in counters))


def matching_group_shape_set_digest(
    shapes: Sequence[tuple[str, int, Sequence[tuple[int, str]]]],
) -> str:
    return stable_m5_digest(
        "m5-persisted-matching-group-shape-set-v1",
        _seq(
            _seq(
                (
                    text_field(group_id),
                    int_field(requirement_count),
                    _seq(
                        _seq((int_field(ordinal), text_field(requirement_id)))
                        for ordinal, requirement_id in requirements
                    ),
                )
            )
            for group_id, requirement_count, requirements in shapes
        ),
    )


def matching_group_shape_set_preimage(
    shapes: Sequence[tuple[str, int, Sequence[tuple[int, str]]]],
) -> bytes:
    return stable_m5_preimage(
        "m5-persisted-matching-group-shape-set-v1",
        _seq(
            _seq(
                (
                    text_field(group_id),
                    int_field(requirement_count),
                    _seq(
                        _seq((int_field(ordinal), text_field(requirement_id)))
                        for ordinal, requirement_id in requirements
                    ),
                )
            )
            for group_id, requirement_count, requirements in shapes
        ),
    )


def matching_point_fields(point: Any) -> tuple[str, ...]:
    """Encode one explicitly allowlisted D25 current/working point."""

    from groundloop.m5.runtime import contracts as contract_types

    name = type(point).__name__
    allowed = {
        contract_types.M5MatchingObservationCurrent,
        contract_types.M5MatchingObservationWorking,
        contract_types.M5MatchingEdgeCurrent,
        contract_types.M5MatchingEdgeWorking,
        contract_types.M5MatchingMaskCurrent,
        contract_types.M5MatchingMaskWorking,
        contract_types.M5MatchingHallCurrent,
        contract_types.M5MatchingHallWorking,
    }
    if type(point) not in allowed:
        raise ValidationError("unsupported D25 matching point")
    enum = enum_field(point.layer)
    if name == "M5MatchingObservationCurrent":
        return _seq(
            (
                enum,
                text_field(point.observation_id),
                text_field(point.requirement_version_id),
                text_field(point.group_version_id),
                int_field(point.requirement_ordinal),
                hash_field(point.text_hash),
                int_field(point.installed_epoch_id),
                int_field(point.installed_revision),
            )
        )
    if name == "M5MatchingObservationWorking":
        return _seq(
            (
                enum,
                int_field(point.epoch_id),
                text_field(point.observation_id),
                text_field(point.requirement_version_id),
                text_field(point.group_version_id),
                int_field(point.requirement_ordinal),
                hash_field(point.text_hash),
                bool_field(point.present),
                int_field(point.updated_revision),
            )
        )
    if name == "M5MatchingEdgeCurrent":
        return _seq(
            (
                enum,
                text_field(point.requirement_version_id),
                hash_field(point.text_hash),
                text_field(point.group_version_id),
                int_field(point.requirement_ordinal),
                int_field(point.refcount),
                int_field(point.installed_epoch_id),
                int_field(point.installed_revision),
            )
        )
    if name == "M5MatchingEdgeWorking":
        return _seq(
            (
                enum,
                int_field(point.epoch_id),
                text_field(point.requirement_version_id),
                hash_field(point.text_hash),
                text_field(point.group_version_id),
                int_field(point.requirement_ordinal),
                int_field(point.refcount),
                int_field(point.updated_revision),
            )
        )
    if name == "M5MatchingMaskCurrent":
        return _seq(
            (
                enum,
                text_field(point.group_version_id),
                hash_field(point.text_hash),
                int_field(point.mask),
                int_field(point.installed_epoch_id),
                int_field(point.installed_revision),
            )
        )
    if name == "M5MatchingMaskWorking":
        return _seq(
            (
                enum,
                int_field(point.epoch_id),
                text_field(point.group_version_id),
                hash_field(point.text_hash),
                int_field(point.mask),
                int_field(point.updated_revision),
            )
        )
    if name in {"M5MatchingHallCurrent", "M5MatchingHallWorking"}:
        if name.endswith("Current"):
            return _seq(
                (
                    enum,
                    text_field(point.group_version_id),
                    int_field(point.requirement_count),
                    _seq(int_field(v) for v in point.mask_histogram),
                    _seq(int_field(v) for v in point.neighbor_counts),
                    _seq(int_field(v) for v in point.deficiencies),
                    int_field(point.maximum_deficiency),
                    int_field(point.matching_size),
                    int_field(point.distinct_hash_count),
                    int_field(point.installed_epoch_id),
                    int_field(point.installed_revision),
                )
            )
        return _seq(
            (
                enum,
                int_field(point.epoch_id),
                text_field(point.group_version_id),
                bool_field(point.present),
                option_field(
                    None
                    if point.requirement_count is None
                    else int_field(point.requirement_count)
                ),
                option_field(
                    None
                    if point.mask_histogram is None
                    else _seq(int_field(v) for v in point.mask_histogram)
                ),
                option_field(
                    None
                    if point.neighbor_counts is None
                    else _seq(int_field(v) for v in point.neighbor_counts)
                ),
                option_field(
                    None
                    if point.deficiencies is None
                    else _seq(int_field(v) for v in point.deficiencies)
                ),
                option_field(
                    None
                    if point.maximum_deficiency is None
                    else int_field(point.maximum_deficiency)
                ),
                option_field(
                    None
                    if point.matching_size is None
                    else int_field(point.matching_size)
                ),
                option_field(
                    None
                    if point.distinct_hash_count is None
                    else int_field(point.distinct_hash_count)
                ),
                int_field(point.updated_revision),
            )
        )
    raise ValidationError("unsupported D25 matching point")


def matching_change_digest(
    domain: str,
    outer_fields: Sequence[tuple[str, ...]],
    before: object | None,
    after: object | None,
) -> str:
    if before is None and after is None:
        raise ValidationError("a matching change requires a before or after point")
    return stable_m5_digest(
        domain,
        *outer_fields,
        option_field(None if before is None else matching_point_fields(before)),
        option_field(None if after is None else matching_point_fields(after)),
    )


def matching_change_preimage(
    domain: str,
    outer_fields: Sequence[tuple[str, ...]],
    before: object | None,
    after: object | None,
) -> bytes:
    if before is None and after is None:
        raise ValidationError("a matching change requires a before or after point")
    return stable_m5_preimage(
        domain,
        *outer_fields,
        option_field(None if before is None else matching_point_fields(before)),
        option_field(None if after is None else matching_point_fields(after)),
    )


def logical_overlay_patch_digest(
    changes: Sequence[tuple[str | Enum, str, str | None, str | None]],
    binding_row_digests: Sequence[str],
    logical_output_digest: str,
    output_bytes: int,
) -> str:
    return stable_m5_digest(
        "m5-persisted-logical-overlay-patch-v1",
        _seq(
            _seq(
                (
                    enum_field(kind),
                    text_field(object_id),
                    option_field(None if before is None else hash_field(before)),
                    option_field(None if after is None else hash_field(after)),
                )
            )
            for kind, object_id, before, after in changes
        ),
        _seq(hash_field(v) for v in binding_row_digests),
        hash_field(logical_output_digest),
        int_field(output_bytes),
    )


def logical_overlay_patch_preimage(
    changes: Sequence[tuple[str | Enum, str, str | None, str | None]],
    binding_row_digests: Sequence[str],
    logical_output_digest: str,
    output_bytes: int,
) -> bytes:
    return stable_m5_preimage(
        "m5-persisted-logical-overlay-patch-v1",
        _seq(
            _seq(
                (
                    enum_field(kind),
                    text_field(object_id),
                    option_field(None if before is None else hash_field(before)),
                    option_field(None if after is None else hash_field(after)),
                )
            )
            for kind, object_id, before, after in changes
        ),
        _seq(hash_field(v) for v in binding_row_digests),
        hash_field(logical_output_digest),
        int_field(output_bytes),
    )


def certificate_binding_row_digest(
    kind: str | Enum,
    epoch_id: int,
    object_id: str,
    valid_from_revision: int,
    valid_to_revision: int | None,
    certificate_digest: str,
) -> str:
    return stable_m5_digest(
        "m5-persisted-certificate-binding-row-v1",
        enum_field(kind),
        int_field(epoch_id),
        text_field(object_id),
        int_field(valid_from_revision),
        option_field(
            None if valid_to_revision is None else int_field(valid_to_revision)
        ),
        hash_field(certificate_digest),
    )


def persisted_matching_patch_digest(
    *,
    source_kind: str | Enum,
    source_id: str,
    source_identity_hash: str,
    before_epoch_id: int,
    before_revision: int,
    resulting_epoch_id: int,
    resulting_revision: int,
    decision_policy_version: str,
    group_shape_set_digest: str,
    observation_change_digests: Sequence[str],
    edge_change_digests: Sequence[str],
    mask_change_digests: Sequence[str],
    hall_change_digests: Sequence[str],
    logical_overlay_patch_digest_value: str,
    matching_work_digest_value: str,
) -> str:
    return stable_m5_digest(
        "m5-persisted-matching-patch-v1",
        enum_field(source_kind),
        text_field(source_id),
        hash_field(source_identity_hash),
        int_field(before_epoch_id),
        int_field(before_revision),
        int_field(resulting_epoch_id),
        int_field(resulting_revision),
        text_field(decision_policy_version),
        hash_field(group_shape_set_digest),
        _seq(hash_field(v) for v in observation_change_digests),
        _seq(hash_field(v) for v in edge_change_digests),
        _seq(hash_field(v) for v in mask_change_digests),
        _seq(hash_field(v) for v in hall_change_digests),
        hash_field(logical_overlay_patch_digest_value),
        hash_field(matching_work_digest_value),
    )


def persisted_matching_patch_preimage(
    **values: Any,
) -> bytes:
    """Exact preimage companion for :func:`persisted_matching_patch_digest`."""
    return stable_m5_preimage(
        "m5-persisted-matching-patch-v1",
        enum_field(values["source_kind"]),
        text_field(values["source_id"]),
        hash_field(values["source_identity_hash"]),
        int_field(values["before_epoch_id"]),
        int_field(values["before_revision"]),
        int_field(values["resulting_epoch_id"]),
        int_field(values["resulting_revision"]),
        text_field(values["decision_policy_version"]),
        hash_field(values["group_shape_set_digest"]),
        _seq(hash_field(v) for v in values["observation_change_digests"]),
        _seq(hash_field(v) for v in values["edge_change_digests"]),
        _seq(hash_field(v) for v in values["mask_change_digests"]),
        _seq(hash_field(v) for v in values["hall_change_digests"]),
        hash_field(values["logical_overlay_patch_digest_value"]),
        hash_field(values["matching_work_digest_value"]),
    )


def matching_work_contribution_digest(
    *,
    epoch_id: int,
    source_kind: str | Enum,
    source_id: str,
    source_identity_hash: str,
    before_epoch_id: int,
    before_revision: int,
    resulting_revision: int,
    patch_digest: str,
    matching_work_digest_value: str,
) -> str:
    return stable_m5_digest(
        "m5-matching-work-contribution-v1",
        int_field(epoch_id),
        enum_field(source_kind),
        text_field(source_id),
        hash_field(source_identity_hash),
        int_field(before_epoch_id),
        int_field(before_revision),
        int_field(resulting_revision),
        hash_field(patch_digest),
        hash_field(matching_work_digest_value),
    )


def persisted_matching_transition_intent_digest(
    *,
    source_kind: str | Enum,
    source_id: str,
    source_identity_hash: str,
    before_epoch_id: int,
    before_revision: int,
    resulting_epoch_id: int,
    resulting_revision: int,
    decision_policy_version: str,
    group_shapes: Sequence[tuple[str, int, Sequence[tuple[int, str]]]],
    observation_ids: Sequence[str],
    edge_keys: Sequence[tuple[str, int, str, str]],
    mask_keys: Sequence[tuple[str, str]],
    hall_group_ids: Sequence[str],
    requirement_state_ids: Sequence[str],
    group_state_ids: Sequence[str],
    claim_state_ids: Sequence[str],
    answer_state_ids: Sequence[str],
    group_certificate_ids: Sequence[str],
    claim_certificate_ids: Sequence[str],
) -> str:
    return stable_m5_digest(
        "m5-persisted-matching-transition-intent-v1",
        enum_field(source_kind),
        text_field(source_id),
        hash_field(source_identity_hash),
        int_field(before_epoch_id),
        int_field(before_revision),
        int_field(resulting_epoch_id),
        int_field(resulting_revision),
        text_field(decision_policy_version),
        _seq(
            _seq(
                (
                    text_field(g),
                    int_field(c),
                    _seq(_seq((int_field(o), text_field(r))) for o, r in rs),
                )
            )
            for g, c, rs in group_shapes
        ),
        _seq(text_field(v) for v in observation_ids),
        _seq(
            _seq((text_field(g), int_field(o), hash_field(h), text_field(r)))
            for g, o, h, r in edge_keys
        ),
        _seq(_seq((text_field(g), hash_field(h))) for g, h in mask_keys),
        *(
            _seq(text_field(v) for v in values)
            for values in (
                hall_group_ids,
                requirement_state_ids,
                group_state_ids,
                claim_state_ids,
                answer_state_ids,
                group_certificate_ids,
                claim_certificate_ids,
            )
        ),
    )


def audit_key_fields(family: str | Enum, key: Sequence[str]) -> tuple[str, ...]:
    wire = _enum_wire(family)
    if wire not in {"observation", "edge", "mask", "hall"}:
        raise ValidationError("invalid D25 audit family")
    expected = 2 if wire in {"edge", "mask"} else 1
    if len(key) != expected:
        raise ValidationError("invalid D25 audit key arity")
    return (
        _seq((text_field(key[0]), hash_field(key[1])))
        if expected == 2
        else _seq((text_field(key[0]),))
    )


def physical_mismatch_fields(
    family: str | Enum,
    key: Sequence[str],
    expected: str | None,
    actual: str | None,
    error: str | Enum | None,
) -> tuple[str, ...]:
    return _seq(
        (
            enum_field(family),
            audit_key_fields(family, key),
            option_field(None if expected is None else hash_field(expected)),
            option_field(None if actual is None else hash_field(actual)),
            option_field(None if error is None else enum_field(error)),
        )
    )


def provenance_mismatch_fields(
    kind: str | Enum, epoch_id: int, expected: str | None, actual: str | None
) -> tuple[str, ...]:
    return _seq(
        (
            enum_field(kind),
            int_field(epoch_id),
            option_field(None if expected is None else hash_field(expected)),
            option_field(None if actual is None else hash_field(actual)),
        )
    )


def physical_audit_digest(
    *,
    head_epoch_id: int,
    head_revision: int,
    decision_policy_version: str,
    python_expected_projection_digest: str,
    sql_expected_projection_digest: str,
    actual_projection_digest: str | None,
    actual_error: str | Enum | None,
    provenance_ok: bool,
    provenance_replay_digest: str | None,
    working_image_expected_provenance_digest: str | None,
    working_image_actual_provenance_digest: str | None,
    accumulator_expected_provenance_digest: str | None,
    accumulator_actual_provenance_digest: str | None,
    mismatches: Sequence[
        tuple[str | Enum, Sequence[str], str | None, str | None, str | Enum | None]
    ],
    provenance_mismatches: Sequence[tuple[str | Enum, int, str | None, str | None]],
) -> str:
    return stable_m5_digest(
        "m5-persisted-matching-physical-audit-v1",
        int_field(head_epoch_id),
        int_field(head_revision),
        text_field(decision_policy_version),
        hash_field(python_expected_projection_digest),
        hash_field(sql_expected_projection_digest),
        option_field(
            None
            if actual_projection_digest is None
            else hash_field(actual_projection_digest)
        ),
        option_field(None if actual_error is None else enum_field(actual_error)),
        bool_field(provenance_ok),
        option_field(
            None
            if provenance_replay_digest is None
            else hash_field(provenance_replay_digest)
        ),
        option_field(
            None
            if working_image_expected_provenance_digest is None
            else hash_field(working_image_expected_provenance_digest)
        ),
        option_field(
            None
            if working_image_actual_provenance_digest is None
            else hash_field(working_image_actual_provenance_digest)
        ),
        option_field(
            None
            if accumulator_expected_provenance_digest is None
            else hash_field(accumulator_expected_provenance_digest)
        ),
        option_field(
            None
            if accumulator_actual_provenance_digest is None
            else hash_field(accumulator_actual_provenance_digest)
        ),
        _seq(physical_mismatch_fields(*v) for v in mismatches),
        _seq(provenance_mismatch_fields(*v) for v in provenance_mismatches),
    )


def persisted_matching_schema_bundle_digest(
    migration_017_sha256: str, accepted_016_bundle_sha256: str
) -> str:
    return stable_m5_digest(
        "m5-persisted-matching-schema-bundle-v1",
        text_field("migrations/017_m5_persisted_matching.sql"),
        hash_field(migration_017_sha256),
        hash_field(accepted_016_bundle_sha256),
    )


def physical_audit_family_digest(
    family: str | Enum, rows: Sequence[tuple[str, ...]]
) -> str:
    return stable_m5_digest(
        "m5-persisted-matching-physical-audit-family-v1",
        enum_field(family),
        int_field(len(rows)),
        _seq(rows),
    )


def audit_observation_fields(
    observation_id: str,
    requirement_version_id: str,
    group_version_id: str,
    requirement_ordinal: int,
    text_hash: str,
) -> tuple[str, ...]:
    return _seq(
        (
            text_field(observation_id),
            text_field(requirement_version_id),
            text_field(group_version_id),
            int_field(requirement_ordinal),
            hash_field(text_hash),
        )
    )


def audit_edge_fields(
    requirement_version_id: str,
    text_hash: str,
    group_version_id: str,
    requirement_ordinal: int,
    refcount: int,
) -> tuple[str, ...]:
    return _seq(
        (
            text_field(requirement_version_id),
            hash_field(text_hash),
            text_field(group_version_id),
            int_field(requirement_ordinal),
            int_field(refcount),
        )
    )


def audit_mask_fields(
    group_version_id: str, text_hash: str, mask: int
) -> tuple[str, ...]:
    return _seq((text_field(group_version_id), hash_field(text_hash), int_field(mask)))


def audit_hall_fields(
    group_version_id: str,
    requirement_count: int,
    mask_histogram: Sequence[int],
    neighbor_counts: Sequence[int],
    deficiencies: Sequence[int],
    maximum_deficiency: int,
    matching_size: int,
    distinct_hash_count: int,
) -> tuple[str, ...]:
    return _seq(
        (
            text_field(group_version_id),
            int_field(requirement_count),
            _seq(int_field(v) for v in mask_histogram),
            _seq(int_field(v) for v in neighbor_counts),
            _seq(int_field(v) for v in deficiencies),
            int_field(maximum_deficiency),
            int_field(matching_size),
            int_field(distinct_hash_count),
        )
    )


def patch_provenance_fields(
    epoch_id: int,
    resulting_revision: int,
    source_kind: str | Enum,
    source_id: str,
    status: str | Enum,
    patch_digest: str,
    contribution_digest: str,
) -> tuple[str, ...]:
    return _seq(
        (
            int_field(epoch_id),
            int_field(resulting_revision),
            enum_field(source_kind),
            text_field(source_id),
            enum_field(status),
            hash_field(patch_digest),
            hash_field(contribution_digest),
        )
    )


def working_image_provenance_fields(
    epoch_id: int,
    status: str | Enum,
    terminal_revision: int | None,
    base_epoch_id: int,
    base_revision: int,
    decision_policy_version: str,
    updated_revision: int,
    last_patch_digest: str,
) -> tuple[str, ...]:
    return _seq(
        (
            int_field(epoch_id),
            enum_field(status),
            option_field(
                None if terminal_revision is None else int_field(terminal_revision)
            ),
            int_field(base_epoch_id),
            int_field(base_revision),
            text_field(decision_policy_version),
            int_field(updated_revision),
            hash_field(last_patch_digest),
        )
    )


def accumulator_provenance_fields(
    epoch_id: int,
    status: str | Enum,
    counters: Sequence[int],
    matching_work_digest_value: str,
    updated_revision: int,
) -> tuple[str, ...]:
    if len(counters) != 37:
        raise ValidationError("accumulator provenance requires 37 counters")
    return _seq(
        (
            int_field(epoch_id),
            enum_field(status),
            *(int_field(v) for v in counters),
            hash_field(matching_work_digest_value),
            int_field(updated_revision),
        )
    )


def working_image_provenance_row_digest(row: tuple[str, ...]) -> str:
    if row[:3] != ("sequence", "int", "8"):
        raise ValidationError("working-image provenance row has invalid framing")
    return stable_m5_digest(
        "m5-persisted-matching-working-image-provenance-row-v1", row[3:]
    )


def accumulator_provenance_row_digest(row: tuple[str, ...]) -> str:
    if row[:3] != ("sequence", "int", "41"):
        raise ValidationError("accumulator provenance row has invalid framing")
    return stable_m5_digest(
        "m5-persisted-matching-accumulator-provenance-row-v1", row[3:]
    )


def current_provenance_fields(
    family: str | Enum,
    key: Sequence[str],
    point: Any,
    last_touch_patch_digest: str | None,
) -> tuple[str, ...]:
    point_fields = matching_point_fields(point)
    if point.layer.value != "current":
        raise ValidationError("current provenance requires a current point")
    if type(point).__name__ != f"M5Matching{_enum_wire(family).title()}Current":
        raise ValidationError("current provenance family and point disagree")
    return _seq(
        (
            enum_field(family),
            audit_key_fields(family, key),
            point_fields,
            option_field(
                None
                if last_touch_patch_digest is None
                else hash_field(last_touch_patch_digest)
            ),
        )
    )


def working_provenance_fields(
    family: str | Enum,
    key: Sequence[str],
    point: Any,
    last_touch_patch_digest: str,
) -> tuple[str, ...]:
    point_fields = matching_point_fields(point)
    if point.layer.value != "working":
        raise ValidationError("working provenance requires a working point")
    if type(point).__name__ != f"M5Matching{_enum_wire(family).title()}Working":
        raise ValidationError("working provenance family and point disagree")
    return _seq(
        (
            enum_field(family),
            audit_key_fields(family, key),
            point_fields,
            hash_field(last_touch_patch_digest),
        )
    )


def physical_audit_projection_digest(
    head_epoch_id: int,
    head_revision: int,
    decision_policy_version: str,
    observation_family_digest: str,
    edge_family_digest: str,
    mask_family_digest: str,
    hall_family_digest: str,
) -> str:
    return stable_m5_digest(
        "m5-persisted-matching-physical-audit-projection-v1",
        int_field(head_epoch_id),
        int_field(head_revision),
        text_field(decision_policy_version),
        hash_field(observation_family_digest),
        hash_field(edge_family_digest),
        hash_field(mask_family_digest),
        hash_field(hall_family_digest),
    )


def physical_audit_row_digest(family: str | Enum, row: tuple[str, ...]) -> str:
    return stable_m5_digest(
        "m5-persisted-matching-physical-audit-row-v1",
        enum_field(family),
        row,
    )


def working_image_provenance_digest(rows: Sequence[tuple[str, ...]]) -> str:
    return stable_m5_digest(
        "m5-persisted-matching-working-image-provenance-v1", _seq(rows)
    )


def accumulator_provenance_digest(rows: Sequence[tuple[str, ...]]) -> str:
    return stable_m5_digest(
        "m5-persisted-matching-accumulator-provenance-v1", _seq(rows)
    )


def provenance_replay_digest(
    head_epoch_id: int,
    head_revision: int,
    patches: Sequence[tuple[str, ...]],
    working_images: Sequence[tuple[str, ...]],
    accumulators: Sequence[tuple[str, ...]],
    current_rows: Sequence[tuple[str, ...]],
    working_rows: Sequence[tuple[str, ...]],
) -> str:
    return stable_m5_digest(
        "m5-persisted-matching-provenance-replay-v1",
        int_field(head_epoch_id),
        int_field(head_revision),
        _seq(patches),
        _seq(working_images),
        _seq(accumulators),
        _seq(current_rows),
        _seq(working_rows),
    )


_LOGICAL_WIRE_FIELDS = {
    "RequirementState": (
        "requirement_version_id",
        "witness_hashes",
        "supporting_observation_ids",
        "witness_count",
        "satisfied",
    ),
    "GroupState": (
        "group_version_id",
        "requirement_count",
        "satisfied_count",
        "matching_size",
        "complete",
    ),
    "CombinedClaimState": (
        "claim_id",
        "support_count",
        "refute_count",
        "best_support_score",
        "best_refute_score",
        "supporting_observation_ids",
        "refuting_observation_ids",
        "complete_group_count",
        "complete_group_ids",
        "status",
    ),
    "CombinedAnswerState": (
        "answer_version_id",
        "required_claim_count",
        "supported_count",
        "unsupported_count",
        "refuted_count",
        "conflicted_count",
        "status",
    ),
    "GroupCertificateRow": (
        "requirement_ordinal",
        "requirement_version_id",
        "text_hash",
        "selected_observation_id",
    ),
    "GroupMatchingCertificateArtifact": (
        "decision_policy_version",
        "group_version_id",
        "rows",
        "certificate_version",
        "certificate_digest",
    ),
    "ClaimCertificateArtifact": (
        "claim_id",
        "decision_policy_version",
        "support_kind",
        "direct_support_observation_id",
        "group_version_id",
        "group_certificate_digest",
        "direct_refute_observation_id",
        "certificate_version",
        "certificate_digest",
    ),
    "WorkingGroupCertificateBinding": (
        "epoch_id",
        "group_version_id",
        "valid_from_revision",
        "valid_to_revision",
        "certificate_digest",
    ),
    "WorkingClaimCertificateBinding": (
        "epoch_id",
        "claim_id",
        "valid_from_revision",
        "valid_to_revision",
        "certificate_digest",
    ),
    "StatusDelta": (
        "event_id",
        "object_type",
        "object_id",
        "old_status",
        "new_status",
        "reason",
    ),
}


def _frame_bytes(value: bytes) -> bytes:
    return len(value).to_bytes(8, "big") + value


def _logical_value_bytes(value: object) -> bytes:
    if value is None:
        return b"n"
    if type(value) is bool:
        return b"b\x01" if value else b"b\x00"
    if isinstance(value, Enum):
        if type(value.value) is not str or not value.value:
            raise ValidationError("logical enums require nonempty string values")
        return b"e" + _frame_bytes(value.value.encode())
    if type(value) is str:
        return b"s" + _frame_bytes(value.encode())
    if type(value) is int:
        return b"i" + _frame_bytes(str(value).encode("ascii"))
    if type(value) is float:
        if not math.isfinite(value):
            raise ValidationError("logical F64 values must be finite")
        return b"f" + struct.pack(">d", value)
    if type(value) is tuple:
        return (
            b"q"
            + len(value).to_bytes(8, "big")
            + b"".join(_frame_bytes(_logical_value_bytes(item)) for item in value)
        )
    name = type(value).__name__
    names = _LOGICAL_WIRE_FIELDS.get(name)
    if names is None:
        raise ValidationError("unsupported logical-output value")
    from groundloop.domain import StatusDelta
    from groundloop.m5.claim_certificates import WorkingClaimCertificateBinding
    from groundloop.m5.domain import (
        ClaimCertificateArtifact,
        CombinedAnswerState,
        CombinedClaimState,
        GroupCertificateRow,
        GroupMatchingCertificateArtifact,
        GroupState,
        RequirementState,
    )
    from groundloop.m5.matching import WorkingGroupCertificateBinding

    allowed = {
        RequirementState,
        GroupState,
        CombinedClaimState,
        CombinedAnswerState,
        GroupCertificateRow,
        GroupMatchingCertificateArtifact,
        ClaimCertificateArtifact,
        WorkingGroupCertificateBinding,
        WorkingClaimCertificateBinding,
        StatusDelta,
    }
    if type(value) not in allowed:
        raise ValidationError("unsupported logical-output value")
    return (
        b"d"
        + _frame_bytes(name.encode())
        + len(names).to_bytes(8, "big")
        + b"".join(
            _frame_bytes(field_name.encode())
            + _frame_bytes(_logical_value_bytes(getattr(value, field_name)))
            for field_name in names
        )
    )


def logical_output_preimage(records: Sequence[tuple[str, str, object]]) -> bytes:
    kinds = (
        "requirement_state",
        "group_state",
        "claim_state",
        "answer_state",
        "group_certificate",
        "claim_certificate",
        "group_binding",
        "claim_binding",
        "status_delta",
    )
    ranks = {value: rank for rank, value in enumerate(kinds)}
    if type(records) is not tuple:
        raise ValidationError("logical-output records must be an exact tuple")
    from groundloop.domain import StatusDelta
    from groundloop.m5.claim_certificates import WorkingClaimCertificateBinding
    from groundloop.m5.domain import (
        ClaimCertificateArtifact,
        CombinedAnswerState,
        CombinedClaimState,
        GroupMatchingCertificateArtifact,
        GroupState,
        RequirementState,
    )
    from groundloop.m5.matching import WorkingGroupCertificateBinding

    value_laws: dict[str, tuple[type[object], str, bool]] = {
        "requirement_state": (RequirementState, "requirement_version_id", True),
        "group_state": (GroupState, "group_version_id", True),
        "claim_state": (CombinedClaimState, "claim_id", True),
        "answer_state": (CombinedAnswerState, "answer_version_id", True),
        "group_certificate": (
            GroupMatchingCertificateArtifact,
            "group_version_id",
            True,
        ),
        "claim_certificate": (ClaimCertificateArtifact, "claim_id", True),
        "group_binding": (
            WorkingGroupCertificateBinding,
            "group_version_id",
            False,
        ),
        "claim_binding": (WorkingClaimCertificateBinding, "claim_id", False),
        "status_delta": (StatusDelta, "object_id", False),
    }
    previous = -1
    for record in records:
        if type(record) is not tuple or len(record) != 3:
            raise ValidationError("logical-output records must be exact 3-tuples")
        kind, object_id, after = record
        if (
            type(kind) is not str
            or type(object_id) is not str
            or kind not in ranks
            or not object_id
            or ranks[kind] < previous
        ):
            raise ValidationError("invalid logical-output relation order")
        value_type, key_field, nullable = value_laws[kind]
        if after is None:
            if not nullable:
                raise ValidationError(f"{kind} logical output cannot be absent")
        elif type(after) is not value_type or getattr(after, key_field) != object_id:
            raise ValidationError(f"{kind} logical output has wrong value/key")
        if kind == "status_delta" and after is not None:
            if after.object_type not in {"claim", "answer"}:
                raise ValidationError("status delta has invalid object type")
        previous = ranks[kind]
    return _logical_value_bytes(("m5-overlay-logical-output-v2", tuple(records)))


def logical_output_digest(
    records: Sequence[tuple[str, str, object]],
) -> tuple[str, int, bytes]:
    preimage = logical_output_preimage(records)
    return hashlib.sha256(preimage).hexdigest(), len(preimage), preimage


def _enum_wire(value: str | Enum) -> str:
    wire_value = value.value if isinstance(value, Enum) else value
    if not isinstance(wire_value, str) or not wire_value:
        raise ValidationError("enum wire values must be nonempty strings")
    return wire_value


def text_normalizer_provenance_digest(
    *,
    normalizer_id: str,
    whitespace_codepoints: Iterable[int],
    boundary_rule: str,
    internal_rule: str,
    other_codepoint_rule: str,
    unicode_normalization_rule: str,
    encoding: str,
    hash_algorithm: str,
) -> str:
    return stable_m5_digest(
        "m5-text-normalizer-provenance-v1",
        text_field(normalizer_id),
        sequence_field(int_field(value) for value in whitespace_codepoints),
        text_field(boundary_rule),
        text_field(internal_rule),
        text_field(other_codepoint_rule),
        text_field(unicode_normalization_rule),
        text_field(encoding),
        text_field(hash_algorithm),
    )


def candidate_policy_manifest_digest(
    *,
    embedding_model_artifact_id: str,
    requirement_role_template_hash: str,
    chunk_role_template_hash: str,
    vector_method_version: str,
    vector_index_kind: str | Enum,
    vector_index_build_config_hash: str,
    vector_search_config_hash: str,
    lexical_method_version: str,
    lexical_config_hash: str,
    lexical_postgres_version: str,
    lexical_regconfig_identity: str,
    fusion_version: str,
    reverse_budget_per_inserted_chunk: int,
    forward_budget_per_requirement: int,
    verifier_execution_spec_hash: str,
    decision_policy_version: str,
    lineage_safety_override: bool,
) -> str:
    return stable_m5_digest(
        "m5-candidate-policy-v2",
        text_field(embedding_model_artifact_id),
        hash_field(requirement_role_template_hash),
        hash_field(chunk_role_template_hash),
        text_field(vector_method_version),
        enum_field(vector_index_kind),
        hash_field(vector_index_build_config_hash),
        hash_field(vector_search_config_hash),
        text_field(lexical_method_version),
        hash_field(lexical_config_hash),
        text_field(lexical_postgres_version),
        text_field(lexical_regconfig_identity),
        text_field(fusion_version),
        int_field(reverse_budget_per_inserted_chunk),
        int_field(forward_budget_per_requirement),
        hash_field(verifier_execution_spec_hash),
        text_field(decision_policy_version),
        bool_field(lineage_safety_override),
    )


def semantic_pair_digest(
    subject_kind: SubjectKind | str,
    subject_id: str,
    chunk_version_id: str,
) -> str:
    return stable_m5_digest(
        "m5-semantic-pair-v2",
        enum_field(subject_kind),
        text_field(subject_id),
        text_field(chunk_version_id),
    )


def requirement_registry_snapshot_digest(
    entries: Sequence[RequirementRegistryRow],
) -> str:
    return stable_m5_digest(
        "m5-requirement-registry-snapshot-v2",
        int_field(len(entries)),
        sequence_field(
            sequence_field(
                (
                    text_field(requirement_version_id),
                    text_field(group_version_id),
                    text_field(group_family_id),
                    text_field(owner_claim_id),
                    text_field(normalized_requirement_text),
                    hash_field(requirement_text_hash),
                )
            )
            for (
                requirement_version_id,
                group_version_id,
                group_family_id,
                owner_claim_id,
                normalized_requirement_text,
                requirement_text_hash,
            ) in entries
        ),
    )


def active_chunk_snapshot_digest(entries: Sequence[ActiveChunkRow]) -> str:
    return stable_m5_digest(
        "m5-active-chunk-snapshot-v2",
        int_field(len(entries)),
        sequence_field(
            sequence_field((text_field(chunk_version_id), hash_field(text_hash)))
            for chunk_version_id, text_hash in entries
        ),
    )


def discovery_scope_contract_digest(
    *,
    direction: str | Enum,
    requirement_version_id: str | None,
    inserted_chunk_version_id: str | None,
    candidate_policy_id: str,
    requirement_registry_snapshot_digest_value: str,
    active_chunk_snapshot_digest_value: str,
) -> str:
    return stable_m5_digest(
        "m5-discovery-scope-contract-v2",
        enum_field(direction),
        option_field(
            text_field(requirement_version_id)
            if requirement_version_id is not None
            else None
        ),
        option_field(
            text_field(inserted_chunk_version_id)
            if inserted_chunk_version_id is not None
            else None
        ),
        text_field(candidate_policy_id),
        hash_field(requirement_registry_snapshot_digest_value),
        hash_field(active_chunk_snapshot_digest_value),
    )


def discovery_scope_closure_digest(
    scope_contract_digest: str,
    semantic_pair_digests: Iterable[str],
) -> str:
    canonical = tuple(sorted(set(semantic_pair_digests)))
    return stable_m5_digest(
        "m5-discovery-scope-closure-v2",
        hash_field(scope_contract_digest),
        sequence_field(hash_field(value) for value in canonical),
    )


def job_payload_digest(
    *,
    job_kind: str | Enum,
    candidate_policy_id: str,
    candidate_policy_manifest_hash: str,
    parent_job_id: str | None,
    semantic_pair_digest_value: str | None,
    scope_contract_digest: str | None,
    requirement_registry_snapshot_digest_value: str,
    active_chunk_snapshot_digest_value: str,
    role_template_hash: str,
    execution_spec_hash: str,
    expandable: bool,
) -> str:
    return stable_m5_digest(
        "m5-job-payload-v2",
        enum_field(job_kind),
        text_field(candidate_policy_id),
        hash_field(candidate_policy_manifest_hash),
        option_field(text_field(parent_job_id) if parent_job_id is not None else None),
        option_field(
            hash_field(semantic_pair_digest_value)
            if semantic_pair_digest_value is not None
            else None
        ),
        option_field(
            hash_field(scope_contract_digest)
            if scope_contract_digest is not None
            else None
        ),
        hash_field(requirement_registry_snapshot_digest_value),
        hash_field(active_chunk_snapshot_digest_value),
        hash_field(role_template_hash),
        hash_field(execution_spec_hash),
        bool_field(expandable),
    )


def logical_job_id(structural_event_id: str, payload_hash: str) -> str:
    return stable_m5_digest(
        "m5-logical-job-v2",
        text_field(structural_event_id),
        hash_field(payload_hash),
    )


def child_set_digest(child_job_ids: Iterable[str]) -> str:
    canonical = tuple(sorted(set(child_job_ids)))
    return stable_m5_digest(
        "m5-child-set-v2",
        sequence_field(text_field(value) for value in canonical),
    )


def job_completion_digest(
    *,
    logical_job_id_value: str,
    payload_hash: str,
    execution_spec_hash: str,
    terminal_state: str | Enum,
    result_artifact_id: str | None,
    result_artifact_hash: str | None,
    scope_closure_digest: str | None,
    child_set_hash: str | None,
    archive_reason: str | Enum | None,
) -> str:
    return stable_m5_digest(
        "m5-job-completion-v2",
        text_field(logical_job_id_value),
        hash_field(payload_hash),
        hash_field(execution_spec_hash),
        enum_field(terminal_state),
        option_field(
            text_field(result_artifact_id) if result_artifact_id is not None else None
        ),
        option_field(
            hash_field(result_artifact_hash)
            if result_artifact_hash is not None
            else None
        ),
        option_field(
            hash_field(scope_closure_digest)
            if scope_closure_digest is not None
            else None
        ),
        option_field(
            hash_field(child_set_hash) if child_set_hash is not None else None
        ),
        option_field(
            enum_field(archive_reason) if archive_reason is not None else None
        ),
    )


def forward_retrieval_execution_spec_digest(
    candidate_policy_manifest_hash: str,
    normalizer_provenance_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-forward-requirement-retrieval-execution-v2",
        hash_field(candidate_policy_manifest_hash),
        hash_field(normalizer_provenance_hash),
    )


def reverse_retrieval_execution_spec_digest(
    candidate_policy_manifest_hash: str,
    normalizer_provenance_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-reverse-requirement-discovery-execution-v2",
        hash_field(candidate_policy_manifest_hash),
        hash_field(normalizer_provenance_hash),
    )


def requirement_verifier_role_binding_digest(
    requirement_role_template_hash: str,
    chunk_role_template_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-requirement-verifier-role-binding-v2",
        hash_field(requirement_role_template_hash),
        hash_field(chunk_role_template_hash),
    )


def requirement_channel_hit_digest(
    *,
    epoch_id: int,
    root_job_id: str,
    scope_contract_digest: str,
    semantic_pair_digest_value: str,
    candidate_policy_id: str,
    channel: str | Enum,
    rank: int,
    score: float | None,
    channel_artifact_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-requirement-channel-hit-v2",
        int_field(epoch_id),
        text_field(root_job_id),
        hash_field(scope_contract_digest),
        hash_field(semantic_pair_digest_value),
        text_field(candidate_policy_id),
        enum_field(channel),
        int_field(rank),
        option_field(f64_field(score) if score is not None else None),
        hash_field(channel_artifact_hash),
    )


def requirement_scope_selection_digest(
    *,
    root_job_id: str,
    scope_contract_digest: str,
    semantic_pair_digest_value: str,
    fused_rank: int,
    reasons: Iterable[str | Enum],
    mandatory_lineage: bool,
) -> str:
    return stable_m5_digest(
        "m5-requirement-scope-selection-v2",
        text_field(root_job_id),
        hash_field(scope_contract_digest),
        hash_field(semantic_pair_digest_value),
        int_field(fused_rank),
        sequence_field(enum_field(reason) for reason in reasons),
        bool_field(mandatory_lineage),
    )


def requirement_discovery_result_digest(
    *,
    root_job_id: str,
    scope_contract_digest: str,
    termination: str | Enum,
    hit_digests: Iterable[str],
    selection_digests: Iterable[str],
    approximate_selection_count: int,
    mandatory_lineage_only_count: int,
) -> str:
    return stable_m5_digest(
        "m5-requirement-discovery-result-v2",
        text_field(root_job_id),
        hash_field(scope_contract_digest),
        enum_field(termination),
        sequence_field(hash_field(value) for value in hit_digests),
        sequence_field(hash_field(value) for value in selection_digests),
        int_field(approximate_selection_count),
        int_field(mandatory_lineage_only_count),
    )


def requirement_discovery_artifact_id(
    root_job_id: str, result_artifact_hash: str
) -> str:
    return stable_m5_digest(
        "m5-requirement-discovery-artifact-v2",
        text_field(root_job_id),
        hash_field(result_artifact_hash),
    )


def requirement_admitted_pair_digest(
    *,
    epoch_id: int,
    semantic_pair_digest_value: str,
    candidate_policy_id: str,
    owner_root_job_id: str,
    sources: Iterable[AdmittedPairSourceRow],
    reasons: Iterable[str | Enum],
    mandatory_lineage: bool,
) -> str:
    return stable_m5_digest(
        "m5-requirement-admitted-pair-v2",
        int_field(epoch_id),
        hash_field(semantic_pair_digest_value),
        text_field(candidate_policy_id),
        text_field(owner_root_job_id),
        sequence_field(
            sequence_field(
                (
                    text_field(root_job_id),
                    hash_field(scope_contract_digest),
                    hash_field(selection_digest),
                )
            )
            for root_job_id, scope_contract_digest, selection_digest in sources
        ),
        sequence_field(enum_field(reason) for reason in reasons),
        bool_field(mandatory_lineage),
    )


def requirement_pair_input_digest(
    *,
    semantic_pair_digest_value: str,
    scope_contract_digest: str,
    candidate_policy_id: str,
    owner_claim_id: str,
    group_version_id: str,
    group_family_id: str,
    requirement_ordinal: int,
    normalized_requirement_text: str,
    requirement_text_hash: str,
    document_version_id: str,
    chunk_index: int,
    chunk_text: str,
    stored_chunk_text_hash: str,
    m5_chunk_text_hash: str,
    chunker_artifact_id: str,
    normalizer_id: str,
    normalizer_provenance_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-requirement-pair-input-v2",
        hash_field(semantic_pair_digest_value),
        hash_field(scope_contract_digest),
        text_field(candidate_policy_id),
        text_field(owner_claim_id),
        text_field(group_version_id),
        text_field(group_family_id),
        int_field(requirement_ordinal),
        text_field(normalized_requirement_text),
        hash_field(requirement_text_hash),
        text_field(document_version_id),
        int_field(chunk_index),
        text_field(chunk_text),
        hash_field(stored_chunk_text_hash),
        hash_field(m5_chunk_text_hash),
        text_field(chunker_artifact_id),
        text_field(normalizer_id),
        hash_field(normalizer_provenance_hash),
    )


def requirement_verifier_result_digest(
    *,
    semantic_pair_digest_value: str,
    pair_input_hash: str,
    execution_spec_hash: str,
    model_artifact_id: str,
    model_id: str,
    model_revision: str,
    prompt_artifact_id: str,
    prompt_version: str,
    calibration_version: str,
    calibration_artifact_hash: str | None,
    temperature: float,
    decision_policy_version: str,
    decision_policy_hash: str,
    support_score: float,
    refute_score: float,
    neutral_score: float,
    raw_logits: Iterable[float],
    raw_output_hash: str,
    operational_label: str | Enum,
) -> str:
    return stable_m5_digest(
        "m5-requirement-verifier-result-v2",
        hash_field(semantic_pair_digest_value),
        hash_field(pair_input_hash),
        hash_field(execution_spec_hash),
        text_field(model_artifact_id),
        text_field(model_id),
        text_field(model_revision),
        text_field(prompt_artifact_id),
        text_field(prompt_version),
        text_field(calibration_version),
        option_field(
            hash_field(calibration_artifact_hash)
            if calibration_artifact_hash is not None
            else None
        ),
        f64_field(temperature),
        text_field(decision_policy_version),
        hash_field(decision_policy_hash),
        f64_field(support_score),
        f64_field(refute_score),
        f64_field(neutral_score),
        sequence_field(f64_field(value) for value in raw_logits),
        hash_field(raw_output_hash),
        enum_field(operational_label),
    )


def requirement_verifier_artifact_id(
    semantic_pair_digest_value: str,
    pair_input_hash: str,
    artifact_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-requirement-verifier-artifact-v2",
        hash_field(semantic_pair_digest_value),
        hash_field(pair_input_hash),
        hash_field(artifact_hash),
    )


def requirement_semantic_observation_id(
    verifier_artifact_id: str,
    task_type: str = "verify_requirement_v1",
) -> str:
    return stable_m5_digest(
        "m5-requirement-semantic-observation-v2",
        hash_field(verifier_artifact_id),
        text_field(task_type),
    )


def job_attempt_id(
    logical_job_id_value: str,
    attempt_ordinal: int,
    execution_spec_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-job-attempt-v2",
        text_field(logical_job_id_value),
        int_field(attempt_ordinal),
        hash_field(execution_spec_hash),
    )


def attempt_output_digest(
    *,
    attempt_id: str,
    logical_job_id_value: str,
    job_epoch_id: int,
    payload_hash: str,
    execution_spec_hash: str,
    result_artifact_id: str,
    result_artifact_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-attempt-output-v2",
        text_field(attempt_id),
        text_field(logical_job_id_value),
        int_field(job_epoch_id),
        hash_field(payload_hash),
        hash_field(execution_spec_hash),
        hash_field(result_artifact_id),
        hash_field(result_artifact_hash),
    )


def attempt_result_artifact_digest(
    attempt_output_digest_value: str,
    values: AttemptResultValues,
) -> str:
    (
        attempt_id,
        logical_job_id_value,
        job_epoch_id,
        job_state_at_receipt,
        job_state_after,
        disposition,
        activity_snapshot_epoch_id,
        activity_snapshot_revision,
        epoch_active,
        chunk_active,
        requirement_active,
        group_active,
        archive_reason,
        cancelled_by_event_id,
        cancelled_by_epoch_id,
        cancellation_reason,
    ) = values
    return stable_m5_digest(
        "m5-attempt-result-artifact-v2",
        hash_field(attempt_output_digest_value),
        text_field(attempt_id),
        text_field(logical_job_id_value),
        int_field(job_epoch_id),
        enum_field(job_state_at_receipt),
        enum_field(job_state_after),
        enum_field(disposition),
        int_field(activity_snapshot_epoch_id),
        int_field(activity_snapshot_revision),
        bool_field(epoch_active),
        option_field(bool_field(chunk_active) if chunk_active is not None else None),
        option_field(
            bool_field(requirement_active) if requirement_active is not None else None
        ),
        option_field(bool_field(group_active) if group_active is not None else None),
        option_field(
            enum_field(archive_reason) if archive_reason is not None else None
        ),
        option_field(
            text_field(cancelled_by_event_id)
            if cancelled_by_event_id is not None
            else None
        ),
        option_field(
            int_field(cancelled_by_epoch_id)
            if cancelled_by_epoch_id is not None
            else None
        ),
        option_field(
            enum_field(cancellation_reason) if cancellation_reason is not None else None
        ),
    )


def attempt_result_artifact_id(
    attempt_id: str, attempt_result_artifact_hash: str
) -> str:
    return stable_m5_digest(
        "m5-attempt-result-id-v2",
        text_field(attempt_id),
        hash_field(attempt_result_artifact_hash),
    )


def activation_request_digest(
    *,
    activation_id: str,
    expected_mode_revision: int,
    expected_base_m4_epoch_id: int,
    expected_m4_publication_id: str,
    core_schema_bundle_sha256: str,
    bootstrap_state_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-activation-request-v2",
        text_field(activation_id),
        int_field(expected_mode_revision),
        int_field(expected_base_m4_epoch_id),
        text_field(expected_m4_publication_id),
        hash_field(core_schema_bundle_sha256),
        hash_field(bootstrap_state_hash),
    )


def activation_receipt_digest(
    *,
    activation_id: str,
    payload_hash: str,
    base_m4_epoch_id: int,
    m4_publication_id: str,
    m5_publication_epoch_id: int,
    mode_revision: int,
    bootstrap_state_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-activation-receipt-v2",
        text_field(activation_id),
        hash_field(payload_hash),
        int_field(base_m4_epoch_id),
        text_field(m4_publication_id),
        int_field(m5_publication_epoch_id),
        int_field(mode_revision),
        hash_field(bootstrap_state_hash),
    )


def changed_state_reference_digest(
    values: ChangedStateReferenceValues,
) -> str:
    kind, object_id, epoch_id, revision, state_artifact_hash = values
    return stable_m5_digest(
        "m5-changed-state-reference-v2",
        enum_field(kind),
        text_field(object_id),
        int_field(epoch_id),
        int_field(revision),
        hash_field(state_artifact_hash),
    )


def requirement_state_artifact_digest(
    *,
    requirement_version_id: str,
    witness_hashes: Iterable[str],
    supporting_observation_ids: Iterable[str],
    witness_count: int,
    satisfied: bool,
    decision_policy_version: str,
) -> str:
    return stable_m5_digest(
        "m5-requirement-state-artifact-v2",
        text_field(requirement_version_id),
        sequence_field(hash_field(value) for value in witness_hashes),
        sequence_field(text_field(value) for value in supporting_observation_ids),
        int_field(witness_count),
        bool_field(satisfied),
        text_field(decision_policy_version),
    )


def group_state_artifact_digest(
    *,
    group_version_id: str,
    requirement_count: int,
    satisfied_count: int,
    matching_size: int,
    complete: bool,
    decision_policy_version: str,
    certificate_digest: str | None,
) -> str:
    return stable_m5_digest(
        "m5-group-state-artifact-v2",
        text_field(group_version_id),
        int_field(requirement_count),
        int_field(satisfied_count),
        int_field(matching_size),
        bool_field(complete),
        text_field(decision_policy_version),
        option_field(
            None if certificate_digest is None else hash_field(certificate_digest)
        ),
    )


def claim_state_artifact_digest(
    *,
    claim_id: str,
    support_count: int,
    refute_count: int,
    best_support_score: float | None,
    best_refute_score: float | None,
    supporting_observation_ids: Iterable[str],
    refuting_observation_ids: Iterable[str],
    complete_group_count: int,
    complete_group_ids: Iterable[str],
    status: str | Enum,
    decision_policy_version: str,
    certificate_digest: str,
) -> str:
    return stable_m5_digest(
        "m5-claim-state-artifact-v2",
        text_field(claim_id),
        int_field(support_count),
        int_field(refute_count),
        option_field(
            None if best_support_score is None else f64_field(best_support_score)
        ),
        option_field(
            None if best_refute_score is None else f64_field(best_refute_score)
        ),
        sequence_field(text_field(value) for value in supporting_observation_ids),
        sequence_field(text_field(value) for value in refuting_observation_ids),
        int_field(complete_group_count),
        sequence_field(text_field(value) for value in complete_group_ids),
        enum_field(status),
        text_field(decision_policy_version),
        hash_field(certificate_digest),
    )


def answer_state_artifact_digest(
    *,
    answer_version_id: str,
    required_claim_count: int,
    supported_count: int,
    unsupported_count: int,
    refuted_count: int,
    conflicted_count: int,
    status: str | Enum,
) -> str:
    return stable_m5_digest(
        "m5-answer-state-artifact-v2",
        text_field(answer_version_id),
        int_field(required_claim_count),
        int_field(supported_count),
        int_field(unsupported_count),
        int_field(refuted_count),
        int_field(conflicted_count),
        enum_field(status),
    )


def changed_state_set_digest(reference_digests: Iterable[str]) -> str:
    return stable_m5_digest(
        "m5-changed-state-set-v2",
        sequence_field(hash_field(value) for value in reference_digests),
    )


def runtime_work_digest(counters: Sequence[int]) -> str:
    return stable_m5_digest(
        "m5-runtime-work-v2", *(int_field(value) for value in counters)
    )


def runtime_operational_config_digest(lease_duration_ms: int) -> str:
    return stable_m5_digest(
        "m5-runtime-operational-config-v1", int_field(lease_duration_ms)
    )


def lease_terminal_projection_digest(
    *,
    logical_job_id: str,
    terminal_state: str | Enum,
    terminal_reason: str | Enum | None,
    completion_digest: str,
) -> str:
    return stable_m5_digest(
        "m5-lease-terminal-projection-v1",
        enum_field("requirement"),
        text_field(logical_job_id),
        enum_field(terminal_state),
        option_field(
            enum_field(terminal_reason) if terminal_reason is not None else None
        ),
        hash_field(completion_digest),
    )


def typed_direct_terminal_projection_digest(
    *,
    job_id: str,
    terminal_state: str | Enum,
    terminal_reason: str | None,
    m4_completion_digest: str | None,
    completed_revision: int,
) -> str:
    return stable_m5_digest(
        "m5-typed-direct-terminal-projection-v1",
        enum_field("direct"),
        text_field(job_id),
        enum_field(terminal_state),
        option_field(
            text_field(terminal_reason) if terminal_reason is not None else None
        ),
        option_field(
            hash_field(m4_completion_digest)
            if m4_completion_digest is not None
            else None
        ),
        int_field(completed_revision),
    )


def dispatch_record_digest(
    *,
    epoch_id: int,
    subgraph: str | Enum,
    attempt_id: str,
    logical_job_id: str,
    attempt_ordinal: int,
    job_kind: str,
    fallback_required: bool,
    dispatched_revision: int,
    maximum_ambiguous_call_work_digest: str,
) -> str:
    return stable_m5_digest(
        "m5-dispatch-record-v1",
        int_field(epoch_id),
        enum_field(subgraph),
        text_field(attempt_id),
        text_field(logical_job_id),
        int_field(attempt_ordinal),
        text_field(job_kind),
        bool_field(fallback_required),
        int_field(dispatched_revision),
        hash_field(maximum_ambiguous_call_work_digest),
    )


def requirement_root_provenance_digest(
    *, epoch_id: int, root_job_id: str, fallback_required: bool
) -> str:
    return stable_m5_digest(
        "m5-requirement-root-provenance-v1",
        int_field(epoch_id),
        text_field(root_job_id),
        bool_field(fallback_required),
    )


def attempt_execution_evidence_digest(
    *,
    epoch_id: int,
    subgraph: str | Enum,
    attempt_id: str,
    disposition: str | Enum,
    result_or_error_hash: str,
    attempt_work_digest: str,
    attempt_timing_digest: str,
) -> str:
    return stable_m5_digest(
        "m5-attempt-execution-evidence-v1",
        int_field(epoch_id),
        enum_field(subgraph),
        text_field(attempt_id),
        enum_field(disposition),
        hash_field(result_or_error_hash),
        hash_field(attempt_work_digest),
        hash_field(attempt_timing_digest),
    )


def runtime_work_contribution_key_digest(
    *, epoch_id: int, contribution_kind: str | Enum, source_id: str
) -> str:
    return stable_m5_digest(
        "m5-runtime-work-contribution-key-v1",
        int_field(epoch_id),
        enum_field(contribution_kind),
        text_field(source_id),
    )


def epoch_failure_contribution_source_digest(
    *, structural_event_id: str, failure_reason: str | Enum
) -> str:
    return stable_m5_digest(
        "m5-epoch-failure-contribution-source-v1",
        text_field(structural_event_id),
        enum_field(failure_reason),
    )


def terminal_job_failure_contribution_source_digest(
    *, logical_job_id: str, terminal_reason: str | Enum, error_hash: str
) -> str:
    return stable_m5_digest(
        "m5-terminal-job-failure-contribution-source-v1",
        text_field(logical_job_id),
        enum_field(terminal_reason),
        hash_field(error_hash),
    )


def seal_contribution_source_digest(
    *,
    structural_event_id: str,
    combined_status_delta_set_hash: str,
    changed_state_set_hash: str,
    publication_id: str,
) -> str:
    return stable_m5_digest(
        "m5-seal-contribution-source-v1",
        text_field(structural_event_id),
        hash_field(combined_status_delta_set_hash),
        hash_field(changed_state_set_hash),
        text_field(publication_id),
    )


def expired_attempt_return_digest(
    *,
    subgraph: str | Enum,
    epoch_id: int,
    attempt_id: str,
    logical_job_id: str,
    worker_output_digest: str,
    worker_artifact_hash: str,
    activity_snapshot_epoch_id: int,
    activity_snapshot_revision: int,
    received_after_terminal: bool,
) -> str:
    return stable_m5_digest(
        "m5-expired-attempt-return-v1",
        enum_field(subgraph),
        int_field(epoch_id),
        text_field(attempt_id),
        text_field(logical_job_id),
        hash_field(worker_output_digest),
        hash_field(worker_artifact_hash),
        int_field(activity_snapshot_epoch_id),
        int_field(activity_snapshot_revision),
        enum_field("attempt_expired"),
        bool_field(received_after_terminal),
    )


def runtime_timing_observation_digest(
    *,
    required_interval_observed: bool,
    coordinator_non_db_non_neural_ns: int | None,
    neural_wall_ns: int | None,
    postgres_roundtrip_wall_ns: int | None,
    external_io_wall_ns: int | None,
    end_to_end_wall_ns: int | None,
    postgres_server_execution_ns: int | None,
    postgres_lock_wait_ns: int | None,
    postgres_wal_bytes: int | None,
    postgres_shared_block_reads: int | None,
) -> str:
    return stable_m5_digest(
        "m5-runtime-timing-observation-v1",
        bool_field(required_interval_observed),
        option_field(
            int_field(coordinator_non_db_non_neural_ns)
            if coordinator_non_db_non_neural_ns is not None
            else None
        ),
        option_field(int_field(neural_wall_ns) if neural_wall_ns is not None else None),
        option_field(
            int_field(postgres_roundtrip_wall_ns)
            if postgres_roundtrip_wall_ns is not None
            else None
        ),
        option_field(
            int_field(external_io_wall_ns) if external_io_wall_ns is not None else None
        ),
        option_field(
            int_field(end_to_end_wall_ns) if end_to_end_wall_ns is not None else None
        ),
        option_field(
            int_field(postgres_server_execution_ns)
            if postgres_server_execution_ns is not None
            else None
        ),
        option_field(
            int_field(postgres_lock_wait_ns)
            if postgres_lock_wait_ns is not None
            else None
        ),
        option_field(
            int_field(postgres_wal_bytes) if postgres_wal_bytes is not None else None
        ),
        option_field(
            int_field(postgres_shared_block_reads)
            if postgres_shared_block_reads is not None
            else None
        ),
    )


def attempt_runtime_timing_digest(
    *,
    epoch_id: int,
    subgraph: str | Enum,
    attempt_id: str,
    observation_digest: str,
) -> str:
    return stable_m5_digest(
        "m5-attempt-runtime-timing-v1",
        int_field(epoch_id),
        enum_field(subgraph),
        text_field(attempt_id),
        hash_field(observation_digest),
    )


def transition_call_timing_digest(
    *,
    epoch_id: int,
    contribution_kind: str | Enum,
    source_id: str,
    contribution_key_digest: str,
    anchor_revision: int,
    observation_digest: str,
) -> str:
    return stable_m5_digest(
        "m5-transition-call-timing-v1",
        int_field(epoch_id),
        enum_field(contribution_kind),
        text_field(source_id),
        hash_field(contribution_key_digest),
        int_field(anchor_revision),
        hash_field(observation_digest),
    )


def typed_direct_late_job_binding_digest(
    *,
    job_id: str,
    event_id: str,
    job_kind: str | Enum,
    candidate_policy_id: str,
    payload_hash: str,
    execution_spec_hash: str,
    parent_job_id: str | None,
    pair_claim_id: str | None,
    pair_chunk_version_id: str | None,
    target_claim_id: str | None,
    target_chunk_version_id: str | None,
    expandable: bool,
) -> str:
    return stable_m5_digest(
        "m5-typed-direct-late-job-binding-v1",
        text_field(job_id),
        text_field(event_id),
        enum_field(job_kind),
        text_field(candidate_policy_id),
        hash_field(payload_hash),
        hash_field(execution_spec_hash),
        option_field(text_field(parent_job_id) if parent_job_id is not None else None),
        option_field(text_field(pair_claim_id) if pair_claim_id is not None else None),
        option_field(
            text_field(pair_chunk_version_id)
            if pair_chunk_version_id is not None
            else None
        ),
        option_field(
            text_field(target_claim_id) if target_claim_id is not None else None
        ),
        option_field(
            text_field(target_chunk_version_id)
            if target_chunk_version_id is not None
            else None
        ),
        bool_field(expandable),
    )


def typed_direct_late_attempt_binding_digest(
    *,
    attempt_id: str,
    job_id: str,
    execution_spec_hash: str,
    attempt_ordinal: int,
    lease_token_hash: str,
) -> str:
    return stable_m5_digest(
        "m5-typed-direct-late-attempt-binding-v1",
        text_field(attempt_id),
        text_field(job_id),
        hash_field(execution_spec_hash),
        int_field(attempt_ordinal),
        hash_field(lease_token_hash),
    )


def typed_direct_late_completion_binding_digest(
    *,
    job_id: str,
    payload_hash: str,
    execution_spec_hash: str,
    result_artifact_id: str,
    result_artifact_hash: str,
    terminal_state: str | Enum,
    completion_digest: str,
    child_parent_job_id: str | None,
    child_completion_digest: str | None,
    child_set_hash: str | None,
    child_job_ids: Iterable[str],
) -> str:
    canonical_child_ids = tuple(
        sorted(child_job_ids, key=lambda value: value.encode("utf-8"))
    )
    return stable_m5_digest(
        "m5-typed-direct-late-completion-binding-v1",
        text_field(job_id),
        hash_field(payload_hash),
        hash_field(execution_spec_hash),
        text_field(result_artifact_id),
        hash_field(result_artifact_hash),
        enum_field(terminal_state),
        hash_field(completion_digest),
        option_field(
            text_field(child_parent_job_id) if child_parent_job_id is not None else None
        ),
        option_field(
            hash_field(child_completion_digest)
            if child_completion_digest is not None
            else None
        ),
        option_field(
            hash_field(child_set_hash) if child_set_hash is not None else None
        ),
        sequence_field(text_field(child_id) for child_id in canonical_child_ids),
    )


def typed_direct_late_discovery_binding_digest(
    *,
    root_job_id: str,
    result_artifact_id: str,
    result_artifact_hash: str,
    fallback_satisfied: bool,
    channel_hit_count: int,
    admitted_pair_count: int,
    channel_set_hash: str,
    admitted_pair_set_hash: str,
    channel_hits: Iterable[TypedDirectChannelHitValues],
    admitted_pairs: Iterable[TypedDirectAdmittedPairValues],
) -> str:
    canonical_hits = tuple(
        sorted(
            channel_hits,
            key=lambda hit: (
                _enum_wire(hit[4]),
                hit[5],
                hit[1],
                hit[2],
                hit[3],
                hit[7],
            ),
        )
    )
    canonical_pairs = tuple(
        sorted(
            admitted_pairs,
            key=lambda pair: (pair[4], pair[1], pair[2], pair[3]),
        )
    )
    return stable_m5_digest(
        "m5-typed-direct-late-discovery-binding-v1",
        text_field(root_job_id),
        text_field(result_artifact_id),
        hash_field(result_artifact_hash),
        bool_field(fallback_satisfied),
        int_field(channel_hit_count),
        int_field(admitted_pair_count),
        hash_field(channel_set_hash),
        hash_field(admitted_pair_set_hash),
        sequence_field(
            sequence_field(
                (
                    int_field(epoch_id),
                    text_field(claim_id),
                    text_field(chunk_version_id),
                    text_field(candidate_policy_id),
                    enum_field(channel),
                    int_field(rank),
                    option_field(f64_field(score) if score is not None else None),
                    hash_field(channel_artifact_hash),
                )
            )
            for (
                epoch_id,
                claim_id,
                chunk_version_id,
                candidate_policy_id,
                channel,
                rank,
                score,
                channel_artifact_hash,
            ) in canonical_hits
        ),
        sequence_field(
            sequence_field(
                (
                    int_field(epoch_id),
                    text_field(claim_id),
                    text_field(chunk_version_id),
                    text_field(candidate_policy_id),
                    int_field(fused_rank),
                    sequence_field(enum_field(reason) for reason in reasons),
                    bool_field(mandatory_lineage),
                )
            )
            for (
                epoch_id,
                claim_id,
                chunk_version_id,
                candidate_policy_id,
                fused_rank,
                reasons,
                mandatory_lineage,
            ) in canonical_pairs
        ),
    )


def typed_direct_late_scope_binding_digest(
    *,
    root_job_id: str,
    epoch_id: int,
    registry_snapshot_id: str,
    registered_claim_ids: Iterable[str],
    closed: bool,
    persisted_scope_kind: str | Enum,
    explicit_claim_ids: Iterable[str] | None,
    closed_revision: int | None,
) -> str:
    explicit = None if explicit_claim_ids is None else tuple(explicit_claim_ids)
    return stable_m5_digest(
        "m5-typed-direct-late-scope-binding-v1",
        text_field(root_job_id),
        int_field(epoch_id),
        text_field(registry_snapshot_id),
        sequence_field(text_field(claim_id) for claim_id in registered_claim_ids),
        bool_field(closed),
        enum_field(persisted_scope_kind),
        option_field(
            sequence_field(text_field(claim_id) for claim_id in explicit)
            if explicit is not None
            else None
        ),
        option_field(
            int_field(closed_revision) if closed_revision is not None else None
        ),
    )


def typed_direct_late_verifier_binding_digest(
    *,
    result_artifact_id: str,
    result_artifact_hash: str,
    verification_execution_present: bool,
    verification_execution: TypedDirectVerificationExecutionValues | None,
    observation_id: str,
    observation_subject_kind: str | Enum,
    observation_subject_id: str,
    observation_chunk_version_id: str,
    observation_task_type: str,
    observation_support_score: float,
    observation_refute_score: float,
    observation_neutral_score: float,
    observation_model_id: str,
    observation_model_version: str,
    observation_prompt_version: str,
    observation_input_hash: str,
    observation_produced_epoch: int,
    observation_raw_output_hash: str,
    observation_eligible_for_currency: bool,
    requested_make_effective: bool,
) -> str:
    execution_fields = None
    if verification_execution is not None:
        (
            execution_observation_id,
            execution_job_id,
            admitted_pair_id,
            model_artifact_id,
            prompt_artifact_id,
            execution_spec_hash,
            pair_input_hash,
            calibration_version,
            calibration_artifact_sha256,
            temperature,
            raw_logits,
            raw_output_hash,
            reused_from_observation_id,
        ) = verification_execution
        execution_fields = sequence_field(
            (
                text_field(execution_observation_id),
                text_field(execution_job_id),
                hash_field(admitted_pair_id),
                text_field(model_artifact_id),
                text_field(prompt_artifact_id),
                hash_field(execution_spec_hash),
                hash_field(pair_input_hash),
                text_field(calibration_version),
                hash_field(calibration_artifact_sha256),
                f64_field(temperature),
                sequence_field(f64_field(logit) for logit in raw_logits),
                hash_field(raw_output_hash),
                option_field(
                    text_field(reused_from_observation_id)
                    if reused_from_observation_id is not None
                    else None
                ),
            )
        )
    return stable_m5_digest(
        "m5-typed-direct-late-verifier-binding-v1",
        text_field(result_artifact_id),
        hash_field(result_artifact_hash),
        bool_field(verification_execution_present),
        option_field(execution_fields),
        text_field(observation_id),
        enum_field(observation_subject_kind),
        text_field(observation_subject_id),
        text_field(observation_chunk_version_id),
        text_field(observation_task_type),
        f64_field(observation_support_score),
        f64_field(observation_refute_score),
        f64_field(observation_neutral_score),
        text_field(observation_model_id),
        text_field(observation_model_version),
        text_field(observation_prompt_version),
        text_field(observation_input_hash),
        int_field(observation_produced_epoch),
        hash_field(observation_raw_output_hash),
        bool_field(observation_eligible_for_currency),
        bool_field(requested_make_effective),
    )


def typed_direct_late_return_envelope_digest(
    *,
    epoch_id: int,
    return_kind: str | Enum,
    job_binding_digest: str,
    attempt_binding_digest: str,
    completion_binding_digest: str,
    discovery_binding_digest: str | None,
    scope_binding_digest: str | None,
    verifier_binding_digest: str | None,
) -> str:
    return stable_m5_digest(
        "m5-typed-direct-late-return-envelope-v1",
        int_field(epoch_id),
        enum_field(return_kind),
        hash_field(job_binding_digest),
        hash_field(attempt_binding_digest),
        hash_field(completion_binding_digest),
        option_field(
            hash_field(discovery_binding_digest)
            if discovery_binding_digest is not None
            else None
        ),
        option_field(
            hash_field(scope_binding_digest)
            if scope_binding_digest is not None
            else None
        ),
        option_field(
            hash_field(verifier_binding_digest)
            if verifier_binding_digest is not None
            else None
        ),
    )


def combined_status_delta_set_digest(deltas: Iterable[StatusDelta]) -> str:
    return stable_m5_digest(
        "m5-combined-status-delta-set-v2",
        sequence_field(
            sequence_field(
                (
                    text_field(delta.event_id),
                    text_field(delta.object_type),
                    text_field(delta.object_id),
                    text_field(delta.old_status),
                    text_field(delta.new_status),
                    text_field(delta.reason),
                )
            )
            for delta in deltas
        ),
    )


def open_event_receipt_binding_digest(
    *,
    epoch_id: int,
    replayed: bool,
    already_sealed: bool,
    publication_id: str | None,
    already_failed: bool,
    failure_reason: str | None,
) -> str:
    return stable_m5_digest(
        "m5-open-event-receipt-binding-v2",
        int_field(epoch_id),
        bool_field(replayed),
        bool_field(already_sealed),
        option_field(
            text_field(publication_id) if publication_id is not None else None
        ),
        bool_field(already_failed),
        option_field(
            text_field(failure_reason) if failure_reason is not None else None
        ),
    )


def publication_receipt_binding_digest(
    *, epoch_id: int, publication_id: str, replayed: bool
) -> str:
    return stable_m5_digest(
        "m5-publication-receipt-binding-v2",
        int_field(epoch_id),
        text_field(publication_id),
        bool_field(replayed),
    )


def event_run_logical_result_digest(
    *,
    event_id: str,
    payload_hash: str,
    epoch_id: int,
    sealed_or_failed_outcome: str | Enum,
    original_open_receipt_binding_hash: str,
    original_publication_receipt_binding_hash: str | None,
    event_work_digest: str,
    combined_status_delta_set_hash: str,
    changed_state_set_hash: str,
    failure_reason: str | Enum | None,
) -> str:
    return stable_m5_digest(
        "m5-event-run-logical-result-v2",
        text_field(event_id),
        hash_field(payload_hash),
        int_field(epoch_id),
        enum_field(sealed_or_failed_outcome),
        hash_field(original_open_receipt_binding_hash),
        option_field(
            hash_field(original_publication_receipt_binding_hash)
            if original_publication_receipt_binding_hash is not None
            else None
        ),
        hash_field(event_work_digest),
        hash_field(combined_status_delta_set_hash),
        hash_field(changed_state_set_hash),
        option_field(
            enum_field(failure_reason) if failure_reason is not None else None
        ),
    )


def requirement_withdrawal_plan_digest(
    *,
    event_id: str,
    deactivated_chunk_version_ids: Iterable[str],
    withdrawn_candidate_pair_digests: Iterable[str],
    withdrawn_observation_ids: Iterable[str],
    cancelled_job_ids: Iterable[str],
    fallback_keys: Iterable[tuple[str, str]],
) -> str:
    return stable_m5_digest(
        "m5-requirement-withdrawal-plan-v2",
        text_field(event_id),
        sequence_field(text_field(value) for value in deactivated_chunk_version_ids),
        sequence_field(hash_field(value) for value in withdrawn_candidate_pair_digests),
        sequence_field(text_field(value) for value in withdrawn_observation_ids),
        sequence_field(text_field(value) for value in cancelled_job_ids),
        sequence_field(
            sequence_field(
                (text_field(requirement_version_id), text_field(candidate_policy_id))
            )
            for requirement_version_id, candidate_policy_id in fallback_keys
        ),
    )


def cancellation_plan_digest(
    *,
    structural_event_id: str,
    epoch_id: int,
    cancelled_job_ids: Iterable[str],
    reason: str | Enum,
) -> str:
    return stable_m5_digest(
        "m5-cancellation-plan-v2",
        text_field(structural_event_id),
        int_field(epoch_id),
        sequence_field(hash_field(value) for value in cancelled_job_ids),
        enum_field(reason),
    )


def requirement_root_set_digest(root_job_ids: Iterable[str]) -> str:
    canonical = tuple(sorted(set(root_job_ids)))
    return stable_m5_digest(
        "m5-requirement-root-set-v2",
        sequence_field(text_field(value) for value in canonical),
    )


def requirement_root_barrier_completion_digest(
    *,
    structural_event_id: str,
    requirement_root_set_hash: str,
    root_result_hashes: Iterable[tuple[str, str]],
    admitted_pair_digests: Iterable[tuple[str, str]],
) -> str:
    canonical_roots = tuple(sorted(root_result_hashes, key=lambda item: item[0]))
    canonical_pairs = tuple(sorted(admitted_pair_digests, key=lambda item: item[0]))
    return stable_m5_digest(
        "m5-requirement-root-barrier-completion-v2",
        text_field(structural_event_id),
        hash_field(requirement_root_set_hash),
        sequence_field(
            sequence_field((text_field(root_id), hash_field(result_hash)))
            for root_id, result_hash in canonical_roots
        ),
        sequence_field(hash_field(digest) for _, digest in canonical_pairs),
    )


def runtime_schema_bundle_digest(
    *,
    migration_sha256: str,
    core_schema_bundle_sha256: str,
) -> str:
    return stable_m5_digest(
        "m5-runtime-schema-bundle-v2",
        text_field("migrations/015_m5_runtime.sql"),
        hash_field(migration_sha256),
        hash_field(core_schema_bundle_sha256),
    )


__all__ = [
    "AuditKeyValues",
    "MatchingWorkValues",
    "TypedDirectAdmittedPairValues",
    "TypedDirectChannelHitValues",
    "TypedDirectVerificationExecutionValues",
    "active_chunk_snapshot_digest",
    "activation_receipt_digest",
    "activation_request_digest",
    "accumulator_provenance_digest",
    "accumulator_provenance_fields",
    "accumulator_provenance_row_digest",
    "audit_edge_fields",
    "audit_hall_fields",
    "audit_mask_fields",
    "audit_observation_fields",
    "answer_state_artifact_digest",
    "attempt_output_digest",
    "attempt_result_artifact_digest",
    "attempt_result_artifact_id",
    "attempt_execution_evidence_digest",
    "attempt_runtime_timing_digest",
    "cancellation_plan_digest",
    "candidate_policy_manifest_digest",
    "changed_state_reference_digest",
    "changed_state_set_digest",
    "child_set_digest",
    "combined_status_delta_set_digest",
    "current_provenance_fields",
    "discovery_scope_closure_digest",
    "discovery_scope_contract_digest",
    "dispatch_record_digest",
    "epoch_failure_contribution_source_digest",
    "event_run_logical_result_digest",
    "expired_attempt_return_digest",
    "forward_retrieval_execution_spec_digest",
    "group_state_artifact_digest",
    "job_attempt_id",
    "job_completion_digest",
    "job_payload_digest",
    "logical_job_id",
    "logical_output_digest",
    "logical_output_preimage",
    "logical_overlay_patch_digest",
    "matching_change_digest",
    "matching_group_shape_set_digest",
    "matching_point_fields",
    "matching_work_contribution_digest",
    "matching_work_digest",
    "open_event_receipt_binding_digest",
    "publication_receipt_binding_digest",
    "persisted_matching_patch_digest",
    "persisted_matching_schema_bundle_digest",
    "persisted_matching_transition_intent_digest",
    "physical_audit_digest",
    "physical_audit_family_digest",
    "physical_audit_projection_digest",
    "physical_audit_row_digest",
    "physical_mismatch_fields",
    "patch_provenance_fields",
    "provenance_mismatch_fields",
    "provenance_replay_digest",
    "claim_state_artifact_digest",
    "requirement_admitted_pair_digest",
    "requirement_state_artifact_digest",
    "requirement_channel_hit_digest",
    "requirement_discovery_artifact_id",
    "requirement_discovery_result_digest",
    "requirement_pair_input_digest",
    "requirement_registry_snapshot_digest",
    "requirement_root_provenance_digest",
    "requirement_root_barrier_completion_digest",
    "requirement_root_set_digest",
    "requirement_scope_selection_digest",
    "requirement_semantic_observation_id",
    "requirement_verifier_artifact_id",
    "requirement_verifier_result_digest",
    "requirement_verifier_role_binding_digest",
    "requirement_withdrawal_plan_digest",
    "reverse_retrieval_execution_spec_digest",
    "runtime_schema_bundle_digest",
    "runtime_operational_config_digest",
    "runtime_timing_observation_digest",
    "runtime_work_contribution_key_digest",
    "runtime_work_digest",
    "seal_contribution_source_digest",
    "semantic_pair_digest",
    "terminal_job_failure_contribution_source_digest",
    "text_normalizer_provenance_digest",
    "transition_call_timing_digest",
    "typed_direct_late_attempt_binding_digest",
    "typed_direct_late_completion_binding_digest",
    "typed_direct_late_discovery_binding_digest",
    "typed_direct_late_job_binding_digest",
    "typed_direct_late_return_envelope_digest",
    "typed_direct_late_scope_binding_digest",
    "typed_direct_late_verifier_binding_digest",
    "typed_direct_terminal_projection_digest",
    "working_image_provenance_digest",
    "working_image_provenance_fields",
    "working_image_provenance_row_digest",
    "working_provenance_fields",
    "certificate_binding_row_digest",
    "lease_terminal_projection_digest",
]
