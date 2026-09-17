"""Cursor-local PostgreSQL primitives for the persisted M5 matching image.

The functions in this module never own a transaction and never advance a
runtime revision.  Their caller owns the tier-1--15 prefix and the outer
commit/rollback decision.  This first store-core checkpoint deliberately
implements only the byte-total, physically and logically empty structural
transition.  Every non-empty transition shape fails before the first write;
later path-exclusive lanes can extend the derivation without introducing a
caller-authored patch surface.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from typing import Any, NoReturn

from psycopg import Cursor, sql

from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m5.incremental_overlay import M5OverlayWork
from groundloop.m5.matching import HallMaskState, MatchingWorkCounters
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    MATCHING_WORK_COUNTER_NAMES,
    M5MatchingEdgeCurrent,
    M5MatchingEdgePoint,
    M5MatchingEdgeWorking,
    M5MatchingHallCurrent,
    M5MatchingHallPoint,
    M5MatchingHallWorking,
    M5MatchingImagePoint,
    M5MatchingLayer,
    M5MatchingMaskCurrent,
    M5MatchingMaskPoint,
    M5MatchingMaskWorking,
    M5MatchingObservationCurrent,
    M5MatchingObservationPoint,
    M5MatchingObservationWorking,
    M5PersistedLogicalOverlayPatch,
    M5PersistedMatchingContribution,
    M5PersistedMatchingPatch,
    M5PersistedMatchingPatchArtifact,
    M5PersistedMatchingPatchReceipt,
    M5PersistedMatchingSourceKind,
    M5PersistedMatchingTransitionIntent,
    m5_overlay_work_values,
)

_PERSISTED_MATCHING_BUNDLE_ROW = (
    "m5-persisted-matching-schema-bundle-v1",
    "52240e19968926d0c051fe6146b3c7d877cf582014341efbfcc78637f3ff5761",
    "e387b01fa80145273ba40d2d83581bc54762d3a4a2edd34669c095076c52154c",
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565",
)
_EMPTY_LOGICAL_OUTPUT_BYTES = bytes.fromhex(
    "710000000000000002000000000000002573000000000000001c"
    "6d352d6f7665726c61792d6c6f676963616c2d6f75747075742d7632"
    "0000000000000009710000000000000000"
)
_EMPTY_LOGICAL_OUTPUT_DIGEST = (
    "b4e641b66a06cb7d204377c37cfe031d958ce6d959832620fc2e9441339581c3"
)
_EMPTY_OUTPUT_BYTES = 71
_MATCHING_ARTIFACT_LOCK_NAMESPACE = 1_295_338_832  # signed int32 for b"M5MP"


def _require_positive_int(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValidationError(f"{name} must be a positive integer")


def _require_nonnegative_int(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValidationError(f"{name} must be a nonnegative integer")


def _require_text(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be non-empty text")


def _require_sha256(name: str, value: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _text(value: object) -> str:
    """Decode a PostgreSQL text value without changing its identity bytes."""

    return str(value)


def _sha256_text(value: object) -> str:
    """Decode a fixed-width PostgreSQL digest, removing only CHAR padding."""

    digest = str(value).rstrip(" ")
    _require_sha256("persisted digest", digest)
    return digest


def _tuple_ints(value: object) -> tuple[int, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValidationError("persisted Hall array has an invalid shape")
    return tuple(int(item) for item in value)


def require_persisted_matching_bundle(cursor: Cursor[Any]) -> None:
    """Require the literal accepted migration-017 five-field ledger tuple."""

    row = cursor.execute(
        """
        SELECT bundle_id, bundle_sha256, migration_sha256,
               oracle_sha256, prerequisite_sha256
        FROM groundloop_m5_schema_bundle
        WHERE bundle_id = %s
        """,
        (_PERSISTED_MATCHING_BUNDLE_ROW[0],),
    ).fetchone()
    actual = (
        None
        if row is None
        else (
            _text(row[0]),
            _sha256_text(row[1]),
            _sha256_text(row[2]),
            _sha256_text(row[3]),
            _sha256_text(row[4]),
        )
    )
    if actual != _PERSISTED_MATCHING_BUNDLE_ROW:
        raise InvalidEventError(
            "typed M5 matching requires the exact accepted migration-017 bundle"
        )


def _authorize_checked_prefix(
    cursor: Cursor[Any], *, epoch_id: int, expected_revision: int
) -> None:
    """Acquire the tier-6 CAS prefix and bind a transaction-local read scope."""

    cursor.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
        (epoch_id, expected_revision),
    )
    cursor.execute(
        """
        SELECT set_config(
                 'groundloop.m5_matching_reader_epoch_id', %s, true
               ),
               set_config(
                 'groundloop.m5_matching_reader_revision', %s, true
               ),
               set_config(
                 'groundloop.m5_matching_reader_backend_pid',
                 pg_backend_pid()::text, true
               ),
               set_config(
                 'groundloop.m5_matching_reader_transaction_id',
                 pg_current_xact_id()::text, true
               )
        """,
        (str(epoch_id), str(expected_revision)),
    )


def _require_checked_prefix(
    cursor: Cursor[Any], *, epoch_id: int, expected_revision: int | None = None
) -> tuple[int, int]:
    """Assert the scoped epoch and re-enter the real lock/CAS authorizer.

    The reader settings bind this module's cursor-local scope, but are never
    accepted as proof of a held row lock.  Only the migration-015 authorizer
    can establish that proof; invoking it again is idempotent when the caller
    already owns the tier-5/tier-6 rows and safely acquires them otherwise.
    """

    setting = cursor.execute(
        """
        SELECT current_setting('groundloop.m5_checked_transition', true),
               current_setting(
                 'groundloop.m5_matching_reader_epoch_id', true
               ),
               current_setting(
                 'groundloop.m5_matching_reader_revision', true
               ),
               current_setting(
                 'groundloop.m5_matching_reader_backend_pid', true
               ),
               current_setting(
                 'groundloop.m5_matching_reader_transaction_id', true
               ),
               pg_backend_pid()::text,
               pg_current_xact_id()::text
        """
    ).fetchone()
    if (
        setting is None
        or setting[0] != "on"
        or setting[1] != str(epoch_id)
        or not setting[2]
        or setting[3] != setting[5]
        or setting[4] != setting[6]
    ):
        raise ValidationError(
            "matching point read lacks the exact checked epoch prefix"
        )
    scoped_revision = int(setting[2])
    if expected_revision is not None and scoped_revision != expected_revision:
        raise EventConflictError("matching point read has a different scoped revision")
    cursor.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
        (epoch_id, scoped_revision),
    )
    row = cursor.execute(
        """
        SELECT epoch.revision, runtime.revision
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        WHERE epoch.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise InvalidEventError("matching point read names an unknown typed epoch")
    revisions = (int(row[0]), int(row[1]))
    if revisions[0] != revisions[1] or revisions[0] != scoped_revision:
        raise EventConflictError("matching point read lost its epoch/runtime CAS")
    return revisions


def effective_matching_image(
    cursor: Cursor[Any], epoch_id: int
) -> M5MatchingImagePoint:
    """Return and lock the exact current-plus-requested-working image headers."""

    _require_positive_int("epoch_id", epoch_id)
    require_persisted_matching_bundle(cursor)
    _, scoped_runtime_revision = _require_checked_prefix(cursor, epoch_id=epoch_id)
    current = cursor.execute(
        """
        SELECT decision_policy_version, installed_epoch_id, installed_revision
        FROM groundloop_m5_matching_image_current
        WHERE singleton
        FOR UPDATE
        """
    ).fetchone()
    if current is None:
        raise ValidationError("persisted matching current image is missing")
    current_policy = _text(current[0])
    current_epoch = int(current[1])
    current_revision = int(current[2])

    current_authority = cursor.execute(
        """
        SELECT m4_head.epoch_id, m5_head.epoch_id, m5_head.sealed_revision,
               predecessor.revision,
               predecessor.structural_status,
               predecessor.semantic_status,
               predecessor.evaluation_state,
               predecessor.sealed_at IS NOT NULL,
               (SELECT count(*)
                  FROM groundloop_decision_policy AS strict_policy
                 WHERE strict_policy.valid_from_epoch <= %s
                   AND (strict_policy.valid_to_epoch IS NULL
                        OR %s < strict_policy.valid_to_epoch)),
               EXISTS (
                 SELECT 1
                   FROM groundloop_decision_policy AS selected_policy
                  WHERE selected_policy.policy_version = %s
                    AND selected_policy.valid_from_epoch <= %s
                    AND (selected_policy.valid_to_epoch IS NULL
                         OR %s < selected_policy.valid_to_epoch)
               )
        FROM groundloop_m4_publication_head AS m4_head
        JOIN groundloop_m5_publication_head AS m5_head ON m5_head.singleton
        JOIN groundloop_epoch AS predecessor ON predecessor.epoch_id = %s
        WHERE m4_head.singleton
        """,
        (
            current_epoch,
            current_epoch,
            current_policy,
            current_epoch,
            current_epoch,
            current_epoch,
        ),
    ).fetchone()
    if (
        current_authority is None
        or int(current_authority[0]) != current_epoch
        or int(current_authority[1]) != current_epoch
        or int(current_authority[2]) != current_revision
        or int(current_authority[3]) != current_revision
        or _text(current_authority[4]) != "committed"
        or _text(current_authority[5]) != "sealed"
        or _text(current_authority[6]) != "complete"
        or not bool(current_authority[7])
        or int(current_authority[8]) != 1
        or not bool(current_authority[9])
    ):
        raise ValidationError("persisted matching current image is inconsistent")

    working = cursor.execute(
        """
        SELECT epoch_id, base_epoch_id, base_revision,
               decision_policy_version, updated_revision
        FROM groundloop_m5_matching_image_working
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if working is None:
        raise ValidationError("persisted matching working image is missing")
    working_epoch = int(working[0])
    working_base_epoch = int(working[1])
    working_base_revision = int(working[2])
    working_policy = _text(working[3])
    working_updated_revision = int(working[4])

    policy_authority = cursor.execute(
        """
        SELECT runtime.expected_previous_published_epoch_id,
               runtime.candidate_policy_id,
               runtime.candidate_policy_manifest_hash,
               typed_update.previous_published_epoch_id,
               typed_update.decision_policy_version,
               typed_policy.candidate_policy_manifest_hash,
               typed_policy.decision_policy_version,
               direct_update.epoch_id,
               direct_update.previous_published_epoch_id,
               direct_update.candidate_policy_id,
               direct_policy.decision_policy_version
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        JOIN groundloop_m5_candidate_policy AS typed_policy
          ON typed_policy.candidate_policy_id = runtime.candidate_policy_id
        LEFT JOIN groundloop_m4_update AS direct_update
          ON direct_update.epoch_id = runtime.epoch_id
        LEFT JOIN groundloop_candidate_policy AS direct_policy
          ON direct_policy.candidate_policy_id = direct_update.candidate_policy_id
        WHERE runtime.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    direct_present = policy_authority is not None and policy_authority[7] is not None
    if (
        policy_authority is None
        or working_epoch != epoch_id
        or working_base_epoch != current_epoch
        or working_base_revision != current_revision
        or int(policy_authority[0]) != current_epoch
        or int(policy_authority[3]) != current_epoch
        or working_updated_revision > scoped_runtime_revision
        or _sha256_text(policy_authority[2]) != _sha256_text(policy_authority[5])
        or _text(policy_authority[4]) != working_policy
        or _text(policy_authority[6]) != working_policy
        or (
            direct_present
            and (
                int(policy_authority[8]) != current_epoch
                or _text(policy_authority[9]) != _text(policy_authority[1])
                or _text(policy_authority[10]) != working_policy
            )
        )
    ):
        raise ValidationError("persisted matching image policy is inconsistent")
    return M5MatchingImagePoint(
        current_policy,
        current_epoch,
        current_revision,
        working_epoch,
        working_base_epoch,
        working_base_revision,
        working_policy,
        working_updated_revision,
    )


def resolved_matching_observation_point(
    cursor: Cursor[Any], epoch_id: int, observation_id: str
) -> M5MatchingObservationPoint | None:
    """Resolve working-before-current without filtering a working tombstone."""

    _require_positive_int("epoch_id", epoch_id)
    _require_text("observation_id", observation_id)
    effective_matching_image(cursor, epoch_id)
    current = cursor.execute(
        """
        SELECT observation_id, requirement_version_id, group_version_id,
               requirement_ordinal, text_hash, installed_epoch_id,
               installed_revision
        FROM groundloop_m5_matching_observation_current
        WHERE observation_id = %s
        FOR UPDATE
        """,
        (observation_id,),
    ).fetchone()
    working = cursor.execute(
        """
        SELECT epoch_id, observation_id, requirement_version_id,
               group_version_id, requirement_ordinal, text_hash,
               present, updated_revision
        FROM groundloop_m5_matching_observation_working
        WHERE epoch_id = %s AND observation_id = %s
        FOR UPDATE
        """,
        (epoch_id, observation_id),
    ).fetchone()
    if working is not None:
        return M5MatchingObservationWorking(
            M5MatchingLayer.WORKING,
            int(working[0]),
            _text(working[1]),
            _text(working[2]),
            _text(working[3]),
            int(working[4]),
            _sha256_text(working[5]),
            bool(working[6]),
            int(working[7]),
        )
    if current is None:
        return None
    return M5MatchingObservationCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        _text(current[1]),
        _text(current[2]),
        int(current[3]),
        _sha256_text(current[4]),
        int(current[5]),
        int(current[6]),
    )


def resolved_matching_edge_point(
    cursor: Cursor[Any],
    epoch_id: int,
    requirement_version_id: str,
    text_hash: str,
) -> M5MatchingEdgePoint | None:
    """Resolve one persisted edge point without collapsing a zero tombstone."""

    _require_positive_int("epoch_id", epoch_id)
    _require_text("requirement_version_id", requirement_version_id)
    _require_sha256("text_hash", text_hash)
    effective_matching_image(cursor, epoch_id)
    current = cursor.execute(
        """
        SELECT requirement_version_id, text_hash, group_version_id,
               requirement_ordinal, refcount, installed_epoch_id,
               installed_revision
        FROM groundloop_m5_matching_edge_current
        WHERE requirement_version_id = %s AND text_hash = %s
        FOR UPDATE
        """,
        (requirement_version_id, text_hash),
    ).fetchone()
    working = cursor.execute(
        """
        SELECT epoch_id, requirement_version_id, text_hash,
               group_version_id, requirement_ordinal, refcount,
               updated_revision
        FROM groundloop_m5_matching_edge_working
        WHERE epoch_id = %s AND requirement_version_id = %s AND text_hash = %s
        FOR UPDATE
        """,
        (epoch_id, requirement_version_id, text_hash),
    ).fetchone()
    if working is not None:
        return M5MatchingEdgeWorking(
            M5MatchingLayer.WORKING,
            int(working[0]),
            _text(working[1]),
            _sha256_text(working[2]),
            _text(working[3]),
            int(working[4]),
            int(working[5]),
            int(working[6]),
        )
    if current is None:
        return None
    return M5MatchingEdgeCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        _sha256_text(current[1]),
        _text(current[2]),
        int(current[3]),
        int(current[4]),
        int(current[5]),
        int(current[6]),
    )


def resolved_matching_mask_point(
    cursor: Cursor[Any], epoch_id: int, group_version_id: str, text_hash: str
) -> M5MatchingMaskPoint | None:
    """Resolve one hash-mask point without collapsing a zero tombstone."""

    _require_positive_int("epoch_id", epoch_id)
    _require_text("group_version_id", group_version_id)
    _require_sha256("text_hash", text_hash)
    effective_matching_image(cursor, epoch_id)
    current = cursor.execute(
        """
        SELECT group_version_id, text_hash, mask,
               installed_epoch_id, installed_revision
        FROM groundloop_m5_matching_hash_mask_current
        WHERE group_version_id = %s AND text_hash = %s
        FOR UPDATE
        """,
        (group_version_id, text_hash),
    ).fetchone()
    working = cursor.execute(
        """
        SELECT epoch_id, group_version_id, text_hash, mask, updated_revision
        FROM groundloop_m5_matching_hash_mask_working
        WHERE epoch_id = %s AND group_version_id = %s AND text_hash = %s
        FOR UPDATE
        """,
        (epoch_id, group_version_id, text_hash),
    ).fetchone()
    if working is not None:
        return M5MatchingMaskWorking(
            M5MatchingLayer.WORKING,
            int(working[0]),
            _text(working[1]),
            _sha256_text(working[2]),
            int(working[3]),
            int(working[4]),
        )
    if current is None:
        return None
    return M5MatchingMaskCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        _sha256_text(current[1]),
        int(current[2]),
        int(current[3]),
        int(current[4]),
    )


def resolved_matching_hall_point(
    cursor: Cursor[Any], epoch_id: int, group_version_id: str
) -> M5MatchingHallPoint | None:
    """Resolve one Hall point while retaining a working absent payload."""

    _require_positive_int("epoch_id", epoch_id)
    _require_text("group_version_id", group_version_id)
    effective_matching_image(cursor, epoch_id)
    current = cursor.execute(
        """
        SELECT group_version_id, requirement_count, mask_histogram,
               neighbor_counts, deficiencies, maximum_deficiency,
               matching_size, distinct_hash_count, installed_epoch_id,
               installed_revision
        FROM groundloop_m5_matching_hall_current
        WHERE group_version_id = %s
        FOR UPDATE
        """,
        (group_version_id,),
    ).fetchone()
    working = cursor.execute(
        """
        SELECT epoch_id, group_version_id, present, requirement_count,
               mask_histogram, neighbor_counts, deficiencies,
               maximum_deficiency, matching_size, distinct_hash_count,
               updated_revision
        FROM groundloop_m5_matching_hall_working
        WHERE epoch_id = %s AND group_version_id = %s
        FOR UPDATE
        """,
        (epoch_id, group_version_id),
    ).fetchone()
    if working is not None:
        present = bool(working[2])
        return M5MatchingHallWorking(
            M5MatchingLayer.WORKING,
            int(working[0]),
            _text(working[1]),
            present,
            None if working[3] is None else int(working[3]),
            None if working[4] is None else _tuple_ints(working[4]),
            None if working[5] is None else _tuple_ints(working[5]),
            None if working[6] is None else _tuple_ints(working[6]),
            None if working[7] is None else int(working[7]),
            None if working[8] is None else int(working[8]),
            None if working[9] is None else int(working[9]),
            int(working[10]),
        )
    if current is None:
        return None
    return M5MatchingHallCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        int(current[1]),
        _tuple_ints(current[2]),
        _tuple_ints(current[3]),
        _tuple_ints(current[4]),
        int(current[5]),
        int(current[6]),
        int(current[7]),
        int(current[8]),
        int(current[9]),
    )


def effective_matching_observation(
    cursor: Cursor[Any], epoch_id: int, observation_id: str
) -> M5MatchingObservationPoint | None:
    point = resolved_matching_observation_point(cursor, epoch_id, observation_id)
    if isinstance(point, M5MatchingObservationWorking) and not point.present:
        return None
    return point


def effective_matching_edge(
    cursor: Cursor[Any],
    epoch_id: int,
    requirement_version_id: str,
    text_hash: str,
) -> M5MatchingEdgePoint | None:
    point = resolved_matching_edge_point(
        cursor, epoch_id, requirement_version_id, text_hash
    )
    if isinstance(point, M5MatchingEdgeWorking) and point.refcount == 0:
        return None
    return point


def effective_matching_mask(
    cursor: Cursor[Any], epoch_id: int, group_version_id: str, text_hash: str
) -> int:
    point = resolved_matching_mask_point(cursor, epoch_id, group_version_id, text_hash)
    return 0 if point is None else point.mask


def effective_matching_hall(
    cursor: Cursor[Any], epoch_id: int, group_version_id: str
) -> HallMaskState | None:
    point = resolved_matching_hall_point(cursor, epoch_id, group_version_id)
    if point is None or (
        isinstance(point, M5MatchingHallWorking) and not point.present
    ):
        return None
    return HallMaskState(
        point.requirement_count,  # type: ignore[arg-type]
        point.mask_histogram,  # type: ignore[arg-type]
        point.neighbor_counts,  # type: ignore[arg-type]
        point.deficiencies,  # type: ignore[arg-type]
        point.maximum_deficiency,  # type: ignore[arg-type]
        point.matching_size,  # type: ignore[arg-type]
        point.distinct_hash_count,  # type: ignore[arg-type]
    )


def least_effective_observation(
    cursor: Cursor[Any],
    epoch_id: int,
    group_version_id: str,
    requirement_ordinal: int,
    text_hash: str,
) -> str | None:
    """Return the least effective observation under PostgreSQL C collation."""

    _require_positive_int("epoch_id", epoch_id)
    _require_text("group_version_id", group_version_id)
    _require_nonnegative_int("requirement_ordinal", requirement_ordinal)
    _require_sha256("text_hash", text_hash)
    effective_matching_image(cursor, epoch_id)
    row = cursor.execute(
        """
        SELECT observation_id
        FROM (
          SELECT working.observation_id
          FROM groundloop_m5_matching_observation_working AS working
          WHERE working.epoch_id = %s
            AND working.group_version_id = %s
            AND working.requirement_ordinal = %s
            AND working.text_hash = %s
            AND working.present
          UNION ALL
          SELECT current_row.observation_id
          FROM groundloop_m5_matching_observation_current AS current_row
          WHERE current_row.group_version_id = %s
            AND current_row.requirement_ordinal = %s
            AND current_row.text_hash = %s
            AND NOT EXISTS (
              SELECT 1
              FROM groundloop_m5_matching_observation_working AS shadow
              WHERE shadow.epoch_id = %s
                AND shadow.observation_id = current_row.observation_id
            )
        ) AS effective
        ORDER BY observation_id COLLATE "C"
        LIMIT 1
        """,
        (
            epoch_id,
            group_version_id,
            requirement_ordinal,
            text_hash,
            group_version_id,
            requirement_ordinal,
            text_hash,
            epoch_id,
        ),
    ).fetchone()
    return None if row is None else _text(row[0])


def representative_effective_hashes(
    cursor: Cursor[Any],
    epoch_id: int,
    group_version_id: str,
    mask: int,
    limit: int,
) -> tuple[str, ...]:
    """Return the bounded C-ordered effective hashes for one exact mask."""

    _require_positive_int("epoch_id", epoch_id)
    _require_text("group_version_id", group_version_id)
    _require_positive_int("mask", mask)
    _require_nonnegative_int("limit", limit)
    if mask > 255 or limit > 8:
        raise ValidationError("representative request exceeds the frozen bound")
    effective_matching_image(cursor, epoch_id)
    hall = effective_matching_hall(cursor, epoch_id, group_version_id)
    if hall is None:
        raise ValidationError("representative request names no effective Hall group")
    if mask >= 1 << hall.requirement_count or limit != hall.requirement_count:
        raise ValidationError("representative request exceeds its group shape")
    rows = cursor.execute(
        """
        SELECT text_hash
        FROM (
          SELECT working.text_hash::text AS text_hash
          FROM groundloop_m5_matching_hash_mask_working AS working
          WHERE working.epoch_id = %s
            AND working.group_version_id = %s
            AND working.mask = %s
          UNION ALL
          SELECT current_row.text_hash::text AS text_hash
          FROM groundloop_m5_matching_hash_mask_current AS current_row
          WHERE current_row.group_version_id = %s
            AND current_row.mask = %s
            AND NOT EXISTS (
              SELECT 1
              FROM groundloop_m5_matching_hash_mask_working AS shadow
              WHERE shadow.epoch_id = %s
                AND shadow.group_version_id = current_row.group_version_id
                AND shadow.text_hash = current_row.text_hash
            )
        ) AS effective
        ORDER BY text_hash COLLATE "C"
        LIMIT %s
        """,
        (epoch_id, group_version_id, mask, group_version_id, mask, epoch_id, limit),
    ).fetchall()
    hashes = tuple(_sha256_text(row[0]) for row in rows)
    expected_count = min(hall.mask_histogram[mask], hall.requirement_count)
    if len(hashes) != expected_count:
        raise ValidationError(
            "representative query cardinality differs from the effective Hall image"
        )
    return hashes


def _fail_nonempty(source_description: str) -> NoReturn:
    raise InvalidEventError(
        "persisted matching store-core checkpoint rejects non-empty "
        f"{source_description} before its first write"
    )


def derive_matching_transition_intent(
    cursor: Cursor[Any],
    epoch_id: int,
    expected_runtime_revision: int,
    resulting_revision: int,
    source_kind: M5PersistedMatchingSourceKind,
    source_id: str,
    expected_source_identity_hash: str | None = None,
) -> M5PersistedMatchingTransitionIntent:
    """Derive the accepted empty structural intent from persisted authority."""

    _require_positive_int("epoch_id", epoch_id)
    _require_positive_int("expected_runtime_revision", expected_runtime_revision)
    _require_positive_int("resulting_revision", resulting_revision)
    if not isinstance(source_kind, M5PersistedMatchingSourceKind):
        raise ValidationError("source_kind must be a persisted-matching source enum")
    _require_text("source_id", source_id)
    if expected_source_identity_hash is not None:
        _require_sha256("expected_source_identity_hash", expected_source_identity_hash)
    require_persisted_matching_bundle(cursor)
    if source_kind is not M5PersistedMatchingSourceKind.STRUCTURAL_OPEN:
        _fail_nonempty(source_kind.value)
    predecessor_row = cursor.execute(
        """
        SELECT typed_update.previous_published_epoch_id
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        WHERE epoch.epoch_id = %s
          AND epoch.event_id = %s
          AND runtime.structural_event_id = %s
        """,
        (epoch_id, source_id, source_id),
    ).fetchone()
    if predecessor_row is None:
        raise InvalidEventError("structural matching source authority is incomplete")
    predecessor_epoch_id = int(predecessor_row[0])
    locked_epoch_rows = cursor.execute(
        """
        SELECT epoch_id
        FROM groundloop_epoch
        WHERE epoch_id IN (%s, %s)
        ORDER BY epoch_id
        FOR UPDATE
        """,
        (predecessor_epoch_id, epoch_id),
    ).fetchall()
    if tuple(int(row[0]) for row in locked_epoch_rows) != tuple(
        sorted((predecessor_epoch_id, epoch_id))
    ):
        raise InvalidEventError("structural matching epoch prefix is incomplete")
    _authorize_checked_prefix(
        cursor, epoch_id=epoch_id, expected_revision=expected_runtime_revision
    )
    if expected_runtime_revision != 1 or resulting_revision != 1:
        raise EventConflictError("structural matching intent must use runtime point 1")
    typed_update_lock = cursor.execute(
        "SELECT epoch_id FROM groundloop_m5_update WHERE epoch_id = %s FOR UPDATE",
        (epoch_id,),
    ).fetchone()
    direct_update_lock = cursor.execute(
        "SELECT epoch_id FROM groundloop_m4_update WHERE epoch_id = %s FOR UPDATE",
        (epoch_id,),
    ).fetchone()
    if typed_update_lock is None or direct_update_lock is None:
        raise InvalidEventError("structural matching update authority is incomplete")

    source = cursor.execute(
        """
        SELECT epoch.payload_hash, typed_update.previous_published_epoch_id,
               predecessor.revision, typed_update.decision_policy_version,
               typed_update.update_kind, runtime.structural_event_id,
               runtime.revision, current_image.installed_epoch_id,
               current_image.installed_revision
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        JOIN groundloop_m5_candidate_policy AS typed_policy
          ON typed_policy.candidate_policy_id = runtime.candidate_policy_id
        JOIN groundloop_m4_update AS direct_update USING (epoch_id)
        JOIN groundloop_candidate_policy AS direct_policy
          ON direct_policy.candidate_policy_id = direct_update.candidate_policy_id
        JOIN groundloop_epoch AS predecessor
          ON predecessor.epoch_id = typed_update.previous_published_epoch_id
        JOIN groundloop_m4_publication_head AS m4_head
          ON m4_head.singleton
         AND m4_head.epoch_id = typed_update.previous_published_epoch_id
        JOIN groundloop_m5_publication_head AS m5_head
          ON m5_head.singleton
         AND m5_head.epoch_id = typed_update.previous_published_epoch_id
        JOIN groundloop_m5_matching_image_current AS current_image
          ON current_image.singleton
         AND current_image.installed_epoch_id = m5_head.epoch_id
         AND current_image.installed_revision = m5_head.sealed_revision
        JOIN groundloop_decision_policy AS current_policy
          ON current_policy.policy_version = current_image.decision_policy_version
        WHERE epoch.epoch_id = %s
          AND epoch.event_id = %s
          AND runtime.structural_event_id = %s
          AND typed_update.previous_published_epoch_id = %s
          AND runtime.expected_previous_published_epoch_id =
              typed_update.previous_published_epoch_id
          AND runtime.candidate_policy_manifest_hash =
              typed_policy.candidate_policy_manifest_hash
          AND typed_policy.decision_policy_version =
              typed_update.decision_policy_version
          AND direct_update.previous_published_epoch_id =
              typed_update.previous_published_epoch_id
          AND direct_update.candidate_policy_id = runtime.candidate_policy_id
          AND direct_policy.decision_policy_version =
              typed_update.decision_policy_version
          AND current_image.decision_policy_version =
              typed_update.decision_policy_version
          AND current_policy.valid_from_epoch <= current_image.installed_epoch_id
          AND (current_policy.valid_to_epoch IS NULL OR
               current_image.installed_epoch_id < current_policy.valid_to_epoch)
          AND 1 = (
              SELECT count(*)
              FROM groundloop_decision_policy AS strict_policy
              WHERE strict_policy.valid_from_epoch <=
                    current_image.installed_epoch_id
                AND (strict_policy.valid_to_epoch IS NULL OR
                     current_image.installed_epoch_id <
                     strict_policy.valid_to_epoch)
          )
          AND predecessor.revision = m5_head.sealed_revision
          AND predecessor.structural_status = 'committed'
          AND predecessor.semantic_status = 'sealed'
          AND predecessor.evaluation_state = 'complete'
          AND predecessor.sealed_at IS NOT NULL
        FOR UPDATE OF epoch, runtime, typed_update, direct_update,
                      predecessor, current_image
        """,
        (epoch_id, source_id, source_id, predecessor_epoch_id),
    ).fetchone()
    if source is None:
        raise InvalidEventError("structural matching source authority is incomplete")
    source_identity_hash = _sha256_text(source[0])
    if expected_source_identity_hash is not None and (
        expected_source_identity_hash != source_identity_hash
    ):
        raise EventConflictError("structural matching source hash changed")
    if (
        int(source[1]) != predecessor_epoch_id
        or int(source[1]) != int(source[7])
        or int(source[2]) != int(source[8])
        or int(source[6]) != 1
    ):
        raise EventConflictError("structural matching predecessor point changed")
    update_kind = _text(source[4])
    if update_kind != "document_insert":
        _fail_nonempty(f"structural source {update_kind}")

    affected = cursor.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_group_family
            WHERE creator_epoch_id = %s),
          (SELECT count(*) FROM groundloop_m5_group_version
            WHERE creator_epoch_id = %s),
          (SELECT count(*) FROM groundloop_m5_requirement_version
            WHERE creator_epoch_id = %s),
          (SELECT count(*) FROM groundloop_m5_group_deactivation
            WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_m5_working_requirement_state
            WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_m5_working_group_state
            WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_m5_working_claim_state
            WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_m5_working_answer_state
            WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_m5_working_group_certificate_binding
            WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_m5_working_claim_certificate_binding
            WHERE epoch_id = %s)
        """,
        (epoch_id,) * 10,
    ).fetchone()
    if affected is None or any(int(value) != 0 for value in affected):
        _fail_nonempty("structural source image")

    values: dict[str, Any] = {
        "source_kind": source_kind,
        "source_id": source_id,
        "source_identity_hash": source_identity_hash,
        "before_epoch_id": int(source[1]),
        "before_revision": int(source[2]),
        "resulting_epoch_id": epoch_id,
        "resulting_revision": resulting_revision,
        "decision_policy_version": _text(source[3]),
        "group_shapes": (),
        "observation_ids": (),
        "edge_keys": (),
        "mask_keys": (),
        "hall_group_ids": (),
        "requirement_state_ids": (),
        "group_state_ids": (),
        "claim_state_ids": (),
        "answer_state_ids": (),
        "group_certificate_ids": (),
        "claim_certificate_ids": (),
    }
    intent_digest = digests.persisted_matching_transition_intent_digest(**values)
    return M5PersistedMatchingTransitionIntent(**values, intent_digest=intent_digest)


def _overlay_work_from_values(values: Sequence[int]) -> M5OverlayWork:
    if len(values) != len(MATCHING_WORK_COUNTER_NAMES):
        raise ValidationError("persisted matching work has the wrong cardinality")
    checked = tuple(int(value) for value in values)
    if any(value < 0 for value in checked):
        raise ValidationError("persisted matching work cannot be negative")
    return M5OverlayWork(
        matching=MatchingWorkCounters(*checked[:31]),
        requirement_state_only_changes=checked[31],
        group_state_only_changes=checked[32],
        claim_state_only_changes=checked[33],
        group_certificate_only_changes=checked[34],
        claim_certificate_only_changes=checked[35],
        public_status_deltas=checked[36],
    )


def _matching_work_read_context(cursor: Cursor[Any], epoch_id: int) -> int:
    """Validate a retained accumulator's nonterminal, failed, or sealed envelope."""

    _, scoped_revision = _require_checked_prefix(cursor, epoch_id=epoch_id)
    current = cursor.execute(
        """
        SELECT decision_policy_version, installed_epoch_id, installed_revision
        FROM groundloop_m5_matching_image_current
        WHERE singleton
        FOR UPDATE
        """
    ).fetchone()
    if current is None:
        raise ValidationError("persisted matching current image is missing")
    current_policy = _text(current[0])
    current_epoch = int(current[1])
    current_revision = int(current[2])

    working = cursor.execute(
        """
        SELECT epoch_id, base_epoch_id, base_revision,
               decision_policy_version, updated_revision
        FROM groundloop_m5_matching_image_working
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (epoch_id,),
    ).fetchone()
    if working is None:
        raise ValidationError("persisted matching working image is missing")
    working_epoch = int(working[0])
    working_base_epoch = int(working[1])
    working_base_revision = int(working[2])
    working_policy = _text(working[3])
    working_updated_revision = int(working[4])

    context = cursor.execute(
        """
        SELECT epoch.revision, epoch.structural_status,
               epoch.semantic_status, epoch.evaluation_state,
               epoch.publication_mode, epoch.sealed_at IS NOT NULL,
               runtime.revision, runtime.runtime_state,
               runtime.terminal_at IS NOT NULL,
               runtime.expected_previous_published_epoch_id,
               typed_update.previous_published_epoch_id,
               typed_update.decision_policy_version,
               typed_policy.candidate_policy_manifest_hash,
               runtime.candidate_policy_manifest_hash,
               typed_policy.decision_policy_version,
               predecessor.revision,
               predecessor.structural_status,
               predecessor.semantic_status,
               predecessor.evaluation_state,
               predecessor.publication_mode,
               predecessor.sealed_at IS NOT NULL,
               m4_head.epoch_id, m5_head.epoch_id,
               m5_head.sealed_revision, live_head.revision,
               live_head.structural_status,
               live_head.semantic_status,
               live_head.evaluation_state,
               live_head.publication_mode,
               live_head.sealed_at IS NOT NULL,
               (SELECT count(*)
                  FROM groundloop_decision_policy AS base_policy
                 WHERE base_policy.valid_from_epoch <= %s
                   AND (base_policy.valid_to_epoch IS NULL
                        OR %s < base_policy.valid_to_epoch)),
               EXISTS (
                 SELECT 1 FROM groundloop_decision_policy AS base_selected
                  WHERE base_selected.policy_version = %s
                    AND base_selected.valid_from_epoch <= %s
                    AND (base_selected.valid_to_epoch IS NULL
                         OR %s < base_selected.valid_to_epoch)
               ),
               (SELECT count(*)
                  FROM groundloop_decision_policy AS current_policy
                 WHERE current_policy.valid_from_epoch <= %s
                   AND (current_policy.valid_to_epoch IS NULL
                        OR %s < current_policy.valid_to_epoch)),
               EXISTS (
                 SELECT 1 FROM groundloop_decision_policy AS current_selected
                  WHERE current_selected.policy_version = %s
                    AND current_selected.valid_from_epoch <= %s
                    AND (current_selected.valid_to_epoch IS NULL
                         OR %s < current_selected.valid_to_epoch)
               ),
               direct_update.epoch_id,
               direct_update.previous_published_epoch_id,
               direct_update.candidate_policy_id,
               runtime.candidate_policy_id,
               direct_policy.decision_policy_version
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_update AS typed_update USING (epoch_id)
        JOIN groundloop_m5_candidate_policy AS typed_policy
          ON typed_policy.candidate_policy_id = runtime.candidate_policy_id
        JOIN groundloop_m4_publication_head AS m4_head ON m4_head.singleton
        JOIN groundloop_m5_publication_head AS m5_head ON m5_head.singleton
        JOIN groundloop_epoch AS live_head ON live_head.epoch_id = m5_head.epoch_id
        JOIN groundloop_epoch AS predecessor
          ON predecessor.epoch_id = typed_update.previous_published_epoch_id
        LEFT JOIN groundloop_m4_update AS direct_update
          ON direct_update.epoch_id = epoch.epoch_id
        LEFT JOIN groundloop_candidate_policy AS direct_policy
          ON direct_policy.candidate_policy_id = direct_update.candidate_policy_id
        WHERE epoch.epoch_id = %s
        """,
        (
            working_base_epoch,
            working_base_epoch,
            working_policy,
            working_base_epoch,
            working_base_epoch,
            current_epoch,
            current_epoch,
            current_policy,
            current_epoch,
            current_epoch,
            epoch_id,
        ),
    ).fetchone()
    if context is None:
        raise ValidationError("persisted matching work envelope is incomplete")
    runtime_state = _text(context[7])
    direct_present = context[34] is not None
    if (
        working_epoch != epoch_id
        or int(context[0]) != int(context[6])
        or int(context[6]) != scoped_revision
        or int(context[9]) != working_base_epoch
        or int(context[10]) != working_base_epoch
        or int(context[15]) != working_base_revision
        or _text(context[11]) != working_policy
        or _text(context[14]) != working_policy
        or _sha256_text(context[12]) != _sha256_text(context[13])
        or _text(context[16]) != "committed"
        or _text(context[17]) != "sealed"
        or _text(context[18]) != "complete"
        or _text(context[19]) != "strict"
        or not bool(context[20])
        or int(context[21]) != current_epoch
        or int(context[22]) != current_epoch
        or int(context[23]) != current_revision
        or int(context[24]) != current_revision
        or _text(context[25]) != "committed"
        or _text(context[26]) != "sealed"
        or _text(context[27]) != "complete"
        or _text(context[28]) != "strict"
        or not bool(context[29])
        or int(context[30]) != 1
        or not bool(context[31])
        or int(context[32]) != 1
        or not bool(context[33])
        or working_updated_revision > int(context[6])
        or (
            direct_present
            and (
                int(context[35]) != working_base_epoch
                or _text(context[36]) != _text(context[37])
                or _text(context[38]) != working_policy
            )
        )
    ):
        raise ValidationError("persisted matching work envelope is inconsistent")

    if runtime_state == "sealed":
        valid_terminal = (
            bool(context[8])
            and _text(context[1]) == "committed"
            and _text(context[2]) == "sealed"
            and _text(context[3]) == "complete"
            and _text(context[4]) == "strict"
            and bool(context[5])
            and current_epoch >= epoch_id
        )
    elif runtime_state == "failed":
        valid_terminal = (
            bool(context[8])
            and _text(context[1]) == "failed"
            and _text(context[2]) == "failed"
            and _text(context[3]) == "failed"
            and _text(context[4]) == "provisional"
            and not bool(context[5])
            and (current_epoch == working_base_epoch or current_epoch > epoch_id)
        )
    else:
        pending_shape = (
            runtime_state in {"structural_committed", "semantic_pending"}
            and _text(context[1]) == "committed"
            and _text(context[2]) == "pending"
            and _text(context[3]) == "pending"
        )
        complete_shape = (
            runtime_state == "semantic_complete"
            and _text(context[1]) == "committed"
            and _text(context[2]) == "complete"
            and _text(context[3]) == "complete"
        )
        valid_terminal = (
            (pending_shape or complete_shape)
            and not bool(context[8])
            and _text(context[4]) == "provisional"
            and not bool(context[5])
            and current_epoch == working_base_epoch
            and current_revision == working_base_revision
            and current_policy == working_policy
        )
    if not valid_terminal:
        raise ValidationError("persisted matching work terminal envelope is invalid")
    return working_updated_revision


def _read_matching_work_accumulator(
    cursor: Cursor[Any], epoch_id: int
) -> tuple[M5OverlayWork, int]:
    _require_positive_int("epoch_id", epoch_id)
    require_persisted_matching_bundle(cursor)
    working_updated_revision = _matching_work_read_context(cursor, epoch_id)
    return _read_locked_matching_work_accumulator(
        cursor, epoch_id, expected_revision=working_updated_revision
    )


def _read_locked_matching_work_accumulator(
    cursor: Cursor[Any], epoch_id: int, *, expected_revision: int
) -> tuple[M5OverlayWork, int]:
    """Read tier 15k after the caller has already locked/validated tier 11b."""

    columns = sql.SQL(", ").join(
        sql.Identifier(name) for name in MATCHING_WORK_COUNTER_NAMES
    )
    row = cursor.execute(
        sql.SQL(
            "SELECT {}, matching_work_digest, updated_revision FROM "
            "groundloop_m5_matching_work_accumulator WHERE epoch_id = %s FOR SHARE"
        ).format(columns),
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise InvalidEventError("typed epoch has no persisted matching work")
    work = _overlay_work_from_values(tuple(int(value) for value in row[:-2]))
    digest = _sha256_text(row[-2])
    updated_revision = int(row[-1])
    if digest != digests.matching_work_digest(m5_overlay_work_values(work)):
        raise ValidationError("persisted matching accumulator digest is inconsistent")
    if updated_revision != expected_revision:
        raise ValidationError(
            "persisted matching accumulator revision differs from its working image"
        )
    return work, updated_revision


def current_matching_work(cursor: Cursor[Any], epoch_id: int) -> M5OverlayWork:
    """Read checked retained work under the exact epoch and image lock scope."""

    return _read_matching_work_accumulator(cursor, epoch_id)[0]


def _empty_patch_artifact(
    intent: M5PersistedMatchingTransitionIntent,
) -> M5PersistedMatchingPatchArtifact:
    logical_output = digests.logical_output_preimage(())
    if (
        logical_output != _EMPTY_LOGICAL_OUTPUT_BYTES
        or len(logical_output) != _EMPTY_OUTPUT_BYTES
        or hashlib.sha256(logical_output).hexdigest() != _EMPTY_LOGICAL_OUTPUT_DIGEST
    ):
        raise ValidationError("empty logical-output golden image changed")
    logical_preimage = digests.logical_overlay_patch_preimage(
        (), (), _EMPTY_LOGICAL_OUTPUT_DIGEST, _EMPTY_OUTPUT_BYTES
    )
    logical_digest = digests.logical_overlay_patch_digest(
        (), (), _EMPTY_LOGICAL_OUTPUT_DIGEST, _EMPTY_OUTPUT_BYTES
    )
    logical_patch = M5PersistedLogicalOverlayPatch(
        resulting_revision=intent.resulting_revision,
        changes=(),
        binding_rows=(),
        output_records=(),
        logical_output_preimage=logical_output,
        logical_output_digest=_EMPTY_LOGICAL_OUTPUT_DIGEST,
        output_bytes=_EMPTY_OUTPUT_BYTES,
        patch_preimage=logical_preimage,
        patch_digest=logical_digest,
    )
    work = M5OverlayWork(matching=MatchingWorkCounters(output_bytes=71))
    work_digest = digests.matching_work_digest(m5_overlay_work_values(work))
    shapes: tuple[tuple[str, int, tuple[tuple[int, str], ...]], ...] = ()
    shape_digest = digests.matching_group_shape_set_digest(shapes)
    patch_values: dict[str, Any] = {
        "source_kind": intent.source_kind,
        "source_id": intent.source_id,
        "source_identity_hash": intent.source_identity_hash,
        "before_epoch_id": intent.before_epoch_id,
        "before_revision": intent.before_revision,
        "resulting_epoch_id": intent.resulting_epoch_id,
        "resulting_revision": intent.resulting_revision,
        "decision_policy_version": intent.decision_policy_version,
        "group_shape_set_digest": shape_digest,
        "observation_change_digests": (),
        "edge_change_digests": (),
        "mask_change_digests": (),
        "hall_change_digests": (),
        "logical_overlay_patch_digest_value": logical_digest,
        "matching_work_digest_value": work_digest,
    }
    patch_digest = digests.persisted_matching_patch_digest(**patch_values)
    patch = M5PersistedMatchingPatch(
        source_kind=intent.source_kind,
        source_id=intent.source_id,
        source_identity_hash=intent.source_identity_hash,
        before_epoch_id=intent.before_epoch_id,
        before_revision=intent.before_revision,
        resulting_epoch_id=intent.resulting_epoch_id,
        resulting_revision=intent.resulting_revision,
        decision_policy_version=intent.decision_policy_version,
        group_shape_set_digest=shape_digest,
        observation_change_digests=(),
        edge_change_digests=(),
        mask_change_digests=(),
        hall_change_digests=(),
        logical_overlay_patch_digest=logical_digest,
        matching_work_digest=work_digest,
        patch_digest=patch_digest,
    )
    return M5PersistedMatchingPatchArtifact(
        patch=patch,
        group_shapes=(),
        group_shape_set_preimage=digests.matching_group_shape_set_preimage(shapes),
        observation_changes=(),
        observation_change_preimages=(),
        edge_changes=(),
        edge_change_preimages=(),
        mask_changes=(),
        mask_change_preimages=(),
        hall_changes=(),
        hall_change_preimages=(),
        logical_patch=logical_patch,
        work=work,
        patch_preimage=digests.persisted_matching_patch_preimage(**patch_values),
    )


def _matching_contribution(
    artifact: M5PersistedMatchingPatchArtifact,
) -> M5PersistedMatchingContribution:
    patch = artifact.patch
    contribution_digest = digests.matching_work_contribution_digest(
        epoch_id=patch.resulting_epoch_id,
        source_kind=patch.source_kind,
        source_id=patch.source_id,
        source_identity_hash=patch.source_identity_hash,
        before_epoch_id=patch.before_epoch_id,
        before_revision=patch.before_revision,
        resulting_revision=patch.resulting_revision,
        patch_digest=patch.patch_digest,
        matching_work_digest_value=patch.matching_work_digest,
    )
    return M5PersistedMatchingContribution(
        epoch_id=patch.resulting_epoch_id,
        source_kind=patch.source_kind,
        source_id=patch.source_id,
        source_identity_hash=patch.source_identity_hash,
        before_epoch_id=patch.before_epoch_id,
        before_revision=patch.before_revision,
        resulting_revision=patch.resulting_revision,
        patch_digest=patch.patch_digest,
        work=artifact.work,
        contribution_digest=contribution_digest,
    )


def _insert_empty_artifact(
    cursor: Cursor[Any], artifact: M5PersistedMatchingPatchArtifact
) -> None:
    patch = artifact.patch
    cursor.execute(
        """
        INSERT INTO groundloop_m5_matching_patch_artifact (
          patch_digest, source_kind, source_id, source_identity_hash,
          before_epoch_id, before_revision, resulting_epoch_id,
          resulting_revision, decision_policy_version,
          group_shape_set_digest, group_shape_set_preimage,
          observation_change_digests, observation_change_preimages,
          edge_change_digests, edge_change_preimages,
          mask_change_digests, mask_change_preimages,
          hall_change_digests, hall_change_preimages,
          logical_overlay_patch_digest, logical_overlay_patch_preimage,
          logical_output_preimage, matching_work_digest,
          canonical_patch_preimage
        ) VALUES (
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
          %s::char(64)[],%s::bytea[],%s::char(64)[],%s::bytea[],
          %s::char(64)[],%s::bytea[],%s::char(64)[],%s::bytea[],
          %s,%s,%s,%s,%s
        )
        ON CONFLICT (patch_digest) DO NOTHING
        """,
        (
            patch.patch_digest,
            patch.source_kind.value,
            patch.source_id,
            patch.source_identity_hash,
            patch.before_epoch_id,
            patch.before_revision,
            patch.resulting_epoch_id,
            patch.resulting_revision,
            patch.decision_policy_version,
            patch.group_shape_set_digest,
            artifact.group_shape_set_preimage,
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            patch.logical_overlay_patch_digest,
            artifact.logical_patch.patch_preimage,
            artifact.logical_patch.logical_output_preimage,
            patch.matching_work_digest,
            artifact.patch_preimage,
        ),
    )


def _insert_contribution_and_accumulator(
    cursor: Cursor[Any], contribution: M5PersistedMatchingContribution
) -> None:
    counter_columns = sql.SQL(", ").join(
        sql.Identifier(name) for name in MATCHING_WORK_COUNTER_NAMES
    )
    contribution_columns = sql.SQL(", ").join(
        (
            sql.SQL(
                "epoch_id, source_kind, source_id, source_identity_hash, "
                "before_epoch_id, before_revision, resulting_revision, patch_digest"
            ),
            counter_columns,
            sql.SQL("matching_work_digest, contribution_digest"),
        )
    )
    contribution_values = (
        contribution.epoch_id,
        contribution.source_kind.value,
        contribution.source_id,
        contribution.source_identity_hash,
        contribution.before_epoch_id,
        contribution.before_revision,
        contribution.resulting_revision,
        contribution.patch_digest,
        *m5_overlay_work_values(contribution.work),
        digests.matching_work_digest(m5_overlay_work_values(contribution.work)),
        contribution.contribution_digest,
    )
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_matching_work_contribution ({}) VALUES ({})"
        ).format(
            contribution_columns,
            sql.SQL(", ").join(sql.Placeholder() for _ in contribution_values),
        ),
        contribution_values,
    )
    accumulator_columns = sql.SQL(", ").join(
        (
            sql.SQL("epoch_id"),
            counter_columns,
            sql.SQL("matching_work_digest, updated_revision"),
        )
    )
    accumulator_values = (
        contribution.epoch_id,
        *m5_overlay_work_values(contribution.work),
        digests.matching_work_digest(m5_overlay_work_values(contribution.work)),
        contribution.resulting_revision,
    )
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_matching_work_accumulator ({}) VALUES ({})"
        ).format(
            accumulator_columns,
            sql.SQL(", ").join(sql.Placeholder() for _ in accumulator_values),
        ),
        accumulator_values,
    )


def _digest_array(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValidationError("persisted digest array has an invalid shape")
    return tuple(_sha256_text(item) for item in value)


def _bytea_array(value: object) -> tuple[bytes, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValidationError("persisted preimage array has an invalid shape")
    return tuple(bytes(item) for item in value)


def _lock_matching_transition_images(
    cursor: Cursor[Any], intent: M5PersistedMatchingTransitionIntent
) -> bool:
    """Lock tier 11b current then working and validate the transition point."""

    expected_runtime_revision = (
        1
        if intent.source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN
        else intent.before_revision
    )
    _require_checked_prefix(
        cursor,
        epoch_id=intent.resulting_epoch_id,
        expected_revision=expected_runtime_revision,
    )
    current = cursor.execute(
        """
        SELECT decision_policy_version, installed_epoch_id, installed_revision
        FROM groundloop_m5_matching_image_current
        WHERE singleton
        FOR UPDATE
        """
    ).fetchone()
    if current is None or (
        _text(current[0]) != intent.decision_policy_version
        or int(current[1]) != intent.before_epoch_id
        or int(current[2]) != intent.before_revision
    ):
        raise EventConflictError("matching transition current image point changed")
    working = cursor.execute(
        """
        SELECT epoch_id, base_epoch_id, base_revision,
               decision_policy_version, updated_revision
        FROM groundloop_m5_matching_image_working
        WHERE epoch_id = %s
        FOR UPDATE
        """,
        (intent.resulting_epoch_id,),
    ).fetchone()
    if working is None:
        return False
    if (
        int(working[0]) != intent.resulting_epoch_id
        or int(working[1]) != intent.before_epoch_id
        or int(working[2]) != intent.before_revision
        or _text(working[3]) != intent.decision_policy_version
        or int(working[4]) != intent.resulting_revision
    ):
        raise EventConflictError("matching replay differs from retained image bytes")
    return True


def _matching_artifact_row(
    cursor: Cursor[Any], patch_digest: str
) -> tuple[Any, ...] | None:
    return cursor.execute(
        """
        SELECT patch_digest, source_kind, source_id, source_identity_hash,
               before_epoch_id, before_revision, resulting_epoch_id,
               resulting_revision, decision_policy_version,
               group_shape_set_digest, group_shape_set_preimage,
               observation_change_digests, observation_change_preimages,
               edge_change_digests, edge_change_preimages,
               mask_change_digests, mask_change_preimages,
               hall_change_digests, hall_change_preimages,
               logical_overlay_patch_digest, logical_overlay_patch_preimage,
               logical_output_preimage, matching_work_digest,
               canonical_patch_preimage
        FROM groundloop_m5_matching_patch_artifact
        WHERE patch_digest = %s
        FOR SHARE
        """,
        (patch_digest,),
    ).fetchone()


def _lock_matching_artifact_key(
    cursor: Cursor[Any], patch_digest: str
) -> tuple[Any, ...] | None:
    lock_key = int(patch_digest[:8], 16)
    if lock_key >= 2**31:
        lock_key -= 2**32
    cursor.execute(
        "SELECT pg_advisory_xact_lock(%s, %s)",
        (_MATCHING_ARTIFACT_LOCK_NAMESPACE, lock_key),
    )
    return _matching_artifact_row(cursor, patch_digest)


def _validate_matching_artifact_row(
    artifact_row: tuple[Any, ...], expected: M5PersistedMatchingPatchArtifact
) -> None:
    expected_patch = expected.patch
    if (
        _sha256_text(artifact_row[0]) != expected_patch.patch_digest
        or _text(artifact_row[1]) != expected_patch.source_kind.value
        or _text(artifact_row[2]) != expected_patch.source_id
        or _sha256_text(artifact_row[3]) != expected_patch.source_identity_hash
        or int(artifact_row[4]) != expected_patch.before_epoch_id
        or int(artifact_row[5]) != expected_patch.before_revision
        or int(artifact_row[6]) != expected_patch.resulting_epoch_id
        or int(artifact_row[7]) != expected_patch.resulting_revision
        or _text(artifact_row[8]) != expected_patch.decision_policy_version
        or _sha256_text(artifact_row[9]) != expected_patch.group_shape_set_digest
        or bytes(artifact_row[10]) != expected.group_shape_set_preimage
        or _digest_array(artifact_row[11]) != expected_patch.observation_change_digests
        or _bytea_array(artifact_row[12]) != expected.observation_change_preimages
        or _digest_array(artifact_row[13]) != expected_patch.edge_change_digests
        or _bytea_array(artifact_row[14]) != expected.edge_change_preimages
        or _digest_array(artifact_row[15]) != expected_patch.mask_change_digests
        or _bytea_array(artifact_row[16]) != expected.mask_change_preimages
        or _digest_array(artifact_row[17]) != expected_patch.hall_change_digests
        or _bytea_array(artifact_row[18]) != expected.hall_change_preimages
        or _sha256_text(artifact_row[19]) != expected_patch.logical_overlay_patch_digest
        or bytes(artifact_row[20]) != expected.logical_patch.patch_preimage
        or bytes(artifact_row[21]) != expected.logical_patch.logical_output_preimage
        or _sha256_text(artifact_row[22]) != expected_patch.matching_work_digest
        or bytes(artifact_row[23]) != expected.patch_preimage
    ):
        raise EventConflictError("matching replay differs from retained patch bytes")


def _insert_or_validate_matching_artifact(
    cursor: Cursor[Any], artifact: M5PersistedMatchingPatchArtifact
) -> None:
    """Serialize globally by digest, then insert or byte-validate at tier 15i."""

    existing = _lock_matching_artifact_key(cursor, artifact.patch.patch_digest)
    if existing is not None:
        _validate_matching_artifact_row(existing, artifact)
    _insert_empty_artifact(cursor, artifact)
    stored = _matching_artifact_row(cursor, artifact.patch.patch_digest)
    if stored is None:
        raise ValidationError("persisted matching artifact insert was not retained")
    _validate_matching_artifact_row(stored, artifact)


def _lock_matching_contribution(
    cursor: Cursor[Any], intent: M5PersistedMatchingTransitionIntent
) -> tuple[Any, ...] | None:
    counter_columns = sql.SQL(", ").join(
        sql.Identifier(name) for name in MATCHING_WORK_COUNTER_NAMES
    )
    source_row = cursor.execute(
        sql.SQL(
            """
            SELECT epoch_id, source_kind, source_id, source_identity_hash,
                   before_epoch_id, before_revision, resulting_revision,
                   patch_digest,
                   {},
                   matching_work_digest, contribution_digest
            FROM groundloop_m5_matching_work_contribution
            WHERE epoch_id = %s AND source_kind = %s AND source_id = %s
            FOR SHARE
            """
        ).format(counter_columns),
        (intent.resulting_epoch_id, intent.source_kind.value, intent.source_id),
    ).fetchone()
    revision_row = cursor.execute(
        """
        SELECT epoch_id, source_kind, source_id, resulting_revision
        FROM groundloop_m5_matching_work_contribution
        WHERE epoch_id = %s AND resulting_revision = %s
        FOR SHARE
        """,
        (intent.resulting_epoch_id, intent.resulting_revision),
    ).fetchone()
    if source_row is None and revision_row is None:
        return None
    if (
        source_row is None
        or revision_row is None
        or (
            int(source_row[0]),
            _text(source_row[1]),
            _text(source_row[2]),
            int(source_row[6]),
        )
        != (
            int(revision_row[0]),
            _text(revision_row[1]),
            _text(revision_row[2]),
            int(revision_row[3]),
        )
    ):
        raise EventConflictError("matching contribution keys name different sources")
    return tuple(source_row)


def _read_matching_transition_replay(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    expected: M5PersistedMatchingPatchArtifact,
    *,
    expected_patch_digest: str | None,
    expected_work: M5OverlayWork | None,
) -> M5PersistedMatchingPatchReceipt | None:
    artifact_row = _lock_matching_artifact_key(cursor, expected.patch.patch_digest)
    if artifact_row is None:
        raise EventConflictError("matching replay lacks its retained patch artifact")
    _validate_matching_artifact_row(artifact_row, expected)
    contribution_row = _lock_matching_contribution(cursor, intent)
    if contribution_row is None:
        return None
    counter_start = 8
    counter_end = counter_start + len(MATCHING_WORK_COUNTER_NAMES)
    stored_work = _overlay_work_from_values(
        tuple(int(value) for value in contribution_row[counter_start:counter_end])
    )
    stored_work_digest = _sha256_text(contribution_row[counter_end])
    contribution_digest = _sha256_text(contribution_row[counter_end + 1])
    expected_patch = expected.patch
    expected_contribution = _matching_contribution(expected)
    if (
        int(contribution_row[0]) != intent.resulting_epoch_id
        or _text(contribution_row[1]) != intent.source_kind.value
        or _text(contribution_row[2]) != intent.source_id
        or _sha256_text(contribution_row[3]) != intent.source_identity_hash
        or int(contribution_row[4]) != intent.before_epoch_id
        or int(contribution_row[5]) != intent.before_revision
        or int(contribution_row[6]) != intent.resulting_revision
        or _sha256_text(contribution_row[7]) != expected_patch.patch_digest
        or stored_work != expected.work
        or stored_work_digest != expected_patch.matching_work_digest
        or contribution_digest != expected_contribution.contribution_digest
    ):
        raise EventConflictError(
            "matching replay differs from retained contribution bytes"
        )

    if expected_patch_digest is not None and (
        expected_patch_digest != expected_patch.patch_digest
    ):
        raise EventConflictError("matching replay differs from expected patch digest")
    if expected_work is not None and expected_work != stored_work:
        raise EventConflictError("matching replay differs from expected work")
    accumulated, accumulator_revision = _read_locked_matching_work_accumulator(
        cursor,
        intent.resulting_epoch_id,
        expected_revision=intent.resulting_revision,
    )
    if accumulated != stored_work or accumulator_revision != intent.resulting_revision:
        raise ValidationError("structural matching accumulator changed after replay")
    return M5PersistedMatchingPatchReceipt(
        expected_patch,
        contribution_digest,
        accumulated,
        intent.resulting_revision,
        True,
    )


def apply_matching_transition(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    expected_patch_digest: str | None = None,
    expected_work: M5OverlayWork | None = None,
) -> M5PersistedMatchingPatchReceipt:
    """Apply or exactly replay the store-derived empty structural transition."""

    if type(intent) is not M5PersistedMatchingTransitionIntent:
        raise ValidationError("matching transition requires the exact intent DTO")
    if expected_patch_digest is not None:
        _require_sha256("expected_patch_digest", expected_patch_digest)
    if expected_work is not None and type(expected_work) is not M5OverlayWork:
        raise ValidationError("expected_work must be an exact M5OverlayWork")
    expected_revision = (
        1
        if intent.source_kind is M5PersistedMatchingSourceKind.STRUCTURAL_OPEN
        else intent.before_revision
    )
    recomputed = derive_matching_transition_intent(
        cursor,
        intent.resulting_epoch_id,
        expected_revision,
        intent.resulting_revision,
        intent.source_kind,
        intent.source_id,
        intent.source_identity_hash,
    )
    if recomputed != intent:
        raise EventConflictError("matching transition intent changed before apply")
    artifact = _empty_patch_artifact(recomputed)
    if expected_patch_digest is not None and (
        expected_patch_digest != artifact.patch.patch_digest
    ):
        raise EventConflictError("computed matching patch digest differs")
    if expected_work is not None and expected_work != artifact.work:
        raise EventConflictError("computed matching work differs")
    working_image_exists = _lock_matching_transition_images(cursor, recomputed)
    if working_image_exists:
        replay = _read_matching_transition_replay(
            cursor,
            recomputed,
            artifact,
            expected_patch_digest=expected_patch_digest,
            expected_work=expected_work,
        )
        if replay is None:
            raise EventConflictError("matching replay lost its retained contribution")
        return replay

    cursor.execute(
        "SELECT groundloop_m5_authorize_persisted_matching_transition(%s,%s,%s,%s,%s)",
        (
            recomputed.resulting_epoch_id,
            1,
            recomputed.resulting_revision,
            recomputed.source_kind.value,
            recomputed.source_id,
        ),
    )
    cursor.execute(
        """
        INSERT INTO groundloop_m5_matching_image_working (
          epoch_id, base_epoch_id, base_revision,
          decision_policy_version, updated_revision
        ) VALUES (%s,%s,%s,%s,%s)
        """,
        (
            recomputed.resulting_epoch_id,
            recomputed.before_epoch_id,
            recomputed.before_revision,
            recomputed.decision_policy_version,
            recomputed.resulting_revision,
        ),
    )
    _insert_or_validate_matching_artifact(cursor, artifact)
    contribution = _matching_contribution(artifact)
    if _lock_matching_contribution(cursor, recomputed) is not None:
        raise EventConflictError("matching contribution appeared during first apply")
    _insert_contribution_and_accumulator(cursor, contribution)
    cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    cursor.execute("SET CONSTRAINTS ALL DEFERRED")
    return M5PersistedMatchingPatchReceipt(
        artifact.patch,
        contribution.contribution_digest,
        artifact.work,
        recomputed.resulting_revision,
        False,
    )


__all__ = [
    "apply_matching_transition",
    "current_matching_work",
    "derive_matching_transition_intent",
    "effective_matching_edge",
    "effective_matching_hall",
    "effective_matching_image",
    "effective_matching_mask",
    "effective_matching_observation",
    "least_effective_observation",
    "representative_effective_hashes",
    "require_persisted_matching_bundle",
    "resolved_matching_edge_point",
    "resolved_matching_hall_point",
    "resolved_matching_mask_point",
    "resolved_matching_observation_point",
]
