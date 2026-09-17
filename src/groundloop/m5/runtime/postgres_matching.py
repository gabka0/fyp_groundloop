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
    return str(value).strip()


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
    if row is None or tuple(_text(value) for value in row) != (
        _PERSISTED_MATCHING_BUNDLE_ROW
    ):
        raise InvalidEventError(
            "typed M5 matching requires the exact accepted migration-017 bundle"
        )


def _authorize_checked_prefix(
    cursor: Cursor[Any], *, epoch_id: int, expected_revision: int
) -> None:
    """Acquire/revalidate the frozen tier-6 epoch/runtime CAS prefix."""

    cursor.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
        (epoch_id, expected_revision),
    )


def _require_checked_prefix(
    cursor: Cursor[Any], *, epoch_id: int, expected_revision: int | None = None
) -> tuple[int, int]:
    """Revalidate the transaction-local checked prefix and its exact rows."""

    setting = cursor.execute(
        "SELECT current_setting('groundloop.m5_checked_transition', true)"
    ).fetchone()
    if setting is None or setting[0] != "on":
        raise ValidationError("matching point read lacks the checked epoch prefix")
    row = cursor.execute(
        """
        SELECT epoch.revision, runtime.revision
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        WHERE epoch.epoch_id = %s
        FOR UPDATE OF epoch, runtime
        """,
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise InvalidEventError("matching point read names an unknown typed epoch")
    revisions = (int(row[0]), int(row[1]))
    if revisions[0] != revisions[1] or (
        expected_revision is not None and revisions[0] != expected_revision
    ):
        raise EventConflictError("matching point read lost its epoch/runtime CAS")
    return revisions


def effective_matching_image(
    cursor: Cursor[Any], epoch_id: int
) -> M5MatchingImagePoint:
    """Return and lock the exact current-plus-requested-working image headers."""

    _require_positive_int("epoch_id", epoch_id)
    require_persisted_matching_bundle(cursor)
    _require_checked_prefix(cursor, epoch_id=epoch_id)
    row = cursor.execute(
        """
        SELECT current_image.decision_policy_version,
               current_image.installed_epoch_id,
               current_image.installed_revision,
               working_image.epoch_id,
               working_image.base_epoch_id,
               working_image.base_revision,
               working_image.decision_policy_version,
               working_image.updated_revision
        FROM groundloop_m5_matching_image_current AS current_image
        JOIN groundloop_m5_matching_image_working AS working_image
          ON working_image.epoch_id = %s
        JOIN groundloop_m5_update AS typed_update
          ON typed_update.epoch_id = working_image.epoch_id
        JOIN groundloop_m4_publication_head AS m4_head ON m4_head.singleton
        JOIN groundloop_m5_publication_head AS m5_head ON m5_head.singleton
        JOIN groundloop_epoch AS predecessor
          ON predecessor.epoch_id = current_image.installed_epoch_id
        WHERE current_image.singleton
          AND current_image.installed_epoch_id = m4_head.epoch_id
          AND current_image.installed_epoch_id = m5_head.epoch_id
          AND current_image.installed_revision = m5_head.sealed_revision
          AND predecessor.revision = current_image.installed_revision
          AND predecessor.structural_status = 'committed'
          AND predecessor.semantic_status = 'sealed'
          AND predecessor.evaluation_state = 'complete'
          AND predecessor.sealed_at IS NOT NULL
          AND working_image.base_epoch_id = current_image.installed_epoch_id
          AND working_image.base_revision = current_image.installed_revision
          AND working_image.decision_policy_version =
              typed_update.decision_policy_version
        FOR UPDATE OF current_image, working_image
        """,
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise ValidationError("persisted matching image headers are inconsistent")
    return M5MatchingImagePoint(
        _text(row[0]),
        int(row[1]),
        int(row[2]),
        int(row[3]),
        int(row[4]),
        int(row[5]),
        _text(row[6]),
        int(row[7]),
    )


def resolved_matching_observation_point(
    cursor: Cursor[Any], epoch_id: int, observation_id: str
) -> M5MatchingObservationPoint | None:
    """Resolve working-before-current without filtering a working tombstone."""

    _require_positive_int("epoch_id", epoch_id)
    _require_text("observation_id", observation_id)
    effective_matching_image(cursor, epoch_id)
    row = cursor.execute(
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
    if row is not None:
        return M5MatchingObservationWorking(
            M5MatchingLayer.WORKING,
            int(row[0]),
            _text(row[1]),
            _text(row[2]),
            _text(row[3]),
            int(row[4]),
            _text(row[5]),
            bool(row[6]),
            int(row[7]),
        )
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
    if current is None:
        return None
    return M5MatchingObservationCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        _text(current[1]),
        _text(current[2]),
        int(current[3]),
        _text(current[4]),
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
    row = cursor.execute(
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
    if row is not None:
        return M5MatchingEdgeWorking(
            M5MatchingLayer.WORKING,
            int(row[0]),
            _text(row[1]),
            _text(row[2]),
            _text(row[3]),
            int(row[4]),
            int(row[5]),
            int(row[6]),
        )
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
    if current is None:
        return None
    return M5MatchingEdgeCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        _text(current[1]),
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
    row = cursor.execute(
        """
        SELECT epoch_id, group_version_id, text_hash, mask, updated_revision
        FROM groundloop_m5_matching_hash_mask_working
        WHERE epoch_id = %s AND group_version_id = %s AND text_hash = %s
        FOR UPDATE
        """,
        (epoch_id, group_version_id, text_hash),
    ).fetchone()
    if row is not None:
        return M5MatchingMaskWorking(
            M5MatchingLayer.WORKING,
            int(row[0]),
            _text(row[1]),
            _text(row[2]),
            int(row[3]),
            int(row[4]),
        )
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
    if current is None:
        return None
    return M5MatchingMaskCurrent(
        M5MatchingLayer.CURRENT,
        _text(current[0]),
        _text(current[1]),
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
    row = cursor.execute(
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
    if row is not None:
        present = bool(row[2])
        return M5MatchingHallWorking(
            M5MatchingLayer.WORKING,
            int(row[0]),
            _text(row[1]),
            present,
            None if row[3] is None else int(row[3]),
            None if row[4] is None else _tuple_ints(row[4]),
            None if row[5] is None else _tuple_ints(row[5]),
            None if row[6] is None else _tuple_ints(row[6]),
            None if row[7] is None else int(row[7]),
            None if row[8] is None else int(row[8]),
            None if row[9] is None else int(row[9]),
            int(row[10]),
        )
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
    hashes = tuple(_text(row[0]) for row in rows)
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
    _authorize_checked_prefix(
        cursor, epoch_id=epoch_id, expected_revision=expected_runtime_revision
    )
    if source_kind is not M5PersistedMatchingSourceKind.STRUCTURAL_OPEN:
        _fail_nonempty(source_kind.value)
    if expected_runtime_revision != 1 or resulting_revision != 1:
        raise EventConflictError("structural matching intent must use runtime point 1")

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
        WHERE epoch.epoch_id = %s
          AND epoch.event_id = %s
          AND runtime.structural_event_id = %s
          AND predecessor.structural_status = 'committed'
          AND predecessor.semantic_status = 'sealed'
          AND predecessor.evaluation_state = 'complete'
          AND predecessor.sealed_at IS NOT NULL
        FOR UPDATE OF epoch, runtime, typed_update, predecessor, current_image
        """,
        (epoch_id, source_id, source_id),
    ).fetchone()
    if source is None:
        raise InvalidEventError("structural matching source authority is incomplete")
    source_identity_hash = _text(source[0])
    if expected_source_identity_hash is not None and (
        expected_source_identity_hash != source_identity_hash
    ):
        raise EventConflictError("structural matching source hash changed")
    if (
        int(source[1]) != int(source[7])
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


def current_matching_work(cursor: Cursor[Any], epoch_id: int) -> M5OverlayWork:
    """Read and digest-check the retained accumulator for any epoch state."""

    _require_positive_int("epoch_id", epoch_id)
    require_persisted_matching_bundle(cursor)
    columns = sql.SQL(", ").join(
        sql.Identifier(name) for name in MATCHING_WORK_COUNTER_NAMES
    )
    row = cursor.execute(
        sql.SQL(
            "SELECT {}, matching_work_digest FROM "
            "groundloop_m5_matching_work_accumulator WHERE epoch_id = %s FOR SHARE"
        ).format(columns),
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise InvalidEventError("typed epoch has no persisted matching work")
    work = _overlay_work_from_values(tuple(int(value) for value in row[:-1]))
    digest = _text(row[-1])
    if digest != digests.matching_work_digest(m5_overlay_work_values(work)):
        raise ValidationError("persisted matching accumulator digest is inconsistent")
    return work


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


def _read_matching_transition_replay(
    cursor: Cursor[Any],
    intent: M5PersistedMatchingTransitionIntent,
    expected: M5PersistedMatchingPatchArtifact,
    *,
    expected_patch_digest: str | None,
    expected_work: M5OverlayWork | None,
) -> M5PersistedMatchingPatchReceipt | None:
    counter_columns = sql.SQL(", ").join(
        sql.Identifier(name) for name in MATCHING_WORK_COUNTER_NAMES
    )
    row = cursor.execute(
        sql.SQL(
            """
            SELECT contribution.source_identity_hash,
                   contribution.before_epoch_id,
                   contribution.before_revision,
                   contribution.resulting_revision,
                   contribution.patch_digest,
                   {},
                   contribution.matching_work_digest,
                   contribution.contribution_digest,
                   artifact.group_shape_set_preimage,
                   artifact.observation_change_digests,
                   artifact.observation_change_preimages,
                   artifact.edge_change_digests,
                   artifact.edge_change_preimages,
                   artifact.mask_change_digests,
                   artifact.mask_change_preimages,
                   artifact.hall_change_digests,
                   artifact.hall_change_preimages,
                   artifact.logical_overlay_patch_preimage,
                   artifact.logical_output_preimage,
                   artifact.canonical_patch_preimage,
                   artifact.decision_policy_version
            FROM groundloop_m5_matching_work_contribution AS contribution
            JOIN groundloop_m5_matching_patch_artifact AS artifact
              ON artifact.patch_digest = contribution.patch_digest
            WHERE contribution.epoch_id = %s
              AND contribution.source_kind = %s
              AND contribution.source_id = %s
            FOR SHARE OF contribution, artifact
            """
        ).format(counter_columns),
        (intent.resulting_epoch_id, intent.source_kind.value, intent.source_id),
    ).fetchone()
    if row is None:
        conflict = cursor.execute(
            """
            SELECT source_kind, source_id
            FROM groundloop_m5_matching_work_contribution
            WHERE epoch_id = %s AND resulting_revision = %s
            FOR SHARE
            """,
            (intent.resulting_epoch_id, intent.resulting_revision),
        ).fetchone()
        if conflict is not None:
            raise EventConflictError(
                "matching resulting revision belongs to a different source"
            )
        return None
    counter_start = 5
    counter_end = counter_start + len(MATCHING_WORK_COUNTER_NAMES)
    stored_work = _overlay_work_from_values(
        tuple(int(value) for value in row[counter_start:counter_end])
    )
    stored_work_digest = _text(row[counter_end])
    contribution_digest = _text(row[counter_end + 1])
    retained = row[counter_end + 2 :]
    expected_patch = expected.patch
    expected_contribution = _matching_contribution(expected)
    if (
        _text(row[0]) != intent.source_identity_hash
        or int(row[1]) != intent.before_epoch_id
        or int(row[2]) != intent.before_revision
        or int(row[3]) != intent.resulting_revision
        or _text(row[4]) != expected_patch.patch_digest
        or stored_work != expected.work
        or stored_work_digest != expected_patch.matching_work_digest
        or contribution_digest != expected_contribution.contribution_digest
        or bytes(retained[0]) != expected.group_shape_set_preimage
        or tuple(retained[1] or ())
        or tuple(retained[2] or ())
        or tuple(retained[3] or ())
        or tuple(retained[4] or ())
        or tuple(retained[5] or ())
        or tuple(retained[6] or ())
        or tuple(retained[7] or ())
        or tuple(retained[8] or ())
        or bytes(retained[9]) != expected.logical_patch.patch_preimage
        or bytes(retained[10]) != expected.logical_patch.logical_output_preimage
        or bytes(retained[11]) != expected.patch_preimage
        or _text(retained[12]) != expected_patch.decision_policy_version
    ):
        raise EventConflictError("matching replay differs from retained patch bytes")
    if expected_patch_digest is not None and (
        expected_patch_digest != expected_patch.patch_digest
    ):
        raise EventConflictError("matching replay differs from expected patch digest")
    if expected_work is not None and expected_work != stored_work:
        raise EventConflictError("matching replay differs from expected work")
    accumulated = current_matching_work(cursor, intent.resulting_epoch_id)
    if accumulated != stored_work:
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
    replay = _read_matching_transition_replay(
        cursor,
        recomputed,
        artifact,
        expected_patch_digest=expected_patch_digest,
        expected_work=expected_work,
    )
    if replay is not None:
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
    _insert_empty_artifact(cursor, artifact)
    contribution = _matching_contribution(artifact)
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
