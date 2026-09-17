"""Cursor-local D25 activation, promotion, and publication-child helpers.

The functions in this module deliberately do not own a transaction.  Their
caller must already be inside the one activation or typed-seal transaction and
is responsible for the semantic promotion, publication heads, durable result,
and commit/rollback boundary.  Migration 017 remains the final database-side
authority for every row written here.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from psycopg import Cursor

from groundloop.domain import StatusDelta
from groundloop.errors import InvalidEventError, ValidationError
from groundloop.m5.digests import hash_field, stable_m5_digest, text_field
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5ChangedStateReference,
    M5StateReferenceKind,
)


@dataclass(frozen=True, slots=True)
class M5MatchingPublicationChildren:
    """Store-derived immutable children for one sealed event result."""

    combined_deltas: tuple[StatusDelta, ...]
    changed_state_references: tuple[M5ChangedStateReference, ...]


@dataclass(frozen=True, slots=True)
class M5MatchingPromotionReceipt:
    """Non-semantic row counts returned by one cursor-local projection."""

    mode: str
    epoch_id: int
    revision: int
    observation_writes: int
    observation_deletes: int
    edge_writes: int
    edge_deletes: int
    mask_writes: int
    mask_deletes: int
    hall_writes: int
    hall_deletes: int

    def __post_init__(self) -> None:
        if self.mode not in {"activation", "seal"}:
            raise ValidationError("matching promotion receipt has an invalid mode")
        if isinstance(self.epoch_id, bool) or self.epoch_id <= 0:
            raise ValidationError("matching promotion receipt requires an epoch")
        if isinstance(self.revision, bool) or self.revision < 0:
            raise ValidationError("matching promotion receipt has an invalid revision")
        for name in (
            "observation_writes",
            "observation_deletes",
            "edge_writes",
            "edge_deletes",
            "mask_writes",
            "mask_deletes",
            "hall_writes",
            "hall_deletes",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValidationError(f"{name} must be a nonnegative integer")


@dataclass(frozen=True, slots=True)
class _LogicalChange:
    revision: int
    source_kind: str
    source_id: str
    source_identity_hash: str
    before_epoch_id: int
    before_revision: int
    kind: str
    object_id: str
    before_hash: str | None
    after_hash: str | None
    output_is_absent: bool


@dataclass(frozen=True, slots=True)
class _SealEnvelope:
    event_id: str
    payload_hash: str
    epoch_id: int
    revision: int
    previous_epoch_id: int
    previous_revision: int
    update_kind: str
    predecessor_group_id: str | None
    deactivation_action: str | None
    successor_group_id: str | None


_REFERENCE_KIND_BY_WIRE = {value.value: value for value in M5StateReferenceKind}
_D26_KINDS = frozenset(
    {
        M5StateReferenceKind.REQUIREMENT_STATE.value,
        M5StateReferenceKind.GROUP_STATE.value,
        M5StateReferenceKind.GROUP_CERTIFICATE.value,
    }
)


def _require_nonnegative_int(name: str, value: int, *, positive: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{name} must be an integer")
    if value < (1 if positive else 0):
        raise ValidationError(f"{name} is outside its accepted range")


def _strip_hash(value: object, *, name: str) -> str:
    result = str(value).strip()
    if len(result) != 64 or any(
        character not in "0123456789abcdef" for character in result
    ):
        raise ValidationError(f"{name} is not a canonical SHA-256 value")
    return result


def install_matching_activation_projection(
    cursor: Cursor[Any],
    *,
    expected_m4_head_epoch_id: int,
    expected_head_epoch_revision: int,
    decision_policy_version: str,
) -> M5MatchingPromotionReceipt:
    """Install the D25 current image inside the public activation transaction.

    This is the only full-scan helper in this module.  Migration 017 explicitly
    permits the activation bootstrap to use the independent SQL initializer;
    the measured open/completion/seal paths must never call this function.
    """

    _require_nonnegative_int(
        "expected_m4_head_epoch_id", expected_m4_head_epoch_id, positive=True
    )
    _require_nonnegative_int(
        "expected_head_epoch_revision", expected_head_epoch_revision
    )
    if (
        not isinstance(decision_policy_version, str)
        or not decision_policy_version.strip()
    ):
        raise ValidationError("decision_policy_version must be nonempty")

    cursor.execute(
        "SELECT groundloop_m5_authorize_persisted_matching_activation(%s,%s,%s)",
        (
            expected_m4_head_epoch_id,
            expected_head_epoch_revision,
            decision_policy_version,
        ),
    )
    cursor.execute(
        """
        INSERT INTO groundloop_m5_matching_image_current (
            singleton, decision_policy_version,
            installed_epoch_id, installed_revision
        ) VALUES (true, %s, %s, %s)
        """,
        (
            decision_policy_version,
            expected_m4_head_epoch_id,
            expected_head_epoch_revision,
        ),
    )

    observation_writes = cursor.execute(
        """
        INSERT INTO groundloop_m5_matching_observation_current (
            observation_id, requirement_version_id, group_version_id,
            requirement_ordinal, text_hash,
            installed_epoch_id, installed_revision
        )
        SELECT observation.observation_id, source.requirement_version_id,
               source.group_version_id, source.requirement_ordinal,
               source.text_hash, %s, %s
        FROM groundloop_m5_active_requirement_edge_oracle AS source
        CROSS JOIN LATERAL unnest(source.active_observation_ids)
             AS observation(observation_id)
        ORDER BY observation.observation_id COLLATE "C"
        """,
        (expected_m4_head_epoch_id, expected_head_epoch_revision),
    ).rowcount
    edge_writes = cursor.execute(
        """
        INSERT INTO groundloop_m5_matching_edge_current (
            requirement_version_id, text_hash, group_version_id,
            requirement_ordinal, refcount,
            installed_epoch_id, installed_revision
        )
        SELECT requirement_version_id, text_hash, group_version_id,
               requirement_ordinal, cardinality(active_observation_ids), %s, %s
        FROM groundloop_m5_active_requirement_edge_oracle
        ORDER BY group_version_id COLLATE "C", requirement_ordinal,
                 text_hash COLLATE "C", requirement_version_id COLLATE "C"
        """,
        (expected_m4_head_epoch_id, expected_head_epoch_revision),
    ).rowcount
    mask_writes = cursor.execute(
        """
        INSERT INTO groundloop_m5_matching_hash_mask_current (
            group_version_id, text_hash, mask,
            installed_epoch_id, installed_revision
        )
        SELECT group_version_id, text_hash,
               sum(1 << requirement_ordinal)::integer, %s, %s
        FROM groundloop_m5_active_requirement_edge_oracle
        GROUP BY group_version_id, text_hash
        ORDER BY group_version_id COLLATE "C", text_hash COLLATE "C"
        """,
        (expected_m4_head_epoch_id, expected_head_epoch_revision),
    ).rowcount
    hall_writes = cursor.execute(
        """
        WITH expected AS (
            SELECT hall.group_version_id,
                   max(hall.requirement_count)::integer AS requirement_count,
                   ARRAY[0::bigint] ||
                     array_agg(hall.neighbor_count ORDER BY hall.subset_mask)
                       AS neighbor_counts,
                   ARRAY[0::bigint] ||
                     array_agg(hall.deficiency ORDER BY hall.subset_mask)
                       AS deficiencies
            FROM groundloop_m5_group_subset_hall_oracle AS hall
            GROUP BY hall.group_version_id
        ), masks AS (
            SELECT group_version_id, text_hash,
                   sum(1 << requirement_ordinal)::integer AS mask
            FROM groundloop_m5_active_requirement_edge_oracle
            GROUP BY group_version_id, text_hash
        ), completed AS (
            SELECT expected.*,
                   ARRAY[0::bigint] || ARRAY(
                       SELECT count(mask_row.group_version_id)::bigint
                       FROM generate_series(
                           1, (1 << expected.requirement_count) - 1
                       ) AS generated(mask_value)
                       LEFT JOIN masks AS mask_row
                         ON mask_row.group_version_id = expected.group_version_id
                        AND mask_row.mask = generated.mask_value
                       GROUP BY generated.mask_value
                       ORDER BY generated.mask_value
                   ) AS mask_histogram
            FROM expected
        )
        INSERT INTO groundloop_m5_matching_hall_current (
            group_version_id, requirement_count, mask_histogram,
            neighbor_counts, deficiencies, maximum_deficiency,
            matching_size, distinct_hash_count,
            installed_epoch_id, installed_revision
        )
        SELECT group_version_id, requirement_count, mask_histogram,
               neighbor_counts, deficiencies,
               greatest(0, (SELECT max(value) FROM unnest(deficiencies) value)),
               requirement_count - greatest(
                   0, (SELECT max(value) FROM unnest(deficiencies) value)
               ),
               (SELECT coalesce(sum(value), 0) FROM unnest(mask_histogram) value),
               %s, %s
        FROM completed
        ORDER BY group_version_id COLLATE "C"
        """,
        (expected_m4_head_epoch_id, expected_head_epoch_revision),
    ).rowcount
    return M5MatchingPromotionReceipt(
        mode="activation",
        epoch_id=expected_m4_head_epoch_id,
        revision=expected_head_epoch_revision,
        observation_writes=observation_writes,
        observation_deletes=0,
        edge_writes=edge_writes,
        edge_deletes=0,
        mask_writes=mask_writes,
        mask_deletes=0,
        hall_writes=hall_writes,
        hall_deletes=0,
    )


def promote_matching_overlay(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    sealed_revision: int,
) -> M5MatchingPromotionReceipt:
    """Promote one epoch-prefixed D25 overlay into the current physical image."""

    _require_nonnegative_int("epoch_id", epoch_id, positive=True)
    _require_nonnegative_int("expected_revision", expected_revision, positive=True)
    _require_nonnegative_int("sealed_revision", sealed_revision, positive=True)
    if sealed_revision != expected_revision + 1:
        raise ValidationError("sealed_revision must be expected_revision + 1")

    cursor.execute("SELECT set_config('groundloop.m5_checked_transition','on',true)")
    cursor.execute(
        "SELECT groundloop_m5_authorize_persisted_matching_seal(%s,%s,%s)",
        (epoch_id, expected_revision, sealed_revision),
    )
    cursor.execute(
        """
        UPDATE groundloop_m5_matching_image_current AS current_image
        SET decision_policy_version = working.decision_policy_version,
            installed_epoch_id = %s,
            installed_revision = %s
        FROM groundloop_m5_matching_image_working AS working
        WHERE current_image.singleton AND working.epoch_id = %s
        """,
        (epoch_id, sealed_revision, epoch_id),
    )
    if cursor.rowcount != 1:
        raise ValidationError("matching seal did not update one current image header")

    observation_deletes = cursor.execute(
        """
        DELETE FROM groundloop_m5_matching_observation_current AS current_row
        USING groundloop_m5_matching_observation_working AS working
        WHERE working.epoch_id = %s AND NOT working.present
          AND current_row.observation_id = working.observation_id
        """,
        (epoch_id,),
    ).rowcount
    observation_writes = cursor.execute(
        """
        INSERT INTO groundloop_m5_matching_observation_current (
            observation_id, requirement_version_id, group_version_id,
            requirement_ordinal, text_hash,
            installed_epoch_id, installed_revision
        )
        SELECT observation_id, requirement_version_id, group_version_id,
               requirement_ordinal, text_hash, %s, %s
        FROM groundloop_m5_matching_observation_working
        WHERE epoch_id = %s AND present
        ORDER BY observation_id COLLATE "C"
        ON CONFLICT (observation_id) DO UPDATE SET
            requirement_version_id = EXCLUDED.requirement_version_id,
            group_version_id = EXCLUDED.group_version_id,
            requirement_ordinal = EXCLUDED.requirement_ordinal,
            text_hash = EXCLUDED.text_hash,
            installed_epoch_id = EXCLUDED.installed_epoch_id,
            installed_revision = EXCLUDED.installed_revision
        """,
        (epoch_id, sealed_revision, epoch_id),
    ).rowcount

    edge_deletes = cursor.execute(
        """
        DELETE FROM groundloop_m5_matching_edge_current AS current_row
        USING groundloop_m5_matching_edge_working AS working
        WHERE working.epoch_id = %s AND working.refcount = 0
          AND current_row.requirement_version_id = working.requirement_version_id
          AND current_row.text_hash = working.text_hash
        """,
        (epoch_id,),
    ).rowcount
    edge_writes = cursor.execute(
        """
        INSERT INTO groundloop_m5_matching_edge_current (
            requirement_version_id, text_hash, group_version_id,
            requirement_ordinal, refcount,
            installed_epoch_id, installed_revision
        )
        SELECT requirement_version_id, text_hash, group_version_id,
               requirement_ordinal, refcount, %s, %s
        FROM groundloop_m5_matching_edge_working
        WHERE epoch_id = %s AND refcount > 0
        ORDER BY group_version_id COLLATE "C", requirement_ordinal,
                 text_hash COLLATE "C", requirement_version_id COLLATE "C"
        ON CONFLICT (requirement_version_id, text_hash) DO UPDATE SET
            group_version_id = EXCLUDED.group_version_id,
            requirement_ordinal = EXCLUDED.requirement_ordinal,
            refcount = EXCLUDED.refcount,
            installed_epoch_id = EXCLUDED.installed_epoch_id,
            installed_revision = EXCLUDED.installed_revision
        """,
        (epoch_id, sealed_revision, epoch_id),
    ).rowcount

    mask_deletes = cursor.execute(
        """
        DELETE FROM groundloop_m5_matching_hash_mask_current AS current_row
        USING groundloop_m5_matching_hash_mask_working AS working
        WHERE working.epoch_id = %s AND working.mask = 0
          AND current_row.group_version_id = working.group_version_id
          AND current_row.text_hash = working.text_hash
        """,
        (epoch_id,),
    ).rowcount
    mask_writes = cursor.execute(
        """
        INSERT INTO groundloop_m5_matching_hash_mask_current (
            group_version_id, text_hash, mask,
            installed_epoch_id, installed_revision
        )
        SELECT group_version_id, text_hash, mask, %s, %s
        FROM groundloop_m5_matching_hash_mask_working
        WHERE epoch_id = %s AND mask > 0
        ORDER BY group_version_id COLLATE "C", text_hash COLLATE "C"
        ON CONFLICT (group_version_id, text_hash) DO UPDATE SET
            mask = EXCLUDED.mask,
            installed_epoch_id = EXCLUDED.installed_epoch_id,
            installed_revision = EXCLUDED.installed_revision
        """,
        (epoch_id, sealed_revision, epoch_id),
    ).rowcount

    hall_deletes = cursor.execute(
        """
        DELETE FROM groundloop_m5_matching_hall_current AS current_row
        USING groundloop_m5_matching_hall_working AS working
        WHERE working.epoch_id = %s AND NOT working.present
          AND current_row.group_version_id = working.group_version_id
        """,
        (epoch_id,),
    ).rowcount
    hall_writes = cursor.execute(
        """
        INSERT INTO groundloop_m5_matching_hall_current (
            group_version_id, requirement_count, mask_histogram,
            neighbor_counts, deficiencies, maximum_deficiency,
            matching_size, distinct_hash_count,
            installed_epoch_id, installed_revision
        )
        SELECT group_version_id, requirement_count, mask_histogram,
               neighbor_counts, deficiencies, maximum_deficiency,
               matching_size, distinct_hash_count, %s, %s
        FROM groundloop_m5_matching_hall_working
        WHERE epoch_id = %s AND present
        ORDER BY group_version_id COLLATE "C"
        ON CONFLICT (group_version_id) DO UPDATE SET
            requirement_count = EXCLUDED.requirement_count,
            mask_histogram = EXCLUDED.mask_histogram,
            neighbor_counts = EXCLUDED.neighbor_counts,
            deficiencies = EXCLUDED.deficiencies,
            maximum_deficiency = EXCLUDED.maximum_deficiency,
            matching_size = EXCLUDED.matching_size,
            distinct_hash_count = EXCLUDED.distinct_hash_count,
            installed_epoch_id = EXCLUDED.installed_epoch_id,
            installed_revision = EXCLUDED.installed_revision
        """,
        (epoch_id, sealed_revision, epoch_id),
    ).rowcount

    return M5MatchingPromotionReceipt(
        mode="seal",
        epoch_id=epoch_id,
        revision=sealed_revision,
        observation_writes=observation_writes,
        observation_deletes=observation_deletes,
        edge_writes=edge_writes,
        edge_deletes=edge_deletes,
        mask_writes=mask_writes,
        mask_deletes=mask_deletes,
        hall_writes=hall_writes,
        hall_deletes=hall_deletes,
    )


def _load_seal_envelope(
    cursor: Cursor[Any], *, epoch_id: int, sealed_revision: int
) -> _SealEnvelope:
    rows = cursor.execute(
        """
        SELECT epoch.event_id, epoch.payload_hash, epoch.epoch_id, epoch.revision,
               epoch.structural_status, epoch.semantic_status,
               epoch.evaluation_state, epoch.publication_mode,
               epoch.sealed_at IS NOT NULL,
               runtime.structural_event_id, runtime.runtime_state,
               runtime.terminal_at IS NOT NULL, runtime.revision,
               runtime.expected_previous_published_epoch_id,
               update_row.previous_published_epoch_id, update_row.update_kind,
               m4_head.epoch_id, m5_head.epoch_id, m5_head.sealed_revision,
               deactivation.event_id, deactivation.group_version_id,
               deactivation.action, deactivation.successor_group_version_id,
               count(deactivation.epoch_id) OVER (), predecessor_epoch.revision
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_update AS update_row USING (epoch_id)
        JOIN groundloop_epoch AS predecessor_epoch
          ON predecessor_epoch.epoch_id = update_row.previous_published_epoch_id
        JOIN groundloop_m4_publication_head AS m4_head ON m4_head.singleton
        JOIN groundloop_m5_publication_head AS m5_head ON m5_head.singleton
        LEFT JOIN groundloop_m5_group_deactivation AS deactivation
          ON deactivation.epoch_id = epoch.epoch_id
        WHERE epoch.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchall()
    if len(rows) != 1:
        raise InvalidEventError("sealed event has an invalid deactivation cardinality")
    row = rows[0]
    if (
        int(row[3]) != sealed_revision
        or tuple(row[4:9]) != ("committed", "sealed", "complete", "strict", True)
        or str(row[9]) != str(row[0])
        or tuple(row[10:13]) != ("sealed", True, sealed_revision)
        or int(row[13]) != int(row[14])
        or tuple(int(row[index]) for index in (16, 17, 18))
        != (epoch_id, epoch_id, sealed_revision)
        or (row[19] is not None and str(row[19]) != str(row[0]))
        or int(row[23]) not in {0, 1}
    ):
        raise InvalidEventError("sealed event coordinates are inconsistent")
    return _SealEnvelope(
        event_id=str(row[0]),
        payload_hash=_strip_hash(row[1], name="event payload hash"),
        epoch_id=epoch_id,
        revision=sealed_revision,
        previous_epoch_id=int(row[14]),
        previous_revision=int(row[24]),
        update_kind=str(row[15]),
        predecessor_group_id=(None if row[20] is None else str(row[20])),
        deactivation_action=(None if row[21] is None else str(row[21])),
        successor_group_id=(None if row[22] is None else str(row[22])),
    )


def _load_logical_changes(
    cursor: Cursor[Any], *, epoch_id: int
) -> tuple[_LogicalChange, ...]:
    rows = cursor.execute(
        """
        SELECT contribution.resulting_revision, contribution.source_kind,
               contribution.source_id, contribution.source_identity_hash,
               contribution.before_epoch_id, contribution.before_revision,
               change.value->'children'->0->>'value' AS kind,
               change.value->'children'->1->>'value' AS object_id,
               CASE WHEN change.value->'children'->2->>'tag' = 'null'
                    THEN NULL ELSE change.value->'children'->2->>'value' END,
               CASE WHEN change.value->'children'->3->>'tag' = 'null'
                    THEN NULL ELSE change.value->'children'->3->>'value' END,
               EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements(
                       groundloop_m5_matching_validate_logical_output(
                           patch.logical_output_preimage
                       )
                   ) AS output(value)
                   WHERE output.value->>'kind' =
                         change.value->'children'->0->>'value'
                     AND output.value->>'object_id' =
                         change.value->'children'->1->>'value'
                     AND output.value->'after'->>'tag' = 'none'
               ),
               patch.logical_overlay_patch_digest,
               patch.logical_overlay_patch_preimage
        FROM groundloop_m5_matching_work_contribution AS contribution
        JOIN groundloop_m5_matching_patch_artifact AS patch
          ON patch.patch_digest = contribution.patch_digest
        CROSS JOIN LATERAL jsonb_array_elements(
            groundloop_m5_matching_parse_typed_preimage(
                patch.logical_overlay_patch_preimage,
                'm5-persisted-logical-overlay-patch-v1'
            )->'children'->0->'children'
        ) AS change(value)
        WHERE contribution.epoch_id = %s
        ORDER BY contribution.resulting_revision,
                 (change.value->'children'->0->>'value') COLLATE "C",
                 (change.value->'children'->1->>'value') COLLATE "C"
        """,
        (epoch_id,),
    ).fetchall()
    verified_preimages: dict[str, bytes] = {}
    changes: list[_LogicalChange] = []
    for row in rows:
        patch_digest = _strip_hash(row[11], name="logical patch digest")
        patch_preimage = bytes(row[12])
        previous = verified_preimages.setdefault(patch_digest, patch_preimage)
        if (
            previous != patch_preimage
            or hashlib.sha256(patch_preimage).hexdigest() != patch_digest
        ):
            raise ValidationError("logical patch digest/preimage mismatch")
        kind = str(row[6])
        if kind not in _REFERENCE_KIND_BY_WIRE:
            raise ValidationError("logical patch contains an unknown state kind")
        before_hash = (
            None if row[8] is None else _strip_hash(row[8], name="logical before hash")
        )
        after_hash = (
            None if row[9] is None else _strip_hash(row[9], name="logical after hash")
        )
        output_is_absent = bool(row[10])
        if output_is_absent != (after_hash is None):
            raise ValidationError("logical change and output presence disagree")
        changes.append(
            _LogicalChange(
                revision=int(row[0]),
                source_kind=str(row[1]),
                source_id=str(row[2]),
                source_identity_hash=_strip_hash(row[3], name="source identity hash"),
                before_epoch_id=int(row[4]),
                before_revision=int(row[5]),
                kind=kind,
                object_id=str(row[7]),
                before_hash=before_hash,
                after_hash=after_hash,
                output_is_absent=output_is_absent,
            )
        )
    return tuple(changes)


def _net_logical_changes(
    changes: tuple[_LogicalChange, ...],
) -> dict[tuple[str, str], tuple[str | None, str | None, tuple[_LogicalChange, ...]]]:
    grouped: dict[tuple[str, str], list[_LogicalChange]] = {}
    for change in changes:
        grouped.setdefault((change.kind, change.object_id), []).append(change)
    result: dict[
        tuple[str, str], tuple[str | None, str | None, tuple[_LogicalChange, ...]]
    ] = {}
    for key, values in grouped.items():
        ordered = tuple(sorted(values, key=lambda value: value.revision))
        for before, after in zip(ordered, ordered[1:], strict=False):
            if before.after_hash != after.before_hash:
                raise ValidationError(
                    "logical change history is not a continuous chain"
                )
        if ordered[0].before_hash != ordered[-1].after_hash:
            result[key] = (
                ordered[0].before_hash,
                ordered[-1].after_hash,
                ordered,
            )
    return result


def _present_artifact_hash(
    cursor: Cursor[Any],
    *,
    kind: str,
    object_id: str,
    epoch_id: int,
    revision: int,
) -> str:
    if kind == M5StateReferenceKind.REQUIREMENT_STATE.value:
        row = cursor.execute(
            """
            SELECT requirement_version_id, witness_hashes,
                   supporting_observation_ids, witness_count, satisfied,
                   decision_policy_version
            FROM groundloop_m5_published_requirement_state
            WHERE requirement_version_id = %s
              AND valid_from_epoch = %s AND sealed_revision = %s
            """,
            (object_id, epoch_id, revision),
        ).fetchone()
        if row is None:
            raise ValidationError(
                "present requirement reference lacks its published row"
            )
        return digests.requirement_state_artifact_digest(
            requirement_version_id=str(row[0]),
            witness_hashes=tuple(str(value) for value in row[1]),
            supporting_observation_ids=tuple(str(value) for value in row[2]),
            witness_count=int(row[3]),
            satisfied=bool(row[4]),
            decision_policy_version=str(row[5]),
        )
    if kind == M5StateReferenceKind.GROUP_STATE.value:
        row = cursor.execute(
            """
            SELECT group_version_id, requirement_count, satisfied_count,
                   matching_size, complete, decision_policy_version,
                   certificate_digest
            FROM groundloop_m5_published_group_state
            WHERE group_version_id = %s
              AND valid_from_epoch = %s AND sealed_revision = %s
            """,
            (object_id, epoch_id, revision),
        ).fetchone()
        if row is None:
            raise ValidationError("present group reference lacks its published row")
        return digests.group_state_artifact_digest(
            group_version_id=str(row[0]),
            requirement_count=int(row[1]),
            satisfied_count=int(row[2]),
            matching_size=int(row[3]),
            complete=bool(row[4]),
            decision_policy_version=str(row[5]),
            certificate_digest=(
                None
                if row[6] is None
                else _strip_hash(row[6], name="group certificate")
            ),
        )
    if kind == M5StateReferenceKind.CLAIM_STATE.value:
        row = cursor.execute(
            """
            SELECT claim_id, support_count, refute_count,
                   best_support_score, best_refute_score,
                   supporting_observation_ids, refuting_observation_ids,
                   complete_group_count, complete_group_ids, status,
                   decision_policy_version, certificate_digest
            FROM groundloop_m5_published_claim_state
            WHERE claim_id = %s
              AND valid_from_epoch = %s AND sealed_revision = %s
            """,
            (object_id, epoch_id, revision),
        ).fetchone()
        if row is None:
            raise ValidationError("present claim reference lacks its published row")
        return digests.claim_state_artifact_digest(
            claim_id=str(row[0]),
            support_count=int(row[1]),
            refute_count=int(row[2]),
            best_support_score=None if row[3] is None else float(row[3]),
            best_refute_score=None if row[4] is None else float(row[4]),
            supporting_observation_ids=tuple(str(value) for value in row[5]),
            refuting_observation_ids=tuple(str(value) for value in row[6]),
            complete_group_count=int(row[7]),
            complete_group_ids=tuple(str(value) for value in row[8]),
            status=str(row[9]),
            decision_policy_version=str(row[10]),
            certificate_digest=_strip_hash(row[11], name="claim certificate"),
        )
    if kind == M5StateReferenceKind.ANSWER_STATE.value:
        row = cursor.execute(
            """
            SELECT answer_version_id, required_claim_count, supported_count,
                   unsupported_count, refuted_count, conflicted_count, status
            FROM groundloop_m5_published_answer_state
            WHERE answer_version_id = %s
              AND valid_from_epoch = %s AND sealed_revision = %s
            """,
            (object_id, epoch_id, revision),
        ).fetchone()
        if row is None:
            raise ValidationError("present answer reference lacks its published row")
        return digests.answer_state_artifact_digest(
            answer_version_id=str(row[0]),
            required_claim_count=int(row[1]),
            supported_count=int(row[2]),
            unsupported_count=int(row[3]),
            refuted_count=int(row[4]),
            conflicted_count=int(row[5]),
            status=str(row[6]),
        )
    if kind == M5StateReferenceKind.GROUP_CERTIFICATE.value:
        table, key = (
            "groundloop_m5_published_group_certificate_binding",
            "group_version_id",
        )
    elif kind == M5StateReferenceKind.CLAIM_CERTIFICATE.value:
        table, key = "groundloop_m5_published_claim_certificate_binding", "claim_id"
    else:
        raise ValidationError("unknown present reference kind")
    # The identifiers above are closed constants rather than caller-authored SQL.
    row = cursor.execute(
        f"SELECT certificate_digest FROM {table} "
        f"WHERE {key} = %s AND valid_from_epoch = %s AND sealed_revision = %s",
        (object_id, epoch_id, revision),
    ).fetchone()
    if row is None:
        raise ValidationError(
            "present certificate reference lacks its published binding"
        )
    return _strip_hash(row[0], name="published certificate digest")


def _validate_d26_absence_set(
    cursor: Cursor[Any],
    *,
    envelope: _SealEnvelope,
    absent: dict[tuple[str, str], tuple[str | None, tuple[_LogicalChange, ...]]],
) -> None:
    if envelope.update_kind not in {"replace_group", "retire_group"}:
        raise ValidationError(
            "absence references require replace_group or retire_group"
        )
    expected_action = "REPLACE" if envelope.update_kind == "replace_group" else "RETIRE"
    if (
        envelope.predecessor_group_id is None
        or envelope.deactivation_action != expected_action
        or (expected_action == "REPLACE") != (envelope.successor_group_id is not None)
    ):
        raise ValidationError("absence reference deactivation mapping is invalid")
    predecessor = envelope.predecessor_group_id
    expected_payload = stable_m5_digest(
        "m5-retire-group-event-v1", text_field(predecessor)
    )
    if expected_action == "REPLACE":
        assert envelope.successor_group_id is not None
        successor = cursor.execute(
            """
            SELECT successor.record_payload_hash,
                   groundloop_m5_expected_group_record(successor.group_version_id),
                   successor.creator_epoch_id, successor.lifecycle_state,
                   successor.group_family_id, predecessor.group_family_id,
                   successor.supersedes_group_version_id,
                   validity.valid_from_epoch, validity.valid_to_epoch,
                   count(requirement.requirement_version_id),
                   min(requirement.ordinal), max(requirement.ordinal),
                   count(DISTINCT requirement.ordinal),
                   count(*) FILTER (WHERE requirement.lifecycle_state='PUBLISHED')
            FROM groundloop_m5_group_version AS successor
            JOIN groundloop_m5_group_version AS predecessor
              ON predecessor.group_version_id = %s
            JOIN groundloop_m5_group_validity AS validity
              ON validity.group_version_id = successor.group_version_id
            JOIN groundloop_m5_requirement_version AS requirement
              ON requirement.group_version_id = successor.group_version_id
            WHERE successor.group_version_id = %s
            GROUP BY successor.group_version_id, predecessor.group_version_id,
                     validity.valid_from_epoch, validity.valid_to_epoch
            """,
            (predecessor, envelope.successor_group_id),
        ).fetchone()
        if successor is None:
            raise ValidationError("replacement absence lacks its exact successor")
        requirement_count = int(successor[9])
        if (
            _strip_hash(successor[0], name="successor record payload")
            != _strip_hash(successor[1], name="recomputed successor record payload")
            or int(successor[2]) != envelope.epoch_id
            or str(successor[3]) != "PUBLISHED"
            or str(successor[4]) != str(successor[5])
            or str(successor[6]) != predecessor
            or int(successor[7]) != envelope.epoch_id
            or successor[8] is not None
            or not 1 <= requirement_count <= 8
            or (
                int(successor[10]),
                int(successor[11]),
                int(successor[12]),
                int(successor[13]),
            )
            != (0, requirement_count - 1, requirement_count, requirement_count)
        ):
            raise ValidationError(
                "replacement successor is not the exact dense published successor"
            )
        expected_payload = stable_m5_digest(
            "m5-replace-group-event-v1",
            text_field(predecessor),
            hash_field(_strip_hash(successor[1], name="successor record payload")),
        )
    if expected_payload != envelope.payload_hash:
        raise ValidationError("absence event payload is not independently derivable")

    validity = cursor.execute(
        """
        SELECT version.lifecycle_state, validity.valid_from_epoch,
               validity.valid_to_epoch
        FROM groundloop_m5_group_version AS version
        JOIN groundloop_m5_group_validity AS validity USING (group_version_id)
        WHERE version.group_version_id = %s
          AND validity.valid_from_epoch <= %s
          AND %s < validity.valid_to_epoch
          AND validity.valid_to_epoch = %s
        """,
        (
            predecessor,
            envelope.previous_epoch_id,
            envelope.previous_epoch_id,
            envelope.epoch_id,
        ),
    ).fetchall()
    if len(validity) != 1 or str(validity[0][0]) != "PUBLISHED":
        raise ValidationError("absence predecessor was not present and closed exactly")

    expected: dict[tuple[str, str], str] = {}
    requirement_rows = cursor.execute(
        """
        SELECT requirement.requirement_version_id, requirement.lifecycle_state,
               state.witness_hashes, state.supporting_observation_ids,
               state.witness_count, state.satisfied,
               state.decision_policy_version
        FROM groundloop_m5_requirement_version AS requirement
        JOIN groundloop_m5_published_requirement_state AS state
          ON state.requirement_version_id = requirement.requirement_version_id
        WHERE requirement.group_version_id = %s
          AND state.valid_from_epoch <= %s AND %s < state.valid_to_epoch
          AND state.valid_to_epoch = %s
        ORDER BY requirement.requirement_version_id COLLATE "C"
        """,
        (
            predecessor,
            envelope.previous_epoch_id,
            envelope.previous_epoch_id,
            envelope.epoch_id,
        ),
    ).fetchall()
    total_requirements = cursor.execute(
        """
        SELECT count(*) FROM groundloop_m5_requirement_version
        WHERE group_version_id = %s AND lifecycle_state = 'PUBLISHED'
        """,
        (predecessor,),
    ).fetchone()
    if total_requirements is None or len(requirement_rows) != int(
        total_requirements[0]
    ):
        raise ValidationError("absence requirement predecessor closure is incomplete")
    for row in requirement_rows:
        if str(row[1]) != "PUBLISHED":
            raise ValidationError("absence requirement predecessor is not published")
        requirement_id = str(row[0])
        expected[(M5StateReferenceKind.REQUIREMENT_STATE.value, requirement_id)] = (
            digests.requirement_state_artifact_digest(
                requirement_version_id=requirement_id,
                witness_hashes=tuple(str(value) for value in row[2]),
                supporting_observation_ids=tuple(str(value) for value in row[3]),
                witness_count=int(row[4]),
                satisfied=bool(row[5]),
                decision_policy_version=str(row[6]),
            )
        )
    group_rows = cursor.execute(
        """
        SELECT requirement_count, satisfied_count, matching_size, complete,
               decision_policy_version, certificate_digest
        FROM groundloop_m5_published_group_state
        WHERE group_version_id = %s
          AND valid_from_epoch <= %s AND %s < valid_to_epoch
          AND valid_to_epoch = %s
        """,
        (
            predecessor,
            envelope.previous_epoch_id,
            envelope.previous_epoch_id,
            envelope.epoch_id,
        ),
    ).fetchall()
    if len(group_rows) != 1:
        raise ValidationError("absence group-state predecessor closure is incomplete")
    group_row = group_rows[0]
    expected[(M5StateReferenceKind.GROUP_STATE.value, predecessor)] = (
        digests.group_state_artifact_digest(
            group_version_id=predecessor,
            requirement_count=int(group_row[0]),
            satisfied_count=int(group_row[1]),
            matching_size=int(group_row[2]),
            complete=bool(group_row[3]),
            decision_policy_version=str(group_row[4]),
            certificate_digest=(
                None
                if group_row[5] is None
                else _strip_hash(group_row[5], name="predecessor certificate")
            ),
        )
    )
    binding_rows = cursor.execute(
        """
        SELECT binding.certificate_digest
        FROM groundloop_m5_published_group_certificate_binding AS binding
        JOIN groundloop_m5_group_certificate_artifact AS artifact
          ON artifact.certificate_digest = binding.certificate_digest
         AND artifact.group_version_id = binding.group_version_id
         AND groundloop_m5_expected_group_certificate(binding.certificate_digest)
             = binding.certificate_digest
        WHERE binding.group_version_id = %s
          AND binding.valid_from_epoch <= %s AND %s < binding.valid_to_epoch
          AND binding.valid_to_epoch = %s
        """,
        (
            predecessor,
            envelope.previous_epoch_id,
            envelope.previous_epoch_id,
            envelope.epoch_id,
        ),
    ).fetchall()
    if len(binding_rows) > 1:
        raise ValidationError("absence predecessor has multiple certificate bindings")
    if binding_rows:
        expected[(M5StateReferenceKind.GROUP_CERTIFICATE.value, predecessor)] = (
            _strip_hash(binding_rows[0][0], name="predecessor certificate binding")
        )

    if set(absent) != set(expected):
        raise ValidationError("D26 absence set is not the exact predecessor closure")
    for key, expected_before in expected.items():
        before_hash, history = absent[key]
        if before_hash != expected_before or len(history) != 1:
            raise ValidationError("D26 absence before hash/history is inconsistent")
        change = history[0]
        if (
            change.source_kind != "structural_open"
            or change.source_id != envelope.event_id
            or change.source_identity_hash != envelope.payload_hash
            or change.before_epoch_id != envelope.previous_epoch_id
            or change.before_revision != envelope.previous_revision
            or change.revision != 1
            or change.after_hash is not None
            or not change.output_is_absent
        ):
            raise ValidationError(
                "D26 absence does not come from the exact structural patch"
            )

    no_successor = cursor.execute(
        """
        SELECT
          EXISTS (
            SELECT 1
            FROM groundloop_m5_published_requirement_state AS state
            JOIN groundloop_m5_requirement_version AS requirement
              USING(requirement_version_id)
            WHERE requirement.group_version_id = %s
              AND (state.valid_from_epoch >= %s OR state.valid_to_epoch IS NULL
                   OR (state.valid_from_epoch <= %s AND %s < state.valid_to_epoch))
          ),
          EXISTS (
            SELECT 1 FROM groundloop_m5_published_group_state
            WHERE group_version_id = %s
              AND (valid_from_epoch >= %s OR valid_to_epoch IS NULL
                   OR (valid_from_epoch <= %s AND %s < valid_to_epoch))
          ),
          EXISTS (
            SELECT 1 FROM groundloop_m5_published_group_certificate_binding
            WHERE group_version_id = %s
              AND (valid_from_epoch >= %s OR valid_to_epoch IS NULL
                   OR (valid_from_epoch <= %s AND %s < valid_to_epoch))
          )
        """,
        (
            predecessor,
            envelope.epoch_id,
            envelope.epoch_id,
            envelope.epoch_id,
            predecessor,
            envelope.epoch_id,
            envelope.epoch_id,
            envelope.epoch_id,
            predecessor,
            envelope.epoch_id,
            envelope.epoch_id,
            envelope.epoch_id,
        ),
    ).fetchone()
    if no_successor is None or any(bool(value) for value in no_successor):
        raise ValidationError("D26 predecessor has a same-object successor")


def _build_combined_deltas(
    cursor: Cursor[Any],
    *,
    envelope: _SealEnvelope,
    changed_keys: set[tuple[str, str]],
) -> tuple[StatusDelta, ...]:
    deltas: list[StatusDelta] = []
    for kind, object_id in sorted(changed_keys):
        if kind not in {
            M5StateReferenceKind.CLAIM_STATE.value,
            M5StateReferenceKind.ANSWER_STATE.value,
        }:
            continue
        if kind == M5StateReferenceKind.CLAIM_STATE.value:
            table, key, object_type = (
                "groundloop_m5_published_claim_state",
                "claim_id",
                "claim",
            )
        else:
            table, key, object_type = (
                "groundloop_m5_published_answer_state",
                "answer_version_id",
                "answer",
            )
        rows = cursor.execute(
            f"SELECT status, valid_from_epoch FROM {table} "
            f"WHERE {key} = %s AND ("
            "(valid_from_epoch <= %s AND %s < valid_to_epoch) OR "
            "(valid_from_epoch = %s AND sealed_revision = %s)) "
            "ORDER BY valid_from_epoch",
            (
                object_id,
                envelope.previous_epoch_id,
                envelope.previous_epoch_id,
                envelope.epoch_id,
                envelope.revision,
            ),
        ).fetchall()
        before = [row for row in rows if int(row[1]) != envelope.epoch_id]
        after = [row for row in rows if int(row[1]) == envelope.epoch_id]
        if len(before) != 1 or len(after) != 1:
            raise ValidationError(
                "changed status lacks exact before/after publication rows"
            )
        old_status, new_status = str(before[0][0]), str(after[0][0])
        if old_status != new_status:
            deltas.append(
                StatusDelta(
                    event_id=envelope.event_id,
                    object_type=object_type,
                    object_id=object_id,
                    old_status=old_status,
                    new_status=new_status,
                    reason=f"typed-event={envelope.event_id}",
                )
            )
    return tuple(sorted(deltas, key=lambda value: (value.object_type, value.object_id)))


def build_matching_publication_children(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    sealed_revision: int,
) -> M5MatchingPublicationChildren:
    """Derive complete net deltas and D22/D26 references from durable rows.

    Call this after semantic/certificate promotion and both heads/epoch headers
    have reached their final sealed coordinates, but before inserting the
    immutable event-result children.  It performs no writes.
    """

    _require_nonnegative_int("epoch_id", epoch_id, positive=True)
    _require_nonnegative_int("sealed_revision", sealed_revision, positive=True)
    envelope = _load_seal_envelope(
        cursor, epoch_id=epoch_id, sealed_revision=sealed_revision
    )
    logical_changes = _load_logical_changes(cursor, epoch_id=epoch_id)
    net = _net_logical_changes(logical_changes)
    absent: dict[tuple[str, str], tuple[str | None, tuple[_LogicalChange, ...]]] = {}
    references: list[M5ChangedStateReference] = []
    for (kind, object_id), (before_hash, after_hash, history) in sorted(net.items()):
        reference_kind = _REFERENCE_KIND_BY_WIRE[kind]
        if after_hash is None:
            if kind not in _D26_KINDS or before_hash is None:
                raise ValidationError(
                    "nonqualifying logical absence cannot be published"
                )
            absent[(kind, object_id)] = (before_hash, history)
            state_artifact_hash = digests.changed_state_absence_artifact_digest(
                reference_kind, object_id
            )
        else:
            state_artifact_hash = _present_artifact_hash(
                cursor,
                kind=kind,
                object_id=object_id,
                epoch_id=epoch_id,
                revision=sealed_revision,
            )
            if state_artifact_hash != after_hash:
                raise ValidationError(
                    "published state disagrees with D25 logical after hash"
                )
        references.append(
            M5ChangedStateReference.build(
                kind=reference_kind,
                object_id=object_id,
                epoch_id=epoch_id,
                revision=sealed_revision,
                state_artifact_hash=state_artifact_hash,
            )
        )
    if absent:
        _validate_d26_absence_set(cursor, envelope=envelope, absent=absent)
    combined_deltas = _build_combined_deltas(
        cursor, envelope=envelope, changed_keys=set(net)
    )
    references_tuple = tuple(
        sorted(
            references,
            key=lambda value: (
                value.kind.value,
                value.object_id,
                value.reference_digest,
            ),
        )
    )
    return M5MatchingPublicationChildren(
        combined_deltas=combined_deltas,
        changed_state_references=references_tuple,
    )


__all__ = [
    "M5MatchingPromotionReceipt",
    "M5MatchingPublicationChildren",
    "build_matching_publication_children",
    "install_matching_activation_projection",
    "promote_matching_overlay",
]
