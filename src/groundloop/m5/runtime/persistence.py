"""Failure-atomic PostgreSQL persistence for the typed M5 runtime.

Migration 015 owns runtime coordination rows while migration 014 owns group
structure and semantic state.  This module is the only M5 runtime layer that
opens database transactions spanning both surfaces.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from psycopg import Connection, Cursor, sql

from groundloop.domain import StatusDelta, SubjectKind
from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.events import (
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
)
from groundloop.m4.application import OpenEventReceipt, PublicationReceipt
from groundloop.m5.domain import EvidenceGroupVersion
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    ReplaceGroupEvent,
    RetireGroupEvent,
)
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    M5CandidatePolicyManifest,
    M5ChangedStateReference,
    M5EventRunResult,
    M5ReplayedOutcome,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5StateReferenceKind,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
)

RuntimeFailureInjector = Callable[[str], None]

_NORMALIZER_ID = "m5-normalize-text-v1"
_NORMALIZER_PROVENANCE_HASH = (
    "d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb"
)
_WORK_COUNTER_COLUMNS = M5RuntimeWork.counter_names()


@dataclass(frozen=True, slots=True)
class _PublishedGroupOwner:
    group_family_id: str
    claim_id: str


def _inject(injector: RuntimeFailureInjector | None, point: str) -> None:
    if injector is not None:
        injector(point)


def _require_fresh_version_identifiers(
    cursor: Cursor[Any], group: EvidenceGroupVersion
) -> None:
    group_collision = cursor.execute(
        """
        SELECT 1
        FROM groundloop_m5_group_version
        WHERE group_version_id = %s
        """,
        (group.group_version_id,),
    ).fetchone()
    if group_collision is not None:
        raise InvalidEventError("group version identifier is already durable")

    requirement_ids = tuple(
        requirement.requirement_version_id for requirement in group.requirements
    )
    requirement_collision = cursor.execute(
        """
        SELECT requirement_version_id
        FROM groundloop_m5_requirement_version
        WHERE requirement_version_id = ANY(%s)
        ORDER BY requirement_version_id COLLATE "C"
        LIMIT 1
        """,
        (list(requirement_ids),),
    ).fetchone()
    if requirement_collision is not None:
        raise InvalidEventError("requirement version identifier is already durable")


def _lock_active_published_group(
    cursor: Cursor[Any], group_version_id: str
) -> _PublishedGroupOwner:
    row = cursor.execute(
        """
        SELECT group_version.group_family_id, family.claim_id
        FROM groundloop_m5_group_version AS group_version
        JOIN groundloop_m5_group_family AS family
          ON family.group_family_id = group_version.group_family_id
        JOIN groundloop_m5_group_validity AS validity
          ON validity.group_version_id = group_version.group_version_id
        WHERE group_version.group_version_id = %s
          AND group_version.lifecycle_state = 'PUBLISHED'
          AND family.lifecycle_state = 'PUBLISHED'
          AND validity.valid_to_epoch IS NULL
        FOR UPDATE OF group_version, family, validity
        """,
        (group_version_id,),
    ).fetchone()
    if row is None:
        raise InvalidEventError(
            "group lifecycle event requires an active published group"
        )
    return _PublishedGroupOwner(str(row[0]), str(row[1]))


def _validate_structure_declaration(
    cursor: Cursor[Any],
    event: RegisterGroupEvent
    | ReplaceGroupEvent
    | RetireGroupEvent
    | ObserveRequirementEvent,
) -> None:
    """Validate lifecycle ownership before the epoch/event rows are inserted."""

    if isinstance(event, RegisterGroupEvent):
        family_collision = cursor.execute(
            """
            SELECT 1 FROM groundloop_m5_group_family
            WHERE group_family_id = %s
            """,
            (event.group.group_family_id,),
        ).fetchone()
        if family_collision is not None:
            raise InvalidEventError("group registration requires a new family")
        if event.group.supersedes_group_version_id is not None:
            raise InvalidEventError("initial group version cannot name a predecessor")
        _require_fresh_version_identifiers(cursor, event.group)
        return

    if isinstance(event, ReplaceGroupEvent):
        owner = _lock_active_published_group(cursor, event.old_group_version_id)
        successor = event.successor
        if successor.supersedes_group_version_id != event.old_group_version_id:
            raise InvalidEventError("group replacement must name its exact predecessor")
        if successor.group_family_id != owner.group_family_id:
            raise InvalidEventError("group replacement cannot change family")
        if successor.owner_claim_id != owner.claim_id:
            raise InvalidEventError("group replacement cannot change owner claim")
        retired = cursor.execute(
            """
            SELECT 1 FROM groundloop_m5_group_family_retirement
            WHERE group_family_id = %s
            """,
            (owner.group_family_id,),
        ).fetchone()
        if retired is not None:
            raise InvalidEventError("a retired group family cannot be replaced")
        _require_fresh_version_identifiers(cursor, successor)
        return

    if isinstance(event, RetireGroupEvent):
        _lock_active_published_group(cursor, event.group_version_id)
        return

    observation = event.observation
    if observation.subject_kind is not SubjectKind.REQUIREMENT:
        raise InvalidEventError(
            "observe-requirement event requires a requirement subject"
        )
    subject = cursor.execute(
        """
        SELECT 1
        FROM groundloop_m5_requirement_version AS requirement
        JOIN groundloop_m5_group_version AS group_version
          ON group_version.group_version_id = requirement.group_version_id
        JOIN groundloop_m5_group_validity AS validity
          ON validity.group_version_id = group_version.group_version_id
        JOIN groundloop_chunk_version AS chunk
          ON chunk.chunk_version_id = %s
        WHERE requirement.requirement_version_id = %s
          AND requirement.lifecycle_state = 'PUBLISHED'
          AND group_version.lifecycle_state = 'PUBLISHED'
          AND validity.valid_to_epoch IS NULL
          AND chunk.valid_to_epoch IS NULL
        FOR UPDATE OF requirement, group_version, validity, chunk
        """,
        (observation.chunk_version_id, observation.subject_id),
    ).fetchone()
    if subject is None:
        raise InvalidEventError(
            "observe-requirement event requires active subject, group, and chunk"
        )


def _insert_staged_group_version(
    cursor: Cursor[Any],
    *,
    group: EvidenceGroupVersion,
    epoch_id: int,
) -> None:
    cursor.execute(
        """
        INSERT INTO groundloop_m5_group_version (
            group_version_id, group_family_id, creator_epoch_id,
            lifecycle_state, group_type, construction_kind,
            construction_source_id, constructor_model_id,
            constructor_model_version, constructor_prompt_version,
            supersedes_group_version_id, semantic_structure_hash,
            record_payload_hash
        ) VALUES (
            %s, %s, %s, 'STAGED', %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            group.group_version_id,
            group.group_family_id,
            epoch_id,
            group.group_type.value,
            group.construction_kind.value,
            group.construction_source_id,
            group.constructor_model_id,
            group.constructor_model_version,
            group.constructor_prompt_version,
            group.supersedes_group_version_id,
            group.semantic_structure_hash,
            group.record_payload_hash,
        ),
    )
    for requirement in group.requirements:
        cursor.execute(
            """
            INSERT INTO groundloop_m5_requirement_version (
                requirement_version_id, group_version_id, creator_epoch_id,
                lifecycle_state, ordinal, requirement_text,
                requirement_text_hash, constructor_model_id,
                constructor_model_version, constructor_prompt_version,
                supersedes_requirement_version_id
            ) VALUES (
                %s, %s, %s, 'STAGED', %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                requirement.requirement_version_id,
                requirement.group_version_id,
                epoch_id,
                requirement.ordinal,
                requirement.requirement_text,
                requirement.requirement_text_hash,
                requirement.constructor_model_id,
                requirement.constructor_model_version,
                requirement.constructor_prompt_version,
                requirement.supersedes_requirement_version_id,
            ),
        )


def _stage_structure(
    cursor: Cursor[Any],
    *,
    event: RegisterGroupEvent
    | ReplaceGroupEvent
    | RetireGroupEvent
    | ObserveRequirementEvent,
    epoch_id: int,
    failure_injector: RuntimeFailureInjector | None,
) -> None:
    if isinstance(event, RegisterGroupEvent):
        cursor.execute(
            """
            INSERT INTO groundloop_m5_group_family (
                group_family_id, claim_id, creator_epoch_id, lifecycle_state
            ) VALUES (%s, %s, %s, 'STAGED')
            """,
            (event.group.group_family_id, event.group.owner_claim_id, epoch_id),
        )
        _inject(failure_injector, "typed_open_family_staged")
        _insert_staged_group_version(cursor, group=event.group, epoch_id=epoch_id)
        _inject(failure_injector, "typed_open_group_staged")
        return

    if isinstance(event, ReplaceGroupEvent):
        _insert_staged_group_version(cursor, group=event.successor, epoch_id=epoch_id)
        _inject(failure_injector, "typed_open_group_staged")
        cursor.execute(
            """
            INSERT INTO groundloop_m5_group_deactivation (
                epoch_id, group_version_id, action,
                successor_group_version_id, event_id
            ) VALUES (%s, %s, 'REPLACE', %s, %s)
            """,
            (
                epoch_id,
                event.old_group_version_id,
                event.successor.group_version_id,
                event.event_id,
            ),
        )
        _inject(failure_injector, "typed_open_deactivation_staged")
        return

    if isinstance(event, RetireGroupEvent):
        cursor.execute(
            """
            INSERT INTO groundloop_m5_group_deactivation (
                epoch_id, group_version_id, action,
                successor_group_version_id, event_id
            ) VALUES (%s, %s, 'RETIRE', NULL, %s)
            """,
            (epoch_id, event.group_version_id, event.event_id),
        )
        _inject(failure_injector, "typed_open_deactivation_staged")


def _mark_event_staged_structure_failed(cursor: Cursor[Any], epoch_id: int) -> None:
    """Retain staged audit rows while making them terminally non-publishable."""

    cursor.execute(
        """
        UPDATE groundloop_m5_requirement_version
        SET lifecycle_state = 'FAILED'
        WHERE creator_epoch_id = %s AND lifecycle_state = 'STAGED'
        """,
        (epoch_id,),
    )
    cursor.execute(
        """
        UPDATE groundloop_m5_group_version
        SET lifecycle_state = 'FAILED'
        WHERE creator_epoch_id = %s AND lifecycle_state = 'STAGED'
        """,
        (epoch_id,),
    )
    cursor.execute(
        """
        UPDATE groundloop_m5_group_family
        SET lifecycle_state = 'FAILED'
        WHERE creator_epoch_id = %s AND lifecycle_state = 'STAGED'
        """,
        (epoch_id,),
    )


def _m5_update_kind(
    event: InsertDocumentEvent
    | DeleteDocumentVersionEvent
    | ReplaceDocumentVersionEvent
    | PolicyChangeEvent
    | ObserveEvent
    | RegisterGroupEvent
    | ReplaceGroupEvent
    | RetireGroupEvent
    | ObserveRequirementEvent,
) -> str:
    if isinstance(event, InsertDocumentEvent):
        return "document_insert"
    if isinstance(event, DeleteDocumentVersionEvent):
        return "document_delete"
    if isinstance(event, ReplaceDocumentVersionEvent):
        return "document_replace"
    if isinstance(event, PolicyChangeEvent):
        return "policy_change"
    if isinstance(event, ObserveEvent):
        raise InvalidEventError(
            "legacy claim-observation events are not M5 typed structural events"
        )
    if isinstance(event, RegisterGroupEvent):
        return "register_group"
    if isinstance(event, ReplaceGroupEvent):
        return "replace_group"
    if isinstance(event, RetireGroupEvent):
        return "retire_group"
    return "observe_requirement"


def _requirement_snapshot_rows(
    snapshot: RequirementRegistrySnapshot,
) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            ordinal,
            entry.requirement_version_id,
            entry.group_version_id,
            entry.group_family_id,
            entry.owner_claim_id,
            entry.normalized_requirement_text,
            entry.requirement_text_hash,
        )
        for ordinal, entry in enumerate(snapshot.entries)
    )


def _chunk_snapshot_rows(
    snapshot: ActiveChunkSnapshot,
) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (ordinal, entry.chunk_version_id, entry.text_hash)
        for ordinal, entry in enumerate(snapshot.entries)
    )


def _validate_effective_snapshots(
    cursor: Cursor[Any], *, plan: M5TypedEventPlan, epoch_id: int
) -> None:
    requirement_rows = tuple(
        tuple(row)
        for row in cursor.execute(
            """
            WITH effective_requirement AS (
                SELECT requirement.requirement_version_id,
                       requirement.group_version_id,
                       group_version.group_family_id,
                       family.claim_id AS owner_claim_id,
                       requirement.requirement_text,
                       requirement.requirement_text_hash
                FROM groundloop_m5_requirement_version AS requirement
                JOIN groundloop_m5_group_version AS group_version
                  ON group_version.group_version_id = requirement.group_version_id
                JOIN groundloop_m5_group_family AS family
                  ON family.group_family_id = group_version.group_family_id
                JOIN groundloop_m5_group_validity AS validity
                  ON validity.group_version_id = group_version.group_version_id
                WHERE requirement.lifecycle_state = 'PUBLISHED'
                  AND group_version.lifecycle_state = 'PUBLISHED'
                  AND family.lifecycle_state = 'PUBLISHED'
                  AND validity.valid_to_epoch IS NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM groundloop_m5_group_deactivation AS deactivation
                      WHERE deactivation.epoch_id = %s
                        AND deactivation.group_version_id =
                            group_version.group_version_id
                  )
                UNION ALL
                SELECT requirement.requirement_version_id,
                       requirement.group_version_id,
                       group_version.group_family_id,
                       family.claim_id AS owner_claim_id,
                       requirement.requirement_text,
                       requirement.requirement_text_hash
                FROM groundloop_m5_requirement_version AS requirement
                JOIN groundloop_m5_group_version AS group_version
                  ON group_version.group_version_id = requirement.group_version_id
                JOIN groundloop_m5_group_family AS family
                  ON family.group_family_id = group_version.group_family_id
                WHERE requirement.creator_epoch_id = %s
                  AND requirement.lifecycle_state = 'STAGED'
                  AND group_version.creator_epoch_id = %s
                  AND group_version.lifecycle_state = 'STAGED'
            )
            SELECT requirement_version_id, group_version_id, group_family_id,
                   owner_claim_id, requirement_text, requirement_text_hash
            FROM effective_requirement
            ORDER BY requirement_version_id COLLATE "C"
            """,
            (epoch_id, epoch_id, epoch_id),
        ).fetchall()
    )
    planned_requirement_rows = tuple(
        (
            entry.requirement_version_id,
            entry.group_version_id,
            entry.group_family_id,
            entry.owner_claim_id,
            entry.normalized_requirement_text,
            entry.requirement_text_hash,
        )
        for entry in plan.requirement_registry_snapshot.entries
    )
    if requirement_rows != planned_requirement_rows:
        raise InvalidEventError(
            "typed requirement snapshot is not the effective structural snapshot"
        )

    chunk_rows = tuple(
        (str(row[0]), str(row[1]))
        for row in cursor.execute(
            """
            SELECT chunk_version_id,
                   encode(
                       digest(
                           convert_to(
                               groundloop_normalize_text_v1(text), 'UTF8'
                           ),
                           'sha256'
                       ),
                       'hex'
                   ) AS normalized_text_hash
            FROM groundloop_chunk_version
            WHERE valid_to_epoch IS NULL
            ORDER BY chunk_version_id COLLATE "C"
            """
        ).fetchall()
    )
    planned_chunk_rows = tuple(
        (entry.chunk_version_id, entry.text_hash)
        for entry in plan.active_chunk_snapshot.entries
    )
    if chunk_rows != planned_chunk_rows:
        raise InvalidEventError(
            "typed chunk snapshot is not the effective active-chunk snapshot"
        )


def _persist_requirement_snapshot(
    cursor: Cursor[Any], *, snapshot: RequirementRegistrySnapshot, epoch_id: int
) -> None:
    inserted = cursor.execute(
        """
        INSERT INTO groundloop_m5_requirement_registry_snapshot (
            requirement_registry_snapshot_digest, requirement_count,
            created_epoch_id
        ) VALUES (%s, %s, %s)
        ON CONFLICT (requirement_registry_snapshot_digest) DO NOTHING
        """,
        (
            snapshot.requirement_registry_snapshot_digest,
            snapshot.requirement_count,
            epoch_id,
        ),
    ).rowcount
    if inserted == 1:
        for row in _requirement_snapshot_rows(snapshot):
            cursor.execute(
                """
                INSERT INTO groundloop_m5_requirement_registry_snapshot_member (
                    requirement_registry_snapshot_digest, member_ordinal,
                    requirement_version_id, group_version_id, group_family_id,
                    owner_claim_id, normalized_requirement_text,
                    requirement_text_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (snapshot.requirement_registry_snapshot_digest, *row),
            )
        return

    header = cursor.execute(
        """
        SELECT requirement_count
        FROM groundloop_m5_requirement_registry_snapshot
        WHERE requirement_registry_snapshot_digest = %s
        """,
        (snapshot.requirement_registry_snapshot_digest,),
    ).fetchone()
    members = tuple(
        tuple(row)
        for row in cursor.execute(
            """
            SELECT member_ordinal, requirement_version_id, group_version_id,
                   group_family_id, owner_claim_id,
                   normalized_requirement_text, requirement_text_hash
            FROM groundloop_m5_requirement_registry_snapshot_member
            WHERE requirement_registry_snapshot_digest = %s
            ORDER BY member_ordinal
            """,
            (snapshot.requirement_registry_snapshot_digest,),
        ).fetchall()
    )
    if header != (snapshot.requirement_count,) or members != (
        _requirement_snapshot_rows(snapshot)
    ):
        raise EventConflictError("requirement snapshot digest has conflicting rows")


def _persist_active_chunk_snapshot(
    cursor: Cursor[Any], *, snapshot: ActiveChunkSnapshot, epoch_id: int
) -> None:
    inserted = cursor.execute(
        """
        INSERT INTO groundloop_m5_active_chunk_snapshot (
            active_chunk_snapshot_digest, chunk_count, created_epoch_id,
            normalizer_id, normalizer_provenance_hash
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (active_chunk_snapshot_digest) DO NOTHING
        """,
        (
            snapshot.active_chunk_snapshot_digest,
            snapshot.chunk_count,
            epoch_id,
            _NORMALIZER_ID,
            _NORMALIZER_PROVENANCE_HASH,
        ),
    ).rowcount
    if inserted == 1:
        for row in _chunk_snapshot_rows(snapshot):
            cursor.execute(
                """
                INSERT INTO groundloop_m5_active_chunk_snapshot_member (
                    active_chunk_snapshot_digest, member_ordinal,
                    chunk_version_id, text_hash
                ) VALUES (%s, %s, %s, %s)
                """,
                (snapshot.active_chunk_snapshot_digest, *row),
            )
        return

    header = cursor.execute(
        """
        SELECT chunk_count, normalizer_id, normalizer_provenance_hash
        FROM groundloop_m5_active_chunk_snapshot
        WHERE active_chunk_snapshot_digest = %s
        """,
        (snapshot.active_chunk_snapshot_digest,),
    ).fetchone()
    members = tuple(
        tuple(row)
        for row in cursor.execute(
            """
            SELECT member_ordinal, chunk_version_id, text_hash
            FROM groundloop_m5_active_chunk_snapshot_member
            WHERE active_chunk_snapshot_digest = %s
            ORDER BY member_ordinal
            """,
            (snapshot.active_chunk_snapshot_digest,),
        ).fetchall()
    )
    if header != (
        snapshot.chunk_count,
        _NORMALIZER_ID,
        _NORMALIZER_PROVENANCE_HASH,
    ) or members != _chunk_snapshot_rows(snapshot):
        raise EventConflictError("active-chunk snapshot digest has conflicting rows")


def _persist_zero_pending_counters(
    cursor: Cursor[Any], *, snapshot: RequirementRegistrySnapshot, epoch_id: int
) -> None:
    owner_claim_ids = tuple(
        sorted({entry.owner_claim_id for entry in snapshot.entries})
    )
    for owner_claim_id in owner_claim_ids:
        cursor.execute(
            """
            INSERT INTO groundloop_m5_owner_pending_counter (
                epoch_id, owner_claim_id, broad_reverse_scope_count,
                forward_scope_count, verifier_job_count,
                blocking_failure_count, updated_revision
            ) VALUES (%s, %s, 0, 0, 0, 0, 1)
            """,
            (epoch_id, owner_claim_id),
        )

    if not owner_claim_ids:
        return
    answer_rows = cursor.execute(
        """
        SELECT DISTINCT answer_version_id
        FROM groundloop_claim
        WHERE claim_id = ANY(%s) AND required
        ORDER BY answer_version_id COLLATE "C"
        """,
        (list(owner_claim_ids),),
    ).fetchall()
    for answer_row in answer_rows:
        cursor.execute(
            """
            INSERT INTO groundloop_m5_answer_pending_counter (
                epoch_id, answer_version_id, broad_reverse_scope_count,
                forward_scope_count, verifier_job_count,
                blocking_failure_count, updated_revision
            ) VALUES (%s, %s, 0, 0, 0, 0, 1)
            """,
            (epoch_id, str(answer_row[0])),
        )


def _insert_runtime_work(
    cursor: Cursor[Any],
    *,
    structural_event_id: str,
    epoch_id: int,
    work_kind: str,
    work: M5RuntimeWork,
) -> None:
    columns = (
        "work_digest",
        "structural_event_id",
        "epoch_id",
        "work_kind",
        *_WORK_COUNTER_COLUMNS,
    )
    statement = sql.SQL(
        "INSERT INTO groundloop_m5_runtime_work ({}) VALUES ({})"
    ).format(
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    cursor.execute(
        statement,
        (
            work.work_digest,
            structural_event_id,
            epoch_id,
            work_kind,
            *work.counter_values(),
        ),
    )


def _load_runtime_work(
    cursor: Cursor[Any], *, structural_event_id: str, work_kind: str
) -> M5RuntimeWork | None:
    selected_columns = ("work_digest", *_WORK_COUNTER_COLUMNS)
    statement = sql.SQL(
        "SELECT {} FROM groundloop_m5_runtime_work "
        "WHERE structural_event_id = %s AND work_kind = %s"
    ).format(sql.SQL(", ").join(sql.Identifier(column) for column in selected_columns))
    row = cursor.execute(statement, (structural_event_id, work_kind)).fetchone()
    if row is None:
        return None
    values = {
        name: int(value)
        for name, value in zip(_WORK_COUNTER_COLUMNS, row[1:], strict=True)
    }
    return M5RuntimeWork(**values, work_digest=str(row[0]).strip())


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _load_terminal_result(
    cursor: Cursor[Any], *, structural_event_id: str, payload_hash: str
) -> M5EventRunResult | None:
    row = cursor.execute(
        """
        SELECT base.epoch_id, base.payload_hash,
               runtime.structural_event_id,
               result.outcome,
               result.original_open_receipt_binding_hash,
               result.publication_id,
               result.original_publication_receipt_binding_hash,
               result.event_work_digest,
               result.combined_status_delta_set_hash,
               result.changed_state_set_hash,
               result.failure_reason,
               result.logical_result_hash,
               result.delta_count,
               result.state_reference_count,
               result.coordinator_non_db_non_neural_ns,
               result.neural_wall_ns,
               result.postgres_roundtrip_wall_ns,
               result.external_io_wall_ns,
               result.end_to_end_wall_ns,
               result.postgres_server_execution_ns,
               result.postgres_lock_wait_ns,
               result.postgres_wal_bytes,
               result.postgres_shared_block_reads
        FROM groundloop_epoch AS base
        LEFT JOIN groundloop_m5_runtime_epoch AS runtime
          ON runtime.epoch_id = base.epoch_id
        LEFT JOIN groundloop_m5_event_result AS result
          ON result.epoch_id = base.epoch_id
        WHERE base.event_id = %s
        """,
        (structural_event_id,),
    ).fetchone()
    if row is None:
        return None
    if str(row[1]).strip() != payload_hash:
        raise EventConflictError(
            "typed structural event ID was reused with another payload"
        )
    if row[2] is None:
        raise EventConflictError("event ID belongs to a non-typed declaration")
    if str(row[2]) != structural_event_id:
        raise ValidationError("typed runtime event binding is corrupt")
    if row[3] is None:
        return None

    epoch_id = int(row[0])
    outcome = M5ReplayedOutcome(str(row[3]))
    failure_reason = None if row[10] is None else M5RunFailureReason(str(row[10]))
    event_work = _load_runtime_work(
        cursor,
        structural_event_id=structural_event_id,
        work_kind="event",
    )
    original_call_work = _load_runtime_work(
        cursor,
        structural_event_id=structural_event_id,
        work_kind="call",
    )
    if event_work is None or original_call_work is None:
        raise ValidationError("terminal M5 result lacks its durable work rows")

    delta_rows = cursor.execute(
        """
        SELECT object_type, object_id, old_status, new_status, reason
        FROM groundloop_m5_event_result_delta
        WHERE structural_event_id = %s
        ORDER BY delta_ordinal
        """,
        (structural_event_id,),
    ).fetchall()
    combined_deltas = tuple(
        StatusDelta(
            event_id=structural_event_id,
            object_type=str(delta[0]),
            object_id=str(delta[1]),
            old_status=str(delta[2]),
            new_status=str(delta[3]),
            reason=str(delta[4]),
        )
        for delta in delta_rows
    )
    reference_rows = cursor.execute(
        """
        SELECT kind, object_id, epoch_id, revision,
               state_artifact_hash, reference_digest
        FROM groundloop_m5_event_result_state_reference
        WHERE structural_event_id = %s
        ORDER BY reference_ordinal
        """,
        (structural_event_id,),
    ).fetchall()
    changed_references = tuple(
        M5ChangedStateReference(
            kind=M5StateReferenceKind(str(reference[0])),
            object_id=str(reference[1]),
            epoch_id=int(reference[2]),
            revision=int(reference[3]),
            state_artifact_hash=str(reference[4]).strip(),
            reference_digest=str(reference[5]).strip(),
        )
        for reference in reference_rows
    )
    if len(combined_deltas) != int(row[12]) or len(changed_references) != int(row[13]):
        raise ValidationError("terminal M5 result child cardinality is corrupt")
    if event_work.work_digest != str(row[7]).strip():
        raise ValidationError("terminal M5 result binds different event work")
    if digests.combined_status_delta_set_digest(combined_deltas) != str(row[8]).strip():
        raise ValidationError("terminal M5 result delta-set digest is corrupt")
    if (
        digests.changed_state_set_digest(
            reference.reference_digest for reference in changed_references
        )
        != str(row[9]).strip()
    ):
        raise ValidationError("terminal M5 result state-set digest is corrupt")

    expected_open_binding = digests.open_event_receipt_binding_digest(
        epoch_id=epoch_id,
        replayed=False,
        already_sealed=False,
        publication_id=None,
        already_failed=False,
        failure_reason=None,
    )
    if expected_open_binding != str(row[4]).strip():
        raise ValidationError("terminal M5 result open-receipt binding is corrupt")

    publication_receipt: PublicationReceipt | None = None
    if outcome is M5ReplayedOutcome.SEALED:
        if row[5] is None or row[6] is None or failure_reason is not None:
            raise ValidationError("sealed M5 result has an invalid durable shape")
        publication_id = str(row[5])
        expected_publication_binding = digests.publication_receipt_binding_digest(
            epoch_id=epoch_id,
            publication_id=publication_id,
            replayed=False,
        )
        if expected_publication_binding != str(row[6]).strip():
            raise ValidationError(
                "terminal M5 result publication-receipt binding is corrupt"
            )
        open_receipt = OpenEventReceipt(
            epoch_id=epoch_id,
            replayed=True,
            already_sealed=True,
            publication_id=publication_id,
        )
        publication_receipt = PublicationReceipt(
            epoch_id=epoch_id,
            publication_id=publication_id,
            replayed=True,
        )
    else:
        if row[5] is not None or row[6] is not None or failure_reason is None:
            raise ValidationError("failed M5 result has an invalid durable shape")
        open_receipt = OpenEventReceipt(
            epoch_id=epoch_id,
            replayed=True,
            already_sealed=False,
            already_failed=True,
            failure_reason=failure_reason.value,
        )

    event_timing = M5RuntimeTiming(
        coordinator_non_db_non_neural_ns=int(row[14]),
        neural_wall_ns=int(row[15]),
        postgres_roundtrip_wall_ns=int(row[16]),
        external_io_wall_ns=int(row[17]),
        end_to_end_wall_ns=int(row[18]),
        postgres_server_execution_ns=_optional_int(row[19]),
        postgres_lock_wait_ns=_optional_int(row[20]),
        postgres_wal_bytes=_optional_int(row[21]),
        postgres_shared_block_reads=_optional_int(row[22]),
    )
    result = M5EventRunResult.build(
        event_id=structural_event_id,
        payload_hash=payload_hash,
        epoch_id=epoch_id,
        state=M5RunState.REPLAYED,
        replayed_outcome=outcome,
        open_receipt=open_receipt,
        publication_receipt=publication_receipt,
        event_work=event_work,
        call_work=M5RuntimeWork(),
        event_timing=event_timing,
        call_timing=M5RuntimeTiming(),
        combined_deltas=combined_deltas,
        changed_state_references=changed_references,
        failure_reason=failure_reason,
    )
    if result.logical_result_hash != str(row[11]).strip():
        raise ValidationError("terminal M5 logical-result hash is corrupt")
    return result


class PostgresM5RuntimeStore:
    """Checked typed-runtime transactions over the migration-014/015 boundary."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def register_candidate_policy(self, manifest: M5CandidatePolicyManifest) -> None:
        expected = (
            manifest.candidate_policy_id,
            manifest.manifest_hash,
            manifest.embedding_model_artifact_id,
            manifest.requirement_role_template_hash,
            manifest.chunk_role_template_hash,
            manifest.vector_method_version,
            manifest.vector_index_kind.value,
            manifest.vector_index_build_config_hash,
            manifest.vector_search_config_hash,
            manifest.lexical_method_version,
            manifest.lexical_config_hash,
            manifest.lexical_postgres_version,
            manifest.lexical_regconfig_identity,
            manifest.fusion_version,
            manifest.reverse_budget_per_inserted_chunk,
            manifest.forward_budget_per_requirement,
            manifest.verifier_execution_spec_hash,
            manifest.decision_policy_version,
            manifest.lineage_safety_override,
        )
        with self._connection.transaction(), self._connection.cursor() as cursor:
            inserted = cursor.execute(
                """
                INSERT INTO groundloop_m5_candidate_policy (
                    candidate_policy_id, candidate_policy_manifest_hash,
                    embedding_model_artifact_id, requirement_role_template_hash,
                    chunk_role_template_hash, vector_method_version,
                    vector_index_kind, vector_index_build_config_hash,
                    vector_search_config_hash, lexical_method_version,
                    lexical_config_hash, lexical_postgres_version,
                    lexical_regconfig_identity, fusion_version,
                    reverse_budget_per_inserted_chunk,
                    forward_budget_per_requirement,
                    verifier_execution_spec_hash, decision_policy_version,
                    lineage_safety_override
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (candidate_policy_id) DO NOTHING
                """,
                expected,
            ).rowcount
            if inserted == 1:
                return
            actual = cursor.execute(
                """
                SELECT candidate_policy_id, candidate_policy_manifest_hash,
                       embedding_model_artifact_id,
                       requirement_role_template_hash,
                       chunk_role_template_hash, vector_method_version,
                       vector_index_kind, vector_index_build_config_hash,
                       vector_search_config_hash, lexical_method_version,
                       lexical_config_hash, lexical_postgres_version,
                       lexical_regconfig_identity, fusion_version,
                       reverse_budget_per_inserted_chunk,
                       forward_budget_per_requirement,
                       verifier_execution_spec_hash, decision_policy_version,
                       lineage_safety_override
                FROM groundloop_m5_candidate_policy
                WHERE candidate_policy_id = %s
                """,
                (manifest.candidate_policy_id,),
            ).fetchone()
            if actual is None or tuple(actual) != expected:
                raise EventConflictError(
                    "candidate policy identifier has conflicting immutable content"
                )

    def read_typed_event_result(
        self, structural_event_id: str, payload_hash: str
    ) -> M5EventRunResult | None:
        """Read one immutable terminal result without locks or writes."""

        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            return _load_terminal_result(
                cursor,
                structural_event_id=structural_event_id,
                payload_hash=payload_hash,
            )

    def _read_existing_open_read_only(
        self, plan: M5TypedEventPlan
    ) -> OpenEventReceipt | None:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            return self._read_existing_open(cursor, plan, for_update=False)

    def open_typed_event_atomically(
        self,
        plan: M5TypedEventPlan,
        *,
        failure_injector: RuntimeFailureInjector | None = None,
    ) -> OpenEventReceipt:
        """Open the first production failure/replay slice.

        Barrier B intentionally admits only a rootless group retirement. The
        remaining event kinds require the full direct/root declaration adapter
        and stay M5.4-PENDING rather than being partially persisted.
        """

        if not isinstance(plan.event, RetireGroupEvent):
            raise InvalidEventError(
                "the M5.3-07 production slice admits retire_group only"
            )
        if plan.direct_plan is not None:
            raise InvalidEventError("group retirement cannot carry a direct M4 plan")
        root_set_hash = digests.requirement_root_set_digest(())

        existing = self._read_existing_open_read_only(plan)
        if existing is not None:
            return existing

        with self._connection.transaction(), self._connection.cursor() as cursor:
            mode_row = cursor.execute(
                """
                SELECT mode
                FROM groundloop_runtime_mode
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            if mode_row is None or str(mode_row[0]) != "m5_active":
                raise InvalidEventError("typed M5 mutation requires activated mode")

            m4_head_row = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_m4_publication_head
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            m5_head_row = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_m5_publication_head
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            if m4_head_row is None or m5_head_row is None:
                raise InvalidEventError("typed publication heads are not initialized")
            m4_head = int(m4_head_row[0])
            m5_head = int(m5_head_row[0])
            if (
                m4_head != m5_head
                or m5_head != plan.expected_previous_published_epoch_id
            ):
                raise InvalidEventError(
                    "typed event predecessor does not equal both publication heads"
                )

            activation = cursor.execute(
                """
                SELECT activation_id
                FROM groundloop_m5_activation
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            if activation is None:
                raise InvalidEventError("typed M5 runtime lacks an activation record")

            existing = self._read_existing_open(cursor, plan, for_update=True)
            if existing is not None:
                return existing

            live_epoch = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_epoch
                WHERE structural_status = 'committed'
                  AND semantic_status IN ('pending', 'complete')
                ORDER BY epoch_id
                FOR UPDATE
                LIMIT 1
                """
            ).fetchone()
            if live_epoch is not None:
                raise InvalidEventError("a structural epoch is already active")

            policy_row = cursor.execute(
                """
                SELECT candidate_policy_manifest_hash, decision_policy_version
                FROM groundloop_m5_candidate_policy
                WHERE candidate_policy_id = %s
                """,
                (plan.candidate_policy_id,),
            ).fetchone()
            if policy_row is None or str(policy_row[0]).strip() != (
                plan.candidate_policy_manifest_hash
            ):
                raise InvalidEventError(
                    "typed plan does not bind a registered candidate policy"
                )
            decision_policy_version = str(policy_row[1])

            epoch_row = cursor.execute(
                """
                INSERT INTO groundloop_epoch (
                    event_id, payload_hash, revision, structural_status,
                    semantic_status, evaluation_state, publication_mode,
                    sealed_at
                ) VALUES (
                    %s, %s, 1, 'committed', 'pending', 'pending',
                    'provisional', NULL
                )
                RETURNING epoch_id
                """,
                (plan.structural_event_id, plan.payload_hash),
            ).fetchone()
            assert epoch_row is not None
            epoch_id = int(epoch_row[0])
            _inject(failure_injector, "typed_open_epoch_inserted")

            cursor.execute(
                """
                INSERT INTO groundloop_m5_update (
                    epoch_id, update_kind, previous_published_epoch_id,
                    decision_policy_version, manifest
                ) VALUES (%s, %s, %s, %s, '{}'::jsonb)
                """,
                (
                    epoch_id,
                    _m5_update_kind(plan.event),
                    plan.expected_previous_published_epoch_id,
                    decision_policy_version,
                ),
            )
            _inject(failure_injector, "typed_open_update_inserted")

            cursor.execute(
                """
                INSERT INTO groundloop_m5_runtime_epoch (
                    epoch_id, structural_event_id, candidate_policy_id,
                    candidate_policy_manifest_hash,
                    requirement_registry_snapshot_digest,
                    active_chunk_snapshot_digest,
                    expected_previous_published_epoch_id,
                    requirement_root_set_hash, runtime_state, revision,
                    open_work_count, open_scope_count,
                    blocking_failure_count, terminal_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    'structural_committed', 1, 0, 0, 0, NULL
                )
                """,
                (
                    epoch_id,
                    plan.structural_event_id,
                    plan.candidate_policy_id,
                    plan.candidate_policy_manifest_hash,
                    plan.requirement_registry_snapshot.requirement_registry_snapshot_digest,
                    plan.active_chunk_snapshot.active_chunk_snapshot_digest,
                    plan.expected_previous_published_epoch_id,
                    root_set_hash,
                ),
            )
            _inject(failure_injector, "typed_open_runtime_header_inserted")

            _validate_structure_declaration(cursor, plan.event)
            _stage_structure(
                cursor,
                event=plan.event,
                epoch_id=epoch_id,
                failure_injector=failure_injector,
            )
            _validate_effective_snapshots(cursor, plan=plan, epoch_id=epoch_id)
            _persist_requirement_snapshot(
                cursor,
                snapshot=plan.requirement_registry_snapshot,
                epoch_id=epoch_id,
            )
            _persist_active_chunk_snapshot(
                cursor,
                snapshot=plan.active_chunk_snapshot,
                epoch_id=epoch_id,
            )
            _persist_zero_pending_counters(
                cursor,
                snapshot=plan.requirement_registry_snapshot,
                epoch_id=epoch_id,
            )
            _inject(failure_injector, "typed_open_snapshots_persisted")
            _inject(failure_injector, "typed_open_before_commit")
            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
            return OpenEventReceipt(
                epoch_id=epoch_id,
                replayed=False,
                already_sealed=False,
            )

    def fail_typed_epoch_atomically(
        self,
        epoch_id: int,
        *,
        expected_revision: int = 1,
        failure_reason: M5RunFailureReason = M5RunFailureReason.INVARIANT_FAILURE,
        failure_injector: RuntimeFailureInjector | None = None,
    ) -> M5EventRunResult:
        """Terminally fail the production M5.3-07 revision-1 slice."""

        if expected_revision < 1:
            raise InvalidEventError("expected runtime revision must be positive")
        if not isinstance(failure_reason, M5RunFailureReason):
            raise ValidationError("failure_reason must be an M5RunFailureReason")

        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            identity = cursor.execute(
                """
                SELECT event_id, payload_hash
                FROM groundloop_epoch
                WHERE epoch_id = %s
                """,
                (epoch_id,),
            ).fetchone()
            if identity is None:
                raise InvalidEventError("typed epoch does not exist")
            structural_event_id = str(identity[0])
            payload_hash = str(identity[1]).strip()
            terminal = _load_terminal_result(
                cursor,
                structural_event_id=structural_event_id,
                payload_hash=payload_hash,
            )
            if terminal is not None:
                return self._validate_failed_replay(terminal, failure_reason)

        with self._connection.transaction(), self._connection.cursor() as cursor:
            mode_row = cursor.execute(
                """
                SELECT mode
                FROM groundloop_runtime_mode
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            if mode_row is None or str(mode_row[0]) != "m5_active":
                raise InvalidEventError("typed M5 mutation requires activated mode")

            m4_head_row = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_m4_publication_head
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            m5_head_row = cursor.execute(
                """
                SELECT epoch_id
                FROM groundloop_m5_publication_head
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            if m4_head_row is None or m5_head_row is None:
                raise InvalidEventError("typed publication heads are not initialized")
            m4_head = int(m4_head_row[0])
            m5_head = int(m5_head_row[0])
            if m4_head != m5_head:
                raise InvalidEventError("typed publication heads have diverged")

            activation = cursor.execute(
                """
                SELECT activation_id
                FROM groundloop_m5_activation
                WHERE singleton
                FOR UPDATE
                """
            ).fetchone()
            if activation is None:
                raise InvalidEventError("typed M5 runtime lacks an activation record")

            epoch_row = cursor.execute(
                """
                SELECT base.event_id, base.payload_hash, base.revision,
                       base.structural_status, base.semantic_status,
                       base.evaluation_state, runtime.revision,
                       runtime.runtime_state,
                       runtime.expected_previous_published_epoch_id
                FROM groundloop_epoch AS base
                JOIN groundloop_m5_runtime_epoch AS runtime
                  ON runtime.epoch_id = base.epoch_id
                WHERE base.epoch_id = %s
                FOR UPDATE OF base, runtime
                """,
                (epoch_id,),
            ).fetchone()
            if epoch_row is None:
                raise InvalidEventError("epoch is not a typed M5 runtime epoch")
            structural_event_id = str(epoch_row[0])
            payload_hash = str(epoch_row[1]).strip()

            terminal = _load_terminal_result(
                cursor,
                structural_event_id=structural_event_id,
                payload_hash=payload_hash,
            )
            if terminal is not None:
                return self._validate_failed_replay(terminal, failure_reason)

            if int(epoch_row[8]) != m5_head:
                raise InvalidEventError(
                    "typed epoch predecessor no longer equals both publication heads"
                )
            if (
                int(epoch_row[2]) != expected_revision
                or int(epoch_row[6]) != expected_revision
            ):
                raise EventConflictError("stale typed runtime revision")
            if (
                str(epoch_row[3]),
                str(epoch_row[4]),
                str(epoch_row[5]),
                str(epoch_row[7]),
            ) != ("committed", "pending", "pending", "structural_committed"):
                raise InvalidEventError(
                    "M5.3-07 failure requires a revision-1 structural epoch"
                )
            if (
                cursor.execute(
                    "SELECT 1 FROM groundloop_m4_update WHERE epoch_id = %s",
                    (epoch_id,),
                ).fetchone()
                is not None
            ):
                raise InvalidEventError(
                    "M5.3-07 retire failure cannot contain direct M4 work"
                )
            runtime_children = cursor.execute(
                """
                SELECT
                    (SELECT count(*) FROM groundloop_m5_semantic_job
                     WHERE epoch_id = %s),
                    (SELECT count(*) FROM groundloop_m5_discovery_scope
                     WHERE epoch_id = %s)
                """,
                (epoch_id, epoch_id),
            ).fetchone()
            if runtime_children is None or tuple(map(int, runtime_children)) != (0, 0):
                raise InvalidEventError(
                    "M5.3-07 retire failure cannot contain requirement work"
                )

            cursor.execute(
                "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
                (epoch_id, expected_revision),
            )
            _inject(failure_injector, "typed_fail_authorized")
            _inject(failure_injector, "typed_fail_jobs_cancelled")

            cursor.execute(
                """
                UPDATE groundloop_m5_owner_pending_counter
                SET broad_reverse_scope_count = 0,
                    forward_scope_count = 0,
                    verifier_job_count = 0,
                    blocking_failure_count = 0,
                    updated_revision = %s
                WHERE epoch_id = %s
                """,
                (expected_revision + 1, epoch_id),
            )
            cursor.execute(
                """
                UPDATE groundloop_m5_answer_pending_counter
                SET broad_reverse_scope_count = 0,
                    forward_scope_count = 0,
                    verifier_job_count = 0,
                    blocking_failure_count = 0,
                    updated_revision = %s
                WHERE epoch_id = %s
                """,
                (expected_revision + 1, epoch_id),
            )
            _inject(failure_injector, "typed_fail_counters_updated")

            _mark_event_staged_structure_failed(cursor, epoch_id)
            _inject(failure_injector, "typed_fail_structure_failed")

            zero_work = M5RuntimeWork()
            _insert_runtime_work(
                cursor,
                structural_event_id=structural_event_id,
                epoch_id=epoch_id,
                work_kind="event",
                work=zero_work,
            )
            _insert_runtime_work(
                cursor,
                structural_event_id=structural_event_id,
                epoch_id=epoch_id,
                work_kind="call",
                work=zero_work,
            )
            _inject(failure_injector, "typed_fail_work_inserted")

            event_timing = M5RuntimeTiming()
            result = M5EventRunResult.build(
                event_id=structural_event_id,
                payload_hash=payload_hash,
                epoch_id=epoch_id,
                state=M5RunState.FAILED,
                replayed_outcome=None,
                open_receipt=OpenEventReceipt(
                    epoch_id=epoch_id,
                    replayed=False,
                    already_sealed=False,
                ),
                publication_receipt=None,
                event_work=zero_work,
                call_work=zero_work,
                event_timing=event_timing,
                call_timing=M5RuntimeTiming(),
                combined_deltas=(),
                changed_state_references=(),
                failure_reason=failure_reason,
            )
            assert result.logical_result_hash is not None
            cursor.execute(
                """
                INSERT INTO groundloop_m5_event_result (
                    structural_event_id, payload_hash, epoch_id, outcome,
                    original_open_receipt_binding_hash, publication_id,
                    original_publication_receipt_binding_hash,
                    event_work_kind, event_work_digest,
                    combined_status_delta_set_hash, changed_state_set_hash,
                    failure_reason, logical_result_hash, delta_count,
                    state_reference_count,
                    coordinator_non_db_non_neural_ns, neural_wall_ns,
                    postgres_roundtrip_wall_ns, external_io_wall_ns,
                    end_to_end_wall_ns, postgres_server_execution_ns,
                    postgres_lock_wait_ns, postgres_wal_bytes,
                    postgres_shared_block_reads
                ) VALUES (
                    %s, %s, %s, 'failed', %s, NULL, NULL, 'event', %s,
                    %s, %s, %s, %s, 0, 0,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    structural_event_id,
                    payload_hash,
                    epoch_id,
                    digests.open_event_receipt_binding_digest(
                        epoch_id=epoch_id,
                        replayed=False,
                        already_sealed=False,
                        publication_id=None,
                        already_failed=False,
                        failure_reason=None,
                    ),
                    zero_work.work_digest,
                    digests.combined_status_delta_set_digest(()),
                    digests.changed_state_set_digest(()),
                    failure_reason.value,
                    result.logical_result_hash,
                    event_timing.coordinator_non_db_non_neural_ns,
                    event_timing.neural_wall_ns,
                    event_timing.postgres_roundtrip_wall_ns,
                    event_timing.external_io_wall_ns,
                    event_timing.end_to_end_wall_ns,
                    event_timing.postgres_server_execution_ns,
                    event_timing.postgres_lock_wait_ns,
                    event_timing.postgres_wal_bytes,
                    event_timing.postgres_shared_block_reads,
                ),
            )
            _inject(failure_injector, "typed_fail_result_inserted")

            updated_base = cursor.execute(
                """
                UPDATE groundloop_epoch
                SET revision = %s, structural_status = 'failed',
                    semantic_status = 'failed', evaluation_state = 'failed',
                    publication_mode = 'provisional', sealed_at = NULL
                WHERE epoch_id = %s AND revision = %s
                """,
                (expected_revision + 1, epoch_id, expected_revision),
            ).rowcount
            if updated_base != 1:
                raise EventConflictError("stale base epoch revision")
            _inject(failure_injector, "typed_fail_base_updated")

            updated_runtime = cursor.execute(
                """
                UPDATE groundloop_m5_runtime_epoch
                SET runtime_state = 'failed', revision = %s,
                    open_work_count = 0, open_scope_count = 0,
                    blocking_failure_count = 0, terminal_at = now()
                WHERE epoch_id = %s AND revision = %s
                  AND runtime_state = 'structural_committed'
                """,
                (expected_revision + 1, epoch_id, expected_revision),
            ).rowcount
            if updated_runtime != 1:
                raise EventConflictError("stale typed runtime revision")
            _inject(failure_injector, "typed_fail_runtime_updated")
            _inject(failure_injector, "typed_fail_before_constraints")
            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
            _inject(failure_injector, "typed_fail_after_constraints")
            return result

    @staticmethod
    def _validate_failed_replay(
        result: M5EventRunResult, failure_reason: M5RunFailureReason
    ) -> M5EventRunResult:
        if result.replayed_outcome is M5ReplayedOutcome.SEALED:
            raise InvalidEventError("sealed typed epoch cannot fail")
        if result.failure_reason is not failure_reason:
            raise EventConflictError(
                "failed typed epoch already records another failure reason"
            )
        return result

    def _read_existing_open(
        self,
        cursor: Cursor[Any],
        plan: M5TypedEventPlan,
        *,
        for_update: bool,
    ) -> OpenEventReceipt | None:
        query = """
            SELECT epoch_id, payload_hash
            FROM groundloop_epoch
            WHERE event_id = %s
        """
        if for_update:
            query += " FOR UPDATE"
        epoch_row = cursor.execute(query, (plan.structural_event_id,)).fetchone()
        if epoch_row is None:
            return None
        epoch_id = int(epoch_row[0])
        if str(epoch_row[1]).strip() != plan.payload_hash:
            raise EventConflictError(
                "typed structural event ID was reused with another payload"
            )
        declaration = cursor.execute(
            """
            SELECT runtime.candidate_policy_id,
                   runtime.candidate_policy_manifest_hash,
                   runtime.requirement_registry_snapshot_digest,
                   runtime.active_chunk_snapshot_digest,
                   runtime.expected_previous_published_epoch_id,
                   update.update_kind, result.outcome, result.publication_id,
                   result.failure_reason
            FROM groundloop_m5_runtime_epoch AS runtime
            JOIN groundloop_m5_update AS update USING (epoch_id)
            LEFT JOIN groundloop_m5_event_result AS result
              ON result.epoch_id = runtime.epoch_id
            WHERE runtime.epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        if declaration is None:
            raise EventConflictError(
                "event ID belongs to a non-typed or incomplete declaration"
            )
        expected = (
            plan.candidate_policy_id,
            plan.candidate_policy_manifest_hash,
            plan.requirement_registry_snapshot.requirement_registry_snapshot_digest,
            plan.active_chunk_snapshot.active_chunk_snapshot_digest,
            plan.expected_previous_published_epoch_id,
            _m5_update_kind(plan.event),
        )
        actual = (
            str(declaration[0]),
            str(declaration[1]).strip(),
            str(declaration[2]).strip(),
            str(declaration[3]).strip(),
            int(declaration[4]),
            str(declaration[5]),
        )
        if actual != expected:
            raise EventConflictError("typed event declaration differs on replay")

        outcome = None if declaration[6] is None else str(declaration[6])
        if outcome is None:
            return OpenEventReceipt(
                epoch_id=epoch_id,
                replayed=True,
                already_sealed=False,
            )
        if outcome == "sealed":
            publication_id = str(declaration[7])
            return OpenEventReceipt(
                epoch_id=epoch_id,
                replayed=True,
                already_sealed=True,
                publication_id=publication_id,
            )
        failure_reason = str(declaration[8])
        return OpenEventReceipt(
            epoch_id=epoch_id,
            replayed=True,
            already_sealed=False,
            already_failed=True,
            failure_reason=failure_reason,
        )
