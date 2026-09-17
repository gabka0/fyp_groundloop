"""Exact replay and retained-work tests for the D25 store core."""

from __future__ import annotations

from typing import Any

import pytest

from groundloop.errors import EventConflictError, ValidationError
from groundloop.m5.incremental_overlay import M5OverlayWork
from groundloop.m5.matching import MatchingWorkCounters
from groundloop.m5.runtime.contracts import M5PersistedMatchingSourceKind
from groundloop.m5.runtime.postgres_matching import (
    apply_matching_transition,
    current_matching_work,
    derive_matching_transition_intent,
)
from tests.m5.postgres_runtime import test_migration_017 as schema_fixture


def _retained_image(connection: Any, epoch_id: int) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        tuple(row)
        for row in connection.execute(
            """
            SELECT 'image', xmin::text, epoch_id::text,
                   base_epoch_id::text, base_revision::text,
                   decision_policy_version, updated_revision::text
            FROM groundloop_m5_matching_image_working WHERE epoch_id = %s
            UNION ALL
            SELECT 'artifact', xmin::text, patch_digest::text,
                   source_kind, source_id, matching_work_digest::text,
                   octet_length(canonical_patch_preimage)::text
            FROM groundloop_m5_matching_patch_artifact
            WHERE resulting_epoch_id = %s
            UNION ALL
            SELECT 'contribution', xmin::text, patch_digest::text,
                   source_kind, source_id, matching_work_digest::text,
                   contribution_digest::text
            FROM groundloop_m5_matching_work_contribution WHERE epoch_id = %s
            UNION ALL
            SELECT 'accumulator', xmin::text, epoch_id::text,
                   updated_revision::text, matching_work_digest::text,
                   output_bytes::text, 'retained'
            FROM groundloop_m5_matching_work_accumulator WHERE epoch_id = %s
            ORDER BY 1, 3
            """,
            (epoch_id,) * 4,
        ).fetchall()
    )


def test_exact_replay_returns_same_bytes_and_performs_zero_writes(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
        )
        first = apply_matching_transition(cursor, intent)
    connection.commit()
    before = _retained_image(connection, database.epoch_id)
    connection.commit()

    with connection.cursor() as cursor:
        replay_intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
            database.payload_hash,
        )
        replay = apply_matching_transition(
            cursor,
            replay_intent,
            expected_patch_digest=first.patch.patch_digest,
            expected_work=first.accumulated_work,
        )
    connection.commit()
    after = _retained_image(connection, database.epoch_id)
    connection.commit()

    assert replay.exact_replay
    assert replay.patch == first.patch
    assert replay.contribution_digest == first.contribution_digest
    assert replay.accumulated_work == first.accumulated_work
    assert after == before


def test_replay_compare_only_work_conflict_preserves_every_retained_row(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
        )
        apply_matching_transition(cursor, intent)
    connection.commit()
    before = _retained_image(connection, database.epoch_id)
    connection.commit()

    wrong_work = M5OverlayWork(matching=MatchingWorkCounters(output_bytes=72))
    with connection.cursor() as cursor:
        replay_intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
        )
        with pytest.raises(EventConflictError, match="computed matching work"):
            apply_matching_transition(cursor, replay_intent, expected_work=wrong_work)
    connection.rollback()
    assert _retained_image(connection, database.epoch_id) == before


@pytest.mark.parametrize(
    ("table", "statement", "error", "message"),
    (
        (
            "groundloop_m5_matching_patch_artifact",
            """
            UPDATE groundloop_m5_matching_patch_artifact
            SET canonical_patch_preimage = canonical_patch_preimage || E'\\\\x00'::bytea
            """,
            EventConflictError,
            "retained patch bytes",
        ),
        (
            "groundloop_m5_matching_work_contribution",
            """
            UPDATE groundloop_m5_matching_work_contribution
            SET contribution_digest = repeat('0', 64)::char(64)
            """,
            EventConflictError,
            "retained patch bytes",
        ),
        (
            "groundloop_m5_matching_work_accumulator",
            """
            UPDATE groundloop_m5_matching_work_accumulator
            SET output_bytes = output_bytes + 1
            """,
            ValidationError,
            "accumulator digest",
        ),
    ),
    ids=("artifact", "contribution", "accumulator"),
)
def test_replay_validates_all_retained_byte_classes_before_return(
    empty_structural_database: Any,
    table: str,
    statement: str,
    error: type[Exception],
    message: str,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
        )
        apply_matching_transition(cursor, intent)
    connection.commit()
    with schema_fixture._b3_user_triggers_disabled(connection, table):
        connection.execute(statement)
    connection.commit()
    retained = _retained_image(connection, database.epoch_id)

    with connection.cursor() as cursor:
        replay_intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
        )
        with pytest.raises(error, match=message):
            apply_matching_transition(cursor, replay_intent)
    connection.rollback()
    assert _retained_image(connection, database.epoch_id) == retained


def test_current_matching_work_reads_the_digest_checked_retained_accumulator(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    with database.connection.cursor() as cursor:
        intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
        )
        receipt = apply_matching_transition(cursor, intent)
        retained = current_matching_work(cursor, database.epoch_id)
    assert retained == receipt.accumulated_work
    assert retained.matching.output_bytes == 71
