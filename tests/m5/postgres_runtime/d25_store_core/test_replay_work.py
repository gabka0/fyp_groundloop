"""Exact replay and retained-work tests for the D25 store core."""

from __future__ import annotations

import hashlib
from typing import Any

import pytest

from groundloop.errors import EventConflictError, ValidationError
from groundloop.m5.incremental_overlay import M5OverlayWork
from groundloop.m5.matching import MatchingWorkCounters
from groundloop.m5.runtime.contracts import M5PersistedMatchingSourceKind
from groundloop.m5.runtime.postgres_matching import (
    _authorize_checked_prefix,
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
            "retained contribution bytes",
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


@pytest.mark.parametrize(
    "statement",
    (
        "UPDATE groundloop_m5_matching_patch_artifact SET source_id=source_id || '-x'",
        "UPDATE groundloop_m5_matching_patch_artifact "
        "SET source_identity_hash=repeat('0',64)::char(64)",
        "UPDATE groundloop_m5_matching_patch_artifact "
        "SET before_epoch_id=resulting_epoch_id",
        "UPDATE groundloop_m5_matching_patch_artifact "
        "SET before_revision=before_revision+1",
        "UPDATE groundloop_m5_matching_patch_artifact "
        "SET resulting_epoch_id=before_epoch_id",
        "UPDATE groundloop_m5_matching_patch_artifact "
        "SET group_shape_set_digest=repeat('0',64)::char(64)",
        "UPDATE groundloop_m5_matching_patch_artifact "
        "SET logical_overlay_patch_digest=repeat('0',64)::char(64)",
        "UPDATE groundloop_m5_matching_patch_artifact "
        "SET matching_work_digest=repeat('0',64)::char(64)",
    ),
    ids=(
        "source-id",
        "source-identity",
        "before-epoch",
        "before-revision",
        "resulting-epoch",
        "group-shape-digest",
        "logical-patch-digest",
        "matching-work-digest",
    ),
)
def test_replay_validates_every_mutable_artifact_scalar(
    empty_structural_database: Any, statement: str
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
    with schema_fixture._b3_user_triggers_disabled(
        connection, "groundloop_m5_matching_patch_artifact"
    ):
        connection.execute(statement)
    connection.commit()

    with connection.cursor() as cursor:
        replay_intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
        )
        with pytest.raises(EventConflictError, match="retained patch bytes"):
            apply_matching_transition(cursor, replay_intent)


def test_replay_rejects_artifact_decision_policy_scalar_corruption(
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
    boundary_id = f"{database.event_id}-artifact-policy-boundary"
    boundary = connection.execute(
        """
        INSERT INTO groundloop_epoch (
          event_id, payload_hash, revision, structural_status, semantic_status,
          evaluation_state, publication_mode, sealed_at
        ) VALUES (%s,%s,0,'committed','sealed','complete','strict',now())
        RETURNING epoch_id
        """,
        (boundary_id, hashlib.sha256(boundary_id.encode()).hexdigest()),
    ).fetchone()
    assert boundary is not None
    alternate_policy = f"{database.policy_version}-artifact-mismatch"
    connection.execute(
        """
        INSERT INTO groundloop_decision_policy (
          policy_version, support_threshold, refute_threshold,
          tie_rule_version, valid_from_epoch, valid_to_epoch
        ) VALUES (%s,.5,.5,'v1',%s,%s)
        """,
        (alternate_policy, database.epoch_id, int(boundary[0])),
    )
    with schema_fixture._b3_user_triggers_disabled(
        connection, "groundloop_m5_matching_patch_artifact"
    ):
        connection.execute(
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET decision_policy_version=%s",
            (alternate_policy,),
        )
    connection.commit()

    with connection.cursor() as cursor:
        replay_intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
        )
        with pytest.raises(EventConflictError, match="retained patch bytes"):
            apply_matching_transition(cursor, replay_intent)


def test_replay_rejects_artifact_patch_digest_key_corruption(
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
    with schema_fixture._d26_replica_trigger_window(connection):
        connection.execute(
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET patch_digest=repeat('0',64)::char(64)"
        )
    connection.commit()

    with connection.cursor() as cursor:
        replay_intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
        )
        with pytest.raises(EventConflictError, match="lacks its retained patch"):
            apply_matching_transition(cursor, replay_intent)


@pytest.mark.parametrize(
    ("statement", "message"),
    (
        (
            "UPDATE groundloop_m5_matching_image_working SET base_epoch_id=epoch_id",
            "current image point|image policy|image bytes",
        ),
        (
            "UPDATE groundloop_m5_matching_image_working "
            "SET base_revision=base_revision+1",
            "image policy|image bytes",
        ),
        (
            "UPDATE groundloop_m5_matching_image_working "
            "SET updated_revision=updated_revision+1",
            "image bytes",
        ),
    ),
    ids=("base-epoch", "base-revision", "updated-revision"),
)
def test_replay_rejects_corrupted_working_image_scalars(
    empty_structural_database: Any, statement: str, message: str
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
    with schema_fixture._b3_user_triggers_disabled(
        connection, "groundloop_m5_matching_image_working"
    ):
        connection.execute(statement)
    connection.commit()

    with connection.cursor() as cursor:
        replay_intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
        )
        with pytest.raises((EventConflictError, ValidationError), match=message):
            apply_matching_transition(cursor, replay_intent)


def test_replay_rejects_accumulator_updated_revision_corruption(
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
    with schema_fixture._b3_user_triggers_disabled(
        connection, "groundloop_m5_matching_work_accumulator"
    ):
        connection.execute(
            "UPDATE groundloop_m5_matching_work_accumulator "
            "SET updated_revision=updated_revision+1"
        )
    connection.commit()

    with connection.cursor() as cursor:
        replay_intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
        )
        with pytest.raises(ValidationError, match="accumulator revision"):
            apply_matching_transition(cursor, replay_intent)


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


def test_current_matching_work_requires_the_exact_checked_image_scope(
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
        apply_matching_transition(cursor, intent)
    database.connection.commit()

    with database.connection.cursor() as cursor:
        with pytest.raises(ValidationError, match="exact checked epoch prefix"):
            current_matching_work(cursor, database.epoch_id)


def test_current_matching_work_rejects_an_incoherent_nonterminal_header(
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
    with schema_fixture._d26_replica_trigger_window(connection):
        connection.execute(
            """
            UPDATE groundloop_epoch
            SET semantic_status='complete', evaluation_state='complete'
            WHERE epoch_id=%s
            """,
            (database.epoch_id,),
        )
    connection.commit()

    with connection.cursor() as cursor:
        _authorize_checked_prefix(
            cursor, epoch_id=database.epoch_id, expected_revision=1
        )
        with pytest.raises(ValidationError, match="terminal envelope"):
            current_matching_work(cursor, database.epoch_id)


def test_current_matching_work_reads_a_retained_failed_epoch(
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
        receipt = apply_matching_transition(cursor, intent)
    connection.commit()
    with schema_fixture._d26_replica_trigger_window(connection):
        connection.execute(
            """
            UPDATE groundloop_epoch
            SET revision=2, structural_status='failed', semantic_status='failed',
                evaluation_state='failed', publication_mode='provisional',
                sealed_at=NULL
            WHERE epoch_id=%s
            """,
            (database.epoch_id,),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_runtime_epoch
            SET revision=2, runtime_state='failed', terminal_at=now()
            WHERE epoch_id=%s
            """,
            (database.epoch_id,),
        )
    connection.commit()

    with connection.cursor() as cursor:
        _authorize_checked_prefix(
            cursor, epoch_id=database.epoch_id, expected_revision=2
        )
        retained = current_matching_work(cursor, database.epoch_id)
    assert retained == receipt.accumulated_work


def test_current_matching_work_reads_a_retained_sealed_epoch(
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
        receipt = apply_matching_transition(cursor, intent)
    connection.commit()
    with schema_fixture._d26_replica_trigger_window(connection):
        connection.execute(
            """
            UPDATE groundloop_epoch
            SET revision=2, structural_status='committed', semantic_status='sealed',
                evaluation_state='complete', publication_mode='strict',
                sealed_at=now()
            WHERE epoch_id=%s
            """,
            (database.epoch_id,),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_runtime_epoch
            SET revision=2, runtime_state='sealed', terminal_at=now()
            WHERE epoch_id=%s
            """,
            (database.epoch_id,),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_matching_image_current
            SET installed_epoch_id=%s, installed_revision=2,
                decision_policy_version=%s
            WHERE singleton
            """,
            (database.epoch_id, database.policy_version),
        )
        connection.execute(
            "UPDATE groundloop_m4_publication_head SET epoch_id=%s WHERE singleton",
            (database.epoch_id,),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_publication_head
            SET epoch_id=%s, sealed_revision=2
            WHERE singleton
            """,
            (database.epoch_id,),
        )
    connection.commit()

    with connection.cursor() as cursor:
        _authorize_checked_prefix(
            cursor, epoch_id=database.epoch_id, expected_revision=2
        )
        retained = current_matching_work(cursor, database.epoch_id)
    assert retained == receipt.accumulated_work
