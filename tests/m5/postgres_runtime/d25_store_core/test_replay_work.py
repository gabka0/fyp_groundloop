"""Exact replay and retained-work tests for the D25 store core."""

from __future__ import annotations

import hashlib
from typing import Any

import pytest
from psycopg.errors import RaiseException

from groundloop.errors import EventConflictError, ValidationError
from groundloop.m5.incremental_overlay import M5OverlayWork
from groundloop.m5.matching import MatchingWorkCounters
from groundloop.m5.runtime.contracts import M5PersistedMatchingSourceKind
from groundloop.m5.runtime.postgres_matching import (
    _authorize_checked_prefix,
    apply_matching_transition,
    current_matching_work,
)
from tests.m5.postgres_runtime import test_migration_017 as schema_fixture
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    authorize_and_derive_matching_transition,
    install_document_matching_foundation_retained_bytes,
    prepare_stage_finalize_matching_transition,
    retained_matching_replay_intent,
)


class _RecordingCursor:
    def __init__(self, cursor: Any) -> None:
        self.cursor = cursor
        self.statements: list[str] = []

    def execute(self, query: Any, params: Any = None) -> Any:
        self.statements.append(str(query))
        return self.cursor.execute(query, params)


def _derive_first_application(cursor: Any, database: Any) -> Any:
    return authorize_and_derive_matching_transition(
        cursor,
        epoch_id=database.epoch_id,
        expected_runtime_revision=1,
        resulting_revision=1,
        source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
        source_id=database.event_id,
    )


def _derive_document_first_application(cursor: Any, database: Any) -> Any:
    return authorize_and_derive_matching_transition(
        cursor,
        epoch_id=database.epoch_id,
        expected_runtime_revision=1,
        resulting_revision=1,
        source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
        source_id=database.event_id,
        expected_source_identity_hash=database.payload_hash,
    )


def _first_apply_and_replay_projection(cursor: Any, database: Any) -> tuple[Any, Any]:
    intent = _derive_first_application(cursor, database)
    receipt = prepare_stage_finalize_matching_transition(cursor, intent)
    replay_intent = retained_matching_replay_intent(
        cursor,
        epoch_id=database.epoch_id,
        source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
        source_id=database.event_id,
    )
    return receipt, replay_intent


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


def _advance_live_matching_head(database: Any, suffix: str) -> int:
    connection = database.connection
    event_id = f"{database.event_id}-{suffix}"
    payload_hash = hashlib.sha256(event_id.encode()).hexdigest()
    with schema_fixture._d26_replica_trigger_window(connection):
        row = connection.execute(
            """
            INSERT INTO groundloop_epoch (
              event_id, payload_hash, revision, structural_status,
              semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (%s,%s,0,'committed','sealed','complete','strict',now())
            RETURNING epoch_id
            """,
            (event_id, payload_hash),
        ).fetchone()
        assert row is not None
        epoch_id = int(row[0])
        connection.execute(
            """
            UPDATE groundloop_m5_matching_image_current
            SET installed_epoch_id=%s, installed_revision=0,
                decision_policy_version=%s
            WHERE singleton
            """,
            (epoch_id, database.policy_version),
        )
        connection.execute(
            "UPDATE groundloop_m4_publication_head SET epoch_id=%s WHERE singleton",
            (epoch_id,),
        )
        connection.execute(
            """
            UPDATE groundloop_m5_publication_head
            SET epoch_id=%s, sealed_revision=0
            WHERE singleton
            """,
            (epoch_id,),
        )
    connection.commit()
    return epoch_id


def test_exact_replay_returns_same_bytes_and_performs_zero_writes(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        first, replay_intent = _first_apply_and_replay_projection(cursor, database)
    connection.commit()
    before = _retained_image(connection, database.epoch_id)
    connection.commit()

    with connection.cursor() as cursor:
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
        _, replay_intent = _first_apply_and_replay_projection(cursor, database)
    connection.commit()
    before = _retained_image(connection, database.epoch_id)
    connection.commit()

    wrong_work = M5OverlayWork(matching=MatchingWorkCounters(output_bytes=72))
    with connection.cursor() as cursor:
        with pytest.raises(
            EventConflictError, match="matching replay differs from expected work"
        ):
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
            ValidationError,
            "retained matching outer patch digest changed",
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
        _, replay_intent = _first_apply_and_replay_projection(cursor, database)
    connection.commit()
    with schema_fixture._b3_user_triggers_disabled(connection, table):
        connection.execute(statement)
    connection.commit()
    retained = _retained_image(connection, database.epoch_id)

    with connection.cursor() as cursor:
        with pytest.raises(error, match=message):
            apply_matching_transition(cursor, replay_intent)
    connection.rollback()
    assert _retained_image(connection, database.epoch_id) == retained


@pytest.mark.parametrize(
    ("statement", "message"),
    (
        (
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET source_id=source_id || '-x'",
            "retained matching outer patch digest changed",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET source_identity_hash=repeat('0',64)::char(64)",
            "retained matching outer patch digest changed",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET before_epoch_id=resulting_epoch_id",
            "retained matching outer patch digest changed",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET before_revision=before_revision+1",
            "retained matching outer patch digest changed",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET resulting_epoch_id=before_epoch_id",
            "retained matching outer patch digest changed",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET group_shape_set_digest=repeat('0',64)::char(64)",
            "retained matching group-shape digest changed",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET logical_overlay_patch_digest=repeat('0',64)::char(64)",
            "retained logical patch digest changed",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET matching_work_digest=repeat('0',64)::char(64)",
            "retained matching outer patch digest changed",
        ),
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
    empty_structural_database: Any, statement: str, message: str
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        _, replay_intent = _first_apply_and_replay_projection(cursor, database)
    connection.commit()
    with schema_fixture._b3_user_triggers_disabled(
        connection, "groundloop_m5_matching_patch_artifact"
    ):
        connection.execute(statement)
    connection.commit()
    retained = _retained_image(connection, database.epoch_id)

    with connection.cursor() as cursor:
        with pytest.raises(ValidationError, match=message):
            apply_matching_transition(cursor, replay_intent)
    connection.rollback()
    assert _retained_image(connection, database.epoch_id) == retained


@pytest.mark.parametrize(
    ("statement", "error", "message"),
    (
        (
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET group_shape_set_preimage="
            "group_shape_set_preimage || decode('00','hex')",
            RaiseException,
            "truncated persisted-matching frame length",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact SET "
            "observation_change_digests=ARRAY[repeat('0',64)::char(64)], "
            "observation_change_preimages=ARRAY[decode('00','hex')]",
            ValidationError,
            "retained matching child digest changed",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact SET "
            "edge_change_digests=ARRAY[repeat('0',64)::char(64)], "
            "edge_change_preimages=ARRAY[decode('00','hex')]",
            ValidationError,
            "retained matching child digest changed",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact SET "
            "mask_change_digests=ARRAY[repeat('0',64)::char(64)], "
            "mask_change_preimages=ARRAY[decode('00','hex')]",
            ValidationError,
            "retained matching child digest changed",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact SET "
            "hall_change_digests=ARRAY[repeat('0',64)::char(64)], "
            "hall_change_preimages=ARRAY[decode('00','hex')]",
            ValidationError,
            "retained matching child digest changed",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact SET "
            "logical_overlay_patch_preimage="
            "logical_overlay_patch_preimage || decode('00','hex')",
            RaiseException,
            "truncated persisted-matching frame length",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact SET "
            "logical_output_preimage="
            "logical_output_preimage || decode('00','hex')",
            RaiseException,
            "logical tuple trailing byte",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact SET "
            "canonical_patch_preimage="
            "canonical_patch_preimage || decode('00','hex')",
            ValidationError,
            "retained matching outer patch digest changed",
        ),
    ),
    ids=(
        "group-shape-preimage",
        "observation-children",
        "edge-children",
        "mask-children",
        "hall-children",
        "logical-patch-preimage",
        "logical-output-preimage",
        "canonical-patch-preimage",
    ),
)
def test_replay_validates_every_artifact_child_and_preimage(
    empty_structural_database: Any,
    statement: str,
    error: type[Exception],
    message: str,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        _, replay_intent = _first_apply_and_replay_projection(cursor, database)
    connection.commit()
    with schema_fixture._b3_user_triggers_disabled(
        connection, "groundloop_m5_matching_patch_artifact"
    ):
        connection.execute(statement)
    connection.commit()
    retained = _retained_image(connection, database.epoch_id)

    with connection.cursor() as cursor:
        with pytest.raises(error, match=message):
            apply_matching_transition(cursor, replay_intent)
    connection.rollback()
    assert _retained_image(connection, database.epoch_id) == retained


def test_replay_rejects_artifact_decision_policy_scalar_corruption(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        _, replay_intent = _first_apply_and_replay_projection(cursor, database)
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
    retained = _retained_image(connection, database.epoch_id)

    with connection.cursor() as cursor:
        with pytest.raises(
            ValidationError, match="retained matching outer patch digest changed"
        ):
            apply_matching_transition(cursor, replay_intent)
    connection.rollback()
    assert _retained_image(connection, database.epoch_id) == retained


def test_replay_rejects_artifact_patch_digest_key_corruption(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        _, replay_intent = _first_apply_and_replay_projection(cursor, database)
    connection.commit()
    with schema_fixture._d26_replica_trigger_window(connection):
        connection.execute(
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET patch_digest=repeat('0',64)::char(64)"
        )
    connection.commit()
    retained = _retained_image(connection, database.epoch_id)

    with connection.cursor() as cursor:
        with pytest.raises(EventConflictError, match="lacks its retained patch"):
            apply_matching_transition(cursor, replay_intent)
    connection.rollback()
    assert _retained_image(connection, database.epoch_id) == retained


@pytest.mark.parametrize(
    "statement",
    (
        "UPDATE groundloop_m5_matching_image_working SET base_epoch_id=epoch_id",
        "UPDATE groundloop_m5_matching_image_working SET base_revision=base_revision+1",
        "UPDATE groundloop_m5_matching_image_working "
        "SET updated_revision=updated_revision+1",
    ),
    ids=("base-epoch", "base-revision", "updated-revision"),
)
def test_replay_rejects_corrupted_working_image_scalars(
    empty_structural_database: Any, statement: str
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        _, replay_intent = _first_apply_and_replay_projection(cursor, database)
    connection.commit()
    with schema_fixture._b3_user_triggers_disabled(
        connection, "groundloop_m5_matching_image_working"
    ):
        connection.execute(statement)
    connection.commit()
    retained = _retained_image(connection, database.epoch_id)

    with connection.cursor() as cursor:
        with pytest.raises(
            ValidationError, match="persisted matching work envelope is inconsistent"
        ):
            apply_matching_transition(cursor, replay_intent)
    connection.rollback()
    assert _retained_image(connection, database.epoch_id) == retained


def test_replay_rejects_accumulator_updated_revision_corruption(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        _, replay_intent = _first_apply_and_replay_projection(cursor, database)
    connection.commit()
    with schema_fixture._b3_user_triggers_disabled(
        connection, "groundloop_m5_matching_work_accumulator"
    ):
        connection.execute(
            "UPDATE groundloop_m5_matching_work_accumulator "
            "SET updated_revision=updated_revision+1"
        )
    connection.commit()
    retained = _retained_image(connection, database.epoch_id)

    with connection.cursor() as cursor:
        with pytest.raises(ValidationError, match="accumulator revision"):
            apply_matching_transition(cursor, replay_intent)
    connection.rollback()
    assert _retained_image(connection, database.epoch_id) == retained


def test_current_matching_work_reads_the_digest_checked_retained_accumulator(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    with database.connection.cursor() as cursor:
        intent = _derive_first_application(cursor, database)
        receipt = prepare_stage_finalize_matching_transition(cursor, intent)
        retained = current_matching_work(cursor, database.epoch_id)
    assert retained == receipt.accumulated_work
    assert retained.matching.output_bytes == 71


def test_current_matching_work_requires_the_exact_checked_image_scope(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    with database.connection.cursor() as cursor:
        intent = _derive_first_application(cursor, database)
        prepare_stage_finalize_matching_transition(cursor, intent)
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
        intent = _derive_first_application(cursor, database)
        prepare_stage_finalize_matching_transition(cursor, intent)
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


def test_current_matching_work_reads_a_retained_failed_epoch_after_head_advance(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_first_application(cursor, database)
        receipt = prepare_stage_finalize_matching_transition(cursor, intent)
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
    later_epoch = _advance_live_matching_head(database, "later-after-failure")
    assert later_epoch > database.epoch_id

    with connection.cursor() as cursor:
        _authorize_checked_prefix(
            cursor, epoch_id=database.epoch_id, expected_revision=2
        )
        retained = current_matching_work(cursor, database.epoch_id)
    assert retained == receipt.accumulated_work


def test_current_matching_work_reads_a_retained_sealed_epoch_after_head_advance(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_first_application(cursor, database)
        receipt = prepare_stage_finalize_matching_transition(cursor, intent)
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
    later_epoch = _advance_live_matching_head(database, "later-after-seal")
    assert later_epoch > database.epoch_id

    with connection.cursor() as cursor:
        _authorize_checked_prefix(
            cursor, epoch_id=database.epoch_id, expected_revision=2
        )
        retained = current_matching_work(cursor, database.epoch_id)
    assert retained == receipt.accumulated_work


def test_committed_nonterminal_document_replay_uses_only_retained_changed_key_bytes(
    document_withdrawal_planning_database: Any,
) -> None:
    database = document_withdrawal_planning_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_document_first_application(cursor, database)
        # Foundation-only retained-byte setup; not D28/C1 application evidence.
        first = install_document_matching_foundation_retained_bytes(cursor, intent)
        replay_intent = retained_matching_replay_intent(
            cursor,
            epoch_id=database.epoch_id,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=database.event_id,
        )
    connection.commit()
    before = _retained_image(connection, database.epoch_id)
    connection.commit()

    # Legal terminalized later-head replay remains C1/D combined acceptance.
    with connection.cursor() as raw_cursor:
        cursor = _RecordingCursor(raw_cursor)
        replay = apply_matching_transition(
            cursor,
            replay_intent,
            expected_patch_digest=first.patch.patch_digest,
            expected_work=first.accumulated_work,
        )
        statements = tuple(cursor.statements)
    connection.commit()

    assert replay.exact_replay
    assert replay.patch == first.patch
    assert replay.contribution_digest == first.contribution_digest
    assert replay.accumulated_work == first.accumulated_work
    assert _retained_image(connection, database.epoch_id) == before
    assert all(
        not statement.lstrip().upper().startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )
    forbidden_live_sources = (
        "groundloop_m4_structural_deactivation",
        "groundloop_observation_currency",
        "groundloop_published_observation_currency",
        "groundloop_m5_requirement_admitted_pair",
        "groundloop_m5_matching_observation_current",
        "groundloop_m5_matching_edge_current",
        "groundloop_m5_matching_hash_mask_current",
        "groundloop_m5_matching_hall_current",
    )
    assert not any(
        relation in statement
        for relation in forbidden_live_sources
        for statement in statements
    )
