"""Store-derived empty transition and fail-closed source tests."""

from __future__ import annotations

from typing import Any

import pytest
from psycopg import sql

from groundloop.errors import EventConflictError, InvalidEventError
from groundloop.m5.runtime.contracts import (
    MATCHING_WORK_COUNTER_NAMES,
    M5PersistedMatchingSourceKind,
)
from groundloop.m5.runtime.postgres_matching import (
    apply_matching_transition,
)
from tests.m5.postgres_runtime import test_migration_017 as schema_fixture
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    authorize_and_derive_matching_transition,
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


def _relation_counts(connection: Any) -> tuple[int, ...]:
    relations = (
        "groundloop_m5_matching_image_working",
        "groundloop_m5_matching_patch_artifact",
        "groundloop_m5_matching_work_contribution",
        "groundloop_m5_matching_work_accumulator",
    )
    return tuple(
        int(
            connection.execute(
                sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(relation))
            ).fetchone()[0]
        )
        for relation in relations
    )


def test_empty_structural_open_derives_and_applies_store_bytes(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = authorize_and_derive_matching_transition(
            cursor,
            epoch_id=database.epoch_id,
            expected_runtime_revision=1,
            resulting_revision=1,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=database.event_id,
            expected_source_identity_hash=database.payload_hash,
        )
        assert intent.before_epoch_id == database.base_epoch_id
        assert intent.before_revision == database.base_revision
        assert intent.decision_policy_version == database.policy_version
        assert intent.group_shapes == ()
        assert intent.observation_ids == ()
        assert intent.edge_keys == ()
        assert intent.mask_keys == ()
        assert intent.hall_group_ids == ()
        assert intent.requirement_state_ids == ()
        assert intent.group_state_ids == ()
        assert intent.claim_state_ids == ()
        assert intent.answer_state_ids == ()
        assert intent.group_certificate_ids == ()
        assert intent.claim_certificate_ids == ()

        receipt = prepare_stage_finalize_matching_transition(cursor, intent)

    assert not receipt.exact_replay
    assert receipt.resulting_revision == 1
    assert receipt.accumulated_work.matching.output_bytes == 71
    assert _relation_counts(connection) == (1, 1, 1, 1)
    assert connection.execute(
        """
        SELECT octet_length(logical_output_preimage),
               cardinality(observation_change_digests),
               cardinality(edge_change_digests),
               cardinality(mask_change_digests),
               cardinality(hall_change_digests)
        FROM groundloop_m5_matching_patch_artifact
        WHERE patch_digest = %s
        """,
        (receipt.patch.patch_digest,),
    ).fetchone() == (71, 0, 0, 0, 0)
    d24_and_d25 = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_runtime_work_contribution
            WHERE epoch_id = %s AND contribution_kind = 'structural_open'),
          (SELECT count(*) FROM groundloop_m5_runtime_work_accumulator
            WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_m5_runtime_timing_accumulator
            WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_m5_matching_work_contribution
            WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_m5_matching_work_accumulator
            WHERE epoch_id = %s)
        """,
        (database.epoch_id,) * 5,
    ).fetchone()
    assert d24_and_d25 == (1, 1, 1, 1, 1)
    stored_counters = connection.execute(
        sql.SQL("SELECT {} FROM groundloop_m5_matching_work_contribution").format(
            sql.SQL(",").join(
                sql.Identifier(name) for name in MATCHING_WORK_COUNTER_NAMES
            )
        )
    ).fetchone()
    assert stored_counters is not None
    assert (
        tuple(int(value) for value in stored_counters) == (0,) * 30 + (71,) + (0,) * 6
    )


def test_first_apply_and_replay_follow_image_artifact_contribution_accumulator_order(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        first_recording = _RecordingCursor(cursor)
        intent = authorize_and_derive_matching_transition(
            first_recording,
            epoch_id=database.epoch_id,
            expected_runtime_revision=1,
            resulting_revision=1,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=database.event_id,
        )
        receipt = prepare_stage_finalize_matching_transition(first_recording, intent)

    transition_authorizer = next(
        index
        for index, statement in enumerate(first_recording.statements)
        if "groundloop_m5_authorize_persisted_matching_transition" in statement
    )
    current_image = next(
        index
        for index, statement in enumerate(first_recording.statements)
        if index > transition_authorizer
        and "FROM groundloop_m5_matching_image_current" in statement
        and "FOR UPDATE" in statement
    )
    working_image = next(
        index
        for index, statement in enumerate(first_recording.statements)
        if index > current_image
        and "FROM groundloop_m5_matching_image_working" in statement
        and "FOR UPDATE" in statement
    )
    artifact_insert = next(
        index
        for index, statement in enumerate(first_recording.statements)
        if "INSERT INTO groundloop_m5_matching_patch_artifact" in statement
    )
    artifact_advisory = max(
        index
        for index, statement in enumerate(first_recording.statements)
        if index < artifact_insert and "pg_advisory_xact_lock" in statement
    )
    contribution_lock = next(
        index
        for index, statement in enumerate(first_recording.statements)
        if index > artifact_insert
        and "FROM groundloop_m5_matching_work_contribution" in statement
        and "FOR SHARE" in statement
    )
    contribution_insert = next(
        index
        for index, statement in enumerate(first_recording.statements)
        if "INSERT INTO groundloop_m5_matching_work_contribution" in statement
    )
    accumulator_insert = next(
        index
        for index, statement in enumerate(first_recording.statements)
        if "INSERT INTO groundloop_m5_matching_work_accumulator" in statement
    )
    assert (
        current_image
        < working_image
        < artifact_advisory
        < artifact_insert
        < contribution_lock
        < contribution_insert
        < accumulator_insert
    )
    preartifact_contribution_reads = tuple(
        statement
        for statement in first_recording.statements[:artifact_advisory]
        if "FROM groundloop_m5_matching_work_contribution" in statement
    )
    assert len(preartifact_contribution_reads) <= 1
    assert not any(
        "FOR SHARE" in statement or "FOR UPDATE" in statement
        for statement in preartifact_contribution_reads
    )

    connection.commit()
    with connection.cursor() as cursor:
        replay_intent = retained_matching_replay_intent(
            cursor,
            epoch_id=database.epoch_id,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=database.event_id,
        )
        replay_recording = _RecordingCursor(cursor)
        replay = apply_matching_transition(replay_recording, replay_intent)
    assert replay.exact_replay
    replay_current = next(
        index
        for index, statement in enumerate(replay_recording.statements)
        if "FROM groundloop_m5_matching_image_current" in statement
        and "FOR UPDATE" in statement
    )
    replay_working = next(
        index
        for index, statement in enumerate(replay_recording.statements)
        if index > replay_current
        and "FROM groundloop_m5_matching_image_working" in statement
        and "FOR UPDATE" in statement
    )
    replay_contribution = next(
        index
        for index, statement in enumerate(replay_recording.statements)
        if index > replay_working
        and "FROM groundloop_m5_matching_work_contribution" in statement
        and "FOR SHARE" in statement
    )
    replay_accumulator = next(
        index
        for index, statement in enumerate(replay_recording.statements)
        if "FROM groundloop_m5_matching_work_accumulator" in statement
    )
    replay_artifact = max(
        index
        for index, statement in enumerate(replay_recording.statements)
        if index < replay_contribution and "pg_advisory_xact_lock" in statement
    )
    assert (
        replay_current
        < replay_working
        < replay_artifact
        < replay_contribution
        < replay_accumulator
    )
    prereplay_artifact_contribution_reads = tuple(
        statement
        for statement in replay_recording.statements[:replay_artifact]
        if "FROM groundloop_m5_matching_work_contribution" in statement
    )
    assert len(prereplay_artifact_contribution_reads) <= 1
    assert not any(
        "FOR SHARE" in statement or "FOR UPDATE" in statement
        for statement in prereplay_artifact_contribution_reads
    )
    assert not any(
        "INSERT INTO groundloop_m5_matching_" in statement
        for statement in replay_recording.statements
    )
    assert receipt.patch.patch_digest == replay.patch.patch_digest


def test_compare_only_source_hash_and_patch_inputs_write_nothing(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    before = _relation_counts(connection)
    with pytest.raises(EventConflictError, match="source hash changed"):
        with connection.transaction():
            with connection.cursor() as cursor:
                authorize_and_derive_matching_transition(
                    cursor,
                    epoch_id=database.epoch_id,
                    expected_runtime_revision=1,
                    resulting_revision=1,
                    source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
                    source_id=database.event_id,
                    expected_source_identity_hash="0" * 64,
                )
    with connection.cursor() as cursor:
        intent = authorize_and_derive_matching_transition(
            cursor,
            epoch_id=database.epoch_id,
            expected_runtime_revision=1,
            resulting_revision=1,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=database.event_id,
        )
        with pytest.raises(EventConflictError, match="patch digest differs"):
            prepare_stage_finalize_matching_transition(
                cursor,
                intent,
                expected_patch_digest="0" * 64,
            )
    assert _relation_counts(connection) == before


@pytest.mark.parametrize("update_kind", ("policy_change", "observe_requirement"))
def test_deliberately_unsupported_structural_forms_fail_before_d25_write(
    empty_structural_database: Any,
    update_kind: str,
) -> None:
    database = empty_structural_database
    connection = database.connection
    before = _relation_counts(connection)
    with connection.cursor() as cursor:
        with schema_fixture._d26_replica_trigger_window(connection):
            changed = cursor.execute(
                "UPDATE groundloop_m5_update SET update_kind=%s WHERE epoch_id=%s",
                (update_kind, database.epoch_id),
            ).rowcount
        assert changed == 1
        with pytest.raises(
            InvalidEventError,
            match=(
                rf"unsupported structural source {update_kind} "
                "before its first write"
            ),
        ):
            authorize_and_derive_matching_transition(
                cursor,
                epoch_id=database.epoch_id,
                expected_runtime_revision=1,
                resulting_revision=1,
                source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
                source_id=database.event_id,
                expected_source_identity_hash=database.payload_hash,
            )
    assert _relation_counts(connection) == before


def test_missing_requirement_completion_source_fails_before_d25_write(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    before = _relation_counts(connection)
    with connection.cursor() as cursor:
        with pytest.raises(
            InvalidEventError,
            match="requirement matching source attempt is absent",
        ):
            authorize_and_derive_matching_transition(
                cursor,
                epoch_id=database.epoch_id,
                expected_runtime_revision=1,
                resulting_revision=2,
                source_kind=M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION,
                source_id="missing-requirement-attempt",
            )
    assert _relation_counts(connection) == before


def test_direct_transition_without_a_reserved_source_fails_before_write(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    before = _relation_counts(connection)
    with connection.cursor() as cursor:
        with pytest.raises(InvalidEventError, match="before its first write"):
            authorize_and_derive_matching_transition(
                cursor,
                epoch_id=database.epoch_id,
                expected_runtime_revision=1,
                resulting_revision=2,
                source_kind=M5PersistedMatchingSourceKind.DIRECT_TRANSITION,
                source_id="unsupported-source",
            )
    assert _relation_counts(connection) == before
