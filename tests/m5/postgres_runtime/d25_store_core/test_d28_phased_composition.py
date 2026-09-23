"""M5-D28 cursor-local phase separation and authority-binding tests."""

from __future__ import annotations

import inspect
from copy import copy
from dataclasses import replace
from typing import Any

import pytest

from groundloop.errors import EventConflictError, ValidationError
from groundloop.m5.runtime.contracts import M5PersistedMatchingSourceKind
from groundloop.m5.runtime.postgres_matching import (
    _complete_reserved_direct_matching_transition,
    _expected_matching_stage_rows,
    _finalize_prepared_matching_transition,
    _MatchingPhase,
    _mint_direct_m4_stage_result,
    _prepare_matching_transition,
    _reserve_direct_matching_transition,
    _stage_prepared_matching_transition,
    apply_matching_transition,
)
from tests.m5.postgres_runtime import test_migration_017 as schema_fixture
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    apply_reserved_direct_stage_coordinate,
    authorize_and_derive_matching_transition,
    authorize_and_derive_requirement_completion,
    install_prepared_matching_headers,
    stage_reserved_direct_m4,
)


class _RecordingCursor:
    def __init__(self, cursor: Any) -> None:
        self.cursor = cursor
        self.statements: list[str] = []

    def execute(self, query: Any, params: Any = None) -> Any:
        self.statements.append(str(query))
        return self.cursor.execute(query, params)


def _derive_empty(cursor: Any, database: Any) -> Any:
    return authorize_and_derive_matching_transition(
        cursor,
        epoch_id=database.epoch_id,
        expected_runtime_revision=1,
        resulting_revision=1,
        source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
        source_id=database.event_id,
        expected_source_identity_hash=database.payload_hash,
    )


def _derive_document(cursor: Any, database: Any) -> Any:
    return authorize_and_derive_matching_transition(
        cursor,
        epoch_id=database.epoch_id,
        expected_runtime_revision=1,
        resulting_revision=1,
        source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
        source_id=database.event_id,
        expected_source_identity_hash=database.payload_hash,
    )


def _reserve_direct(cursor: Any, database: Any) -> Any:
    return _reserve_direct_matching_transition(
        cursor,
        epoch_id=database.epoch_id,
        expected_runtime_revision=database.expected_revision,
        resulting_revision=database.resulting_revision,
        proposed_source_id=database.proposed_source_id,
        job_id=database.job_id,
        attempt_id=database.attempt_id,
    )


def _reserve_and_stage_direct(cursor: Any, database: Any) -> tuple[Any, Any]:
    reservation = _reserve_direct(cursor, database)
    stage_result = stage_reserved_direct_m4(
        cursor,
        reservation,
        apply_reserved_direct_stage_coordinate,
    )
    return reservation, stage_result


def test_direct_source_completion_never_reenters_public_derivation_or_prepare() -> None:
    source = inspect.getsource(_complete_reserved_direct_matching_transition)
    assert "derive_matching_transition_intent(" not in source
    assert "_prepare_matching_transition(" not in source


def _retained_counts(connection: Any, epoch_id: int) -> tuple[int, int, int, int]:
    row = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_matching_image_working
            WHERE epoch_id=%s),
          (SELECT count(*) FROM groundloop_m5_matching_patch_artifact
            WHERE resulting_epoch_id=%s),
          (SELECT count(*) FROM groundloop_m5_matching_work_contribution
            WHERE epoch_id=%s),
          (SELECT count(*) FROM groundloop_m5_matching_work_accumulator
            WHERE epoch_id=%s)
        """,
        (epoch_id,) * 4,
    ).fetchone()
    assert row is not None
    return tuple(int(value) for value in row)  # type: ignore[return-value]


def _direct_retained_authority(
    connection: Any,
    *,
    epoch_id: int,
    source_id: str,
) -> tuple[int, int, object]:
    row = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_matching_patch_artifact
            WHERE resulting_epoch_id=%s AND source_kind='direct_transition'
              AND source_id=%s),
          (SELECT count(*) FROM groundloop_m5_matching_work_contribution
            WHERE epoch_id=%s AND source_kind='direct_transition'
              AND source_id=%s),
          (SELECT to_jsonb(accumulator)
             FROM groundloop_m5_matching_work_accumulator AS accumulator
            WHERE accumulator.epoch_id=%s)
        """,
        (epoch_id, source_id, epoch_id, source_id, epoch_id),
    ).fetchone()
    assert row is not None
    return int(row[0]), int(row[1]), row[2]


def _document_stage_counts(connection: Any, epoch_id: int) -> tuple[int, ...]:
    relations = (
        ("groundloop_m5_working_currency_history", "epoch_id"),
        ("groundloop_m5_matching_image_working", "epoch_id"),
        ("groundloop_m5_matching_observation_working", "epoch_id"),
        ("groundloop_m5_matching_edge_working", "epoch_id"),
        ("groundloop_m5_matching_hash_mask_working", "epoch_id"),
        ("groundloop_m5_matching_hall_working", "epoch_id"),
        ("groundloop_m5_working_requirement_state", "epoch_id"),
        ("groundloop_m5_working_group_state", "epoch_id"),
        ("groundloop_m5_working_claim_state", "epoch_id"),
        ("groundloop_m5_working_answer_state", "epoch_id"),
        ("groundloop_m5_working_group_certificate_binding", "epoch_id"),
        ("groundloop_m5_working_claim_certificate_binding", "epoch_id"),
        ("groundloop_m5_matching_patch_artifact", "resulting_epoch_id"),
        ("groundloop_m5_matching_work_contribution", "epoch_id"),
        ("groundloop_m5_matching_work_accumulator", "epoch_id"),
    )
    return tuple(
        int(
            connection.execute(
                f"SELECT count(*) FROM {relation} WHERE {key_column}=%s",
                (epoch_id,),
            ).fetchone()[0]
        )
        for relation, key_column in relations
    )


def test_public_apply_is_replay_only_and_rejects_absent_contribution(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    before = _retained_counts(connection, database.epoch_id)
    with connection.cursor() as cursor:
        intent = _derive_empty(cursor, database)
        with pytest.raises(
            EventConflictError,
            match="first application requires explicit prepare/stage/finalize",
        ):
            apply_matching_transition(cursor, intent)
    assert _retained_counts(connection, database.epoch_id) == before


def test_prepare_stage_finalize_have_disjoint_write_boundaries(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as raw_cursor:
        cursor = _RecordingCursor(raw_cursor)
        intent = _derive_empty(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        assert prepared.phase is _MatchingPhase.READY_TO_ADVANCE
        assert _retained_counts(connection, database.epoch_id) == (0, 0, 0, 0)

        staged = _stage_prepared_matching_transition(cursor, intent, prepared)
        assert staged is prepared
        assert staged.phase is _MatchingPhase.STAGED
        assert _retained_counts(connection, database.epoch_id) == (1, 0, 0, 0)
        stage_boundary = len(cursor.statements)

        receipt = _finalize_prepared_matching_transition(cursor, intent, staged)
        assert staged.phase is _MatchingPhase.CONSUMED
        finalizer_statements = cursor.statements[stage_boundary:]

    assert not receipt.exact_replay
    assert _retained_counts(connection, database.epoch_id) == (1, 1, 1, 1)
    assert any(
        "INSERT INTO groundloop_m5_matching_patch_artifact" in statement
        for statement in finalizer_statements
    )
    assert any(
        "INSERT INTO groundloop_m5_matching_work_contribution" in statement
        for statement in finalizer_statements
    )
    assert any(
        "INSERT INTO groundloop_m5_matching_work_accumulator" in statement
        for statement in finalizer_statements
    )
    assert not any(
        relation in statement
        for statement in finalizer_statements
        for relation in (
            "INSERT INTO groundloop_m5_matching_image_working",
            "INSERT INTO groundloop_m5_matching_observation_working",
            "INSERT INTO groundloop_m5_matching_edge_working",
            "INSERT INTO groundloop_m5_matching_hash_mask_working",
            "INSERT INTO groundloop_m5_matching_hall_working",
            "INSERT INTO groundloop_m5_working_requirement_state",
            "INSERT INTO groundloop_m5_working_group_state",
            "INSERT INTO groundloop_m5_working_claim_state",
            "INSERT INTO groundloop_m5_working_answer_state",
        )
    )
    assert not any(
        "UPDATE groundloop_epoch" in statement
        or "UPDATE groundloop_m5_runtime_epoch" in statement
        for statement in cursor.statements
    )


def test_requirement_source_and_phase_trace_obeys_the_d28_order(
    requirement_completion_database: Any,
) -> None:
    database = requirement_completion_database
    connection = database.connection
    with connection.cursor() as raw_cursor:
        cursor = _RecordingCursor(raw_cursor)
        intent, _ = authorize_and_derive_requirement_completion(cursor, database)
        derive_boundary = len(cursor.statements)
        prepared = _prepare_matching_transition(cursor, intent)
        prepare_boundary = len(cursor.statements)
        assert prepared.phase is _MatchingPhase.READY_TO_ADVANCE
        install_prepared_matching_headers(
            cursor, prepared, expected_revision=database.expected_revision
        )
        staged = _stage_prepared_matching_transition(cursor, intent, prepared)

    statements = cursor.statements

    def index_after(fragment: str, start: int = -1) -> int:
        return next(
            index
            for index, statement in enumerate(statements)
            if index > start and fragment in statement
        )

    def locked_index_after(fragment: str, start: int = -1) -> int:
        return next(
            index
            for index, statement in enumerate(statements)
            if index > start and fragment in statement and "FOR UPDATE" in statement
        )

    transition_authorizers = tuple(
        index
        for index, statement in enumerate(statements)
        if "groundloop_m5_authorize_persisted_matching_transition" in statement
    )
    assert len(transition_authorizers) == 1
    authorizer = transition_authorizers[0]
    job_lock = index_after("FROM groundloop_m5_semantic_job", authorizer)
    attempt_lock = index_after("FROM groundloop_m5_job_attempt", job_lock)
    result_lock = index_after(
        "FROM groundloop_m5_attempt_result_artifact", attempt_lock
    )
    pair_lock = index_after("FROM groundloop_m5_requirement_pair_input", result_lock)
    verifier_lock = index_after(
        "FROM groundloop_m5_requirement_verifier_artifact", pair_lock
    )
    observation_lock = index_after(
        "FROM groundloop_semantic_observation", verifier_lock
    )
    execution_lock = index_after(
        "FROM groundloop_m5_requirement_verifier_execution", observation_lock
    )
    assert all(
        "FOR UPDATE" in statements[index]
        for index in (
            job_lock,
            attempt_lock,
            result_lock,
            pair_lock,
            verifier_lock,
            observation_lock,
            execution_lock,
        )
    )

    tier_11a = locked_index_after(
        "FROM groundloop_semantic_observation", execution_lock
    )
    assert not any(
        "FOR UPDATE" in statement
        for statement in statements[execution_lock + 1 : tier_11a]
    )
    currency_lock = index_after("FROM groundloop_m5_working_currency_history", tier_11a)
    assert "FOR UPDATE" in statements[currency_lock]
    current_image = index_after(
        "FROM groundloop_m5_matching_image_current", currency_lock
    )
    working_image = index_after(
        "FROM groundloop_m5_matching_image_working", current_image
    )
    assert "FOR UPDATE" in statements[current_image]
    assert "FOR UPDATE" in statements[working_image]
    assert not any(
        "FOR UPDATE" in statement
        or "groundloop_m5_authorize_persisted_matching_transition" in statement
        for statement in statements[derive_boundary:prepare_boundary]
    )

    base_header = index_after("UPDATE groundloop_epoch", prepare_boundary - 1)
    runtime_header = index_after("UPDATE groundloop_m5_runtime_epoch", base_header)
    currency_dml = index_after(
        "INSERT INTO groundloop_m5_working_currency_history", runtime_header
    )
    matching_image_dml = index_after(
        "UPDATE groundloop_m5_matching_image_working", currency_dml
    )
    certificate_dml = index_after(
        "INSERT INTO groundloop_m5_group_certificate_artifact", matching_image_dml
    )
    assert (
        authorizer
        < job_lock
        < attempt_lock
        < result_lock
        < pair_lock
        < verifier_lock
        < observation_lock
        < execution_lock
        < tier_11a
        < currency_lock
        < current_image
        < working_image
        < base_header
        < runtime_header
        < currency_dml
        < matching_image_dml
        < certificate_dml
    )
    assert not any(
        "groundloop_working_observation_delta" in statement for statement in statements
    )
    assert staged.phase is _MatchingPhase.STAGED


def test_prepared_authority_rejects_another_cursor_object_before_stage(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_empty(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        with connection.cursor() as other_cursor:
            with pytest.raises(ValidationError, match="another cursor"):
                _stage_prepared_matching_transition(other_cursor, intent, prepared)
    assert _retained_counts(connection, database.epoch_id) == (0, 0, 0, 0)


def test_prepared_authority_rejects_a_copied_value_before_stage(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_empty(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        copied = copy(prepared)
        assert copied is not prepared
        with pytest.raises(ValidationError, match="copied or replaced"):
            _stage_prepared_matching_transition(cursor, intent, copied)
    assert _retained_counts(connection, database.epoch_id) == (0, 0, 0, 0)


def test_prepared_authority_rejects_changed_transition_context_before_stage(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_empty(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        cursor.execute(
            "SELECT set_config('groundloop.m5_matching_source_id', %s, true)",
            (f"{database.event_id}-changed",),
        )
        with pytest.raises(ValidationError, match="transition context changed"):
            _stage_prepared_matching_transition(cursor, intent, prepared)
    assert _retained_counts(connection, database.epoch_id) == (0, 0, 0, 0)


def test_prepared_phase_is_single_use_and_header_image_is_compare_only(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_empty(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        assert prepared.phase is _MatchingPhase.READY_TO_ADVANCE
        original_header = prepared.base_header_after_image
        prepared.base_header_after_image = replace(
            original_header, revision=original_header.revision + 1
        )
        with pytest.raises(EventConflictError, match="header after-image changed"):
            _stage_prepared_matching_transition(cursor, intent, prepared)
        prepared.base_header_after_image = original_header

        staged = _stage_prepared_matching_transition(cursor, intent, prepared)
        assert staged.phase is _MatchingPhase.STAGED
        with pytest.raises(EventConflictError, match="another phase"):
            _stage_prepared_matching_transition(cursor, intent, staged)
        _finalize_prepared_matching_transition(cursor, intent, staged)
        assert staged.phase is _MatchingPhase.CONSUMED
        with pytest.raises(EventConflictError, match="another phase"):
            _finalize_prepared_matching_transition(cursor, intent, staged)


@pytest.mark.parametrize(
    ("relation", "statement"),
    (
        (
            "groundloop_epoch",
            "UPDATE groundloop_epoch SET publication_mode='strict' WHERE epoch_id=%s",
        ),
        (
            "groundloop_m5_runtime_epoch",
            "UPDATE groundloop_m5_runtime_epoch "
            "SET open_work_count=open_work_count+1 WHERE epoch_id=%s",
        ),
    ),
    ids=("base-header", "runtime-header"),
)
def test_stage_revalidates_complete_installed_header_after_images_before_dml(
    empty_structural_database: Any,
    relation: str,
    statement: str,
) -> None:
    database = empty_structural_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_empty(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        with schema_fixture._d26_replica_trigger_window(connection):
            cursor.execute(statement, (database.epoch_id,))
        with pytest.raises(EventConflictError, match="header after-image changed"):
            _stage_prepared_matching_transition(cursor, intent, prepared)

    assert relation in statement
    assert _retained_counts(connection, database.epoch_id) == (0, 0, 0, 0)


def test_stage_revalidates_locked_physical_before_images_before_dml(
    rich_retirement_planning_database: Any,
) -> None:
    database = rich_retirement_planning_database
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
        prepared = _prepare_matching_transition(cursor, intent)
        with schema_fixture._d26_replica_trigger_window(connection):
            cursor.execute(
                """
                UPDATE groundloop_m5_matching_hall_current
                SET installed_revision=installed_revision+1
                WHERE group_version_id=%s
                """,
                (database.snapshot.complete_group_id,),
            )
        with pytest.raises(EventConflictError, match="changed"):
            _stage_prepared_matching_transition(cursor, intent, prepared)

    assert _retained_counts(connection, database.epoch_id) == (0, 0, 0, 0)


def test_stage_rejects_a_mutated_complete_write_plan_before_dml(
    rich_registration_planning_database: Any,
) -> None:
    database = rich_registration_planning_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_empty(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        plan = prepared.physical_and_logical_write_plan
        assert plan.hall_rows
        prepared.physical_and_logical_write_plan = replace(plan, hall_rows=())
        with pytest.raises(EventConflictError, match="write plan changed"):
            _stage_prepared_matching_transition(cursor, intent, prepared)

    assert _retained_counts(connection, database.epoch_id) == (0, 0, 0, 0)


def test_stage_rejects_a_reordered_complete_write_plan_before_dml(
    rich_retirement_planning_database: Any,
) -> None:
    database = rich_retirement_planning_database
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
        prepared = _prepare_matching_transition(cursor, intent)
        plan = prepared.physical_and_logical_write_plan
        assert len(plan.observation_rows) > 1
        prepared.physical_and_logical_write_plan = replace(
            plan,
            observation_rows=tuple(reversed(plan.observation_rows)),
        )
        with pytest.raises(EventConflictError, match="write plan changed"):
            _stage_prepared_matching_transition(cursor, intent, prepared)

    assert _retained_counts(connection, database.epoch_id) == (0, 0, 0, 0)


def test_finalizer_rejects_changed_stage_journal_bytes_before_tier_15i(
    rich_registration_planning_database: Any,
) -> None:
    database = rich_registration_planning_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_empty(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        staged = _stage_prepared_matching_transition(cursor, intent, prepared)
        changed = cursor.execute(
            """
            UPDATE pg_temp.groundloop_m5_matching_change_journal
            SET final_new='{}'::jsonb
            WHERE ctid=(
              SELECT ctid
              FROM pg_temp.groundloop_m5_matching_change_journal
              ORDER BY relation_name COLLATE "C", key_preimage
              LIMIT 1
            )
            """
        ).rowcount
        assert changed == 1
        with pytest.raises(
            EventConflictError, match="journal operation or after-image differs"
        ):
            _finalize_prepared_matching_transition(cursor, intent, staged)

    assert _retained_counts(connection, database.epoch_id)[1:] == (0, 0, 0)


@pytest.mark.parametrize(
    "cut",
    ("authorized", "prepared", "staged", "finalized"),
)
def test_structural_phase_crash_cut_rolls_back_the_complete_outer_transaction(
    rich_registration_planning_database: Any,
    cut: str,
) -> None:
    database = rich_registration_planning_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_empty(cursor, database)
        if cut != "authorized":
            prepared = _prepare_matching_transition(cursor, intent)
            if cut in {"staged", "finalized"}:
                staged = _stage_prepared_matching_transition(cursor, intent, prepared)
                if cut == "finalized":
                    _finalize_prepared_matching_transition(cursor, intent, staged)

    connection.rollback()
    assert connection.execute(
        "SELECT count(*) FROM groundloop_epoch WHERE epoch_id=%s",
        (database.epoch_id,),
    ).fetchone() == (0,)
    assert _retained_counts(connection, database.epoch_id) == (0, 0, 0, 0)


def test_document_stage_writes_exact_currency_lower_tiers_and_journal(
    document_withdrawal_planning_database: Any,
) -> None:
    database = document_withdrawal_planning_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_document(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        plan = prepared.physical_and_logical_write_plan
        staged = _stage_prepared_matching_transition(cursor, intent, prepared)
        assert staged.phase is _MatchingPhase.STAGED

        currency = tuple(
            tuple(row)
            for row in cursor.execute(
                """
                SELECT subject_id, chunk_version_id, task_type, observation_id,
                       valid_from_revision, valid_to_revision
                FROM groundloop_m5_working_currency_history
                WHERE epoch_id=%s
                ORDER BY subject_id COLLATE "C", chunk_version_id COLLATE "C",
                         task_type COLLATE "C", valid_from_revision
                """,
                (database.epoch_id,),
            ).fetchall()
        )
        assert currency == tuple(
            sorted(
                (
                    row.subject_id,
                    row.chunk_version_id,
                    row.task_type,
                    None,
                    1,
                    None,
                )
                for row in plan.observation_currency_rows
            )
        )
        assert any(
            row[0] == database.noncanonical_requirement_id
            and row[1] == database.noncanonical_chunk_id
            and row[2] == "document_matching_noncanonical_v1"
            for row in currency
        )

        expected_journal = _expected_matching_stage_rows(cursor, prepared)
        actual_journal = cursor.execute(
            """
            SELECT relation_name, key_preimage, first_old, final_new,
                   first_operation, last_operation, mutation_count,
                   saw_insert, saw_update, saw_delete
            FROM pg_temp.groundloop_m5_matching_change_journal
            ORDER BY relation_name COLLATE "C", key_preimage
            """
        ).fetchall()
        assert {
            (str(row[0]), bytes(row[1])): row[3] for row in actual_journal
        } == expected_journal
        assert all(
            row[2] is None
            and row[4:7] == ("INSERT", "INSERT", 1)
            and row[7:10] == (True, False, False)
            for row in actual_journal
        )
        journal_counts = {
            str(relation): int(count)
            for relation, count in cursor.execute(
                """
                SELECT relation_name, count(*)
                FROM pg_temp.groundloop_m5_matching_change_journal
                GROUP BY relation_name
                """
            ).fetchall()
        }
        assert journal_counts == {
            "groundloop_m5_claim_certificate_artifact": len(
                plan.claim_certificate_artifact_rows
            ),
            "groundloop_m5_group_certificate_artifact": len(
                plan.group_certificate_artifact_rows
            ),
            "groundloop_m5_group_certificate_artifact_row": sum(
                artifact.requirement_count
                for artifact in plan.group_certificate_artifact_rows
            ),
            "groundloop_m5_matching_edge_working": len(plan.edge_rows),
            "groundloop_m5_matching_hall_working": len(plan.hall_rows),
            "groundloop_m5_matching_hash_mask_working": len(plan.mask_rows),
            "groundloop_m5_matching_image_working": 1,
            "groundloop_m5_matching_observation_working": len(plan.observation_rows),
            "groundloop_m5_working_answer_state": len(plan.answer_state_rows),
            "groundloop_m5_working_claim_certificate_binding": len(
                plan.claim_binding_rows
            ),
            "groundloop_m5_working_claim_state": len(plan.claim_state_rows),
            "groundloop_m5_working_group_certificate_binding": len(
                plan.group_binding_rows
            ),
            "groundloop_m5_working_group_state": len(plan.group_state_rows),
            "groundloop_m5_working_requirement_state": len(plan.requirement_state_rows),
        }

        receipt = _finalize_prepared_matching_transition(cursor, intent, staged)

    assert not receipt.exact_replay
    assert _document_stage_counts(connection, database.epoch_id) == (
        len(plan.observation_currency_rows),
        1,
        len(plan.observation_rows),
        len(plan.edge_rows),
        len(plan.mask_rows),
        len(plan.hall_rows),
        len(plan.requirement_state_rows),
        len(plan.group_state_rows),
        len(plan.claim_state_rows),
        len(plan.answer_state_rows),
        len(plan.group_binding_rows),
        len(plan.claim_binding_rows),
        1,
        1,
        1,
    )


def test_document_stage_revalidates_each_held_currency_before_first_d25_dml(
    document_withdrawal_planning_database: Any,
) -> None:
    database = document_withdrawal_planning_database
    connection = database.connection
    before = _document_stage_counts(connection, database.epoch_id)
    assert before == (0,) * len(before)
    with connection.cursor() as cursor:
        intent = _derive_document(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        with schema_fixture._d26_replica_trigger_window(connection):
            changed = cursor.execute(
                """
                UPDATE groundloop_observation_currency
                SET installed_revision=installed_revision+1
                WHERE subject_kind='requirement' AND subject_id=%s
                  AND chunk_version_id=%s
                  AND task_type='document_matching_noncanonical_v1'
                """,
                (
                    database.noncanonical_requirement_id,
                    database.noncanonical_chunk_id,
                ),
            ).rowcount
        assert changed == 1
        with pytest.raises(EventConflictError, match="currency changed"):
            _stage_prepared_matching_transition(cursor, intent, prepared)

    assert _document_stage_counts(connection, database.epoch_id) == before


def test_document_journal_tamper_rolls_back_currency_and_every_lower_tier(
    document_withdrawal_planning_database: Any,
) -> None:
    database = document_withdrawal_planning_database
    connection = database.connection
    before = _document_stage_counts(connection, database.epoch_id)
    assert before == (0,) * len(before)

    with pytest.raises(
        EventConflictError,
        match="matching stage journal operation or after-image differs",
    ):
        with connection.transaction(), connection.cursor() as cursor:
            intent = _derive_document(cursor, database)
            prepared = _prepare_matching_transition(cursor, intent)
            staged = _stage_prepared_matching_transition(cursor, intent, prepared)
            changed = cursor.execute(
                """
                UPDATE pg_temp.groundloop_m5_matching_change_journal
                SET final_new='{}'::jsonb
                WHERE relation_name='groundloop_m5_matching_hall_working'
                  AND ctid=(
                    SELECT ctid
                    FROM pg_temp.groundloop_m5_matching_change_journal
                    WHERE relation_name='groundloop_m5_matching_hall_working'
                    ORDER BY key_preimage LIMIT 1
                  )
                """
            ).rowcount
            assert changed == 1
            _finalize_prepared_matching_transition(cursor, intent, staged)

    assert _document_stage_counts(connection, database.epoch_id) == before


def test_direct_reserve_real_stage_and_complete_returns_ready_authority_without_d25_dml(
    direct_seam_database: Any,
) -> None:
    database = direct_seam_database
    connection = database.connection
    with connection.cursor() as raw_cursor:
        cursor = _RecordingCursor(raw_cursor)
        reservation = _reserve_direct_matching_transition(
            cursor,
            epoch_id=database.epoch_id,
            expected_runtime_revision=database.expected_revision,
            resulting_revision=database.resulting_revision,
            proposed_source_id=database.proposed_source_id,
            job_id=database.job_id,
            attempt_id=database.attempt_id,
        )
        assert reservation.phase == "reserved"
        assert reservation.precursor.source_id == database.proposed_source_id
        assert (
            tuple(image.coordinate for image in reservation.before_images)
            == reservation.stage_coordinates
        )

        stage_result = stage_reserved_direct_m4(
            cursor,
            reservation,
            apply_reserved_direct_stage_coordinate,
        )
        assert stage_result.phase == "staged"
        assert (
            tuple(record.coordinate for record in stage_result.mutation_records)
            == reservation.stage_coordinates
        )
        assert all(
            record.statement_rowcount == 1 and record.first_old != record.final_new
            for record in stage_result.mutation_records
        )
        before_completion_counts = _document_stage_counts(connection, database.epoch_id)
        completion_boundary = len(cursor.statements)

        intent, prepared = _complete_reserved_direct_matching_transition(
            cursor, reservation, stage_result
        )
        completion_statements = cursor.statements[completion_boundary:]

    assert (
        _document_stage_counts(connection, database.epoch_id)
        == before_completion_counts
    )
    d25_relations = (
        "groundloop_m5_working_currency_history",
        "groundloop_m5_matching_image_working",
        "groundloop_m5_matching_observation_working",
        "groundloop_m5_matching_edge_working",
        "groundloop_m5_matching_hash_mask_working",
        "groundloop_m5_matching_hall_working",
        "groundloop_m5_working_requirement_state",
        "groundloop_m5_working_group_state",
        "groundloop_m5_working_claim_state",
        "groundloop_m5_working_answer_state",
        "groundloop_m5_working_group_certificate_binding",
        "groundloop_m5_working_claim_certificate_binding",
        "groundloop_m5_matching_patch_artifact",
        "groundloop_m5_matching_work_contribution",
        "groundloop_m5_matching_work_accumulator",
    )
    assert not any(
        operation in statement and relation in statement
        for statement in completion_statements
        for operation in ("INSERT INTO", "UPDATE", "DELETE FROM")
        for relation in d25_relations
    )
    assert reservation.phase == "consumed"
    assert stage_result.phase == "consumed"
    assert prepared.phase is _MatchingPhase.READY_TO_ADVANCE
    assert prepared.official_intent is intent
    assert prepared.direct_reservation_or_none is reservation
    assert prepared.direct_m4_stage_evidence_or_none is stage_result
    assert intent.source_id == database.proposed_source_id
    assert intent.before_revision == database.expected_revision
    assert intent.resulting_revision == database.resulting_revision

    plan = prepared.physical_and_logical_write_plan
    if database.job_kind == "verify_pair":
        assert intent.claim_state_ids == (database.claim_id,)
        assert intent.answer_state_ids == (database.answer_version_id,)
        assert intent.claim_certificate_ids == (database.claim_id,)
        assert plan.claim_state_rows
        assert plan.answer_state_rows
        assert plan.claim_certificate_artifact_rows
        assert plan.claim_binding_rows
    else:
        assert not any(
            (
                intent.group_shapes,
                intent.observation_ids,
                intent.edge_keys,
                intent.mask_keys,
                intent.hall_group_ids,
                intent.requirement_state_ids,
                intent.group_state_ids,
                intent.claim_state_ids,
                intent.answer_state_ids,
                intent.group_certificate_ids,
                intent.claim_certificate_ids,
                plan.observation_currency_rows,
                plan.observation_rows,
                plan.edge_rows,
                plan.mask_rows,
                plan.hall_rows,
                plan.requirement_state_rows,
                plan.group_state_rows,
                plan.claim_state_rows,
                plan.answer_state_rows,
                plan.group_certificate_artifact_rows,
                plan.claim_certificate_artifact_rows,
                plan.group_binding_rows,
                plan.claim_binding_rows,
            )
        )


def test_direct_reservation_rejects_copy_and_cross_cursor(
    direct_seam_database: Any,
) -> None:
    database = direct_seam_database
    connection = database.connection
    with connection.cursor() as cursor:
        reservation = _reserve_direct(cursor, database)
        copied = copy(reservation)
        with pytest.raises(ValidationError, match="reservation was copied"):
            _mint_direct_m4_stage_result(cursor, copied, ())
        with connection.cursor() as other_cursor:
            with pytest.raises(ValidationError, match="belongs to another cursor"):
                _mint_direct_m4_stage_result(other_cursor, reservation, ())
        assert reservation.phase == "reserved"


def test_direct_reservation_rejects_wrong_proposed_source(
    direct_seam_database: Any,
) -> None:
    database = direct_seam_database
    assert database.proposed_source_id != "0" * 64
    with database.connection.cursor() as cursor:
        with pytest.raises(EventConflictError, match="proposed source differs"):
            _reserve_direct_matching_transition(
                cursor,
                epoch_id=database.epoch_id,
                expected_runtime_revision=database.expected_revision,
                resulting_revision=database.resulting_revision,
                proposed_source_id="0" * 64,
                job_id=database.job_id,
                attempt_id=database.attempt_id,
            )
    job = database.connection.execute(
        "SELECT job_state,completion_digest FROM groundloop_semantic_job "
        "WHERE job_id=%s",
        (database.job_id,),
    ).fetchone()
    attempt = database.connection.execute(
        "SELECT attempt_state,finished_at FROM groundloop_semantic_job_attempt "
        "WHERE attempt_id=%s",
        (database.attempt_id,),
    ).fetchone()
    assert job == ("running", None)
    assert attempt == ("leased", None)


def test_direct_stage_evidence_rejects_copy_and_is_single_use(
    direct_seam_database: Any,
) -> None:
    database = direct_seam_database
    with database.connection.cursor() as cursor:
        reservation, stage_result = _reserve_and_stage_direct(cursor, database)
        copied = copy(stage_result)
        with pytest.raises(ValidationError, match="stage evidence was copied"):
            _complete_reserved_direct_matching_transition(cursor, reservation, copied)
        with database.connection.cursor() as other_cursor:
            with pytest.raises(ValidationError, match="belongs to another cursor"):
                _complete_reserved_direct_matching_transition(
                    other_cursor, reservation, stage_result
                )
        intent, prepared = _complete_reserved_direct_matching_transition(
            cursor, reservation, stage_result
        )
        with pytest.raises(EventConflictError, match="evidence is in another phase"):
            _complete_reserved_direct_matching_transition(
                cursor, reservation, stage_result
            )
    assert intent.source_id == database.proposed_source_id
    assert prepared.phase is _MatchingPhase.READY_TO_ADVANCE
    assert reservation.phase == "consumed"
    assert stage_result.phase == "consumed"


@pytest.mark.parametrize("corruption", ("projection", "remainder"))
def test_direct_completion_rejects_changed_reserved_authority(
    direct_seam_database: Any,
    corruption: str,
) -> None:
    database = direct_seam_database
    with database.connection.cursor() as cursor:
        reservation, stage_result = _reserve_and_stage_direct(cursor, database)
        if corruption == "projection":
            object.__setattr__(
                reservation,
                "d25_projection",
                replace(
                    reservation.d25_projection,
                    claim_state_ids=("corrupt-claim",),
                ),
            )
            expected = "official projection differs"
        else:
            object.__setattr__(
                reservation,
                "remainder_coordinates",
                reservation.remainder_coordinates[:-1],
            )
            expected = "official remainder changed"
        with pytest.raises(EventConflictError, match=expected):
            _complete_reserved_direct_matching_transition(
                cursor, reservation, stage_result
            )
    assert reservation.phase == "reserved"
    assert stage_result.phase == "staged"


@pytest.mark.parametrize("corruption", ("rowcount", "after_image"))
def test_direct_stage_evidence_rejects_mutated_records(
    direct_seam_database: Any,
    corruption: str,
) -> None:
    database = direct_seam_database
    with database.connection.cursor() as cursor:
        reservation, stage_result = _reserve_and_stage_direct(cursor, database)
        first = stage_result.mutation_records[0]
        if corruption == "rowcount":
            changed_first = replace(first, statement_rowcount=2)
        else:
            changed_first = replace(first, final_new=first.first_old)
        changed = replace(
            stage_result,
            mutation_records=(changed_first, *stage_result.mutation_records[1:]),
        )
        object.__setattr__(changed, "stage_result_identity", id(changed))
        with pytest.raises(EventConflictError, match="direct M4"):
            _complete_reserved_direct_matching_transition(cursor, reservation, changed)

        intent, prepared = _complete_reserved_direct_matching_transition(
            cursor, reservation, stage_result
        )
    assert intent.source_id == database.proposed_source_id
    assert prepared.phase is _MatchingPhase.READY_TO_ADVANCE


@pytest.mark.parametrize("direct_seam_database", ("verify_pair",), indirect=True)
@pytest.mark.parametrize("corruption", ("artifact", "plan"))
def test_direct_finalizer_revalidates_complete_prepared_authority_before_tier_15i(
    direct_seam_database: Any,
    corruption: str,
) -> None:
    database = direct_seam_database
    connection = database.connection
    with connection.cursor() as raw_cursor:
        cursor = _RecordingCursor(raw_cursor)
        reservation, stage_result = _reserve_and_stage_direct(cursor, database)
        intent, prepared = _complete_reserved_direct_matching_transition(
            cursor, reservation, stage_result
        )
        install_prepared_matching_headers(
            cursor,
            prepared,
            expected_revision=database.expected_revision,
        )
        staged = _stage_prepared_matching_transition(cursor, intent, prepared)
        before_counts = _retained_counts(connection, database.epoch_id)
        before_authority = _direct_retained_authority(
            connection,
            epoch_id=database.epoch_id,
            source_id=database.proposed_source_id,
        )
        assert before_authority[:2] == (0, 0)

        if corruption == "artifact":
            changed_artifact = copy(staged.prewrite_patch_artifact)
            object.__setattr__(
                changed_artifact,
                "patch_preimage",
                changed_artifact.patch_preimage + b"corrupt",
            )
            staged.prewrite_patch_artifact = changed_artifact
            expected = "prepared patch"
        else:
            plan = staged.physical_and_logical_write_plan
            assert plan.claim_state_rows
            staged.physical_and_logical_write_plan = replace(
                plan,
                claim_state_rows=(),
            )
            expected = "write plan"

        boundary = len(cursor.statements)
        with pytest.raises(EventConflictError, match=expected):
            _finalize_prepared_matching_transition(cursor, intent, staged)
        finalizer_statements = cursor.statements[boundary:]

    assert _retained_counts(connection, database.epoch_id) == before_counts
    assert (
        _direct_retained_authority(
            connection,
            epoch_id=database.epoch_id,
            source_id=database.proposed_source_id,
        )
        == before_authority
    )
    assert not any(
        operation in statement and relation in statement
        for statement in finalizer_statements
        for operation in ("INSERT INTO", "UPDATE", "DELETE FROM")
        for relation in (
            "groundloop_m5_matching_patch_artifact",
            "groundloop_m5_matching_work_contribution",
            "groundloop_m5_matching_work_accumulator",
        )
    )
    assert staged.phase is _MatchingPhase.STAGED
