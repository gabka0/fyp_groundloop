"""Nonempty store-owned D25 structural planning and materialization."""

from __future__ import annotations

from typing import Any

import pytest

from groundloop.errors import EventConflictError
from groundloop.m5.domain import GroupMatchingCertificateArtifact
from groundloop.m5.runtime.contracts import (
    M5PersistedBindingKind,
    M5PersistedLogicalChangeKind,
    M5PersistedMatchingSourceKind,
)
from groundloop.m5.runtime.postgres_matching import (
    _expected_matching_stage_rows,
    _finalize_prepared_matching_transition,
    _prepare_matching_transition,
    _stage_prepared_matching_transition,
    apply_matching_transition,
)
from tests.m5.postgres.helpers import sha
from tests.m5.postgres_runtime import test_migration_017 as schema_fixture
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    authorize_and_derive_matching_transition,
    authorize_and_derive_requirement_completion,
    install_prepared_matching_headers,
    retained_matching_replay_intent,
    terminalize_requirement_completion_source,
)


def _derive_retirement(cursor: Any, database: Any) -> Any:
    return authorize_and_derive_matching_transition(
        cursor,
        epoch_id=database.epoch_id,
        expected_runtime_revision=1,
        resulting_revision=1,
        source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
        source_id=database.event_id,
        expected_source_identity_hash=database.payload_hash,
    )


def _derive_document_withdrawal(cursor: Any, database: Any) -> Any:
    return authorize_and_derive_matching_transition(
        cursor,
        epoch_id=database.epoch_id,
        expected_runtime_revision=1,
        resulting_revision=1,
        source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
        source_id=database.event_id,
        expected_source_identity_hash=database.payload_hash,
    )


def _logical_change_keys(
    prepared: Any,
) -> dict[tuple[Any, str], tuple[str | None, str | None]]:
    return {
        (change.kind, change.object_id): (change.before_hash, change.after_hash)
        for change in prepared.prewrite_patch_artifact.logical_patch.changes
    }


def _d25_row_counts(connection: Any, epoch_id: int) -> tuple[int, ...]:
    return tuple(
        int(row[0])
        for row in (
            connection.execute(
                "SELECT count(*) FROM groundloop_m5_matching_image_working "
                "WHERE epoch_id=%s",
                (epoch_id,),
            ).fetchone(),
            connection.execute(
                "SELECT count(*) FROM groundloop_m5_matching_observation_working "
                "WHERE epoch_id=%s",
                (epoch_id,),
            ).fetchone(),
            connection.execute(
                "SELECT count(*) FROM groundloop_m5_matching_edge_working "
                "WHERE epoch_id=%s",
                (epoch_id,),
            ).fetchone(),
            connection.execute(
                "SELECT count(*) FROM groundloop_m5_matching_hash_mask_working "
                "WHERE epoch_id=%s",
                (epoch_id,),
            ).fetchone(),
            connection.execute(
                "SELECT count(*) FROM groundloop_m5_matching_hall_working "
                "WHERE epoch_id=%s",
                (epoch_id,),
            ).fetchone(),
            connection.execute(
                "SELECT count(*) FROM groundloop_m5_matching_work_contribution "
                "WHERE epoch_id=%s",
                (epoch_id,),
            ).fetchone(),
            connection.execute(
                "SELECT count(*) FROM groundloop_m5_matching_work_accumulator "
                "WHERE epoch_id=%s",
                (epoch_id,),
            ).fetchone(),
        )
    )


def _derive_requirement_and_prepare(cursor: Any, database: Any) -> tuple[Any, Any, Any]:
    intent, result = authorize_and_derive_requirement_completion(cursor, database)
    prepared = _prepare_matching_transition(cursor, intent)
    return intent, result, prepared


def _requirement_d25_footprint(connection: Any, database: Any) -> tuple[Any, ...]:
    row = connection.execute(
        """
        SELECT epoch.revision, runtime.revision, image.updated_revision,
          (SELECT count(*) FROM groundloop_m5_working_currency_history
           WHERE epoch_id=%s AND valid_from_revision=%s
             AND valid_to_revision IS NULL),
          (SELECT count(*)
           FROM groundloop_m5_working_group_certificate_binding
           WHERE epoch_id=%s AND valid_from_revision=%s),
          (SELECT count(*) FROM groundloop_m5_matching_work_contribution
           WHERE epoch_id=%s AND source_kind='requirement_completion'
             AND source_id=%s),
          (SELECT count(*) FROM groundloop_m5_matching_patch_artifact
           WHERE resulting_epoch_id=%s AND source_kind='requirement_completion'
             AND source_id=%s)
        FROM groundloop_epoch AS epoch
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_matching_image_working AS image USING (epoch_id)
        WHERE epoch.epoch_id=%s
        """,
        (
            database.epoch_id,
            database.resulting_revision,
            database.epoch_id,
            database.resulting_revision,
            database.epoch_id,
            database.attempt_id,
            database.epoch_id,
            database.attempt_id,
            database.epoch_id,
        ),
    ).fetchone()
    assert row is not None
    return tuple(row)


def _persist_group_certificate_fixture(
    connection: Any,
    artifact: GroupMatchingCertificateArtifact,
    *,
    mode: str,
) -> None:
    """Install exact or deliberately colliding immutable bytes outside D25."""

    if mode not in {"exact", "header_collision", "missing_child", "changed_child"}:
        raise AssertionError(f"unsupported certificate fixture mode: {mode}")
    requirement_count = (
        artifact.requirement_count + 1
        if mode == "header_collision"
        else artifact.requirement_count
    )
    with connection.transaction():
        with schema_fixture._d26_replica_trigger_window(connection):
            connection.execute(
                """
                INSERT INTO groundloop_m5_group_certificate_artifact (
                  certificate_digest, decision_policy_version,
                  certificate_version, group_version_id, requirement_count
                ) VALUES (%s,%s,%s,%s,%s)
                """,
                (
                    artifact.certificate_digest,
                    artifact.decision_policy_version,
                    artifact.certificate_version,
                    artifact.group_version_id,
                    requirement_count,
                ),
            )
            if mode == "missing_child":
                return
            for row in artifact.rows:
                connection.execute(
                    """
                    INSERT INTO groundloop_m5_group_certificate_artifact_row (
                      certificate_digest, requirement_ordinal,
                      requirement_version_id, text_hash,
                      selected_observation_id
                    ) VALUES (%s,%s,%s,%s,%s)
                    """,
                    (
                        artifact.certificate_digest,
                        row.requirement_ordinal,
                        row.requirement_version_id,
                        (
                            sha("d25-changed-certificate-child")
                            if mode == "changed_child" and row.requirement_ordinal == 0
                            else row.text_hash
                        ),
                        row.selected_observation_id,
                    ),
                )


def test_retirement_derives_complete_nonempty_intent_and_prewrite_plan(
    rich_retirement_planning_database: Any,
) -> None:
    database = rich_retirement_planning_database
    connection = database.connection
    before = _d25_row_counts(connection, database.epoch_id)
    assert before == (0,) * 7

    with connection.cursor() as cursor:
        intent = _derive_retirement(cursor, database)
        group_id = database.snapshot.complete_group_id
        assert tuple(shape.group_version_id for shape in intent.group_shapes) == (
            group_id,
        )
        assert intent.observation_ids
        assert intent.edge_keys
        assert intent.mask_keys
        assert intent.hall_group_ids == (group_id,)
        assert intent.requirement_state_ids == tuple(
            sorted(
                requirement_id
                for shape in intent.group_shapes
                for _, requirement_id in shape.requirements
            )
        )
        assert intent.group_state_ids == (group_id,)
        owner_group_ids = tuple(
            str(value)
            for value in cursor.execute(
                """
                SELECT complete_group_ids
                FROM groundloop_m5_published_claim_state
                WHERE claim_id=%s AND valid_from_epoch <= %s
                  AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
                """,
                (
                    intent.claim_state_ids[0],
                    intent.before_epoch_id,
                    intent.before_epoch_id,
                ),
            ).fetchone()[0]
        )
        alternate_group_id = next(
            value for value in owner_group_ids if value != group_id
        )
        assert intent.group_certificate_ids == tuple(
            sorted((group_id, alternate_group_id))
        )
        assert intent.claim_state_ids
        assert intent.answer_state_ids
        assert intent.claim_certificate_ids == intent.claim_state_ids
        assert _d25_row_counts(connection, database.epoch_id) == before

        prepared = _prepare_matching_transition(cursor, intent)
        plan = prepared.physical_and_logical_write_plan
        logical = prepared.prewrite_patch_artifact.logical_patch
        absent = {
            (change.kind, change.object_id, change.before_hash)
            for change in logical.changes
            if change.after_hash is None
        }
        assert all(before_hash is not None for _, _, before_hash in absent)
        assert {(kind, object_id) for kind, object_id, _ in absent} == {
            *(
                (M5PersistedLogicalChangeKind.REQUIREMENT_STATE, object_id)
                for object_id in intent.requirement_state_ids
            ),
            (M5PersistedLogicalChangeKind.GROUP_STATE, group_id),
            (M5PersistedLogicalChangeKind.GROUP_CERTIFICATE, group_id),
        }
        assert len(plan.observation_rows) == len(intent.observation_ids)
        assert len(plan.edge_rows) == len(intent.edge_keys)
        assert len(plan.mask_rows) == len(intent.mask_keys)
        assert len(plan.hall_rows) == len(intent.hall_group_ids)
        assert plan.requirement_state_rows == ()
        assert plan.group_state_rows == ()
        assert plan.group_certificate_artifact_rows == ()
        assert plan.group_binding_rows == ()
        assert prepared.requirement_state_write_count_diagnostic == 0
        assert cursor.execute(
            """
            SELECT action, successor_group_version_id, event_id
            FROM groundloop_m5_group_deactivation
            WHERE epoch_id=%s AND group_version_id=%s
            """,
            (database.epoch_id, group_id),
        ).fetchone() == ("RETIRE", None, database.event_id)
        assert _d25_row_counts(connection, database.epoch_id) == before


def test_retirement_retains_alternate_group_certificate_only_as_a_noop_lock_key(
    rich_retirement_planning_database: Any,
) -> None:
    database = rich_retirement_planning_database
    connection = database.connection
    group_id = database.snapshot.complete_group_id
    with connection.cursor() as cursor:
        intent = _derive_retirement(cursor, database)
        assert group_id in intent.group_certificate_ids
        alternate_group_id = next(
            value for value in intent.group_certificate_ids if value != group_id
        )
        prepared = _prepare_matching_transition(cursor, intent)
        group_certificate_changes = {
            change.object_id
            for change in prepared.prewrite_patch_artifact.logical_patch.changes
            if change.kind is M5PersistedLogicalChangeKind.GROUP_CERTIFICATE
        }
        assert group_certificate_changes == {group_id}
        assert alternate_group_id not in group_certificate_changes
        assert prepared.physical_and_logical_write_plan.group_binding_rows == ()
        staged = _stage_prepared_matching_transition(cursor, intent, prepared)
        _finalize_prepared_matching_transition(cursor, intent, staged)
        replay_intent = retained_matching_replay_intent(
            cursor,
            epoch_id=database.epoch_id,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=database.event_id,
        )

    assert group_id in replay_intent.group_certificate_ids
    assert alternate_group_id not in replay_intent.group_certificate_ids


def test_retirement_stage_writes_lower_tiers_before_d25_finalizer(
    rich_retirement_planning_database: Any,
) -> None:
    database = rich_retirement_planning_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_retirement(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        plan = prepared.physical_and_logical_write_plan
        staged = _stage_prepared_matching_transition(cursor, intent, prepared)

        expected_journal = _expected_matching_stage_rows(cursor, prepared)
        journal_rows = cursor.execute(
            """
            SELECT relation_name, key_preimage, first_old, final_new,
                   first_operation, last_operation, mutation_count,
                   saw_insert, saw_update, saw_delete
            FROM pg_temp.groundloop_m5_matching_change_journal
            ORDER BY relation_name COLLATE "C", key_preimage
            """
        ).fetchall()
        assert {
            (str(row[0]), bytes(row[1])): row[3] for row in journal_rows
        } == expected_journal
        assert all(
            row[2] is None
            and row[4:7] == ("INSERT", "INSERT", 1)
            and row[7:10] == (True, False, False)
            for row in journal_rows
        )

        staged_counts = _d25_row_counts(connection, database.epoch_id)
        assert staged_counts == (
            1,
            len(plan.observation_rows),
            len(plan.edge_rows),
            len(plan.mask_rows),
            len(plan.hall_rows),
            0,
            0,
        )
        assert connection.execute(
            """
            SELECT
              (SELECT count(*) FROM groundloop_m5_working_requirement_state
               WHERE epoch_id=%s),
              (SELECT count(*) FROM groundloop_m5_working_group_state
               WHERE epoch_id=%s),
              (SELECT count(*)
               FROM groundloop_m5_working_group_certificate_binding
               WHERE epoch_id=%s)
            """,
            (database.epoch_id,) * 3,
        ).fetchone() == (0, 0, 0)
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_matching_patch_artifact "
            "WHERE patch_digest=%s",
            (prepared.prewrite_patch_artifact.patch.patch_digest,),
        ).fetchone() == (0,)

        receipt = _finalize_prepared_matching_transition(cursor, intent, staged)

    assert not receipt.exact_replay
    assert receipt.patch.patch_digest == (
        prepared.prewrite_patch_artifact.patch.patch_digest
    )
    assert _d25_row_counts(connection, database.epoch_id)[-2:] == (1, 1)


def test_replacement_materializes_absent_predecessor_and_present_successor(
    rich_replacement_planning_database: Any,
) -> None:
    database = rich_replacement_planning_database
    connection = database.connection
    predecessor_id = database.snapshot.partial_group_id
    with connection.cursor() as cursor:
        intent = _derive_retirement(cursor, database)
        deactivation = cursor.execute(
            """
            SELECT action, successor_group_version_id
            FROM groundloop_m5_group_deactivation
            WHERE epoch_id=%s AND group_version_id=%s
            """,
            (database.epoch_id, predecessor_id),
        ).fetchone()
        assert deactivation is not None
        assert deactivation[0] == "REPLACE"
        successor_id = str(deactivation[1])
        assert tuple(shape.group_version_id for shape in intent.group_shapes) == tuple(
            sorted((predecessor_id, successor_id))
        )
        prepared = _prepare_matching_transition(cursor, intent)
        plan = prepared.physical_and_logical_write_plan
        changes = _logical_change_keys(prepared)
        predecessor_shape = next(
            shape
            for shape in intent.group_shapes
            if shape.group_version_id == predecessor_id
        )
        successor_shape = next(
            shape
            for shape in intent.group_shapes
            if shape.group_version_id == successor_id
        )

        predecessor_state_keys = {
            *(
                (M5PersistedLogicalChangeKind.REQUIREMENT_STATE, requirement_id)
                for _, requirement_id in predecessor_shape.requirements
            ),
            (M5PersistedLogicalChangeKind.GROUP_STATE, predecessor_id),
        }
        successor_state_keys = {
            *(
                (M5PersistedLogicalChangeKind.REQUIREMENT_STATE, requirement_id)
                for _, requirement_id in successor_shape.requirements
            ),
            (M5PersistedLogicalChangeKind.GROUP_STATE, successor_id),
        }
        assert all(
            changes[key][0] is not None and changes[key][1] is None
            for key in predecessor_state_keys
        )
        assert all(
            changes[key][0] is None and changes[key][1] is not None
            for key in successor_state_keys
        )
        assert {
            row.state.requirement_version_id for row in plan.requirement_state_rows
        } == {requirement_id for _, requirement_id in successor_shape.requirements}
        assert {row.state.group_version_id for row in plan.group_state_rows} == {
            successor_id
        }
        assert {(row.group_version_id, row.present) for row in plan.hall_rows} == {
            (predecessor_id, False),
            (successor_id, True),
        }
        staged = _stage_prepared_matching_transition(cursor, intent, prepared)
        receipt = _finalize_prepared_matching_transition(cursor, intent, staged)

    assert not receipt.exact_replay
    assert connection.execute(
        """
        SELECT group_version_id, present
        FROM groundloop_m5_matching_hall_working
        WHERE epoch_id=%s
        ORDER BY group_version_id COLLATE "C"
        """,
        (database.epoch_id,),
    ).fetchall() == [(predecessor_id, False), (successor_id, True)]


def test_requirement_completion_stages_complete_group_certificate_bytes_and_journal(
    requirement_completion_database: Any,
) -> None:
    database = requirement_completion_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent, _, prepared = _derive_requirement_and_prepare(cursor, database)
        plan = prepared.physical_and_logical_write_plan
        assert len(plan.observation_currency_rows) == 1
        assert len(plan.group_certificate_artifact_rows) == 1
        assert len(plan.group_binding_rows) == 1
        artifact = plan.group_certificate_artifact_rows[0]
        binding = plan.group_binding_rows[0]
        expected_observation_id = (
            database.verifier.verifier_artifact.to_semantic_observation().observation_id
        )
        assert artifact.certificate_version == "m5-group-certificate-v1"
        assert artifact.group_version_id == (
            database.verifier.pair_input.group_version_id
        )
        assert tuple(
            (
                row.requirement_ordinal,
                row.requirement_version_id,
                row.text_hash,
                row.selected_observation_id,
            )
            for row in artifact.rows
        ) == (
            (
                database.verifier.pair_input.requirement_ordinal,
                database.verifier.pair_input.pair.subject_id,
                database.verifier.pair_input.m5_chunk_text_hash,
                expected_observation_id,
            ),
        )
        assert (
            binding.epoch_id,
            binding.group_version_id,
            binding.valid_from_revision,
            binding.valid_to_revision,
            binding.certificate_digest,
        ) == (
            database.epoch_id,
            artifact.group_version_id,
            database.resulting_revision,
            None,
            artifact.certificate_digest,
        )

        install_prepared_matching_headers(
            cursor, prepared, expected_revision=database.expected_revision
        )
        _stage_prepared_matching_transition(cursor, intent, prepared)
        expected_journal = _expected_matching_stage_rows(cursor, prepared)
        certificate_relations = {
            "groundloop_m5_group_certificate_artifact",
            "groundloop_m5_group_certificate_artifact_row",
            "groundloop_m5_working_group_certificate_binding",
        }
        expected_certificate_journal = {
            key: value
            for key, value in expected_journal.items()
            if key[0] in certificate_relations
        }
        journal_rows = cursor.execute(
            """
            SELECT relation_name, key_preimage, first_old, final_new,
                   first_operation, last_operation, mutation_count,
                   saw_insert, saw_update, saw_delete
            FROM pg_temp.groundloop_m5_matching_change_journal
            WHERE relation_name=ANY(%s)
            ORDER BY relation_name COLLATE "C", key_preimage
            """,
            (list(certificate_relations),),
        ).fetchall()
        assert {
            (str(row[0]), bytes(row[1])): row[3] for row in journal_rows
        } == expected_certificate_journal
        assert len(journal_rows) == 2 + artifact.requirement_count
        assert all(
            row[2] is None
            and row[4:7] == ("INSERT", "INSERT", 1)
            and row[7:10] == (True, False, False)
            for row in journal_rows
        )

        assert cursor.execute(
            """
            SELECT decision_policy_version, certificate_version,
                   group_version_id, requirement_count
            FROM groundloop_m5_group_certificate_artifact
            WHERE certificate_digest=%s
            """,
            (artifact.certificate_digest,),
        ).fetchone() == (
            artifact.decision_policy_version,
            artifact.certificate_version,
            artifact.group_version_id,
            artifact.requirement_count,
        )
        assert cursor.execute(
            """
            SELECT requirement_ordinal, requirement_version_id, text_hash,
                   selected_observation_id
            FROM groundloop_m5_group_certificate_artifact_row
            WHERE certificate_digest=%s
            ORDER BY requirement_ordinal
            """,
            (artifact.certificate_digest,),
        ).fetchall() == [
            (
                row.requirement_ordinal,
                row.requirement_version_id,
                row.text_hash,
                row.selected_observation_id,
            )
            for row in artifact.rows
        ]
        assert cursor.execute(
            """
            SELECT epoch_id, group_version_id, valid_from_revision,
                   valid_to_revision, certificate_digest
            FROM groundloop_m5_working_group_certificate_binding
            WHERE epoch_id=%s AND group_version_id=%s
              AND valid_from_revision=%s
            """,
            (
                database.epoch_id,
                artifact.group_version_id,
                database.resulting_revision,
            ),
        ).fetchone() == (
            binding.epoch_id,
            binding.group_version_id,
            binding.valid_from_revision,
            binding.valid_to_revision,
            binding.certificate_digest,
        )


def test_group_only_requirement_completion_uses_typed_m5_currency_history(
    requirement_completion_database: Any,
) -> None:
    database = requirement_completion_database
    connection = database.connection
    assert connection.execute(
        "SELECT count(*) FROM groundloop_m5_update WHERE epoch_id=%s",
        (database.epoch_id,),
    ).fetchone() == (1,)
    assert connection.execute(
        "SELECT count(*) FROM groundloop_m4_update WHERE epoch_id=%s",
        (database.epoch_id,),
    ).fetchone() == (0,)

    with connection.cursor() as cursor:
        intent, _, prepared = _derive_requirement_and_prepare(cursor, database)
        currency_rows = (
            prepared.physical_and_logical_write_plan.observation_currency_rows
        )
        assert len(currency_rows) == 1
        currency = currency_rows[0]
        assert (
            currency.epoch_id,
            currency.subject_kind,
            currency.subject_id,
            currency.chunk_version_id,
            currency.task_type,
            currency.observation_id,
            currency.valid_from_revision,
            currency.valid_to_revision,
        ) == (
            database.epoch_id,
            "requirement",
            database.verifier.pair_input.pair.subject_id,
            database.verifier.pair_input.pair.chunk_version_id,
            "verify_requirement_v1",
            database.verifier.verifier_artifact.to_semantic_observation().observation_id,
            database.resulting_revision,
            None,
        )
        install_prepared_matching_headers(
            cursor, prepared, expected_revision=database.expected_revision
        )
        _stage_prepared_matching_transition(cursor, intent, prepared)

        assert cursor.execute(
            """
            SELECT epoch_id, subject_kind, subject_id, chunk_version_id,
                   task_type, observation_id, valid_from_revision,
                   valid_to_revision
            FROM groundloop_m5_working_currency_history
            WHERE epoch_id=%s AND subject_kind=%s AND subject_id=%s
              AND chunk_version_id=%s AND task_type=%s
            ORDER BY valid_from_revision
            """,
            (
                currency.epoch_id,
                currency.subject_kind,
                currency.subject_id,
                currency.chunk_version_id,
                currency.task_type,
            ),
        ).fetchall() == [
            (
                currency.epoch_id,
                currency.subject_kind,
                currency.subject_id,
                currency.chunk_version_id,
                currency.task_type,
                currency.observation_id,
                currency.valid_from_revision,
                currency.valid_to_revision,
            )
        ]
        assert cursor.execute(
            "SELECT count(*) FROM groundloop_working_observation_delta "
            "WHERE epoch_id=%s",
            (database.epoch_id,),
        ).fetchone() == (0,)
        assert cursor.execute(
            "SELECT count(*) FROM groundloop_m4_update WHERE epoch_id=%s",
            (database.epoch_id,),
        ).fetchone() == (0,)


def test_requirement_claim_state_change_carries_published_binding_without_rewrite(
    requirement_completion_carry_forward_database: Any,
) -> None:
    database = requirement_completion_carry_forward_database
    connection = database.connection
    claim_id = database.verifier.pair_input.owner_claim_id
    published = connection.execute(
        """
        SELECT state.complete_group_ids, state.certificate_digest,
               binding.certificate_digest
        FROM groundloop_m5_published_claim_state AS state
        JOIN groundloop_m5_published_claim_certificate_binding AS binding
          ON binding.claim_id=state.claim_id
         AND binding.valid_from_epoch<=%s
         AND (binding.valid_to_epoch IS NULL OR %s<binding.valid_to_epoch)
        WHERE state.claim_id=%s AND state.valid_from_epoch<=%s
          AND (state.valid_to_epoch IS NULL OR %s<state.valid_to_epoch)
        """,
        (
            database.base_epoch_id,
            database.base_epoch_id,
            claim_id,
            database.base_epoch_id,
            database.base_epoch_id,
        ),
    ).fetchone()
    assert published is not None
    assert len(published[0]) == 1
    published_digest = str(published[1]).rstrip(" ")
    assert str(published[2]).rstrip(" ") == published_digest

    with connection.cursor() as cursor:
        intent, _, prepared = _derive_requirement_and_prepare(cursor, database)
        plan = prepared.physical_and_logical_write_plan
        assert len(plan.claim_state_rows) == 1
        claim_after = plan.claim_state_rows[0]
        assert claim_after.state.claim_id == claim_id
        assert claim_after.state.status.value == "supported"
        assert len(claim_after.state.complete_group_ids) == 2
        assert claim_after.certificate_digest == published_digest
        assert plan.claim_certificate_artifact_rows == ()
        assert len(plan.claim_binding_rows) == 1
        binding = plan.claim_binding_rows[0]
        assert (
            binding.epoch_id,
            binding.claim_id,
            binding.valid_from_revision,
            binding.valid_to_revision,
            binding.certificate_digest,
        ) == (
            database.epoch_id,
            claim_id,
            database.resulting_revision,
            None,
            published_digest,
        )

        logical_patch = prepared.prewrite_patch_artifact.logical_patch
        assert not any(
            change.kind is M5PersistedLogicalChangeKind.CLAIM_CERTIFICATE
            and change.object_id == claim_id
            for change in logical_patch.changes
        )
        assert tuple(
            row for row in logical_patch.binding_rows if row.object_id == claim_id
        ) == (
            next(
                row
                for row in logical_patch.binding_rows
                if row.kind is M5PersistedBindingKind.CLAIM
                and row.object_id == claim_id
            ),
        )
        retained_binding = next(
            row
            for row in logical_patch.binding_rows
            if row.kind is M5PersistedBindingKind.CLAIM and row.object_id == claim_id
        )
        assert (
            retained_binding.epoch_id,
            retained_binding.valid_from_revision,
            retained_binding.valid_to_revision,
            retained_binding.certificate_digest,
        ) == (
            binding.epoch_id,
            binding.valid_from_revision,
            binding.valid_to_revision,
            binding.certificate_digest,
        )
        assert (
            prepared.d24_owned_planned_write_counts.certificate_binding_write_count
            == len(plan.group_binding_rows) + 1
        )

        install_prepared_matching_headers(
            cursor, prepared, expected_revision=database.expected_revision
        )
        _stage_prepared_matching_transition(cursor, intent, prepared)
        assert cursor.execute(
            """
            SELECT relation_name, count(*)
            FROM pg_temp.groundloop_m5_matching_change_journal
            WHERE relation_name IN (
              'groundloop_m5_claim_certificate_artifact',
              'groundloop_m5_working_claim_certificate_binding'
            )
            GROUP BY relation_name
            ORDER BY relation_name COLLATE "C"
            """
        ).fetchall() == [("groundloop_m5_working_claim_certificate_binding", 1)]
        assert cursor.execute(
            """
            SELECT valid_from_revision, valid_to_revision, certificate_digest
            FROM groundloop_m5_working_claim_certificate_binding
            WHERE epoch_id=%s AND claim_id=%s
            ORDER BY valid_from_revision
            """,
            (database.epoch_id, claim_id),
        ).fetchall() == [(database.resulting_revision, None, published_digest)]


def test_requirement_completion_reuses_identical_group_certificate_without_dml(
    requirement_completion_database: Any,
) -> None:
    database = requirement_completion_database
    connection = database.connection
    with connection.cursor() as cursor:
        _, _, preview = _derive_requirement_and_prepare(cursor, database)
        preview_rows = preview.physical_and_logical_write_plan
        assert len(preview_rows.group_certificate_artifact_rows) == 1
        artifact = preview_rows.group_certificate_artifact_rows[0]
    connection.rollback()
    _persist_group_certificate_fixture(connection, artifact, mode="exact")

    with connection.cursor() as cursor:
        intent, _, prepared = _derive_requirement_and_prepare(cursor, database)
        plan = prepared.physical_and_logical_write_plan
        assert plan.group_certificate_artifact_rows == ()
        assert len(plan.group_binding_rows) == 1
        assert plan.group_binding_rows[0].certificate_digest == (
            artifact.certificate_digest
        )
        install_prepared_matching_headers(
            cursor, prepared, expected_revision=database.expected_revision
        )
        _stage_prepared_matching_transition(cursor, intent, prepared)
        assert cursor.execute(
            """
            SELECT relation_name, count(*)
            FROM pg_temp.groundloop_m5_matching_change_journal
            WHERE relation_name IN (
              'groundloop_m5_group_certificate_artifact',
              'groundloop_m5_group_certificate_artifact_row',
              'groundloop_m5_working_group_certificate_binding'
            )
            GROUP BY relation_name
            ORDER BY relation_name COLLATE "C"
            """
        ).fetchall() == [("groundloop_m5_working_group_certificate_binding", 1)]
        assert cursor.execute(
            """
            SELECT
              (SELECT count(*)
               FROM groundloop_m5_group_certificate_artifact
               WHERE certificate_digest=%s),
              (SELECT count(*)
               FROM groundloop_m5_group_certificate_artifact_row
               WHERE certificate_digest=%s)
            """,
            (artifact.certificate_digest, artifact.certificate_digest),
        ).fetchone() == (1, artifact.requirement_count)
        binding = plan.group_binding_rows[0]
        assert cursor.execute(
            """
            SELECT count(*), min(valid_from_revision), max(valid_to_revision),
                   min(certificate_digest)
            FROM groundloop_m5_working_group_certificate_binding
            WHERE epoch_id=%s AND group_version_id=%s
            """,
            (database.epoch_id, binding.group_version_id),
        ).fetchone() == (
            1,
            database.resulting_revision,
            None,
            artifact.certificate_digest,
        )


@pytest.mark.parametrize(
    "mode",
    ("header_collision", "missing_child", "changed_child"),
)
def test_requirement_completion_rejects_group_certificate_collision_before_dml(
    requirement_completion_database: Any,
    mode: str,
) -> None:
    database = requirement_completion_database
    connection = database.connection
    with connection.cursor() as cursor:
        _, _, preview = _derive_requirement_and_prepare(cursor, database)
        artifact_rows = (
            preview.physical_and_logical_write_plan.group_certificate_artifact_rows
        )
        artifact = artifact_rows[0]
    connection.rollback()
    _persist_group_certificate_fixture(connection, artifact, mode=mode)
    before = _requirement_d25_footprint(connection, database)

    with connection.cursor() as cursor:
        with pytest.raises(
            EventConflictError, match="matching group certificate digest collision"
        ):
            _derive_requirement_and_prepare(cursor, database)

    assert _requirement_d25_footprint(connection, database) == before


@pytest.mark.parametrize(
    ("tamper", "message"),
    (
        (
            "artifact_row",
            "matching staged row differs from its prepared final after-image",
        ),
        ("journal", "matching stage journal operation or after-image differs"),
    ),
)
def test_requirement_certificate_tamper_rolls_back_every_staged_row(
    requirement_completion_database: Any,
    tamper: str,
    message: str,
) -> None:
    database = requirement_completion_database
    connection = database.connection
    before = _requirement_d25_footprint(connection, database)

    with pytest.raises(EventConflictError, match=message):
        with connection.transaction(), connection.cursor() as cursor:
            intent, _, prepared = _derive_requirement_and_prepare(cursor, database)
            artifact_rows = (
                prepared.physical_and_logical_write_plan.group_certificate_artifact_rows
            )
            artifact = artifact_rows[0]
            install_prepared_matching_headers(
                cursor, prepared, expected_revision=database.expected_revision
            )
            staged = _stage_prepared_matching_transition(cursor, intent, prepared)
            if tamper == "artifact_row":
                with schema_fixture._d26_replica_trigger_window(connection):
                    changed = cursor.execute(
                        """
                        UPDATE groundloop_m5_group_certificate_artifact_row
                        SET text_hash=%s
                        WHERE certificate_digest=%s AND requirement_ordinal=0
                        """,
                        (
                            sha("d25-staged-certificate-tamper"),
                            artifact.certificate_digest,
                        ),
                    ).rowcount
            else:
                changed = cursor.execute(
                    """
                    UPDATE pg_temp.groundloop_m5_matching_change_journal
                    SET final_new='{}'::jsonb
                    WHERE relation_name='groundloop_m5_group_certificate_artifact'
                      AND final_new->>'certificate_digest'=%s
                    """,
                    (artifact.certificate_digest,),
                ).rowcount
            assert changed == 1
            _finalize_prepared_matching_transition(cursor, intent, staged)

    assert _requirement_d25_footprint(connection, database) == before


def test_requirement_completion_retained_projection_replays_exact_certificate(
    requirement_completion_database: Any,
) -> None:
    database = requirement_completion_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent, _, prepared = _derive_requirement_and_prepare(cursor, database)
        artifact_rows = (
            prepared.physical_and_logical_write_plan.group_certificate_artifact_rows
        )
        artifact = artifact_rows[0]
        install_prepared_matching_headers(
            cursor, prepared, expected_revision=database.expected_revision
        )
        staged = _stage_prepared_matching_transition(cursor, intent, prepared)
        terminalize_requirement_completion_source(cursor, database)
        first = _finalize_prepared_matching_transition(cursor, intent, staged)
        replay_intent = retained_matching_replay_intent(
            cursor,
            epoch_id=database.epoch_id,
            source_kind=M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION,
            source_id=database.attempt_id,
        )
        assert replay_intent.group_certificate_ids == (artifact.group_version_id,)
        before = _requirement_d25_footprint(connection, database)
        replay = apply_matching_transition(
            cursor,
            replay_intent,
            expected_patch_digest=first.patch.patch_digest,
            expected_work=prepared.d25_contribution_work,
        )
        after = _requirement_d25_footprint(connection, database)

    assert replay.exact_replay
    assert replay.patch == first.patch
    assert replay.contribution_digest == first.contribution_digest
    assert replay.accumulated_work == first.accumulated_work
    assert after == before


def test_document_delete_and_replace_derive_the_same_bounded_withdrawal_shape(
    document_withdrawal_planning_database: Any,
) -> None:
    database = document_withdrawal_planning_database
    connection = database.connection
    with connection.cursor() as cursor:
        current_currency = tuple(
            tuple(row)
            for row in cursor.execute(
                """
                SELECT subject_id, chunk_version_id, task_type, observation_id
                FROM groundloop_observation_currency
                WHERE subject_kind='requirement'
                  AND chunk_version_id=ANY(%s)
                ORDER BY subject_id COLLATE "C", chunk_version_id COLLATE "C",
                         task_type COLLATE "C"
                """,
                (list(database.deactivated_chunk_ids),),
            ).fetchall()
        )
        canonical_removed = tuple(
            sorted(
                str(row[0])
                for row in cursor.execute(
                    """
                    SELECT matching.observation_id
                    FROM groundloop_m5_matching_observation_current AS matching
                    JOIN groundloop_observation_currency AS currency
                      ON currency.observation_id=matching.observation_id
                    WHERE currency.subject_kind='requirement'
                      AND currency.chunk_version_id=ANY(%s)
                    """,
                    (list(database.deactivated_chunk_ids),),
                ).fetchall()
            )
        )
        assert len(current_currency) == 6
        assert len(canonical_removed) == 5
        assert database.noncanonical_observation_id not in canonical_removed
        assert database.observation_only_id in canonical_removed
        assert cursor.execute(
            """
            SELECT count(*)
            FROM groundloop_m5_requirement_admitted_pair
            WHERE admitted_pair_digest=%s AND subject_id=%s
              AND chunk_version_id=%s
            """,
            (
                database.candidate_only_pair_digest,
                database.candidate_only_requirement_id,
                database.candidate_only_chunk_id,
            ),
        ).fetchone() == (1,)
        assert cursor.execute(
            """
            SELECT count(*)
            FROM groundloop_observation_currency
            WHERE subject_kind='requirement' AND subject_id=%s
              AND chunk_version_id=%s
            """,
            (
                database.candidate_only_requirement_id,
                database.candidate_only_chunk_id,
            ),
        ).fetchone() == (0,)
        assert cursor.execute(
            """
            SELECT count(*)
            FROM groundloop_semantic_observation AS observation
            JOIN groundloop_m5_requirement_admitted_pair AS admitted
              ON admitted.subject_id=observation.subject_id
             AND admitted.chunk_version_id=observation.chunk_version_id
            WHERE observation.observation_id=%s
            """,
            (database.observation_only_id,),
        ).fetchone() == (0,)

        intent = _derive_document_withdrawal(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        plan = prepared.physical_and_logical_write_plan

    expected_currency_keys = {
        (str(subject_id), str(chunk_id), str(task_type))
        for subject_id, chunk_id, task_type, _ in current_currency
    }
    actual_currency_keys = {
        (row.subject_id, row.chunk_version_id, row.task_type)
        for row in plan.observation_currency_rows
    }
    assert actual_currency_keys == expected_currency_keys
    assert all(
        row.epoch_id == database.epoch_id
        and row.observation_id is None
        and row.valid_from_revision == 1
        and row.valid_to_revision is None
        for row in plan.observation_currency_rows
    )
    assert {row.observation_id for row in plan.observation_rows} == set(
        canonical_removed
    )
    assert all(not row.present for row in plan.observation_rows)
    assert database.noncanonical_observation_id not in intent.observation_ids
    assert database.candidate_only_requirement_id not in intent.requirement_state_ids
    assert database.observation_only_id in intent.observation_ids

    # The retained alternate is a no-op lock/representative, not a removal.
    assert database.survivor_observation_id in intent.observation_ids
    assert database.survivor_observation_id not in {
        row.observation_id for row in plan.observation_rows
    }
    survivor_edges = tuple(
        row
        for row in plan.edge_rows
        if row.group_version_id == database.survivor_group_id
    )
    assert len(survivor_edges) == 1 and survivor_edges[0].refcount == 1
    assert all(
        row.group_version_id != database.survivor_group_id for row in plan.mask_rows
    )
    assert all(
        row.group_version_id != database.survivor_group_id for row in plan.hall_rows
    )

    # Two removed observations share one exact edge, so the patch is coalesced.
    duplicate_rows = tuple(
        row
        for row in plan.observation_rows
        if row.requirement_version_id == database.snapshot.complete_requirement_ids[0]
        and row.text_hash
        == next(
            text_hash
            for _, requirement_id, _, _, text_hash in (
                database.snapshot.requirement_observations
            )
            if requirement_id == database.snapshot.complete_requirement_ids[0]
        )
    )
    assert len(duplicate_rows) == 2
    duplicate_edge_keys = {
        (row.requirement_version_id, row.text_hash)
        for row in plan.edge_rows
        if row.requirement_version_id == database.snapshot.complete_requirement_ids[0]
    }
    assert len(duplicate_edge_keys) == 1
    assert len(plan.observation_rows) == 5
    assert len(plan.edge_rows) == 4
    assert len(plan.mask_rows) == 3
    assert len(plan.hall_rows) == 2


def test_document_withdrawal_materializes_exact_logical_certificate_and_counts(
    document_withdrawal_planning_database: Any,
) -> None:
    database = document_withdrawal_planning_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent = _derive_document_withdrawal(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        plan = prepared.physical_and_logical_write_plan
        logical = prepared.prewrite_patch_artifact.logical_patch

    change_keys = {(change.kind, change.object_id) for change in logical.changes}
    assert {
        requirement.state.requirement_version_id
        for requirement in plan.requirement_state_rows
    } == set(intent.requirement_state_ids)
    assert len(plan.requirement_state_rows) == 4
    assert len(plan.group_state_rows) == 3
    assert len(plan.claim_state_rows) == 1
    assert len(plan.answer_state_rows) == 1
    assert len(plan.group_certificate_artifact_rows) == 1
    assert plan.group_certificate_artifact_rows[0].group_version_id == (
        database.survivor_group_id
    )
    assert len(plan.claim_certificate_artifact_rows) == 1
    assert len(plan.group_binding_rows) == 1
    assert plan.group_binding_rows[0].group_version_id == database.survivor_group_id
    assert len(plan.claim_binding_rows) == 1
    assert (
        M5PersistedLogicalChangeKind.GROUP_CERTIFICATE,
        database.snapshot.complete_group_id,
    ) in change_keys
    assert (
        M5PersistedLogicalChangeKind.GROUP_CERTIFICATE,
        database.survivor_group_id,
    ) in change_keys
    assert {row.kind for row in logical.binding_rows} == {
        M5PersistedBindingKind.GROUP,
        M5PersistedBindingKind.CLAIM,
    }

    status_deltas = tuple(
        value for kind, _, value in logical.output_records if kind == "status_delta"
    )
    assert {
        (delta.object_type, delta.object_id, delta.old_status, delta.new_status)
        for delta in status_deltas
    } == {
        ("claim", database.snapshot.claim_ids[0], "supported", "unsupported"),
        ("answer", database.snapshot.answer_id, "valid", "unsupported"),
    }
    expected_operation = (
        "DeleteDocumentVersionEvent"
        if database.update_kind == "document_delete"
        else "ReplaceDocumentVersionEvent"
    )
    assert all(
        delta.event_id == database.event_id
        and delta.reason == f"event={database.event_id} op={expected_operation}"
        for delta in status_deltas
    )
    assert prepared.requirement_state_write_count_diagnostic == 4
    counts = prepared.d24_owned_planned_write_counts
    assert (
        counts.group_state_write_count,
        counts.claim_state_write_count,
        counts.answer_state_write_count,
        counts.certificate_binding_write_count,
        counts.public_delta_write_count,
    ) == (3, 1, 1, 2, 2)
    assert prepared.d25_contribution_work.public_status_deltas == 2
