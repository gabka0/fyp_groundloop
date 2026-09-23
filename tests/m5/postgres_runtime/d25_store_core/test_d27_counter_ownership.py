"""M5-D27 requirement-state counter ownership falsifiers."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from groundloop.errors import EventConflictError
from groundloop.m5.runtime.contracts import (
    MATCHING_WORK_COUNTER_NAMES,
    M5PersistedLogicalChangeKind,
    M5PersistedMatchingSourceKind,
    M5RuntimeWork,
)
from groundloop.m5.runtime.postgres_matching import (
    _finalize_prepared_matching_transition,
    _prepare_matching_transition,
    _stage_prepared_matching_transition,
    apply_matching_transition,
)
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    authorize_and_derive_matching_transition,
    authorize_and_derive_requirement_completion,
    install_prepared_matching_headers,
    retained_matching_replay_intent,
)


def _derive_and_prepare(cursor: Any, database: Any) -> tuple[Any, Any]:
    intent = authorize_and_derive_matching_transition(
        cursor,
        epoch_id=database.epoch_id,
        expected_runtime_revision=1,
        resulting_revision=1,
        source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
        source_id=database.event_id,
        expected_source_identity_hash=database.payload_hash,
    )
    return intent, _prepare_matching_transition(cursor, intent)


def test_requirement_state_writes_have_only_the_d27_local_diagnostic(
    rich_registration_planning_database: Any,
) -> None:
    database = rich_registration_planning_database
    with database.connection.cursor() as cursor:
        intent, prepared = _derive_and_prepare(cursor, database)

    runtime_names = M5RuntimeWork.counter_names()
    assert "requirement_state_write_count" not in runtime_names
    assert (
        "group_state_write_count",
        "claim_state_write_count",
        "answer_state_write_count",
        "certificate_binding_write_count",
        "public_delta_count",
    ) == tuple(
        name
        for name in runtime_names
        if name
        in {
            "group_state_write_count",
            "claim_state_write_count",
            "answer_state_write_count",
            "certificate_binding_write_count",
            "public_delta_count",
        }
    )
    assert "requirement_state_write_count" not in MATCHING_WORK_COUNTER_NAMES

    plan = prepared.physical_and_logical_write_plan
    counts = prepared.d24_owned_planned_write_counts
    assert len(plan.requirement_state_rows) > 0
    requirement_changes = tuple(
        change
        for change in prepared.prewrite_patch_artifact.logical_patch.changes
        if change.kind is M5PersistedLogicalChangeKind.REQUIREMENT_STATE
    )
    assert tuple(change.object_id for change in requirement_changes) == (
        intent.requirement_state_ids
    )
    assert all(
        change.before_hash is None and change.after_hash is not None
        for change in requirement_changes
    )
    assert prepared.requirement_state_write_count_diagnostic == len(
        plan.requirement_state_rows
    )
    assert counts.group_state_write_count == len(plan.group_state_rows)
    assert counts.claim_state_write_count == len(plan.claim_state_rows)
    assert counts.answer_state_write_count == len(plan.answer_state_rows)
    assert counts.certificate_binding_write_count == (
        len(plan.group_binding_rows) + len(plan.claim_binding_rows)
    )
    assert counts.public_delta_write_count == (
        prepared.d25_contribution_work.public_status_deltas
    )


@pytest.mark.parametrize(
    "surrogate",
    ("group_local_state_operations", "requirement_state_only_changes"),
)
def test_requirement_state_count_cannot_alias_a_d25_work_coordinate(
    rich_registration_planning_database: Any,
    surrogate: str,
) -> None:
    database = rich_registration_planning_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent, prepared = _derive_and_prepare(cursor, database)
        work = prepared.d25_contribution_work
        if surrogate == "group_local_state_operations":
            wrong = replace(
                work,
                matching=replace(
                    work.matching,
                    group_local_state_operations=(
                        work.matching.group_local_state_operations
                        + prepared.requirement_state_write_count_diagnostic
                    ),
                ),
            )
        else:
            wrong = replace(
                work,
                requirement_state_only_changes=(
                    work.requirement_state_only_changes
                    + prepared.requirement_state_write_count_diagnostic
                ),
            )

        with pytest.raises(EventConflictError, match="computed matching work differs"):
            _prepare_matching_transition(cursor, intent, expected_work=wrong)

    assert connection.execute(
        "SELECT count(*) FROM groundloop_m5_matching_image_working WHERE epoch_id=%s",
        (database.epoch_id,),
    ).fetchone() == (0,)


def test_registration_stage_realizes_the_local_diagnostic_and_owned_counts(
    rich_registration_planning_database: Any,
) -> None:
    database = rich_registration_planning_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent, prepared = _derive_and_prepare(cursor, database)
        _stage_prepared_matching_transition(cursor, intent, prepared)

    actual = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_working_requirement_state
           WHERE epoch_id=%s),
          (SELECT count(*) FROM groundloop_m5_working_group_state
           WHERE epoch_id=%s),
          (SELECT count(*) FROM groundloop_m5_working_claim_state
           WHERE epoch_id=%s),
          (SELECT count(*) FROM groundloop_m5_working_answer_state
           WHERE epoch_id=%s),
          ((SELECT count(*)
            FROM groundloop_m5_working_group_certificate_binding
            WHERE epoch_id=%s) +
           (SELECT count(*)
            FROM groundloop_m5_working_claim_certificate_binding
            WHERE epoch_id=%s))
        """,
        (database.epoch_id,) * 6,
    ).fetchone()
    assert actual is not None
    counts = prepared.d24_owned_planned_write_counts
    assert tuple(int(value) for value in actual) == (
        prepared.requirement_state_write_count_diagnostic,
        counts.group_state_write_count,
        counts.claim_state_write_count,
        counts.answer_state_write_count,
        counts.certificate_binding_write_count,
    )
    assert counts.public_delta_write_count == (
        prepared.d25_contribution_work.public_status_deltas
    )


def test_mutated_d24_count_plan_fails_before_stage_dml(
    rich_registration_planning_database: Any,
) -> None:
    database = rich_registration_planning_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent, prepared = _derive_and_prepare(cursor, database)
        counts = prepared.d24_owned_planned_write_counts
        prepared.d24_owned_planned_write_counts = replace(
            counts,
            group_state_write_count=(
                counts.group_state_write_count
                + prepared.requirement_state_write_count_diagnostic
            ),
        )
        with pytest.raises(EventConflictError, match="planned write counts changed"):
            _stage_prepared_matching_transition(cursor, intent, prepared)

    assert connection.execute(
        "SELECT count(*) FROM groundloop_m5_matching_image_working WHERE epoch_id=%s",
        (database.epoch_id,),
    ).fetchone() == (0,)


def test_certificate_artifact_insert_is_not_a_certificate_binding_write(
    rich_retirement_planning_database: Any,
) -> None:
    database = rich_retirement_planning_database
    with database.connection.cursor() as cursor:
        _, prepared = _derive_and_prepare(cursor, database)

    plan = prepared.physical_and_logical_write_plan
    assert plan.claim_certificate_artifact_rows
    assert plan.claim_binding_rows
    assert prepared.d24_owned_planned_write_counts.certificate_binding_write_count == (
        len(plan.group_binding_rows) + len(plan.claim_binding_rows)
    )
    assert prepared.d24_owned_planned_write_counts.certificate_binding_write_count != (
        len(plan.group_certificate_artifact_rows)
        + len(plan.claim_certificate_artifact_rows)
        + len(plan.group_binding_rows)
        + len(plan.claim_binding_rows)
    )


def test_requirement_group_certificate_artifact_and_rows_do_not_alias_binding_count(
    requirement_completion_database: Any,
) -> None:
    database = requirement_completion_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent, _ = authorize_and_derive_requirement_completion(cursor, database)
        prepared = _prepare_matching_transition(cursor, intent)
        plan = prepared.physical_and_logical_write_plan
        assert len(plan.group_certificate_artifact_rows) == 1
        artifact = plan.group_certificate_artifact_rows[0]
        assert artifact.rows
        assert len(plan.group_binding_rows) == 1
        counts = prepared.d24_owned_planned_write_counts
        expected_binding_count = len(plan.group_binding_rows) + len(
            plan.claim_binding_rows
        )
        expected_artifact_count = (
            len(plan.group_certificate_artifact_rows)
            + sum(
                item.requirement_count for item in plan.group_certificate_artifact_rows
            )
            + len(plan.claim_certificate_artifact_rows)
        )
        assert counts.certificate_binding_write_count == expected_binding_count
        assert counts.certificate_binding_write_count != (
            expected_binding_count + expected_artifact_count
        )

        install_prepared_matching_headers(
            cursor, prepared, expected_revision=database.expected_revision
        )
        _stage_prepared_matching_transition(cursor, intent, prepared)
        actual = cursor.execute(
            """
            SELECT
              ((SELECT count(*)
                FROM groundloop_m5_working_group_certificate_binding
                WHERE epoch_id=%s AND valid_from_revision=%s) +
               (SELECT count(*)
                FROM groundloop_m5_working_claim_certificate_binding
                WHERE epoch_id=%s AND valid_from_revision=%s)),
              (SELECT count(*)
               FROM groundloop_m5_group_certificate_artifact
               WHERE certificate_digest=%s),
              (SELECT count(*)
               FROM groundloop_m5_group_certificate_artifact_row
               WHERE certificate_digest=%s)
            """,
            (
                database.epoch_id,
                database.resulting_revision,
                database.epoch_id,
                database.resulting_revision,
                artifact.certificate_digest,
                artifact.certificate_digest,
            ),
        ).fetchone()
        assert actual == (
            expected_binding_count,
            1,
            artifact.requirement_count,
        )


def test_registration_requirement_state_patch_replays_without_counter_reconstruction(
    rich_registration_planning_database: Any,
) -> None:
    database = rich_registration_planning_database
    connection = database.connection
    with connection.cursor() as cursor:
        intent, prepared = _derive_and_prepare(cursor, database)
        staged = _stage_prepared_matching_transition(cursor, intent, prepared)
        first = _finalize_prepared_matching_transition(cursor, intent, staged)
        replay_intent = retained_matching_replay_intent(
            cursor,
            epoch_id=database.epoch_id,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=database.event_id,
        )
    connection.commit()
    retained_counts = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_matching_patch_artifact
           WHERE resulting_epoch_id=%s),
          (SELECT count(*) FROM groundloop_m5_matching_work_contribution
           WHERE epoch_id=%s),
          (SELECT count(*) FROM groundloop_m5_matching_work_accumulator
           WHERE epoch_id=%s)
        """,
        (database.epoch_id,) * 3,
    ).fetchone()

    with connection.cursor() as cursor:
        replay = apply_matching_transition(
            cursor,
            replay_intent,
            expected_patch_digest=first.patch.patch_digest,
            expected_work=prepared.d25_contribution_work,
        )

    assert replay.exact_replay
    assert replay.patch == first.patch
    assert replay.accumulated_work == first.accumulated_work
    assert (
        connection.execute(
            """
        SELECT
          (SELECT count(*) FROM groundloop_m5_matching_patch_artifact
           WHERE resulting_epoch_id=%s),
          (SELECT count(*) FROM groundloop_m5_matching_work_contribution
           WHERE epoch_id=%s),
          (SELECT count(*) FROM groundloop_m5_matching_work_accumulator
           WHERE epoch_id=%s)
        """,
            (database.epoch_id,) * 3,
        ).fetchone()
        == retained_counts
    )
