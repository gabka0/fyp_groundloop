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
    derive_matching_transition_intent,
)


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
        intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
            database.payload_hash,
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

        receipt = apply_matching_transition(cursor, intent)

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


def test_compare_only_source_hash_and_patch_inputs_write_nothing(
    empty_structural_database: Any,
) -> None:
    database = empty_structural_database
    connection = database.connection
    before = _relation_counts(connection)
    with connection.cursor() as cursor:
        with pytest.raises(EventConflictError, match="source hash changed"):
            derive_matching_transition_intent(
                cursor,
                database.epoch_id,
                1,
                1,
                M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
                database.event_id,
                "0" * 64,
            )
        intent = derive_matching_transition_intent(
            cursor,
            database.epoch_id,
            1,
            1,
            M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            database.event_id,
        )
        with pytest.raises(EventConflictError, match="patch digest differs"):
            apply_matching_transition(
                cursor,
                intent,
                expected_patch_digest="0" * 64,
            )
    assert _relation_counts(connection) == before


@pytest.mark.parametrize(
    "source_kind",
    (
        M5PersistedMatchingSourceKind.REQUIREMENT_COMPLETION,
        M5PersistedMatchingSourceKind.DIRECT_TRANSITION,
    ),
)
def test_nonempty_semantic_source_kinds_fail_before_write(
    empty_structural_database: Any,
    source_kind: M5PersistedMatchingSourceKind,
) -> None:
    database = empty_structural_database
    connection = database.connection
    before = _relation_counts(connection)
    with connection.cursor() as cursor:
        with pytest.raises(InvalidEventError, match="before its first write"):
            derive_matching_transition_intent(
                cursor,
                database.epoch_id,
                1,
                2,
                source_kind,
                "unsupported-source",
            )
    assert _relation_counts(connection) == before
