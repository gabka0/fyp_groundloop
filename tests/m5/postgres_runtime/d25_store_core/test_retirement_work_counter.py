"""Migration-017 regressions for structural retirement local-work accounting."""

from __future__ import annotations

from typing import Any

import pytest

from groundloop.m5.runtime.contracts import M5PersistedMatchingSourceKind
from groundloop.m5.runtime.postgres_matching import (
    _finalize_prepared_matching_transition,
    _prepare_matching_transition,
    _stage_prepared_matching_transition,
    apply_matching_transition,
)
from groundloop.postgres.migrations import install_m5_persisted_matching_bundle
from tests.m5.postgres_runtime import test_migration_017 as schema_fixture
from tests.m5.postgres_runtime.d25_store_core.conftest import (
    _install_d24_open_accounting,
    authorize_and_derive_matching_transition,
    retained_matching_replay_intent,
)


def _matching_evidence(connection: Any, epoch_id: int) -> tuple[Any, ...]:
    """Return exact immutable/accumulated D25 evidence for replay comparison."""

    contribution = connection.execute(
        """
        SELECT patch_digest, group_local_state_operations,
               matching_work_digest, contribution_digest
        FROM groundloop_m5_matching_work_contribution
        WHERE epoch_id=%s AND source_kind='structural_open'
        """,
        (epoch_id,),
    ).fetchone()
    accumulator = connection.execute(
        """
        SELECT group_local_state_operations, matching_work_digest,
               updated_revision
        FROM groundloop_m5_matching_work_accumulator
        WHERE epoch_id=%s
        """,
        (epoch_id,),
    ).fetchone()
    artifact = connection.execute(
        """
        SELECT patch_digest, cardinality(mask_change_digests),
               mask_change_digests, mask_change_preimages,
               matching_work_digest, canonical_patch_preimage
        FROM groundloop_m5_matching_patch_artifact
        WHERE resulting_epoch_id=%s AND source_kind='structural_open'
        """,
        (epoch_id,),
    ).fetchone()
    assert contribution is not None
    assert accumulator is not None
    assert artifact is not None
    return tuple(contribution), tuple(accumulator), tuple(artifact)


@pytest.mark.parametrize(
    (
        "action",
        "predecessor_kind",
        "expected_mask_change_count",
        "expected_group_local_operations",
    ),
    (
        ("RETIRE", "zero_hash", 0, 0),
        ("REPLACE", "zero_hash", 0, 0),
        ("RETIRE", "complete", 2, 1),
    ),
)
def test_structural_retirement_counts_distinct_mask_change_groups(
    action: str,
    predecessor_kind: str,
    expected_mask_change_count: int,
    expected_group_local_operations: int,
) -> None:
    """Persist zero/nonzero retirement counters and replay exact stored bytes."""

    prefix = f"d30-c1-counter-{action.lower()}-{predecessor_kind}"
    with schema_fixture._pre017_schema() as (connection, _):
        snapshot = schema_fixture._seed_b3_activated_snapshot(
            connection,
            prefix,
            complete_owner_survivor=predecessor_kind == "complete",
        )
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        schema_fixture._register_d26_candidate_policy(connection, snapshot)
        connection.commit()

        predecessor_group_id = (
            snapshot.zero_hash_group_id
            if predecessor_kind == "zero_hash"
            else snapshot.complete_group_id
        )
        open_prefix = f"{prefix}-open"
        event_id = f"{open_prefix}-event"
        payload_hash, successor_group_id = schema_fixture._d26_structural_payload(
            connection,
            group_id=predecessor_group_id,
            event_id=event_id,
            action=action,
        )
        assert (successor_group_id is None) == (action == "RETIRE")
        epoch_id, actual_event_id, actual_payload_hash = (
            schema_fixture._open_b2_runtime_epoch(
                connection,
                open_prefix,
                snapshot.epoch_id,
                snapshot.policy_version,
                update_kind=f"{action.lower()}_group",
                payload_override=payload_hash,
            )
        )
        assert (actual_event_id, actual_payload_hash) == (event_id, payload_hash)
        schema_fixture._stage_b2_group_deactivation(
            connection,
            epoch=epoch_id,
            event=event_id,
            group=predecessor_group_id,
            action=action,
        )
        _install_d24_open_accounting(
            connection,
            epoch_id=epoch_id,
            event_id=event_id,
            payload_hash=payload_hash,
        )

        with connection.cursor() as cursor:
            intent = authorize_and_derive_matching_transition(
                cursor,
                epoch_id=epoch_id,
                expected_runtime_revision=1,
                resulting_revision=1,
                source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
                source_id=event_id,
                expected_source_identity_hash=payload_hash,
            )
            prepared = _prepare_matching_transition(cursor, intent)
            artifact = prepared.prewrite_patch_artifact
            assert len(artifact.mask_changes) == expected_mask_change_count
            assert {change.group_version_id for change in artifact.mask_changes} == (
                {predecessor_group_id} if expected_mask_change_count else set()
            )
            assert (
                artifact.work.matching.group_local_state_operations
                == expected_group_local_operations
            )
            staged = _stage_prepared_matching_transition(cursor, intent, prepared)
            receipt = _finalize_prepared_matching_transition(cursor, intent, staged)
            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

        assert not receipt.exact_replay
        assert receipt.patch == artifact.patch
        assert receipt.accumulated_work == artifact.work
        assert (
            receipt.accumulated_work.matching.group_local_state_operations
            == expected_group_local_operations
        )
        first_evidence = _matching_evidence(connection, epoch_id)
        contribution, accumulator, stored_artifact = first_evidence
        assert str(contribution[0]).rstrip(" ") == artifact.patch.patch_digest
        assert int(contribution[1]) == expected_group_local_operations
        assert str(contribution[2]).rstrip(" ") == artifact.patch.matching_work_digest
        assert str(contribution[3]).rstrip(" ") == receipt.contribution_digest
        assert int(accumulator[0]) == expected_group_local_operations
        assert str(accumulator[1]).rstrip(" ") == artifact.patch.matching_work_digest
        assert int(accumulator[2]) == 1
        assert str(stored_artifact[0]).rstrip(" ") == artifact.patch.patch_digest
        assert int(stored_artifact[1]) == expected_mask_change_count
        assert (
            str(stored_artifact[4]).rstrip(" ") == artifact.patch.matching_work_digest
        )
        assert bytes(stored_artifact[5]) == artifact.patch_preimage
        connection.commit()

        with connection.cursor() as cursor:
            replay_intent = retained_matching_replay_intent(
                cursor,
                epoch_id=epoch_id,
                source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
                source_id=event_id,
            )
            assert replay_intent.mask_keys == tuple(
                (change.group_version_id, change.text_hash)
                for change in artifact.mask_changes
            )
            replay = apply_matching_transition(
                cursor,
                replay_intent,
                expected_patch_digest=artifact.patch.patch_digest,
                expected_work=artifact.work,
            )
            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

        assert replay.exact_replay
        assert replay.patch == receipt.patch
        assert replay.contribution_digest == receipt.contribution_digest
        assert replay.accumulated_work == receipt.accumulated_work
        assert _matching_evidence(connection, epoch_id) == first_evidence
        connection.commit()
