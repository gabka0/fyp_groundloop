"""One-epoch promotion ownership and ordering tests."""

from __future__ import annotations

import importlib
from typing import Any, cast

import psycopg
import pytest
from psycopg import sql

from groundloop.errors import ValidationError
from groundloop.m5.runtime.postgres_matching_publication import (
    promote_matching_overlay,
)


class _RecordingCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.rowcount = 0

    def execute(self, query: str, params: object = None) -> _RecordingCursor:
        normalized = " ".join(query.split())
        self.calls.append((normalized, params))
        if normalized.startswith("UPDATE groundloop_m5_matching_image_current"):
            self.rowcount = 1
        elif normalized.startswith("DELETE FROM"):
            self.rowcount = 2
        elif normalized.startswith("INSERT INTO"):
            self.rowcount = 3
        else:
            self.rowcount = 0
        return self


def test_seal_promotion_is_one_epoch_and_does_not_own_outer_transaction() -> None:
    cursor = _RecordingCursor()
    receipt = promote_matching_overlay(
        cast(Any, cursor), epoch_id=17, expected_revision=4, sealed_revision=5
    )
    assert receipt.mode == "seal"
    assert receipt.observation_deletes == 2
    assert receipt.observation_writes == 3
    assert receipt.edge_deletes == 2
    assert receipt.edge_writes == 3
    assert receipt.mask_deletes == 2
    assert receipt.mask_writes == 3
    assert receipt.hall_deletes == 2
    assert receipt.hall_writes == 3
    sql = "\n".join(query for query, _ in cursor.calls).lower()
    assert "groundloop_m5_authorize_checked_transition" in sql
    assert "groundloop_m5_authorize_persisted_matching_seal" in sql
    assert "set_config('groundloop.m5_checked_transition'" not in sql
    assert cursor.calls[0][1] == (17, 4)
    assert "where epoch_id = %s" in sql
    assert "groundloop_m5_publication_head" not in sql
    assert "groundloop_m5_event_result" not in sql
    assert "commit" not in sql
    assert "rollback" not in sql


def test_seal_promotion_rejects_nonadjacent_seal_before_sql() -> None:
    cursor = _RecordingCursor()
    with pytest.raises(ValidationError, match=r"expected_revision \+ 1"):
        promote_matching_overlay(
            cast(Any, cursor), epoch_id=17, expected_revision=4, sealed_revision=6
        )
    assert cursor.calls == []


def test_live_nonempty_tombstone_promotion_uses_checked_authority() -> None:
    migration017 = cast(
        Any,
        importlib.import_module("tests.m5.postgres_runtime.test_migration_017"),
    )

    prefix = "lane-b-live-promotion"
    with migration017._pre017_schema() as (connection, schema_name):
        snapshot = migration017._seed_b3_activated_snapshot(
            connection,
            prefix,
            complete_owner_survivor=True,
        )
        assert migration017.install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        migration017._register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = migration017._d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.partial_group_id,
            base=snapshot.epoch_id,
        )
        assert predecessor.certificate_digest is None
        open_prefix = f"{prefix}-open"
        event_id = f"{open_prefix}-event"
        payload_hash, _ = migration017._d26_structural_payload(
            connection,
            group_id=predecessor.group_id,
            event_id=event_id,
            action="RETIRE",
        )
        epoch_id, actual_event, actual_payload = migration017._open_b2_runtime_epoch(
            connection,
            open_prefix,
            snapshot.epoch_id,
            snapshot.policy_version,
            direct_bridge=False,
            update_kind="retire_group",
            payload_override=payload_hash,
        )
        assert (actual_event, actual_payload) == (event_id, payload_hash)
        migration017._stage_b2_group_deactivation(
            connection,
            epoch=epoch_id,
            event=event_id,
            group=predecessor.group_id,
            action="RETIRE",
        )
        absent_states = tuple(
            [
                *zip(
                    ("requirement_state",) * len(predecessor.requirement_ids),
                    predecessor.requirement_ids,
                    predecessor.requirement_state_hashes,
                    strict=True,
                ),
                (
                    "group_state",
                    predecessor.group_id,
                    predecessor.group_state_hash,
                ),
            ]
        )
        current_relations = (
            "groundloop_m5_matching_observation_current",
            "groundloop_m5_matching_edge_current",
            "groundloop_m5_matching_hash_mask_current",
            "groundloop_m5_matching_hall_current",
        )
        before_counts = tuple(
            int(
                connection.execute(
                    f"SELECT count(*) FROM {relation} WHERE group_version_id=%s",
                    (predecessor.group_id,),
                ).fetchone()[0]
            )
            for relation in current_relations
        )
        assert all(count > 0 for count in before_counts)
        migration017._apply_empty_b2_structural(
            connection,
            epoch=epoch_id,
            event=event_id,
            payload=payload_hash,
            base=snapshot.epoch_id,
            base_revision=snapshot.revision,
            policy=snapshot.policy_version,
            absent_states=absent_states,
            remove_current_group=predecessor.group_id,
        )
        connection.commit()

        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s,1)",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_epoch SET revision=2 WHERE epoch_id=%s AND revision=1",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_m5_runtime_epoch "
            "SET runtime_state='semantic_pending',revision=2 "
            "WHERE epoch_id=%s AND revision=1",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_m5_owner_pending_counter "
            "SET updated_revision=2 WHERE epoch_id=%s",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_m5_answer_pending_counter "
            "SET updated_revision=2 WHERE epoch_id=%s",
            (epoch_id,),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s,2)",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_epoch "
            "SET revision=3,semantic_status='complete',evaluation_state='complete' "
            "WHERE epoch_id=%s AND revision=2",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_m5_runtime_epoch "
            "SET runtime_state='semantic_complete',revision=3 "
            "WHERE epoch_id=%s AND revision=2",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_m5_owner_pending_counter "
            "SET updated_revision=3 WHERE epoch_id=%s",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_m5_answer_pending_counter "
            "SET updated_revision=3 WHERE epoch_id=%s",
            (epoch_id,),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        receipt = promote_matching_overlay(
            connection.cursor(),
            epoch_id=epoch_id,
            expected_revision=3,
            sealed_revision=4,
        )
        assert (
            receipt.observation_deletes,
            receipt.edge_deletes,
            receipt.mask_deletes,
            receipt.hall_deletes,
        ) == before_counts
        assert (
            receipt.observation_writes,
            receipt.edge_writes,
            receipt.mask_writes,
            receipt.hall_writes,
        ) == (0, 0, 0, 0)
        assert connection.execute(
            "SELECT installed_epoch_id,installed_revision "
            "FROM groundloop_m5_matching_image_current WHERE singleton"
        ).fetchone() == (epoch_id, 4)
        assert all(
            connection.execute(
                f"SELECT count(*) FROM {relation} WHERE group_version_id=%s",
                (predecessor.group_id,),
            ).fetchone()
            == (0,)
            for relation in current_relations
        )
        assert connection.execute(
            """SELECT
                 (SELECT count(*) FROM groundloop_m5_matching_observation_working
                   WHERE epoch_id=%s AND NOT present) +
                 (SELECT count(*) FROM groundloop_m5_matching_edge_working
                   WHERE epoch_id=%s AND refcount=0) +
                 (SELECT count(*) FROM groundloop_m5_matching_hash_mask_working
                   WHERE epoch_id=%s AND mask=0) +
                 (SELECT count(*) FROM groundloop_m5_matching_hall_working
                   WHERE epoch_id=%s AND NOT present)""",
            (epoch_id, epoch_id, epoch_id, epoch_id),
        ).fetchone() == (sum(before_counts),)
        with psycopg.connect(migration017._database_url()) as contender:
            contender.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(schema_name)
                )
            )
            contender.execute("SET lock_timeout='500ms'")
            with pytest.raises(psycopg.errors.LockNotAvailable):
                promote_matching_overlay(
                    contender.cursor(),
                    epoch_id=epoch_id,
                    expected_revision=3,
                    sealed_revision=4,
                )
            contender.rollback()
        connection.rollback()
        assert connection.execute(
            "SELECT installed_epoch_id,installed_revision "
            "FROM groundloop_m5_matching_image_current WHERE singleton"
        ).fetchone() == (snapshot.epoch_id, snapshot.revision)
