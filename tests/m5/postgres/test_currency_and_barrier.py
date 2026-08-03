"""Revision history and schema-owned activation-barrier integration tests."""

from __future__ import annotations

from typing import Any

import pytest
from psycopg import Connection, errors

from groundloop.domain import SubjectKind
from groundloop.errors import ValidationError
from groundloop.m5.domain import ClaimSupportKind
from groundloop.postgres.m5 import (
    build_m5_bootstrap_projection,
    read_m5_currency_at,
    replace_m5_working_currency,
)

from .helpers import (
    force_deferred_checks,
    insert_observation,
    install_current_currency,
    install_test_activation_barrier,
    open_m5_update,
    seed_base,
)


def test_first_currency_change_uses_published_fallback_and_rejects_redundancy(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="currency", chunk_texts=("old", "other"))
    for observation_id in ("currency-old", "currency-new"):
        insert_observation(
            m5_connection,
            observation_id=observation_id,
            subject_kind="claim",
            subject_id=base.claim_ids[0],
            chunk_id=base.chunk_ids[0],
            produced_epoch=base.epoch_id,
            task_type="nli",
        )
    install_current_currency(
        m5_connection,
        observation_id="currency-old",
        subject_kind="claim",
        subject_id=base.claim_ids[0],
        chunk_id=base.chunk_ids[0],
        task_type="nli",
        epoch_id=base.epoch_id,
    )
    install_test_activation_barrier(m5_connection, base)
    epoch_id = open_m5_update(m5_connection, base, revision=0)

    with pytest.raises(ValidationError, match="effective holder"):
        replace_m5_working_currency(
            m5_connection,
            epoch_id=epoch_id,
            subject_kind=SubjectKind.CLAIM,
            subject_id=base.claim_ids[0],
            chunk_version_id=base.chunk_ids[0],
            task_type="nli",
            observation_id="currency-old",
            revision=0,
        )

    replace_m5_working_currency(
        m5_connection,
        epoch_id=epoch_id,
        subject_kind=SubjectKind.CLAIM,
        subject_id=base.claim_ids[0],
        chunk_version_id=base.chunk_ids[0],
        task_type="nli",
        observation_id="currency-new",
        revision=0,
    )
    key = (SubjectKind.CLAIM, base.claim_ids[0], base.chunk_ids[0], "nli")
    assert (
        read_m5_currency_at(m5_connection, epoch_id=epoch_id, revision=0)[key]
        == "currency-new"
    )

    m5_connection.execute(
        "UPDATE groundloop_epoch SET revision = 1 WHERE epoch_id = %s", (epoch_id,)
    )
    replace_m5_working_currency(
        m5_connection,
        epoch_id=epoch_id,
        subject_kind=SubjectKind.CLAIM,
        subject_id=base.claim_ids[0],
        chunk_version_id=base.chunk_ids[0],
        task_type="nli",
        observation_id=None,
        revision=1,
    )
    assert (
        read_m5_currency_at(m5_connection, epoch_id=epoch_id, revision=1)[key] is None
    )
    rows = m5_connection.execute(
        """
        SELECT observation_id, valid_from_revision, valid_to_revision
        FROM groundloop_m5_working_currency_history
        WHERE epoch_id = %s
        ORDER BY valid_from_revision
        """,
        (epoch_id,),
    ).fetchall()
    assert rows == [("currency-new", 0, 1), (None, 1, None)]
    force_deferred_checks(m5_connection)


def test_first_tombstone_close_once_ineligible_and_terminal_guards(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="tombstone", chunk_texts=("one", "two"))
    insert_observation(
        m5_connection,
        observation_id="tombstone-old",
        subject_kind="claim",
        subject_id=base.claim_ids[0],
        chunk_id=base.chunk_ids[0],
        produced_epoch=base.epoch_id,
        task_type="nli",
    )
    install_current_currency(
        m5_connection,
        observation_id="tombstone-old",
        subject_kind="claim",
        subject_id=base.claim_ids[0],
        chunk_id=base.chunk_ids[0],
        task_type="nli",
        epoch_id=base.epoch_id,
    )
    insert_observation(
        m5_connection,
        observation_id="tombstone-ineligible",
        subject_kind="claim",
        subject_id=base.claim_ids[0],
        chunk_id=base.chunk_ids[1],
        produced_epoch=base.epoch_id,
        task_type="nli",
        eligible=False,
    )
    install_test_activation_barrier(m5_connection, base)
    epoch_id = open_m5_update(m5_connection, base, revision=1)
    replace_m5_working_currency(
        m5_connection,
        epoch_id=epoch_id,
        subject_kind=SubjectKind.CLAIM,
        subject_id=base.claim_ids[0],
        chunk_version_id=base.chunk_ids[0],
        task_type="nli",
        observation_id=None,
        revision=1,
    )
    key = (SubjectKind.CLAIM, base.claim_ids[0], base.chunk_ids[0], "nli")
    assert (
        read_m5_currency_at(m5_connection, epoch_id=epoch_id, revision=1)[key] is None
    )

    m5_connection.execute(
        "UPDATE groundloop_epoch SET revision = 2 WHERE epoch_id = %s", (epoch_id,)
    )
    with pytest.raises(errors.RaiseException, match="not eligible"):
        with m5_connection.transaction():
            replace_m5_working_currency(
                m5_connection,
                epoch_id=epoch_id,
                subject_kind=SubjectKind.CLAIM,
                subject_id=base.claim_ids[0],
                chunk_version_id=base.chunk_ids[1],
                task_type="nli",
                observation_id="tombstone-ineligible",
                revision=2,
            )

    replace_m5_working_currency(
        m5_connection,
        epoch_id=epoch_id,
        subject_kind=SubjectKind.CLAIM,
        subject_id=base.claim_ids[0],
        chunk_version_id=base.chunk_ids[0],
        task_type="nli",
        observation_id="tombstone-old",
        revision=2,
    )
    m5_connection.execute(
        "UPDATE groundloop_epoch SET revision = 3 WHERE epoch_id = %s", (epoch_id,)
    )

    with pytest.raises(errors.RaiseException, match="close exactly once"):
        with m5_connection.transaction():
            m5_connection.execute(
                """
                UPDATE groundloop_m5_working_currency_history
                SET valid_to_revision = 3
                WHERE epoch_id = %s AND valid_from_revision = 1
                """,
                (epoch_id,),
            )

    m5_connection.execute(
        """
        UPDATE groundloop_epoch
        SET semantic_status = 'failed', evaluation_state = 'failed'
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    )
    with pytest.raises(errors.RaiseException, match="terminal or unopened"):
        with m5_connection.transaction():
            replace_m5_working_currency(
                m5_connection,
                epoch_id=epoch_id,
                subject_kind=SubjectKind.CLAIM,
                subject_id=base.claim_ids[0],
                chunk_version_id=base.chunk_ids[1],
                task_type="nli",
                observation_id="tombstone-ineligible",
                revision=3,
            )


def test_bootstrap_projection_is_digest_neutral_and_builds_none_direct_states(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="bootstrap", claim_count=2)
    insert_observation(
        m5_connection,
        observation_id="bootstrap-direct",
        subject_kind="claim",
        subject_id=base.claim_ids[0],
        chunk_id=base.chunk_ids[0],
        produced_epoch=base.epoch_id,
        task_type="nli",
    )
    install_current_currency(
        m5_connection,
        observation_id="bootstrap-direct",
        subject_kind="claim",
        subject_id=base.claim_ids[0],
        chunk_id=base.chunk_ids[0],
        task_type="nli",
        epoch_id=base.epoch_id,
    )
    projection = build_m5_bootstrap_projection(m5_connection)
    assert projection.epoch_id == base.epoch_id
    assert projection.claim_certificates[base.claim_ids[0]].support_kind is (
        ClaimSupportKind.DIRECT
    )
    assert projection.claim_certificates[base.claim_ids[1]].support_kind is (
        ClaimSupportKind.NONE
    )
    assert not hasattr(projection, "bootstrap_state_hash")


def test_runtime_mode_blocks_v1_open_and_typed_open_requires_equal_active_heads(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="barrier")
    with pytest.raises(errors.RaiseException, match="requires activated"):
        with m5_connection.transaction():
            row = m5_connection.execute(
                """
                INSERT INTO groundloop_epoch (
                    event_id, payload_hash, revision, structural_status,
                    semantic_status, evaluation_state, publication_mode
                ) VALUES (
                    'typed-before-active', %s, 0, 'committed', 'pending',
                    'pending', 'provisional'
                ) RETURNING epoch_id
                """,
                ("b" * 64,),
            ).fetchone()
            assert row is not None
            m5_connection.execute(
                """
                INSERT INTO groundloop_m5_update (
                    epoch_id, update_kind, previous_published_epoch_id,
                    decision_policy_version, manifest
                ) VALUES (%s, 'observe_requirement', %s, %s, '{}'::jsonb)
                """,
                (int(row[0]), base.epoch_id, base.policy_version),
            )

    install_test_activation_barrier(m5_connection, base)
    with pytest.raises(errors.RaiseException, match="disabled after M5 activation"):
        with m5_connection.transaction():
            row = m5_connection.execute(
                """
                INSERT INTO groundloop_epoch (
                    event_id, payload_hash, revision, structural_status,
                    semantic_status, evaluation_state, publication_mode
                ) VALUES (
                    'legacy-after-active', %s, 0, 'committed', 'pending',
                    'pending', 'provisional'
                ) RETURNING epoch_id
                """,
                ("c" * 64,),
            ).fetchone()
            assert row is not None
            m5_connection.execute(
                """
                INSERT INTO groundloop_m4_update (
                    epoch_id, update_kind, candidate_policy_id,
                    previous_published_epoch_id, registry_snapshot_id, manifest
                ) VALUES (%s, 'insert', 'missing-policy', %s,
                          'missing-registry', '{}'::jsonb)
                """,
                (int(row[0]), base.epoch_id),
            )

    epoch_id = open_m5_update(m5_connection, base, event_id="typed-after-active")
    assert m5_connection.execute(
        """
        SELECT previous_published_epoch_id
        FROM groundloop_m5_update
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone() == (base.epoch_id,)
