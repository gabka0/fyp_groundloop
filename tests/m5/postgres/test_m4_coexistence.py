"""Live M4 claim-only behavior beside M5 requirement currency."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg import Connection, sql

from groundloop.m4.application import DynamicEventPlan
from groundloop.m4.contracts import CorpusUpdateIdentity, UpdateKind
from groundloop.m4.pipeline import PostgresM4ApplicationPorts, bootstrap_m4_publication
from groundloop.postgres.migrations import (
    apply_legacy_migrations,
    install_m5_core_bundle,
)

from .conftest import M5Schema
from .helpers import (
    force_deferred_checks,
    insert_observation,
    insert_published_group,
    install_current_currency,
    make_group,
    seed_base,
    seed_candidate_policy,
    sha,
)


@contextmanager
def _m5_autocommit_schema(dsn: str) -> Iterator[Connection[Any]]:
    schema_name = f"groundloop_m5_coexist_{uuid.uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
    try:
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(schema_name)
                )
            )
            with connection.transaction():
                apply_legacy_migrations(connection)
            install_m5_core_bundle(connection)
            yield connection
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )


def test_m4_bootstrap_reconnect_withdrawal_publication_and_replay_ignore_requirements(
    m5_schema: M5Schema,
) -> None:
    with _m5_autocommit_schema(m5_schema.dsn) as connection:
        with connection.transaction():
            base = seed_base(
                connection,
                prefix="m4-m5-coexist",
                chunk_texts=("shared evidence",),
            )
            connection.execute("DELETE FROM groundloop_m4_publication_head")
            candidate_policy_id = seed_candidate_policy(
                connection,
                base,
                prefix="m4-m5-coexist",
            )
            registry_snapshot_id = "m4-m5-coexist-registry"
            connection.execute(
                """
                INSERT INTO groundloop_m4_claim_registry_snapshot (
                    claim_registry_snapshot_id, claim_count, claim_set_hash
                ) VALUES (%s, 1, %s)
                """,
                (registry_snapshot_id, sha(base.claim_ids[0])),
            )
            connection.execute(
                """
                INSERT INTO groundloop_m4_claim_registry_member (
                    claim_registry_snapshot_id, claim_id, member_ordinal
                ) VALUES (%s, %s, 0)
                """,
                (registry_snapshot_id, base.claim_ids[0]),
            )
            insert_observation(
                connection,
                observation_id="m4-m5-claim-observation-old",
                subject_kind="claim",
                subject_id=base.claim_ids[0],
                chunk_id=base.chunk_ids[0],
                produced_epoch=base.epoch_id,
                task_type="verify_claim_v1",
            )
            install_current_currency(
                connection,
                observation_id="m4-m5-claim-observation-old",
                subject_kind="claim",
                subject_id=base.claim_ids[0],
                chunk_id=base.chunk_ids[0],
                task_type="verify_claim_v1",
                epoch_id=base.epoch_id,
                publish=False,
            )
            group = make_group(
                group_id="m4-m5-coexist-group",
                family_id="m4-m5-coexist-family",
                claim_id=base.claim_ids[0],
                texts=("coexisting requirement",),
            )
            insert_published_group(
                connection,
                group=group,
                epoch_id=base.epoch_id,
            )
            requirement_id = group.requirements[0].requirement_version_id
            insert_observation(
                connection,
                observation_id="m4-m5-requirement-observation",
                subject_kind="requirement",
                subject_id=requirement_id,
                chunk_id=base.chunk_ids[0],
                produced_epoch=base.epoch_id,
            )
            install_current_currency(
                connection,
                observation_id="m4-m5-requirement-observation",
                subject_kind="requirement",
                subject_id=requirement_id,
                chunk_id=base.chunk_ids[0],
                task_type="verify_requirement_v1",
                epoch_id=base.epoch_id,
                publish=False,
            )
            force_deferred_checks(connection)

        bootstrap_m4_publication(connection, sealed_epoch_id=base.epoch_id)
        published_subjects = connection.execute(
            """
            SELECT subject_kind::text, observation_id
            FROM groundloop_published_observation_currency
            ORDER BY subject_kind, observation_id
            """
        ).fetchall()
        assert published_subjects == [("claim", "m4-m5-claim-observation-old")]
        assert connection.execute(
            """
            SELECT observation_id
            FROM groundloop_observation_currency
            WHERE subject_kind = 'requirement'
            """
        ).fetchall() == [("m4-m5-requirement-observation",)]

        ports = PostgresM4ApplicationPorts(connection, structural_payloads={})
        published_snapshot = ports._published_repository.export_snapshot()
        assert tuple(
            observation.observation_id
            for observation in published_snapshot.observations
        ) == ("m4-m5-claim-observation-old",)

        with connection.transaction():
            insert_observation(
                connection,
                observation_id="m4-m5-claim-observation-new",
                subject_kind="claim",
                subject_id=base.claim_ids[0],
                chunk_id=base.chunk_ids[0],
                produced_epoch=base.epoch_id,
                task_type="verify_claim_v1",
            )
            epoch_row = connection.execute(
                """
                INSERT INTO groundloop_epoch (
                    event_id, payload_hash, revision, structural_status,
                    semantic_status, evaluation_state, publication_mode
                ) VALUES (
                    'm4-m5-coexist-delete', %s, 1, 'committed',
                    'pending', 'pending', 'provisional'
                ) RETURNING epoch_id
                """,
                (sha("m4-m5-coexist-delete-event"),),
            ).fetchone()
            assert epoch_row is not None
            epoch_id = int(epoch_row[0])
            connection.execute(
                """
                INSERT INTO groundloop_m4_update (
                    epoch_id, update_kind, candidate_policy_id,
                    previous_published_epoch_id, registry_snapshot_id, manifest
                ) VALUES (%s, 'delete', %s, %s, %s, '{}'::jsonb)
                """,
                (
                    epoch_id,
                    candidate_policy_id,
                    base.epoch_id,
                    registry_snapshot_id,
                ),
            )
            connection.execute(
                """
                INSERT INTO groundloop_working_observation_delta (
                    epoch_id, subject_kind, subject_id, chunk_version_id,
                    task_type, base_observation_id, working_observation_id,
                    installed_revision
                ) VALUES
                    (%s, 'claim', %s, %s, 'verify_claim_v1',
                     'm4-m5-claim-observation-old',
                     'm4-m5-claim-observation-new', 1),
                    (%s, 'requirement', %s, %s, 'verify_requirement_v1',
                     'm4-m5-requirement-observation', NULL, 1)
                """,
                (
                    epoch_id,
                    base.claim_ids[0],
                    base.chunk_ids[0],
                    epoch_id,
                    requirement_id,
                    base.chunk_ids[0],
                ),
            )

        event = DynamicEventPlan(
            CorpusUpdateIdentity(
                "m4-m5-coexist-delete",
                sha("m4-m5-coexist-delete-event"),
                UpdateKind.DELETE,
                base.epoch_id,
                candidate_policy_id,
            ),
            (),
            (base.chunk_ids[0],),
            (base.claim_ids[0],),
            registry_snapshot_id,
        )
        first_plan = ports.plan_exact_withdrawal(event)
        second_plan = ports.plan_exact_withdrawal(event)
        assert first_plan == second_plan
        assert first_plan.plan.observation_ids == ("m4-m5-claim-observation-old",)

        with connection.transaction():
            with connection.cursor() as cursor:
                ports._promote_observation_currency(cursor, epoch_id)
            connection.execute(
                """
                UPDATE groundloop_m4_publication_head
                SET epoch_id = %s, updated_at = now()
                WHERE singleton
                """,
                (epoch_id,),
            )
            connection.execute(
                """
                UPDATE groundloop_epoch
                SET semantic_status = 'sealed', evaluation_state = 'complete',
                    publication_mode = 'strict', sealed_at = now()
                WHERE epoch_id = %s
                """,
                (epoch_id,),
            )

        assert connection.execute(
            """
            SELECT observation_id, installed_revision
            FROM groundloop_observation_currency
            WHERE subject_kind = 'claim'
            """
        ).fetchone() == ("m4-m5-claim-observation-new", epoch_id)
        assert connection.execute(
            """
            SELECT observation_id
            FROM groundloop_observation_currency
            WHERE subject_kind = 'requirement'
            """
        ).fetchone() == ("m4-m5-requirement-observation",)
        assert (
            connection.execute(
                """
            SELECT valid_to_epoch
            FROM groundloop_published_observation_currency
            WHERE subject_kind = 'requirement'
            """
            ).fetchall()
            == []
        )

        restarted = PostgresM4ApplicationPorts(connection, structural_payloads={})
        restarted_snapshot = restarted._published_repository.export_snapshot()
        assert tuple(
            observation.observation_id
            for observation in restarted_snapshot.observations
        ) == ("m4-m5-claim-observation-new",)
        assert restarted.plan_exact_withdrawal(event) == first_plan
