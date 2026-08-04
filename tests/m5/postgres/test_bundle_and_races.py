"""Atomic bundle installation and serialized migration/runtime barriers."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any

import psycopg
import pytest
from psycopg import Connection, errors, sql

from groundloop.postgres.m5 import (
    build_m5_bootstrap_projection,
    read_m5_mismatch_counts,
    write_m5_materialized_states,
)
from groundloop.postgres.migrations import (
    M5_ORACLE_PATH,
    M5BundleHashConflictError,
    M5PrerequisiteError,
    apply_legacy_migrations,
    install_m5_core_bundle,
)

from .conftest import M5Schema
from .helpers import (
    force_deferred_checks,
    insert_observation,
    insert_published_group,
    install_current_currency,
    install_test_activation_barrier,
    make_group,
    seed_base,
    seed_candidate_policy,
    sha,
)


@contextmanager
def _legacy_schema(dsn: str) -> Iterator[str]:
    schema_name = f"groundloop_m5_bundle_{uuid.uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
    try:
        with psycopg.connect(dsn) as connection:
            _select_schema(connection, schema_name)
            with connection.transaction():
                apply_legacy_migrations(connection)
        yield schema_name
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )


def _select_schema(connection: Connection[Any], schema_name: str) -> None:
    connection.execute(
        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
    )
    connection.commit()


def test_populated_upgrade_is_ledgered_replayable_and_hash_conflict_safe(
    m5_schema: M5Schema,
) -> None:
    with _legacy_schema(m5_schema.dsn) as schema_name:
        with psycopg.connect(m5_schema.dsn) as connection:
            _select_schema(connection, schema_name)
            base = seed_base(connection, prefix="populated-upgrade")
            connection.execute(
                """
                INSERT INTO groundloop_semantic_observation (
                    observation_id, subject_kind, subject_id,
                    chunk_version_id, task_type, support_score,
                    refute_score, neutral_score, model_id, model_version,
                    prompt_version, input_hash, produced_epoch, raw_output_hash
                ) VALUES (
                    'pre-m5-observation', 'claim', %s, %s, 'nli',
                    0.9, 0.05, 0.05, 'fixture', 'v1', 'p1', %s, %s, NULL
                )
                """,
                (
                    base.claim_ids[0],
                    base.chunk_ids[0],
                    sha("pre-m5-input"),
                    base.epoch_id,
                ),
            )
            connection.commit()

            installed = install_m5_core_bundle(connection)
            assert installed.applied
            connection.commit()
            ledger = connection.execute(
                """
                SELECT bundle_sha256, migration_sha256, oracle_sha256,
                       prerequisite_sha256
                FROM groundloop_m5_schema_bundle
                WHERE bundle_id = %s
                """,
                (installed.identity.bundle_id,),
            ).fetchone()
            assert ledger == (
                installed.identity.bundle_sha256,
                installed.identity.migration_sha256,
                installed.identity.oracle_sha256,
                installed.identity.prerequisite_source_sha256,
            )
            assert connection.execute(
                """
                SELECT subject_kind::text, subject_id
                FROM groundloop_semantic_subject
                WHERE subject_kind = 'claim' AND subject_id = %s
                """,
                (base.claim_ids[0],),
            ).fetchone() == ("claim", base.claim_ids[0])
            assert connection.execute(
                """
                SELECT eligible_for_currency
                FROM groundloop_semantic_observation
                WHERE observation_id = 'pre-m5-observation'
                """
            ).fetchone() == (True,)

            connection.commit()
            replay = install_m5_core_bundle(connection)
            assert not replay.applied
            assert replay.identity == installed.identity
            connection.commit()

            conflicting_oracle = M5_ORACLE_PATH.read_bytes() + b"\n-- conflict\n"
            with pytest.raises(M5BundleHashConflictError):
                install_m5_core_bundle(
                    connection,
                    oracle_bytes=conflicting_oracle,
                )
            connection.rollback()
            assert connection.execute(
                "SELECT count(*) FROM groundloop_m5_schema_bundle"
            ).fetchone() == (1,)
            extensions = connection.execute(
                """
                SELECT extname
                FROM pg_extension
                WHERE extname IN ('btree_gist', 'pgcrypto')
                ORDER BY extname
                """
            ).fetchall()
            assert extensions == [("btree_gist",), ("pgcrypto",)]


def test_dropped_critical_trigger_rejects_bundle_before_014_or_ledger(
    m5_schema: M5Schema,
) -> None:
    with _legacy_schema(m5_schema.dsn) as schema_name:
        with psycopg.connect(m5_schema.dsn) as connection:
            _select_schema(connection, schema_name)
            connection.execute(
                """
                DROP TRIGGER groundloop_working_observation_delta_immutable
                ON groundloop_working_observation_delta
                """
            )
            with pytest.raises(
                M5PrerequisiteError,
                match="critical trigger catalog is not exact",
            ):
                install_m5_core_bundle(connection)
            assert connection.execute(
                "SELECT to_regclass('groundloop_m5_schema_bundle')"
            ).fetchone() == (None,)
            assert connection.execute(
                "SELECT to_regclass('groundloop_m5_group_version')"
            ).fetchone() == (None,)


def test_disabled_critical_trigger_is_a_catalog_near_miss_and_fails_closed(
    m5_schema: M5Schema,
) -> None:
    with _legacy_schema(m5_schema.dsn) as schema_name:
        with psycopg.connect(m5_schema.dsn) as connection:
            _select_schema(connection, schema_name)
            connection.execute(
                """
                ALTER TABLE groundloop_semantic_job
                DISABLE TRIGGER groundloop_semantic_job_open_count
                """
            )
            with pytest.raises(
                M5PrerequisiteError,
                match="critical trigger catalog is not exact",
            ):
                install_m5_core_bundle(connection)
            assert connection.execute(
                "SELECT to_regclass('groundloop_m5_schema_bundle')"
            ).fetchone() == (None,)


def test_bootstrap_publish_true_commits_and_reloads_all_states_and_bindings(
    m5_schema: M5Schema,
) -> None:
    with _legacy_schema(m5_schema.dsn) as schema_name:
        with psycopg.connect(m5_schema.dsn) as connection:
            _select_schema(connection, schema_name)
            install_m5_core_bundle(connection)
            base = seed_base(
                connection,
                prefix="committed-bootstrap",
                claim_count=2,
                chunk_texts=("bootstrap evidence",),
                epoch_revision=3,
            )
            group = make_group(
                group_id="committed-bootstrap-group",
                family_id="committed-bootstrap-family",
                claim_id=base.claim_ids[0],
                texts=("bootstrap evidence",),
            )
            insert_published_group(
                connection,
                group=group,
                epoch_id=base.epoch_id,
            )
            requirement_id = group.requirements[0].requirement_version_id
            insert_observation(
                connection,
                observation_id="committed-bootstrap-observation",
                subject_kind="requirement",
                subject_id=requirement_id,
                chunk_id=base.chunk_ids[0],
                produced_epoch=base.epoch_id,
            )
            install_current_currency(
                connection,
                observation_id="committed-bootstrap-observation",
                subject_kind="requirement",
                subject_id=requirement_id,
                chunk_id=base.chunk_ids[0],
                task_type="verify_requirement_v1",
                epoch_id=base.epoch_id,
                revision=3,
            )
            projection = build_m5_bootstrap_projection(connection)
            write_m5_materialized_states(
                connection,
                states=projection.states,
                decision_policy_version=projection.decision_policy_version,
                epoch_id=projection.epoch_id,
                revision=projection.revision,
                group_certificates=projection.group_certificates,
                claim_certificates=projection.claim_certificates,
                publish=True,
            )
            force_deferred_checks(connection)
            connection.commit()

        with psycopg.connect(m5_schema.dsn) as connection:
            _select_schema(connection, schema_name)
            expected_counts = {
                "groundloop_m5_published_requirement_state": 1,
                "groundloop_m5_published_group_state": 1,
                "groundloop_m5_published_claim_state": 2,
                "groundloop_m5_published_answer_state": 1,
                "groundloop_m5_published_group_certificate_binding": 1,
                "groundloop_m5_published_claim_certificate_binding": 2,
            }
            for relation, expected_count in expected_counts.items():
                row = connection.execute(f"SELECT count(*) FROM {relation}").fetchone()
                assert row == (expected_count,)
            requirement = connection.execute(
                """
                SELECT requirement_version_id, witness_count, satisfied,
                       sealed_revision, decision_policy_version
                FROM groundloop_m5_published_requirement_state
                """
            ).fetchone()
            assert requirement == (
                requirement_id,
                1,
                True,
                3,
                base.policy_version,
            )
            group_row = connection.execute(
                """
                SELECT complete, certificate_digest
                FROM groundloop_m5_published_group_state
                WHERE group_version_id = %s
                """,
                (group.group_version_id,),
            ).fetchone()
            assert group_row is not None and group_row[0] is True
            assert (
                str(group_row[1]).strip()
                == projection.group_certificates[
                    group.group_version_id
                ].certificate_digest
            )
            claim_rows = connection.execute(
                """
                SELECT claim_id, status, certificate_digest
                FROM groundloop_m5_published_claim_state
                ORDER BY claim_id
                """
            ).fetchall()
            assert [str(row[0]) for row in claim_rows] == list(base.claim_ids)
            assert all(str(row[2]).strip() for row in claim_rows)
            assert connection.execute(
                """
                SELECT answer_version_id, status
                FROM groundloop_m5_published_answer_state
                """
            ).fetchone() == (
                base.answer_id,
                projection.states.answers[base.answer_id].status.value,
            )
            assert read_m5_mismatch_counts(connection) == type(
                read_m5_mismatch_counts(connection)
            )(0, 0, 0, 0, 0, 0, 0)


@pytest.mark.parametrize("failure_stage", ["after_schema", "after_oracle"])
def test_mid_bundle_failure_rolls_back_every_object_and_allows_retry(
    m5_schema: M5Schema,
    failure_stage: str,
) -> None:
    with _legacy_schema(m5_schema.dsn) as schema_name:
        with psycopg.connect(m5_schema.dsn) as connection:
            _select_schema(connection, schema_name)

            def fail(stage: str) -> None:
                if stage == failure_stage:
                    raise RuntimeError(f"injected {stage}")

            with pytest.raises(RuntimeError, match=failure_stage):
                install_m5_core_bundle(connection, failure_injector=fail)
            connection.rollback()
            assert connection.execute(
                "SELECT to_regclass('groundloop_m5_schema_bundle')"
            ).fetchone() == (None,)
            assert connection.execute(
                "SELECT to_regclass('groundloop_m5_group_state_oracle')"
            ).fetchone() == (None,)
            connection.commit()

            result = install_m5_core_bundle(connection)
            assert result.applied
            connection.commit()


def test_open_epoch_rejection_is_atomic_and_retry_succeeds_when_terminal(
    m5_schema: M5Schema,
) -> None:
    with _legacy_schema(m5_schema.dsn) as schema_name:
        with psycopg.connect(m5_schema.dsn) as connection:
            _select_schema(connection, schema_name)
            row = connection.execute(
                """
                INSERT INTO groundloop_epoch (
                    event_id, payload_hash, structural_status,
                    semantic_status, evaluation_state, publication_mode
                ) VALUES (
                    'open-before-m5', %s, 'committed', 'pending',
                    'pending', 'provisional'
                ) RETURNING epoch_id
                """,
                (sha("open-before-m5"),),
            ).fetchone()
            assert row is not None
            epoch_id = int(row[0])
            connection.commit()

            with pytest.raises(
                errors.RaiseException,
                match="requires no committed pending/complete epoch",
            ):
                install_m5_core_bundle(connection)
            connection.rollback()
            assert connection.execute(
                "SELECT to_regclass('groundloop_m5_schema_bundle')"
            ).fetchone() == (None,)
            connection.execute(
                """
                UPDATE groundloop_epoch
                SET structural_status = 'failed', semantic_status = 'failed',
                    evaluation_state = 'failed'
                WHERE epoch_id = %s
                """,
                (epoch_id,),
            )
            connection.commit()
            assert install_m5_core_bundle(connection).applied
            connection.commit()


def test_bundle_access_exclusive_lock_serializes_concurrent_epoch_writer(
    m5_schema: M5Schema,
) -> None:
    with _legacy_schema(m5_schema.dsn) as schema_name:
        migration_locked = threading.Event()
        release_migration = threading.Event()
        writer_finished = threading.Event()

        def installer() -> bool:
            with psycopg.connect(m5_schema.dsn) as connection:
                _select_schema(connection, schema_name)

                def hold(stage: str) -> None:
                    if stage == "after_schema":
                        migration_locked.set()
                        assert release_migration.wait(timeout=10)

                result = install_m5_core_bundle(
                    connection,
                    failure_injector=hold,
                )
                connection.commit()
                return result.applied

        def writer() -> int:
            assert migration_locked.wait(timeout=10)
            with psycopg.connect(m5_schema.dsn) as connection:
                _select_schema(connection, schema_name)
                row = connection.execute(
                    """
                    INSERT INTO groundloop_epoch (
                        event_id, payload_hash, structural_status,
                        semantic_status, evaluation_state, publication_mode,
                        sealed_at
                    ) VALUES (
                        'serialized-writer', %s, 'committed', 'sealed',
                        'complete', 'strict', now()
                    ) RETURNING epoch_id
                    """,
                    (sha("serialized-writer"),),
                ).fetchone()
                assert row is not None
                connection.commit()
                writer_finished.set()
                return int(row[0])

        with ThreadPoolExecutor(max_workers=2) as executor:
            install_future = executor.submit(installer)
            assert migration_locked.wait(timeout=10)
            writer_future = executor.submit(writer)
            assert not writer_finished.wait(timeout=0.25)
            release_migration.set()
            assert install_future.result(timeout=20)
            writer_epoch = writer_future.result(timeout=20)

        with psycopg.connect(m5_schema.dsn) as connection:
            _select_schema(connection, schema_name)
            assert connection.execute(
                "SELECT event_id FROM groundloop_epoch WHERE epoch_id = %s",
                (writer_epoch,),
            ).fetchone() == ("serialized-writer",)


def test_two_concurrent_installers_apply_once_then_replay_same_identity(
    m5_schema: M5Schema,
) -> None:
    with _legacy_schema(m5_schema.dsn) as schema_name:
        first_has_schema = threading.Event()
        release_first = threading.Event()
        second_started = threading.Event()

        def first_installer() -> Any:
            with psycopg.connect(m5_schema.dsn) as connection:
                _select_schema(connection, schema_name)

                def hold(stage: str) -> None:
                    if stage == "after_schema":
                        first_has_schema.set()
                        assert release_first.wait(timeout=10)

                result = install_m5_core_bundle(
                    connection,
                    failure_injector=hold,
                )
                connection.commit()
                return result

        def second_installer() -> Any:
            assert first_has_schema.wait(timeout=10)
            second_started.set()
            with psycopg.connect(m5_schema.dsn) as connection:
                _select_schema(connection, schema_name)
                result = install_m5_core_bundle(connection)
                connection.commit()
                return result

        with ThreadPoolExecutor(max_workers=2) as executor:
            first_future = executor.submit(first_installer)
            assert first_has_schema.wait(timeout=10)
            second_future = executor.submit(second_installer)
            assert second_started.wait(timeout=10)
            assert not second_future.done()
            release_first.set()
            first = first_future.result(timeout=20)
            second = second_future.result(timeout=20)

        assert first.applied
        assert not second.applied
        assert first.identity == second.identity
        with psycopg.connect(m5_schema.dsn) as connection:
            _select_schema(connection, schema_name)
            assert connection.execute(
                "SELECT count(*) FROM groundloop_m5_schema_bundle"
            ).fetchone() == (1,)


def test_v1_open_wins_mode_lock_then_activation_observes_durable_open_epoch(
    m5_schema: M5Schema,
) -> None:
    with _legacy_schema(m5_schema.dsn) as schema_name:
        with psycopg.connect(m5_schema.dsn) as setup:
            _select_schema(setup, schema_name)
            install_m5_core_bundle(setup)
            base = seed_base(setup, prefix="v1-wins")
            candidate_policy_id = seed_candidate_policy(
                setup,
                base,
                prefix="v1-wins",
            )
            setup.commit()

        v1_durable = threading.Event()
        release_v1 = threading.Event()
        activation_locked = threading.Event()

        def v1_open() -> int:
            with psycopg.connect(m5_schema.dsn) as connection:
                _select_schema(connection, schema_name)
                row = connection.execute(
                    """
                    INSERT INTO groundloop_epoch (
                        event_id, payload_hash, structural_status,
                        semantic_status, evaluation_state, publication_mode
                    ) VALUES (
                        'v1-wins-open', %s, 'committed', 'pending',
                        'pending', 'provisional'
                    ) RETURNING epoch_id
                    """,
                    (sha("v1-wins-open"),),
                ).fetchone()
                assert row is not None
                epoch_id = int(row[0])
                connection.execute(
                    """
                    INSERT INTO groundloop_m4_update (
                        epoch_id, update_kind, candidate_policy_id,
                        previous_published_epoch_id, registry_snapshot_id,
                        manifest
                    ) VALUES (%s, 'insert', %s, %s, %s, '{}'::jsonb)
                    """,
                    (
                        epoch_id,
                        candidate_policy_id,
                        base.epoch_id,
                        "v1-wins-registry",
                    ),
                )
                v1_durable.set()
                assert release_v1.wait(timeout=10)
                connection.commit()
                return epoch_id

        def activation_attempt() -> bool:
            assert v1_durable.wait(timeout=10)
            with psycopg.connect(m5_schema.dsn) as connection:
                _select_schema(connection, schema_name)
                connection.execute(
                    """
                    SELECT mode FROM groundloop_runtime_mode
                    WHERE singleton FOR UPDATE
                    """
                ).fetchone()
                activation_locked.set()
                has_open = connection.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM groundloop_epoch
                        WHERE structural_status = 'committed'
                          AND semantic_status IN ('pending', 'complete')
                    )
                    """
                ).fetchone()
                connection.rollback()
                assert has_open is not None
                return bool(has_open[0])

        with ThreadPoolExecutor(max_workers=2) as executor:
            v1_future = executor.submit(v1_open)
            assert v1_durable.wait(timeout=10)
            activation_future = executor.submit(activation_attempt)
            assert not activation_locked.wait(timeout=0.25)
            release_v1.set()
            epoch_id = v1_future.result(timeout=20)
            assert activation_future.result(timeout=20)

        with psycopg.connect(m5_schema.dsn) as connection:
            _select_schema(connection, schema_name)
            assert connection.execute(
                "SELECT semantic_status FROM groundloop_epoch WHERE epoch_id = %s",
                (epoch_id,),
            ).fetchone() == ("pending",)


def test_activation_wins_mode_lock_and_rolls_back_losing_v1_open(
    m5_schema: M5Schema,
) -> None:
    with _legacy_schema(m5_schema.dsn) as schema_name:
        with psycopg.connect(m5_schema.dsn) as setup:
            _select_schema(setup, schema_name)
            install_m5_core_bundle(setup)
            base = seed_base(setup, prefix="activation-wins")
            candidate_policy_id = seed_candidate_policy(
                setup,
                base,
                prefix="activation-wins",
            )
            setup.commit()

        activation_ready = threading.Event()
        release_activation = threading.Event()
        v1_finished = threading.Event()

        def activation() -> None:
            with psycopg.connect(m5_schema.dsn) as connection:
                _select_schema(connection, schema_name)
                connection.execute(
                    """
                    SELECT mode FROM groundloop_runtime_mode
                    WHERE singleton FOR UPDATE
                    """
                ).fetchone()
                install_test_activation_barrier(
                    connection,
                    base,
                    activation_id="activation-wins-id",
                )
                activation_ready.set()
                assert release_activation.wait(timeout=10)
                connection.commit()

        def losing_v1_open() -> str:
            assert activation_ready.wait(timeout=10)
            try:
                with psycopg.connect(m5_schema.dsn) as connection:
                    _select_schema(connection, schema_name)
                    row = connection.execute(
                        """
                        INSERT INTO groundloop_epoch (
                            event_id, payload_hash, structural_status,
                            semantic_status, evaluation_state, publication_mode
                        ) VALUES (
                            'activation-wins-loser', %s, 'committed',
                            'pending', 'pending', 'provisional'
                        ) RETURNING epoch_id
                        """,
                        (sha("activation-wins-loser"),),
                    ).fetchone()
                    assert row is not None
                    connection.execute(
                        """
                        INSERT INTO groundloop_m4_update (
                            epoch_id, update_kind, candidate_policy_id,
                            previous_published_epoch_id, registry_snapshot_id,
                            manifest
                        ) VALUES (%s, 'insert', %s, %s, %s, '{}'::jsonb)
                        """,
                        (
                            int(row[0]),
                            candidate_policy_id,
                            base.epoch_id,
                            "activation-wins-registry",
                        ),
                    )
            except errors.RaiseException as error:
                v1_finished.set()
                return str(error)
            raise AssertionError("v1 open unexpectedly crossed M5 activation")

        with ThreadPoolExecutor(max_workers=2) as executor:
            activation_future = executor.submit(activation)
            assert activation_ready.wait(timeout=10)
            v1_future = executor.submit(losing_v1_open)
            assert not v1_finished.wait(timeout=0.25)
            release_activation.set()
            activation_future.result(timeout=20)
            assert "disabled after M5 activation" in v1_future.result(timeout=20)

        with psycopg.connect(m5_schema.dsn) as connection:
            _select_schema(connection, schema_name)
            assert connection.execute(
                """
                SELECT count(*) FROM groundloop_epoch
                WHERE event_id = 'activation-wins-loser'
                """
            ).fetchone() == (0,)
