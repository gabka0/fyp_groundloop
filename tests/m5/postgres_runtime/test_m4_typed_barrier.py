"""Live D21 barriers between public M4 and cursor-local typed coordination."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import Any

import pytest
from m5.postgres_runtime.test_migration_015 import (
    _insert_direct_update,
    _insert_typed_header,
    _isolated_schema,
    _seed_bridge_fixture,
)
from psycopg import Connection, Cursor, sql
from psycopg.types.json import Jsonb

from groundloop.errors import InvalidEventError
from groundloop.m4.contracts import (
    CorpusUpdateIdentity,
    JobAttempt,
    JobCompletion,
    JobKind,
    JobState,
    LogicalJobSpec,
    PairKey,
    UpdateKind,
)
from groundloop.m4.persistence import PostgresM4RuntimeStore
from groundloop.m4.runtime.epoch import CompletionPlan
from groundloop.postgres.migrations import install_m5_runtime_bundle


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _table_snapshot(connection: Connection[Any]) -> tuple[object, ...]:
    tables = tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT relation.relname
            FROM pg_class AS relation
            JOIN pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = current_schema()
              AND relation.relkind = 'r'
              AND relation.relname LIKE 'groundloop_%'
            ORDER BY relation.relname
            """
        ).fetchall()
    )
    snapshot: list[object] = []
    for table in tables:
        row = connection.execute(
            sql.SQL(
                """
                SELECT count(*),
                       md5(COALESCE(
                           string_agg(to_jsonb(item)::text, E'\\n'
                                      ORDER BY to_jsonb(item)::text),
                           ''
                       ))
                FROM {} AS item
                """
            ).format(sql.Identifier(table))
        ).fetchone()
        assert row is not None
        snapshot.append((table, int(row[0]), str(row[1])))
    return tuple(snapshot)


def _dummy_job(event_id: str, policy_id: str, claim_id: str) -> LogicalJobSpec:
    execution_hash = _sha("typed-barrier-execution")
    pair = PairKey(claim_id, "typed-barrier-chunk")
    job_id = LogicalJobSpec.derive_job_id(
        event_id=event_id,
        kind=JobKind.VERIFY_PAIR,
        candidate_policy_id=policy_id,
        execution_spec_hash=execution_hash,
        claim_id=pair.claim_id,
        chunk_version_id=pair.chunk_version_id,
    )
    return LogicalJobSpec(
        job_id=job_id,
        event_id=event_id,
        kind=JobKind.VERIFY_PAIR,
        candidate_policy_id=policy_id,
        payload_hash=_sha("typed-barrier-job-payload"),
        execution_spec_hash=execution_hash,
        pair=pair,
        expandable=False,
    )


def _dummy_completion(job: LogicalJobSpec) -> JobCompletion:
    return JobCompletion.build(
        job_id=job.job_id,
        payload_hash=job.payload_hash,
        execution_spec_hash=job.execution_spec_hash,
        result_artifact_id="typed-barrier-artifact",
        result_artifact_hash=_sha("typed-barrier-result"),
        terminal_state=JobState.COMPLETED_ACTIVE,
    )


def test_public_m4_mutation_surface_rejects_typed_epoch_without_row_change() -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix="public-barrier",
        )
        connection.commit()
        prefix = "public-barrier-open"
        epoch_id = _insert_typed_header(connection, fixture, prefix=prefix)
        _insert_direct_update(connection, fixture, epoch_id)
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        event_id = f"{prefix}-event"
        update = CorpusUpdateIdentity(
            event_id=event_id,
            payload_hash=_sha(f"{prefix}-payload"),
            update_kind=UpdateKind.INSERT,
            previous_published_epoch_id=fixture.base_epoch_id,
            candidate_policy_id=fixture.direct_policy_id,
        )
        job = _dummy_job(event_id, fixture.direct_policy_id, fixture.owner_claim_id)
        attempt = JobAttempt(
            attempt_id="typed-barrier-attempt",
            job_id=job.job_id,
            execution_spec_hash=job.execution_spec_hash,
            attempt_ordinal=1,
            lease_token_hash=_sha("typed-barrier-lease"),
        )
        completion = _dummy_completion(job)
        plan = CompletionPlan(epoch_id, 1, completion)
        store = PostgresM4RuntimeStore(connection)
        publication_called = False

        def publication_action(_cursor: Cursor[Any], _epoch_id: int) -> None:
            nonlocal publication_called
            publication_called = True

        operations: tuple[tuple[str, Callable[[], object]], ...] = (
            (
                "resume",
                lambda: store.open_epoch(
                    update,
                    (),
                    registry_snapshot_id=fixture.registry_snapshot_id,
                    structural_action=lambda _cursor, _epoch_id: None,
                ),
            ),
            (
                "point acquire",
                lambda: store.start_attempt_point(epoch_id, 1, attempt),
            ),
            (
                "audit acquire",
                lambda: store.start_attempt(epoch_id, attempt),
            ),
            (
                "point retry failure",
                lambda: store.mark_retryable_failure_point(
                    epoch_id, 1, job.job_id, attempt.attempt_id
                ),
            ),
            (
                "audit retry failure",
                lambda: store.mark_retryable_failure(
                    epoch_id, job.job_id, attempt.attempt_id
                ),
            ),
            (
                "point completion",
                lambda: store.complete_point(
                    plan,
                    attempt_id=attempt.attempt_id,
                    lease_token_hash=attempt.lease_token_hash,
                    lease_expected_revision=1,
                ),
            ),
            (
                "audit completion",
                lambda: store.complete(
                    plan,
                    active_chunk_ids=frozenset(),
                    attempt_id=attempt.attempt_id,
                    lease_token_hash=attempt.lease_token_hash,
                    lease_expected_revision=1,
                ),
            ),
            (
                "point failure",
                lambda: store.fail_epoch_point(epoch_id, 1, "blocked public fail"),
            ),
            (
                "audit failure",
                lambda: store.fail_epoch(epoch_id, 1, "blocked public fail"),
            ),
            (
                "point seal",
                lambda: store.seal_epoch_point(
                    epoch_id,
                    1,
                    publication_action=publication_action,
                ),
            ),
            (
                "audit seal",
                lambda: store.seal_epoch(
                    epoch_id,
                    1,
                    publication_action=publication_action,
                ),
            ),
        )
        for operation_name, operation in operations:
            before = _table_snapshot(connection)
            with pytest.raises(
                InvalidEventError,
                match="typed M5 epochs can be mutated only by the typed coordinator",
            ):
                operation()
            assert _table_snapshot(connection) == before, operation_name
        assert not publication_called


def test_runtime_015_v1_only_public_failure_remains_compatible() -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        fixture = _seed_bridge_fixture(
            connection,
            activate=False,
            prefix="v1-compatible",
        )
        connection.commit()
        row = connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (
                'v1-compatible-open', %s, 1, 'committed', 'complete',
                'complete', 'provisional', NULL
            ) RETURNING epoch_id
            """,
            (_sha("v1-compatible-open-payload"),),
        ).fetchone()
        assert row is not None
        epoch_id = int(row[0])
        connection.execute(
            """
            INSERT INTO groundloop_m4_update (
                epoch_id, update_kind, candidate_policy_id,
                previous_published_epoch_id, registry_snapshot_id, manifest
            ) VALUES (%s, 'insert', %s, %s, %s, %s)
            """,
            (
                epoch_id,
                fixture.direct_policy_id,
                fixture.base_epoch_id,
                fixture.registry_snapshot_id,
                Jsonb(
                    {
                        "_groundloop_m4_runtime_v1": {
                            "event_manifest": {},
                            "scope_claim_ids": {},
                            "failure_reason": None,
                        }
                    }
                ),
            ),
        )
        connection.commit()

        failed = PostgresM4RuntimeStore(connection).fail_epoch_point(
            epoch_id,
            1,
            "v1-compatible-failure",
        )
        assert not failed.replayed
        assert failed.header.revision == 2
        assert failed.header.state.value == "failed"
        connection.commit()
