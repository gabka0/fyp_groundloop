"""Live recovery and replay falsifiers for M5-D24 typed-direct persistence."""

from __future__ import annotations

import struct
from datetime import timedelta
from typing import Any

import pytest
from psycopg import Connection, sql
from psycopg.errors import RaiseException

from groundloop.errors import EventConflictError, ValidationError
from groundloop.m4.contracts import LogicalJobSpec
from groundloop.m5.runtime import digests as runtime_digests
from groundloop.m5.runtime import postgres_direct_recovery as direct_recovery_module
from groundloop.m5.runtime.contracts import (
    M5AcquisitionDisposition,
    M5CallAmbiguityReport,
    M5DirectLateReturnDisposition,
    M5DispatchRecord,
    M5ExecutionEvidenceDisposition,
    M5RuntimeSubgraph,
    M5RuntimeTiming,
    M5RuntimeTimingObservation,
    M5RuntimeWork,
    M5TypedDirectJobLease,
    M5TypedDirectLateReturnEnvelope,
    M5TypedDirectReturnKind,
)
from tests.m5.postgres_runtime.d24_direct.conftest import (
    DirectD24Database,
    OpenedDirectEpoch,
    assert_literal_recovery_ledger,
    cancel_direct_job_for_audit,
    deterministic_token,
    discovery_envelope,
    open_direct_epoch,
    verifier_envelope,
    wait_until_expired,
)

_RETURN_TABLES = (
    "groundloop_epoch",
    "groundloop_m5_runtime_epoch",
    "groundloop_semantic_job",
    "groundloop_semantic_job_attempt",
    "groundloop_m5_dispatch_record",
    "groundloop_m5_attempt_execution_evidence",
    "groundloop_m5_runtime_work_contribution",
    "groundloop_m5_runtime_work_accumulator",
    "groundloop_m5_runtime_timing_contribution",
    "groundloop_m5_transition_call_timing",
    "groundloop_m5_runtime_timing_accumulator",
    "groundloop_m5_expired_attempt_return",
    "groundloop_m5_typed_direct_late_return_envelope",
    "groundloop_m5_post_terminal_attempt_timing",
    "groundloop_m5_post_terminal_attempt_audit",
)
_EVENT_MUTATION_TABLES = (
    "groundloop_epoch",
    "groundloop_m5_runtime_epoch",
    "groundloop_semantic_job",
    "groundloop_semantic_job_attempt",
    "groundloop_m5_dispatch_record",
    "groundloop_m5_runtime_work_contribution",
    "groundloop_m5_runtime_work_accumulator",
    "groundloop_m5_runtime_timing_contribution",
    "groundloop_m5_transition_call_timing",
    "groundloop_m5_runtime_timing_accumulator",
    "groundloop_m5_runtime_work",
    "groundloop_m5_event_result",
    "groundloop_m5_event_timing_coverage",
    "groundloop_m4_evaluation_epoch_counter",
    "groundloop_m5_direct_terminal_projection",
    "groundloop_m4_publication_head",
    "groundloop_m5_publication_head",
)
_VERIFIER_REPLAY_TABLES = _RETURN_TABLES + (
    "groundloop_m5_direct_terminal_projection",
    "groundloop_discovery_scope",
    "groundloop_m4_discovery_result",
    "groundloop_impact_channel_hit",
    "groundloop_admitted_pair",
    "groundloop_candidate_frontier",
    "groundloop_semantic_observation",
    "groundloop_m4_verification_execution",
    "groundloop_working_observation_delta",
    "groundloop_working_transition",
    "groundloop_object_evaluation",
    "groundloop_m4_working_claim_state",
    "groundloop_m4_working_answer_state",
    "groundloop_m4_evaluation_epoch_counter",
    "groundloop_m4_evaluation_override_counter",
    "groundloop_m4_evaluation_counter_transition",
)
_VERIFIER_M4_STATE_TABLES = (
    "groundloop_semantic_job",
    "groundloop_semantic_job_attempt",
    "groundloop_discovery_scope",
    "groundloop_m4_discovery_result",
    "groundloop_impact_channel_hit",
    "groundloop_admitted_pair",
    "groundloop_candidate_frontier",
    "groundloop_semantic_observation",
    "groundloop_m4_verification_execution",
    "groundloop_working_observation_delta",
    "groundloop_working_transition",
    "groundloop_object_evaluation",
    "groundloop_m4_working_claim_state",
    "groundloop_m4_working_answer_state",
    "groundloop_m4_evaluation_epoch_counter",
    "groundloop_m4_evaluation_override_counter",
    "groundloop_m4_evaluation_counter_transition",
)
_POSTTERMINAL_VERIFIER_IMMUTABLE_TABLES = tuple(
    dict.fromkeys(_EVENT_MUTATION_TABLES + _VERIFIER_M4_STATE_TABLES)
)
_VERIFIER_LATE_TABLES = tuple(
    dict.fromkeys(
        _RETURN_TABLES
        + _VERIFIER_M4_STATE_TABLES
        + ("groundloop_m5_direct_terminal_projection",)
    )
)
_POSTTERMINAL_VERIFIER_TABLES = tuple(
    dict.fromkeys(_VERIFIER_LATE_TABLES + _EVENT_MUTATION_TABLES)
)
_ATOMIC_DIRECT_TABLES = tuple(
    dict.fromkeys(
        _POSTTERMINAL_VERIFIER_TABLES
        + _VERIFIER_REPLAY_TABLES
        + ("groundloop_semantic_job_dependency",)
    )
)


def _snapshot(
    connection: Connection[Any], table_names: tuple[str, ...] = _RETURN_TABLES
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    captured = []
    for table_name in table_names:
        rows = connection.execute(
            sql.SQL(
                "SELECT xmin::text, to_jsonb(snapshot_row)::text "
                "FROM {} AS snapshot_row "
                'ORDER BY to_jsonb(snapshot_row)::text COLLATE "C", xmin::text'
            ).format(sql.Identifier(table_name))
        ).fetchall()
        captured.append((table_name, tuple((str(row[0]), str(row[1])) for row in rows)))
    return tuple(captured)


def _install_crash_cutoff_trigger(
    connection: Connection[Any], *, table_name: str, operation: str
) -> None:
    if operation not in {"INSERT", "UPDATE"}:
        raise AssertionError("unsupported crash-cutoff trigger operation")
    function_name = "groundloop_d24_direct_injected_crash"
    trigger_name = "groundloop_d24_direct_injected_crash"
    with connection.transaction():
        connection.execute(
            sql.SQL(
                "CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN RAISE EXCEPTION 'injected D24 direct crash cutoff'; END $$"
            ).format(sql.Identifier(function_name))
        )
        connection.execute(
            sql.SQL(
                "CREATE TRIGGER {} BEFORE {} ON {} FOR EACH ROW EXECUTE FUNCTION {}()"
            ).format(
                sql.Identifier(trigger_name),
                sql.SQL(operation),
                sql.Identifier(table_name),
                sql.Identifier(function_name),
            )
        )


def _force_accumulator_revision_for_test(
    connection: Connection[Any], *, table_name: str, epoch_id: int, revision: int
) -> None:
    if table_name not in {
        "groundloop_m5_runtime_work_accumulator",
        "groundloop_m5_runtime_timing_accumulator",
    }:
        raise AssertionError("test drift helper received an unexpected table")
    table = sql.Identifier(table_name)
    connection.execute(sql.SQL("ALTER TABLE {} DISABLE TRIGGER USER").format(table))
    try:
        connection.execute(
            sql.SQL("UPDATE {} SET updated_revision = %s WHERE epoch_id = %s").format(
                table
            ),
            (revision, epoch_id),
        )
    finally:
        connection.execute(sql.SQL("ALTER TABLE {} ENABLE TRIGGER USER").format(table))


def _acquire(
    opened: OpenedDirectEpoch,
    *,
    expected_revision: int,
    ordinal: int,
    job: LogicalJobSpec | None = None,
) -> Any:
    connection = opened.database.connection
    selected = opened.root if job is None else job
    with connection.transaction(), connection.cursor() as cursor:
        lease = opened.adapter.acquire_direct_job(
            cursor,
            opened.epoch_id,
            expected_revision,
            selected,
            deterministic_token(selected, ordinal),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return lease


def _open_verifier_attempt(
    database: DirectD24Database,
    *,
    lease_duration_ms: int = 60_000,
) -> tuple[OpenedDirectEpoch, LogicalJobSpec, M5TypedDirectJobLease]:
    opened = open_direct_epoch(database, lease_duration_ms=lease_duration_ms)
    root_lease = _acquire(opened, expected_revision=1, ordinal=1)
    expansion, children = discovery_envelope(
        opened,
        root_lease,
        attempt_ordinal=1,
    )
    expanded = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        root_lease,
        expansion,
        children,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert expanded.normal is not None and expanded.normal.resulting_revision == 3
    child = children[0]
    lease = _acquire(
        opened,
        expected_revision=3,
        ordinal=1,
        job=child,
    )
    assert lease.disposition is M5AcquisitionDisposition.DISPATCH_NEW
    assert lease.resulting_revision == 4
    return opened, child, lease


def _assert_verifier_late_envelope(
    opened: OpenedDirectEpoch,
    envelope: M5TypedDirectLateReturnEnvelope,
) -> None:
    observation = envelope.observation
    assert observation is not None
    expected_verifier_binding = {
        "result_artifact_id": envelope.result_artifact_id,
        "result_artifact_hash": envelope.result_artifact_hash,
        "verification_execution": None,
        "observation": {
            "observation_id": observation.observation_id,
            "subject_kind": observation.subject_kind.value,
            "subject_id": observation.subject_id,
            "chunk_version_id": observation.chunk_version_id,
            "task_type": observation.task_type,
            "support_score": struct.pack(">d", observation.support_score).hex(),
            "refute_score": struct.pack(">d", observation.refute_score).hex(),
            "neutral_score": struct.pack(">d", observation.neutral_score).hex(),
            "model_id": observation.producer.model_id,
            "model_version": observation.producer.model_version,
            "prompt_version": observation.producer.prompt_version,
            "input_hash": observation.input_hash,
            "produced_epoch": envelope.observation_produced_epoch,
            "raw_output_hash": envelope.observation_raw_output_hash,
            "eligible_for_currency": envelope.observation_eligible_for_currency,
            "requested_make_effective": envelope.requested_make_effective,
        },
    }
    row = opened.database.connection.execute(
        """
        SELECT return_kind, verification_execution_present,
               observation_eligible_for_currency, requested_make_effective,
               job_binding IS NOT NULL, attempt_binding IS NOT NULL,
               completion_binding IS NOT NULL,
               discovery_binding IS NULL, scope_binding IS NULL,
               verifier_binding,
               btrim(job_binding_digest), btrim(attempt_binding_digest),
               btrim(completion_binding_digest), discovery_binding_digest,
               scope_binding_digest, btrim(verifier_binding_digest),
               btrim(envelope_digest)
        FROM groundloop_m5_typed_direct_late_return_envelope
        WHERE epoch_id = %s AND attempt_id = %s
        """,
        (opened.epoch_id, envelope.attempt_id),
    ).fetchone()
    assert row == (
        "verifier",
        False,
        True,
        envelope.requested_make_effective,
        True,
        True,
        True,
        True,
        True,
        expected_verifier_binding,
        envelope.job_binding_digest,
        envelope.attempt_binding_digest,
        envelope.completion_binding_digest,
        None,
        None,
        envelope.verifier_binding_digest,
        envelope.envelope_digest,
    )


def _insert_runtime_work_row(
    connection: Connection[Any],
    *,
    opened: OpenedDirectEpoch,
    work_kind: str,
    work: M5RuntimeWork,
) -> None:
    columns = (
        "work_digest",
        "structural_event_id",
        "epoch_id",
        "work_kind",
        *M5RuntimeWork.counter_names(),
    )
    connection.execute(
        sql.SQL("INSERT INTO groundloop_m5_runtime_work ({}) VALUES ({})").format(
            sql.SQL(", ").join(sql.Identifier(name) for name in columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        (
            work.work_digest,
            opened.event.update.event_id,
            opened.epoch_id,
            work_kind,
            *work.counter_values(),
        ),
    )


def _terminalize_epoch_for_audit(opened: OpenedDirectEpoch) -> str:
    """Install the minimal accepted-016 failed cutoff without private helpers."""

    connection = opened.database.connection
    identity = connection.execute(
        """
        SELECT base.event_id, btrim(base.payload_hash), base.revision,
               runtime.runtime_state
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        WHERE base.epoch_id = %s
        """,
        (opened.epoch_id,),
    ).fetchone()
    assert identity is not None
    structural_event_id = str(identity[0])
    payload_hash = str(identity[1])
    revision = int(identity[2])
    assert str(identity[3]) in {"structural_committed", "semantic_pending"}

    counters = M5RuntimeWork.counter_names()
    current = connection.execute(
        sql.SQL(
            "SELECT {}, work_digest, updated_revision, terminalized "
            "FROM groundloop_m5_runtime_work_accumulator WHERE epoch_id = %s"
        ).format(sql.SQL(", ").join(sql.Identifier(name) for name in counters)),
        (opened.epoch_id,),
    ).fetchone()
    assert current is not None
    values = tuple(int(value) for value in current[: len(counters)])
    event_work = M5RuntimeWork(
        **dict(zip(counters, values, strict=True)),
        work_digest=str(current[len(counters)]).strip(),
    )
    assert int(current[-2]) == revision and not bool(current[-1])
    zero = M5RuntimeWork()
    open_hash = runtime_digests.open_event_receipt_binding_digest(
        epoch_id=opened.epoch_id,
        replayed=False,
        already_sealed=False,
        publication_id=None,
        already_failed=False,
        failure_reason=None,
    )
    combined_hash = runtime_digests.combined_status_delta_set_digest(())
    changed_hash = runtime_digests.changed_state_set_digest(())
    logical_result_hash = runtime_digests.event_run_logical_result_digest(
        event_id=structural_event_id,
        payload_hash=payload_hash,
        epoch_id=opened.epoch_id,
        sealed_or_failed_outcome="failed",
        original_open_receipt_binding_hash=open_hash,
        original_publication_receipt_binding_hash=None,
        event_work_digest=event_work.work_digest,
        combined_status_delta_set_hash=combined_hash,
        changed_state_set_hash=changed_hash,
        failure_reason="invariant_failure",
    )
    failure_source_hash = runtime_digests.epoch_failure_contribution_source_digest(
        structural_event_id=structural_event_id,
        failure_reason="invariant_failure",
    )
    failure_key = runtime_digests.runtime_work_contribution_key_digest(
        epoch_id=opened.epoch_id,
        contribution_kind="epoch_failure",
        source_id=structural_event_id,
    )
    missing = M5RuntimeTimingObservation.build(None)

    with connection.transaction():
        connection.execute(
            "SELECT 1 FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s FOR UPDATE",
            (opened.epoch_id,),
        )
        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (opened.epoch_id, revision),
        )
        contribution_columns = (
            "epoch_id",
            *counters,
            "work_digest",
            "contribution_kind",
            "source_id",
            "source_identity_hash",
            "contribution_key_digest",
            "applied_revision",
        )
        connection.execute(
            sql.SQL(
                "INSERT INTO groundloop_m5_runtime_work_contribution ({}) VALUES ({})"
            ).format(
                sql.SQL(", ").join(
                    sql.Identifier(name) for name in contribution_columns
                ),
                sql.SQL(", ").join(sql.Placeholder() for _ in contribution_columns),
            ),
            (
                opened.epoch_id,
                *zero.counter_values(),
                zero.work_digest,
                "epoch_failure",
                structural_event_id,
                failure_source_hash,
                failure_key,
                revision + 1,
            ),
        )
        pending = connection.execute(
            """
            SELECT pending_contribution_kind, pending_source_id,
                   btrim(pending_contribution_key_digest),
                   pending_anchor_revision
            FROM groundloop_m5_runtime_timing_accumulator
            WHERE epoch_id = %s
            """,
            (opened.epoch_id,),
        ).fetchone()
        assert pending is not None and pending[3] is not None
        pending_timing_digest = runtime_digests.transition_call_timing_digest(
            epoch_id=opened.epoch_id,
            contribution_kind=str(pending[0]),
            source_id=str(pending[1]),
            contribution_key_digest=str(pending[2]),
            anchor_revision=int(pending[3]),
            observation_digest=missing.observation_digest,
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_transition_call_timing (
                epoch_id, contribution_kind, source_id,
                contribution_key_digest, anchor_revision,
                required_interval_observed,
                coordinator_non_db_non_neural_ns, neural_wall_ns,
                postgres_roundtrip_wall_ns, external_io_wall_ns,
                end_to_end_wall_ns, postgres_server_execution_ns,
                postgres_lock_wait_ns, postgres_wal_bytes,
                postgres_shared_block_reads, observation_digest,
                transition_timing_digest
            ) VALUES (
                %s, %s, %s, %s, %s, false,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                %s, %s
            )
            """,
            (
                opened.epoch_id,
                pending[0],
                pending[1],
                pending[2],
                pending[3],
                missing.observation_digest,
                pending_timing_digest,
            ),
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_epoch
            SET revision = revision + 1, structural_status = 'failed',
                semantic_status = 'failed', evaluation_state = 'failed',
                publication_mode = 'provisional', sealed_at = NULL
            WHERE epoch_id = %s AND revision = %s
            """,
                (opened.epoch_id, revision),
            ).rowcount
            == 1
        )
        connection.execute(
            "UPDATE groundloop_m5_owner_pending_counter "
            "SET updated_revision = %s WHERE epoch_id = %s",
            (revision + 1, opened.epoch_id),
        )
        connection.execute(
            "UPDATE groundloop_m5_answer_pending_counter "
            "SET updated_revision = %s WHERE epoch_id = %s",
            (revision + 1, opened.epoch_id),
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_runtime_epoch
            SET runtime_state = 'failed', revision = revision + 1,
                terminal_at = clock_timestamp()
            WHERE epoch_id = %s AND revision = %s
              AND runtime_state IN ('structural_committed', 'semantic_pending')
            """,
                (opened.epoch_id, revision),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_runtime_work_accumulator
            SET work_digest = %s, updated_revision = %s,
                terminalized = true, updated_at = clock_timestamp()
            WHERE epoch_id = %s AND updated_revision = %s
              AND NOT terminalized
            """,
                (event_work.work_digest, revision + 1, opened.epoch_id, revision),
            ).rowcount
            == 1
        )
        assert (
            connection.execute(
                """
            UPDATE groundloop_m5_runtime_timing_accumulator
            SET required_expected_count = required_expected_count + 1,
                required_missing_count = required_missing_count + 2,
                postgres_server_execution_expected_count =
                    postgres_server_execution_expected_count + 1,
                postgres_server_execution_missing_count =
                    postgres_server_execution_missing_count + 2,
                postgres_lock_wait_expected_count =
                    postgres_lock_wait_expected_count + 1,
                postgres_lock_wait_missing_count =
                    postgres_lock_wait_missing_count + 2,
                postgres_wal_bytes_expected_count =
                    postgres_wal_bytes_expected_count + 1,
                postgres_wal_bytes_missing_count =
                    postgres_wal_bytes_missing_count + 2,
                postgres_shared_block_reads_expected_count =
                    postgres_shared_block_reads_expected_count + 1,
                postgres_shared_block_reads_missing_count =
                    postgres_shared_block_reads_missing_count + 2,
                pending_contribution_kind = NULL,
                pending_source_id = NULL,
                pending_contribution_key_digest = NULL,
                pending_anchor_revision = NULL,
                updated_revision = %s, terminalized = true,
                updated_at = clock_timestamp()
            WHERE epoch_id = %s AND updated_revision = %s
              AND NOT terminalized
            """,
                (revision + 1, opened.epoch_id, revision),
            ).rowcount
            == 1
        )
        timing = connection.execute(
            """
            SELECT coordinator_non_db_non_neural_ns, neural_wall_ns,
                   postgres_roundtrip_wall_ns, external_io_wall_ns,
                   end_to_end_wall_ns,
                   required_expected_count, required_observed_count,
                   required_missing_count,
                   postgres_server_execution_expected_count,
                   postgres_server_execution_observed_count,
                   postgres_server_execution_missing_count,
                   postgres_lock_wait_expected_count,
                   postgres_lock_wait_observed_count,
                   postgres_lock_wait_missing_count,
                   postgres_wal_bytes_expected_count,
                   postgres_wal_bytes_observed_count,
                   postgres_wal_bytes_missing_count,
                   postgres_shared_block_reads_expected_count,
                   postgres_shared_block_reads_observed_count,
                   postgres_shared_block_reads_missing_count
            FROM groundloop_m5_runtime_timing_accumulator
            WHERE epoch_id = %s
            """,
            (opened.epoch_id,),
        ).fetchone()
        assert timing is not None
        _insert_runtime_work_row(
            connection, opened=opened, work_kind="event", work=event_work
        )
        _insert_runtime_work_row(connection, opened=opened, work_kind="call", work=zero)
        connection.execute(
            """
            INSERT INTO groundloop_m5_event_result (
                structural_event_id, payload_hash, epoch_id, outcome,
                original_open_receipt_binding_hash, publication_id,
                original_publication_receipt_binding_hash,
                event_work_kind, event_work_digest,
                combined_status_delta_set_hash, changed_state_set_hash,
                failure_reason, logical_result_hash, delta_count,
                state_reference_count, coordinator_non_db_non_neural_ns,
                neural_wall_ns, postgres_roundtrip_wall_ns,
                external_io_wall_ns, end_to_end_wall_ns,
                postgres_server_execution_ns, postgres_lock_wait_ns,
                postgres_wal_bytes, postgres_shared_block_reads
            ) VALUES (
                %s, %s, %s, 'failed', %s, NULL, NULL, 'event', %s,
                %s, %s, 'invariant_failure', %s, 0, 0,
                %s, %s, %s, %s, %s, NULL, NULL, NULL, NULL
            )
            """,
            (
                structural_event_id,
                payload_hash,
                opened.epoch_id,
                open_hash,
                event_work.work_digest,
                combined_hash,
                changed_hash,
                logical_result_hash,
                *timing[:5],
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_event_timing_coverage (
                structural_event_id, epoch_id,
                required_expected_count, required_observed_count,
                required_missing_count,
                postgres_server_execution_expected_count,
                postgres_server_execution_observed_count,
                postgres_server_execution_missing_count,
                postgres_lock_wait_expected_count,
                postgres_lock_wait_observed_count,
                postgres_lock_wait_missing_count,
                postgres_wal_bytes_expected_count,
                postgres_wal_bytes_observed_count,
                postgres_wal_bytes_missing_count,
                postgres_shared_block_reads_expected_count,
                postgres_shared_block_reads_observed_count,
                postgres_shared_block_reads_missing_count,
                terminal_client_roundtrip_included
            ) VALUES (
                %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, false
            )
            """,
            (structural_event_id, opened.epoch_id, *timing[5:]),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return logical_result_hash


def test_literal_016_fixture_reconnects_with_distinct_backends(
    d24_direct_db: DirectD24Database,
) -> None:
    assert_literal_recovery_ledger(d24_direct_db.connection)
    with d24_direct_db.reconnect() as reconnect:
        assert_literal_recovery_ledger(reconnect)
        assert reconnect.execute("SELECT current_schema()").fetchone() == (
            d24_direct_db.schema_name,
        )
    with d24_direct_db.two_connections() as (left, right):
        assert (
            left.execute("SELECT pg_backend_pid()").fetchone()
            != right.execute("SELECT pg_backend_pid()").fetchone()
        )
    opened = open_direct_epoch(d24_direct_db)
    with d24_direct_db.reconnect() as reconnect:
        structural = reconnect.execute(
            """
            SELECT contribution.bytes_hashed,
                   contribution.bytes_serialized,
                   accumulator.bytes_hashed,
                   accumulator.bytes_serialized,
                   contribution.work_digest = accumulator.work_digest
            FROM groundloop_m5_runtime_work_contribution AS contribution
            JOIN groundloop_m5_runtime_work_accumulator AS accumulator
              USING (epoch_id)
            WHERE contribution.epoch_id = %s
              AND contribution.contribution_kind = 'structural_open'
            """,
            (opened.epoch_id,),
        ).fetchone()
        assert structural is not None
        assert structural[0] == structural[1] == structural[2] == structural[3]
        assert int(structural[0]) > 0 and bool(structural[4])


def test_deadline_equality_is_takeover_eligible(
    d24_direct_db: DirectD24Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = open_direct_epoch(d24_direct_db, lease_duration_ms=300)
    first = _acquire(opened, expected_revision=1, ordinal=1)
    assert first.lease_expires_at is not None
    equality_decision_time = first.lease_expires_at
    successor_expiry = equality_decision_time + timedelta(milliseconds=300)
    monkeypatch.setattr(
        direct_recovery_module,
        "_sample_database_deadline",
        lambda _cursor, _epoch_id: (equality_decision_time, successor_expiry),
    )

    successor = _acquire(opened, expected_revision=2, ordinal=2)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert successor.resulting_revision == 3
    assert successor.lease_expires_at == successor_expiry
    assert d24_direct_db.connection.execute(
        """
        SELECT attempt_ordinal, attempt_state, finished_at
        FROM groundloop_semantic_job_attempt
        WHERE job_id = %s ORDER BY attempt_ordinal
        """,
        (opened.root.job_id,),
    ).fetchall() == [
        (1, "expired", equality_decision_time),
        (2, "leased", None),
    ]


def test_committed_dispatch_without_evidence_has_exact_ambiguity_bounds(
    d24_direct_db: DirectD24Database,
) -> None:
    opened = open_direct_epoch(d24_direct_db)
    lease = _acquire(opened, expected_revision=1, ordinal=1)
    assert lease.attempt_id is not None and lease.lease_expires_at is not None
    with d24_direct_db.reconnect() as reconnect:
        stored = reconnect.execute(
            """
            SELECT logical_job_id, attempt_ordinal, job_kind, fallback_required,
                   dispatched_revision, lease_expires_at,
                   maximum_direct_discovery_call_count,
                   maximum_embedding_model_call_count,
                   btrim(maximum_work_digest), btrim(record_digest)
            FROM groundloop_m5_dispatch_record
            WHERE epoch_id = %s AND subgraph = 'direct' AND attempt_id = %s
            """,
            (opened.epoch_id, lease.attempt_id),
        ).fetchone()
        assert reconnect.execute(
            "SELECT count(*) FROM groundloop_m5_attempt_execution_evidence "
            "WHERE epoch_id = %s AND attempt_id = %s",
            (opened.epoch_id, lease.attempt_id),
        ).fetchone() == (0,)
    assert stored is not None
    dispatch = M5DispatchRecord.build(
        epoch_id=opened.epoch_id,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id=lease.attempt_id,
        logical_job_id=str(stored[0]),
        attempt_ordinal=int(stored[1]),
        job_kind=str(stored[2]),
        fallback_required=bool(stored[3]),
        dispatched_revision=int(stored[4]),
        lease_expires_at=stored[5],
    )
    assert stored[6:] == (
        1,
        1,
        dispatch.maximum_ambiguous_call_work.work_digest,
        dispatch.record_digest,
    )
    report = M5CallAmbiguityReport.build(dispatches=(dispatch,), execution_evidence=())
    assert report.durable_dispatch_count == 1
    assert report.confirmed_execution_count == 0
    assert report.unresolved_dispatch_count == 1
    assert report.confirmed_call_lower == M5RuntimeWork()
    assert report.possible_call_upper == M5RuntimeWork(
        direct_discovery_call_count=1,
        embedding_model_call_count=1,
    )


@pytest.mark.parametrize(
    ("table_name", "operation"),
    (
        ("groundloop_epoch", "UPDATE"),
        ("groundloop_m5_attempt_execution_evidence", "INSERT"),
        ("groundloop_m5_runtime_work_contribution", "INSERT"),
        ("groundloop_m5_runtime_work_accumulator", "UPDATE"),
        ("groundloop_m5_runtime_timing_accumulator", "UPDATE"),
    ),
)
def test_normal_return_crash_cutoffs_roll_back_every_group_after_reconnect(
    d24_direct_db: DirectD24Database,
    table_name: str,
    operation: str,
) -> None:
    opened = open_direct_epoch(d24_direct_db)
    lease = _acquire(opened, expected_revision=1, ordinal=1)
    envelope, children = discovery_envelope(
        opened,
        lease,
        attempt_ordinal=1,
    )
    _install_crash_cutoff_trigger(
        d24_direct_db.connection,
        table_name=table_name,
        operation=operation,
    )
    before = _snapshot(d24_direct_db.connection, _ATOMIC_DIRECT_TABLES)

    with pytest.raises(RaiseException, match="injected D24 direct crash cutoff"):
        opened.adapter.settle_direct_expansion_atomically(
            opened.epoch_id,
            2,
            lease,
            envelope,
            children,
            M5ExecutionEvidenceDisposition.RETURNED,
            M5RuntimeWork(),
            None,
        )

    with d24_direct_db.reconnect() as reconnect:
        assert _snapshot(reconnect, _ATOMIC_DIRECT_TABLES) == before


@pytest.mark.parametrize(
    ("table_name", "operation"),
    (
        ("groundloop_m5_attempt_execution_evidence", "INSERT"),
        ("groundloop_m5_typed_direct_late_return_envelope", "INSERT"),
        ("groundloop_m5_expired_attempt_return", "INSERT"),
        ("groundloop_m5_runtime_timing_contribution", "INSERT"),
        ("groundloop_m5_runtime_work_contribution", "INSERT"),
        ("groundloop_m5_runtime_work_accumulator", "UPDATE"),
        ("groundloop_m5_runtime_timing_accumulator", "UPDATE"),
    ),
)
def test_preterminal_late_sidecar_cutoffs_are_atomic_after_reconnect(
    d24_direct_db: DirectD24Database,
    table_name: str,
    operation: str,
) -> None:
    opened = open_direct_epoch(d24_direct_db, lease_duration_ms=50)
    first = _acquire(opened, expected_revision=1, ordinal=1)
    assert first.lease_expires_at is not None
    wait_until_expired(d24_direct_db.connection, first.lease_expires_at)
    successor = _acquire(opened, expected_revision=2, ordinal=2)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    envelope, children = discovery_envelope(
        opened,
        first,
        attempt_ordinal=1,
    )
    _install_crash_cutoff_trigger(
        d24_direct_db.connection,
        table_name=table_name,
        operation=operation,
    )
    before = _snapshot(d24_direct_db.connection, _ATOMIC_DIRECT_TABLES)

    with pytest.raises(RaiseException, match="injected D24 direct crash cutoff"):
        opened.adapter.settle_direct_expansion_atomically(
            opened.epoch_id,
            2,
            first,
            envelope,
            children,
            M5ExecutionEvidenceDisposition.RETURNED,
            M5RuntimeWork(),
            None,
        )

    with d24_direct_db.reconnect() as reconnect:
        assert _snapshot(reconnect, _ATOMIC_DIRECT_TABLES) == before


@pytest.mark.parametrize(
    "table_name",
    (
        "groundloop_m5_post_terminal_attempt_timing",
        "groundloop_m5_post_terminal_attempt_audit",
    ),
)
def test_postterminal_late_sidecar_cutoffs_are_atomic_after_reconnect(
    d24_direct_db: DirectD24Database,
    table_name: str,
) -> None:
    opened = open_direct_epoch(d24_direct_db, lease_duration_ms=60_000)
    lease = _acquire(opened, expected_revision=1, ordinal=1)
    cancel_direct_job_for_audit(opened, terminal_reason="epoch_failed")
    _terminalize_epoch_for_audit(opened)
    envelope, children = discovery_envelope(
        opened,
        lease,
        attempt_ordinal=1,
    )
    _install_crash_cutoff_trigger(
        d24_direct_db.connection,
        table_name=table_name,
        operation="INSERT",
    )
    before = _snapshot(d24_direct_db.connection, _ATOMIC_DIRECT_TABLES)

    with pytest.raises(RaiseException, match="injected D24 direct crash cutoff"):
        opened.adapter.settle_direct_expansion_atomically(
            opened.epoch_id,
            2,
            lease,
            envelope,
            children,
            M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
            M5RuntimeWork(),
            None,
        )

    with d24_direct_db.reconnect() as reconnect:
        assert _snapshot(reconnect, _ATOMIC_DIRECT_TABLES) == before


def test_normal_discovery_is_atomic_replay_exact_and_conflicts_are_zero_write(
    d24_direct_db: DirectD24Database,
) -> None:
    opened = open_direct_epoch(d24_direct_db)
    lease = _acquire(opened, expected_revision=1, ordinal=1)
    assert lease.disposition is M5AcquisitionDisposition.DISPATCH_NEW
    envelope, children = discovery_envelope(
        opened,
        lease,
        attempt_ordinal=1,
    )
    zero = M5RuntimeWork()
    first = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        lease,
        envelope,
        children,
        M5ExecutionEvidenceDisposition.RETURNED,
        zero,
        None,
    )
    assert first.normal is not None and first.late is None
    assert not first.normal.exact_replay
    assert first.normal.resulting_revision == 3
    assert first.normal.transition_anchor is not None

    before_replay = _snapshot(d24_direct_db.connection)
    replay = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        lease,
        envelope,
        children,
        M5ExecutionEvidenceDisposition.RETURNED,
        zero,
        None,
    )
    assert replay.normal is not None and replay.normal.exact_replay
    assert replay.normal.transition_anchor is None
    assert _snapshot(d24_direct_db.connection) == before_replay

    for disposition, work, timing in (
        (M5ExecutionEvidenceDisposition.REUSED_ARTIFACT, zero, None),
        (
            M5ExecutionEvidenceDisposition.RETURNED,
            M5RuntimeWork(direct_discovery_call_count=1),
            None,
        ),
        (M5ExecutionEvidenceDisposition.RETURNED, zero, M5RuntimeTiming()),
    ):
        before_conflict = _snapshot(d24_direct_db.connection)
        with pytest.raises(EventConflictError):
            opened.adapter.settle_direct_expansion_atomically(
                opened.epoch_id,
                2,
                lease,
                envelope,
                children,
                disposition,
                work,
                timing,
            )
        assert _snapshot(d24_direct_db.connection) == before_conflict


def test_reused_artifact_is_distinct_from_equal_zero_returned_evidence(
    d24_direct_db: DirectD24Database,
) -> None:
    opened = open_direct_epoch(d24_direct_db)
    lease = _acquire(opened, expected_revision=1, ordinal=1)
    envelope, children = discovery_envelope(
        opened,
        lease,
        attempt_ordinal=1,
    )
    zero = M5RuntimeWork()
    receipt = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        lease,
        envelope,
        children,
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
        zero,
        None,
    )
    assert receipt.normal is not None
    row = d24_direct_db.connection.execute(
        """
        SELECT disposition, attempt_work_digest,
               attempt_direct_discovery_call_count,
               attempt_embedding_model_call_count,
               attempt_embedding_input_token_count
        FROM groundloop_m5_attempt_execution_evidence
        WHERE epoch_id = %s AND subgraph = 'direct'
        """,
        (opened.epoch_id,),
    ).fetchone()
    assert row == ("reused_artifact", zero.work_digest, 0, 0, 0)
    before = _snapshot(d24_direct_db.connection)
    with pytest.raises(EventConflictError):
        opened.adapter.settle_direct_expansion_atomically(
            opened.epoch_id,
            2,
            lease,
            envelope,
            children,
            M5ExecutionEvidenceDisposition.RETURNED,
            zero,
            None,
        )
    assert _snapshot(d24_direct_db.connection) == before


@pytest.mark.parametrize(
    ("table_name", "message"),
    (
        (
            "groundloop_m5_runtime_work_accumulator",
            "work accumulator revision differs from locked cutoff",
        ),
        (
            "groundloop_m5_runtime_timing_accumulator",
            "timing accumulator revision differs from locked cutoff",
        ),
    ),
)
def test_direct_settlement_rejects_accumulator_revision_drift_without_writes(
    d24_direct_db: DirectD24Database,
    table_name: str,
    message: str,
) -> None:
    opened = open_direct_epoch(d24_direct_db)
    lease = _acquire(opened, expected_revision=1, ordinal=1)
    envelope, children = discovery_envelope(
        opened,
        lease,
        attempt_ordinal=1,
    )
    before = _snapshot(d24_direct_db.connection)
    with pytest.raises(ValidationError, match=message):
        with d24_direct_db.connection.transaction():
            _force_accumulator_revision_for_test(
                d24_direct_db.connection,
                table_name=table_name,
                epoch_id=opened.epoch_id,
                revision=1,
            )
            opened.adapter.settle_direct_expansion_atomically(
                opened.epoch_id,
                2,
                lease,
                envelope,
                children,
                M5ExecutionEvidenceDisposition.RETURNED,
                M5RuntimeWork(),
                None,
            )
    assert _snapshot(d24_direct_db.connection) == before


def _assert_preterminal_zero_revision_anchor(
    opened: OpenedDirectEpoch, *, attempt_id: str, revision: int
) -> None:
    row = opened.database.connection.execute(
        """
        SELECT base.revision, runtime.revision,
               work.updated_revision, timing.updated_revision,
               timing.pending_contribution_kind, timing.pending_source_id,
               btrim(timing.pending_contribution_key_digest),
               timing.pending_anchor_revision
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_runtime_work_accumulator AS work USING (epoch_id)
        JOIN groundloop_m5_runtime_timing_accumulator AS timing USING (epoch_id)
        WHERE base.epoch_id = %s
        """,
        (opened.epoch_id,),
    ).fetchone()
    expected_key = runtime_digests.runtime_work_contribution_key_digest(
        epoch_id=opened.epoch_id,
        contribution_kind="preterminal_late_return",
        source_id=attempt_id,
    )
    assert row == (
        revision,
        revision,
        revision,
        revision,
        "preterminal_late_return",
        attempt_id,
        expected_key,
        revision,
    )
    assert opened.database.connection.execute(
        """
        SELECT count(*)
        FROM groundloop_m5_runtime_work_contribution
        WHERE epoch_id = %s
          AND contribution_kind = 'preterminal_late_return'
          AND source_id = %s AND applied_revision = %s
        """,
        (opened.epoch_id, attempt_id, revision),
    ).fetchone() == (1,)
    assert opened.database.connection.execute(
        """
        SELECT count(*)
        FROM groundloop_m5_transition_call_timing
        WHERE epoch_id = %s
          AND contribution_kind = 'preterminal_late_return'
          AND source_id = %s AND anchor_revision = %s
        """,
        (opened.epoch_id, attempt_id, revision),
    ).fetchone() == (0,)


def test_first_expired_preterminal_return_uses_zero_revision_anchor_and_replays(
    d24_direct_db: DirectD24Database,
) -> None:
    opened = open_direct_epoch(d24_direct_db, lease_duration_ms=50)
    first_lease = _acquire(opened, expected_revision=1, ordinal=1)
    assert first_lease.lease_expires_at is not None
    wait_until_expired(d24_direct_db.connection, first_lease.lease_expires_at)
    successor = _acquire(opened, expected_revision=2, ordinal=2)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    envelope, children = discovery_envelope(
        opened,
        first_lease,
        attempt_ordinal=1,
    )
    receipt = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        first_lease,
        envelope,
        children,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert receipt.normal is None and receipt.late is not None
    assert receipt.late.disposition is M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL
    assert not receipt.late.exact_replay
    assert receipt.late.resulting_revision == 3
    assert receipt.late.transition_anchor is not None
    assert receipt.late.expired_return_digest is not None
    assert first_lease.attempt_id is not None
    _assert_preterminal_zero_revision_anchor(
        opened,
        attempt_id=first_lease.attempt_id,
        revision=3,
    )

    before_replay = _snapshot(d24_direct_db.connection)
    replay = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        first_lease,
        envelope,
        children,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert replay.late is not None and replay.late.exact_replay
    assert replay.late.transition_anchor is None
    assert _snapshot(d24_direct_db.connection) == before_replay


def test_first_nonexpired_terminal_audit_uses_zero_revision_anchor(
    d24_direct_db: DirectD24Database,
) -> None:
    opened = open_direct_epoch(d24_direct_db, lease_duration_ms=60_000)
    lease = _acquire(opened, expected_revision=1, ordinal=1)
    cancel_direct_job_for_audit(opened, terminal_reason="subject_inactive")
    envelope, children = discovery_envelope(
        opened,
        lease,
        attempt_ordinal=1,
    )
    receipt = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        lease,
        envelope,
        children,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert receipt.normal is None and receipt.late is not None
    assert (
        receipt.late.disposition
        is M5DirectLateReturnDisposition.TERMINAL_AUDIT_PRETERMINAL
    )
    assert not receipt.late.exact_replay
    assert receipt.late.resulting_revision == 2
    assert receipt.late.expired_return_digest is None
    assert receipt.late.transition_anchor is not None
    assert lease.attempt_id is not None
    _assert_preterminal_zero_revision_anchor(
        opened,
        attempt_id=lease.attempt_id,
        revision=2,
    )
    assert d24_direct_db.connection.execute(
        "SELECT count(*) FROM groundloop_m5_expired_attempt_return WHERE epoch_id = %s",
        (opened.epoch_id,),
    ).fetchone() == (0,)


def test_epoch_failed_terminal_audit_is_rejected_before_terminal_cutoff(
    d24_direct_db: DirectD24Database,
) -> None:
    opened = open_direct_epoch(d24_direct_db, lease_duration_ms=60_000)
    lease = _acquire(opened, expected_revision=1, ordinal=1)
    cancel_direct_job_for_audit(opened, terminal_reason="epoch_failed")
    envelope, children = discovery_envelope(
        opened,
        lease,
        attempt_ordinal=1,
    )
    before = _snapshot(d24_direct_db.connection)
    with pytest.raises(ValidationError, match="projection is inexact"):
        opened.adapter.settle_direct_expansion_atomically(
            opened.epoch_id,
            2,
            lease,
            envelope,
            children,
            M5ExecutionEvidenceDisposition.RETURNED,
            M5RuntimeWork(),
            None,
        )
    assert _snapshot(d24_direct_db.connection) == before


def test_epoch_failed_nonexpired_postterminal_return_is_audit_only_and_exact(
    d24_direct_db: DirectD24Database,
) -> None:
    opened = open_direct_epoch(d24_direct_db, lease_duration_ms=60_000)
    lease = _acquire(opened, expected_revision=1, ordinal=1)
    cancel_direct_job_for_audit(opened, terminal_reason="epoch_failed")
    terminal_hash = _terminalize_epoch_for_audit(opened)
    assert d24_direct_db.connection.execute(
        "SELECT clock_timestamp() < %s",
        (lease.lease_expires_at,),
    ).fetchone() == (True,)
    envelope, children = discovery_envelope(
        opened,
        lease,
        attempt_ordinal=1,
    )
    before_event = _snapshot(d24_direct_db.connection, _EVENT_MUTATION_TABLES)
    receipt = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        lease,
        envelope,
        children,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert receipt.normal is None and receipt.late is not None
    assert (
        receipt.late.disposition
        is M5DirectLateReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL
    )
    assert not receipt.late.exact_replay
    assert receipt.late.resulting_revision == 3
    assert receipt.late.current_terminal_logical_result_hash == terminal_hash
    assert receipt.late.transition_anchor is None
    assert receipt.late.expired_return_digest is None
    assert _snapshot(d24_direct_db.connection, _EVENT_MUTATION_TABLES) == before_event
    assert d24_direct_db.connection.execute(
        """
        SELECT evidence.disposition, envelope.return_kind,
               timing.required_interval_observed, audit.return_kind,
               btrim(audit.terminal_logical_result_hash)
        FROM groundloop_m5_attempt_execution_evidence AS evidence
        JOIN groundloop_m5_typed_direct_late_return_envelope AS envelope
          USING (epoch_id, attempt_id)
        JOIN groundloop_m5_post_terminal_attempt_timing AS timing
          USING (epoch_id, subgraph, attempt_id)
        JOIN groundloop_m5_post_terminal_attempt_audit AS audit
          USING (epoch_id, subgraph, attempt_id)
        WHERE evidence.epoch_id = %s AND evidence.subgraph = 'direct'
        """,
        (opened.epoch_id,),
    ).fetchone() == (
        "returned",
        "discovery",
        False,
        "terminal_audit_only",
        terminal_hash,
    )
    assert d24_direct_db.connection.execute(
        "SELECT count(*) FROM groundloop_m5_expired_attempt_return WHERE epoch_id = %s",
        (opened.epoch_id,),
    ).fetchone() == (0,)

    before_replay = _snapshot(d24_direct_db.connection)
    replay = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        lease,
        envelope,
        children,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert replay.late is not None and replay.late.exact_replay
    assert replay.late.current_terminal_logical_result_hash == terminal_hash
    assert _snapshot(d24_direct_db.connection) == before_replay

    for disposition, timing in (
        (M5ExecutionEvidenceDisposition.REUSED_ARTIFACT, None),
        (M5ExecutionEvidenceDisposition.RETURNED, M5RuntimeTiming()),
    ):
        before_conflict = _snapshot(d24_direct_db.connection)
        with pytest.raises(EventConflictError):
            opened.adapter.settle_direct_expansion_atomically(
                opened.epoch_id,
                2,
                lease,
                envelope,
                children,
                disposition,
                M5RuntimeWork(),
                timing,
            )
        assert _snapshot(d24_direct_db.connection) == before_conflict


def test_verifier_replay_survives_legitimate_same_pair_requeue_at_later_revision(
    d24_direct_db: DirectD24Database,
) -> None:
    opened = open_direct_epoch(
        d24_direct_db,
        lease_duration_ms=60_000,
        include_requeue_root=True,
    )
    assert opened.requeue_root is not None and opened.requeue_scope is not None
    assert d24_direct_db.connection.execute(
        """
        SELECT btrim(root_job_id), closed_revision
        FROM groundloop_discovery_scope
        WHERE epoch_id = %s
        ORDER BY root_job_id
        """,
        (opened.epoch_id,),
    ).fetchall() == sorted(
        (
            (opened.root.job_id, None),
            (opened.requeue_root.job_id, None),
        )
    )

    root_lease = _acquire(opened, expected_revision=1, ordinal=1)
    root_envelope, children = discovery_envelope(
        opened,
        root_lease,
        attempt_ordinal=1,
    )
    child = children[0]
    assert child.pair is not None
    root_receipt = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        2,
        root_lease,
        root_envelope,
        children,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert root_receipt.normal is not None
    assert root_receipt.normal.resulting_revision == 3

    verifier_lease = _acquire(
        opened,
        expected_revision=3,
        ordinal=1,
        job=child,
    )
    verifier_return = verifier_envelope(
        opened,
        verifier_lease,
        child,
        attempt_ordinal=1,
    )
    first = opened.adapter.settle_direct_verifier_atomically(
        opened.epoch_id,
        4,
        verifier_lease,
        verifier_return,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert first.return_kind is M5TypedDirectReturnKind.VERIFIER
    assert first.normal is not None and first.late is None
    assert not first.normal.exact_replay
    assert first.normal.resulting_revision == 5
    assert first.normal.observation_completion is not None
    assert first.normal.observation_completion.artifact_stored
    assert first.normal.observation_completion.made_effective
    assert d24_direct_db.connection.execute(
        """
        SELECT frontier_state
        FROM groundloop_candidate_frontier
        WHERE claim_id = %s AND chunk_version_id = %s
          AND candidate_policy_id = %s AND valid_from_epoch = %s
        """,
        (
            child.pair.claim_id,
            child.pair.chunk_version_id,
            child.candidate_policy_id,
            opened.epoch_id,
        ),
    ).fetchone() == ("verified_current",)

    requeue_lease = _acquire(
        opened,
        expected_revision=5,
        ordinal=1,
        job=opened.requeue_root,
    )
    requeue_envelope, requeue_children = discovery_envelope(
        opened,
        requeue_lease,
        attempt_ordinal=1,
        job=opened.requeue_root,
    )
    assert requeue_children[0].pair == child.pair
    assert requeue_children[0].job_id != child.job_id
    requeued = opened.adapter.settle_direct_expansion_atomically(
        opened.epoch_id,
        6,
        requeue_lease,
        requeue_envelope,
        requeue_children,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert requeued.normal is not None and requeued.normal.resulting_revision == 7
    assert d24_direct_db.connection.execute(
        """
        SELECT btrim(root_job_id), closed_revision
        FROM groundloop_discovery_scope
        WHERE epoch_id = %s
        ORDER BY root_job_id
        """,
        (opened.epoch_id,),
    ).fetchall() == sorted(
        (
            (opened.root.job_id, 3),
            (opened.requeue_root.job_id, 7),
        )
    )
    assert d24_direct_db.connection.execute(
        """
        SELECT frontier_state
        FROM groundloop_candidate_frontier
        WHERE claim_id = %s AND chunk_version_id = %s
          AND candidate_policy_id = %s AND valid_from_epoch = %s
        """,
        (
            child.pair.claim_id,
            child.pair.chunk_version_id,
            child.candidate_policy_id,
            opened.epoch_id,
        ),
    ).fetchone() == ("queued",)

    after_requeue = _snapshot(d24_direct_db.connection, _VERIFIER_REPLAY_TABLES)
    replay = opened.adapter.settle_direct_verifier_atomically(
        opened.epoch_id,
        4,
        verifier_lease,
        verifier_return,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert replay.return_kind is M5TypedDirectReturnKind.VERIFIER
    assert replay.normal is not None and replay.late is None
    assert replay.normal.exact_replay
    assert replay.normal.resulting_revision == 7
    assert replay.normal.observation_completion == first.normal.observation_completion
    assert replay.normal.transition_anchor is None
    assert _snapshot(d24_direct_db.connection, _VERIFIER_REPLAY_TABLES) == after_requeue

    with pytest.raises(EventConflictError):
        opened.adapter.settle_direct_verifier_atomically(
            opened.epoch_id,
            4,
            verifier_lease,
            verifier_return,
            M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
            M5RuntimeWork(),
            None,
        )
    assert _snapshot(d24_direct_db.connection, _VERIFIER_REPLAY_TABLES) == after_requeue


def test_expired_preterminal_verifier_return_has_zero_revision_anchor_and_no_m4_write(
    d24_direct_db: DirectD24Database,
) -> None:
    opened, child, first_lease = _open_verifier_attempt(
        d24_direct_db,
        lease_duration_ms=500,
    )
    assert first_lease.lease_expires_at is not None
    wait_until_expired(d24_direct_db.connection, first_lease.lease_expires_at)
    successor = _acquire(
        opened,
        expected_revision=4,
        ordinal=2,
        job=child,
    )
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert successor.resulting_revision == 5
    envelope = verifier_envelope(
        opened,
        first_lease,
        child,
        attempt_ordinal=1,
        make_effective=True,
    )
    before_m4 = _snapshot(d24_direct_db.connection, _VERIFIER_M4_STATE_TABLES)
    receipt = opened.adapter.settle_direct_verifier_atomically(
        opened.epoch_id,
        4,
        first_lease,
        envelope,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert receipt.return_kind is M5TypedDirectReturnKind.VERIFIER
    assert receipt.normal is None and receipt.late is not None
    assert receipt.late.disposition is M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL
    assert not receipt.late.exact_replay
    assert receipt.late.resulting_revision == 5
    assert receipt.late.expired_return_digest is not None
    assert receipt.late.transition_anchor is not None
    assert _snapshot(d24_direct_db.connection, _VERIFIER_M4_STATE_TABLES) == before_m4
    assert first_lease.attempt_id is not None
    _assert_preterminal_zero_revision_anchor(
        opened,
        attempt_id=first_lease.attempt_id,
        revision=5,
    )
    _assert_verifier_late_envelope(opened, envelope)
    assert d24_direct_db.connection.execute(
        """
        SELECT evidence.disposition, btrim(evidence.result_or_error_hash),
               timing.required_interval_observed,
               expired.received_after_terminal,
               btrim(expired.worker_output_digest),
               count(*) OVER ()
        FROM groundloop_m5_attempt_execution_evidence AS evidence
        JOIN groundloop_m5_runtime_timing_contribution AS timing
          USING (epoch_id, subgraph, attempt_id)
        JOIN groundloop_m5_expired_attempt_return AS expired
          USING (epoch_id, subgraph, attempt_id)
        WHERE evidence.epoch_id = %s AND evidence.attempt_id = %s
        """,
        (opened.epoch_id, first_lease.attempt_id),
    ).fetchone() == (
        "returned",
        envelope.envelope_digest,
        False,
        False,
        envelope.envelope_digest,
        1,
    )

    stable = _snapshot(d24_direct_db.connection, _VERIFIER_LATE_TABLES)
    replay = opened.adapter.settle_direct_verifier_atomically(
        opened.epoch_id,
        4,
        first_lease,
        envelope,
        M5ExecutionEvidenceDisposition.RETURNED,
        M5RuntimeWork(),
        None,
    )
    assert replay.late is not None and replay.late.exact_replay
    assert replay.late.resulting_revision == 5
    assert replay.late.transition_anchor is None
    assert _snapshot(d24_direct_db.connection, _VERIFIER_LATE_TABLES) == stable

    different_envelope = verifier_envelope(
        opened,
        first_lease,
        child,
        attempt_ordinal=1,
        make_effective=False,
    )
    for disposition, candidate, timing in (
        (M5ExecutionEvidenceDisposition.REUSED_ARTIFACT, envelope, None),
        (M5ExecutionEvidenceDisposition.RETURNED, envelope, M5RuntimeTiming()),
        (M5ExecutionEvidenceDisposition.RETURNED, different_envelope, None),
    ):
        with pytest.raises(EventConflictError):
            opened.adapter.settle_direct_verifier_atomically(
                opened.epoch_id,
                4,
                first_lease,
                candidate,
                disposition,
                M5RuntimeWork(),
                timing,
            )
        assert _snapshot(d24_direct_db.connection, _VERIFIER_LATE_TABLES) == stable


def test_reused_postterminal_verifier_return_is_audit_only_replay_exact_and_isolated(
    d24_direct_db: DirectD24Database,
) -> None:
    opened, child, lease = _open_verifier_attempt(d24_direct_db)
    cancel_direct_job_for_audit(
        opened,
        terminal_reason="epoch_failed",
        job=child,
    )
    terminal_hash = _terminalize_epoch_for_audit(opened)
    assert d24_direct_db.connection.execute(
        "SELECT clock_timestamp() < %s",
        (lease.lease_expires_at,),
    ).fetchone() == (True,)
    envelope = verifier_envelope(
        opened,
        lease,
        child,
        attempt_ordinal=1,
        make_effective=False,
    )
    before_terminal = _snapshot(
        d24_direct_db.connection,
        _POSTTERMINAL_VERIFIER_IMMUTABLE_TABLES,
    )
    receipt = opened.adapter.settle_direct_verifier_atomically(
        opened.epoch_id,
        4,
        lease,
        envelope,
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
        M5RuntimeWork(),
        None,
    )
    assert receipt.return_kind is M5TypedDirectReturnKind.VERIFIER
    assert receipt.normal is None and receipt.late is not None
    assert (
        receipt.late.disposition
        is M5DirectLateReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL
    )
    assert not receipt.late.exact_replay
    assert receipt.late.resulting_revision == 5
    assert receipt.late.current_terminal_logical_result_hash == terminal_hash
    assert receipt.late.transition_anchor is None
    assert receipt.late.expired_return_digest is None
    assert (
        _snapshot(
            d24_direct_db.connection,
            _POSTTERMINAL_VERIFIER_IMMUTABLE_TABLES,
        )
        == before_terminal
    )
    _assert_verifier_late_envelope(opened, envelope)
    assert d24_direct_db.connection.execute(
        """
        SELECT evidence.disposition, btrim(evidence.result_or_error_hash),
               timing.required_interval_observed, audit.return_kind,
               btrim(audit.return_artifact_digest),
               btrim(audit.terminal_logical_result_hash)
        FROM groundloop_m5_attempt_execution_evidence AS evidence
        JOIN groundloop_m5_post_terminal_attempt_timing AS timing
          USING (epoch_id, subgraph, attempt_id)
        JOIN groundloop_m5_post_terminal_attempt_audit AS audit
          USING (epoch_id, subgraph, attempt_id)
        WHERE evidence.epoch_id = %s AND evidence.attempt_id = %s
        """,
        (opened.epoch_id, lease.attempt_id),
    ).fetchone() == (
        "reused_artifact",
        envelope.envelope_digest,
        False,
        "terminal_audit_only",
        envelope.envelope_digest,
        terminal_hash,
    )
    assert d24_direct_db.connection.execute(
        "SELECT count(*) FROM groundloop_m5_expired_attempt_return WHERE epoch_id = %s",
        (opened.epoch_id,),
    ).fetchone() == (0,)

    stable = _snapshot(d24_direct_db.connection, _POSTTERMINAL_VERIFIER_TABLES)
    replay = opened.adapter.settle_direct_verifier_atomically(
        opened.epoch_id,
        4,
        lease,
        envelope,
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
        M5RuntimeWork(),
        None,
    )
    assert replay.late is not None and replay.late.exact_replay
    assert replay.late.resulting_revision == 5
    assert replay.late.current_terminal_logical_result_hash == terminal_hash
    assert replay.late.transition_anchor is None
    assert _snapshot(d24_direct_db.connection, _POSTTERMINAL_VERIFIER_TABLES) == stable

    different_envelope = verifier_envelope(
        opened,
        lease,
        child,
        attempt_ordinal=1,
        make_effective=True,
    )
    for disposition, candidate, timing in (
        (M5ExecutionEvidenceDisposition.RETURNED, envelope, None),
        (M5ExecutionEvidenceDisposition.REUSED_ARTIFACT, envelope, M5RuntimeTiming()),
        (M5ExecutionEvidenceDisposition.REUSED_ARTIFACT, different_envelope, None),
    ):
        with pytest.raises(EventConflictError):
            opened.adapter.settle_direct_verifier_atomically(
                opened.epoch_id,
                4,
                lease,
                candidate,
                disposition,
                M5RuntimeWork(),
                timing,
            )
        assert (
            _snapshot(d24_direct_db.connection, _POSTTERMINAL_VERIFIER_TABLES) == stable
        )
