"""Live PostgreSQL acceptance tests for the M5 job/lease lifecycle."""

from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier
from typing import Any

import pytest
from psycopg import Connection, sql

from groundloop.errors import EventConflictError
from groundloop.m5.events import RegisterGroupEvent
from groundloop.m5.runtime.contracts import (
    M5AcquisitionDisposition,
    M5AttemptCompletionReceipt,
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5JobAttempt,
    M5JobKind,
    M5JobLease,
    M5LogicalJobSpec,
    M5RunFailureReason,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5TypedEventPlan,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore

LIFECYCLE_TABLES = (
    "groundloop_epoch",
    "groundloop_m5_runtime_epoch",
    "groundloop_m5_semantic_job",
    "groundloop_m5_job_attempt",
    "groundloop_m5_owner_pending_counter",
    "groundloop_m5_answer_pending_counter",
    "groundloop_m5_runtime_work",
    "groundloop_m5_runtime_operational_config",
    "groundloop_m5_requirement_root_provenance",
    "groundloop_m5_dispatch_record",
    "groundloop_m5_attempt_execution_evidence",
    "groundloop_m5_runtime_work_contribution",
    "groundloop_m5_runtime_work_accumulator",
    "groundloop_m5_runtime_timing_contribution",
    "groundloop_m5_transition_call_timing",
    "groundloop_m5_runtime_timing_accumulator",
    "groundloop_m5_event_result",
    "groundloop_m5_event_timing_coverage",
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _root_jobs(
    plan: M5TypedEventPlan, manifest: M5CandidatePolicyManifest
) -> tuple[M5LogicalJobSpec, ...]:
    assert isinstance(plan.event, RegisterGroupEvent)
    jobs = []
    for requirement in plan.event.group.requirements:
        scope = M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            requirement_version_id=requirement.requirement_version_id,
            inserted_chunk_version_id=None,
            candidate_policy_id=manifest.candidate_policy_id,
            requirement_registry_snapshot_digest=(
                plan.requirement_registry_snapshot.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=(
                plan.active_chunk_snapshot.active_chunk_snapshot_digest
            ),
        )
        jobs.append(
            M5LogicalJobSpec.build(
                structural_event_id=plan.structural_event_id,
                job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
                manifest=manifest,
                scope=scope,
            )
        )
    return tuple(sorted(jobs, key=lambda job: job.logical_job_id))


def _open_recovery_event(
    store: PostgresM5RuntimeStore,
    database: Any,
    plan: M5TypedEventPlan,
) -> Any:
    jobs = _root_jobs(plan, database.manifest)
    return store.open_typed_event_atomically(
        plan,
        recovery_operational_config=database.operational_config,
        recovery_root_fallback_required={job.logical_job_id: False for job in jobs},
    )


def _forward_attempt_work() -> M5RuntimeWork:
    return M5RuntimeWork(
        requirement_forward_retrieval_call_count=1,
        embedding_model_call_count=1,
    )


def _attempt_timing() -> M5RuntimeTiming:
    return M5RuntimeTiming()


def _snapshot(
    connection: Connection[Any],
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    """Capture logical rows plus xmin so zero-write replay is observable."""

    connection.commit()
    captured = []
    for table_name in LIFECYCLE_TABLES:
        statement = sql.SQL(
            "SELECT xmin::text, to_jsonb(snapshot_row)::text "
            "FROM {} AS snapshot_row "
            'ORDER BY to_jsonb(snapshot_row)::text COLLATE "C", xmin::text'
        ).format(sql.Identifier(table_name))
        rows = tuple(
            (str(row[0]), str(row[1]))
            for row in connection.execute(statement).fetchall()
        )
        captured.append((table_name, rows))
    connection.commit()
    return tuple(captured)


def _pending_rows(
    connection: Connection[Any], epoch_id: int
) -> tuple[tuple[tuple[Any, ...], ...], tuple[tuple[Any, ...], ...]]:
    owner_rows = tuple(
        tuple(row)
        for row in connection.execute(
            """
            SELECT owner_claim_id, broad_reverse_scope_count,
                   forward_scope_count, verifier_job_count,
                   blocking_failure_count, pending_multiplicity,
                   updated_revision
            FROM groundloop_m5_owner_pending_counter
            WHERE epoch_id = %s
            ORDER BY owner_claim_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )
    answer_rows = tuple(
        tuple(row)
        for row in connection.execute(
            """
            SELECT answer_version_id, broad_reverse_scope_count,
                   forward_scope_count, verifier_job_count,
                   blocking_failure_count, pending_multiplicity,
                   updated_revision
            FROM groundloop_m5_answer_pending_counter
            WHERE epoch_id = %s
            ORDER BY answer_version_id COLLATE "C"
            """,
            (epoch_id,),
        ).fetchall()
    )
    return owner_rows, answer_rows


def test_acquire_persists_complete_attempt_and_exact_running_replay(
    m5_runtime_db: Any,
) -> None:
    plan = m5_runtime_db.register_plan(event_id="job-acquire")
    store = PostgresM5RuntimeStore(m5_runtime_db.connection)
    opened = _open_recovery_event(store, m5_runtime_db, plan)
    jobs = _root_jobs(plan, m5_runtime_db.manifest)
    assert len(jobs) == 2

    opened_work = store.current_event_work(opened.epoch_id)
    before_reads = _snapshot(m5_runtime_db.connection)
    assert store.current_revision(opened.epoch_id) == 1
    assert store.verifier_jobs(opened.epoch_id) == ()
    assert store.current_event_work(opened.epoch_id) == opened_work
    assert _snapshot(m5_runtime_db.connection) == before_reads

    before_pending = _pending_rows(m5_runtime_db.connection, opened.epoch_id)
    m5_runtime_db.connection.commit()
    lease = store.acquire_m5_job(opened.epoch_id, 1, jobs[0])
    assert lease.logical_job_id == jobs[0].logical_job_id
    assert lease.attempt is not None
    assert lease.attempt.logical_job_id == jobs[0].logical_job_id
    assert lease.attempt.attempt_ordinal == 1
    assert lease.attempt.execution_spec_hash == jobs[0].execution_spec_hash
    assert lease.should_execute
    assert not lease.exact_replay
    assert lease.resulting_revision == 2
    assert lease.disposition is M5AcquisitionDisposition.DISPATCH_NEW

    assert m5_runtime_db.connection.execute(
        """
        SELECT attempt_id, logical_job_id, attempt_ordinal,
               execution_spec_hash, lease_token_hash, attempt_state,
               attempt_output_digest, error_hash, finished_at
        FROM groundloop_m5_job_attempt
        WHERE attempt_id = %s
        """,
        (lease.attempt.attempt_id,),
    ).fetchone() == (
        lease.attempt.attempt_id,
        lease.attempt.logical_job_id,
        1,
        lease.attempt.execution_spec_hash,
        lease.attempt.lease_token_hash,
        "dispatched",
        None,
        None,
        None,
    )
    assert m5_runtime_db.connection.execute(
        """
        SELECT base.revision, base.structural_status, base.semantic_status,
               base.evaluation_state, runtime.revision, runtime.runtime_state,
               runtime.open_work_count, runtime.open_scope_count,
               runtime.blocking_failure_count, job.job_state
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
        WHERE base.epoch_id = %s AND job.logical_job_id = %s
        """,
        (opened.epoch_id, jobs[0].logical_job_id),
    ).fetchone() == (
        2,
        "committed",
        "pending",
        "pending",
        2,
        "semantic_pending",
        2,
        2,
        0,
        "running",
    )
    after_pending = _pending_rows(m5_runtime_db.connection, opened.epoch_id)
    assert tuple(row[:-1] for row in after_pending[0]) == tuple(
        row[:-1] for row in before_pending[0]
    )
    assert tuple(row[:-1] for row in after_pending[1]) == tuple(
        row[:-1] for row in before_pending[1]
    )
    assert {int(row[-1]) for rows in after_pending for row in rows} == {2}

    before_replay = _snapshot(m5_runtime_db.connection)
    replay = store.acquire_m5_job(opened.epoch_id, 1, jobs[0])
    assert replay.attempt == lease.attempt
    assert replay.resulting_revision == 2
    assert not replay.should_execute and replay.exact_replay
    assert replay.lease_expires_at == lease.lease_expires_at
    assert replay.dispatch_record_digest == lease.dispatch_record_digest
    assert replay.disposition is M5AcquisitionDisposition.LIVE_LEASE
    assert _snapshot(m5_runtime_db.connection) == before_replay

    with pytest.raises(EventConflictError, match="stale typed runtime revision"):
        store.acquire_m5_job(opened.epoch_id, 1, jobs[1])
    assert _snapshot(m5_runtime_db.connection) == before_replay


def test_retryable_failure_has_distinct_error_identity_and_dense_retry(
    m5_runtime_db: Any,
) -> None:
    plan = m5_runtime_db.register_plan(event_id="job-retryable-failure")
    store = PostgresM5RuntimeStore(m5_runtime_db.connection)
    opened = _open_recovery_event(store, m5_runtime_db, plan)
    job = _root_jobs(plan, m5_runtime_db.manifest)[0]
    lease = store.acquire_m5_job(opened.epoch_id, 1, job)
    assert lease.attempt is not None

    before_pending = _pending_rows(m5_runtime_db.connection, opened.epoch_id)
    m5_runtime_db.connection.commit()
    error_hash = _sha("temporary retrieval failure")
    attempt_work = _forward_attempt_work()
    attempt_timing = _attempt_timing()
    receipt = store.mark_m5_retryable_failure(
        opened.epoch_id,
        2,
        lease,
        error_hash,
        attempt_work,
        attempt_timing,
    )
    assert receipt.logical_job_id == job.logical_job_id
    assert receipt.attempt_id == lease.attempt.attempt_id
    assert receipt.resulting_revision == 3
    assert not receipt.exact_replay
    assert m5_runtime_db.connection.execute(
        """
        SELECT job.job_state, attempt.attempt_state,
               attempt.attempt_output_digest, attempt.error_hash,
               attempt.finished_at IS NOT NULL,
               base.revision, runtime.revision, runtime.runtime_state
        FROM groundloop_m5_semantic_job AS job
        JOIN groundloop_m5_job_attempt AS attempt USING (logical_job_id)
        JOIN groundloop_epoch AS base ON base.epoch_id = job.epoch_id
        JOIN groundloop_m5_runtime_epoch AS runtime
          ON runtime.epoch_id = job.epoch_id
        WHERE job.logical_job_id = %s
        """,
        (job.logical_job_id,),
    ).fetchone() == (
        "retryable_failed",
        "failed",
        None,
        error_hash,
        True,
        3,
        3,
        "semantic_pending",
    )
    after_pending = _pending_rows(m5_runtime_db.connection, opened.epoch_id)
    assert tuple(row[:-1] for row in after_pending[0]) == tuple(
        row[:-1] for row in before_pending[0]
    )
    assert tuple(row[:-1] for row in after_pending[1]) == tuple(
        row[:-1] for row in before_pending[1]
    )
    assert {int(row[-1]) for rows in after_pending for row in rows} == {3}

    before_replay = _snapshot(m5_runtime_db.connection)
    exact = store.mark_m5_retryable_failure(
        opened.epoch_id,
        2,
        lease,
        error_hash,
        attempt_work,
        attempt_timing,
    )
    assert exact.logical_job_id == receipt.logical_job_id
    assert exact.attempt_id == receipt.attempt_id
    assert exact.resulting_revision == receipt.resulting_revision
    assert exact.exact_replay
    assert _snapshot(m5_runtime_db.connection) == before_replay

    with pytest.raises(EventConflictError, match="changed immutable evidence"):
        store.mark_m5_retryable_failure(
            opened.epoch_id,
            2,
            lease,
            _sha("different retrieval failure"),
            attempt_work,
            attempt_timing,
        )
    assert _snapshot(m5_runtime_db.connection) == before_replay

    forged_attempt = M5JobAttempt.build(
        logical_job_id=lease.attempt.logical_job_id,
        attempt_ordinal=lease.attempt.attempt_ordinal,
        execution_spec_hash=lease.attempt.execution_spec_hash,
        lease_token_hash=_sha("forged lease token"),
        lease_expires_at=lease.attempt.lease_expires_at,
        attempt_work_digest=lease.attempt.attempt_work_digest,
    )
    forged_lease = replace(lease, attempt=forged_attempt)
    with pytest.raises(
        EventConflictError, match="lease attempt identity is not durable"
    ):
        store.mark_m5_retryable_failure(
            opened.epoch_id,
            2,
            forged_lease,
            error_hash,
            attempt_work,
            attempt_timing,
        )
    assert _snapshot(m5_runtime_db.connection) == before_replay

    retry = store.acquire_m5_job(opened.epoch_id, 3, job)
    assert retry.attempt is not None
    assert retry.attempt.attempt_ordinal == 2
    assert retry.attempt.attempt_id != lease.attempt.attempt_id
    assert retry.attempt.lease_token_hash != lease.attempt.lease_token_hash
    assert retry.resulting_revision == 4
    assert retry.should_execute
    assert not retry.exact_replay
    assert m5_runtime_db.connection.execute(
        """
        SELECT attempt_ordinal, attempt_state, attempt_output_digest, error_hash
        FROM groundloop_m5_job_attempt
        WHERE logical_job_id = %s
        ORDER BY attempt_ordinal
        """,
        (job.logical_job_id,),
    ).fetchall() == [(1, "failed", None, error_hash), (2, "dispatched", None, None)]

    before_successor_replay = _snapshot(m5_runtime_db.connection)
    successor_replay = store.mark_m5_retryable_failure(
        opened.epoch_id,
        2,
        lease,
        error_hash,
        attempt_work,
        attempt_timing,
    )
    assert successor_replay.exact_replay
    assert successor_replay.resulting_revision == 4
    assert _snapshot(m5_runtime_db.connection) == before_successor_replay


def test_two_acquirers_commit_one_attempt_and_one_dispatch(
    m5_runtime_db: Any,
) -> None:
    plan = m5_runtime_db.register_plan(event_id="job-acquire-race")
    opened = _open_recovery_event(
        PostgresM5RuntimeStore(m5_runtime_db.connection),
        m5_runtime_db,
        plan,
    )
    job = _root_jobs(plan, m5_runtime_db.manifest)[0]
    m5_runtime_db.connection.commit()
    start = Barrier(2)

    def acquire() -> M5JobLease:
        with m5_runtime_db.reconnect() as connection:
            start.wait(timeout=10)
            return PostgresM5RuntimeStore(connection).acquire_m5_job(
                opened.epoch_id, 1, job
            )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (executor.submit(acquire), executor.submit(acquire))
        leases = tuple(future.result(timeout=20) for future in futures)

    assert {lease.should_execute for lease in leases} == {False, True}
    assert {lease.exact_replay for lease in leases} == {False, True}
    assert {lease.resulting_revision for lease in leases} == {2}
    assert all(lease.attempt is not None for lease in leases)
    assert len({lease.attempt for lease in leases}) == 1
    with m5_runtime_db.reconnect() as reconnect:
        assert reconnect.execute(
            """
            SELECT count(*), min(attempt_ordinal), max(attempt_ordinal),
                   min(attempt_state), max(attempt_state)
            FROM groundloop_m5_job_attempt
            WHERE logical_job_id = %s
            """,
            (job.logical_job_id,),
        ).fetchone() == (1, 1, 1, "dispatched", "dispatched")
        assert reconnect.execute(
            """
            SELECT base.revision, runtime.revision, job.job_state
            FROM groundloop_epoch AS base
            JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
            JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
            WHERE base.epoch_id = %s AND job.logical_job_id = %s
            """,
            (opened.epoch_id, job.logical_job_id),
        ).fetchone() == (2, 2, "running")


def test_two_retryable_failures_commit_once_and_replay_once(
    m5_runtime_db: Any,
) -> None:
    plan = m5_runtime_db.register_plan(event_id="job-failure-race")
    store = PostgresM5RuntimeStore(m5_runtime_db.connection)
    opened = _open_recovery_event(store, m5_runtime_db, plan)
    job = _root_jobs(plan, m5_runtime_db.manifest)[0]
    lease = store.acquire_m5_job(opened.epoch_id, 1, job)
    assert lease.attempt is not None
    error_hash = _sha("shared transient failure")
    m5_runtime_db.connection.commit()
    start = Barrier(2)

    def fail() -> M5AttemptCompletionReceipt:
        with m5_runtime_db.reconnect() as connection:
            start.wait(timeout=10)
            return PostgresM5RuntimeStore(connection).mark_m5_retryable_failure(
                opened.epoch_id,
                2,
                lease,
                error_hash,
                _forward_attempt_work(),
                _attempt_timing(),
            )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (executor.submit(fail), executor.submit(fail))
        receipts = tuple(future.result(timeout=20) for future in futures)

    assert {receipt.exact_replay for receipt in receipts} == {False, True}
    assert {receipt.resulting_revision for receipt in receipts} == {3}
    assert {receipt.attempt_id for receipt in receipts} == {lease.attempt.attempt_id}
    with m5_runtime_db.reconnect() as reconnect:
        assert reconnect.execute(
            """
            SELECT job.job_state, attempt.attempt_state,
                   attempt.attempt_output_digest, attempt.error_hash,
                   base.revision, runtime.revision
            FROM groundloop_m5_semantic_job AS job
            JOIN groundloop_m5_job_attempt AS attempt USING (logical_job_id)
            JOIN groundloop_epoch AS base ON base.epoch_id = job.epoch_id
            JOIN groundloop_m5_runtime_epoch AS runtime
              ON runtime.epoch_id = job.epoch_id
            WHERE job.logical_job_id = %s
            """,
            (job.logical_job_id,),
        ).fetchone() == (
            "retryable_failed",
            "failed",
            None,
            error_hash,
            3,
            3,
        )


def test_terminal_event_work_is_read_from_immutable_persisted_row(
    m5_runtime_db: Any,
) -> None:
    plan = m5_runtime_db.register_plan(event_id="job-event-work")
    store = PostgresM5RuntimeStore(m5_runtime_db.connection)
    opened = _open_recovery_event(store, m5_runtime_db, plan)
    failure_revision = store.current_revision(opened.epoch_id)
    work_before_failure = store.current_event_work(opened.epoch_id)
    assert work_before_failure == store.current_event_work(opened.epoch_id)

    result = store.fail_typed_epoch_atomically(
        opened.epoch_id,
        expected_revision=failure_revision,
        failure_reason=M5RunFailureReason.INVARIANT_FAILURE,
        call_work=M5RuntimeWork(),
    )
    before_reads = _snapshot(m5_runtime_db.connection)
    root_count = len(_root_jobs(plan, m5_runtime_db.manifest))
    expected_work = replace(
        work_before_failure,
        requirement_cancelled_job_count=(
            work_before_failure.requirement_cancelled_job_count + root_count
        ),
        work_digest="",
    )
    assert result.event_work == expected_work
    assert result.call_work.is_zero
    assert store.current_revision(opened.epoch_id) == failure_revision + 1
    assert store.current_event_work(opened.epoch_id) == expected_work
    assert store.current_event_timing(opened.epoch_id) == (
        result.event_timing,
        result.event_timing_coverage,
    )
    assert store.verifier_jobs(opened.epoch_id) == ()
    assert _snapshot(m5_runtime_db.connection) == before_reads
